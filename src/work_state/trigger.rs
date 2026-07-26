//! Trigger storage: every statement that reads or writes a trigger.
//!
//! The pure half of the concept lives in [`crate::domain::trigger`]; this is
//! the coordination half. Two rules keep the concept owned rather than
//! scattered:
//!
//! - **Each statement is written once.** The functions here take
//!   `&Connection`, and `Transaction` dereferences to `Connection`, so a
//!   caller inside a transaction and a caller on its own lock share one
//!   statement instead of two copies that drift.
//! - **Every write is a named transition.** No call site outside this module
//!   spells `INSERT INTO triggers`, `UPDATE triggers`, or `DELETE FROM
//!   triggers`; it names the transition it wants and this module decides the
//!   SQL and the guard.

use std::collections::BTreeSet;
use std::fmt;

use rusqlite::types::Type;
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};

use crate::db::StoreError;
use crate::domain::trigger::{Effect, Trigger, TriggerKind, TriggerState};
use crate::ids::TRIGGER_ID_PREFIX;
use crate::work_state::local::LocalSqlite;

/// A trigger row to be inserted. The id is supplied because minting it is a
/// separate reservation; [`enqueue`] is the verb that does both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTrigger<'a> {
    pub id: &'a str,
    pub kind: TriggerKind,
    pub ticket_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub eligible_at_ms: Option<i64>,
    pub interval_ms: Option<i64>,
}

/// A queued trigger as the dispatcher sees it: the demand plus what it points
/// at. Convert with `Trigger::from` to ask the domain a question about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedTrigger {
    pub id: String,
    pub kind: TriggerKind,
    pub ticket_id: Option<String>,
    pub project_id: Option<String>,
    pub eligible_at_ms: Option<i64>,
    pub interval_ms: Option<i64>,
}

impl From<&QueuedTrigger> for Trigger {
    fn from(queued: &QueuedTrigger) -> Self {
        Self {
            state: TriggerState::Queued,
            kind: queued.kind,
            eligible_at_ms: queued.eligible_at_ms,
            interval_ms: queued.interval_ms,
        }
    }
}

/// What to do when the requested demand already exists as a queued trigger.
/// The two creation paths genuinely disagree, so the disagreement is an
/// argument rather than a property of which function was called.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Duplicates {
    /// Reuse a queued trigger of the same kind pinned to the same ticket, so
    /// reposting a ticket cannot pile up demand. `sloop post`.
    Reuse,
    /// Always mint a new trigger: two invocations mean two runs. `sloop run`.
    Allow,
}

/// One request for demand. `filters` are the `--only` ticket ids, which are
/// part of the request rather than a follow-up write — see [`enqueue`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnqueueRequest<'a> {
    pub kind: TriggerKind,
    pub ticket_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub eligible_at_ms: Option<i64>,
    pub interval_ms: Option<i64>,
    pub filters: &'a [String],
    pub duplicates: Duplicates,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Enqueued {
    pub id: String,
    /// Whether an existing queued trigger absorbed the request.
    pub reused: bool,
}

/// Triggers to unpin or delete alongside the rows a reindex is removing.
/// Computing the plan is separate from applying it because cross-store cleanup
/// of runs needs the doomed set before the trigger rows go away.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DeletionPlan {
    /// Triggers whose demand dies with the rows being deleted.
    pub(crate) doomed: BTreeSet<String>,
    /// Settled triggers scoped to a deleted project. They keep their history
    /// with the scope dropped, so the project row can go.
    pub(crate) unpinned: Vec<String>,
}

/// The one creation verb. Mints the id, writes the row, and writes every
/// filter — all inside the caller's transaction.
///
/// Taking a `&Transaction` rather than a `&Connection` is the fix for a bug,
/// not a style choice. The row and its filters must land together: an absent
/// filter row reads as *no restriction*, so a half-written
/// `sloop run --only TICK-5` used to become an *unrestricted* trigger able to
/// select any ready ticket in the repository. It failed open, in the direction
/// of running work nobody asked for.
pub(crate) fn enqueue(
    transaction: &Transaction<'_>,
    request: &EnqueueRequest<'_>,
    now_ms: i64,
) -> Result<Enqueued, StoreError> {
    let existing = match (request.duplicates, request.ticket_id) {
        (Duplicates::Reuse, Some(ticket_id)) => {
            queued_of_kind(transaction, ticket_id, request.kind)?
        }
        _ => None,
    };
    let (id, reused) = match existing {
        Some(id) => {
            if let Some(eligible_at_ms) = request.eligible_at_ms {
                reschedule(transaction, &id, eligible_at_ms, now_ms)?;
            }
            (id, true)
        }
        None => {
            let id = format!("{TRIGGER_ID_PREFIX}{}", reserve_ordinal(transaction)?);
            insert(
                transaction,
                &NewTrigger {
                    id: &id,
                    kind: request.kind,
                    ticket_id: request.ticket_id,
                    project_id: request.project_id,
                    eligible_at_ms: request.eligible_at_ms,
                    interval_ms: request.interval_ms,
                },
                now_ms,
            )?;
            (id, false)
        }
    };
    for ticket_id in request.filters {
        insert_filter(transaction, &id, ticket_id)?;
    }
    Ok(Enqueued { id, reused })
}

pub(crate) fn insert(
    connection: &Connection,
    trigger: &NewTrigger<'_>,
    now_ms: i64,
) -> Result<(), StoreError> {
    connection.execute(
        "INSERT INTO triggers
             (id, kind, state, ticket_id, project_id, eligible_at_ms, interval_ms,
              created_at_ms, updated_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
        params![
            trigger.id,
            trigger.kind.as_str(),
            TriggerState::Queued.as_str(),
            trigger.ticket_id,
            trigger.project_id,
            trigger.eligible_at_ms,
            trigger.interval_ms,
            now_ms,
        ],
    )?;
    Ok(())
}

pub(crate) fn insert_filter(
    connection: &Connection,
    trigger_id: &str,
    ticket_id: &str,
) -> Result<(), StoreError> {
    connection.execute(
        "INSERT OR IGNORE INTO trigger_filters (trigger_id, ticket_id) VALUES (?1, ?2)",
        params![trigger_id, ticket_id],
    )?;
    Ok(())
}

/// The queued trigger of this kind pinned to this ticket, if there is one.
/// This is what makes reposting idempotent.
pub(crate) fn queued_of_kind(
    connection: &Connection,
    ticket_id: &str,
    kind: TriggerKind,
) -> Result<Option<String>, StoreError> {
    connection
        .query_row(
            "SELECT id FROM triggers
             WHERE ticket_id = ?1 AND kind = ?2 AND state = 'queued'
             ORDER BY created_at_ms LIMIT 1",
            params![ticket_id, kind.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(StoreError::from)
}

/// Moves a queued trigger's eligibility without touching its state. Reposting
/// with a different `--at` time is the only caller.
pub(crate) fn reschedule(
    connection: &Connection,
    trigger_id: &str,
    eligible_at_ms: i64,
    now_ms: i64,
) -> Result<(), StoreError> {
    connection.execute(
        "UPDATE triggers
         SET eligible_at_ms = ?2, updated_at_ms = ?3
         WHERE id = ?1 AND state = 'queued'",
        params![trigger_id, eligible_at_ms, now_ms],
    )?;
    Ok(())
}

/// Persists the effects of firing a trigger, inside the claim's transaction.
///
/// The `state` and `kind` guards are the concurrency control, not decoration:
/// exactly one claimer can move a queued trigger, so a zero-row update means
/// another claimer won the race and the whole claim must be rejected. The
/// domain decided *which* effect; this decides nothing.
pub(crate) fn consume(
    connection: &Connection,
    trigger_id: &str,
    effects: &[Effect],
    now_ms: i64,
) -> Result<(), StoreError> {
    for effect in effects {
        let changed = match effect {
            Effect::Rearm { eligible_at_ms } => connection.execute(
                "UPDATE triggers
                 SET eligible_at_ms = ?2, updated_at_ms = ?3
                 WHERE id = ?1 AND state = 'queued' AND kind = 'every'",
                params![trigger_id, eligible_at_ms, now_ms],
            )?,
            Effect::Complete => connection.execute(
                "UPDATE triggers SET state = 'completed', updated_at_ms = ?2
                 WHERE id = ?1 AND state = 'queued' AND kind != 'every'",
                params![trigger_id, now_ms],
            )?,
            Effect::Fault(_) | Effect::Requeue { .. } => {
                return Err(StoreError::TriggerNotQueued {
                    trigger_id: trigger_id.into(),
                });
            }
        };
        if changed != 1 {
            return Err(StoreError::TriggerNotQueued {
                trigger_id: trigger_id.into(),
            });
        }
    }
    Ok(())
}

/// Returns a trigger to the queue at a given instant. A retried run hands its
/// demand back so the ticket can be picked up again once its cooldown expires.
pub(crate) fn requeue(
    connection: &Connection,
    trigger_id: &str,
    eligible_at_ms: i64,
    now_ms: i64,
) -> Result<usize, StoreError> {
    connection
        .execute(
            "UPDATE triggers
             SET state = 'queued', eligible_at_ms = ?2, updated_at_ms = ?3
             WHERE id = ?1",
            params![trigger_id, eligible_at_ms, now_ms],
        )
        .map_err(StoreError::from)
}

/// Retires the triggers pinned to a ticket that has just settled to `merged`.
/// A pinned trigger resolves through `ticket_is_dispatchable`, which demands
/// `state = 'ready'`, and a merged ticket never returns there: leaving it
/// queued is demand that can never be met but is still counted. Running this
/// in the settle transaction means the trigger dies at the instant the ticket
/// merges, with no window where the two disagree.
///
/// Kind is deliberately not consulted — see `domain::trigger::step` under
/// `Event::Completed`, which is where that rule is defined and tested. An
/// unpinned trigger is demand for whatever is ready, so it is out of scope by
/// construction: the `ticket_id` match excludes it.
pub(crate) fn complete_for_ticket(
    connection: &Connection,
    ticket_id: &str,
    now_ms: i64,
) -> Result<usize, StoreError> {
    connection
        .execute(
            "UPDATE triggers SET state = 'completed', updated_at_ms = ?2
             WHERE ticket_id = ?1 AND state = 'queued'",
            params![ticket_id, now_ms],
        )
        .map_err(StoreError::from)
}

/// One trigger's schedulable state, whatever it is — the input the domain
/// transition needs when a caller holds only an id.
pub(crate) fn facts(
    connection: &Connection,
    trigger_id: &str,
) -> Result<Option<Trigger>, StoreError> {
    let row: Option<(String, String, Option<i64>, Option<i64>)> = connection
        .query_row(
            "SELECT state, kind, eligible_at_ms, interval_ms FROM triggers WHERE id = ?1",
            params![trigger_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((state, kind, eligible_at_ms, interval_ms)) = row else {
        return Ok(None);
    };
    let state = TriggerState::parse(&state).ok_or_else(|| {
        StoreError::from(rusqlite::Error::FromSqlConversionFailure(
            0,
            Type::Text,
            Box::new(UnknownTriggerKind(state)),
        ))
    })?;
    let kind = TriggerKind::parse(&kind).ok_or_else(|| {
        StoreError::from(rusqlite::Error::FromSqlConversionFailure(
            1,
            Type::Text,
            Box::new(UnknownTriggerKind(kind)),
        ))
    })?;
    Ok(Some(Trigger {
        state,
        kind,
        eligible_at_ms,
        interval_ms,
    }))
}

/// The queued triggers, oldest first. This is the queue as `sloop status`
/// reports it, without the due-ness filter.
pub(crate) fn queued(connection: &Connection) -> Result<Vec<QueuedTrigger>, StoreError> {
    let mut statement = connection.prepare(
        "SELECT id, kind, ticket_id, project_id, eligible_at_ms, interval_ms
         FROM triggers WHERE state = 'queued'
         ORDER BY created_at_ms, id",
    )?;
    let triggers = statement
        .query_map([], queued_trigger)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(triggers)
}

/// The due triggers, oldest first — the dispatcher's scan.
pub(crate) fn dispatchable(
    connection: &Connection,
    now_ms: i64,
) -> Result<Vec<QueuedTrigger>, StoreError> {
    let mut statement = connection.prepare(&format!(
        "SELECT id, kind, ticket_id, project_id, eligible_at_ms, interval_ms
         FROM triggers
         WHERE {}
         ORDER BY created_at_ms, id",
        due_predicate("", "?1")
    ))?;
    let triggers = statement
        .query_map(params![now_ms], queued_trigger)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(triggers)
}

/// The earliest future eligibility instant, which is how long the dispatcher
/// may sleep before demand it already holds becomes due.
pub(crate) fn next_eligible_at_ms(
    connection: &Connection,
    now_ms: i64,
) -> Result<Option<i64>, StoreError> {
    connection
        .query_row(
            "SELECT MIN(eligible_at_ms) FROM triggers
             WHERE state = 'queued' AND eligible_at_ms > ?1",
            params![now_ms],
            |row| row.get(0),
        )
        .map_err(StoreError::from)
}

/// The due trigger that would select this ticket, if any. Shared by the claim
/// path, which asks inside its transaction, and by the reporting path, which
/// asks read-only.
pub(crate) fn claimable_on(
    connection: &Connection,
    ticket_id: &str,
    now_ms: i64,
) -> Result<Option<QueuedTrigger>, StoreError> {
    connection
        .query_row(
            &format!(
                "SELECT tr.id, tr.kind, tr.ticket_id, tr.project_id, tr.eligible_at_ms,
                        tr.interval_ms
                 FROM triggers tr
                 JOIN tickets t ON t.id = ?1
                 WHERE {due}
                   AND {targets}
                   AND {passes_filters}
                 ORDER BY tr.created_at_ms, tr.id
                 LIMIT 1",
                due = due_predicate("tr.", "?2"),
                targets = TARGETS_TICKET,
                passes_filters = passes_filters("tr.id"),
            ),
            params![ticket_id, now_ms],
            queued_trigger,
        )
        .optional()
        .map_err(StoreError::from)
}

/// The trigger a released run should hand its demand back to when the lease
/// did not record one. Deliberately looser than [`claimable_on`]: the trigger
/// that fired has already been completed or rearmed, so this searches settled
/// and recurring ones, most recently touched first.
pub(crate) fn for_release(
    connection: &Connection,
    ticket_id: &str,
) -> Result<Option<String>, StoreError> {
    connection
        .query_row(
            &format!(
                "SELECT tr.id
                 FROM triggers tr
                 JOIN tickets t ON t.id = ?1
                 WHERE (tr.state = 'completed' OR (tr.state = 'queued' AND tr.kind = 'every'))
                   AND {targets}
                   AND {passes_filters}
                 ORDER BY tr.updated_at_ms DESC, tr.created_at_ms, tr.id
                 LIMIT 1",
                targets = TARGETS_TICKET,
                passes_filters = passes_filters("tr.id"),
            ),
            params![ticket_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(StoreError::from)
}

/// Which triggers a reindex must unpin or delete. Read-only, so the caller can
/// hand the doomed set to run cleanup before the rows disappear.
pub(crate) fn deletion_plan(
    connection: &Connection,
    stale_tickets: &[String],
    stale_projects: &[String],
) -> Result<DeletionPlan, StoreError> {
    let mut plan = DeletionPlan::default();
    for ticket_id in stale_tickets {
        let mut statement = connection.prepare("SELECT id FROM triggers WHERE ticket_id = ?1")?;
        plan.doomed.extend(
            statement
                .query_map(params![ticket_id], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    for project_id in stale_projects {
        let mut statement =
            connection.prepare("SELECT id, state FROM triggers WHERE project_id = ?1")?;
        let triggers = statement
            .query_map(params![project_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (trigger_id, state) in triggers {
            if state == TriggerState::Queued.as_str() {
                plan.doomed.insert(trigger_id);
            } else {
                plan.unpinned.push(trigger_id);
            }
        }
    }
    Ok(plan)
}

/// Carries out a [`DeletionPlan`] and returns the number of rows deleted.
/// Unpinning is not counted: nothing was dropped, only rescoped.
pub(crate) fn apply_deletion(
    connection: &Connection,
    plan: &DeletionPlan,
) -> Result<usize, StoreError> {
    for trigger_id in &plan.unpinned {
        connection.execute(
            "UPDATE triggers SET project_id = NULL WHERE id = ?1",
            params![trigger_id],
        )?;
    }
    let mut deleted = 0;
    for trigger_id in &plan.doomed {
        deleted += connection.execute(
            "DELETE FROM trigger_filters WHERE trigger_id = ?1",
            params![trigger_id],
        )?;
        deleted += connection.execute("DELETE FROM triggers WHERE id = ?1", params![trigger_id])?;
    }
    Ok(deleted)
}

/// Drops the `--only` restrictions naming a ticket that no longer exists.
/// Separate from [`apply_deletion`] because it is the filter rows that point
/// at the ticket, not the triggers.
pub(crate) fn delete_filters_for_ticket(
    connection: &Connection,
    ticket_id: &str,
) -> Result<usize, StoreError> {
    connection
        .execute(
            "DELETE FROM trigger_filters WHERE ticket_id = ?1",
            params![ticket_id],
        )
        .map_err(StoreError::from)
}

/// Reserves the next trigger ordinal, taking the high-water mark of the live
/// table into account so a restored or hand-edited database cannot mint an id
/// that already exists.
fn reserve_ordinal(connection: &Connection) -> Result<i64, StoreError> {
    let reserved: i64 = connection.query_row(
        "SELECT next_ordinal FROM id_counters WHERE kind = 'trigger'",
        [],
        |row| row.get(0),
    )?;
    let existing: i64 = connection.query_row(
        "SELECT COALESCE(MAX(CAST(SUBSTR(id, 3) AS INTEGER)), 0) + 1 FROM triggers",
        [],
        |row| row.get(0),
    )?;
    let ordinal = reserved.max(existing);
    connection.execute(
        "UPDATE id_counters SET next_ordinal = ?1 WHERE kind = 'trigger'",
        params![ordinal + 1],
    )?;
    Ok(ordinal)
}

/// The SQL mirror of [`Trigger::is_due`], written once and formatted into every
/// query that scans for due demand, so the two cannot drift into disagreement
/// the way two hand-copied predicates would. `alias` qualifies the trigger
/// table (`""` or `"tr."`) and `now` names the bound parameter.
fn due_predicate(alias: &str, now: &str) -> String {
    format!(
        "{alias}state = 'queued' \
         AND ({alias}kind IN ('immediate', 'auto') OR {alias}eligible_at_ms <= {now})"
    )
}

/// Whether trigger `tr` aims at ticket `t`: pinned to it, or unpinned and
/// either unscoped or scoped to the ticket's project.
const TARGETS_TICKET: &str = "(tr.ticket_id = t.id
     OR (tr.ticket_id IS NULL
         AND (tr.project_id IS NULL OR tr.project_id = t.project_id)))";

/// Whether ticket `t` survives the `--only` restriction of the trigger named by
/// the SQL expression `trigger` — a column reference in the join queries here,
/// a bound parameter in ticket selection.
///
/// Absent filter rows mean *no restriction*, which is exactly why [`enqueue`]
/// must write a trigger and its filters atomically: a trigger that outlived its
/// filters would read as unrestricted.
pub(crate) fn passes_filters(trigger: &str) -> String {
    format!(
        "(NOT EXISTS (SELECT 1 FROM trigger_filters f
                      WHERE f.trigger_id = {trigger})
         OR EXISTS (SELECT 1 FROM trigger_filters f
                    WHERE f.trigger_id = {trigger} AND f.ticket_id = t.id))"
    )
}

#[derive(Debug)]
struct UnknownTriggerKind(String);

impl fmt::Display for UnknownTriggerKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "unknown trigger kind `{}`", self.0)
    }
}

impl std::error::Error for UnknownTriggerKind {}

fn queued_trigger(row: &Row<'_>) -> rusqlite::Result<QueuedTrigger> {
    let raw: String = row.get(1)?;
    let kind = TriggerKind::parse(&raw).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(1, Type::Text, Box::new(UnknownTriggerKind(raw)))
    })?;
    Ok(QueuedTrigger {
        id: row.get(0)?,
        kind,
        ticket_id: row.get(2)?,
        project_id: row.get(3)?,
        eligible_at_ms: row.get(4)?,
        interval_ms: row.get(5)?,
    })
}

impl LocalSqlite {
    /// The standalone half of [`enqueue`]: opens the transaction the atomicity
    /// guarantee needs, for a caller that has none of its own. `sloop run`
    /// takes this path; `sloop post` joins its ticket write instead.
    pub(crate) fn enqueue_trigger(
        &self,
        request: &EnqueueRequest<'_>,
        now_ms: i64,
    ) -> Result<Enqueued, StoreError> {
        let db = self.db();
        let mut connection = db.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let enqueued = enqueue(&transaction, request, now_ms)?;
        transaction.commit()?;
        Ok(enqueued)
    }

    /// Seeding primitive: writes one trigger row with a caller-chosen id.
    /// Tests use it to build a fixture queue; production creation is
    /// [`LocalSqlite::enqueue_trigger`], which mints the id and cannot leave a
    /// trigger without its filters.
    pub fn insert_trigger(&self, trigger: &NewTrigger<'_>, now_ms: i64) -> Result<(), StoreError> {
        let db = self.db();
        let connection = db.lock();
        insert(&connection, trigger, now_ms)
    }

    /// Seeding primitive; see [`LocalSqlite::insert_trigger`].
    pub fn insert_trigger_filter(
        &self,
        trigger_id: &str,
        ticket_id: &str,
    ) -> Result<(), StoreError> {
        let db = self.db();
        let connection = db.lock();
        insert_filter(&connection, trigger_id, ticket_id)
    }

    pub fn queued_triggers(&self) -> Result<Vec<QueuedTrigger>, StoreError> {
        let db = self.db();
        let connection = db.lock();
        queued(&connection)
    }

    pub fn dispatchable_triggers(&self, now_ms: i64) -> Result<Vec<QueuedTrigger>, StoreError> {
        let db = self.db();
        let connection = db.lock();
        dispatchable(&connection, now_ms)
    }

    /// Whether any queued trigger could select this ticket: the reporting
    /// mirror of the claim path's selection, asked per ticket rather than as
    /// "does the queue hold anything at all".
    pub fn has_claimable_trigger(&self, ticket_id: &str, now_ms: i64) -> Result<bool, StoreError> {
        let db = self.db();
        let connection = db.lock();
        Ok(claimable_on(&connection, ticket_id, now_ms)?.is_some())
    }

    pub fn next_trigger_eligible_at_ms(&self, now_ms: i64) -> Result<Option<i64>, StoreError> {
        let db = self.db();
        let connection = db.lock();
        next_eligible_at_ms(&connection, now_ms)
    }

    /// Retires triggers left queued against a ticket that merged before the
    /// settle path knew to retire them. [`complete_for_ticket`] only applies
    /// from the next settlement onwards, and a merged ticket never settles
    /// again, so anything already stranded needs this one-off sweep.
    ///
    /// Returns the `(trigger_id, ticket_id)` pairs it completed, so the caller
    /// can report a startup mutation rather than perform it silently. A
    /// database with nothing stranded selects no rows and writes nothing, which
    /// makes repeated runs free.
    pub fn complete_merged_ticket_triggers(
        &self,
        now_ms: i64,
    ) -> Result<Vec<(String, String)>, StoreError> {
        let db = self.db();
        let mut connection = db.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let stranded = {
            let mut statement = transaction.prepare(
                "SELECT tr.id, tr.ticket_id
                 FROM triggers tr
                 JOIN tickets t ON t.id = tr.ticket_id
                 WHERE tr.state = 'queued' AND t.state = 'merged'
                 ORDER BY tr.created_at_ms, tr.id",
            )?;
            statement
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<Result<Vec<(String, String)>, _>>()?
        };
        if stranded.is_empty() {
            return Ok(Vec::new());
        }
        let tickets: BTreeSet<&str> = stranded
            .iter()
            .map(|(_, ticket_id)| ticket_id.as_str())
            .collect();
        for ticket_id in tickets {
            complete_for_ticket(&transaction, ticket_id, now_ms)?;
        }
        transaction.commit()?;
        Ok(stranded)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::db::Db;
    use crate::domain::ticket::TicketState;
    use crate::domain::trigger::{Event, Fault, step};

    fn seeded(path: &std::path::Path) -> LocalSqlite {
        let store = LocalSqlite::from_db(Db::open(path, 1_000).unwrap());
        store
            .insert_local_project("default", "projects/default.md", "Default", 1_000)
            .unwrap();
        for ticket in ["T1", "T2", "T3"] {
            store
                .insert_local_ticket(
                    ticket,
                    "default",
                    &format!("tickets/{ticket}.md"),
                    ticket,
                    &[],
                    &format!("sloop/{ticket}"),
                    Some("claude"),
                    None,
                    None,
                    "default",
                    TicketState::Ready,
                    1_000,
                )
                .unwrap();
        }
        store
    }

    fn request<'a>(kind: TriggerKind, filters: &'a [String]) -> EnqueueRequest<'a> {
        EnqueueRequest {
            kind,
            ticket_id: None,
            project_id: None,
            eligible_at_ms: None,
            interval_ms: None,
            filters,
            duplicates: Duplicates::Allow,
        }
    }

    /// Every combination of state, kind, and schedule, so neither the SQL nor
    /// the domain function can be right by coincidence on a narrow fixture.
    fn due_ness_matrix() -> Vec<(String, Trigger)> {
        let mut rows = Vec::new();
        let mut ordinal = 0;
        for state in [
            TriggerState::Queued,
            TriggerState::Completed,
            TriggerState::Cancelled,
        ] {
            for kind in [
                TriggerKind::Immediate,
                TriggerKind::Auto,
                TriggerKind::At,
                TriggerKind::Every,
                TriggerKind::Overnight,
            ] {
                for eligible_at_ms in [None, Some(1_999), Some(2_000), Some(2_001)] {
                    ordinal += 1;
                    rows.push((
                        format!("TR{ordinal}"),
                        Trigger {
                            state,
                            kind,
                            eligible_at_ms,
                            interval_ms: Some(60_000),
                        },
                    ));
                }
            }
        }
        rows
    }

    /// The anti-drift test the reporting and dispatch gates did not have. The
    /// SQL predicate exists only so a large queue can be filtered in the
    /// database; `Trigger::is_due` is the definition, and if the two ever
    /// disagree this fails rather than silently dispatching the wrong set.
    #[test]
    fn the_sql_due_predicate_and_the_domain_function_agree() {
        let directory = tempdir().unwrap();
        let store = seeded(&directory.path().join("sloop.db"));
        let matrix = due_ness_matrix();
        {
            let db = store.db();
            let connection = db.lock();
            for (id, trigger) in &matrix {
                insert(
                    &connection,
                    &NewTrigger {
                        id,
                        kind: trigger.kind,
                        ticket_id: None,
                        project_id: None,
                        eligible_at_ms: trigger.eligible_at_ms,
                        interval_ms: trigger.interval_ms,
                    },
                    1_000,
                )
                .unwrap();
                connection
                    .execute(
                        "UPDATE triggers SET state = ?2 WHERE id = ?1",
                        params![id, trigger.state.as_str()],
                    )
                    .unwrap();
            }
        }

        for now_ms in [0, 1_999, 2_000, 2_001, i64::MAX] {
            let from_sql: BTreeSet<String> = store
                .dispatchable_triggers(now_ms)
                .unwrap()
                .into_iter()
                .map(|trigger| trigger.id)
                .collect();
            let from_domain: BTreeSet<String> = matrix
                .iter()
                .filter(|(_, trigger)| trigger.is_due(now_ms))
                .map(|(id, _)| id.clone())
                .collect();
            assert_eq!(from_sql, from_domain, "disagreement at now_ms = {now_ms}");
        }
        assert!(!store.dispatchable_triggers(2_000).unwrap().is_empty());
    }

    /// The claim path's per-ticket lookup must answer with the same due-ness as
    /// the dispatcher's scan; both format the one shared predicate.
    #[test]
    fn the_claim_path_lookup_shares_the_dispatch_scan_due_ness() {
        let directory = tempdir().unwrap();
        let store = seeded(&directory.path().join("sloop.db"));
        for (ordinal, kind) in [TriggerKind::Immediate, TriggerKind::At, TriggerKind::Every]
            .into_iter()
            .enumerate()
        {
            store
                .insert_trigger(
                    &NewTrigger {
                        id: &format!("TR{}", ordinal + 1),
                        kind,
                        ticket_id: Some("T1"),
                        project_id: None,
                        eligible_at_ms: Some(2_000),
                        interval_ms: Some(60_000),
                    },
                    1_000,
                )
                .unwrap();
        }

        for now_ms in [1_999, 2_000] {
            let scanned = store
                .dispatchable_triggers(now_ms)
                .unwrap()
                .into_iter()
                .next()
                .map(|trigger| trigger.id);
            let db = store.db();
            let connection = db.lock();
            let claimable = claimable_on(&connection, "T1", now_ms)
                .unwrap()
                .map(|trigger| trigger.id);
            assert_eq!(scanned, claimable, "at now_ms = {now_ms}");
        }
    }

    /// The bug the second creation path hid. `select_ready_ticket` treats
    /// absent filter rows as *no restriction*, so a trigger that survived
    /// without its filters would select any ready ticket in the repository.
    #[test]
    fn a_failure_after_the_row_insert_leaves_no_trigger_at_all() {
        let directory = tempdir().unwrap();
        let store = seeded(&directory.path().join("sloop.db"));
        let filters = ["T2".to_owned(), "GONE".to_owned()];
        let error = store
            .enqueue_trigger(&request(TriggerKind::Immediate, &filters), 2_000)
            .expect_err("a filter naming a missing ticket must be rejected");
        assert!(
            matches!(error, StoreError::Sqlite(_)),
            "unexpected error: {error}"
        );

        assert!(
            store.queued_triggers().unwrap().is_empty(),
            "the trigger row outlived its filters, so it is now unrestricted"
        );
        let db = store.db();
        let connection = db.lock();
        let filter_rows: i64 = connection
            .query_row("SELECT COUNT(*) FROM trigger_filters", [], |row| row.get(0))
            .unwrap();
        assert_eq!(filter_rows, 0);
    }

    #[test]
    fn enqueue_writes_the_row_and_every_filter_together() {
        let directory = tempdir().unwrap();
        let store = seeded(&directory.path().join("sloop.db"));
        let filters = ["T2".to_owned(), "T3".to_owned()];
        let enqueued = store
            .enqueue_trigger(&request(TriggerKind::Immediate, &filters), 2_000)
            .unwrap();
        assert!(!enqueued.reused);

        let queued = store.queued_triggers().unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].id, enqueued.id);
        assert_eq!(queued[0].kind, TriggerKind::Immediate);
        assert_eq!(
            store
                .select_ready_ticket(None, &enqueued.id, 2_000)
                .unwrap()
                .as_deref(),
            Some("T2")
        );
    }

    #[test]
    fn reuse_absorbs_a_repost_while_allow_mints_a_second_trigger() {
        let directory = tempdir().unwrap();
        let store = seeded(&directory.path().join("sloop.db"));
        let reposted = EnqueueRequest {
            ticket_id: Some("T1"),
            duplicates: Duplicates::Reuse,
            ..request(TriggerKind::Auto, &[])
        };
        let first = store.enqueue_trigger(&reposted, 2_000).unwrap();
        let second = store.enqueue_trigger(&reposted, 3_000).unwrap();
        assert_eq!(first.id, second.id);
        assert!(second.reused);
        assert_eq!(store.queued_triggers().unwrap().len(), 1);

        let scheduled = EnqueueRequest {
            eligible_at_ms: Some(9_000),
            ..EnqueueRequest {
                ticket_id: Some("T1"),
                duplicates: Duplicates::Reuse,
                ..request(TriggerKind::At, &[])
            }
        };
        let third = store.enqueue_trigger(&scheduled, 3_000).unwrap();
        assert_ne!(third.id, first.id);
        assert_eq!(store.queued_triggers().unwrap().len(), 2);

        let retimed = EnqueueRequest {
            eligible_at_ms: Some(11_000),
            ..scheduled.clone()
        };
        let fourth = store.enqueue_trigger(&retimed, 4_000).unwrap();
        assert_eq!(fourth.id, third.id);
        let requeued = store.queued_triggers().unwrap();
        assert_eq!(requeued.len(), 2);
        assert_eq!(
            requeued
                .iter()
                .find(|trigger| trigger.id == third.id)
                .unwrap()
                .eligible_at_ms,
            Some(11_000)
        );

        let run = EnqueueRequest {
            ticket_id: Some("T1"),
            ..request(TriggerKind::Immediate, &[])
        };
        let fifth = store.enqueue_trigger(&run, 5_000).unwrap();
        let sixth = store.enqueue_trigger(&run, 5_000).unwrap();
        assert_ne!(fifth.id, sixth.id);
        assert_eq!(store.queued_triggers().unwrap().len(), 4);
    }

    #[test]
    fn ids_never_collide_with_a_trigger_the_counter_has_forgotten() {
        let directory = tempdir().unwrap();
        let store = seeded(&directory.path().join("sloop.db"));
        store
            .insert_trigger(
                &NewTrigger {
                    id: "TR7",
                    kind: TriggerKind::Immediate,
                    ticket_id: Some("T1"),
                    project_id: None,
                    eligible_at_ms: None,
                    interval_ms: None,
                },
                1_000,
            )
            .unwrap();
        let enqueued = store
            .enqueue_trigger(&request(TriggerKind::Immediate, &[]), 2_000)
            .unwrap();
        assert_eq!(enqueued.id, "TR8");
    }

    /// The transition and its storage guard, together: firing a recurring
    /// trigger rearms it and leaves it queued, firing a one-shot one retires it,
    /// and a claimer that arrives after the retirement is told so.
    #[test]
    fn consume_persists_the_transition_the_domain_chose() {
        let directory = tempdir().unwrap();
        let store = seeded(&directory.path().join("sloop.db"));
        let recurring = store
            .enqueue_trigger(
                &EnqueueRequest {
                    eligible_at_ms: Some(2_000),
                    interval_ms: Some(60_000),
                    ..request(TriggerKind::Every, &[])
                },
                1_000,
            )
            .unwrap();
        let once = store
            .enqueue_trigger(&request(TriggerKind::Immediate, &[]), 1_000)
            .unwrap();

        let db = store.db();
        let connection = db.lock();
        for (id, expected) in [
            (
                &recurring.id,
                vec![Effect::Rearm {
                    eligible_at_ms: 62_000,
                }],
            ),
            (&once.id, vec![Effect::Complete]),
        ] {
            let row = queued(&connection)
                .unwrap()
                .into_iter()
                .find(|queued| &queued.id == id)
                .expect("trigger is queued");
            let mut trigger = Trigger::from(&row);
            let effects = step(&mut trigger, Event::Fired, 2_000);
            assert_eq!(effects, expected);
            consume(&connection, id, &effects, 2_000).unwrap();
        }
        assert_eq!(
            queued(&connection).unwrap().len(),
            1,
            "the recurring trigger stays queued and the one-shot one does not"
        );
        assert!(matches!(
            consume(&connection, &once.id, &[Effect::Complete], 2_000),
            Err(StoreError::TriggerNotQueued { .. })
        ));
    }

    #[test]
    fn a_faulting_effect_is_never_written() {
        let directory = tempdir().unwrap();
        let store = seeded(&directory.path().join("sloop.db"));
        let enqueued = store
            .enqueue_trigger(
                &EnqueueRequest {
                    eligible_at_ms: Some(2_000),
                    interval_ms: None,
                    ..request(TriggerKind::Every, &[])
                },
                1_000,
            )
            .unwrap();
        let db = store.db();
        let connection = db.lock();
        let mut trigger = Trigger::from(&queued(&connection).unwrap()[0]);
        let effects = step(&mut trigger, Event::Fired, 2_000);
        assert_eq!(effects, [Effect::Fault(Fault::InvalidCadence)]);
        assert!(matches!(
            consume(&connection, &enqueued.id, &effects, 2_000),
            Err(StoreError::TriggerNotQueued { .. })
        ));
        assert_eq!(queued(&connection).unwrap().len(), 1);
    }
}
