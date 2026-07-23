use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};

use crate::db::Db;
use crate::domain::ticket::TicketState;
use crate::store::{ClaimRequest, StoreError};

const TICKET_RECORD_SELECT: &str =
    "SELECT id, project_id, file_path, source, source_ref, state, name, worktree,
            target, model, effort, flow, attempts, body, held_reason, created_at_ms
     FROM tickets";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TicketCounts {
    pub ready: u64,
    pub held: u64,
    pub blocked: u64,
    pub claimed: u64,
    pub merged: u64,
    pub failed: u64,
    pub needs_review: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectRecord {
    pub id: String,
    pub file_path: Option<String>,
    pub title: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalTicketFile {
    pub id: String,
    pub file_path: String,
    pub state: String,
    pub missing_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TicketRecord {
    pub id: String,
    pub project_id: String,
    pub file_path: Option<String>,
    pub source: String,
    pub source_ref: Option<String>,
    pub state: String,
    pub name: String,
    pub blocked_by: Vec<String>,
    pub worktree: Option<String>,
    pub target: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub flow: Option<String>,
    pub attempts: i64,
    pub body: Option<String>,
    pub held_reason: Option<String>,
    /// When the ticket was registered. `sloop list` orders on this.
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReindexTicket {
    pub id: String,
    pub project_id: String,
    pub source: String,
    pub source_ref: String,
    pub file_path: Option<String>,
    pub name: String,
    pub blocked_by: Vec<String>,
    pub worktree: String,
    pub target: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub flow: String,
    pub body: String,
    pub held_reason: Option<String>,
    pub derived_state: Option<TicketState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReindexStateChange {
    pub ticket_id: String,
    pub previous_state: String,
    pub state: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReindexResult {
    pub state_changes: Vec<ReindexStateChange>,
    pub rows_dropped: usize,
}

fn ticket_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<TicketRecord> {
    Ok(TicketRecord {
        id: row.get(0)?,
        project_id: row.get(1)?,
        file_path: row.get(2)?,
        source: row.get(3)?,
        source_ref: row.get(4)?,
        state: row.get(5)?,
        name: row.get(6)?,
        blocked_by: Vec::new(),
        worktree: row.get(7)?,
        target: row.get(8)?,
        model: row.get(9)?,
        effort: row.get(10)?,
        flow: row.get(11)?,
        attempts: row.get(12)?,
        body: row.get(13)?,
        held_reason: row.get(14)?,
        created_at_ms: row.get(15)?,
    })
}

pub(crate) mod tx {
    use super::*;

    pub(crate) fn replace_ticket_blockers(
        transaction: &Transaction<'_>,
        ticket_id: &str,
        blocked_by: &[String],
    ) -> rusqlite::Result<()> {
        transaction.execute(
            "DELETE FROM ticket_blockers WHERE ticket_id = ?1",
            params![ticket_id],
        )?;
        for (position, blocker_id) in blocked_by.iter().enumerate() {
            transaction.execute(
                "INSERT OR IGNORE INTO ticket_blockers (ticket_id, blocker_id, position)
                 VALUES (?1, ?2, ?3)",
                params![ticket_id, blocker_id, position as i64],
            )?;
        }
        Ok(())
    }

    pub(crate) fn claim_ticket(
        transaction: &Transaction<'_>,
        claim: &ClaimRequest<'_>,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        let changed = transaction.execute(
            "UPDATE tickets
             SET state = 'claimed', held_reason = NULL, attempts = attempts + 1, updated_at_ms = ?2
             WHERE id = ?1 AND state = 'ready' AND missing_at_ms IS NULL
               AND NOT EXISTS (SELECT 1 FROM ticket_blockers b
                               JOIN tickets bt ON bt.id = b.blocker_id
                               WHERE b.ticket_id = tickets.id
                                 AND bt.state != 'merged')",
            params![claim.ticket_id, now_ms],
        )?;
        if changed != 1 {
            let state: Option<String> = transaction
                .query_row(
                    "SELECT CASE
                              WHEN missing_at_ms IS NOT NULL THEN 'missing'
                              WHEN state = 'ready' AND EXISTS (
                                  SELECT 1 FROM ticket_blockers b
                                  JOIN tickets bt ON bt.id = b.blocker_id
                                  WHERE b.ticket_id = tickets.id
                                    AND bt.state != 'merged'
                              ) THEN 'blocked'
                              ELSE state
                            END
                     FROM tickets WHERE id = ?1",
                    params![claim.ticket_id],
                    |row| row.get(0),
                )
                .optional()?;
            return Err(StoreError::TicketNotReady {
                ticket_id: claim.ticket_id.into(),
                state,
            });
        }
        Ok(())
    }

    pub(crate) fn settle_ticket(
        transaction: &Transaction<'_>,
        ticket_id: &str,
        ticket_state: TicketState,
        now_ms: i64,
    ) -> rusqlite::Result<usize> {
        transaction.execute(
            "UPDATE tickets SET state = ?2, held_reason = NULL, updated_at_ms = ?3
             WHERE id = ?1 AND state = 'claimed'",
            params![ticket_id, ticket_state.as_str(), now_ms],
        )
    }

    pub(crate) fn abort_ticket(
        transaction: &Transaction<'_>,
        ticket_id: &str,
        now_ms: i64,
    ) -> rusqlite::Result<usize> {
        transaction.execute(
            "UPDATE tickets SET state = 'ready', held_reason = NULL, updated_at_ms = ?2
             WHERE id = ?1 AND state = 'claimed'",
            params![ticket_id, now_ms],
        )
    }

    pub(crate) fn settle_external_merge(
        transaction: &Transaction<'_>,
        ticket_id: &str,
        now_ms: i64,
    ) -> rusqlite::Result<usize> {
        transaction.execute(
            "UPDATE tickets SET state = 'merged', held_reason = NULL, updated_at_ms = ?2
             WHERE id = ?1 AND state = 'needs_review'",
            params![ticket_id, now_ms],
        )
    }

    pub(crate) fn retry_ticket(
        transaction: &Transaction<'_>,
        id: &str,
        now_ms: i64,
    ) -> rusqlite::Result<usize> {
        transaction.execute(
            "UPDATE tickets SET state = 'ready', held_reason = NULL, attempts = 0, updated_at_ms = ?2
             WHERE id = ?1 AND state = 'failed'",
            params![id, now_ms],
        )
    }
}

pub struct LocalSqlite {
    db: Db,
}

impl LocalSqlite {
    pub(crate) fn from_db(db: Db) -> Self {
        Self { db }
    }

    pub fn insert_local_project(
        &self,
        id: &str,
        file_path: &str,
        title: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.db.lock().execute(
            "INSERT INTO projects (id, file_path, source, title, created_at_ms, updated_at_ms)
             VALUES (?1, ?2, 'local', ?3, ?4, ?4)",
            params![id, file_path, title, now_ms],
        )?;
        Ok(())
    }

    /// Inserts or refreshes a project indexed from a committed file. Startup
    /// and reindex call this for every configured project file, so it must tolerate
    /// rows that already exist.
    pub fn upsert_local_project(
        &self,
        id: &str,
        file_path: &str,
        title: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.db.lock().execute(
            "INSERT INTO projects (id, file_path, source, title, created_at_ms, updated_at_ms)
             VALUES (?1, ?2, 'local', ?3, ?4, ?4)
             ON CONFLICT(id) DO UPDATE SET
                 file_path = excluded.file_path,
                 title = excluded.title,
                 updated_at_ms = excluded.updated_at_ms",
            params![id, file_path, title, now_ms],
        )?;
        Ok(())
    }

    pub fn project_exists(&self, id: &str) -> Result<bool, StoreError> {
        let found: Option<i64> = self
            .db
            .lock()
            .query_row("SELECT 1 FROM projects WHERE id = ?1", params![id], |row| {
                row.get(0)
            })
            .optional()?;
        Ok(found.is_some())
    }

    pub fn project(&self, id: &str) -> Result<Option<ProjectRecord>, StoreError> {
        self.db
            .lock()
            .query_row(
                "SELECT id, file_path, title FROM projects WHERE id = ?1",
                params![id],
                |row| {
                    Ok(ProjectRecord {
                        id: row.get(0)?,
                        file_path: row.get(1)?,
                        title: row.get(2)?,
                    })
                },
            )
            .optional()
            .map_err(StoreError::from)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_local_ticket(
        &self,
        id: &str,
        project_id: &str,
        file_path: &str,
        name: &str,
        blocked_by: &[String],
        worktree: &str,
        target: Option<&str>,
        model: Option<&str>,
        effort: Option<&str>,
        flow: &str,
        state: TicketState,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        let mut connection = self.db.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "INSERT INTO tickets
                 (id, project_id, file_path, source, state, name, worktree, target, model, effort,
                    flow, created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, 'local', ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?11)",
            params![
                id,
                project_id,
                file_path,
                state.as_str(),
                name,
                worktree,
                target,
                model,
                effort,
                flow,
                now_ms
            ],
        )?;
        tx::replace_ticket_blockers(&transaction, id, blocked_by)?;
        transaction.commit()?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_local_ticket(
        &self,
        id: &str,
        name: &str,
        blocked_by: &[String],
        worktree: &str,
        target: Option<&str>,
        model: Option<&str>,
        effort: Option<&str>,
        flow: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        let mut connection = self.db.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "UPDATE tickets
             SET name = ?2, worktree = ?3, target = ?4, model = ?5, effort = ?6, flow = ?7,
                  held_reason = NULL, missing_at_ms = NULL, updated_at_ms = ?8
             WHERE id = ?1",
            params![id, name, worktree, target, model, effort, flow, now_ms],
        )?;
        tx::replace_ticket_blockers(&transaction, id, blocked_by)?;
        transaction.commit()?;
        Ok(())
    }

    /// Applies a complete authored ticket snapshot without disturbing runtime
    /// history for IDs that remain present. Cross-store cleanup is supplied by
    /// coordination and runs in this transaction.
    pub(crate) fn apply_reindex<DropRuns, MarkRuns>(
        &self,
        project_ids: &[String],
        tickets: &[ReindexTicket],
        now_ms: i64,
        mut drop_runs: DropRuns,
        mut mark_runs_cleanup_eligible: MarkRuns,
    ) -> Result<ReindexResult, StoreError>
    where
        DropRuns:
            FnMut(&Transaction<'_>, &[String], &BTreeSet<String>) -> Result<usize, StoreError>,
        MarkRuns: FnMut(&Transaction<'_>, &str, i64) -> Result<(), StoreError>,
    {
        let mut connection = self.db.lock();
        let existing: BTreeMap<String, TicketRecord> = Self::tickets_on(&connection)?
            .into_iter()
            .map(|ticket| (ticket.id.clone(), ticket))
            .collect();
        let desired_ticket_ids: BTreeSet<&str> =
            tickets.iter().map(|ticket| ticket.id.as_str()).collect();
        let desired_project_ids: BTreeSet<&str> = project_ids.iter().map(String::as_str).collect();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let stale_tickets = {
            let mut statement = transaction.prepare("SELECT id FROM tickets ORDER BY id")?;
            statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .filter(|id| !desired_ticket_ids.contains(id.as_str()))
                .collect::<Vec<_>>()
        };
        let stale_projects = {
            let mut statement = transaction
                .prepare("SELECT id FROM projects WHERE source = 'local' ORDER BY id")?;
            statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .filter(|id| !desired_project_ids.contains(id.as_str()))
                .collect::<Vec<_>>()
        };

        let mut doomed_activations = BTreeSet::new();
        for ticket_id in &stale_tickets {
            let mut statement =
                transaction.prepare("SELECT id FROM activations WHERE ticket_id = ?1")?;
            doomed_activations.extend(
                statement
                    .query_map(params![ticket_id], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        for project_id in &stale_projects {
            let activations = {
                let mut statement = transaction
                    .prepare("SELECT id, state FROM activations WHERE project_id = ?1")?;
                statement
                    .query_map(params![project_id], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })?
                    .collect::<Result<Vec<_>, _>>()?
            };
            for (activation_id, activation_state) in activations {
                if activation_state == "queued" {
                    doomed_activations.insert(activation_id);
                } else {
                    transaction.execute(
                        "UPDATE activations SET project_id = NULL WHERE id = ?1",
                        params![activation_id],
                    )?;
                }
            }
        }

        let mut rows_dropped = drop_runs(&transaction, &stale_tickets, &doomed_activations)?;
        for activation_id in &doomed_activations {
            rows_dropped += transaction.execute(
                "DELETE FROM activation_filters WHERE activation_id = ?1",
                params![activation_id],
            )?;
            rows_dropped += transaction.execute(
                "DELETE FROM activations WHERE id = ?1",
                params![activation_id],
            )?;
        }
        for ticket_id in &stale_tickets {
            rows_dropped += transaction.execute(
                "DELETE FROM activation_filters WHERE ticket_id = ?1",
                params![ticket_id],
            )?;
            rows_dropped += transaction.execute(
                "DELETE FROM leases WHERE ticket_id = ?1",
                params![ticket_id],
            )?;
            rows_dropped += transaction.execute(
                "DELETE FROM ticket_blockers WHERE ticket_id = ?1 OR blocker_id = ?1",
                params![ticket_id],
            )?;
            rows_dropped +=
                transaction.execute("DELETE FROM tickets WHERE id = ?1", params![ticket_id])?;
        }
        let mut state_changes = Vec::new();
        for ticket in tickets {
            let previous = existing.get(&ticket.id);
            let state = if ticket.held_reason.is_some() {
                TicketState::Held.as_str()
            } else {
                match (previous, ticket.derived_state) {
                    (Some(_), Some(derived)) => derived.as_str(),
                    (Some(existing), None) if existing.held_reason.is_some() => {
                        TicketState::Ready.as_str()
                    }
                    (Some(existing), None) => existing.state.as_str(),
                    (None, Some(derived)) => derived.as_str(),
                    (None, None) => TicketState::Ready.as_str(),
                }
            };
            if let Some(previous) = previous
                && previous.state != state
            {
                state_changes.push(ReindexStateChange {
                    ticket_id: ticket.id.clone(),
                    previous_state: previous.state.clone(),
                    state: state.to_owned(),
                });
                if state == TicketState::Merged.as_str()
                    && matches!(previous.state.as_str(), "failed" | "needs_review")
                {
                    mark_runs_cleanup_eligible(&transaction, &ticket.id, now_ms)?;
                }
            }
            transaction.execute(
                "INSERT INTO tickets
                     (id, project_id, file_path, source, source_ref, state, name, worktree, target,
                      model, effort, flow, body, held_reason, created_at_ms, updated_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?15)
                 ON CONFLICT(id) DO UPDATE SET
                     project_id = excluded.project_id,
                     file_path = excluded.file_path,
                     source = excluded.source,
                     source_ref = excluded.source_ref,
                     state = excluded.state,
                     name = excluded.name,
                     worktree = excluded.worktree,
                     target = excluded.target,
                     model = excluded.model,
                     effort = excluded.effort,
                     flow = excluded.flow,
                     body = excluded.body,
                     held_reason = excluded.held_reason,
                     missing_at_ms = NULL,
                     updated_at_ms = excluded.updated_at_ms",
                params![
                    ticket.id,
                    ticket.project_id,
                    ticket.file_path,
                    ticket.source,
                    ticket.source_ref,
                    state,
                    ticket.name,
                    ticket.worktree,
                    ticket.target,
                    ticket.model,
                    ticket.effort,
                    ticket.flow,
                    ticket.body,
                    ticket.held_reason,
                    now_ms,
                ],
            )?;
        }
        for ticket in tickets {
            tx::replace_ticket_blockers(&transaction, &ticket.id, &ticket.blocked_by)?;
        }
        for project_id in &stale_projects {
            rows_dropped +=
                transaction.execute("DELETE FROM projects WHERE id = ?1", params![project_id])?;
        }

        state_changes.sort_by(|left, right| left.ticket_id.cmp(&right.ticket_id));
        transaction.commit()?;
        Ok(ReindexResult {
            state_changes,
            rows_dropped,
        })
    }

    pub fn update_ticket_execution(
        &self,
        id: &str,
        target: Option<&str>,
        model: Option<&str>,
        effort: Option<&str>,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.db.lock().execute(
            "UPDATE tickets SET target = ?2, model = ?3, effort = ?4, updated_at_ms = ?5 WHERE id = ?1",
            params![id, target, model, effort, now_ms],
        )?;
        Ok(())
    }

    pub fn update_ticket_body(&self, id: &str, body: &str, now_ms: i64) -> Result<(), StoreError> {
        self.db.lock().execute(
            "UPDATE tickets SET body = ?2, updated_at_ms = ?3 WHERE id = ?1",
            params![id, body, now_ms],
        )?;
        Ok(())
    }

    pub fn backfill_ticket_targets(
        &self,
        default_target: &str,
        now_ms: i64,
    ) -> Result<usize, StoreError> {
        self.db
            .lock()
            .execute(
                "UPDATE tickets SET target = ?1, updated_at_ms = ?2 WHERE target IS NULL",
                params![default_target, now_ms],
            )
            .map_err(StoreError::from)
    }

    pub fn ticket(&self, id: &str) -> Result<Option<TicketRecord>, StoreError> {
        let connection = self.db.lock();
        let mut ticket = connection
            .query_row(
                &format!("{TICKET_RECORD_SELECT} WHERE id = ?1"),
                params![id],
                ticket_record,
            )
            .optional()?;
        if let Some(ticket) = ticket.as_mut() {
            ticket.blocked_by = Self::ticket_blockers(&connection, &ticket.id)?;
        }
        Ok(ticket)
    }

    pub fn ticket_by_name(&self, name: &str) -> Result<Option<TicketRecord>, StoreError> {
        let connection = self.db.lock();
        let mut ticket = connection
            .query_row(
                &format!("{TICKET_RECORD_SELECT} WHERE name = ?1 ORDER BY id LIMIT 1"),
                params![name],
                ticket_record,
            )
            .optional()?;
        if let Some(ticket) = ticket.as_mut() {
            ticket.blocked_by = Self::ticket_blockers(&connection, &ticket.id)?;
        }
        Ok(ticket)
    }

    pub fn ticket_by_file(&self, file_path: &str) -> Result<Option<TicketRecord>, StoreError> {
        let connection = self.db.lock();
        let mut ticket = connection
            .query_row(
                &format!("{TICKET_RECORD_SELECT} WHERE file_path = ?1"),
                params![file_path],
                ticket_record,
            )
            .optional()?;
        if let Some(ticket) = ticket.as_mut() {
            ticket.blocked_by = Self::ticket_blockers(&connection, &ticket.id)?;
        }
        Ok(ticket)
    }

    pub fn ticket_by_source_ref(
        &self,
        source: &str,
        source_ref: &str,
    ) -> Result<Option<TicketRecord>, StoreError> {
        let connection = self.db.lock();
        let mut ticket = connection
            .query_row(
                &format!("{TICKET_RECORD_SELECT} WHERE source = ?1 AND source_ref = ?2"),
                params![source, source_ref],
                ticket_record,
            )
            .optional()?;
        if let Some(ticket) = ticket.as_mut() {
            ticket.blocked_by = Self::ticket_blockers(&connection, &ticket.id)?;
        }
        Ok(ticket)
    }

    pub fn tickets(&self) -> Result<Vec<TicketRecord>, StoreError> {
        Self::tickets_on(&self.db.lock())
    }

    fn tickets_on(connection: &Connection) -> Result<Vec<TicketRecord>, StoreError> {
        let mut statement = connection.prepare(&format!(
            "{TICKET_RECORD_SELECT} ORDER BY created_at_ms DESC, id DESC"
        ))?;
        let mut tickets = statement
            .query_map([], ticket_record)?
            .collect::<Result<Vec<_>, _>>()?;
        tickets.sort_by_key(|ticket| {
            (
                std::cmp::Reverse(ticket.created_at_ms),
                std::cmp::Reverse(crate::ids::ordinal(&ticket.id).unwrap_or(0)),
            )
        });
        let mut blockers = Self::all_ticket_blockers(connection)?;
        for ticket in &mut tickets {
            ticket.blocked_by = blockers.remove(&ticket.id).unwrap_or_default();
        }
        Ok(tickets)
    }

    pub fn tickets_for_project(&self, project_id: &str) -> Result<Vec<TicketRecord>, StoreError> {
        let connection = self.db.lock();
        let mut statement = connection.prepare(&format!(
            "{TICKET_RECORD_SELECT} WHERE project_id = ?1 ORDER BY id"
        ))?;
        let mut tickets = statement
            .query_map(params![project_id], ticket_record)?
            .collect::<Result<Vec<_>, _>>()?;
        let mut blockers = Self::all_ticket_blockers(&connection)?;
        for ticket in &mut tickets {
            ticket.blocked_by = blockers.remove(&ticket.id).unwrap_or_default();
        }
        Ok(tickets)
    }

    pub fn ticket_dependencies(&self) -> Result<BTreeMap<String, Vec<String>>, StoreError> {
        let mut dependencies = BTreeMap::new();
        let connection = self.db.lock();
        let mut statement = connection.prepare("SELECT id FROM tickets")?;
        let ids = statement.query_map([], |row| row.get::<_, String>(0))?;
        for id in ids {
            dependencies.insert(id?, Vec::new());
        }
        for (ticket_id, blockers) in Self::all_ticket_blockers(&connection)? {
            if let Some(entry) = dependencies.get_mut(&ticket_id) {
                *entry = blockers;
            }
        }
        Ok(dependencies)
    }

    pub fn ticket_is_dispatchable(&self, ticket_id: &str) -> Result<bool, StoreError> {
        self.db
            .lock()
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM tickets t
                     WHERE t.id = ?1
                       AND t.state = 'ready'
                       AND t.missing_at_ms IS NULL
                       AND NOT EXISTS (SELECT 1 FROM ticket_blockers b
                                       JOIN tickets bt ON bt.id = b.blocker_id
                                       WHERE b.ticket_id = t.id
                                         AND bt.state != 'merged')
                 )",
                params![ticket_id],
                |row| row.get(0),
            )
            .map_err(StoreError::from)
    }

    pub fn unmerged_blockers(&self, ticket_id: &str) -> Result<Vec<String>, StoreError> {
        let connection = self.db.lock();
        let mut statement = connection.prepare(
            "SELECT b.blocker_id FROM ticket_blockers b
             JOIN tickets bt ON bt.id = b.blocker_id
             WHERE b.ticket_id = ?1 AND bt.state != 'merged'
             ORDER BY b.position, b.blocker_id",
        )?;
        statement
            .query_map(params![ticket_id], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    fn all_ticket_blockers(
        connection: &Connection,
    ) -> Result<BTreeMap<String, Vec<String>>, StoreError> {
        let mut statement = connection.prepare(
            "SELECT ticket_id, blocker_id FROM ticket_blockers
             ORDER BY ticket_id, position, blocker_id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut blockers: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for row in rows {
            let (ticket_id, blocker_id) = row?;
            blockers.entry(ticket_id).or_default().push(blocker_id);
        }
        Ok(blockers)
    }

    fn ticket_blockers(connection: &Connection, id: &str) -> Result<Vec<String>, StoreError> {
        let mut statement = connection.prepare(
            "SELECT blocker_id FROM ticket_blockers
             WHERE ticket_id = ?1 ORDER BY position, blocker_id",
        )?;
        statement
            .query_map(params![id], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    pub fn ticket_ids(&self) -> Result<Vec<String>, StoreError> {
        let connection = self.db.lock();
        let mut statement = connection.prepare("SELECT id FROM tickets")?;
        let rows = statement.query_map([], |row| row.get(0))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    pub fn local_ticket_files(&self) -> Result<Vec<LocalTicketFile>, StoreError> {
        let connection = self.db.lock();
        let mut statement = connection.prepare(
            "SELECT id, file_path, state, missing_at_ms FROM tickets
             WHERE source = 'local' AND file_path IS NOT NULL
             ORDER BY id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(LocalTicketFile {
                id: row.get(0)?,
                file_path: row.get(1)?,
                state: row.get(2)?,
                missing_at_ms: row.get(3)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    pub(crate) fn ticket_has_work_references_on(
        connection: &Connection,
        id: &str,
    ) -> rusqlite::Result<bool> {
        connection.query_row(
            "SELECT EXISTS (SELECT 1 FROM leases WHERE ticket_id = ?1)
                 OR EXISTS (SELECT 1 FROM activations WHERE ticket_id = ?1)
                 OR EXISTS (SELECT 1 FROM activation_filters WHERE ticket_id = ?1)
                 OR EXISTS (SELECT 1 FROM ticket_blockers WHERE blocker_id = ?1)",
            params![id],
            |row| row.get(0),
        )
    }

    pub fn delete_ticket(&self, id: &str) -> Result<(), StoreError> {
        self.db
            .lock()
            .execute("DELETE FROM tickets WHERE id = ?1", params![id])?;
        Ok(())
    }

    pub fn mark_ticket_missing(&self, id: &str, now_ms: i64) -> Result<(), StoreError> {
        self.db.lock().execute(
            "UPDATE tickets SET missing_at_ms = ?2, updated_at_ms = ?2
             WHERE id = ?1 AND missing_at_ms IS NULL",
            params![id, now_ms],
        )?;
        Ok(())
    }

    pub fn clear_ticket_missing(&self, id: &str, now_ms: i64) -> Result<(), StoreError> {
        self.db.lock().execute(
            "UPDATE tickets SET missing_at_ms = NULL, updated_at_ms = ?2
             WHERE id = ?1 AND missing_at_ms IS NOT NULL",
            params![id, now_ms],
        )?;
        Ok(())
    }

    pub fn ticket_state(&self, id: &str) -> Result<Option<String>, StoreError> {
        Self::ticket_state_on(&self.db.lock(), id)
    }

    pub(crate) fn ticket_state_on(
        connection: &Connection,
        id: &str,
    ) -> Result<Option<String>, StoreError> {
        connection
            .query_row(
                "SELECT state FROM tickets WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()
            .map_err(StoreError::from)
    }

    pub fn set_ticket_hold(
        &self,
        id: &str,
        state: TicketState,
        now_ms: i64,
    ) -> Result<String, StoreError> {
        debug_assert!(matches!(state, TicketState::Ready | TicketState::Held));
        let connection = self.db.lock();
        let requested = state.as_str();
        let previous =
            Self::ticket_state_on(&connection, id)?.ok_or_else(|| StoreError::TicketNotFound {
                ticket_id: id.into(),
            })?;
        if previous == requested {
            return Ok(previous);
        }
        let allowed_previous = match state {
            TicketState::Ready => TicketState::Held.as_str(),
            TicketState::Held => TicketState::Ready.as_str(),
            _ => unreachable!("hold transitions only use ready and held"),
        };
        let changed = connection.execute(
            "UPDATE tickets SET state = ?2, held_reason = NULL, updated_at_ms = ?3
             WHERE id = ?1 AND state = ?4",
            params![id, requested, now_ms, allowed_previous],
        )?;
        if changed != 1 {
            return Err(StoreError::TicketStateConflict {
                ticket_id: id.into(),
                state: previous,
                requested: requested.into(),
            });
        }
        Ok(previous)
    }

    pub(crate) fn retry_ticket<Cleanup>(
        &self,
        id: &str,
        now_ms: i64,
        cleanup: Cleanup,
    ) -> Result<String, StoreError>
    where
        Cleanup: FnOnce(&Transaction<'_>, &str, i64) -> Result<(), StoreError>,
    {
        let mut connection = self.db.lock();
        let previous =
            Self::ticket_state_on(&connection, id)?.ok_or_else(|| StoreError::TicketNotFound {
                ticket_id: id.into(),
            })?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx::retry_ticket(&transaction, id, now_ms)?;
        if changed != 1 {
            return Err(StoreError::TicketStateConflict {
                ticket_id: id.into(),
                state: previous,
                requested: TicketState::Ready.as_str().into(),
            });
        }
        cleanup(&transaction, id, now_ms)?;
        transaction.commit()?;
        Ok(previous)
    }

    pub fn ticket_counts(&self) -> Result<TicketCounts, StoreError> {
        let connection = self.db.lock();
        let mut statement = connection.prepare(
            "SELECT CASE
                      WHEN t.state = 'ready' AND EXISTS (
                          SELECT 1 FROM ticket_blockers b
                          JOIN tickets bt ON bt.id = b.blocker_id
                          WHERE b.ticket_id = t.id AND bt.state != 'merged'
                      ) THEN 'blocked'
                      ELSE t.state
                    END AS display_state,
                    COUNT(*)
             FROM tickets t
             GROUP BY display_state",
        )?;
        let mut rows = statement.query([])?;
        let mut counts = TicketCounts::default();
        while let Some(row) = rows.next()? {
            let state: String = row.get(0)?;
            let count = row.get::<_, i64>(1)?.max(0) as u64;
            match state.as_str() {
                "ready" => counts.ready = count,
                "held" => counts.held = count,
                "blocked" => counts.blocked = count,
                "claimed" => counts.claimed = count,
                "merged" => counts.merged = count,
                "failed" => counts.failed = count,
                "needs_review" => counts.needs_review = count,
                _ => {}
            }
        }
        Ok(counts)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::coordination::{Claim, ClaimDenial, Coordination};
    use crate::domain::ticket::TicketState;
    use crate::outcome::Outcome;
    use crate::store::{
        ActivationKind, ClaimRequest, ClaimedRun, NewActivation, QueuedActivation, ReindexTicket,
        Store, StoreError,
    };

    fn open_seeded(path: &std::path::Path) -> Store {
        let store = Store::open(path, 1_000).unwrap();
        store
            .insert_local_project(
                "default",
                ".agents/sloop/projects/default.md",
                "Default",
                1_000,
            )
            .unwrap();
        store
            .insert_local_ticket(
                "T1",
                "default",
                ".agents/sloop/tickets/t1.md",
                "Ticket one",
                &[],
                "sloop/T1",
                Some("claude"),
                Some("sonnet"),
                Some("medium"),
                "default",
                TicketState::Ready,
                1_000,
            )
            .unwrap();
        store
            .insert_activation(
                &NewActivation {
                    id: "A1",
                    kind: ActivationKind::Immediate,
                    ticket_id: Some("T1"),
                    project_id: None,
                    eligible_at_ms: None,
                    interval_ms: None,
                },
                1_000,
            )
            .unwrap();
        store
    }

    fn claim_t1<'a>(run_id: &'a str) -> ClaimRequest<'a> {
        ClaimRequest {
            ticket_id: "T1",
            run_id,
            activation_id: "A1",
            owner_id: "daemon-1",
            lease_ms: 60_000,
            next_activation_eligible_at_ms: None,
            flow_json: "{}",
            ticket_json: "{}",
        }
    }

    fn granted_claim(store: &Store, claim: &ClaimRequest<'_>, now_ms: i64) -> ClaimedRun {
        match Coordination::from_shared(store)
            .claim(claim, now_ms)
            .unwrap()
        {
            Claim::Granted(claimed) => claimed,
            Claim::Denied(denial) => panic!("claim denied: {denial:?}"),
        }
    }

    #[test]
    fn missing_tickets_are_not_selected_and_cannot_be_claimed() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));
        store.mark_ticket_missing("T1", 2_000).unwrap();
        let activation = QueuedActivation {
            id: "A1".into(),
            kind: "immediate".into(),
            ticket_id: None,
            project_id: None,
            eligible_at_ms: None,
            interval_ms: None,
        };
        assert_eq!(store.select_ready_ticket(&activation, 2_000).unwrap(), None);
        assert_eq!(
            Coordination::from_shared(&store)
                .claim(&claim_t1("R1"), 2_000)
                .unwrap(),
            Claim::Denied(ClaimDenial::NotReady)
        );

        store.mark_ticket_missing("T1", 5_000).unwrap();
        assert_eq!(
            store.local_ticket_files().unwrap()[0].missing_at_ms,
            Some(2_000)
        );
        store.clear_ticket_missing("T1", 6_000).unwrap();
        assert_eq!(
            store
                .select_ready_ticket(&activation, 6_000)
                .unwrap()
                .as_deref(),
            Some("T1")
        );
    }

    #[test]
    fn blockers_gate_selection_claims_and_derived_counts_until_merged() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));
        store
            .insert_local_ticket(
                "T2",
                "default",
                ".agents/sloop/tickets/t2.md",
                "Ticket two",
                &["T1".into()],
                "sloop/T2",
                Some("claude"),
                None,
                None,
                "default",
                TicketState::Ready,
                1_500,
            )
            .unwrap();
        let activation = QueuedActivation {
            id: "A1".into(),
            kind: "immediate".into(),
            ticket_id: None,
            project_id: None,
            eligible_at_ms: None,
            interval_ms: None,
        };

        assert_eq!(store.unmerged_blockers("T2").unwrap(), ["T1"]);
        assert_eq!(
            store
                .select_ready_ticket(&activation, 2_000)
                .unwrap()
                .as_deref(),
            Some("T1")
        );
        assert_eq!(store.ticket_counts().unwrap().blocked, 1);

        store
            .db()
            .lock()
            .execute("UPDATE tickets SET state = 'failed' WHERE id = 'T1'", [])
            .unwrap();
        assert_eq!(store.select_ready_ticket(&activation, 2_000).unwrap(), None);
        assert_eq!(
            Coordination::from_shared(&store)
                .claim(
                    &ClaimRequest {
                        ticket_id: "T2",
                        run_id: "R2",
                        activation_id: "A1",
                        owner_id: "daemon-1",
                        lease_ms: 60_000,
                        next_activation_eligible_at_ms: None,
                        flow_json: "{}",
                        ticket_json: "{}",
                    },
                    2_000,
                )
                .unwrap(),
            Claim::Denied(ClaimDenial::NotReady)
        );
        assert_eq!(store.ticket("T2").unwrap().unwrap().attempts, 0);

        store
            .db()
            .lock()
            .execute("UPDATE tickets SET state = 'merged' WHERE id = 'T1'", [])
            .unwrap();
        assert!(store.unmerged_blockers("T2").unwrap().is_empty());
        assert_eq!(
            store
                .select_ready_ticket(&activation, 2_000)
                .unwrap()
                .as_deref(),
            Some("T2")
        );
        let counts = store.ticket_counts().unwrap();
        assert_eq!(counts.ready, 1);
        assert_eq!(counts.blocked, 0);
    }

    #[test]
    fn state_survives_reopening_the_database() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sloop.db");
        let store = open_seeded(&path);
        granted_claim(&store, &claim_t1("R1"), 2_000);
        drop(store);

        let store = Store::open(&path, 3_000).unwrap();
        assert_eq!(
            store.ticket_state("T1").unwrap().as_deref(),
            Some("claimed")
        );
        assert_eq!(store.ticket_counts().unwrap().claimed, 1);
        let ticket = store.ticket("T1").unwrap().unwrap();
        assert_eq!(ticket.target.as_deref(), Some("claude"));
        assert_eq!(ticket.model.as_deref(), Some("sonnet"));
        assert_eq!(ticket.effort.as_deref(), Some("medium"));
        assert_eq!(ticket.name, "Ticket one");
        assert!(ticket.blocked_by.is_empty());
        assert_eq!(ticket.worktree.as_deref(), Some("sloop/T1"));
    }

    #[test]
    fn blocked_by_and_worktree_round_trip() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sloop.db");
        let store = open_seeded(&path);
        store
            .insert_local_ticket(
                "T2",
                "default",
                ".agents/sloop/tickets/t2.md",
                "Ticket two",
                &["T1".to_owned()],
                "feature/t2",
                None,
                None,
                None,
                "default",
                TicketState::Ready,
                2_000,
            )
            .unwrap();
        drop(store);

        let ticket = Store::open(&path, 3_000)
            .unwrap()
            .ticket("T2")
            .unwrap()
            .unwrap();
        assert_eq!(ticket.name, "Ticket two");
        assert_eq!(ticket.blocked_by, ["T1"]);
        assert_eq!(ticket.worktree.as_deref(), Some("feature/t2"));
    }

    #[test]
    fn tickets_are_ordered_newest_first_and_include_attempts() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));
        store
            .insert_local_project("alpha", ".agents/sloop/projects/alpha.md", "Alpha", 1_000)
            .unwrap();
        for (id, project, state, created_at_ms) in [
            ("T0", "alpha", TicketState::Held, 3_000),
            ("T2", "default", TicketState::Ready, 1_000),
        ] {
            store
                .insert_local_ticket(
                    id,
                    project,
                    &format!(".agents/sloop/tickets/{}.md", id.to_lowercase()),
                    id,
                    &[],
                    &format!("sloop/{id}"),
                    None,
                    None,
                    None,
                    "default",
                    state,
                    created_at_ms,
                )
                .unwrap();
        }
        granted_claim(&store, &claim_t1("R1"), 2_000);

        let tickets = store.tickets().unwrap();
        assert_eq!(
            tickets
                .iter()
                .map(|ticket| ticket.id.as_str())
                .collect::<Vec<_>>(),
            ["T0", "T2", "T1"]
        );
        assert_eq!(tickets[2].attempts, 1);
    }

    #[test]
    fn operator_hold_transitions_are_narrow_and_idempotent() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));
        assert_eq!(
            store
                .set_ticket_hold("T1", TicketState::Held, 2_000)
                .unwrap(),
            "ready"
        );
        assert_eq!(store.ticket_counts().unwrap().held, 1);
        assert_eq!(
            store
                .set_ticket_hold("T1", TicketState::Held, 2_100)
                .unwrap(),
            "held"
        );
        assert_eq!(
            store
                .set_ticket_hold("T1", TicketState::Ready, 2_200)
                .unwrap(),
            "held"
        );
    }

    #[test]
    fn validation_hold_reasons_set_and_clear_without_releasing_operator_holds() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));
        let ticket = |held_reason: Option<&str>| ReindexTicket {
            id: "T1".into(),
            project_id: "default".into(),
            source: "markdown".into(),
            source_ref: ".agents/sloop/tickets/t1.md".into(),
            file_path: Some(".agents/sloop/tickets/t1.md".into()),
            name: "Ticket one".into(),
            blocked_by: Vec::new(),
            worktree: "sloop/T1".into(),
            target: Some("claude".into()),
            model: Some("sonnet".into()),
            effort: Some("medium".into()),
            flow: "default".into(),
            body: "work".into(),
            held_reason: held_reason.map(str::to_owned),
            derived_state: None,
        };

        store
            .apply_reindex(
                &["default".into()],
                &[ticket(Some("flow `missing` is not defined"))],
                2_000,
            )
            .unwrap();
        assert_eq!(
            store.ticket("T1").unwrap().unwrap().held_reason.as_deref(),
            Some("flow `missing` is not defined")
        );
        store
            .apply_reindex(&["default".into()], &[ticket(None)], 2_100)
            .unwrap();
        assert_eq!(store.ticket_state("T1").unwrap().as_deref(), Some("ready"));

        store
            .set_ticket_hold("T1", TicketState::Held, 2_200)
            .unwrap();
        store
            .apply_reindex(&["default".into()], &[ticket(None)], 2_300)
            .unwrap();
        let operator_held = store.ticket("T1").unwrap().unwrap();
        assert_eq!(operator_held.state, "held");
        assert_eq!(operator_held.held_reason, None);
    }

    #[test]
    fn operator_hold_cannot_steal_a_claim() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));
        granted_claim(&store, &claim_t1("R1"), 2_000);
        assert!(matches!(
            store.set_ticket_hold("T1", TicketState::Held, 2_100),
            Err(StoreError::TicketStateConflict { state, .. }) if state == "claimed"
        ));
    }

    #[test]
    fn retry_only_requeues_failed_tickets_and_resets_attempts() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));
        assert_eq!(granted_claim(&store, &claim_t1("R1"), 2_000).attempt, 1);
        Coordination::from_shared(&store)
            .settle("R1", "T1", Some(0), Outcome::Failed, &[], None, 2_100)
            .unwrap();

        assert_eq!(store.retry_ticket("T1", 2_200).unwrap(), "failed");
        store
            .insert_activation(
                &NewActivation {
                    id: "A2",
                    kind: ActivationKind::Immediate,
                    ticket_id: Some("T1"),
                    project_id: None,
                    eligible_at_ms: None,
                    interval_ms: None,
                },
                2_300,
            )
            .unwrap();
        assert_eq!(
            granted_claim(
                &store,
                &ClaimRequest {
                    activation_id: "A2",
                    ..claim_t1("R2")
                },
                2_300,
            )
            .attempt,
            2
        );
        assert_eq!(store.ticket("T1").unwrap().unwrap().attempts, 1);
        assert!(matches!(
            store.retry_ticket("T1", 2_400),
            Err(StoreError::TicketStateConflict { state, .. }) if state == "claimed"
        ));
        assert!(matches!(
            store.retry_ticket("missing", 2_400),
            Err(StoreError::TicketNotFound { .. })
        ));
    }

    #[test]
    fn configured_default_backfills_tickets_that_predate_target_snapshots() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));
        store
            .update_ticket_execution("T1", None, Some("sonnet"), Some("medium"), 2_000)
            .unwrap();
        assert_eq!(store.backfill_ticket_targets("codex", 3_000).unwrap(), 1);
        assert_eq!(
            store.ticket("T1").unwrap().unwrap().target.as_deref(),
            Some("codex")
        );
        assert_eq!(store.backfill_ticket_targets("claude", 4_000).unwrap(), 0);
    }
}
