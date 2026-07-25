use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};

use crate::clock::{Clock, SystemClock};
use crate::config::{AgentConfig, expand_agent_cmd};
use crate::db::{Db, StoreError};
use crate::domain::ticket::TicketState;
use crate::domain::work::{
    Disposition, ExecutionHints, OwnerId, SourceVersion, TicketRef, WorkOutcome, WorkTicket,
    WorkTicketState,
};
use crate::flow::Flow;
use crate::frontmatter;
use crate::ids::next_id;
use crate::reindex::ReindexError;
use crate::work_state::exec::ExecTicketSource;
use crate::work_state::{
    ActiveClaim, ClaimResult, ClaimStrength, SourceError, TicketFeeder, WorkState,
};

const TICKET_RECORD_SELECT: &str =
    "SELECT id, project_id, file_path, source, source_ref, state, name, worktree,
            target, model, effort, flow, attempts, body, held_reason, created_at_ms, updated_at_ms
     FROM tickets";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivationKind {
    Immediate,
    Auto,
    At,
    Every,
    Overnight,
}

impl ActivationKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Immediate => "immediate",
            Self::Auto => "auto",
            Self::At => "at",
            Self::Every => "every",
            Self::Overnight => "overnight",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivationState {
    Queued,
    Completed,
    Cancelled,
}

impl ActivationState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewActivation<'a> {
    pub id: &'a str,
    pub kind: ActivationKind,
    pub ticket_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub eligible_at_ms: Option<i64>,
    pub interval_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClaimTransaction<'a> {
    pub(crate) ticket_id: &'a str,
    pub(crate) run_id: &'a str,
    pub(crate) activation_id: &'a str,
    pub(crate) owner_id: &'a str,
    pub(crate) lease_ms: i64,
    pub(crate) next_activation_eligible_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedActivation {
    pub id: String,
    pub kind: String,
    pub ticket_id: Option<String>,
    pub project_id: Option<String>,
    pub eligible_at_ms: Option<i64>,
    pub interval_ms: Option<i64>,
}

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
    pub updated_at_ms: i64,
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

pub(crate) struct LocalTicketWrite<'a> {
    pub id: &'a str,
    pub project_id: &'a str,
    pub file_path: &'a str,
    pub name: &'a str,
    pub blocked_by: &'a [String],
    pub worktree: &'a str,
    pub target: Option<&'a str>,
    pub model: Option<&'a str>,
    pub effort: Option<&'a str>,
    pub flow: &'a str,
    pub state: TicketState,
    pub body: &'a str,
    pub content_hash: &'a str,
    pub now_ms: i64,
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
        updated_at_ms: row.get(16)?,
    })
}

pub(crate) mod tx {
    use super::*;

    pub(crate) fn insert_authored_ticket(
        transaction: &Transaction<'_>,
        ticket: &LocalTicketWrite<'_>,
    ) -> Result<(), StoreError> {
        transaction.execute(
            "INSERT INTO tickets
                 (id, project_id, file_path, source, state, name, worktree, target, model, effort,
                    flow, body, content_hash, created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, 'local', ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?13)",
            params![
                ticket.id,
                ticket.project_id,
                ticket.file_path,
                ticket.state.as_str(),
                ticket.name,
                ticket.worktree,
                ticket.target,
                ticket.model,
                ticket.effort,
                ticket.flow,
                ticket.body,
                ticket.content_hash,
                ticket.now_ms,
            ],
        )?;
        replace_ticket_blockers(transaction, ticket.id, ticket.blocked_by)?;
        Ok(())
    }

    pub(crate) fn update_authored_ticket(
        transaction: &Transaction<'_>,
        ticket: &LocalTicketWrite<'_>,
    ) -> Result<(), StoreError> {
        transaction.execute(
            "UPDATE tickets
             SET name = ?2, worktree = ?3, target = ?4, model = ?5, effort = ?6, flow = ?7,
                 body = ?8, content_hash = ?9, held_reason = NULL, missing_at_ms = NULL,
                 updated_at_ms = ?10
             WHERE id = ?1",
            params![
                ticket.id,
                ticket.name,
                ticket.worktree,
                ticket.target,
                ticket.model,
                ticket.effort,
                ticket.flow,
                ticket.body,
                ticket.content_hash,
                ticket.now_ms,
            ],
        )?;
        replace_ticket_blockers(transaction, ticket.id, ticket.blocked_by)?;
        Ok(())
    }

    pub(crate) fn queued_ticket_activation(
        transaction: &Transaction<'_>,
        ticket_id: &str,
        kind: ActivationKind,
    ) -> rusqlite::Result<Option<String>> {
        transaction
            .query_row(
                "SELECT id FROM activations
                 WHERE ticket_id = ?1 AND kind = ?2 AND state = 'queued'
                 ORDER BY created_at_ms LIMIT 1",
                params![ticket_id, kind.as_str()],
                |row| row.get(0),
            )
            .optional()
    }

    pub(crate) fn reschedule_activation(
        transaction: &Transaction<'_>,
        id: &str,
        eligible_at_ms: i64,
        now_ms: i64,
    ) -> rusqlite::Result<()> {
        transaction.execute(
            "UPDATE activations
             SET eligible_at_ms = ?2, updated_at_ms = ?3
             WHERE id = ?1 AND state = 'queued'",
            params![id, eligible_at_ms, now_ms],
        )?;
        Ok(())
    }

    pub(crate) fn insert_activation(
        transaction: &Transaction<'_>,
        activation: &NewActivation<'_>,
        now_ms: i64,
    ) -> rusqlite::Result<()> {
        transaction.execute(
            "INSERT INTO activations
                 (id, kind, state, ticket_id, project_id, eligible_at_ms, interval_ms,
                  created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
            params![
                activation.id,
                activation.kind.as_str(),
                ActivationState::Queued.as_str(),
                activation.ticket_id,
                activation.project_id,
                activation.eligible_at_ms,
                activation.interval_ms,
                now_ms,
            ],
        )?;
        Ok(())
    }

    pub(crate) fn advance_activation(
        transaction: &Transaction<'_>,
        claim: &ClaimTransaction<'_>,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        let activation_changed = match claim.next_activation_eligible_at_ms {
            Some(eligible_at_ms) => transaction.execute(
                "UPDATE activations
                 SET eligible_at_ms = ?2, updated_at_ms = ?3
                 WHERE id = ?1 AND state = 'queued' AND kind = 'every'",
                params![claim.activation_id, eligible_at_ms, now_ms],
            )?,
            None => transaction.execute(
                "UPDATE activations SET state = 'completed', updated_at_ms = ?2
                 WHERE id = ?1 AND state = 'queued' AND kind != 'every'",
                params![claim.activation_id, now_ms],
            )?,
        };
        if activation_changed != 1 {
            return Err(StoreError::ActivationNotQueued {
                activation_id: claim.activation_id.into(),
            });
        }
        Ok(())
    }

    pub(crate) fn insert_lease(
        transaction: &Transaction<'_>,
        claim: &ClaimTransaction<'_>,
        now_ms: i64,
    ) -> Result<i64, StoreError> {
        let expires_at_ms = now_ms + claim.lease_ms;
        transaction.execute(
            "INSERT INTO leases
                 (ticket_id, run_id, owner_id, acquired_at_ms, renewed_at_ms, expires_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?4, ?5)",
            params![
                claim.ticket_id,
                claim.run_id,
                claim.owner_id,
                now_ms,
                expires_at_ms,
            ],
        )?;
        Ok(expires_at_ms)
    }

    pub(crate) fn delete_lease(
        transaction: &Transaction<'_>,
        run_id: &str,
    ) -> rusqlite::Result<usize> {
        transaction.execute("DELETE FROM leases WHERE run_id = ?1", params![run_id])
    }

    pub(crate) fn requeue_activation(
        transaction: &Transaction<'_>,
        activation_id: &str,
        now_ms: i64,
    ) -> rusqlite::Result<usize> {
        transaction.execute(
            "UPDATE activations SET state = 'queued', updated_at_ms = ?2 WHERE id = ?1",
            params![activation_id, now_ms],
        )
    }

    /// Retires the activations pinned to a ticket that has just settled to
    /// `merged`. A pinned activation resolves through `ticket_is_dispatchable`,
    /// which demands `state = 'ready'`, and a merged ticket never returns
    /// there: leaving it queued is demand that can never be met but is still
    /// counted. Running this in the settle transaction means the activation
    /// dies at the instant the ticket merges, with no window where the two
    /// disagree.
    ///
    /// Kind is deliberately not consulted. An `every` activation pinned to a
    /// merged ticket is as unfireable as a `once`; its rearm arithmetic in
    /// `advance_activation` is untouched and simply has nothing left to rearm.
    /// An unpinned activation is demand for whatever is ready, so it is out of
    /// scope by construction — the `ticket_id` match excludes it.
    pub(crate) fn complete_ticket_activations(
        transaction: &Transaction<'_>,
        ticket_id: &str,
        now_ms: i64,
    ) -> rusqlite::Result<usize> {
        transaction.execute(
            "UPDATE activations SET state = 'completed', updated_at_ms = ?2
             WHERE ticket_id = ?1 AND state = 'queued'",
            params![ticket_id, now_ms],
        )
    }

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
        claim: &ClaimTransaction<'_>,
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
    clock: Arc<dyn Clock>,
    last_sync_ms: AtomicI64,
    outcome_reporter: Option<ExecTicketSource>,
}

impl LocalSqlite {
    pub fn from_db(db: Db) -> Self {
        Self::from_db_with_clock(db, Arc::new(SystemClock))
    }

    pub fn from_db_with_clock(db: Db, clock: Arc<dyn Clock>) -> Self {
        Self::from_db_with_clock_and_reporter(db, clock, None)
    }

    pub(crate) fn from_db_with_clock_and_reporter(
        db: Db,
        clock: Arc<dyn Clock>,
        outcome_reporter: Option<ExecTicketSource>,
    ) -> Self {
        Self {
            db,
            clock,
            last_sync_ms: AtomicI64::new(i64::MIN),
            outcome_reporter,
        }
    }

    pub(crate) fn db(&self) -> Db {
        self.db.clone()
    }

    pub fn last_sync_ms(&self) -> Option<i64> {
        match self.last_sync_ms.load(Ordering::Acquire) {
            i64::MIN => None,
            timestamp => Some(timestamp),
        }
    }

    pub fn insert_activation(
        &self,
        activation: &NewActivation<'_>,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.db.lock().execute(
            "INSERT INTO activations
                 (id, kind, state, ticket_id, project_id, eligible_at_ms, interval_ms,
                  created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
            params![
                activation.id,
                activation.kind.as_str(),
                ActivationState::Queued.as_str(),
                activation.ticket_id,
                activation.project_id,
                activation.eligible_at_ms,
                activation.interval_ms,
                now_ms,
            ],
        )?;
        Ok(())
    }

    pub fn insert_activation_filter(
        &self,
        activation_id: &str,
        ticket_id: &str,
    ) -> Result<(), StoreError> {
        self.db.lock().execute(
            "INSERT OR IGNORE INTO activation_filters (activation_id, ticket_id) VALUES (?1, ?2)",
            params![activation_id, ticket_id],
        )?;
        Ok(())
    }

    pub fn queued_activations(&self) -> Result<Vec<QueuedActivation>, StoreError> {
        let connection = self.db.lock();
        let mut statement = connection.prepare(
            "SELECT id, kind, ticket_id, project_id, eligible_at_ms, interval_ms
             FROM activations WHERE state = 'queued'
             ORDER BY created_at_ms, id",
        )?;
        let activations = statement
            .query_map([], |row| {
                Ok(QueuedActivation {
                    id: row.get(0)?,
                    kind: row.get(1)?,
                    ticket_id: row.get(2)?,
                    project_id: row.get(3)?,
                    eligible_at_ms: row.get(4)?,
                    interval_ms: row.get(5)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(activations)
    }

    pub fn dispatchable_activations(
        &self,
        now_ms: i64,
    ) -> Result<Vec<QueuedActivation>, StoreError> {
        let connection = self.db.lock();
        let mut statement = connection.prepare(
            "SELECT id, kind, ticket_id, project_id, eligible_at_ms, interval_ms
             FROM activations
             WHERE state = 'queued'
               AND (kind IN ('immediate', 'auto') OR eligible_at_ms <= ?1)
             ORDER BY created_at_ms, id",
        )?;
        let activations = statement
            .query_map(params![now_ms], |row| {
                Ok(QueuedActivation {
                    id: row.get(0)?,
                    kind: row.get(1)?,
                    ticket_id: row.get(2)?,
                    project_id: row.get(3)?,
                    eligible_at_ms: row.get(4)?,
                    interval_ms: row.get(5)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(activations)
    }

    /// Retires activations left queued against a ticket that merged before the
    /// settle path knew to retire them. The rule in `tx::complete_ticket_activations`
    /// only applies from the next settlement onwards, and a merged ticket never
    /// settles again, so anything already stranded needs this one-off sweep.
    ///
    /// Returns the `(activation_id, ticket_id)` pairs it completed, so the
    /// caller can report a startup mutation rather than perform it silently. A
    /// database with nothing stranded selects no rows and writes nothing, which
    /// makes repeated runs free.
    pub fn complete_merged_ticket_activations(
        &self,
        now_ms: i64,
    ) -> Result<Vec<(String, String)>, StoreError> {
        let mut connection = self.db.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let stranded = {
            let mut statement = transaction.prepare(
                "SELECT a.id, a.ticket_id
                 FROM activations a
                 JOIN tickets t ON t.id = a.ticket_id
                 WHERE a.state = 'queued' AND t.state = 'merged'
                 ORDER BY a.created_at_ms, a.id",
            )?;
            statement
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<Result<Vec<(String, String)>, _>>()?
        };
        if stranded.is_empty() {
            return Ok(Vec::new());
        }
        transaction.execute(
            "UPDATE activations SET state = 'completed', updated_at_ms = ?1
             WHERE state = 'queued'
               AND ticket_id IN (SELECT id FROM tickets WHERE state = 'merged')",
            params![now_ms],
        )?;
        transaction.commit()?;
        Ok(stranded)
    }

    pub fn next_activation_eligible_at_ms(&self, now_ms: i64) -> Result<Option<i64>, StoreError> {
        self.db
            .lock()
            .query_row(
                "SELECT MIN(eligible_at_ms) FROM activations
                 WHERE state = 'queued' AND eligible_at_ms > ?1",
                params![now_ms],
                |row| row.get(0),
            )
            .map_err(StoreError::from)
    }

    pub fn queued_ticket_activation(
        &self,
        ticket_id: &str,
        kind: ActivationKind,
    ) -> Result<Option<String>, StoreError> {
        self.db
            .lock()
            .query_row(
                "SELECT id FROM activations
                 WHERE ticket_id = ?1 AND kind = ?2 AND state = 'queued'
                 ORDER BY created_at_ms LIMIT 1",
                params![ticket_id, kind.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(StoreError::from)
    }

    pub fn reschedule_activation(
        &self,
        id: &str,
        eligible_at_ms: i64,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.db.lock().execute(
            "UPDATE activations
             SET eligible_at_ms = ?2, updated_at_ms = ?3
             WHERE id = ?1 AND state = 'queued'",
            params![id, eligible_at_ms, now_ms],
        )?;
        Ok(())
    }

    pub fn next_activation_ordinal(&self) -> Result<i64, StoreError> {
        let mut connection = self.db.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let reserved: i64 = transaction.query_row(
            "SELECT next_ordinal FROM id_counters WHERE kind = 'activation'",
            [],
            |row| row.get(0),
        )?;
        let existing: i64 = transaction.query_row(
            "SELECT COALESCE(MAX(CAST(SUBSTR(id, 2) AS INTEGER)), 0) + 1 FROM activations",
            [],
            |row| row.get(0),
        )?;
        let ordinal = reserved.max(existing);
        transaction.execute(
            "UPDATE id_counters SET next_ordinal = ?1 WHERE kind = 'activation'",
            params![ordinal + 1],
        )?;
        transaction.commit()?;
        Ok(ordinal)
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

    /// Pulls and applies a complete authored ticket snapshot. Invalid authored
    /// tickets remain visible on hold; only source-wide identity conflicts and
    /// dependency cycles reject the pull.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sync_from_source<DropRuns, MarkRuns>(
        &self,
        root: &Path,
        ticket_source: &TicketFeeder,
        worktree_dir: &Path,
        now_ms: i64,
        ticket_prefix: &str,
        project_ids: &[String],
        agent: Option<&AgentConfig>,
        flows: &BTreeMap<String, Flow>,
        default_flow: &str,
        drop_runs: DropRuns,
        mark_runs_cleanup_eligible: MarkRuns,
    ) -> Result<Value, ReindexError>
    where
        DropRuns:
            FnMut(&Transaction<'_>, &[String], &BTreeSet<String>) -> Result<usize, StoreError>,
        MarkRuns: FnMut(&Transaction<'_>, &str, i64) -> Result<(), StoreError>,
    {
        let authored = ticket_source
            .pull()
            .map_err(|error| ReindexError(error.to_string()))?;
        let mut known_ids: Vec<String> = authored
            .iter()
            .filter_map(|ticket| ticket.frontmatter.id.clone())
            .collect();
        let mut unique_ids = BTreeSet::new();
        for id in &known_ids {
            if !unique_ids.insert(id.clone()) {
                return Err(ReindexError(format!(
                    "duplicate ticket ID `{id}` in the configured ticket directory"
                )));
            }
        }
        let mut unique_refs = BTreeSet::new();
        for ticket in &authored {
            if !unique_refs.insert((&ticket.source, &ticket.source_ref)) {
                return Err(ReindexError(format!(
                    "duplicate source reference `{}` from `{}`",
                    ticket.source_ref, ticket.source
                )));
            }
        }
        let known_projects: BTreeSet<&str> = project_ids.iter().map(String::as_str).collect();
        let fallback_project = if known_projects.contains("default") {
            "default".to_owned()
        } else {
            project_ids.first().cloned().ok_or_else(|| {
                ReindexError("cannot index tickets without an indexed project".into())
            })?
        };

        let mut tickets = Vec::with_capacity(authored.len());
        let mut assigned_ids = BTreeSet::new();
        for authored_ticket in authored {
            let prior_id = self
                .ticket_by_source_ref(&authored_ticket.source, &authored_ticket.source_ref)
                .map_err(|error| ReindexError(error.to_string()))?
                .map(|ticket| ticket.id);
            let id = match authored_ticket.frontmatter.id.clone().or(prior_id) {
                Some(id) => id,
                None => {
                    let id = next_id(ticket_prefix, known_ids.iter().map(String::as_str))
                        .map_err(|error| ReindexError(error.to_string()))?;
                    known_ids.push(id.clone());
                    id
                }
            };
            if !assigned_ids.insert(id.clone()) {
                return Err(ReindexError(format!(
                    "duplicate ticket ID `{id}` in the pulled ticket source"
                )));
            }
            if !known_ids.contains(&id) {
                known_ids.push(id.clone());
            }
            let authored_project = authored_ticket
                .frontmatter
                .project
                .clone()
                .unwrap_or_else(|| "default".to_owned());
            let mut held_reason = authored_ticket.validation_error.clone();
            if authored_ticket.frontmatter.name.trim().is_empty() {
                held_reason.get_or_insert_with(|| {
                    format!(
                        "{}: frontmatter field `name` is required and must be non-empty",
                        authored_ticket.source_ref
                    )
                });
            }
            if authored_ticket.body.trim().is_empty() {
                held_reason.get_or_insert_with(|| {
                    format!(
                        "{}: ticket body must be non-empty",
                        authored_ticket.source_ref
                    )
                });
            }
            let project = if known_projects.contains(authored_project.as_str()) {
                authored_project.clone()
            } else {
                held_reason.get_or_insert_with(|| {
                    format!(
                        "{}: project `{authored_project}` is not indexed",
                        authored_ticket.source_ref
                    )
                });
                fallback_project.clone()
            };
            let flow = authored_ticket
                .frontmatter
                .flow
                .clone()
                .unwrap_or_else(|| default_flow.to_owned());
            if !flows.contains_key(&flow) {
                held_reason.get_or_insert_with(|| {
                    format!(
                        "{}: flow `{flow}` is not defined",
                        authored_ticket.source_ref
                    )
                });
            }
            let target = match authored_ticket.frontmatter.target.as_deref() {
                Some(target) if agent.is_some_and(|agent| agent.targets.contains_key(target)) => {
                    Some(target.to_owned())
                }
                Some(target) => {
                    held_reason.get_or_insert_with(|| {
                        format!(
                            "{}: agent target `{target}` is not configured",
                            authored_ticket.source_ref
                        )
                    });
                    Some(target.to_owned())
                }
                None => agent.map(|agent| agent.default_target.clone()),
            };
            if let (Some(agent), Some(target)) = (agent, target.as_deref()) {
                if let Some(command) = agent.targets.get(target)
                    && let Err(message) = expand_agent_cmd(
                        command,
                        authored_ticket.frontmatter.model.as_deref(),
                        authored_ticket.frontmatter.effort.as_deref(),
                        "",
                    )
                {
                    held_reason.get_or_insert_with(|| {
                        format!(
                            "{}: ticket using agent target `{target}` {message}",
                            authored_ticket.source_ref
                        )
                    });
                }
            }
            let worktree = match authored_ticket.frontmatter.worktree.clone() {
                Some(worktree) => worktree,
                None => {
                    let stem = authored_ticket
                        .file_path
                        .as_deref()
                        .and_then(Path::file_stem)
                        .and_then(|stem| stem.to_str());
                    match crate::ids::default_worktree(stem, &id) {
                        Ok(branch) => branch,
                        Err(reason) => {
                            held_reason.get_or_insert_with(|| {
                                format!("{}: {reason}", authored_ticket.source_ref)
                            });
                            format!("sloop/{id}")
                        }
                    }
                }
            };
            if held_reason.is_none()
                && let (Some(path), Some(content)) = (
                    authored_ticket.file_path.as_ref(),
                    authored_ticket.original_content.as_ref(),
                )
                && let Some(updated) = frontmatter::stamp(content, &id, &project, &worktree, &flow)
                    .map_err(|error| {
                        ReindexError(format!("{}: {error}", authored_ticket.source_ref))
                    })?
            {
                let absolute = root.join(path);
                fs::write(&absolute, updated)
                    .map_err(|source| ReindexError::io(&absolute, source))?;
            }

            tickets.push(ReindexTicket {
                id,
                project_id: project,
                source: authored_ticket.source,
                source_ref: authored_ticket.source_ref,
                file_path: authored_ticket
                    .file_path
                    .map(|path| path.to_string_lossy().into_owned()),
                name: authored_ticket.frontmatter.name,
                blocked_by: authored_ticket.frontmatter.blocked_by,
                worktree,
                target,
                model: authored_ticket.frontmatter.model,
                effort: authored_ticket.frontmatter.effort,
                flow,
                body: authored_ticket.body,
                held_reason,
                derived_state: None,
            });
        }

        let ticket_ids: BTreeSet<String> = tickets.iter().map(|ticket| ticket.id.clone()).collect();
        let mut dependencies = BTreeMap::new();
        for ticket in &mut tickets {
            let unknown_blocker = ticket
                .blocked_by
                .iter()
                .find(|blocker| !ticket_ids.contains(*blocker))
                .cloned();
            if let Some(blocker) = unknown_blocker {
                ticket.held_reason.get_or_insert_with(|| {
                    format!(
                        "ticket `{}` field `blocked_by` references unknown ticket `{blocker}`; edit `{}` to drop or correct the reference",
                        ticket.id, ticket.source_ref
                    )
                });
                ticket.blocked_by.clear();
            } else {
                dependencies.insert(ticket.id.clone(), ticket.blocked_by.clone());
            }
        }
        if let Some(chain) = crate::domain::graph::find_cycle(&dependencies) {
            return Err(ReindexError(format!(
                "field `blocked_by` creates a dependency cycle: {}",
                chain.join(" -> ")
            )));
        }

        crate::reindex_evidence::derive_states(root, worktree_dir, &mut tickets)?;
        let result = self
            .apply_reindex(
                project_ids,
                &tickets,
                now_ms,
                drop_runs,
                mark_runs_cleanup_eligible,
            )
            .map_err(|error| ReindexError(error.to_string()))?;
        self.last_sync_ms.store(now_ms, Ordering::Release);
        let state_changes: Vec<Value> = result
            .state_changes
            .into_iter()
            .map(|change| {
                json!({
                    "ticket": change.ticket_id,
                    "previous_state": change.previous_state,
                    "state": change.state,
                })
            })
            .collect();
        Ok(json!({
            "projects_indexed": project_ids.len(),
            "tickets_indexed": tickets.len(),
            "tickets_state_changed": state_changes.len(),
            "state_changes": state_changes,
            "rows_dropped": result.rows_dropped,
        }))
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

    pub fn select_ready_ticket(
        &self,
        project_id: Option<&str>,
        activation_id: &str,
        now_ms: i64,
    ) -> Result<Option<String>, StoreError> {
        self.db
            .lock()
            .query_row(
                "SELECT t.id FROM tickets t
                 WHERE t.state = 'ready'
                   AND t.missing_at_ms IS NULL
                   AND (?1 IS NULL OR t.project_id = ?1)
                   AND NOT EXISTS (SELECT 1 FROM ticket_blockers b
                                   JOIN tickets bt ON bt.id = b.blocker_id
                                   WHERE b.ticket_id = t.id
                                     AND bt.state != 'merged')
                    AND (NOT EXISTS (SELECT 1 FROM activation_filters f
                                    WHERE f.activation_id = ?2)
                        OR EXISTS (SELECT 1 FROM activation_filters f
                                    WHERE f.activation_id = ?2 AND f.ticket_id = t.id))
                   AND NOT EXISTS (SELECT 1 FROM cooldowns c
                                   WHERE c.key = 'agent_target:' || t.target
                                     AND c.until_ms > ?3)
                  ORDER BY t.created_at_ms, t.id
                  LIMIT 1",
                params![project_id, activation_id, now_ms],
                |row| row.get(0),
            )
            .optional()
            .map_err(StoreError::from)
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

    pub fn readopt_lease(
        &self,
        ticket_id: &str,
        run_id: &str,
        lease_ms: i64,
        now_ms: i64,
    ) -> Result<i64, StoreError> {
        let expires_at_ms = now_ms + lease_ms;
        let changed = self.db.lock().execute(
            "UPDATE leases
             SET renewed_at_ms = ?3, expires_at_ms = ?4
             WHERE ticket_id = ?1 AND run_id = ?2
               AND EXISTS (SELECT 1 FROM runs
                           WHERE id = ?2 AND exited_at_ms IS NULL)",
            params![ticket_id, run_id, now_ms, expires_at_ms],
        )?;
        if changed != 1 {
            return Err(StoreError::LeaseNotHeld {
                ticket_id: ticket_id.into(),
                run_id: run_id.into(),
            });
        }
        Ok(expires_at_ms)
    }

    pub fn renew_lease(
        &self,
        ticket_id: &str,
        run_id: &str,
        lease_ms: i64,
        now_ms: i64,
    ) -> Result<i64, StoreError> {
        let expires_at_ms = now_ms + lease_ms;
        let changed = self.db.lock().execute(
            "UPDATE leases
             SET renewed_at_ms = ?3, expires_at_ms = ?4
             WHERE ticket_id = ?1 AND run_id = ?2 AND expires_at_ms > ?3",
            params![ticket_id, run_id, now_ms, expires_at_ms],
        )?;
        if changed != 1 {
            return Err(StoreError::LeaseNotHeld {
                ticket_id: ticket_id.into(),
                run_id: run_id.into(),
            });
        }
        Ok(expires_at_ms)
    }

    pub(crate) fn active_lease_count(&self) -> Result<usize, StoreError> {
        let count: i64 = self
            .db
            .lock()
            .query_row("SELECT COUNT(*) FROM leases", [], |row| row.get(0))?;
        Ok(count as usize)
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

    pub(crate) fn ticket_has_work_references(&self, id: &str) -> Result<bool, StoreError> {
        Self::ticket_has_work_references_on(&self.db.lock(), id).map_err(StoreError::from)
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

    fn ticket_on(connection: &Connection, id: &str) -> Result<Option<TicketRecord>, StoreError> {
        let mut ticket = connection
            .query_row(
                &format!("{TICKET_RECORD_SELECT} WHERE id = ?1"),
                params![id],
                ticket_record,
            )
            .optional()?;
        if let Some(ticket) = ticket.as_mut() {
            ticket.blocked_by = Self::ticket_blockers(connection, &ticket.id)?;
        }
        Ok(ticket)
    }

    fn claimable_activation_on(
        transaction: &Transaction<'_>,
        ticket_id: &str,
        now_ms: i64,
    ) -> Result<Option<QueuedActivation>, StoreError> {
        transaction
            .query_row(
                "SELECT a.id, a.kind, a.ticket_id, a.project_id, a.eligible_at_ms, a.interval_ms
                 FROM activations a
                 JOIN tickets t ON t.id = ?1
                 WHERE a.state = 'queued'
                   AND (a.kind IN ('immediate', 'auto') OR a.eligible_at_ms <= ?2)
                   AND (a.ticket_id = t.id
                        OR (a.ticket_id IS NULL
                            AND (a.project_id IS NULL OR a.project_id = t.project_id)))
                   AND (NOT EXISTS (SELECT 1 FROM activation_filters f
                                    WHERE f.activation_id = a.id)
                        OR EXISTS (SELECT 1 FROM activation_filters f
                                   WHERE f.activation_id = a.id AND f.ticket_id = t.id))
                 ORDER BY a.created_at_ms, a.id
                 LIMIT 1",
                params![ticket_id, now_ms],
                |row| {
                    Ok(QueuedActivation {
                        id: row.get(0)?,
                        kind: row.get(1)?,
                        ticket_id: row.get(2)?,
                        project_id: row.get(3)?,
                        eligible_at_ms: row.get(4)?,
                        interval_ms: row.get(5)?,
                    })
                },
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn activation_for_release_on(
        transaction: &Transaction<'_>,
        ticket_id: &str,
    ) -> Result<Option<String>, StoreError> {
        transaction
            .query_row(
                "SELECT a.id
                 FROM activations a
                 JOIN tickets t ON t.id = ?1
                 WHERE (a.state = 'completed' OR (a.state = 'queued' AND a.kind = 'every'))
                   AND (a.ticket_id = t.id
                        OR (a.ticket_id IS NULL
                            AND (a.project_id IS NULL OR a.project_id = t.project_id)))
                   AND (NOT EXISTS (SELECT 1 FROM activation_filters f
                                    WHERE f.activation_id = a.id)
                        OR EXISTS (SELECT 1 FROM activation_filters f
                                   WHERE f.activation_id = a.id AND f.ticket_id = t.id))
                 ORDER BY a.updated_at_ms DESC, a.created_at_ms, a.id
                 LIMIT 1",
                params![ticket_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(StoreError::from)
    }
}

fn source_error(error: StoreError) -> SourceError {
    match error {
        StoreError::TicketNotFound { .. }
        | StoreError::TicketStateConflict { .. }
        | StoreError::ActivationNotQueued { .. }
        | StoreError::LeaseNotHeld { .. }
        | StoreError::TicketNotReady { .. } => SourceError::Rejected {
            message: error.to_string(),
        },
        _ => SourceError::Unavailable { retry_after: None },
    }
}

fn lease_owner(owner: &OwnerId, activation_id: &str) -> String {
    json!({"owner": owner.0, "activation": activation_id}).to_string()
}

fn decode_lease_owner(stored: &str) -> (OwnerId, Option<String>) {
    let parsed = serde_json::from_str::<Value>(stored).ok();
    let owner = parsed
        .as_ref()
        .and_then(|value| value["owner"].as_str())
        .unwrap_or(stored);
    let activation = parsed
        .as_ref()
        .and_then(|value| value["activation"].as_str())
        .map(str::to_owned);
    (OwnerId(owner.into()), activation)
}

fn lease_ms(ttl: Duration) -> Result<i64, SourceError> {
    i64::try_from(ttl.as_millis()).map_err(|_| SourceError::Rejected {
        message: "lease duration is outside the supported range".into(),
    })
}

fn rearm_every_at(eligible_at_ms: i64, interval_ms: i64, now_ms: i64) -> Option<i64> {
    if interval_ms <= 0 || eligible_at_ms > now_ms {
        return None;
    }
    let missed = now_ms.checked_sub(eligible_at_ms)?.div_euclid(interval_ms);
    let steps = missed.checked_add(1)?;
    eligible_at_ms.checked_add(interval_ms.checked_mul(steps)?)
}

fn work_ticket(
    record: TicketRecord,
    blocked: bool,
    owner: OwnerId,
) -> Result<WorkTicket, SourceError> {
    let state = match record.state.as_str() {
        "ready" => TicketState::Ready,
        "held" => TicketState::Held,
        "claimed" => TicketState::Claimed,
        "merged" => TicketState::Merged,
        "failed" => TicketState::Failed,
        "needs_review" => TicketState::NeedsReview,
        state => {
            return Err(SourceError::Corrupt {
                message: format!("ticket `{}` has unknown state `{state}`", record.id),
            });
        }
    };
    let attempts = u32::try_from(record.attempts).map_err(|_| SourceError::Corrupt {
        message: format!(
            "ticket `{}` has invalid attempt count {}",
            record.id, record.attempts
        ),
    })?;

    Ok(WorkTicket {
        id: record.id,
        project_id: record.project_id,
        name: record.name,
        body: record.body.unwrap_or_default(),
        state: WorkTicketState::from_ticket_state(
            state,
            blocked,
            record.held_reason.unwrap_or_default(),
            owner,
        ),
        blocked_by: record.blocked_by,
        attempts,
        hints: ExecutionHints {
            worktree: record.worktree,
            activation_id: None,
            target: record.target,
            model: record.model,
            effort: record.effort,
            flow: record.flow,
        },
        version: SourceVersion(record.updated_at_ms.to_string()),
    })
}

/// SQLite satisfies atomic claims with an IMMEDIATE transaction and a
/// conditional ticket update. The ticket row is authoritative for attempts:
/// claim consumes one attempt, while release preserves that count as evidence
/// of the completed try. This backend never records runs, and its local-source
/// outcome push is idempotent because release already applied the durable
/// ticket state.
#[async_trait]
impl WorkState for LocalSqlite {
    fn claim_strength(&self) -> ClaimStrength {
        ClaimStrength::Atomic
    }

    async fn pull_ready(&self) -> Result<Vec<WorkTicket>, SourceError> {
        let now_ms = self.clock.now_ms();
        let activations = self
            .dispatchable_activations(now_ms)
            .map_err(source_error)?;
        let mut seen = BTreeSet::new();
        let mut selected = Vec::new();
        for activation in activations {
            let ticket_id = match activation.ticket_id {
                Some(ticket_id) => self
                    .ticket_is_dispatchable(&ticket_id)
                    .map_err(source_error)?
                    .then_some(ticket_id),
                None => self
                    .select_ready_ticket(activation.project_id.as_deref(), &activation.id, now_ms)
                    .map_err(source_error)?,
            };
            if let Some(ticket_id) = ticket_id
                && seen.insert(ticket_id.clone())
            {
                selected.push(ticket_id);
            }
        }

        selected
            .into_iter()
            .map(|id| {
                let record = self.ticket(&id).map_err(source_error)?.ok_or_else(|| {
                    SourceError::Corrupt {
                        message: format!("selected ticket `{id}` no longer exists"),
                    }
                })?;
                work_ticket(record, false, OwnerId(String::new()))
            })
            .collect()
    }

    async fn active_claims(&self) -> Result<Vec<ActiveClaim>, SourceError> {
        let connection = self.db.lock();
        let mut statement = connection
            .prepare(
                "SELECT t.id, t.source, t.source_ref, l.run_id
                 FROM leases l
                 JOIN tickets t ON t.id = l.ticket_id
                 ORDER BY l.acquired_at_ms, l.ticket_id",
            )
            .map_err(StoreError::from)
            .map_err(source_error)?;
        statement
            .query_map([], |row| {
                Ok(ActiveClaim {
                    ticket: TicketRef {
                        id: row.get(0)?,
                        source: row.get(1)?,
                        source_ref: row.get(2)?,
                    },
                    owner: OwnerId(row.get(3)?),
                })
            })
            .map_err(StoreError::from)
            .map_err(source_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
            .map_err(source_error)
    }

    async fn claim(
        &self,
        ticket: &TicketRef,
        owner: &OwnerId,
        ttl: Duration,
    ) -> Result<ClaimResult, SourceError> {
        let now_ms = self.clock.now_ms();
        let lease_ms = lease_ms(ttl)?;
        let mut connection = self.db.lock();
        // The unchanged schema still points leases at runs. Admission now
        // commits the source claim first, so this one connection suspends FK
        // enforcement only while creating that recoverable dangling lease.
        connection
            .pragma_update(None, "foreign_keys", false)
            .map_err(StoreError::from)
            .map_err(source_error)?;
        let result = (|| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(StoreError::from)
                .map_err(source_error)?;
            let Some(activation) = Self::claimable_activation_on(&transaction, &ticket.id, now_ms)
                .map_err(source_error)?
            else {
                return Ok(ClaimResult::Lost { held_by: None });
            };
            let next_activation_eligible_at_ms = if activation.kind == "every" {
                match (activation.eligible_at_ms, activation.interval_ms) {
                    (Some(eligible_at_ms), Some(interval_ms)) => {
                        rearm_every_at(eligible_at_ms, interval_ms, now_ms)
                    }
                    _ => None,
                }
            } else {
                None
            };
            if activation.kind == "every" && next_activation_eligible_at_ms.is_none() {
                return Err(SourceError::Corrupt {
                    message: format!(
                        "recurring activation `{}` has an invalid cadence",
                        activation.id
                    ),
                });
            }
            let stored_owner = lease_owner(owner, &activation.id);
            let claim = ClaimTransaction {
                ticket_id: &ticket.id,
                run_id: &owner.0,
                activation_id: &activation.id,
                owner_id: &stored_owner,
                lease_ms,
                next_activation_eligible_at_ms,
            };

            match tx::claim_ticket(&transaction, &claim, now_ms) {
                Ok(()) => {}
                Err(StoreError::TicketNotReady { .. }) => {
                    let held_by = transaction
                        .query_row(
                            "SELECT owner_id FROM leases WHERE ticket_id = ?1",
                            params![ticket.id],
                            |row| row.get::<_, String>(0),
                        )
                        .optional()
                        .map_err(StoreError::from)
                        .map_err(source_error)?
                        .map(|stored| decode_lease_owner(&stored).0);
                    return Ok(ClaimResult::Lost { held_by });
                }
                Err(error) => return Err(source_error(error)),
            }
            tx::advance_activation(&transaction, &claim, now_ms).map_err(source_error)?;
            tx::insert_lease(&transaction, &claim, now_ms).map_err(source_error)?;
            let record = Self::ticket_on(&transaction, &ticket.id)
                .map_err(source_error)?
                .ok_or_else(|| SourceError::Corrupt {
                    message: format!("claimed ticket `{}` no longer exists", ticket.id),
                })?;
            let mut ticket = work_ticket(record, false, owner.clone())?;
            ticket.hints.activation_id = Some(activation.id.clone());
            transaction
                .commit()
                .map_err(StoreError::from)
                .map_err(source_error)?;
            Ok(ClaimResult::Claimed { ticket })
        })();
        let restored = connection
            .pragma_update(None, "foreign_keys", true)
            .map_err(StoreError::from)
            .map_err(source_error);
        match (result, restored) {
            (_, Err(error)) => Err(error),
            (result, Ok(())) => result,
        }
    }

    async fn renew(&self, ticket: &TicketRef, owner: &OwnerId) -> Result<ClaimResult, SourceError> {
        let now_ms = self.clock.now_ms();
        let lease_ms = self
            .db
            .lock()
            .query_row(
                "SELECT expires_at_ms - renewed_at_ms FROM leases
                 WHERE ticket_id = ?1 AND run_id = ?2",
                params![ticket.id, owner.0],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(StoreError::from)
            .map_err(source_error)?;
        let Some(lease_ms) = lease_ms else {
            return Ok(ClaimResult::Lost { held_by: None });
        };
        match self.renew_lease(&ticket.id, &owner.0, lease_ms, now_ms) {
            Ok(_) => {}
            Err(StoreError::LeaseNotHeld { .. }) => {
                return Ok(ClaimResult::Lost { held_by: None });
            }
            Err(error) => return Err(source_error(error)),
        }
        let record = self
            .ticket(&ticket.id)
            .map_err(source_error)?
            .ok_or_else(|| SourceError::Corrupt {
                message: format!("leased ticket `{}` no longer exists", ticket.id),
            })?;
        Ok(ClaimResult::Claimed {
            ticket: work_ticket(record, false, owner.clone())?,
        })
    }

    async fn release(
        &self,
        ticket: &TicketRef,
        owner: &OwnerId,
        disposition: Disposition,
    ) -> Result<(), SourceError> {
        let now_ms = self.clock.now_ms();
        let mut connection = self.db.lock();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::from)
            .map_err(source_error)?;
        let lease = transaction
            .query_row(
                "SELECT run_id, owner_id FROM leases WHERE ticket_id = ?1",
                params![ticket.id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(StoreError::from)
            .map_err(source_error)?;
        let claimed_activation_id = match lease {
            Some((run_id, stored_owner)) if run_id == owner.0 => {
                decode_lease_owner(&stored_owner).1
            }
            Some(_) => {
                // A newer owner proves this owner's release already completed.
                // The stale retry must not disturb the newer claim.
                transaction
                    .commit()
                    .map_err(StoreError::from)
                    .map_err(source_error)?;
                return Ok(());
            }
            None => {
                let (state, held_reason) = transaction
                    .query_row(
                        "SELECT state, held_reason FROM tickets WHERE id = ?1",
                        params![ticket.id],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
                    )
                    .optional()
                    .map_err(StoreError::from)
                    .map_err(source_error)?
                    .ok_or_else(|| SourceError::Rejected {
                        message: format!("ticket `{}` does not exist", ticket.id),
                    })?;
                let converged = match &disposition {
                    Disposition::Complete => state == TicketState::Merged.as_str(),
                    Disposition::Retry { .. } => state == TicketState::Ready.as_str(),
                    Disposition::Park { reason } if reason == "needs-review" => {
                        state == TicketState::NeedsReview.as_str()
                    }
                    Disposition::Park { reason } => {
                        state == TicketState::Held.as_str()
                            && held_reason.as_deref() == Some(reason.as_str())
                    }
                    Disposition::Abandon => state == TicketState::Failed.as_str(),
                };
                if converged {
                    transaction
                        .commit()
                        .map_err(StoreError::from)
                        .map_err(source_error)?;
                    return Ok(());
                }
                if matches!(disposition, Disposition::Complete)
                    && state == TicketState::NeedsReview.as_str()
                {
                    let changed = tx::settle_external_merge(&transaction, &ticket.id, now_ms)
                        .map_err(StoreError::from)
                        .map_err(source_error)?;
                    if changed == 1 {
                        tx::complete_ticket_activations(&transaction, &ticket.id, now_ms)
                            .map_err(StoreError::from)
                            .map_err(source_error)?;
                    }
                    transaction
                        .commit()
                        .map_err(StoreError::from)
                        .map_err(source_error)?;
                    return Ok(());
                }
                if state != TicketState::Claimed.as_str() {
                    transaction
                        .commit()
                        .map_err(StoreError::from)
                        .map_err(source_error)?;
                    return Ok(());
                }
                return Err(SourceError::Rejected {
                    message: format!("ticket `{}` is no longer claimed", ticket.id),
                });
            }
        };

        let changed = match disposition {
            Disposition::Complete => {
                let changed =
                    tx::settle_ticket(&transaction, &ticket.id, TicketState::Merged, now_ms)
                        .map_err(StoreError::from)
                        .map_err(source_error)?;
                if changed == 1 {
                    tx::complete_ticket_activations(&transaction, &ticket.id, now_ms)
                        .map_err(StoreError::from)
                        .map_err(source_error)?;
                }
                changed
            }
            Disposition::Retry { not_before_ms } => {
                let changed = tx::abort_ticket(&transaction, &ticket.id, now_ms)
                    .map_err(StoreError::from)
                    .map_err(source_error)?;
                if let Some(eligible_at_ms) = not_before_ms {
                    let activation_id = match claimed_activation_id {
                        Some(activation_id) => activation_id,
                        None => Self::activation_for_release_on(&transaction, &ticket.id)
                            .map_err(source_error)?
                            .ok_or_else(|| SourceError::Corrupt {
                                message: format!(
                                    "ticket `{}` has no activation to retry",
                                    ticket.id
                                ),
                            })?,
                    };
                    tx::requeue_activation(&transaction, &activation_id, now_ms)
                        .map_err(StoreError::from)
                        .map_err(source_error)?;
                    transaction
                        .execute(
                            "UPDATE activations
                             SET eligible_at_ms = ?2, updated_at_ms = ?3
                             WHERE id = ?1 AND state = 'queued'",
                            params![activation_id, eligible_at_ms, now_ms],
                        )
                        .map_err(StoreError::from)
                        .map_err(source_error)?;
                }
                changed
            }
            Disposition::Park { reason } => {
                let ticket_state = if reason == "needs-review" {
                    TicketState::NeedsReview
                } else {
                    TicketState::Held
                };
                let changed = tx::settle_ticket(&transaction, &ticket.id, ticket_state, now_ms)
                    .map_err(StoreError::from)
                    .map_err(source_error)?;
                if changed == 1 && ticket_state == TicketState::Held {
                    transaction
                        .execute(
                            "UPDATE tickets SET held_reason = ?2 WHERE id = ?1 AND state = 'held'",
                            params![ticket.id, reason],
                        )
                        .map_err(StoreError::from)
                        .map_err(source_error)?;
                }
                changed
            }
            Disposition::Abandon => {
                tx::settle_ticket(&transaction, &ticket.id, TicketState::Failed, now_ms)
                    .map_err(StoreError::from)
                    .map_err(source_error)?
            }
        };
        if changed != 1 {
            return Err(SourceError::Rejected {
                message: format!("ticket `{}` is no longer claimed", ticket.id),
            });
        }
        tx::delete_lease(&transaction, &owner.0)
            .map_err(StoreError::from)
            .map_err(source_error)?;
        transaction
            .commit()
            .map_err(StoreError::from)
            .map_err(source_error)?;
        Ok(())
    }

    async fn push_outcome(&self, outcome: &WorkOutcome) -> Result<(), SourceError> {
        let Some(reporter) = self.outcome_reporter.clone() else {
            return Ok(());
        };
        let ticket_id = outcome.ticket_id.clone();
        let verdict = outcome.verdict;
        reporter
            .report(&ticket_id, &verdict)
            .map_err(|error| SourceError::Rejected {
                message: error.to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::process::Command;
    use std::sync::Arc;
    use std::time::Duration;

    use tempfile::tempdir;

    use crate::db::{Db, StoreError};
    use crate::domain::ticket::TicketState;
    use crate::domain::work::{Disposition, OwnerId, TicketRef, WorkOutcome, WorkTicket};
    use crate::outcome::Outcome;
    use crate::work_state::{ClaimResult, TicketFeeder, WorkState};

    use super::{
        ActivationKind, ClaimTransaction, LocalSqlite, NewActivation, QueuedActivation,
        ReindexTicket,
    };

    fn open_seeded(path: &std::path::Path) -> LocalSqlite {
        let store = LocalSqlite::from_db(Db::open(path, 1_000).unwrap());
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

    fn insert_claimed_run(store: &LocalSqlite, claim: &ClaimTransaction<'_>, now_ms: i64) -> i64 {
        let connection = store.db.lock();
        let attempt = connection
            .query_row(
                "SELECT COALESCE(MAX(attempt), 0) + 1 FROM runs WHERE ticket_id = ?1",
                rusqlite::params![claim.ticket_id],
                |row| row.get(0),
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO runs
                     (id, activation_id, ticket_id, state, attempt, flow_json, ticket_json,
                      created_at_ms, updated_at_ms)
                 VALUES (?1, ?2, ?3, 'claimed', ?4, '{}', '{}', ?5, ?5)",
                rusqlite::params![
                    claim.run_id,
                    claim.activation_id,
                    claim.ticket_id,
                    attempt,
                    now_ms
                ],
            )
            .unwrap();
        attempt
    }

    fn granted_claim(store: &LocalSqlite, claim: &ClaimTransaction<'_>, now_ms: i64) -> i64 {
        let mut connection = store.db.lock();
        connection
            .pragma_update(None, "foreign_keys", false)
            .unwrap();
        {
            let transaction = connection
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .unwrap();
            super::tx::claim_ticket(&transaction, claim, now_ms).unwrap();
            super::tx::advance_activation(&transaction, claim, now_ms).unwrap();
            super::tx::insert_lease(&transaction, claim, now_ms).unwrap();
            transaction.commit().unwrap();
        }
        connection
            .pragma_update(None, "foreign_keys", true)
            .unwrap();
        drop(connection);
        insert_claimed_run(store, claim, now_ms)
    }

    fn settle_for_test(store: &LocalSqlite, run_id: &str, outcome: Outcome, now_ms: i64) {
        let state = outcome.as_str();
        let ticket_state = match outcome {
            Outcome::Merged => "merged",
            Outcome::Failed => "failed",
            Outcome::NeedsReview => "needs_review",
            Outcome::Cancelled | Outcome::RateLimited | Outcome::Orphaned => "ready",
        };
        let mut connection = store.db.lock();
        let transaction = connection.transaction().unwrap();
        transaction
            .execute(
                "UPDATE runs SET state = ?2, exited_at_ms = ?3, updated_at_ms = ?3 WHERE id = ?1",
                rusqlite::params![run_id, state, now_ms],
            )
            .unwrap();
        transaction
            .execute(
                "UPDATE tickets SET state = ?2, updated_at_ms = ?3
                 WHERE id = (SELECT ticket_id FROM runs WHERE id = ?1)",
                rusqlite::params![run_id, ticket_state, now_ms],
            )
            .unwrap();
        transaction
            .execute("DELETE FROM leases WHERE run_id = ?1", [run_id])
            .unwrap();
        transaction.commit().unwrap();
    }

    fn select_ready_ticket(
        store: &LocalSqlite,
        activation: &QueuedActivation,
        now_ms: i64,
    ) -> Option<String> {
        store
            .select_ready_ticket(activation.project_id.as_deref(), &activation.id, now_ms)
            .unwrap()
    }

    fn apply_reindex(store: &LocalSqlite, tickets: &[ReindexTicket], now_ms: i64) {
        store
            .apply_reindex(
                &["default".into()],
                tickets,
                now_ms,
                |_, _, _| Ok(0),
                |_, _, _| Ok(()),
            )
            .unwrap();
    }

    fn open_seeded_local() -> (tempfile::TempDir, LocalSqlite) {
        let directory = tempdir().unwrap();
        let local =
            LocalSqlite::from_db(Db::open(&directory.path().join("sloop.db"), 1_000).unwrap());
        local
            .insert_local_project(
                "default",
                ".agents/sloop/projects/default.md",
                "Default",
                1_000,
            )
            .unwrap();
        local
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
        local
            .insert_activation(
                &super::NewActivation {
                    id: "A1",
                    kind: super::ActivationKind::Immediate,
                    ticket_id: Some("T1"),
                    project_id: None,
                    eligible_at_ms: None,
                    interval_ms: None,
                },
                1_000,
            )
            .unwrap();
        (directory, local)
    }

    fn ticket_ref() -> TicketRef {
        TicketRef {
            id: "T1".into(),
            source: "local".into(),
            source_ref: None,
        }
    }

    async fn claim_local(local: &LocalSqlite, run_id: &str) -> ClaimResult {
        local
            .claim(
                &ticket_ref(),
                &OwnerId(run_id.into()),
                Duration::from_secs(60),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn local_work_state_claim_is_atomic() {
        let (_directory, local) = open_seeded_local();

        assert!(matches!(
            claim_local(&local, "R1").await,
            ClaimResult::Claimed {
                ticket: WorkTicket { attempts: 1, .. }
            }
        ));
        {
            let connection = local.db.lock();
            assert_eq!(
                connection
                    .query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                1
            );
            assert_eq!(
                connection
                    .query_row("SELECT COUNT(*) FROM runs", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                0
            );
            assert_eq!(
                connection
                    .prepare("PRAGMA foreign_key_check")
                    .unwrap()
                    .query_map([], |_| Ok(()))
                    .unwrap()
                    .count(),
                1
            );
        }
        assert_eq!(local.active_claims().await.unwrap().len(), 1);
        insert_claimed_run(
            &local,
            &ClaimTransaction {
                ticket_id: "T1",
                run_id: "R1",
                activation_id: "A1",
                owner_id: "R1",
                lease_ms: 60_000,
                next_activation_eligible_at_ms: None,
            },
            2_000,
        );
        assert_eq!(
            local
                .db
                .lock()
                .prepare("PRAGMA foreign_key_check")
                .unwrap()
                .query_map([], |_| Ok(()))
                .unwrap()
                .count(),
            0
        );
        assert_eq!(
            claim_local(&local, "R1").await,
            ClaimResult::Lost { held_by: None }
        );
    }

    #[tokio::test]
    async fn local_work_state_retry_preserves_attempts_until_the_next_claim() {
        let (_directory, local) = open_seeded_local();
        claim_local(&local, "R1").await;

        local
            .release(
                &ticket_ref(),
                &OwnerId("R1".into()),
                Disposition::Retry {
                    not_before_ms: Some(2_000),
                },
            )
            .await
            .unwrap();
        let retried = local.ticket("T1").unwrap().unwrap();
        assert_eq!(retried.state, "ready");
        assert_eq!(retried.attempts, 1);
        assert_eq!(
            local
                .queued_activations()
                .unwrap()
                .first()
                .and_then(|activation| activation.eligible_at_ms),
            Some(2_000)
        );

        assert!(matches!(
            claim_local(&local, "R2").await,
            ClaimResult::Claimed {
                ticket: WorkTicket { attempts: 2, .. }
            }
        ));
    }

    #[tokio::test]
    async fn local_work_state_park_and_abandon_apply_the_disposition() {
        let (_park_directory, parked) = open_seeded_local();
        claim_local(&parked, "R1").await;
        parked
            .release(
                &ticket_ref(),
                &OwnerId("R1".into()),
                Disposition::Park {
                    reason: "operator review".into(),
                },
            )
            .await
            .unwrap();
        let ticket = parked.ticket("T1").unwrap().unwrap();
        assert_eq!(ticket.state, "held");
        assert_eq!(ticket.held_reason.as_deref(), Some("operator review"));

        let (_abandon_directory, abandoned) = open_seeded_local();
        claim_local(&abandoned, "R1").await;
        abandoned
            .release(&ticket_ref(), &OwnerId("R1".into()), Disposition::Abandon)
            .await
            .unwrap();
        assert_eq!(abandoned.ticket("T1").unwrap().unwrap().state, "failed");
    }

    #[tokio::test]
    async fn local_work_state_release_is_idempotent_and_preserves_outcome_states() {
        let (_complete_directory, completed) = open_seeded_local();
        claim_local(&completed, "R1").await;
        completed
            .release(&ticket_ref(), &OwnerId("R1".into()), Disposition::Complete)
            .await
            .unwrap();
        completed
            .release(&ticket_ref(), &OwnerId("R1".into()), Disposition::Complete)
            .await
            .unwrap();
        assert_eq!(completed.ticket("T1").unwrap().unwrap().state, "merged");

        let (_review_directory, review) = open_seeded_local();
        claim_local(&review, "R1").await;
        review
            .release(
                &ticket_ref(),
                &OwnerId("R1".into()),
                Disposition::Park {
                    reason: "needs-review".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(review.ticket("T1").unwrap().unwrap().state, "needs_review");
    }

    fn insert_ready_ticket(local: &LocalSqlite, id: &str, now_ms: i64) {
        local
            .insert_local_ticket(
                id,
                "default",
                &format!(".agents/sloop/tickets/{}.md", id.to_lowercase()),
                "Another ticket",
                &[],
                &format!("sloop/{id}"),
                Some("claude"),
                Some("sonnet"),
                Some("medium"),
                "default",
                TicketState::Ready,
                now_ms,
            )
            .unwrap();
    }

    fn insert_queued_activation(
        local: &LocalSqlite,
        id: &str,
        kind: ActivationKind,
        ticket_id: Option<&str>,
        project_id: Option<&str>,
    ) {
        local
            .insert_activation(
                &NewActivation {
                    id,
                    kind,
                    ticket_id,
                    project_id,
                    eligible_at_ms: None,
                    interval_ms: None,
                },
                1_000,
            )
            .unwrap();
    }

    fn queued_activation_ids(local: &LocalSqlite) -> Vec<String> {
        local
            .queued_activations()
            .unwrap()
            .into_iter()
            .map(|activation| activation.id)
            .collect()
    }

    #[tokio::test]
    async fn local_work_state_complete_retires_activations_pinned_to_the_merged_ticket() {
        let (_directory, local) = open_seeded_local();
        insert_ready_ticket(&local, "T2", 1_000);
        // A2 is a recurring trigger pinned to the merging ticket: not yet
        // eligible, so the claim leaves it alone, and unfireable forever once
        // T1 merges. A3 is pinned elsewhere and A4 is unpinned demand, so both
        // must survive.
        local
            .insert_activation(
                &NewActivation {
                    id: "A2",
                    kind: ActivationKind::Every,
                    ticket_id: Some("T1"),
                    project_id: None,
                    eligible_at_ms: Some(50_000),
                    interval_ms: Some(60_000),
                },
                1_000,
            )
            .unwrap();
        insert_queued_activation(&local, "A3", ActivationKind::Immediate, Some("T2"), None);
        insert_queued_activation(&local, "A4", ActivationKind::Auto, None, Some("default"));

        claim_local(&local, "R1").await;
        local
            .release(&ticket_ref(), &OwnerId("R1".into()), Disposition::Complete)
            .await
            .unwrap();

        assert_eq!(local.ticket("T1").unwrap().unwrap().state, "merged");
        assert_eq!(queued_activation_ids(&local), vec!["A3", "A4"]);
        // The surviving unpinned activation still resolves to whatever is ready.
        assert_eq!(
            local
                .select_ready_ticket(Some("default"), "A4", 2_000)
                .unwrap(),
            Some("T2".to_owned())
        );
        assert_eq!(
            local
                .pull_ready()
                .await
                .unwrap()
                .into_iter()
                .map(|ticket| ticket.id)
                .collect::<Vec<_>>(),
            vec!["T2".to_owned()]
        );
    }

    #[tokio::test]
    async fn local_work_state_external_merge_retires_pinned_activations() {
        let (_directory, local) = open_seeded_local();
        insert_queued_activation(&local, "A2", ActivationKind::Immediate, Some("T1"), None);
        claim_local(&local, "R1").await;
        local
            .release(
                &ticket_ref(),
                &OwnerId("R1".into()),
                Disposition::Park {
                    reason: "needs-review".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(queued_activation_ids(&local), vec!["A2"]);

        // A settled review branch merged outside the run reaches release with
        // no lease left to consume; the pinned activation dies there too.
        local
            .release(&ticket_ref(), &OwnerId("R1".into()), Disposition::Complete)
            .await
            .unwrap();

        assert_eq!(local.ticket("T1").unwrap().unwrap().state, "merged");
        assert!(queued_activation_ids(&local).is_empty());
    }

    #[tokio::test]
    async fn local_work_state_non_merge_dispositions_leave_pinned_activations_queued() {
        // `failed`, `held` and `needs_review` are not final the way `merged`
        // is: the ticket can return to `ready`, so its triggers must survive.
        for disposition in [
            Disposition::Abandon,
            Disposition::Park {
                reason: "operator review".into(),
            },
            Disposition::Park {
                reason: "needs-review".into(),
            },
            Disposition::Retry {
                not_before_ms: None,
            },
        ] {
            let (_directory, local) = open_seeded_local();
            insert_queued_activation(&local, "A2", ActivationKind::Immediate, Some("T1"), None);
            claim_local(&local, "R1").await;
            local
                .release(&ticket_ref(), &OwnerId("R1".into()), disposition)
                .await
                .unwrap();
            assert_eq!(queued_activation_ids(&local), vec!["A2"]);
        }
    }

    #[test]
    fn complete_merged_ticket_activations_sweeps_only_stranded_pinned_rows() {
        let (_directory, local) = open_seeded_local();
        insert_ready_ticket(&local, "T2", 1_000);
        insert_queued_activation(&local, "A2", ActivationKind::Immediate, Some("T2"), None);
        insert_queued_activation(&local, "A3", ActivationKind::Auto, None, Some("default"));
        // A1 is pinned to T1, which reached `merged` without the settle-time
        // rule ever running — exactly the row the sweep exists for.
        local
            .db
            .lock()
            .execute("UPDATE tickets SET state = 'merged' WHERE id = 'T1'", [])
            .unwrap();

        assert_eq!(
            local.complete_merged_ticket_activations(2_000).unwrap(),
            vec![("A1".to_owned(), "T1".to_owned())]
        );
        assert_eq!(queued_activation_ids(&local), vec!["A2", "A3"]);

        // Idempotent: a database with nothing stranded reports nothing.
        assert!(
            local
                .complete_merged_ticket_activations(3_000)
                .unwrap()
                .is_empty()
        );
        assert_eq!(queued_activation_ids(&local), vec!["A2", "A3"]);
    }

    #[tokio::test]
    async fn local_work_state_denies_renewal_of_an_expired_lease() {
        let (_directory, local) = open_seeded_local();
        claim_local(&local, "R1").await;
        local
            .db
            .lock()
            .execute(
                "UPDATE leases SET expires_at_ms = renewed_at_ms WHERE run_id = 'R1'",
                [],
            )
            .unwrap();

        assert_eq!(
            local
                .renew(&ticket_ref(), &OwnerId("R1".into()))
                .await
                .unwrap(),
            ClaimResult::Lost { held_by: None }
        );
    }

    #[tokio::test]
    async fn local_work_state_reports_exec_outcomes_with_the_existing_wire_shape() {
        let directory = tempdir().unwrap();
        let request_path = directory.path().join("report.json");
        let feeder = TicketFeeder::exec(
            directory.path(),
            vec![
                "sh".into(),
                "-c".into(),
                "cat > \"$1\"".into(),
                "ticket-source".into(),
                request_path.to_string_lossy().into_owned(),
            ],
        );
        let local = LocalSqlite::from_db_with_clock_and_reporter(
            Db::open(&directory.path().join("sloop.db"), 1_000).unwrap(),
            Arc::new(crate::clock::SystemClock),
            feeder.exec_reporter(),
        );

        local
            .push_outcome(&WorkOutcome {
                ticket_id: "T1".into(),
                owner: OwnerId("R1".into()),
                verdict: Outcome::Merged,
                branch: Some("sloop/T1".into()),
                commit_count: 1,
                attempt: 1,
                finished_at_ms: 2_000,
            })
            .await
            .unwrap();

        let request: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(request_path).unwrap()).unwrap();
        assert_eq!(
            request,
            serde_json::json!({
                "verb": "report",
                "ticket": "T1",
                "outcome": "merged",
            })
        );
    }

    #[tokio::test]
    async fn local_work_state_without_an_exec_reporter_keeps_outcome_push_a_no_op() {
        let directory = tempdir().unwrap();
        let local =
            LocalSqlite::from_db(Db::open(&directory.path().join("sloop.db"), 1_000).unwrap());

        local
            .push_outcome(&WorkOutcome {
                ticket_id: "T1".into(),
                owner: OwnerId("R1".into()),
                verdict: Outcome::Merged,
                branch: None,
                commit_count: 0,
                attempt: 1,
                finished_at_ms: 2_000,
            })
            .await
            .unwrap();
    }

    #[test]
    fn successful_sync_records_the_last_sync_timestamp_in_memory() {
        let directory = tempdir().unwrap();
        assert!(
            Command::new("git")
                .args(["init", "--quiet"])
                .current_dir(directory.path())
                .status()
                .unwrap()
                .success()
        );
        let work_state =
            LocalSqlite::from_db(Db::open(&directory.path().join("sloop.db"), 1_000).unwrap());
        work_state
            .insert_local_project("default", "projects/default.md", "Default", 1_000)
            .unwrap();
        let empty_source = TicketFeeder::markdown(directory.path(), "tickets");
        let failing_source = TicketFeeder::exec(
            directory.path(),
            vec![
                "sh".into(),
                "-c".into(),
                "printf 'pull failed' >&2; exit 1".into(),
            ],
        );
        let project_ids = vec!["default".to_owned()];
        let drop_runs = |_: &rusqlite::Transaction<'_>, _: &[String], _: &_| Ok(0);
        let mark_runs = |_: &rusqlite::Transaction<'_>, _: &str, _: i64| Ok(());

        assert_eq!(work_state.last_sync_ms(), None);
        work_state
            .sync_from_source(
                directory.path(),
                &empty_source,
                &directory.path().join("worktrees"),
                2_000,
                "T",
                &project_ids,
                None,
                &BTreeMap::new(),
                "default",
                drop_runs,
                mark_runs,
            )
            .unwrap();
        assert_eq!(work_state.last_sync_ms(), Some(2_000));

        let error = work_state
            .sync_from_source(
                directory.path(),
                &failing_source,
                &directory.path().join("worktrees"),
                3_000,
                "T",
                &project_ids,
                None,
                &BTreeMap::new(),
                "default",
                drop_runs,
                mark_runs,
            )
            .unwrap_err();
        assert!(error.to_string().contains("pull failed"));
        assert_eq!(work_state.last_sync_ms(), Some(2_000));
    }

    fn claim_t1<'a>(run_id: &'a str) -> ClaimTransaction<'a> {
        ClaimTransaction {
            ticket_id: "T1",
            run_id,
            activation_id: "A1",
            owner_id: "daemon-1",
            lease_ms: 60_000,
            next_activation_eligible_at_ms: None,
        }
    }

    #[test]
    fn renewing_a_held_lease_extends_its_expiry() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));
        granted_claim(&store, &claim_t1("R1"), 2_000);

        let expires = store.renew_lease("T1", "R1", 60_000, 10_000).unwrap();
        assert_eq!(expires, 70_000);
    }

    #[test]
    fn a_run_cannot_renew_a_lease_it_does_not_hold() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));
        granted_claim(&store, &claim_t1("R1"), 2_000);

        let error = store.renew_lease("T1", "R2", 60_000, 10_000).unwrap_err();
        assert!(matches!(error, StoreError::LeaseNotHeld { .. }));
    }

    #[test]
    fn an_expired_lease_cannot_be_renewed() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));
        granted_claim(&store, &claim_t1("R1"), 2_000);

        // The lease expires at 62_000; renewal at or after that must fail.
        let error = store.renew_lease("T1", "R1", 60_000, 62_000).unwrap_err();
        assert!(matches!(error, StoreError::LeaseNotHeld { .. }));
    }

    #[test]
    fn a_readopted_lease_is_re_armed_even_after_it_expired() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));
        granted_claim(&store, &claim_t1("R1"), 2_000);

        // The lease expired at 62_000, so ordinary renewal is refused...
        assert!(store.renew_lease("T1", "R1", 60_000, 90_000).is_err());
        // ...while adoption re-arms it, and renewal works again afterwards.
        assert_eq!(
            store.readopt_lease("T1", "R1", 60_000, 90_000).unwrap(),
            150_000
        );
        assert_eq!(
            store.renew_lease("T1", "R1", 60_000, 100_000).unwrap(),
            160_000
        );
    }

    #[test]
    fn a_settled_run_cannot_be_readopted() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));
        granted_claim(&store, &claim_t1("R1"), 2_000);
        settle_for_test(&store, "R1", Outcome::Failed, 3_000);

        let error = store.readopt_lease("T1", "R1", 60_000, 4_000).unwrap_err();
        assert!(matches!(error, StoreError::LeaseNotHeld { .. }));
    }

    #[test]
    fn ready_work_selection_is_deterministic_and_respects_filters() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));
        store
            .insert_local_ticket(
                "T0",
                "default",
                ".agents/sloop/tickets/t0.md",
                "Ticket zero",
                &[],
                "sloop/T0",
                None,
                None,
                None,
                "default",
                TicketState::Ready,
                2_000,
            )
            .unwrap();
        store
            .insert_activation(
                &NewActivation {
                    id: "A2",
                    kind: ActivationKind::Immediate,
                    ticket_id: None,
                    project_id: None,
                    eligible_at_ms: None,
                    interval_ms: None,
                },
                2_000,
            )
            .unwrap();
        let activation = QueuedActivation {
            id: "A2".into(),
            kind: "immediate".into(),
            ticket_id: None,
            project_id: None,
            eligible_at_ms: None,
            interval_ms: None,
        };

        // T1 was registered first, so it wins despite T0 sorting lower.
        assert_eq!(
            select_ready_ticket(&store, &activation, 2_000).as_deref(),
            Some("T1")
        );

        store.insert_activation_filter("A2", "T0").unwrap();
        assert_eq!(
            select_ready_ticket(&store, &activation, 2_000).as_deref(),
            Some("T0")
        );

        let scoped = QueuedActivation {
            project_id: Some("elsewhere".into()),
            ..activation
        };
        assert_eq!(select_ready_ticket(&store, &scoped, 2_000), None);
    }

    #[test]
    fn tickets_with_unmerged_blockers_are_never_selected() {
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
                Some("sonnet"),
                Some("medium"),
                "default",
                TicketState::Ready,
                1_500,
            )
            .unwrap();
        granted_claim(&store, &claim_t1("R1"), 2_000);

        let activation = QueuedActivation {
            id: "A1".into(),
            kind: "immediate".into(),
            ticket_id: None,
            project_id: None,
            eligible_at_ms: None,
            interval_ms: None,
        };
        // T1 is claimed and T2's blocker has not merged: nothing is ready.
        assert_eq!(select_ready_ticket(&store, &activation, 2_000), None);

        settle_for_test(&store, "R1", Outcome::Merged, 3_000);
        assert_eq!(
            select_ready_ticket(&store, &activation, 3_000).as_deref(),
            Some("T2")
        );
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
        assert_eq!(select_ready_ticket(&store, &activation, 2_000), None);
        assert_eq!(store.ticket("T1").unwrap().unwrap().attempts, 0);

        store.mark_ticket_missing("T1", 5_000).unwrap();
        assert_eq!(
            store.local_ticket_files().unwrap()[0].missing_at_ms,
            Some(2_000)
        );
        store.clear_ticket_missing("T1", 6_000).unwrap();
        assert_eq!(
            select_ready_ticket(&store, &activation, 6_000).as_deref(),
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
            select_ready_ticket(&store, &activation, 2_000).as_deref(),
            Some("T1")
        );
        assert_eq!(store.ticket_counts().unwrap().blocked, 1);

        store
            .db
            .lock()
            .execute("UPDATE tickets SET state = 'failed' WHERE id = 'T1'", [])
            .unwrap();
        assert_eq!(select_ready_ticket(&store, &activation, 2_000), None);
        assert_eq!(store.ticket("T2").unwrap().unwrap().attempts, 0);

        store
            .db
            .lock()
            .execute("UPDATE tickets SET state = 'merged' WHERE id = 'T1'", [])
            .unwrap();
        assert!(store.unmerged_blockers("T2").unwrap().is_empty());
        assert_eq!(
            select_ready_ticket(&store, &activation, 2_000).as_deref(),
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

        let store = LocalSqlite::from_db(Db::open(&path, 3_000).unwrap());
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

        let ticket = LocalSqlite::from_db(Db::open(&path, 3_000).unwrap())
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

        apply_reindex(
            &store,
            &[ticket(Some("flow `missing` is not defined"))],
            2_000,
        );
        assert_eq!(
            store.ticket("T1").unwrap().unwrap().held_reason.as_deref(),
            Some("flow `missing` is not defined")
        );
        apply_reindex(&store, &[ticket(None)], 2_100);
        assert_eq!(store.ticket_state("T1").unwrap().as_deref(), Some("ready"));

        store
            .set_ticket_hold("T1", TicketState::Held, 2_200)
            .unwrap();
        apply_reindex(&store, &[ticket(None)], 2_300);
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
        assert_eq!(granted_claim(&store, &claim_t1("R1"), 2_000), 1);
        settle_for_test(&store, "R1", Outcome::Failed, 2_100);

        assert_eq!(
            store.retry_ticket("T1", 2_200, |_, _, _| Ok(())).unwrap(),
            "failed"
        );
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
                &ClaimTransaction {
                    activation_id: "A2",
                    ..claim_t1("R2")
                },
                2_300,
            ),
            2
        );
        assert_eq!(store.ticket("T1").unwrap().unwrap().attempts, 1);
        assert!(matches!(
            store.retry_ticket("T1", 2_400, |_, _, _| Ok(())),
            Err(StoreError::TicketStateConflict { state, .. }) if state == "claimed"
        ));
        assert!(matches!(
            store.retry_ticket("missing", 2_400, |_, _, _| Ok(())),
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
