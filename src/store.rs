use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

pub use crate::db::SCHEMA_VERSION;
use crate::db::{Db, DbError};
use crate::domain::ticket::TicketState;
pub use crate::run_store::{
    ActiveRun, CooldownRecord, CooldownUpdate, EventRecord, EvidenceRecord, ProjectNote, RunRecord,
    RunState, RunTimeline, StageRecord,
};
pub(crate) use crate::run_store::{NeedsReviewBranch, RecoverableRun, WorktreeCleanupCandidate};
use crate::run_store::{RunStore, evidence, limits, runs};

impl RunState {
    /// Reads a state written by an older or newer binary. An unrecognized
    /// value is an error rather than a fallback: silently treating it as
    /// nonterminal would let the daemon act on a run it cannot classify.
    pub fn parse(value: &str) -> Result<Self, StoreError> {
        Self::from_stored(value).ok_or_else(|| StoreError::UnknownRunState {
            state: value.into(),
        })
    }
}

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
pub struct ClaimRequest<'a> {
    pub ticket_id: &'a str,
    pub run_id: &'a str,
    pub activation_id: &'a str,
    pub owner_id: &'a str,
    pub lease_ms: i64,
    pub next_activation_eligible_at_ms: Option<i64>,
    pub flow_json: &'a str,
    pub ticket_json: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedRun {
    pub run_id: String,
    pub attempt: i64,
    pub lease_expires_at_ms: i64,
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
pub struct QueuedActivation {
    pub id: String,
    pub kind: String,
    pub ticket_id: Option<String>,
    pub project_id: Option<String>,
    pub eligible_at_ms: Option<i64>,
    pub interval_ms: Option<i64>,
}

const TICKET_RECORD_SELECT: &str =
    "SELECT id, project_id, file_path, source, source_ref, state, name, worktree,
            target, model, effort, flow, attempts, body, held_reason, created_at_ms
     FROM tickets";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectRecord {
    pub id: String,
    pub file_path: Option<String>,
    pub title: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectCommitEvidence {
    pub run_id: String,
    pub ticket_id: String,
    pub data_json: String,
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

fn replace_ticket_blockers(
    transaction: &rusqlite::Transaction<'_>,
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

pub struct Store {
    db: Db,
}

impl Store {
    /// Opens (creating if needed) the database and migrates it to the current
    /// schema version. The daemon is the only writer; `now_ms` is injected so
    /// decision-adjacent timestamps never read the wall clock here.
    pub fn open(path: &Path, now_ms: i64) -> Result<Self, StoreError> {
        Db::open(path, now_ms)
            .map(Self::from_db)
            .map_err(StoreError::from)
    }

    pub fn from_db(db: Db) -> Self {
        Self { db }
    }

    pub fn db(&self) -> Db {
        self.db.clone()
    }

    fn run_store(&self) -> RunStore {
        RunStore::from_db(self.db.clone())
    }

    fn with_connection<T>(
        &self,
        operation: impl FnOnce(&mut Connection) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let mut connection = self.db.lock();
        operation(&mut connection)
    }

    pub fn insert_local_project(
        &self,
        id: &str,
        file_path: &str,
        title: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.with_connection(|connection| {
            connection.execute(
                "INSERT INTO projects (id, file_path, source, title, created_at_ms, updated_at_ms)
                 VALUES (?1, ?2, 'local', ?3, ?4, ?4)",
                params![id, file_path, title, now_ms],
            )?;
            Ok(())
        })
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
        self.with_connection(|connection| {
            connection.execute(
                "INSERT INTO projects (id, file_path, source, title, created_at_ms, updated_at_ms)
                 VALUES (?1, ?2, 'local', ?3, ?4, ?4)
                 ON CONFLICT(id) DO UPDATE SET
                     file_path = excluded.file_path,
                     title = excluded.title,
                     updated_at_ms = excluded.updated_at_ms",
                params![id, file_path, title, now_ms],
            )?;
            Ok(())
        })
    }

    pub fn project_exists(&self, id: &str) -> Result<bool, StoreError> {
        self.with_connection(|connection| {
            let found: Option<i64> = connection
                .query_row("SELECT 1 FROM projects WHERE id = ?1", params![id], |row| {
                    row.get(0)
                })
                .optional()?;
            Ok(found.is_some())
        })
    }

    pub fn project(&self, id: &str) -> Result<Option<ProjectRecord>, StoreError> {
        self.with_connection(|connection| {
            connection
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
        })
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
        replace_ticket_blockers(&transaction, id, blocked_by)?;
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
        replace_ticket_blockers(&transaction, id, blocked_by)?;
        transaction.commit()?;
        Ok(())
    }

    /// Applies a complete authored ticket snapshot without disturbing runtime
    /// history for IDs that remain present. Missing local rows and everything
    /// that depends on them are removed explicitly so the operation can report
    /// how much non-derivable state was discarded.
    pub fn apply_reindex(
        &self,
        project_ids: &[String],
        tickets: &[ReindexTicket],
        now_ms: i64,
    ) -> Result<ReindexResult, StoreError> {
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

        let mut doomed_runs = BTreeSet::new();
        for ticket_id in &stale_tickets {
            doomed_runs.extend(runs::tx::ids_for_ticket(&transaction, ticket_id)?);
        }
        for activation_id in &doomed_activations {
            doomed_runs.extend(runs::tx::ids_for_activation(&transaction, activation_id)?);
        }

        let mut rows_dropped = 0;
        for run_id in &doomed_runs {
            limits::tx::detach_cooldowns_from_run(&transaction, run_id)?;
            rows_dropped +=
                transaction.execute("DELETE FROM leases WHERE run_id = ?1", params![run_id])?;
            rows_dropped += evidence::tx::delete_for_run(&transaction, run_id)?;
            rows_dropped += limits::tx::delete_budget_reservation_for_run(&transaction, run_id)?;
            rows_dropped += runs::tx::delete_notes_for_run(&transaction, run_id)?;
            rows_dropped += runs::tx::delete(&transaction, run_id)?;
        }
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
                    runs::tx::mark_failed_or_review_runs_cleanup_eligible(
                        &transaction,
                        &ticket.id,
                        now_ms,
                    )?;
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
            replace_ticket_blockers(&transaction, &ticket.id, &ticket.blocked_by)?;
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

    /// Version-two rows predate target snapshots. Once a repository has a
    /// target configuration, persist its default before dispatch can observe
    /// those rows.
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

    /// Resolves a ticket by its human-facing name. Names are not guaranteed
    /// unique across projects, so the lowest id wins deterministically; `show`
    /// tries this only after an exact id match fails.
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

    /// Every ticket, newest registration first. `sloop list` answers "what is
    /// going on right now?", so recency leads; SQL settles the coarse order and
    /// a stable pass re-breaks ties on the id's numeric ordinal, which string
    /// comparison gets wrong (`TICK-9` sorts above `TICK-38`). Ids with no
    /// ordinal keep the deterministic `id DESC` order SQL gave them.
    pub fn tickets(&self) -> Result<Vec<TicketRecord>, StoreError> {
        self.with_connection(|connection| Self::tickets_on(connection))
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

    pub fn ticket_dependencies(
        &self,
    ) -> Result<std::collections::BTreeMap<String, Vec<String>>, StoreError> {
        let mut dependencies = std::collections::BTreeMap::new();
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

    /// Every ticket's blockers in one pass, keeping each list in declared
    /// order. Loading these per ticket turns any all-tickets read into a
    /// query per row, which is what the post path pays cycle checks against.
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

    /// Whether run history, a lease, an activation, or another ticket's
    /// blocker list still points at this row; deleting it would then violate
    /// a foreign key or orphan run evidence.
    pub fn ticket_is_referenced(&self, id: &str) -> Result<bool, StoreError> {
        self.run_store()
            .ticket_is_referenced(id)
            .map_err(StoreError::from)
    }

    pub fn delete_ticket(&self, id: &str) -> Result<(), StoreError> {
        self.db
            .lock()
            .execute("DELETE FROM tickets WHERE id = ?1", params![id])?;
        Ok(())
    }

    /// Stamps a ticket whose committed file has disappeared. The stamp keeps
    /// the row out of selection without disturbing its state; an existing
    /// stamp is preserved so the deletion clock starts at the first pass.
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
        self.with_connection(|connection| Self::ticket_state_on(connection, id))
    }

    fn ticket_state_on(connection: &Connection, id: &str) -> Result<Option<String>, StoreError> {
        let state = connection
            .query_row(
                "SELECT state FROM tickets WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(state)
    }

    /// Applies the operator-controlled ready/held side-state transition. The
    /// conditional update prevents an override from stealing a live claim or
    /// rewriting an evidence-derived outcome.
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

    /// Returns a failed ticket to the ready queue and starts its attempt
    /// counter over. Other states remain evidence-derived and immutable here.
    pub fn retry_ticket(&self, id: &str, now_ms: i64) -> Result<String, StoreError> {
        let mut connection = self.db.lock();
        let previous =
            Self::ticket_state_on(&connection, id)?.ok_or_else(|| StoreError::TicketNotFound {
                ticket_id: id.into(),
            })?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "UPDATE tickets SET state = 'ready', held_reason = NULL, attempts = 0, updated_at_ms = ?2
             WHERE id = ?1 AND state = 'failed'",
            params![id, now_ms],
        )?;
        if changed != 1 {
            return Err(StoreError::TicketStateConflict {
                ticket_id: id.into(),
                state: previous,
                requested: TicketState::Ready.as_str().into(),
            });
        }
        runs::tx::mark_ticket_runs_cleanup_eligible(&transaction, id, RunState::Failed, now_ms)?;
        transaction.commit()?;
        Ok(previous)
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

    /// Queued activations whose time gate is open, oldest first.
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

    /// Deterministic ready-work selection within an activation's scope:
    /// oldest registration first, ticket ID as the tiebreak. `--only` filters
    /// apply when the activation has filter rows.
    pub fn select_ready_ticket(
        &self,
        activation: &QueuedActivation,
        now_ms: i64,
    ) -> Result<Option<String>, StoreError> {
        self.run_store()
            .select_ready_ticket(activation.project_id.as_deref(), &activation.id, now_ms)
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

    /// Records a successful launch: the run turns `running` and carries the
    /// worktree, branch, and durable process identity.
    #[allow(clippy::too_many_arguments)]
    pub fn mark_run_running(
        &self,
        run_id: &str,
        branch: &str,
        worktree_path: &str,
        pid: u32,
        pid_start_time: Option<i64>,
        process_group_id: u32,
        worker_token: &str,
        worker_socket_path: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        let mut connection = self.db.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = runs::tx::mark_running(
            &transaction,
            run_id,
            branch,
            worktree_path,
            pid,
            pid_start_time,
            process_group_id,
            worker_token,
            worker_socket_path,
            now_ms,
        )?;
        if changed != 1 {
            let state = runs::tx::state(&transaction, run_id)?;
            return Err(StoreError::RunStateConflict {
                run_id: run_id.into(),
                state,
                requested: RunState::Running.as_str().into(),
            });
        }
        let ticket_id = runs::tx::ticket_id(&transaction, run_id)?;
        runs::tx::record_event(
            &transaction,
            now_ms,
            "run_started",
            Some(run_id),
            Some(&ticket_id),
            "{}",
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Terminates a run in one transaction: the raw exit and derived outcome
    /// land on the run, evidence is appended, the lease is
    /// freed, and the ticket moves to its terminal state or back to `ready`
    /// when cancellation or recovery releases it.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn finish_run(
        &mut self,
        run_id: &str,
        ticket_id: &str,
        exit_code: Option<i32>,
        outcome: crate::outcome::Outcome,
        evidence: &[EvidenceRecord],
        cooldown: Option<&CooldownUpdate<'_>>,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        use crate::outcome::Outcome;

        let mut connection = self.db.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let run_state = RunState::from(outcome);
        let changed = runs::tx::finish(&transaction, run_id, run_state, exit_code, now_ms)?;
        if changed == 0 {
            let existing = runs::tx::state_and_exit(&transaction, run_id)?;
            match existing {
                Some((_, Some(_))) => {
                    transaction.commit()?;
                    return Ok(false);
                }
                Some((state, None)) => {
                    return Err(StoreError::RunStateConflict {
                        run_id: run_id.into(),
                        state: Some(state),
                        requested: run_state.as_str().into(),
                    });
                }
                None => {
                    return Err(StoreError::RunNotFound {
                        run_id: run_id.into(),
                    });
                }
            }
        }
        transaction.execute("DELETE FROM leases WHERE run_id = ?1", params![run_id])?;

        let ticket_state = TicketState::after_outcome(outcome);
        transaction.execute(
            "UPDATE tickets SET state = ?2, held_reason = NULL, updated_at_ms = ?3
             WHERE id = ?1 AND state = 'claimed'",
            params![ticket_id, ticket_state.as_str(), now_ms],
        )?;
        if outcome == Outcome::RateLimited {
            let activation_id = runs::tx::activation_id(&transaction, run_id)?;
            transaction.execute(
                "UPDATE activations SET state = 'queued', updated_at_ms = ?2 WHERE id = ?1",
                params![activation_id, now_ms],
            )?;
        }

        if let Some(cooldown) = cooldown {
            limits::tx::upsert_cooldown(&transaction, run_id, cooldown, now_ms)?;
        }

        evidence::tx::record_settlement(&transaction, run_id, evidence, now_ms)?;
        runs::tx::record_event(
            &transaction,
            now_ms,
            "run_finished",
            Some(run_id),
            Some(ticket_id),
            &serde_json::json!({
                "outcome": outcome.as_str(),
                "exit_code": exit_code,
                "ticket_state": ticket_state.as_str(),
            })
            .to_string(),
        )?;
        transaction.commit()?;
        Ok(true)
    }

    /// Records one completed flow stage. The flow index is the idempotency
    /// key, so recovery can re-derive the first stage still lacking a verdict.
    pub(crate) fn record_aftercare_stage(
        &self,
        run_id: &str,
        stage: &StageRecord,
    ) -> Result<(), StoreError> {
        self.run_store()
            .record_aftercare_stage(run_id, stage)
            .map_err(StoreError::from)
    }

    pub(crate) fn aftercare_stages(&self, run_id: &str) -> Result<Vec<StageRecord>, StoreError> {
        self.run_store()
            .aftercare_stages(run_id)
            .map_err(StoreError::from)
    }

    /// Checkpoints the agent's exit before aftercare starts. The lease and
    /// ticket remain claimed until final settlement, but recovery can now
    /// resume with the exact exit and branch-activity facts. Only the caller that
    /// wins this transition owns exit processing and aftercare for the run.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_agent_exit(
        &mut self,
        run_id: &str,
        exit_code: Option<i32>,
        capture_complete: bool,
        commits_json: &str,
        vendor_error: Option<&crate::vendor_error::VendorErrorMatch>,
        cooldown_until_ms: Option<i64>,
        now_ms: i64,
    ) -> Result<ExitClaim, StoreError> {
        let mut connection = self.db.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = runs::tx::claim_agent_exit(&transaction, run_id, exit_code, now_ms)?;
        if changed == 0 {
            let state = runs::tx::state(&transaction, run_id)?;
            return match state {
                Some(state) => Ok(ExitClaim::AlreadyClaimed { state }),
                None => Err(StoreError::RunNotFound {
                    run_id: run_id.into(),
                }),
            };
        }
        evidence::tx::record_agent_exit(
            &transaction,
            run_id,
            exit_code,
            capture_complete,
            commits_json,
            vendor_error,
            cooldown_until_ms,
            now_ms,
        )?;
        transaction.commit()?;
        Ok(ExitClaim::Claimed)
    }

    pub(crate) fn record_aftercare_evidence(
        &self,
        run_id: &str,
        kind: &str,
        data_json: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.run_store()
            .record_aftercare_evidence(run_id, kind, data_json, now_ms)
            .map_err(StoreError::from)
    }

    pub(crate) fn clear_aftercare_process(&self, run_id: &str) -> Result<(), StoreError> {
        self.run_store()
            .clear_aftercare_process(run_id)
            .map_err(StoreError::from)
    }

    /// Durably records an operator's cancellation intent, idempotently: the
    /// dedupe key makes a repeated `cancel` a no-op rather than new evidence.
    pub fn record_cancel_requested(&self, run_id: &str, now_ms: i64) -> Result<(), StoreError> {
        self.run_store()
            .record_cancel_requested(run_id, now_ms)
            .map_err(StoreError::from)
    }

    /// Whether cancellation intent was recorded for the run, so an exit event
    /// racing the cancel still resolves to `Cancelled`.
    pub fn cancellation_requested(&self, run_id: &str) -> Result<bool, StoreError> {
        self.run_store()
            .cancellation_requested(run_id)
            .map_err(StoreError::from)
    }

    /// Appends a worker's advisory note. The agent's only write: it records
    /// text against the run and moves nothing.
    pub fn insert_note(
        &self,
        id: &str,
        run_id: &str,
        text: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.run_store()
            .insert_note(id, run_id, text, now_ms)
            .map_err(StoreError::from)
    }

    /// Notes recorded against one run, in the order they arrived.
    pub fn notes_for_run(&self, run_id: &str) -> Result<Vec<String>, StoreError> {
        self.run_store()
            .notes_for_run(run_id)
            .map_err(StoreError::from)
    }

    /// Records the first worker-reported verdict for one stage. The unique
    /// dedupe key is the at-most-once gate; later reports cannot overwrite it.
    pub(crate) fn record_stage_verdict(
        &self,
        run_id: &str,
        stage: &str,
        verdict: &str,
        reason: Option<&str>,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        self.run_store()
            .record_stage_verdict(run_id, stage, verdict, reason, now_ms)
            .map_err(StoreError::from)
    }

    pub fn notes_for_project(&self, project_id: &str) -> Result<Vec<ProjectNote>, StoreError> {
        self.run_store()
            .notes_for_project(project_id)
            .map_err(StoreError::from)
    }

    pub fn commit_evidence_for_project(
        &self,
        project_id: &str,
    ) -> Result<Vec<ProjectCommitEvidence>, StoreError> {
        self.run_store()
            .commit_evidence_for_project(project_id)?
            .into_iter()
            .map(|(run_id, ticket_id, data_json)| {
                Ok(ProjectCommitEvidence {
                    run_id,
                    ticket_id,
                    data_json,
                })
            })
            .collect()
    }

    pub fn next_note_ordinal(&self) -> Result<i64, StoreError> {
        self.run_store()
            .next_note_ordinal()
            .map_err(StoreError::from)
    }

    /// Evidence rows for one run in observation order, as (kind, data_json).
    pub fn run_evidence(&self, run_id: &str) -> Result<Vec<(String, String)>, StoreError> {
        self.run_store()
            .run_evidence(run_id)
            .map_err(StoreError::from)
    }

    pub fn vendor_error_for_run(
        &self,
        run_id: &str,
    ) -> Result<Option<crate::vendor_error::VendorErrorMatch>, StoreError> {
        let data = self.run_store().vendor_error_for_run(run_id)?;
        Ok(data.and_then(|data| serde_json::from_str(&data).ok()))
    }

    pub fn latest_vendor_error_for_ticket(
        &self,
        ticket_id: &str,
    ) -> Result<Option<crate::vendor_error::VendorErrorMatch>, StoreError> {
        let data = self.run_store().latest_vendor_error_for_ticket(ticket_id)?;
        Ok(data.and_then(|data| serde_json::from_str(&data).ok()))
    }

    /// Rolls back a claim whose launch failed before a process existed: the
    /// lease is released, the run is closed, and the ticket returns to
    /// `ready`. The consumed attempt is kept as evidence of the try.
    pub(crate) fn abort_claim(
        &mut self,
        run_id: &str,
        ticket_id: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        let mut connection = self.db.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute("DELETE FROM leases WHERE run_id = ?1", params![run_id])?;
        runs::tx::abort(&transaction, run_id, now_ms)?;
        transaction.execute(
            "UPDATE tickets SET state = 'ready', held_reason = NULL, updated_at_ms = ?2
             WHERE id = ?1 AND state = 'claimed'",
            params![ticket_id, now_ms],
        )?;
        runs::tx::record_event(
            &transaction,
            now_ms,
            "run_aborted",
            Some(run_id),
            Some(ticket_id),
            "{}",
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Reads activity-feed rows with `sequence > after`, oldest first. The
    /// last row's sequence is the caller's next cursor.
    pub fn events_after(&self, after: i64, limit: usize) -> Result<Vec<EventRecord>, StoreError> {
        self.run_store()
            .events_after(after, limit)
            .map_err(StoreError::from)
    }

    /// The claim/start/finish instants of each named run, read back out of the
    /// activity feed. The feed is written in the same transaction as the
    /// transition it narrates, so these are the authoritative wall-clock
    /// boundaries of a run — nothing extra is stored to render them.
    ///
    /// A run with no finish row is still in flight; callers render it
    /// open-ended rather than inventing an end. `run_aborted` counts as a
    /// finish because it is the terminal row for runs that never settled.
    pub fn run_timelines(
        &self,
        run_ids: &[&str],
    ) -> Result<std::collections::HashMap<String, RunTimeline>, StoreError> {
        self.run_store()
            .run_timelines(run_ids)
            .map_err(StoreError::from)
    }

    pub fn latest_event_sequence(&self) -> Result<i64, StoreError> {
        self.run_store()
            .latest_event_sequence()
            .map_err(StoreError::from)
    }

    /// Drops all but the newest `keep` activity-feed rows. Sequences are never
    /// reused after a trim, so cursors held by watchers stay valid.
    pub fn trim_events(&self, keep: i64) -> Result<(), StoreError> {
        self.run_store().trim_events(keep).map_err(StoreError::from)
    }

    pub fn run(&self, id: &str) -> Result<Option<RunRecord>, StoreError> {
        self.run_store().run(id).map_err(StoreError::from)
    }

    /// The run a `<ticket>-r<attempt>` alias names. The pair is unique because
    /// attempts are allocated once per ticket at claim time.
    pub fn run_for_ticket_attempt(
        &self,
        ticket_id: &str,
        attempt: i64,
    ) -> Result<Option<RunRecord>, StoreError> {
        self.run_store()
            .run_for_ticket_attempt(ticket_id, attempt)
            .map_err(StoreError::from)
    }

    /// Every run a ticket has produced, newest attempt first, so a bare ticket
    /// reference can name the latest run and still report the earlier ones.
    pub fn runs_for_ticket(&self, ticket_id: &str) -> Result<Vec<RunRecord>, StoreError> {
        self.run_store()
            .runs_for_ticket(ticket_id)
            .map_err(StoreError::from)
    }

    /// Runs whose internal id starts with `prefix`. More than one row means the
    /// reference is ambiguous, so the caller needs the candidates, not a pick.
    pub fn runs_with_id_prefix(&self, prefix: &str) -> Result<Vec<RunRecord>, StoreError> {
        // `LIKE` would treat `%` and `_` in a prefix as wildcards; run ids are
        // hexadecimal, but comparing on the substring keeps that beyond doubt.
        self.run_store()
            .runs_with_id_prefix(prefix)
            .map_err(StoreError::from)
    }

    /// Every `needs_review` ticket paired with the branch of the run that
    /// produced it, so the daemon can freshly test each branch tip for external
    /// integration. Only the newest `needs_review` run with a branch is
    /// returned per ticket; the tip itself is never cached here.
    pub(crate) fn needs_review_branches(&self) -> Result<Vec<NeedsReviewBranch>, StoreError> {
        self.run_store()
            .needs_review_branches()
            .map_err(StoreError::from)
    }

    pub(crate) fn worktree_cleanup_candidates(
        &self,
    ) -> Result<Vec<WorktreeCleanupCandidate>, StoreError> {
        self.run_store()
            .worktree_cleanup_candidates()
            .map_err(StoreError::from)
    }

    pub(crate) fn next_worktree_cleanup_at_ms(
        &self,
        retention_ms: i64,
        now_ms: i64,
    ) -> Result<Option<i64>, StoreError> {
        self.run_store()
            .next_worktree_cleanup_at_ms(retention_ms, now_ms)
            .map_err(StoreError::from)
    }

    pub(crate) fn mark_run_worktree_cleaned(
        &self,
        candidate: &WorktreeCleanupCandidate,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        self.run_store()
            .mark_run_worktree_cleaned(candidate, now_ms)
            .map_err(StoreError::from)
    }

    /// Settles a `needs_review` ticket whose run branch an operator merged by
    /// hand: the ticket becomes `merged`, releasing its `blocked_by` dependents
    /// exactly as a flow merge would, and the observation is recorded as
    /// evidence. The ticket-state gate makes a repeated pass a no-op and the
    /// `dedupe_key` UNIQUE gate keeps the evidence row unique across restarts.
    /// Returns whether this call performed the transition.
    pub(crate) fn settle_external_merge(
        &mut self,
        run_id: &str,
        ticket_id: &str,
        branch: &str,
        branch_tip: &str,
        observed_default_tip: &str,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        let mut connection = self.db.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "UPDATE tickets SET state = 'merged', held_reason = NULL, updated_at_ms = ?2
             WHERE id = ?1 AND state = 'needs_review'",
            params![ticket_id, now_ms],
        )?;
        if changed == 0 {
            transaction.commit()?;
            return Ok(false);
        }
        runs::tx::mark_cleanup_eligible(&transaction, run_id, now_ms)?;
        let data_json = serde_json::json!({
            "branch": branch,
            "branch_tip": branch_tip,
            "observed_default_tip": observed_default_tip,
        })
        .to_string();
        evidence::tx::record_external_merge(&transaction, run_id, &data_json, now_ms)?;
        runs::tx::record_event(
            &transaction,
            now_ms,
            "external_merge_reconciled",
            Some(run_id),
            Some(ticket_id),
            &data_json,
        )?;
        transaction.commit()?;
        Ok(true)
    }

    /// The ticket's live run as `(id, attempt)`. The attempt travels with the
    /// id so callers can name the run by alias without re-reading the row.
    pub fn active_run_for_ticket(
        &self,
        ticket_id: &str,
    ) -> Result<Option<(String, i64)>, StoreError> {
        self.run_store()
            .active_run_for_ticket(ticket_id)
            .map_err(StoreError::from)
    }

    /// Leased nonterminal runs that consume capacity, oldest first.
    pub fn active_runs(&self) -> Result<Vec<ActiveRun>, StoreError> {
        self.run_store().active_runs().map_err(StoreError::from)
    }

    /// Every nonterminal run that still owns a lease, oldest first. Startup
    /// must classify all of these before making another spawn decision.
    pub(crate) fn recoverable_runs(&self) -> Result<Vec<RecoverableRun>, StoreError> {
        self.run_store()
            .recoverable_runs()
            .map_err(StoreError::from)
    }

    /// Finds a still-queued activation of `kind` scoped to one ticket, used
    /// to keep reposting the same file idempotent.
    pub fn queued_ticket_activation(
        &self,
        ticket_id: &str,
        kind: ActivationKind,
    ) -> Result<Option<String>, StoreError> {
        let id = self
            .db
            .lock()
            .query_row(
                "SELECT id FROM activations
                 WHERE ticket_id = ?1 AND kind = ?2 AND state = 'queued'
                 ORDER BY created_at_ms LIMIT 1",
                params![ticket_id, kind.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        Ok(id)
    }

    /// Moves a still-queued timed activation to a new eligibility instant,
    /// so reposting a ticket with a different `--at` time reschedules the
    /// existing activation instead of queueing a duplicate.
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

    /// Reserves the next activation ordinal without reusing IDs removed by
    /// reindex.
    pub fn next_activation_ordinal(&self) -> Result<i64, StoreError> {
        self.run_store()
            .next_activation_ordinal()
            .map_err(StoreError::from)
    }

    /// Claims a ready ticket for one run in a single transaction. The
    /// conditional update plus the primary key on `leases.ticket_id` are the
    /// durable guards against a double claim.
    pub(crate) fn claim_ticket(
        &mut self,
        claim: &ClaimRequest<'_>,
        now_ms: i64,
    ) -> Result<ClaimedRun, StoreError> {
        let mut connection = self.db.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;

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

        // The run's attempt counts runs, not the ticket's retry budget:
        // `retry` resets `tickets.attempts`, and a reused number would make two
        // runs answer to the same alias. Allocating inside the claim
        // transaction keeps the sequence gap-free under concurrent claims.
        let attempt = runs::tx::next_attempt(&transaction, claim.ticket_id)?;

        runs::tx::insert_claimed(
            &transaction,
            claim.run_id,
            claim.activation_id,
            claim.ticket_id,
            attempt,
            claim.flow_json,
            claim.ticket_json,
            now_ms,
        )?;

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
        runs::tx::record_event(
            &transaction,
            now_ms,
            "run_claimed",
            Some(claim.run_id),
            Some(claim.ticket_id),
            &serde_json::json!({"attempt": attempt}).to_string(),
        )?;

        transaction.commit()?;
        Ok(ClaimedRun {
            run_id: claim.run_id.into(),
            attempt,
            lease_expires_at_ms: expires_at_ms,
        })
    }

    /// Re-arms the lease of a run this daemon has just adopted, returning the
    /// new expiry. Unlike [`Store::renew_lease`] this accepts an already
    /// expired lease: a daemon down longer than the TTL comes back to leases
    /// that lapsed while nobody was renewing them, and ordinary renewal could
    /// never lift them again. It is not a weaker renewal — the guard moves
    /// from the clock to the run itself, so only a run that has not settled
    /// can be re-armed, and a dead run's lease stays expired because recovery
    /// settles it instead of adopting it.
    pub(crate) fn readopt_lease(
        &mut self,
        ticket_id: &str,
        run_id: &str,
        lease_ms: i64,
        now_ms: i64,
    ) -> Result<i64, StoreError> {
        let expires_at_ms = now_ms + lease_ms;
        let changed = self
            .run_store()
            .readopt_lease(ticket_id, run_id, now_ms, expires_at_ms)?;
        if changed != 1 {
            return Err(StoreError::LeaseNotHeld {
                ticket_id: ticket_id.into(),
                run_id: run_id.into(),
            });
        }
        Ok(expires_at_ms)
    }

    /// Renews the lease that `run_id` holds on `ticket_id`, returning the new
    /// expiry. Renewal is strict: an expired lease cannot be renewed, so once
    /// recovery treats expiry as "run is lost" a revived run can never
    /// resurrect a lease that recovery may be reclaiming. Re-arming an expired
    /// lease is a separate, adoption-only verb ([`Store::readopt_lease`]).
    pub(crate) fn renew_lease(
        &mut self,
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

    pub fn paused(&self) -> Result<bool, StoreError> {
        self.run_store().paused().map_err(StoreError::from)
    }

    pub fn clear_restart_draining(&self, now_ms: i64) -> Result<(), StoreError> {
        self.run_store()
            .clear_restart_draining(now_ms)
            .map_err(StoreError::from)
    }

    pub fn restart_draining(&self) -> Result<bool, StoreError> {
        self.run_store()
            .restart_draining()
            .map_err(StoreError::from)
    }

    pub fn active_cooldown_for_target(
        &self,
        target: &str,
        now_ms: i64,
    ) -> Result<Option<CooldownRecord>, StoreError> {
        self.run_store()
            .active_cooldown_for_target(target, now_ms)
            .map_err(StoreError::from)
    }

    /// Number of runs currently holding a durable lease. Used as the capacity
    /// gate for repair spawns, which run inside an already-leased run.
    pub(crate) fn active_lease_count(&self) -> Result<usize, StoreError> {
        let count: i64 = self
            .db
            .lock()
            .query_row("SELECT COUNT(*) FROM leases", [], |row| row.get(0))?;
        Ok(count as usize)
    }

    /// Persists one repair attempt for a stage. The dedupe key is per
    /// (run, stage, attempt), so recovery counts consumed attempts without
    /// repeating or losing one, and the retry verdict can be filled in later
    /// by upserting the same key.
    pub(crate) fn record_repair_attempt(
        &self,
        run_id: &str,
        stage: &str,
        attempt: u32,
        data_json: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.run_store()
            .record_repair_attempt(run_id, stage, attempt, data_json, now_ms)
            .map_err(StoreError::from)
    }

    pub fn active_cooldowns(&self, now_ms: i64) -> Result<Vec<CooldownRecord>, StoreError> {
        self.run_store()
            .active_cooldowns(now_ms)
            .map_err(StoreError::from)
    }

    pub fn next_active_cooldown(&self, now_ms: i64) -> Result<Option<i64>, StoreError> {
        self.run_store()
            .next_active_cooldown(now_ms)
            .map_err(StoreError::from)
    }

    pub fn set_paused(&self, paused: bool, now_ms: i64) -> Result<(), StoreError> {
        self.run_store()
            .set_paused(paused, now_ms)
            .map_err(StoreError::from)
    }

    pub fn begin_restart_draining(
        &mut self,
        active_runs: usize,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        self.run_store()
            .begin_restart_draining(active_runs, now_ms)
            .map_err(StoreError::from)
    }

    /// Resuming cancels both scheduler holds in one durable transition.
    pub fn resume_scheduler(&mut self, now_ms: i64) -> Result<bool, StoreError> {
        self.run_store()
            .resume_scheduler(now_ms)
            .map_err(StoreError::from)
    }

    /// Performs a small committed write used to detect when SQLite can make
    /// progress again after returning `SQLITE_FULL`.
    pub(crate) fn probe_writable(&self, now_ms: i64) -> Result<(), StoreError> {
        self.run_store()
            .probe_writable(now_ms)
            .map_err(StoreError::from)
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

/// Whether the caller won the `running` → `aftercare` transition and with it
/// ownership of exit processing and aftercare for the run.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ExitClaim {
    Claimed,
    AlreadyClaimed { state: String },
}

#[derive(Debug)]
pub enum StoreError {
    Open {
        path: PathBuf,
        source: rusqlite::Error,
    },
    Sqlite(rusqlite::Error),
    UnsupportedSchemaVersion(u32),
    TicketNotReady {
        ticket_id: String,
        state: Option<String>,
    },
    TicketNotFound {
        ticket_id: String,
    },
    TicketStateConflict {
        ticket_id: String,
        state: String,
        requested: String,
    },
    ActivationNotQueued {
        activation_id: String,
    },
    LeaseNotHeld {
        ticket_id: String,
        run_id: String,
    },
    RunNotFound {
        run_id: String,
    },
    RunStateConflict {
        run_id: String,
        state: Option<String>,
        requested: String,
    },
    UnknownRunState {
        state: String,
    },
}

impl StoreError {
    pub(crate) fn is_disk_full(&self) -> bool {
        let source = match self {
            Self::Open { source, .. } | Self::Sqlite(source) => source,
            _ => return false,
        };
        matches!(
            source,
            rusqlite::Error::SqliteFailure(error, _)
                if error.code == rusqlite::ffi::ErrorCode::DiskFull
        )
    }
}

impl From<rusqlite::Error> for StoreError {
    fn from(source: rusqlite::Error) -> Self {
        Self::Sqlite(source)
    }
}

impl From<DbError> for StoreError {
    fn from(source: DbError) -> Self {
        match source {
            DbError::Open { path, source } => Self::Open { path, source },
            DbError::Sqlite(source) => Self::Sqlite(source),
            DbError::UnsupportedSchemaVersion(version) => Self::UnsupportedSchemaVersion(version),
        }
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open { path, source } => {
                write!(formatter, "cannot open {}: {source}", path.display())
            }
            Self::Sqlite(source) => write!(formatter, "database error: {source}"),
            Self::UnsupportedSchemaVersion(version) => {
                write!(formatter, "unsupported database schema version {version}")
            }
            Self::TicketNotReady { ticket_id, state } => match state {
                Some(state) => write!(formatter, "ticket `{ticket_id}` is `{state}`, not `ready`"),
                None => write!(formatter, "ticket `{ticket_id}` does not exist"),
            },
            Self::TicketNotFound { ticket_id } => {
                write!(formatter, "ticket `{ticket_id}` does not exist")
            }
            Self::TicketStateConflict {
                ticket_id,
                state,
                requested,
            } => write!(
                formatter,
                "ticket `{ticket_id}` is `{state}` and cannot be changed to `{requested}`"
            ),
            Self::ActivationNotQueued { activation_id } => write!(
                formatter,
                "activation `{activation_id}` is not queued for dispatch"
            ),
            Self::LeaseNotHeld { ticket_id, run_id } => write!(
                formatter,
                "run `{run_id}` does not hold the lease on ticket `{ticket_id}`"
            ),
            Self::RunNotFound { run_id } => write!(formatter, "run `{run_id}` does not exist"),
            Self::RunStateConflict {
                run_id,
                state,
                requested,
            } => match state {
                Some(state) => write!(
                    formatter,
                    "run `{run_id}` is `{state}` and cannot be changed to `{requested}`"
                ),
                None => write!(formatter, "run `{run_id}` does not exist"),
            },
            Self::UnknownRunState { state } => {
                write!(formatter, "unrecognized run state `{state}`")
            }
        }
    }
}

impl std::error::Error for StoreError {}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{ActivationKind, ClaimRequest, NewActivation, ReindexTicket, Store, StoreError};
    use crate::domain::ticket::TicketState;

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

    #[test]
    fn sqlite_full_errors_are_classified_for_backpressure() {
        let sqlite = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_FULL),
            None,
        );
        assert!(StoreError::from(sqlite).is_disk_full());
        assert!(
            !StoreError::TicketNotFound {
                ticket_id: "T1".into()
            }
            .is_disk_full()
        );
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

    #[test]
    fn missing_tickets_are_not_selected_and_cannot_be_claimed() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        store.mark_ticket_missing("T1", 2_000).unwrap();

        let activation = super::QueuedActivation {
            id: "A1".into(),
            kind: "immediate".into(),
            ticket_id: None,
            project_id: None,
            eligible_at_ms: None,
            interval_ms: None,
        };
        assert_eq!(store.select_ready_ticket(&activation, 2_000).unwrap(), None);
        match store.claim_ticket(&claim_t1("R1"), 2_000).unwrap_err() {
            StoreError::TicketNotReady { state, .. } => {
                assert_eq!(state.as_deref(), Some("missing"));
            }
            other => panic!("unexpected error: {other:?}"),
        }

        // A second stamp must not restart the deletion clock.
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
        let mut store = open_seeded(&directory.path().join("sloop.db"));
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
        let activation = super::QueuedActivation {
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
            .db
            .lock()
            .execute("UPDATE tickets SET state = 'failed' WHERE id = 'T1'", [])
            .unwrap();
        assert_eq!(store.select_ready_ticket(&activation, 2_000).unwrap(), None);
        match store
            .claim_ticket(
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
            .unwrap_err()
        {
            StoreError::TicketNotReady { state, .. } => {
                assert_eq!(state.as_deref(), Some("blocked"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert_eq!(store.ticket("T2").unwrap().unwrap().attempts, 0);

        store
            .db
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

        let mut store = open_seeded(&path);
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();
        drop(store);

        let store = Store::open(&path, 3_000).unwrap();
        assert_eq!(store.ticket_state("T1").unwrap().unwrap(), "claimed");
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

        let store = Store::open(&path, 3_000).unwrap();
        let ticket = store.ticket("T2").unwrap().unwrap();
        assert_eq!(ticket.name, "Ticket two");
        assert_eq!(ticket.blocked_by, ["T1"]);
        assert_eq!(ticket.worktree.as_deref(), Some("feature/t2"));
    }

    #[test]
    fn a_claimed_ticket_cannot_be_claimed_again() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));

        let claimed = store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();
        assert_eq!(claimed.attempt, 1);
        assert_eq!(claimed.lease_expires_at_ms, 62_000);

        let error = store.claim_ticket(&claim_t1("R2"), 2_100).unwrap_err();
        assert!(matches!(
            error,
            StoreError::TicketNotReady { state: Some(ref state), .. } if state == "claimed"
        ));
    }

    #[test]
    fn tickets_are_ordered_newest_first_and_include_attempts() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        store
            .insert_local_project("alpha", ".agents/sloop/projects/alpha.md", "Alpha", 1_000)
            .unwrap();
        store
            .insert_local_ticket(
                "T0",
                "alpha",
                ".agents/sloop/tickets/t0.md",
                "Ticket zero",
                &[],
                "sloop/T0",
                None,
                None,
                None,
                "default",
                TicketState::Held,
                3_000,
            )
            .unwrap();
        store
            .insert_local_ticket(
                "T2",
                "default",
                ".agents/sloop/tickets/t2.md",
                "Ticket two",
                &[],
                "sloop/T2",
                None,
                None,
                None,
                "default",
                TicketState::Ready,
                1_000,
            )
            .unwrap();
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();

        let tickets = store.tickets().unwrap();
        // T0 registered last, so it leads despite the lowest ordinal and a
        // different project; T2 and T1 tie on time and fall back to ordinal.
        assert_eq!(
            tickets
                .iter()
                .map(|ticket| ticket.id.as_str())
                .collect::<Vec<_>>(),
            ["T0", "T2", "T1"]
        );
        assert_eq!(tickets[0].attempts, 0);
        assert_eq!(tickets[1].attempts, 0);
        assert_eq!(tickets[2].attempts, 1);
    }

    #[test]
    fn aborted_claims_are_closed_and_no_longer_active() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();

        store.abort_claim("R1", "T1", 2_100).unwrap();

        assert_eq!(store.run("R1").unwrap().unwrap().state, "aborted");
        assert_eq!(store.active_run_for_ticket("T1").unwrap(), None);
        assert_eq!(store.ticket_state("T1").unwrap().as_deref(), Some("ready"));
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
        let held = store.ticket("T1").unwrap().unwrap();
        assert_eq!(held.state, "held");
        assert_eq!(
            held.held_reason.as_deref(),
            Some("flow `missing` is not defined")
        );

        store
            .apply_reindex(&["default".into()], &[ticket(None)], 2_100)
            .unwrap();
        let released = store.ticket("T1").unwrap().unwrap();
        assert_eq!(released.state, "ready");
        assert_eq!(released.held_reason, None);

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
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();

        assert!(matches!(
            store.set_ticket_hold("T1", TicketState::Held, 2_100),
            Err(StoreError::TicketStateConflict { state, .. }) if state == "claimed"
        ));
    }

    #[test]
    fn retry_only_requeues_failed_tickets_and_resets_attempts() {
        use crate::outcome::Outcome;

        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));

        let first = store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();
        assert_eq!(first.attempt, 1);
        store
            .finish_run("R1", "T1", Some(0), Outcome::Failed, &[], None, 2_100)
            .unwrap();

        assert_eq!(store.retry_ticket("T1", 2_200).unwrap(), "failed");
        assert_eq!(store.ticket_state("T1").unwrap().as_deref(), Some("ready"));
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
        let retried = store
            .claim_ticket(
                &ClaimRequest {
                    activation_id: "A2",
                    ..claim_t1("R2")
                },
                2_300,
            )
            .unwrap();
        // `retry` resets the ticket's attempt budget, but a run's attempt
        // counts runs of that ticket: it must keep climbing, or two runs would
        // answer to the same `T1-r1` alias.
        assert_eq!(retried.attempt, 2);
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
    fn claiming_an_unknown_ticket_reports_it_missing() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));

        let error = store
            .claim_ticket(
                &ClaimRequest {
                    ticket_id: "missing",
                    ..claim_t1("R1")
                },
                2_000,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            StoreError::TicketNotReady { state: None, .. }
        ));
    }

    #[test]
    fn concurrent_connections_cannot_both_claim_one_ticket() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sloop.db");
        open_seeded(&path);

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let claims: Vec<_> = ["R1", "R2"]
            .into_iter()
            .map(|run_id| {
                let path = path.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let mut store = Store::open(&path, 2_000).unwrap();
                    barrier.wait();
                    store.claim_ticket(&claim_t1(run_id), 2_000).is_ok()
                })
            })
            .collect();

        let successes = claims
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .filter(|claimed| *claimed)
            .count();
        assert_eq!(successes, 1);
    }

    #[test]
    fn renewing_a_held_lease_extends_its_expiry() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();

        let expires = store.renew_lease("T1", "R1", 60_000, 10_000).unwrap();
        assert_eq!(expires, 70_000);
    }

    #[test]
    fn a_run_cannot_renew_a_lease_it_does_not_hold() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();

        let error = store.renew_lease("T1", "R2", 60_000, 10_000).unwrap_err();
        assert!(matches!(error, StoreError::LeaseNotHeld { .. }));
    }

    #[test]
    fn an_expired_lease_cannot_be_renewed() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();

        // The lease expires at 62_000; renewal at or after that must fail.
        let error = store.renew_lease("T1", "R1", 60_000, 62_000).unwrap_err();
        assert!(matches!(error, StoreError::LeaseNotHeld { .. }));
    }

    #[test]
    fn a_readopted_lease_is_re_armed_even_after_it_expired() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();

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
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();
        store
            .finish_run(
                "R1",
                "T1",
                Some(0),
                crate::outcome::Outcome::Failed,
                &[],
                None,
                3_000,
            )
            .unwrap();

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
        let activation = super::QueuedActivation {
            id: "A2".into(),
            kind: "immediate".into(),
            ticket_id: None,
            project_id: None,
            eligible_at_ms: None,
            interval_ms: None,
        };

        // T1 was registered first, so it wins despite T0 sorting lower.
        assert_eq!(
            store
                .select_ready_ticket(&activation, 2_000)
                .unwrap()
                .as_deref(),
            Some("T1")
        );

        store.insert_activation_filter("A2", "T0").unwrap();
        assert_eq!(
            store
                .select_ready_ticket(&activation, 2_000)
                .unwrap()
                .as_deref(),
            Some("T0")
        );

        let scoped = super::QueuedActivation {
            project_id: Some("elsewhere".into()),
            ..activation
        };
        assert_eq!(store.select_ready_ticket(&scoped, 2_000).unwrap(), None);
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
        assert_eq!(
            store.ticket("T1").unwrap().unwrap().target.as_deref(),
            Some("codex")
        );
    }

    #[test]
    fn tickets_with_unmerged_blockers_are_never_selected() {
        use crate::outcome::Outcome;
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
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
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();

        let activation = super::QueuedActivation {
            id: "A1".into(),
            kind: "immediate".into(),
            ticket_id: None,
            project_id: None,
            eligible_at_ms: None,
            interval_ms: None,
        };
        // T1 is claimed and T2's blocker has not merged: nothing is ready.
        assert_eq!(store.select_ready_ticket(&activation, 2_000).unwrap(), None);

        store
            .finish_run("R1", "T1", Some(0), Outcome::Merged, &[], None, 3_000)
            .unwrap();
        assert_eq!(
            store
                .select_ready_ticket(&activation, 3_000)
                .unwrap()
                .as_deref(),
            Some("T2")
        );
    }

    #[test]
    fn orphaning_a_run_releases_the_ticket_without_failing_it() {
        use crate::outcome::Outcome;
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();

        store
            .finish_run("R1", "T1", None, Outcome::Orphaned, &[], None, 3_000)
            .unwrap();

        assert_eq!(store.run("R1").unwrap().unwrap().state, "orphaned");
        assert_eq!(store.ticket_state("T1").unwrap().as_deref(), Some("ready"));
    }
}
