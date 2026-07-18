use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

pub const SCHEMA_VERSION: u32 = 8;

const CONNECTION_PRAGMAS: &str = "
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 5000;
";

const SCHEMA_V1: &str = "
CREATE TABLE projects (
    id              TEXT PRIMARY KEY,
    file_path       TEXT UNIQUE,
    source          TEXT NOT NULL DEFAULT 'local',
    source_ref      TEXT,
    title           TEXT NOT NULL,
    created_at_ms   INTEGER NOT NULL,
    updated_at_ms   INTEGER NOT NULL,
    UNIQUE (source, source_ref),
    CHECK (file_path IS NOT NULL OR source_ref IS NOT NULL)
);

CREATE TABLE tickets (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL REFERENCES projects(id),
    file_path       TEXT UNIQUE,
    source          TEXT NOT NULL DEFAULT 'local',
    source_ref      TEXT,
    state           TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    content_hash    TEXT,
    name            TEXT NOT NULL DEFAULT '',
    worktree        TEXT,
    target          TEXT,
    model           TEXT,
    effort          TEXT,
    flow            TEXT,
    missing_at_ms   INTEGER,
    created_at_ms   INTEGER NOT NULL,
    updated_at_ms   INTEGER NOT NULL,
    UNIQUE (source, source_ref),
    CHECK (file_path IS NOT NULL OR source_ref IS NOT NULL)
);

CREATE INDEX tickets_by_project_state
ON tickets(project_id, state);

-- Dependencies are normalized so references are foreign-key checked and
-- graph reads do not require decoding serialized ticket data.
CREATE TABLE ticket_blockers (
    ticket_id       TEXT NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
    blocker_id      TEXT NOT NULL REFERENCES tickets(id),
    position        INTEGER NOT NULL,
    PRIMARY KEY (ticket_id, blocker_id)
);

CREATE TABLE activations (
    id              TEXT PRIMARY KEY,
    kind            TEXT NOT NULL,
    state           TEXT NOT NULL,
    ticket_id       TEXT REFERENCES tickets(id),
    project_id      TEXT REFERENCES projects(id),
    eligible_at_ms  INTEGER,
    interval_ms     INTEGER,
    created_at_ms   INTEGER NOT NULL,
    updated_at_ms   INTEGER NOT NULL,
    CHECK (ticket_id IS NULL OR project_id IS NULL)
);

CREATE TABLE activation_filters (
    activation_id   TEXT NOT NULL REFERENCES activations(id) ON DELETE CASCADE,
    ticket_id       TEXT NOT NULL REFERENCES tickets(id),
    PRIMARY KEY (activation_id, ticket_id)
);

CREATE TABLE runs (
    id                    TEXT PRIMARY KEY,
    activation_id         TEXT NOT NULL REFERENCES activations(id),
    ticket_id             TEXT NOT NULL REFERENCES tickets(id),
    state                 TEXT NOT NULL,
    attempt               INTEGER NOT NULL,
    branch                TEXT,
    worktree_path         TEXT,
    pid                   INTEGER,
    pid_start_time        INTEGER,
    process_group_id      INTEGER,
    worker_token          TEXT,
    worker_socket_path    TEXT,
    started_at_ms         INTEGER,
    exited_at_ms          INTEGER,
    exit_code             INTEGER,
    created_at_ms         INTEGER NOT NULL,
    updated_at_ms         INTEGER NOT NULL
);

CREATE INDEX runs_by_ticket ON runs(ticket_id, created_at_ms);
CREATE INDEX runs_by_activation ON runs(activation_id, created_at_ms);

CREATE TABLE leases (
    ticket_id       TEXT PRIMARY KEY REFERENCES tickets(id),
    run_id          TEXT NOT NULL UNIQUE REFERENCES runs(id),
    owner_id        TEXT NOT NULL,
    acquired_at_ms  INTEGER NOT NULL,
    renewed_at_ms   INTEGER NOT NULL,
    expires_at_ms   INTEGER NOT NULL
);

CREATE INDEX leases_by_expiry ON leases(expires_at_ms);

CREATE TABLE run_evidence (
    sequence        INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          TEXT NOT NULL REFERENCES runs(id),
    kind            TEXT NOT NULL,
    observed_at_ms  INTEGER NOT NULL,
    dedupe_key      TEXT UNIQUE,
    data_json       TEXT NOT NULL
);

CREATE INDEX evidence_by_run ON run_evidence(run_id, sequence);

CREATE TABLE aftercare_stages (
    run_id          TEXT NOT NULL REFERENCES runs(id),
    stage_index     INTEGER NOT NULL,
    stage           TEXT NOT NULL,
    state           TEXT NOT NULL,
    attempt         INTEGER NOT NULL DEFAULT 1,
    started_at_ms   INTEGER,
    finished_at_ms  INTEGER,
    exit_code       INTEGER,
    evidence_json   TEXT,
    PRIMARY KEY (run_id, stage_index, attempt)
);

CREATE TABLE cooldowns (
    key             TEXT PRIMARY KEY,
    until_ms        INTEGER NOT NULL,
    reason          TEXT NOT NULL,
    source_run_id   TEXT REFERENCES runs(id),
    updated_at_ms   INTEGER NOT NULL
);

CREATE TABLE budget_reservations (
    run_id              TEXT PRIMARY KEY REFERENCES runs(id),
    reserved_tokens     INTEGER NOT NULL,
    actual_tokens       INTEGER,
    state               TEXT NOT NULL,
    created_at_ms       INTEGER NOT NULL,
    reconciled_at_ms    INTEGER
);

CREATE TABLE scheduler_state (
    singleton       INTEGER PRIMARY KEY CHECK (singleton = 1),
    paused          INTEGER NOT NULL CHECK (paused IN (0, 1)),
    updated_at_ms   INTEGER NOT NULL
);

CREATE TABLE notes (
    id              TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL REFERENCES runs(id),
    text            TEXT NOT NULL,
    recorded_at_ms  INTEGER NOT NULL
);
";

const ID_COUNTER_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS id_counters (
    kind            TEXT PRIMARY KEY,
    next_ordinal    INTEGER NOT NULL CHECK (next_ordinal > 0)
);
INSERT OR IGNORE INTO id_counters (kind, next_ordinal)
SELECT 'activation', COALESCE(MAX(CAST(SUBSTR(id, 2) AS INTEGER)), 0) + 1 FROM activations;
INSERT OR IGNORE INTO id_counters (kind, next_ordinal)
SELECT 'run', COALESCE(MAX(CAST(SUBSTR(id, 2) AS INTEGER)), 0) + 1 FROM runs;
INSERT OR IGNORE INTO id_counters (kind, next_ordinal)
SELECT 'note', COALESCE(MAX(CAST(SUBSTR(id, 2) AS INTEGER)), 0) + 1 FROM notes;
";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TicketState {
    Ready,
    Held,
    Claimed,
    Merged,
    Failed,
    NeedsReview,
}

impl TicketState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Held => "held",
            Self::Claimed => "claimed",
            Self::Merged => "merged",
            Self::Failed => "failed",
            Self::NeedsReview => "needs_review",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    Claimed,
    Running,
}

impl RunState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Claimed => "claimed",
            Self::Running => "running",
        }
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

/// One appended `run_evidence` row: a kind plus kind-specific JSON facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceRecord {
    pub kind: &'static str,
    pub data_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CooldownUpdate<'a> {
    pub target: &'a str,
    pub until_ms: i64,
    pub reason: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CooldownRecord {
    pub target: String,
    pub until_ms: i64,
    pub reason: String,
}

/// One executed aftercare stage, persisted alongside the run's outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageRecord {
    pub stage_index: usize,
    pub stage: String,
    pub state: String,
    pub started_at_ms: i64,
    pub finished_at_ms: i64,
    pub exit_code: Option<i32>,
    pub output_ref: String,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveRun {
    pub id: String,
    pub ticket_id: String,
    pub project_id: String,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRecord {
    pub id: String,
    pub ticket_id: String,
    pub state: String,
    pub branch: Option<String>,
    pub worktree_path: Option<String>,
    pub pid: Option<i64>,
    pub pid_start_time: Option<i64>,
    pub process_group_id: Option<i64>,
    pub exit_code: Option<i64>,
    pub exited_at_ms: Option<i64>,
}

/// One lease that must be classified when a daemon starts. Process identity
/// and worker credentials are returned only to the daemon recovery path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecoverableRun {
    pub(crate) id: String,
    pub(crate) ticket_id: String,
    pub(crate) target: String,
    pub(crate) state: String,
    pub(crate) branch: Option<String>,
    pub(crate) worktree_path: Option<String>,
    pub(crate) pid: Option<i64>,
    pub(crate) pid_start_time: Option<i64>,
    pub(crate) process_group_id: Option<i64>,
    pub(crate) worker_token: Option<String>,
    pub(crate) worker_socket_path: Option<String>,
    pub(crate) exit_code: Option<i64>,
    pub(crate) lease_expires_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectRecord {
    pub id: String,
    pub file_path: Option<String>,
    pub title: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectNote {
    pub id: String,
    pub run_id: String,
    pub ticket_id: String,
    pub text: String,
    pub recorded_at_ms: i64,
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
    pub state: String,
    pub name: String,
    pub blocked_by: Vec<String>,
    pub worktree: Option<String>,
    pub target: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub flow: Option<String>,
    pub attempts: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReindexTicket {
    pub id: String,
    pub project_id: String,
    pub file_path: String,
    pub name: String,
    pub blocked_by: Vec<String>,
    pub worktree: String,
    pub target: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub flow: String,
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
        state: row.get(3)?,
        name: row.get(4)?,
        blocked_by: Vec::new(),
        worktree: row.get(5)?,
        target: row.get(6)?,
        model: row.get(7)?,
        effort: row.get(8)?,
        flow: row.get(9)?,
        attempts: row.get(10)?,
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
    connection: Connection,
}

impl Store {
    /// Opens (creating if needed) the database and migrates it to the current
    /// schema version. The daemon is the only writer; `now_ms` is injected so
    /// decision-adjacent timestamps never read the wall clock here.
    pub fn open(path: &Path, now_ms: i64) -> Result<Self, StoreError> {
        let connection = Connection::open(path).map_err(|source| StoreError::Open {
            path: path.to_path_buf(),
            source,
        })?;
        connection.execute_batch(CONNECTION_PRAGMAS)?;

        let mut store = Self { connection };
        store.migrate(now_ms)?;
        Ok(store)
    }

    fn migrate(&mut self, now_ms: i64) -> Result<(), StoreError> {
        let version: u32 = self
            .connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))?;
        match version {
            0 => {
                let transaction = self
                    .connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute_batch(SCHEMA_V1)?;
                transaction.execute_batch(ID_COUNTER_SCHEMA)?;
                transaction.execute(
                    "INSERT INTO scheduler_state (singleton, paused, updated_at_ms)
                     VALUES (1, 0, ?1)",
                    params![now_ms],
                )?;
                transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
                transaction.commit()?;
                Ok(())
            }
            1 => {
                let transaction = self
                    .connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute_batch(
                    "ALTER TABLE tickets ADD COLUMN model TEXT;
                     ALTER TABLE tickets ADD COLUMN effort TEXT;
                     ALTER TABLE tickets ADD COLUMN target TEXT;
                     ALTER TABLE tickets ADD COLUMN name TEXT NOT NULL DEFAULT '';
                     ALTER TABLE tickets ADD COLUMN worktree TEXT;
                     ALTER TABLE tickets ADD COLUMN flow TEXT;
                     ALTER TABLE tickets ADD COLUMN missing_at_ms INTEGER;
                     ALTER TABLE runs ADD COLUMN worker_socket_path TEXT;
                     CREATE TABLE ticket_blockers (
                         ticket_id TEXT NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
                         blocker_id TEXT NOT NULL REFERENCES tickets(id),
                         position INTEGER NOT NULL,
                         PRIMARY KEY (ticket_id, blocker_id)
                     );",
                )?;
                transaction.execute_batch(ID_COUNTER_SCHEMA)?;
                transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
                transaction.commit()?;
                Ok(())
            }
            2 => {
                let transaction = self
                    .connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute_batch(
                    "ALTER TABLE tickets ADD COLUMN target TEXT;
                     ALTER TABLE tickets ADD COLUMN name TEXT NOT NULL DEFAULT '';
                     ALTER TABLE tickets ADD COLUMN worktree TEXT;
                     ALTER TABLE tickets ADD COLUMN flow TEXT;
                     ALTER TABLE tickets ADD COLUMN missing_at_ms INTEGER;
                     ALTER TABLE runs ADD COLUMN worker_socket_path TEXT;
                     CREATE TABLE ticket_blockers (
                         ticket_id TEXT NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
                         blocker_id TEXT NOT NULL REFERENCES tickets(id),
                         position INTEGER NOT NULL,
                         PRIMARY KEY (ticket_id, blocker_id)
                     );",
                )?;
                transaction.execute_batch(ID_COUNTER_SCHEMA)?;
                transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
                transaction.commit()?;
                Ok(())
            }
            3 => {
                let transaction = self
                    .connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute_batch(
                    "ALTER TABLE tickets ADD COLUMN name TEXT NOT NULL DEFAULT '';
                     ALTER TABLE tickets ADD COLUMN worktree TEXT;
                     ALTER TABLE tickets ADD COLUMN flow TEXT;
                     ALTER TABLE tickets ADD COLUMN missing_at_ms INTEGER;
                     ALTER TABLE runs ADD COLUMN worker_socket_path TEXT;
                     CREATE TABLE ticket_blockers (
                         ticket_id TEXT NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
                         blocker_id TEXT NOT NULL REFERENCES tickets(id),
                         position INTEGER NOT NULL,
                         PRIMARY KEY (ticket_id, blocker_id)
                     );",
                )?;
                transaction.execute_batch(ID_COUNTER_SCHEMA)?;
                transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
                transaction.commit()?;
                Ok(())
            }
            4 => {
                let transaction = self
                    .connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute_batch(
                    "ALTER TABLE tickets ADD COLUMN flow TEXT;
                     ALTER TABLE tickets ADD COLUMN missing_at_ms INTEGER;
                     ALTER TABLE runs ADD COLUMN worker_socket_path TEXT;",
                )?;
                transaction.execute_batch(ID_COUNTER_SCHEMA)?;
                transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
                transaction.commit()?;
                Ok(())
            }
            5 => {
                let transaction = self
                    .connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute_batch(
                    "ALTER TABLE tickets ADD COLUMN missing_at_ms INTEGER;
                         ALTER TABLE runs ADD COLUMN worker_socket_path TEXT;",
                )?;
                transaction.execute_batch(ID_COUNTER_SCHEMA)?;
                transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
                transaction.commit()?;
                Ok(())
            }
            6 => {
                let transaction = self
                    .connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction
                    .execute_batch("ALTER TABLE runs ADD COLUMN worker_socket_path TEXT;")?;
                transaction.execute_batch(ID_COUNTER_SCHEMA)?;
                transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
                transaction.commit()?;
                Ok(())
            }
            7 => {
                let transaction = self
                    .connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute_batch(ID_COUNTER_SCHEMA)?;
                transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
                transaction.commit()?;
                Ok(())
            }
            SCHEMA_VERSION => Ok(()),
            newer => Err(StoreError::UnsupportedSchemaVersion(newer)),
        }
    }

    pub fn insert_local_project(
        &self,
        id: &str,
        file_path: &str,
        title: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.connection.execute(
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
        self.connection.execute(
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
            .connection
            .query_row("SELECT 1 FROM projects WHERE id = ?1", params![id], |row| {
                row.get(0)
            })
            .optional()?;
        Ok(found.is_some())
    }

    pub fn project(&self, id: &str) -> Result<Option<ProjectRecord>, StoreError> {
        self.connection
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
        let transaction = self.connection.unchecked_transaction()?;
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
        let transaction = self.connection.unchecked_transaction()?;
        transaction.execute(
            "UPDATE tickets
             SET name = ?2, worktree = ?3, target = ?4, model = ?5, effort = ?6, flow = ?7,
                 missing_at_ms = NULL, updated_at_ms = ?8
             WHERE id = ?1",
            params![id, name, worktree, target, model, effort, flow, now_ms],
        )?;
        replace_ticket_blockers(&transaction, id, blocked_by)?;
        transaction.commit()?;
        Ok(())
    }

    /// Applies a complete committed ticket snapshot without disturbing runtime
    /// history for IDs that remain present. Missing local rows and everything
    /// that depends on them are removed explicitly so the operation can report
    /// how much non-derivable state was discarded.
    pub fn apply_reindex(
        &self,
        project_ids: &[String],
        tickets: &[ReindexTicket],
        now_ms: i64,
    ) -> Result<ReindexResult, StoreError> {
        let existing: BTreeMap<String, TicketRecord> = self
            .tickets()?
            .into_iter()
            .map(|ticket| (ticket.id.clone(), ticket))
            .collect();
        let desired_ticket_ids: BTreeSet<&str> =
            tickets.iter().map(|ticket| ticket.id.as_str()).collect();
        let desired_project_ids: BTreeSet<&str> = project_ids.iter().map(String::as_str).collect();
        let transaction = self.connection.unchecked_transaction()?;

        let stale_tickets = {
            let mut statement =
                transaction.prepare("SELECT id FROM tickets WHERE source = 'local' ORDER BY id")?;
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
            let mut statement = transaction.prepare("SELECT id FROM runs WHERE ticket_id = ?1")?;
            doomed_runs.extend(
                statement
                    .query_map(params![ticket_id], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        for activation_id in &doomed_activations {
            let mut statement =
                transaction.prepare("SELECT id FROM runs WHERE activation_id = ?1")?;
            doomed_runs.extend(
                statement
                    .query_map(params![activation_id], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }

        let mut rows_dropped = 0;
        for run_id in &doomed_runs {
            transaction.execute(
                "UPDATE cooldowns SET source_run_id = NULL WHERE source_run_id = ?1",
                params![run_id],
            )?;
            for table in [
                "leases",
                "run_evidence",
                "aftercare_stages",
                "budget_reservations",
                "notes",
            ] {
                rows_dropped += transaction.execute(
                    &format!("DELETE FROM {table} WHERE run_id = ?1"),
                    params![run_id],
                )?;
            }
            rows_dropped +=
                transaction.execute("DELETE FROM runs WHERE id = ?1", params![run_id])?;
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
            let state = match (existing.get(&ticket.id), ticket.derived_state) {
                (Some(existing), Some(derived)) => {
                    if existing.state != derived.as_str() {
                        state_changes.push(ReindexStateChange {
                            ticket_id: ticket.id.clone(),
                            previous_state: existing.state.clone(),
                            state: derived.as_str().to_owned(),
                        });
                    }
                    derived.as_str()
                }
                (Some(existing), None) => existing.state.as_str(),
                (None, Some(derived)) => derived.as_str(),
                (None, None) => TicketState::Ready.as_str(),
            };
            transaction.execute(
                "INSERT INTO tickets
                     (id, project_id, file_path, source, state, name, worktree, target, model,
                      effort, flow, created_at_ms, updated_at_ms)
                 VALUES (?1, ?2, ?3, 'local', ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?11)
                 ON CONFLICT(id) DO UPDATE SET
                     project_id = excluded.project_id,
                     file_path = excluded.file_path,
                     source = 'local',
                     state = excluded.state,
                     name = excluded.name,
                     worktree = excluded.worktree,
                     target = excluded.target,
                     model = excluded.model,
                     effort = excluded.effort,
                     flow = excluded.flow,
                     missing_at_ms = NULL,
                     updated_at_ms = excluded.updated_at_ms",
                params![
                    ticket.id,
                    ticket.project_id,
                    ticket.file_path,
                    state,
                    ticket.name,
                    ticket.worktree,
                    ticket.target,
                    ticket.model,
                    ticket.effort,
                    ticket.flow,
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
        self.connection.execute(
            "UPDATE tickets SET target = ?2, model = ?3, effort = ?4, updated_at_ms = ?5 WHERE id = ?1",
            params![id, target, model, effort, now_ms],
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
        self.connection
            .execute(
                "UPDATE tickets SET target = ?1, updated_at_ms = ?2 WHERE target IS NULL",
                params![default_target, now_ms],
            )
            .map_err(StoreError::from)
    }

    pub fn ticket(&self, id: &str) -> Result<Option<TicketRecord>, StoreError> {
        let mut ticket = self
            .connection
            .query_row(
                "SELECT id, project_id, file_path, state, name, worktree, target, model, effort, flow, attempts
                 FROM tickets WHERE id = ?1",
                params![id],
                ticket_record,
            )
            .optional()?;
        if let Some(ticket) = ticket.as_mut() {
            ticket.blocked_by = self.ticket_blockers(&ticket.id)?;
        }
        Ok(ticket)
    }

    pub fn ticket_by_file(&self, file_path: &str) -> Result<Option<TicketRecord>, StoreError> {
        let mut ticket = self
            .connection
            .query_row(
                "SELECT id, project_id, file_path, state, name, worktree, target, model, effort, flow, attempts
                 FROM tickets WHERE file_path = ?1",
                params![file_path],
                ticket_record,
            )
            .optional()?;
        if let Some(ticket) = ticket.as_mut() {
            ticket.blocked_by = self.ticket_blockers(&ticket.id)?;
        }
        Ok(ticket)
    }

    pub fn tickets(&self) -> Result<Vec<TicketRecord>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, project_id, file_path, state, name, worktree, target, model, effort, flow, attempts
             FROM tickets ORDER BY project_id, id",
        )?;
        let mut tickets = statement
            .query_map([], ticket_record)?
            .collect::<Result<Vec<_>, _>>()?;
        for ticket in &mut tickets {
            ticket.blocked_by = self.ticket_blockers(&ticket.id)?;
        }
        Ok(tickets)
    }

    pub fn tickets_for_project(&self, project_id: &str) -> Result<Vec<TicketRecord>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, project_id, file_path, state, name, worktree, target, model, effort, flow, attempts
             FROM tickets WHERE project_id = ?1 ORDER BY id",
        )?;
        let mut tickets = statement
            .query_map(params![project_id], ticket_record)?
            .collect::<Result<Vec<_>, _>>()?;
        for ticket in &mut tickets {
            ticket.blocked_by = self.ticket_blockers(&ticket.id)?;
        }
        Ok(tickets)
    }

    pub fn ticket_dependencies(
        &self,
    ) -> Result<std::collections::BTreeMap<String, Vec<String>>, StoreError> {
        let mut dependencies = std::collections::BTreeMap::new();
        for ticket in self.tickets()? {
            dependencies.insert(ticket.id, ticket.blocked_by);
        }
        Ok(dependencies)
    }

    fn ticket_blockers(&self, id: &str) -> Result<Vec<String>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT blocker_id FROM ticket_blockers
             WHERE ticket_id = ?1 ORDER BY position, blocker_id",
        )?;
        statement
            .query_map(params![id], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    pub fn ticket_ids(&self) -> Result<Vec<String>, StoreError> {
        let mut statement = self.connection.prepare("SELECT id FROM tickets")?;
        let rows = statement.query_map([], |row| row.get(0))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    pub fn local_ticket_files(&self) -> Result<Vec<LocalTicketFile>, StoreError> {
        let mut statement = self.connection.prepare(
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
        let referenced = self.connection.query_row(
            "SELECT EXISTS (SELECT 1 FROM runs WHERE ticket_id = ?1)
                 OR EXISTS (SELECT 1 FROM leases WHERE ticket_id = ?1)
                 OR EXISTS (SELECT 1 FROM activations WHERE ticket_id = ?1)
                 OR EXISTS (SELECT 1 FROM activation_filters WHERE ticket_id = ?1)
                 OR EXISTS (SELECT 1 FROM ticket_blockers WHERE blocker_id = ?1)",
            params![id],
            |row| row.get(0),
        )?;
        Ok(referenced)
    }

    pub fn delete_ticket(&self, id: &str) -> Result<(), StoreError> {
        self.connection
            .execute("DELETE FROM tickets WHERE id = ?1", params![id])?;
        Ok(())
    }

    /// Stamps a ticket whose committed file has disappeared. The stamp keeps
    /// the row out of selection without disturbing its state; an existing
    /// stamp is preserved so the deletion clock starts at the first pass.
    pub fn mark_ticket_missing(&self, id: &str, now_ms: i64) -> Result<(), StoreError> {
        self.connection.execute(
            "UPDATE tickets SET missing_at_ms = ?2, updated_at_ms = ?2
             WHERE id = ?1 AND missing_at_ms IS NULL",
            params![id, now_ms],
        )?;
        Ok(())
    }

    pub fn clear_ticket_missing(&self, id: &str, now_ms: i64) -> Result<(), StoreError> {
        self.connection.execute(
            "UPDATE tickets SET missing_at_ms = NULL, updated_at_ms = ?2
             WHERE id = ?1 AND missing_at_ms IS NOT NULL",
            params![id, now_ms],
        )?;
        Ok(())
    }

    pub fn ticket_state(&self, id: &str) -> Result<Option<String>, StoreError> {
        let state = self
            .connection
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
        let requested = state.as_str();
        let previous = self
            .ticket_state(id)?
            .ok_or_else(|| StoreError::TicketNotFound {
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
        let changed = self.connection.execute(
            "UPDATE tickets SET state = ?2, updated_at_ms = ?3
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
        let previous = self
            .ticket_state(id)?
            .ok_or_else(|| StoreError::TicketNotFound {
                ticket_id: id.into(),
            })?;
        let changed = self.connection.execute(
            "UPDATE tickets SET state = 'ready', attempts = 0, updated_at_ms = ?2
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
        Ok(previous)
    }

    pub fn insert_activation(
        &self,
        activation: &NewActivation<'_>,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.connection.execute(
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
        self.connection.execute(
            "INSERT OR IGNORE INTO activation_filters (activation_id, ticket_id) VALUES (?1, ?2)",
            params![activation_id, ticket_id],
        )?;
        Ok(())
    }

    pub fn queued_activations(&self) -> Result<Vec<QueuedActivation>, StoreError> {
        let mut statement = self.connection.prepare(
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
        let mut statement = self.connection.prepare(
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
        self.connection
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
        let ticket = self
            .connection
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
                params![activation.project_id, activation.id, now_ms],
                |row| row.get(0),
            )
            .optional()?;
        Ok(ticket)
    }

    pub fn ticket_is_dispatchable(&self, ticket_id: &str) -> Result<bool, StoreError> {
        self.connection
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
        let mut statement = self.connection.prepare(
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

    /// Returns the next run ordinal. A successful claim advances the durable
    /// high-water counter so IDs and output paths cannot be reused.
    pub fn next_run_ordinal(&self) -> Result<i64, StoreError> {
        self.next_ordinal("run", "runs")
    }

    pub fn ensure_next_run_ordinal(&self, minimum: i64) -> Result<(), StoreError> {
        self.connection.execute(
            "UPDATE id_counters
             SET next_ordinal = MAX(next_ordinal, ?1)
             WHERE kind = 'run'",
            params![minimum],
        )?;
        Ok(())
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
        let changed = self.connection.execute(
            "UPDATE runs
             SET state = 'running', branch = ?2, worktree_path = ?3, pid = ?4,
                 pid_start_time = ?5, process_group_id = ?6, worker_token = ?7,
                 worker_socket_path = ?8, started_at_ms = ?9, updated_at_ms = ?9
             WHERE id = ?1 AND state = 'claimed' AND exited_at_ms IS NULL",
            params![
                run_id,
                branch,
                worktree_path,
                i64::from(pid),
                pid_start_time,
                i64::from(process_group_id),
                worker_token,
                worker_socket_path,
                now_ms,
            ],
        )?;
        if changed != 1 {
            let state = self
                .connection
                .query_row(
                    "SELECT state FROM runs WHERE id = ?1",
                    params![run_id],
                    |row| row.get(0),
                )
                .optional()?;
            return Err(StoreError::RunStateConflict {
                run_id: run_id.into(),
                state,
                requested: "running".into(),
            });
        }
        Ok(())
    }

    /// Terminates a run in one transaction: the raw exit and derived outcome
    /// land on the run, evidence is appended, the lease is
    /// freed, and the ticket moves to its terminal state or back to `ready`
    /// when cancellation or recovery releases it.
    #[allow(clippy::too_many_arguments)]
    pub fn finish_run(
        &mut self,
        run_id: &str,
        ticket_id: &str,
        exit_code: Option<i32>,
        outcome: crate::outcome::Outcome,
        evidence: &[EvidenceRecord],
        cooldown: Option<&CooldownUpdate<'_>>,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        use crate::outcome::Outcome;

        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "UPDATE runs
             SET state = ?2, exited_at_ms = ?3, exit_code = ?4, updated_at_ms = ?3
             WHERE id = ?1 AND exited_at_ms IS NULL",
            params![run_id, outcome.as_str(), now_ms, exit_code],
        )?;
        if changed == 0 {
            let existing: Option<(String, Option<i64>)> = transaction
                .query_row(
                    "SELECT state, exited_at_ms FROM runs WHERE id = ?1",
                    params![run_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            match existing {
                Some((_, Some(_))) => {
                    transaction.commit()?;
                    return Ok(());
                }
                Some((state, None)) => {
                    return Err(StoreError::RunStateConflict {
                        run_id: run_id.into(),
                        state: Some(state),
                        requested: outcome.as_str().into(),
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

        let ticket_state = match outcome {
            Outcome::Merged => TicketState::Merged,
            Outcome::Failed => TicketState::Failed,
            Outcome::NeedsReview => TicketState::NeedsReview,
            Outcome::Cancelled => TicketState::Ready,
            Outcome::RateLimited => TicketState::Ready,
            Outcome::Orphaned => TicketState::Ready,
        };
        transaction.execute(
            "UPDATE tickets SET state = ?2, updated_at_ms = ?3
             WHERE id = ?1 AND state = 'claimed'",
            params![ticket_id, ticket_state.as_str(), now_ms],
        )?;
        if outcome == Outcome::RateLimited {
            transaction.execute(
                "UPDATE activations SET state = 'queued', updated_at_ms = ?2
                 WHERE id = (SELECT activation_id FROM runs WHERE id = ?1)",
                params![run_id, now_ms],
            )?;
        }

        if let Some(cooldown) = cooldown {
            upsert_cooldown(&transaction, run_id, cooldown, now_ms)?;
        }

        for record in evidence {
            transaction.execute(
                "INSERT OR IGNORE INTO run_evidence
                     (run_id, kind, observed_at_ms, dedupe_key, data_json)
                 VALUES (?1, ?2, ?3, 'settlement:' || ?1 || ':' || ?2, ?4)",
                params![run_id, record.kind, now_ms, record.data_json],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Records one completed flow stage. The flow index is the idempotency
    /// key, so recovery can re-derive the first stage still lacking a verdict.
    pub(crate) fn record_aftercare_stage(
        &self,
        run_id: &str,
        stage: &StageRecord,
    ) -> Result<(), StoreError> {
        let evidence_json = serde_json::json!({"output": stage.output_ref}).to_string();
        self.connection.execute(
            "INSERT INTO aftercare_stages
                 (run_id, stage_index, stage, state, started_at_ms, finished_at_ms, exit_code,
                  evidence_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(run_id, stage_index, attempt) DO UPDATE SET
                 stage = excluded.stage,
                 state = excluded.state,
                 started_at_ms = excluded.started_at_ms,
                 finished_at_ms = excluded.finished_at_ms,
                 exit_code = excluded.exit_code,
                 evidence_json = excluded.evidence_json",
            params![
                run_id,
                stage.stage_index as i64,
                stage.stage,
                stage.state,
                stage.started_at_ms,
                stage.finished_at_ms,
                stage.exit_code,
                evidence_json,
            ],
        )?;
        Ok(())
    }

    pub(crate) fn aftercare_stages(&self, run_id: &str) -> Result<Vec<StageRecord>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT stage_index, stage, state, started_at_ms, finished_at_ms, exit_code,
                    evidence_json
             FROM aftercare_stages WHERE run_id = ?1 ORDER BY stage_index",
        )?;
        statement
            .query_map(params![run_id], |row| {
                let evidence_json: Option<String> = row.get(6)?;
                let output_ref = evidence_json
                    .as_deref()
                    .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
                    .and_then(|value| value["output"].as_str().map(str::to_owned))
                    .unwrap_or_default();
                Ok(StageRecord {
                    stage_index: row.get::<_, i64>(0)? as usize,
                    stage: row.get(1)?,
                    state: row.get(2)?,
                    started_at_ms: row.get(3)?,
                    finished_at_ms: row.get(4)?,
                    exit_code: row.get(5)?,
                    output_ref,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
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
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "UPDATE runs
             SET state = 'aftercare', exit_code = ?2, updated_at_ms = ?3
             WHERE id = ?1 AND state = 'running' AND exited_at_ms IS NULL",
            params![run_id, exit_code, now_ms],
        )?;
        if changed == 0 {
            let state: Option<String> = transaction
                .query_row(
                    "SELECT state FROM runs WHERE id = ?1",
                    params![run_id],
                    |row| row.get(0),
                )
                .optional()?;
            return match state {
                Some(state) => Ok(ExitClaim::AlreadyClaimed { state }),
                None => Err(StoreError::RunNotFound {
                    run_id: run_id.into(),
                }),
            };
        }
        for (kind, data_json) in [
            (
                "exit_classified",
                serde_json::json!({"exit_code": exit_code}).to_string(),
            ),
            ("commits_observed", commits_json.to_owned()),
        ] {
            transaction.execute(
                "INSERT OR IGNORE INTO run_evidence
                     (run_id, kind, observed_at_ms, dedupe_key, data_json)
                 VALUES (?1, ?2, ?3, 'settlement:' || ?1 || ':' || ?2, ?4)",
                params![run_id, kind, now_ms, data_json],
            )?;
        }
        if let Some(vendor_error) = vendor_error {
            transaction.execute(
                "INSERT OR IGNORE INTO run_evidence
                     (run_id, kind, observed_at_ms, dedupe_key, data_json)
                 VALUES (?1, 'vendor_error_classified', ?2,
                         'settlement:' || ?1 || ':vendor_error_classified', ?3)",
                params![
                    run_id,
                    now_ms,
                    vendor_error.evidence_json(cooldown_until_ms)
                ],
            )?;
        }
        if !capture_complete {
            transaction.execute(
                "INSERT OR IGNORE INTO run_evidence
                     (run_id, kind, observed_at_ms, dedupe_key, data_json)
                 VALUES (?1, 'capture_incomplete', ?2,
                         'settlement:' || ?1 || ':capture_incomplete', '{}')",
                params![run_id, now_ms],
            )?;
        }
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
        self.connection.execute(
            "INSERT INTO run_evidence
                 (run_id, kind, observed_at_ms, dedupe_key, data_json)
             VALUES (?1, ?2, ?3, 'settlement:' || ?1 || ':' || ?2, ?4)
             ON CONFLICT(dedupe_key) DO UPDATE SET
                 observed_at_ms = excluded.observed_at_ms,
                 data_json = excluded.data_json",
            params![run_id, kind, now_ms, data_json],
        )?;
        Ok(())
    }

    pub(crate) fn clear_aftercare_process(&self, run_id: &str) -> Result<(), StoreError> {
        self.connection.execute(
            "DELETE FROM run_evidence
             WHERE run_id = ?1 AND dedupe_key = 'settlement:' || ?1 || ':aftercare_process'",
            params![run_id],
        )?;
        Ok(())
    }

    /// Durably records an operator's cancellation intent, idempotently: the
    /// dedupe key makes a repeated `cancel` a no-op rather than new evidence.
    pub fn record_cancel_requested(&self, run_id: &str, now_ms: i64) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT OR IGNORE INTO run_evidence
                 (run_id, kind, observed_at_ms, dedupe_key, data_json)
             VALUES (?1, 'cancel_requested', ?2, 'cancel_requested:' || ?1, '{}')",
            params![run_id, now_ms],
        )?;
        Ok(())
    }

    /// Whether cancellation intent was recorded for the run, so an exit event
    /// racing the cancel still resolves to `Cancelled`.
    pub fn cancellation_requested(&self, run_id: &str) -> Result<bool, StoreError> {
        let found: Option<i64> = self
            .connection
            .query_row(
                "SELECT 1 FROM run_evidence
                 WHERE run_id = ?1 AND kind = 'cancel_requested'",
                params![run_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(found.is_some())
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
        self.connection.execute(
            "INSERT INTO notes (id, run_id, text, recorded_at_ms)
             VALUES (?1, ?2, ?3, ?4)",
            params![id, run_id, text, now_ms],
        )?;
        Ok(())
    }

    /// Notes recorded against one run, in the order they arrived.
    pub fn notes_for_run(&self, run_id: &str) -> Result<Vec<String>, StoreError> {
        let mut statement = self
            .connection
            .prepare("SELECT text FROM notes WHERE run_id = ?1 ORDER BY recorded_at_ms, id")?;
        let rows = statement
            .query_map(params![run_id], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn notes_for_project(&self, project_id: &str) -> Result<Vec<ProjectNote>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT n.id, n.run_id, r.ticket_id, n.text, n.recorded_at_ms
             FROM notes n
             JOIN runs r ON r.id = n.run_id
             JOIN tickets t ON t.id = r.ticket_id
             WHERE t.project_id = ?1
             ORDER BY r.ticket_id, n.recorded_at_ms, n.id",
        )?;
        statement
            .query_map(params![project_id], |row| {
                Ok(ProjectNote {
                    id: row.get(0)?,
                    run_id: row.get(1)?,
                    ticket_id: row.get(2)?,
                    text: row.get(3)?,
                    recorded_at_ms: row.get(4)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    pub fn commit_evidence_for_project(
        &self,
        project_id: &str,
    ) -> Result<Vec<ProjectCommitEvidence>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT r.id, r.ticket_id, e.data_json
             FROM run_evidence e
             JOIN runs r ON r.id = e.run_id
             JOIN tickets t ON t.id = r.ticket_id
             WHERE t.project_id = ?1 AND e.kind = 'commits_observed'
             ORDER BY r.ticket_id, r.created_at_ms, r.id, e.sequence",
        )?;
        statement
            .query_map(params![project_id], |row| {
                Ok(ProjectCommitEvidence {
                    run_id: row.get(0)?,
                    ticket_id: row.get(1)?,
                    data_json: row.get(2)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    pub fn next_note_ordinal(&self) -> Result<i64, StoreError> {
        self.reserve_ordinal("note", "notes")
    }

    /// Evidence rows for one run in observation order, as (kind, data_json).
    pub fn run_evidence(&self, run_id: &str) -> Result<Vec<(String, String)>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT kind, data_json FROM run_evidence WHERE run_id = ?1 ORDER BY sequence",
        )?;
        let rows = statement
            .query_map(params![run_id], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn vendor_error_for_run(
        &self,
        run_id: &str,
    ) -> Result<Option<crate::vendor_error::VendorErrorMatch>, StoreError> {
        let data: Option<String> = self
            .connection
            .query_row(
                "SELECT data_json FROM run_evidence
                 WHERE run_id = ?1 AND kind = 'vendor_error_classified'
                 ORDER BY sequence DESC LIMIT 1",
                params![run_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(data.and_then(|data| serde_json::from_str(&data).ok()))
    }

    pub fn latest_vendor_error_for_ticket(
        &self,
        ticket_id: &str,
    ) -> Result<Option<crate::vendor_error::VendorErrorMatch>, StoreError> {
        let data: Option<String> = self
            .connection
            .query_row(
                "SELECT e.data_json FROM run_evidence e
                 JOIN runs r ON r.id = e.run_id
                 WHERE r.id = (SELECT latest.id FROM runs latest
                               WHERE latest.ticket_id = ?1
                               ORDER BY latest.created_at_ms DESC, latest.id DESC LIMIT 1)
                   AND e.kind = 'vendor_error_classified'
                 ORDER BY e.sequence DESC LIMIT 1",
                params![ticket_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(data.and_then(|data| serde_json::from_str(&data).ok()))
    }

    /// Rolls back a claim whose launch failed before a process existed: the
    /// lease is released, the run is closed, and the ticket returns to
    /// `ready`. The consumed attempt is kept as evidence of the try.
    pub fn abort_claim(
        &mut self,
        run_id: &str,
        ticket_id: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute("DELETE FROM leases WHERE run_id = ?1", params![run_id])?;
        transaction.execute(
            "UPDATE runs
             SET state = 'aborted', exited_at_ms = ?2, updated_at_ms = ?2
             WHERE id = ?1 AND exited_at_ms IS NULL",
            params![run_id, now_ms],
        )?;
        transaction.execute(
            "UPDATE tickets SET state = 'ready', updated_at_ms = ?2
             WHERE id = ?1 AND state = 'claimed'",
            params![ticket_id, now_ms],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn run(&self, id: &str) -> Result<Option<RunRecord>, StoreError> {
        let run = self
            .connection
            .query_row(
                "SELECT id, ticket_id, state, branch, worktree_path, pid,
                        pid_start_time, process_group_id, exit_code, exited_at_ms
                 FROM runs WHERE id = ?1",
                params![id],
                |row| {
                    Ok(RunRecord {
                        id: row.get(0)?,
                        ticket_id: row.get(1)?,
                        state: row.get(2)?,
                        branch: row.get(3)?,
                        worktree_path: row.get(4)?,
                        pid: row.get(5)?,
                        pid_start_time: row.get(6)?,
                        process_group_id: row.get(7)?,
                        exit_code: row.get(8)?,
                        exited_at_ms: row.get(9)?,
                    })
                },
            )
            .optional()?;
        Ok(run)
    }

    pub fn active_run_for_ticket(&self, ticket_id: &str) -> Result<Option<String>, StoreError> {
        let run = self
            .connection
            .query_row(
                "SELECT id FROM runs
                 WHERE ticket_id = ?1 AND state IN ('claimed', 'running', 'aftercare')
                   AND exited_at_ms IS NULL
                 ORDER BY created_at_ms DESC, id DESC LIMIT 1",
                params![ticket_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(run)
    }

    /// Runs that have started and not yet exited, oldest first.
    pub fn active_runs(&self) -> Result<Vec<ActiveRun>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT r.id, r.ticket_id, t.project_id, r.state FROM runs r
             JOIN tickets t ON t.id = r.ticket_id
             WHERE r.exited_at_ms IS NULL AND r.state IN ('running', 'aftercare')
             ORDER BY r.created_at_ms, r.id",
        )?;
        let runs = statement
            .query_map([], |row| {
                Ok(ActiveRun {
                    id: row.get(0)?,
                    ticket_id: row.get(1)?,
                    project_id: row.get(2)?,
                    state: row.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(runs)
    }

    /// Every nonterminal run that still owns a lease, oldest first. Startup
    /// must classify all of these before making another spawn decision.
    pub(crate) fn recoverable_runs(&self) -> Result<Vec<RecoverableRun>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT r.id, r.ticket_id, t.target, r.state, r.branch, r.worktree_path,
                    r.pid, r.pid_start_time, r.process_group_id, r.worker_token,
                    r.worker_socket_path, r.exit_code, l.expires_at_ms
             FROM runs r
             JOIN leases l ON l.run_id = r.id
             JOIN tickets t ON t.id = r.ticket_id
             WHERE r.exited_at_ms IS NULL
               AND r.state IN ('claimed', 'running', 'aftercare')
             ORDER BY r.created_at_ms, r.id",
        )?;
        statement
            .query_map([], |row| {
                Ok(RecoverableRun {
                    id: row.get(0)?,
                    ticket_id: row.get(1)?,
                    target: row.get(2)?,
                    state: row.get(3)?,
                    branch: row.get(4)?,
                    worktree_path: row.get(5)?,
                    pid: row.get(6)?,
                    pid_start_time: row.get(7)?,
                    process_group_id: row.get(8)?,
                    worker_token: row.get(9)?,
                    worker_socket_path: row.get(10)?,
                    exit_code: row.get(11)?,
                    lease_expires_at_ms: row.get(12)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
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
            .connection
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
        self.connection.execute(
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
        self.reserve_ordinal("activation", "activations")
    }

    fn reserve_ordinal(&self, kind: &str, table: &str) -> Result<i64, StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        let reserved: i64 = transaction.query_row(
            "SELECT next_ordinal FROM id_counters WHERE kind = ?1",
            params![kind],
            |row| row.get(0),
        )?;
        let existing: i64 = transaction.query_row(
            &format!("SELECT COALESCE(MAX(CAST(SUBSTR(id, 2) AS INTEGER)), 0) + 1 FROM {table}"),
            [],
            |row| row.get(0),
        )?;
        let ordinal = reserved.max(existing);
        transaction.execute(
            "UPDATE id_counters SET next_ordinal = ?2 WHERE kind = ?1",
            params![kind, ordinal + 1],
        )?;
        transaction.commit()?;
        Ok(ordinal)
    }

    fn next_ordinal(&self, kind: &str, table: &str) -> Result<i64, StoreError> {
        let reserved: i64 = self.connection.query_row(
            "SELECT next_ordinal FROM id_counters WHERE kind = ?1",
            params![kind],
            |row| row.get(0),
        )?;
        let existing: i64 = self.connection.query_row(
            &format!("SELECT COALESCE(MAX(CAST(SUBSTR(id, 2) AS INTEGER)), 0) + 1 FROM {table}"),
            [],
            |row| row.get(0),
        )?;
        Ok(reserved.max(existing))
    }

    /// Claims a ready ticket for one run in a single transaction. The
    /// conditional update plus the primary key on `leases.ticket_id` are the
    /// durable guards against a double claim.
    pub fn claim_ticket(
        &mut self,
        claim: &ClaimRequest<'_>,
        now_ms: i64,
    ) -> Result<ClaimedRun, StoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

        let changed = transaction.execute(
            "UPDATE tickets
             SET state = 'claimed', attempts = attempts + 1, updated_at_ms = ?2
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

        let attempt: i64 = transaction.query_row(
            "SELECT attempts FROM tickets WHERE id = ?1",
            params![claim.ticket_id],
            |row| row.get(0),
        )?;

        transaction.execute(
            "INSERT INTO runs
                 (id, activation_id, ticket_id, state, attempt, created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
            params![
                claim.run_id,
                claim.activation_id,
                claim.ticket_id,
                RunState::Claimed.as_str(),
                attempt,
                now_ms,
            ],
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
        if let Some(ordinal) = claim
            .run_id
            .strip_prefix('R')
            .and_then(|value| value.parse::<i64>().ok())
        {
            transaction.execute(
                "UPDATE id_counters
                 SET next_ordinal = MAX(next_ordinal, ?1)
                 WHERE kind = 'run'",
                params![ordinal + 1],
            )?;
        }

        transaction.commit()?;
        Ok(ClaimedRun {
            run_id: claim.run_id.into(),
            attempt,
            lease_expires_at_ms: expires_at_ms,
        })
    }

    /// Renews the lease that `run_id` holds on `ticket_id`, returning the new
    /// expiry. Renewal is strict: an expired lease cannot be renewed, so once
    /// recovery treats expiry as "run is lost" a revived run can never
    /// resurrect a lease that recovery may be reclaiming.
    pub fn renew_lease(
        &mut self,
        ticket_id: &str,
        run_id: &str,
        lease_ms: i64,
        now_ms: i64,
    ) -> Result<i64, StoreError> {
        let expires_at_ms = now_ms + lease_ms;
        let changed = self.connection.execute(
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
        let paused: i64 = self.connection.query_row(
            "SELECT paused FROM scheduler_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        Ok(paused != 0)
    }

    pub fn active_cooldown_for_target(
        &self,
        target: &str,
        now_ms: i64,
    ) -> Result<Option<CooldownRecord>, StoreError> {
        self.connection
            .query_row(
                "SELECT ?1, until_ms, reason FROM cooldowns
                 WHERE key = 'agent_target:' || ?1 AND until_ms > ?2",
                params![target, now_ms],
                |row| {
                    Ok(CooldownRecord {
                        target: row.get(0)?,
                        until_ms: row.get(1)?,
                        reason: row.get(2)?,
                    })
                },
            )
            .optional()
            .map_err(StoreError::from)
    }

    pub fn active_cooldowns(&self, now_ms: i64) -> Result<Vec<CooldownRecord>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT SUBSTR(key, 14), until_ms, reason FROM cooldowns
             WHERE key LIKE 'agent_target:%' AND until_ms > ?1
             ORDER BY key",
        )?;
        statement
            .query_map(params![now_ms], |row| {
                Ok(CooldownRecord {
                    target: row.get(0)?,
                    until_ms: row.get(1)?,
                    reason: row.get(2)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    pub fn next_active_cooldown(&self, now_ms: i64) -> Result<Option<i64>, StoreError> {
        self.connection
            .query_row(
                "SELECT MIN(until_ms) FROM cooldowns WHERE until_ms > ?1",
                params![now_ms],
                |row| row.get(0),
            )
            .map_err(StoreError::from)
    }

    pub fn set_paused(&self, paused: bool, now_ms: i64) -> Result<(), StoreError> {
        self.connection.execute(
            "UPDATE scheduler_state SET paused = ?1, updated_at_ms = ?2 WHERE singleton = 1",
            params![i64::from(paused), now_ms],
        )?;
        Ok(())
    }

    /// Performs a small committed write used to detect when SQLite can make
    /// progress again after returning `SQLITE_FULL`.
    pub(crate) fn probe_writable(&self, now_ms: i64) -> Result<(), StoreError> {
        self.connection.execute(
            "UPDATE scheduler_state SET updated_at_ms = ?1 WHERE singleton = 1",
            params![now_ms],
        )?;
        Ok(())
    }

    pub fn ticket_counts(&self) -> Result<TicketCounts, StoreError> {
        let mut statement = self.connection.prepare(
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

fn upsert_cooldown(
    transaction: &rusqlite::Transaction<'_>,
    run_id: &str,
    cooldown: &CooldownUpdate<'_>,
    now_ms: i64,
) -> Result<(), rusqlite::Error> {
    transaction.execute(
        "INSERT INTO cooldowns (key, until_ms, reason, source_run_id, updated_at_ms)
         VALUES ('agent_target:' || ?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(key) DO UPDATE SET
             until_ms = MAX(cooldowns.until_ms, excluded.until_ms),
             reason = CASE WHEN excluded.until_ms >= cooldowns.until_ms
                           THEN excluded.reason ELSE cooldowns.reason END,
             source_run_id = CASE WHEN excluded.until_ms >= cooldowns.until_ms
                                  THEN excluded.source_run_id ELSE cooldowns.source_run_id END,
             updated_at_ms = excluded.updated_at_ms",
        params![
            cooldown.target,
            cooldown.until_ms,
            cooldown.reason,
            run_id,
            now_ms
        ],
    )?;
    Ok(())
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
        }
    }
}

impl std::error::Error for StoreError {}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{
        ActivationKind, ClaimRequest, ExitClaim, NewActivation, Store, StoreError, TicketState,
    };
    use crate::outcome::Outcome;

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

    #[test]
    fn writable_probe_commits_without_changing_pause_state() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));

        store.probe_writable(2_000).unwrap();

        assert!(!store.paused().unwrap());
        let updated_at_ms: i64 = store
            .connection
            .query_row(
                "SELECT updated_at_ms FROM scheduler_state WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(updated_at_ms, 2_000);
    }

    fn claim_t1<'a>(run_id: &'a str) -> ClaimRequest<'a> {
        ClaimRequest {
            ticket_id: "T1",
            run_id,
            activation_id: "A1",
            owner_id: "daemon-1",
            lease_ms: 60_000,
            next_activation_eligible_at_ms: None,
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
            .connection
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
            .connection
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
    fn tickets_are_ordered_by_project_and_id_and_include_attempts() {
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
                1_000,
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
        assert_eq!(
            tickets
                .iter()
                .map(|ticket| ticket.id.as_str())
                .collect::<Vec<_>>(),
            ["T0", "T1", "T2"]
        );
        assert_eq!(tickets[0].attempts, 0);
        assert_eq!(tickets[1].attempts, 1);
        assert_eq!(tickets[2].attempts, 0);
    }

    #[test]
    fn active_run_for_ticket_tracks_claimed_and_running_runs_only() {
        use crate::outcome::Outcome;

        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        assert_eq!(store.active_run_for_ticket("T1").unwrap(), None);

        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();
        assert_eq!(
            store.active_run_for_ticket("T1").unwrap().as_deref(),
            Some("R1")
        );
        store
            .mark_run_running(
                "R1",
                "branch",
                "/tmp/worktree",
                1,
                Some(1),
                1,
                "token",
                "/runtime/R1.sock",
                2_100,
            )
            .unwrap();
        assert_eq!(
            store.active_run_for_ticket("T1").unwrap().as_deref(),
            Some("R1")
        );

        store
            .finish_run("R1", "T1", Some(1), Outcome::Failed, &[], None, 2_200)
            .unwrap();
        assert_eq!(store.active_run_for_ticket("T1").unwrap(), None);
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
    fn recoverable_runs_round_trip_process_identity_and_lease() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sloop.db");
        let mut store = open_seeded(&path);
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();
        store
            .mark_run_running(
                "R1",
                "sloop/T1-a1-R1",
                "/worktrees/R1",
                123,
                Some(456),
                123,
                "worker-token",
                "/runtime/R1.sock",
                2_100,
            )
            .unwrap();
        drop(store);

        let store = Store::open(&path, 3_000).unwrap();
        let runs = store.recoverable_runs().unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].id, "R1");
        assert_eq!(runs[0].ticket_id, "T1");
        assert_eq!(runs[0].pid, Some(123));
        assert_eq!(runs[0].pid_start_time, Some(456));
        assert_eq!(runs[0].process_group_id, Some(123));
        assert_eq!(runs[0].worker_token.as_deref(), Some("worker-token"));
        assert_eq!(
            runs[0].worker_socket_path.as_deref(),
            Some("/runtime/R1.sock")
        );
        assert_eq!(runs[0].exit_code, None);
        assert_eq!(runs[0].lease_expires_at_ms, 62_000);
    }

    #[test]
    fn agent_exit_and_aftercare_results_are_checkpointed_idempotently() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();
        store
            .mark_run_running(
                "R1",
                "branch",
                "/worktree",
                123,
                Some(456),
                123,
                "token",
                "/runtime/R1.sock",
                2_100,
            )
            .unwrap();

        store
            .record_agent_exit(
                "R1",
                Some(0),
                true,
                r#"{"count":1,"oids":["abc"]}"#,
                None,
                None,
                2_200,
            )
            .unwrap();
        store
            .record_aftercare_evidence(
                "R1",
                "test_result",
                r#"{"passed":true,"exit_code":0}"#,
                2_300,
            )
            .unwrap();
        store
            .record_aftercare_evidence(
                "R1",
                "test_result",
                r#"{"passed":true,"exit_code":0}"#,
                2_400,
            )
            .unwrap();

        let run = store.run("R1").unwrap().unwrap();
        assert_eq!(run.state, "aftercare");
        assert_eq!(run.exit_code, Some(0));
        assert_eq!(store.recoverable_runs().unwrap()[0].state, "aftercare");
        let evidence = store.run_evidence("R1").unwrap();
        assert_eq!(
            evidence
                .iter()
                .filter(|(kind, _)| kind == "test_result")
                .count(),
            1
        );
    }

    fn running_r1(store: &mut Store) {
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();
        store
            .mark_run_running(
                "R1",
                "branch",
                "/worktree",
                123,
                Some(456),
                123,
                "token",
                "/runtime/R1.sock",
                2_100,
            )
            .unwrap();
    }

    #[test]
    fn agent_exit_checkpoint_is_an_exclusive_ownership_handoff() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        running_r1(&mut store);

        let first = store
            .record_agent_exit(
                "R1",
                Some(0),
                true,
                r#"{"oids":["abc"]}"#,
                None,
                None,
                2_200,
            )
            .unwrap();
        assert_eq!(first, ExitClaim::Claimed);
        assert_eq!(store.run("R1").unwrap().unwrap().state, "aftercare");
        let evidence = store.run_evidence("R1").unwrap();
        assert!(evidence.iter().any(|(kind, _)| kind == "exit_classified"));
        assert!(evidence.iter().any(|(kind, _)| kind == "commits_observed"));

        let second = store
            .record_agent_exit("R1", Some(1), false, r#"{"oids":[]}"#, None, None, 2_300)
            .unwrap();
        assert_eq!(
            second,
            ExitClaim::AlreadyClaimed {
                state: "aftercare".into()
            }
        );
        let run = store.run("R1").unwrap().unwrap();
        assert_eq!(run.state, "aftercare");
        assert_eq!(run.exit_code, Some(0));
        assert_eq!(store.run_evidence("R1").unwrap(), evidence);
    }

    #[test]
    fn agent_exit_checkpoint_reports_terminal_and_missing_runs() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        running_r1(&mut store);
        store
            .finish_run("R1", "T1", Some(0), Outcome::Merged, &[], None, 2_200)
            .unwrap();

        let claim = store
            .record_agent_exit(
                "R1",
                Some(0),
                true,
                r#"{"count":0,"oids":[]}"#,
                None,
                None,
                2_300,
            )
            .unwrap();
        assert_eq!(
            claim,
            ExitClaim::AlreadyClaimed {
                state: "merged".into()
            }
        );
        assert_eq!(store.run("R1").unwrap().unwrap().state, "merged");

        let missing = store.record_agent_exit(
            "R9",
            Some(0),
            true,
            r#"{"count":0,"oids":[]}"#,
            None,
            None,
            2_300,
        );
        assert!(matches!(missing, Err(StoreError::RunNotFound { .. })));
    }

    #[test]
    fn finish_run_settles_exactly_once() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        running_r1(&mut store);
        store
            .record_agent_exit(
                "R1",
                Some(0),
                true,
                r#"{"count":1,"oids":["abc"]}"#,
                None,
                None,
                2_200,
            )
            .unwrap();

        store
            .finish_run("R1", "T1", Some(0), Outcome::Merged, &[], None, 2_300)
            .unwrap();
        assert_eq!(store.ticket_state("T1").unwrap().as_deref(), Some("merged"));
        assert_eq!(store.active_run_for_ticket("T1").unwrap(), None);
        let evidence = store.run_evidence("R1").unwrap();

        store
            .finish_run("R1", "T1", Some(1), Outcome::Failed, &[], None, 2_400)
            .unwrap();
        let run = store.run("R1").unwrap().unwrap();
        assert_eq!(run.state, "merged");
        assert_eq!(run.exit_code, Some(0));
        assert_eq!(store.ticket_state("T1").unwrap().as_deref(), Some("merged"));
        assert_eq!(store.run_evidence("R1").unwrap(), evidence);
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
        assert_eq!(retried.attempt, 1);

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
    fn notes_round_trip_in_arrival_order() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();

        assert_eq!(store.next_note_ordinal().unwrap(), 1);
        store.insert_note("N1", "R1", "first", 3_000).unwrap();
        store.insert_note("N2", "R1", "second", 3_000).unwrap();
        assert_eq!(store.next_note_ordinal().unwrap(), 3);

        assert_eq!(
            store.notes_for_run("R1").unwrap(),
            vec!["first".to_owned(), "second".to_owned()]
        );
        assert!(store.notes_for_run("R2").unwrap().is_empty());
    }

    #[test]
    fn version_three_migrates_ticket_metadata_and_newer_schemas_are_rejected() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sloop.db");
        drop(Store::open(&path, 1_000).unwrap());

        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "DROP TABLE ticket_blockers;
                 ALTER TABLE tickets DROP COLUMN name;
                 ALTER TABLE tickets DROP COLUMN worktree;
                 ALTER TABLE tickets DROP COLUMN flow;
                 ALTER TABLE tickets DROP COLUMN missing_at_ms;
                 ALTER TABLE runs DROP COLUMN worker_socket_path;",
            )
            .unwrap();
        connection.pragma_update(None, "user_version", 3).unwrap();
        drop(connection);

        let store = Store::open(&path, 2_000).unwrap();
        assert!(!store.paused().unwrap());
        store
            .insert_local_project(
                "default",
                ".agents/sloop/projects/default.md",
                "Default",
                2_000,
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
                Some("codex"),
                None,
                None,
                "default",
                TicketState::Ready,
                2_000,
            )
            .unwrap();
        assert_eq!(
            store.ticket("T1").unwrap().unwrap().target.as_deref(),
            Some("codex")
        );
        drop(store);

        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.pragma_update(None, "user_version", 99).unwrap();
        drop(connection);

        assert!(matches!(
            Store::open(&path, 3_000),
            Err(StoreError::UnsupportedSchemaVersion(99))
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
    fn finishing_a_run_settles_ticket_lease_and_evidence_atomically() {
        use crate::outcome::Outcome;
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();
        store
            .record_aftercare_stage(
                "R1",
                &super::StageRecord {
                    stage_index: 0,
                    stage: "test".into(),
                    state: "passed".into(),
                    started_at_ms: 2_500,
                    finished_at_ms: 2_900,
                    exit_code: Some(0),
                    output_ref: "runs/R1/output.ndjson".into(),
                },
            )
            .unwrap();

        store
            .finish_run(
                "R1",
                "T1",
                Some(0),
                Outcome::Merged,
                &[super::EvidenceRecord {
                    kind: "commits_observed",
                    data_json: "{\"oids\":[\"abc\",\"def\"]}".into(),
                }],
                None,
                3_000,
            )
            .unwrap();

        assert_eq!(store.ticket_state("T1").unwrap().unwrap(), "merged");
        let run = store.run("R1").unwrap().unwrap();
        assert_eq!(run.state, "merged");
        assert_eq!(run.exit_code, Some(0));
        assert_eq!(run.exited_at_ms, Some(3_000));
        let evidence = store.run_evidence("R1").unwrap();
        assert_eq!(evidence[0].0, "commits_observed");
        assert_eq!(store.aftercare_stages("R1").unwrap()[0].stage, "test");
        // The lease is gone: the same run cannot renew it.
        assert!(store.renew_lease("T1", "R1", 60_000, 3_100).is_err());
    }

    #[test]
    fn finishing_a_run_is_idempotent() {
        use crate::outcome::Outcome;
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();
        let evidence = [super::EvidenceRecord {
            kind: "exit_classified",
            data_json: "{\"exit_code\":1}".into(),
        }];

        store
            .finish_run("R1", "T1", Some(1), Outcome::Failed, &evidence, None, 3_000)
            .unwrap();
        store
            .finish_run("R1", "T1", Some(1), Outcome::Failed, &evidence, None, 3_100)
            .unwrap();

        assert_eq!(store.run_evidence("R1").unwrap().len(), 1);
        assert_eq!(store.run("R1").unwrap().unwrap().exited_at_ms, Some(3_000));
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

    #[test]
    fn a_cancelled_outcome_returns_the_ticket_to_ready() {
        use crate::outcome::Outcome;
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();

        assert!(!store.cancellation_requested("R1").unwrap());
        store.record_cancel_requested("R1", 2_500).unwrap();
        store.record_cancel_requested("R1", 2_600).unwrap();
        assert!(store.cancellation_requested("R1").unwrap());

        store
            .finish_run("R1", "T1", None, Outcome::Cancelled, &[], None, 3_000)
            .unwrap();
        assert_eq!(store.ticket_state("T1").unwrap().unwrap(), "ready");
        assert_eq!(store.ticket_counts().unwrap().ready, 1);

        // Intent stayed deduplicated to one evidence row.
        let cancels = store
            .run_evidence("R1")
            .unwrap()
            .into_iter()
            .filter(|(kind, _)| kind == "cancel_requested")
            .count();
        assert_eq!(cancels, 1);
    }

    #[test]
    fn paused_state_persists() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sloop.db");

        let store = Store::open(&path, 1_000).unwrap();
        store.set_paused(true, 2_000).unwrap();
        drop(store);

        assert!(Store::open(&path, 3_000).unwrap().paused().unwrap());
    }
}
