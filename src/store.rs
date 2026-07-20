use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use crate::domain::ticket::TicketState;

pub const SCHEMA_VERSION: u32 = 13;

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
    body            TEXT,
    held_reason     TEXT,
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
    cleanup_eligible_at_ms INTEGER,
    cleaned_at_ms         INTEGER,
    flow_json             TEXT,
    ticket_json           TEXT,
    created_at_ms         INTEGER NOT NULL,
    updated_at_ms         INTEGER NOT NULL
);

CREATE INDEX runs_by_ticket ON runs(ticket_id, created_at_ms);
CREATE INDEX runs_by_activation ON runs(activation_id, created_at_ms);

-- A lease is time-bounded ownership of a ticket by the daemon, taken
-- atomically at claim time. `ticket_id` is the PRIMARY KEY and `run_id` is
-- UNIQUE, so the engine itself enforces at most one lease per ticket and per
-- run: the durable guard against double-spawn, backstopping the conditional
-- `UPDATE ... WHERE state='ready'` in `claim_ticket`.
--
-- Leases are held only by the daemon; `owner_id` records which daemon process
-- took the claim. Workers never hold, renew, or observe leases — a worker's
-- only credential is a per-run capability token granting the worker verbs on
-- its own run.
--
-- `expires_at_ms` gates renewal only: an expired lease cannot be renewed, so a
-- revived process cannot resurrect a claim recovery has decided is lost.
-- Liveness of a run is determined by process identity (pid + pid start time +
-- process group id), never by lease expiry.
--
-- The daemon renews the lease of every run it supervises, so `expires_at_ms`
-- stays in the future for as long as a run is alive and an expired row means
-- nobody was there to renew it. Because renewal is strict, a daemon returning
-- after longer than the TTL re-arms a readopted run's lapsed lease through
-- `readopt_lease` rather than through renewal.
--
-- A lease is released by deleting its row: on settlement (`finish_run`) or on
-- claim rollback (`abort_claim`). An expired-but-present row is evidence of an
-- owner that died mid-work.
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
    draining        INTEGER NOT NULL DEFAULT 0 CHECK (draining IN (0, 1)),
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
SELECT 'note', COALESCE(MAX(CAST(SUBSTR(id, 2) AS INTEGER)), 0) + 1 FROM notes;
";

const RUN_SNAPSHOT_COLUMNS: &str = "
ALTER TABLE runs ADD COLUMN flow_json TEXT;
ALTER TABLE runs ADD COLUMN ticket_json TEXT;
";

// The activity feed read by `sloop watch`. Rows are written inside the same
// transaction as the state transition they describe, so the feed can never
// disagree with the tables it narrates.
const EVENTS_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS events (
    sequence        INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at_ms  INTEGER NOT NULL,
    kind            TEXT NOT NULL,
    run_id          TEXT,
    ticket_id       TEXT,
    data_json       TEXT NOT NULL DEFAULT '{}'
);
";

const TICKET_SOURCE_COLUMNS: &str = "
ALTER TABLE tickets ADD COLUMN body TEXT;
ALTER TABLE tickets ADD COLUMN held_reason TEXT;
";

const RESTART_DRAINING_COLUMN: &str = "
ALTER TABLE scheduler_state ADD COLUMN draining INTEGER NOT NULL DEFAULT 0
CHECK (draining IN (0, 1));
";

const WORKTREE_CLEANUP_COLUMNS: &str = "
ALTER TABLE runs ADD COLUMN cleanup_eligible_at_ms INTEGER;
ALTER TABLE runs ADD COLUMN cleaned_at_ms INTEGER;
";

/// Every value the `runs.state` column can hold. The ladder runs
/// `claimed → running → aftercare` and then to one terminal state, either an
/// outcome written by [`Store::finish_run`] or `aborted` from a rolled-back
/// claim. Values are the exact strings already stored; there is no migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    Claimed,
    Running,
    Aftercare,
    /// A claim rolled back before a process existed.
    Aborted,
    Merged,
    Failed,
    NeedsReview,
    Cancelled,
    RateLimited,
    Orphaned,
}

/// The nonterminal run states, in ladder order. A run in one of these still
/// owns its lease and is a candidate for recovery.
pub(crate) const NONTERMINAL_RUN_STATES: [RunState; 3] =
    [RunState::Claimed, RunState::Running, RunState::Aftercare];

impl RunState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Claimed => "claimed",
            Self::Running => "running",
            Self::Aftercare => "aftercare",
            Self::Aborted => "aborted",
            Self::Merged => "merged",
            Self::Failed => "failed",
            Self::NeedsReview => "needs_review",
            Self::Cancelled => "cancelled",
            Self::RateLimited => "rate_limited",
            Self::Orphaned => "orphaned",
        }
    }

    /// Reads a state written by an older or newer binary. An unrecognized
    /// value is an error rather than a fallback: silently treating it as
    /// nonterminal would let the daemon act on a run it cannot classify.
    pub fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "claimed" => Ok(Self::Claimed),
            "running" => Ok(Self::Running),
            "aftercare" => Ok(Self::Aftercare),
            "aborted" => Ok(Self::Aborted),
            "merged" => Ok(Self::Merged),
            "failed" => Ok(Self::Failed),
            "needs_review" => Ok(Self::NeedsReview),
            "cancelled" => Ok(Self::Cancelled),
            "rate_limited" => Ok(Self::RateLimited),
            "orphaned" => Ok(Self::Orphaned),
            other => Err(StoreError::UnknownRunState {
                state: other.into(),
            }),
        }
    }

    /// Whether the run has stopped: no lease, no supervision, no renewal.
    pub fn is_terminal(self) -> bool {
        !NONTERMINAL_RUN_STATES.contains(&self)
    }
}

/// Reads `runs.state` as a typed value. An unrecognized string fails the row
/// rather than defaulting, so a state this binary does not understand can
/// never be mistaken for a live or a settled run.
impl rusqlite::types::FromSql for RunState {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let text = value.as_str()?;
        Self::parse(text).map_err(|error| rusqlite::types::FromSqlError::Other(Box::new(error)))
    }
}

/// Binds the nonterminal states as `?1, ?2, ?3` for the `IN` clauses that
/// select live runs.
fn nonterminal_state_params() -> [&'static str; 3] {
    [
        NONTERMINAL_RUN_STATES[0].as_str(),
        NONTERMINAL_RUN_STATES[1].as_str(),
        NONTERMINAL_RUN_STATES[2].as_str(),
    ]
}

impl From<crate::outcome::Outcome> for RunState {
    fn from(outcome: crate::outcome::Outcome) -> Self {
        use crate::outcome::Outcome;
        match outcome {
            Outcome::Merged => Self::Merged,
            Outcome::Failed => Self::Failed,
            Outcome::NeedsReview => Self::NeedsReview,
            Outcome::Cancelled => Self::Cancelled,
            Outcome::RateLimited => Self::RateLimited,
            Outcome::Orphaned => Self::Orphaned,
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

/// One appended `run_evidence` row: a kind plus kind-specific JSON facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceRecord {
    pub kind: &'static str,
    pub data_json: String,
}

/// One row of the activity feed, ordered by `sequence`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRecord {
    pub sequence: i64,
    pub occurred_at_ms: i64,
    pub kind: String,
    pub run_id: Option<String>,
    pub ticket_id: Option<String>,
    pub data_json: String,
}

/// Appends one activity-feed row. Callers pass the transaction performing the
/// transition so the event commits or rolls back with it.
fn record_event(
    connection: &Connection,
    now_ms: i64,
    kind: &str,
    run_id: Option<&str>,
    ticket_id: Option<&str>,
    data_json: &str,
) -> Result<(), rusqlite::Error> {
    connection.execute(
        "INSERT INTO events (occurred_at_ms, kind, run_id, ticket_id, data_json)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![now_ms, kind, run_id, ticket_id, data_json],
    )?;
    Ok(())
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
    pub verdict_source: String,
    pub reason: Option<String>,
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
    /// Carried so callers can render the run's alias without a second lookup.
    pub attempt: i64,
    pub ticket_name: String,
    pub project_id: String,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRecord {
    pub id: String,
    pub ticket_id: String,
    /// The per-ticket attempt this run served. Frozen at claim, so it is the
    /// second half of the run's alias.
    pub attempt: i64,
    pub state: String,
    pub branch: Option<String>,
    pub worktree_path: Option<String>,
    pub pid: Option<i64>,
    pub pid_start_time: Option<i64>,
    pub process_group_id: Option<i64>,
    pub exit_code: Option<i64>,
    pub exited_at_ms: Option<i64>,
    pub flow_json: Option<String>,
    pub ticket_json: Option<String>,
}

/// Every `RunRecord` read uses this projection so the column order and the
/// mapper below can never drift apart.
const RUN_RECORD_SELECT: &str = "SELECT id, ticket_id, attempt, state, branch, worktree_path, pid,
            pid_start_time, process_group_id, exit_code, exited_at_ms,
            flow_json, ticket_json
     FROM runs";

const TICKET_RECORD_SELECT: &str =
    "SELECT id, project_id, file_path, source, source_ref, state, name, worktree,
            target, model, effort, flow, attempts, body, held_reason, created_at_ms
     FROM tickets";

fn run_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<RunRecord> {
    Ok(RunRecord {
        id: row.get(0)?,
        ticket_id: row.get(1)?,
        attempt: row.get(2)?,
        state: row.get(3)?,
        branch: row.get(4)?,
        worktree_path: row.get(5)?,
        pid: row.get(6)?,
        pid_start_time: row.get(7)?,
        process_group_id: row.get(8)?,
        exit_code: row.get(9)?,
        exited_at_ms: row.get(10)?,
        flow_json: row.get(11)?,
        ticket_json: row.get(12)?,
    })
}

/// A `needs_review` ticket paired with the preserved run branch whose tip the
/// daemon can test for external integration against the default branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NeedsReviewBranch {
    pub(crate) ticket_id: String,
    pub(crate) run_id: String,
    pub(crate) branch: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorktreeCleanupCandidate {
    pub(crate) run_id: String,
    pub(crate) ticket_id: String,
    pub(crate) branch: String,
    pub(crate) worktree_path: String,
    pub(crate) cleanup_eligible_at_ms: i64,
}

/// One lease that must be classified when a daemon starts. Process identity
/// and worker credentials are returned only to the daemon recovery path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecoverableRun {
    pub(crate) id: String,
    pub(crate) ticket_id: String,
    pub(crate) target: String,
    pub(crate) state: RunState,
    pub(crate) branch: Option<String>,
    pub(crate) worktree_path: Option<String>,
    pub(crate) pid: Option<i64>,
    pub(crate) pid_start_time: Option<i64>,
    pub(crate) process_group_id: Option<i64>,
    pub(crate) worker_token: Option<String>,
    pub(crate) worker_socket_path: Option<String>,
    pub(crate) exit_code: Option<i64>,
    pub(crate) lease_expires_at_ms: i64,
    pub(crate) flow_json: Option<String>,
    pub(crate) ticket_json: Option<String>,
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
                transaction.execute_batch(EVENTS_SCHEMA)?;
                transaction.execute(
                    "INSERT INTO scheduler_state (singleton, paused, draining, updated_at_ms)
                     VALUES (1, 0, 0, ?1)",
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
                transaction.execute_batch(RUN_SNAPSHOT_COLUMNS)?;
                transaction.execute_batch(ID_COUNTER_SCHEMA)?;
                transaction.execute_batch(EVENTS_SCHEMA)?;
                transaction.execute_batch(TICKET_SOURCE_COLUMNS)?;
                transaction.execute_batch(RESTART_DRAINING_COLUMN)?;
                transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
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
                transaction.execute_batch(RUN_SNAPSHOT_COLUMNS)?;
                transaction.execute_batch(ID_COUNTER_SCHEMA)?;
                transaction.execute_batch(EVENTS_SCHEMA)?;
                transaction.execute_batch(TICKET_SOURCE_COLUMNS)?;
                transaction.execute_batch(RESTART_DRAINING_COLUMN)?;
                transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
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
                transaction.execute_batch(RUN_SNAPSHOT_COLUMNS)?;
                transaction.execute_batch(ID_COUNTER_SCHEMA)?;
                transaction.execute_batch(EVENTS_SCHEMA)?;
                transaction.execute_batch(TICKET_SOURCE_COLUMNS)?;
                transaction.execute_batch(RESTART_DRAINING_COLUMN)?;
                transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
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
                transaction.execute_batch(RUN_SNAPSHOT_COLUMNS)?;
                transaction.execute_batch(ID_COUNTER_SCHEMA)?;
                transaction.execute_batch(EVENTS_SCHEMA)?;
                transaction.execute_batch(TICKET_SOURCE_COLUMNS)?;
                transaction.execute_batch(RESTART_DRAINING_COLUMN)?;
                transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
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
                transaction.execute_batch(RUN_SNAPSHOT_COLUMNS)?;
                transaction.execute_batch(ID_COUNTER_SCHEMA)?;
                transaction.execute_batch(EVENTS_SCHEMA)?;
                transaction.execute_batch(TICKET_SOURCE_COLUMNS)?;
                transaction.execute_batch(RESTART_DRAINING_COLUMN)?;
                transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
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
                transaction.execute_batch(RUN_SNAPSHOT_COLUMNS)?;
                transaction.execute_batch(ID_COUNTER_SCHEMA)?;
                transaction.execute_batch(EVENTS_SCHEMA)?;
                transaction.execute_batch(TICKET_SOURCE_COLUMNS)?;
                transaction.execute_batch(RESTART_DRAINING_COLUMN)?;
                transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
                transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
                transaction.commit()?;
                Ok(())
            }
            7 => {
                let transaction = self
                    .connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute_batch(RUN_SNAPSHOT_COLUMNS)?;
                transaction.execute_batch(ID_COUNTER_SCHEMA)?;
                transaction.execute_batch(EVENTS_SCHEMA)?;
                transaction.execute_batch(TICKET_SOURCE_COLUMNS)?;
                transaction.execute_batch(RESTART_DRAINING_COLUMN)?;
                transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
                transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
                transaction.commit()?;
                Ok(())
            }
            8 => {
                let transaction = self
                    .connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute_batch(RUN_SNAPSHOT_COLUMNS)?;
                transaction.execute_batch(EVENTS_SCHEMA)?;
                transaction.execute_batch(TICKET_SOURCE_COLUMNS)?;
                transaction.execute_batch(RESTART_DRAINING_COLUMN)?;
                transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
                transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
                transaction.commit()?;
                Ok(())
            }
            9 => {
                let transaction = self
                    .connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute_batch(EVENTS_SCHEMA)?;
                transaction.execute_batch(TICKET_SOURCE_COLUMNS)?;
                transaction.execute_batch(RESTART_DRAINING_COLUMN)?;
                transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
                transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
                transaction.commit()?;
                Ok(())
            }
            10 => {
                let transaction = self
                    .connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute_batch(TICKET_SOURCE_COLUMNS)?;
                transaction.execute_batch(RESTART_DRAINING_COLUMN)?;
                transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
                transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
                transaction.commit()?;
                Ok(())
            }
            11 => {
                let transaction = self
                    .connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute_batch(RESTART_DRAINING_COLUMN)?;
                transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
                transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
                transaction.commit()?;
                Ok(())
            }
            12 => {
                let transaction = self
                    .connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
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
        let transaction = self.immediate_transaction()?;
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
        let transaction = self.immediate_transaction()?;
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
        let existing: BTreeMap<String, TicketRecord> = self
            .tickets()?
            .into_iter()
            .map(|ticket| (ticket.id.clone(), ticket))
            .collect();
        let desired_ticket_ids: BTreeSet<&str> =
            tickets.iter().map(|ticket| ticket.id.as_str()).collect();
        let desired_project_ids: BTreeSet<&str> = project_ids.iter().map(String::as_str).collect();
        let transaction = self.immediate_transaction()?;

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
                    transaction.execute(
                        "UPDATE runs SET cleanup_eligible_at_ms = ?2
                         WHERE ticket_id = ?1 AND state IN ('failed', 'needs_review')
                           AND cleanup_eligible_at_ms IS NULL AND cleaned_at_ms IS NULL",
                        params![ticket.id, now_ms],
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
        self.connection.execute(
            "UPDATE tickets SET target = ?2, model = ?3, effort = ?4, updated_at_ms = ?5 WHERE id = ?1",
            params![id, target, model, effort, now_ms],
        )?;
        Ok(())
    }

    pub fn update_ticket_body(&self, id: &str, body: &str, now_ms: i64) -> Result<(), StoreError> {
        self.connection.execute(
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
                &format!("{TICKET_RECORD_SELECT} WHERE id = ?1"),
                params![id],
                ticket_record,
            )
            .optional()?;
        if let Some(ticket) = ticket.as_mut() {
            ticket.blocked_by = self.ticket_blockers(&ticket.id)?;
        }
        Ok(ticket)
    }

    /// Resolves a ticket by its human-facing name. Names are not guaranteed
    /// unique across projects, so the lowest id wins deterministically; `show`
    /// tries this only after an exact id match fails.
    pub fn ticket_by_name(&self, name: &str) -> Result<Option<TicketRecord>, StoreError> {
        let mut ticket = self
            .connection
            .query_row(
                &format!("{TICKET_RECORD_SELECT} WHERE name = ?1 ORDER BY id LIMIT 1"),
                params![name],
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
                &format!("{TICKET_RECORD_SELECT} WHERE file_path = ?1"),
                params![file_path],
                ticket_record,
            )
            .optional()?;
        if let Some(ticket) = ticket.as_mut() {
            ticket.blocked_by = self.ticket_blockers(&ticket.id)?;
        }
        Ok(ticket)
    }

    pub fn ticket_by_source_ref(
        &self,
        source: &str,
        source_ref: &str,
    ) -> Result<Option<TicketRecord>, StoreError> {
        let mut ticket = self
            .connection
            .query_row(
                &format!("{TICKET_RECORD_SELECT} WHERE source = ?1 AND source_ref = ?2"),
                params![source, source_ref],
                ticket_record,
            )
            .optional()?;
        if let Some(ticket) = ticket.as_mut() {
            ticket.blocked_by = self.ticket_blockers(&ticket.id)?;
        }
        Ok(ticket)
    }

    /// Every ticket, newest registration first. `sloop list` answers "what is
    /// going on right now?", so recency leads; SQL settles the coarse order and
    /// a stable pass re-breaks ties on the id's numeric ordinal, which string
    /// comparison gets wrong (`TICK-9` sorts above `TICK-38`). Ids with no
    /// ordinal keep the deterministic `id DESC` order SQL gave them.
    pub fn tickets(&self) -> Result<Vec<TicketRecord>, StoreError> {
        let mut statement = self.connection.prepare(&format!(
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
        let mut blockers = self.all_ticket_blockers()?;
        for ticket in &mut tickets {
            ticket.blocked_by = blockers.remove(&ticket.id).unwrap_or_default();
        }
        Ok(tickets)
    }

    pub fn tickets_for_project(&self, project_id: &str) -> Result<Vec<TicketRecord>, StoreError> {
        let mut statement = self.connection.prepare(&format!(
            "{TICKET_RECORD_SELECT} WHERE project_id = ?1 ORDER BY id"
        ))?;
        let mut tickets = statement
            .query_map(params![project_id], ticket_record)?
            .collect::<Result<Vec<_>, _>>()?;
        let mut blockers = self.all_ticket_blockers()?;
        for ticket in &mut tickets {
            ticket.blocked_by = blockers.remove(&ticket.id).unwrap_or_default();
        }
        Ok(tickets)
    }

    pub fn ticket_dependencies(
        &self,
    ) -> Result<std::collections::BTreeMap<String, Vec<String>>, StoreError> {
        let mut dependencies = std::collections::BTreeMap::new();
        let mut statement = self.connection.prepare("SELECT id FROM tickets")?;
        let ids = statement.query_map([], |row| row.get::<_, String>(0))?;
        for id in ids {
            dependencies.insert(id?, Vec::new());
        }
        for (ticket_id, blockers) in self.all_ticket_blockers()? {
            if let Some(entry) = dependencies.get_mut(&ticket_id) {
                *entry = blockers;
            }
        }
        Ok(dependencies)
    }

    /// Every ticket's blockers in one pass, keeping each list in declared
    /// order. Loading these per ticket turns any all-tickets read into a
    /// query per row, which is what the post path pays cycle checks against.
    fn all_ticket_blockers(&self) -> Result<BTreeMap<String, Vec<String>>, StoreError> {
        let mut statement = self.connection.prepare(
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
        let previous = self
            .ticket_state(id)?
            .ok_or_else(|| StoreError::TicketNotFound {
                ticket_id: id.into(),
            })?;
        let transaction = self.immediate_transaction()?;
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
        transaction.execute(
            "UPDATE runs SET cleanup_eligible_at_ms = ?2
             WHERE ticket_id = ?1 AND state = 'failed'
               AND cleanup_eligible_at_ms IS NULL AND cleaned_at_ms IS NULL",
            params![id, now_ms],
        )?;
        transaction.commit()?;
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
        let transaction = self.immediate_transaction()?;
        let changed = transaction.execute(
            "UPDATE runs
             SET state = ?10, branch = ?2, worktree_path = ?3, pid = ?4,
                 pid_start_time = ?5, process_group_id = ?6, worker_token = ?7,
                 worker_socket_path = ?8, started_at_ms = ?9, updated_at_ms = ?9
             WHERE id = ?1 AND state = ?11 AND exited_at_ms IS NULL",
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
                RunState::Running.as_str(),
                RunState::Claimed.as_str(),
            ],
        )?;
        if changed != 1 {
            let state = transaction
                .query_row(
                    "SELECT state FROM runs WHERE id = ?1",
                    params![run_id],
                    |row| row.get(0),
                )
                .optional()?;
            return Err(StoreError::RunStateConflict {
                run_id: run_id.into(),
                state,
                requested: RunState::Running.as_str().into(),
            });
        }
        let ticket_id: String = transaction.query_row(
            "SELECT ticket_id FROM runs WHERE id = ?1",
            params![run_id],
            |row| row.get(0),
        )?;
        record_event(
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

        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let run_state = RunState::from(outcome);
        let changed = transaction.execute(
            "UPDATE runs
             SET state = ?2, exited_at_ms = ?3, exit_code = ?4, updated_at_ms = ?3,
                 cleanup_eligible_at_ms = CASE WHEN ?2 = ?5 THEN ?3 ELSE NULL END
             WHERE id = ?1 AND exited_at_ms IS NULL",
            params![
                run_id,
                run_state.as_str(),
                now_ms,
                exit_code,
                RunState::Merged.as_str(),
            ],
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
        record_event(
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
        let evidence_json = serde_json::json!({
            "output": stage.output_ref,
            "verdict_source": stage.verdict_source,
            "reason": stage.reason,
        })
        .to_string();
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
                let evidence = evidence_json
                    .as_deref()
                    .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok());
                Ok(StageRecord {
                    stage_index: row.get::<_, i64>(0)? as usize,
                    stage: row.get(1)?,
                    state: row.get(2)?,
                    started_at_ms: row.get(3)?,
                    finished_at_ms: row.get(4)?,
                    exit_code: row.get(5)?,
                    output_ref,
                    verdict_source: evidence
                        .as_ref()
                        .and_then(|value| value["verdict_source"].as_str())
                        .unwrap_or("exit_code")
                        .to_owned(),
                    reason: evidence
                        .as_ref()
                        .and_then(|value| value["reason"].as_str())
                        .map(str::to_owned),
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
             SET state = ?4, exit_code = ?2, updated_at_ms = ?3
             WHERE id = ?1 AND state = ?5 AND exited_at_ms IS NULL",
            params![
                run_id,
                exit_code,
                now_ms,
                RunState::Aftercare.as_str(),
                RunState::Running.as_str(),
            ],
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
        let dedupe_key = format!("verdict:{run_id}:{stage}");
        let data_json =
            serde_json::json!({"stage": stage, "verdict": verdict, "reason": reason}).to_string();
        let inserted = self.connection.execute(
            "INSERT OR IGNORE INTO run_evidence
                 (run_id, kind, observed_at_ms, dedupe_key, data_json)
             VALUES (?1, 'stage_verdict', ?2, ?3, ?4)",
            params![run_id, now_ms, dedupe_key, data_json],
        )?;
        Ok(inserted == 1)
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
    pub(crate) fn abort_claim(
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
             SET state = ?3, exited_at_ms = ?2, updated_at_ms = ?2
             WHERE id = ?1 AND exited_at_ms IS NULL",
            params![run_id, now_ms, RunState::Aborted.as_str()],
        )?;
        transaction.execute(
            "UPDATE tickets SET state = 'ready', held_reason = NULL, updated_at_ms = ?2
             WHERE id = ?1 AND state = 'claimed'",
            params![ticket_id, now_ms],
        )?;
        record_event(
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
        let mut statement = self.connection.prepare(
            "SELECT sequence, occurred_at_ms, kind, run_id, ticket_id, data_json
             FROM events WHERE sequence > ?1 ORDER BY sequence LIMIT ?2",
        )?;
        statement
            .query_map(params![after, limit as i64], |row| {
                Ok(EventRecord {
                    sequence: row.get(0)?,
                    occurred_at_ms: row.get(1)?,
                    kind: row.get(2)?,
                    run_id: row.get(3)?,
                    ticket_id: row.get(4)?,
                    data_json: row.get(5)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    pub fn latest_event_sequence(&self) -> Result<i64, StoreError> {
        let latest = self.connection.query_row(
            "SELECT COALESCE(MAX(sequence), 0) FROM events",
            [],
            |row| row.get(0),
        )?;
        Ok(latest)
    }

    /// Drops all but the newest `keep` activity-feed rows. Sequences are never
    /// reused after a trim, so cursors held by watchers stay valid.
    pub fn trim_events(&self, keep: i64) -> Result<(), StoreError> {
        self.connection.execute(
            "DELETE FROM events
             WHERE sequence <= (SELECT COALESCE(MAX(sequence), 0) FROM events) - ?1",
            params![keep],
        )?;
        Ok(())
    }

    pub fn run(&self, id: &str) -> Result<Option<RunRecord>, StoreError> {
        let run = self
            .connection
            .query_row(
                &format!("{RUN_RECORD_SELECT} WHERE id = ?1"),
                params![id],
                run_record,
            )
            .optional()?;
        Ok(run)
    }

    /// The run a `<ticket>-r<attempt>` alias names. The pair is unique because
    /// attempts are allocated once per ticket at claim time.
    pub fn run_for_ticket_attempt(
        &self,
        ticket_id: &str,
        attempt: i64,
    ) -> Result<Option<RunRecord>, StoreError> {
        let run = self
            .connection
            .query_row(
                &format!(
                    "{RUN_RECORD_SELECT} WHERE ticket_id = ?1 AND attempt = ?2
                     ORDER BY created_at_ms DESC LIMIT 1"
                ),
                params![ticket_id, attempt],
                run_record,
            )
            .optional()?;
        Ok(run)
    }

    /// Every run a ticket has produced, newest attempt first, so a bare ticket
    /// reference can name the latest run and still report the earlier ones.
    pub fn runs_for_ticket(&self, ticket_id: &str) -> Result<Vec<RunRecord>, StoreError> {
        let mut statement = self.connection.prepare(&format!(
            "{RUN_RECORD_SELECT} WHERE ticket_id = ?1 ORDER BY attempt DESC, created_at_ms DESC"
        ))?;
        let runs = statement
            .query_map(params![ticket_id], run_record)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(runs)
    }

    /// Runs whose internal id starts with `prefix`. More than one row means the
    /// reference is ambiguous, so the caller needs the candidates, not a pick.
    pub fn runs_with_id_prefix(&self, prefix: &str) -> Result<Vec<RunRecord>, StoreError> {
        // `LIKE` would treat `%` and `_` in a prefix as wildcards; run ids are
        // hexadecimal, but comparing on the substring keeps that beyond doubt.
        let mut statement = self.connection.prepare(&format!(
            "{RUN_RECORD_SELECT} WHERE SUBSTR(id, 1, ?2) = ?1 ORDER BY created_at_ms, id"
        ))?;
        let runs = statement
            .query_map(params![prefix, prefix.len() as i64], run_record)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(runs)
    }

    /// Every `needs_review` ticket paired with the branch of the run that
    /// produced it, so the daemon can freshly test each branch tip for external
    /// integration. Only the newest `needs_review` run with a branch is
    /// returned per ticket; the tip itself is never cached here.
    pub(crate) fn needs_review_branches(&self) -> Result<Vec<NeedsReviewBranch>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT t.id, r.id, r.branch
             FROM tickets t
             JOIN runs r ON r.id = (
                 SELECT r2.id FROM runs r2
                 WHERE r2.ticket_id = t.id
                   AND r2.state = 'needs_review'
                   AND r2.branch IS NOT NULL
                 ORDER BY r2.created_at_ms DESC, r2.id DESC
                 LIMIT 1
             )
             WHERE t.state = 'needs_review'",
        )?;
        statement
            .query_map([], |row| {
                Ok(NeedsReviewBranch {
                    ticket_id: row.get(0)?,
                    run_id: row.get(1)?,
                    branch: row.get(2)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    pub(crate) fn worktree_cleanup_candidates(
        &self,
    ) -> Result<Vec<WorktreeCleanupCandidate>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT r.id, r.ticket_id, r.branch, r.worktree_path, r.cleanup_eligible_at_ms
             FROM runs r
             WHERE r.cleanup_eligible_at_ms IS NOT NULL
               AND r.cleaned_at_ms IS NULL
               AND r.branch IS NOT NULL
               AND r.worktree_path IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM leases l WHERE l.run_id = r.id)
             ORDER BY r.cleanup_eligible_at_ms, r.id",
        )?;
        statement
            .query_map([], |row| {
                Ok(WorktreeCleanupCandidate {
                    run_id: row.get(0)?,
                    ticket_id: row.get(1)?,
                    branch: row.get(2)?,
                    worktree_path: row.get(3)?,
                    cleanup_eligible_at_ms: row.get(4)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    pub(crate) fn next_worktree_cleanup_at_ms(
        &self,
        retention_ms: i64,
        now_ms: i64,
    ) -> Result<Option<i64>, StoreError> {
        let eligible_at: Option<i64> = self.connection.query_row(
            "SELECT MIN(r.cleanup_eligible_at_ms)
             FROM runs r
             WHERE r.cleanup_eligible_at_ms IS NOT NULL
               AND r.cleaned_at_ms IS NULL
               AND r.branch IS NOT NULL
               AND r.worktree_path IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM leases l WHERE l.run_id = r.id)",
            [],
            |row| row.get(0),
        )?;
        Ok(eligible_at.and_then(|value| {
            let deadline = value.saturating_add(retention_ms);
            (deadline > now_ms).then_some(deadline)
        }))
    }

    pub(crate) fn mark_run_worktree_cleaned(
        &self,
        candidate: &WorktreeCleanupCandidate,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        let transaction = self.immediate_transaction()?;
        let changed = transaction.execute(
            "UPDATE runs SET cleaned_at_ms = ?2, updated_at_ms = ?2
             WHERE id = ?1 AND cleanup_eligible_at_ms IS NOT NULL
               AND cleaned_at_ms IS NULL
               AND NOT EXISTS (SELECT 1 FROM leases l WHERE l.run_id = runs.id)",
            params![candidate.run_id, now_ms],
        )?;
        if changed == 1 {
            record_event(
                &transaction,
                now_ms,
                "run_worktree_cleaned",
                Some(&candidate.run_id),
                Some(&candidate.ticket_id),
                &serde_json::json!({
                    "branch": candidate.branch,
                    "worktree": candidate.worktree_path,
                })
                .to_string(),
            )?;
        }
        transaction.commit()?;
        Ok(changed == 1)
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
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "UPDATE tickets SET state = 'merged', held_reason = NULL, updated_at_ms = ?2
             WHERE id = ?1 AND state = 'needs_review'",
            params![ticket_id, now_ms],
        )?;
        if changed == 0 {
            transaction.commit()?;
            return Ok(false);
        }
        transaction.execute(
            "UPDATE runs SET cleanup_eligible_at_ms = ?2
             WHERE id = ?1 AND cleanup_eligible_at_ms IS NULL AND cleaned_at_ms IS NULL",
            params![run_id, now_ms],
        )?;
        let data_json = serde_json::json!({
            "branch": branch,
            "branch_tip": branch_tip,
            "observed_default_tip": observed_default_tip,
        })
        .to_string();
        transaction.execute(
            "INSERT OR IGNORE INTO run_evidence
                 (run_id, kind, observed_at_ms, dedupe_key, data_json)
             VALUES (?1, 'external_merge_observed', ?2, 'external_merge:' || ?1, ?3)",
            params![run_id, now_ms, data_json],
        )?;
        record_event(
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
        let run = self
            .connection
            .query_row(
                "SELECT r.id, r.attempt FROM runs r
                 JOIN leases l ON l.run_id = r.id
                 WHERE r.ticket_id = ?1
                   AND r.state IN (?2, ?3, ?4)
                   AND r.exited_at_ms IS NULL
                 ORDER BY r.created_at_ms DESC, r.id DESC LIMIT 1",
                params![
                    ticket_id,
                    NONTERMINAL_RUN_STATES[0].as_str(),
                    NONTERMINAL_RUN_STATES[1].as_str(),
                    NONTERMINAL_RUN_STATES[2].as_str(),
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        Ok(run)
    }

    /// Leased nonterminal runs that consume capacity, oldest first.
    pub fn active_runs(&self) -> Result<Vec<ActiveRun>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT r.id, r.ticket_id, r.attempt, t.name, t.project_id, r.state FROM runs r
             JOIN leases l ON l.run_id = r.id
             JOIN tickets t ON t.id = r.ticket_id
             WHERE r.exited_at_ms IS NULL
               AND r.state IN (?1, ?2, ?3)
             ORDER BY r.created_at_ms, r.id",
        )?;
        let runs = statement
            .query_map(nonterminal_state_params(), |row| {
                Ok(ActiveRun {
                    id: row.get(0)?,
                    ticket_id: row.get(1)?,
                    attempt: row.get(2)?,
                    ticket_name: row.get(3)?,
                    project_id: row.get(4)?,
                    state: row.get(5)?,
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
                    r.worker_socket_path, r.exit_code, l.expires_at_ms, r.flow_json,
                    r.ticket_json
             FROM runs r
             JOIN leases l ON l.run_id = r.id
             JOIN tickets t ON t.id = r.ticket_id
             WHERE r.exited_at_ms IS NULL
               AND r.state IN (?1, ?2, ?3)
             ORDER BY r.created_at_ms, r.id",
        )?;
        statement
            .query_map(nonterminal_state_params(), |row| {
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
                    flow_json: row.get(13)?,
                    ticket_json: row.get(14)?,
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

    /// Begins a write transaction that takes the write lock up front. A
    /// deferred transaction that reads before its first write can hit an
    /// immediate `SQLITE_BUSY` when a sibling connection commits in between:
    /// its snapshot is stale, so SQLite fails fast instead of honoring
    /// `busy_timeout`. Starting immediate makes contending writers queue.
    fn immediate_transaction(&self) -> Result<rusqlite::Transaction<'_>, rusqlite::Error> {
        rusqlite::Transaction::new_unchecked(&self.connection, TransactionBehavior::Immediate)
    }

    fn reserve_ordinal(&self, kind: &str, table: &str) -> Result<i64, StoreError> {
        let transaction = self.immediate_transaction()?;
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

    /// Claims a ready ticket for one run in a single transaction. The
    /// conditional update plus the primary key on `leases.ticket_id` are the
    /// durable guards against a double claim.
    pub(crate) fn claim_ticket(
        &mut self,
        claim: &ClaimRequest<'_>,
        now_ms: i64,
    ) -> Result<ClaimedRun, StoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

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
        let attempt: i64 = transaction.query_row(
            "SELECT COALESCE(MAX(attempt), 0) + 1 FROM runs WHERE ticket_id = ?1",
            params![claim.ticket_id],
            |row| row.get(0),
        )?;

        transaction.execute(
            "INSERT INTO runs
                 (id, activation_id, ticket_id, state, attempt, flow_json, ticket_json,
                  created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
            params![
                claim.run_id,
                claim.activation_id,
                claim.ticket_id,
                RunState::Claimed.as_str(),
                attempt,
                claim.flow_json,
                claim.ticket_json,
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
        record_event(
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
        let changed = self.connection.execute(
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

    pub fn clear_restart_draining(&self, now_ms: i64) -> Result<(), StoreError> {
        self.connection.execute(
            "UPDATE scheduler_state SET draining = 0, updated_at_ms = ?1 WHERE singleton = 1",
            params![now_ms],
        )?;
        Ok(())
    }

    pub fn restart_draining(&self) -> Result<bool, StoreError> {
        let draining: i64 = self.connection.query_row(
            "SELECT draining FROM scheduler_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        Ok(draining != 0)
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

    /// Number of runs currently holding a durable lease. Used as the capacity
    /// gate for repair spawns, which run inside an already-leased run.
    pub(crate) fn active_lease_count(&self) -> Result<usize, StoreError> {
        let count: i64 = self
            .connection
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
        self.connection.execute(
            "INSERT INTO run_evidence
                 (run_id, kind, observed_at_ms, dedupe_key, data_json)
             VALUES (?1, 'repair_attempt', ?2,
                     'repair:' || ?1 || ':' || ?3 || ':' || ?4, ?5)
             ON CONFLICT(dedupe_key) DO UPDATE SET
                 observed_at_ms = excluded.observed_at_ms,
                 data_json = excluded.data_json",
            params![run_id, now_ms, stage, attempt as i64, data_json],
        )?;
        Ok(())
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

    pub fn begin_restart_draining(
        &mut self,
        active_runs: usize,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "UPDATE scheduler_state SET draining = 1, updated_at_ms = ?1
             WHERE singleton = 1 AND draining = 0",
            params![now_ms],
        )? != 0;
        if changed {
            record_event(
                &transaction,
                now_ms,
                "daemon_restart_requested",
                None,
                None,
                &serde_json::json!({"active_runs": active_runs}).to_string(),
            )?;
        }
        transaction.commit()?;
        Ok(changed)
    }

    /// Resuming cancels both scheduler holds in one durable transition.
    pub fn resume_scheduler(&mut self, now_ms: i64) -> Result<bool, StoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let was_draining: bool = transaction.query_row(
            "SELECT draining FROM scheduler_state WHERE singleton = 1",
            [],
            |row| row.get::<_, i64>(0).map(|value| value != 0),
        )?;
        transaction.execute(
            "UPDATE scheduler_state
             SET paused = 0, draining = 0, updated_at_ms = ?1
             WHERE singleton = 1",
            params![now_ms],
        )?;
        transaction.commit()?;
        Ok(was_draining)
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
    use rusqlite::Connection;
    use tempfile::tempdir;

    use super::{
        ActivationKind, ClaimRequest, ExitClaim, NewActivation, ReindexTicket, RunState,
        SCHEMA_VERSION, Store, StoreError,
    };
    use crate::domain::ticket::{TicketSnapshot, TicketState};
    use crate::flow::{Flow, Stage, StageKind, VerdictPolicy};
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
            flow_json: "{}",
            ticket_json: "{}",
        }
    }

    #[test]
    fn claims_persist_flow_and_ticket_snapshots() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        let flow = Flow {
            name: "default".into(),
            stages: vec![
                Stage {
                    name: "build".into(),
                    kind: StageKind::Agent,
                    verdict: VerdictPolicy::Commits,
                    on_fail: None,
                },
                Stage {
                    name: "check".into(),
                    kind: StageKind::Exec {
                        cmd: vec!["cargo".into(), "test".into()],
                    },
                    verdict: VerdictPolicy::Exit,
                    on_fail: None,
                },
            ],
        };
        let ticket = TicketSnapshot {
            id: "T1".into(),
            name: "Ticket one".into(),
            blocked_by: vec![],
            worktree: Some("sloop/T1".into()),
            target: Some("claude".into()),
            model: Some("sonnet".into()),
            effort: Some("medium".into()),
            body: "# Original body\n".into(),
        };
        let flow_json = serde_json::to_string(&flow).unwrap();
        let ticket_json = serde_json::to_string(&ticket).unwrap();

        store
            .claim_ticket(
                &ClaimRequest {
                    flow_json: &flow_json,
                    ticket_json: &ticket_json,
                    ..claim_t1("R1")
                },
                2_000,
            )
            .unwrap();

        let run = store.run("R1").unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<Flow>(run.flow_json.as_deref().unwrap()).unwrap(),
            flow
        );
        assert_eq!(
            serde_json::from_str::<TicketSnapshot>(run.ticket_json.as_deref().unwrap()).unwrap(),
            ticket
        );
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
    fn active_run_for_ticket_tracks_claimed_and_running_runs_only() {
        use crate::outcome::Outcome;

        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        assert_eq!(store.active_run_for_ticket("T1").unwrap(), None);

        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();
        assert_eq!(
            store.active_run_for_ticket("T1").unwrap(),
            Some(("R1".into(), 1))
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
            store.active_run_for_ticket("T1").unwrap(),
            Some(("R1".into(), 1))
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
        assert_eq!(
            store.recoverable_runs().unwrap()[0].state,
            RunState::Aftercare
        );
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
    fn lifecycle_transitions_append_ordered_events() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        running_r1(&mut store);
        store
            .finish_run("R1", "T1", Some(0), Outcome::Merged, &[], None, 2_300)
            .unwrap();

        let events = store.events_after(0, 10).unwrap();
        let kinds: Vec<&str> = events.iter().map(|event| event.kind.as_str()).collect();
        assert_eq!(kinds, ["run_claimed", "run_started", "run_finished"]);
        assert!(events.iter().all(|event| {
            event.run_id.as_deref() == Some("R1") && event.ticket_id.as_deref() == Some("T1")
        }));
        let finished: serde_json::Value = serde_json::from_str(&events[2].data_json).unwrap();
        assert_eq!(finished["outcome"], "merged");
        assert_eq!(finished["ticket_state"], "merged");

        // Settling twice is idempotent, so no duplicate event appears.
        store
            .finish_run("R1", "T1", Some(1), Outcome::Failed, &[], None, 2_400)
            .unwrap();
        assert_eq!(store.latest_event_sequence().unwrap(), events[2].sequence);

        let rest = store.events_after(events[0].sequence, 10).unwrap();
        assert_eq!(rest.len(), 2);
        assert_eq!(rest[0].kind, "run_started");

        store.trim_events(1).unwrap();
        let kept = store.events_after(0, 10).unwrap();
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].sequence, events[2].sequence);
    }

    #[test]
    fn abandoned_claims_append_an_abort_event() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();
        store.abort_claim("R1", "T1", 2_100).unwrap();

        let kinds: Vec<String> = store
            .events_after(0, 10)
            .unwrap()
            .into_iter()
            .map(|event| event.kind)
            .collect();
        assert_eq!(kinds, ["run_claimed", "run_aborted"]);
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
    fn every_run_state_round_trips_through_its_stored_string() {
        let states = [
            RunState::Claimed,
            RunState::Running,
            RunState::Aftercare,
            RunState::Aborted,
            RunState::Merged,
            RunState::Failed,
            RunState::NeedsReview,
            RunState::Cancelled,
            RunState::RateLimited,
            RunState::Orphaned,
        ];
        for state in states {
            assert_eq!(RunState::parse(state.as_str()).unwrap(), state);
        }
        // Every outcome `finish_run` can write is one of those variants.
        for outcome in [
            crate::outcome::Outcome::Merged,
            crate::outcome::Outcome::Failed,
            crate::outcome::Outcome::NeedsReview,
            crate::outcome::Outcome::Cancelled,
            crate::outcome::Outcome::RateLimited,
            crate::outcome::Outcome::Orphaned,
        ] {
            assert_eq!(RunState::from(outcome).as_str(), outcome.as_str());
            assert!(RunState::from(outcome).is_terminal());
        }
        for state in [RunState::Claimed, RunState::Running, RunState::Aftercare] {
            assert!(!state.is_terminal());
        }
        assert!(RunState::Aborted.is_terminal());
    }

    #[test]
    fn an_unknown_stored_run_state_is_an_error_not_a_fallback() {
        let error = RunState::parse("half_running").unwrap_err();
        assert!(matches!(error, StoreError::UnknownRunState { state } if state == "half_running"));
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
                 ALTER TABLE tickets DROP COLUMN body;
                 ALTER TABLE tickets DROP COLUMN held_reason;
                 ALTER TABLE tickets DROP COLUMN missing_at_ms;
                 ALTER TABLE scheduler_state DROP COLUMN draining;
                 ALTER TABLE runs DROP COLUMN worker_socket_path;
                 ALTER TABLE runs DROP COLUMN flow_json;
                 ALTER TABLE runs DROP COLUMN ticket_json;
                 ALTER TABLE runs DROP COLUMN cleanup_eligible_at_ms;
                 ALTER TABLE runs DROP COLUMN cleaned_at_ms;",
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
    fn version_eight_migrates_existing_runs_with_null_snapshots() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sloop.db");
        let mut store = open_seeded(&path);
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();
        drop(store);

        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "ALTER TABLE runs DROP COLUMN flow_json;
                 ALTER TABLE runs DROP COLUMN ticket_json;
                 ALTER TABLE tickets DROP COLUMN body;
                 ALTER TABLE tickets DROP COLUMN held_reason;
                 ALTER TABLE scheduler_state DROP COLUMN draining;
                 ALTER TABLE runs DROP COLUMN cleanup_eligible_at_ms;
                 ALTER TABLE runs DROP COLUMN cleaned_at_ms;",
            )
            .unwrap();
        connection.pragma_update(None, "user_version", 8).unwrap();
        drop(connection);

        let store = Store::open(&path, 3_000).unwrap();
        let run = store.run("R1").unwrap().unwrap();
        assert_eq!(run.flow_json, None);
        assert_eq!(run.ticket_json, None);
    }

    #[test]
    fn version_ten_adds_source_metadata_without_disturbing_ticket_state() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sloop.db");
        let store = open_seeded(&path);
        store
            .connection
            .execute(
                "UPDATE tickets SET state = 'held', attempts = 3 WHERE id = 'T1'",
                [],
            )
            .unwrap();
        drop(store);

        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "ALTER TABLE tickets DROP COLUMN body;
                 ALTER TABLE tickets DROP COLUMN held_reason;
                 ALTER TABLE scheduler_state DROP COLUMN draining;
                 ALTER TABLE runs DROP COLUMN cleanup_eligible_at_ms;
                 ALTER TABLE runs DROP COLUMN cleaned_at_ms;",
            )
            .unwrap();
        connection.pragma_update(None, "user_version", 10).unwrap();
        drop(connection);

        let store = Store::open(&path, 3_000).unwrap();
        let ticket = store.ticket("T1").unwrap().unwrap();
        assert_eq!(ticket.state, "held");
        assert_eq!(ticket.attempts, 3);
        assert_eq!(ticket.body, None);
        assert_eq!(ticket.held_reason, None);
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
                    verdict_source: "exit_code".into(),
                    reason: None,
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

    #[test]
    fn restart_draining_is_durable_idempotent_and_cancelled_by_resume() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sloop.db");
        let mut store = Store::open(&path, 1_000).unwrap();

        assert!(store.begin_restart_draining(2, 2_000).unwrap());
        assert!(!store.begin_restart_draining(2, 2_100).unwrap());
        assert!(store.restart_draining().unwrap());
        assert_eq!(
            store
                .events_after(0, 10)
                .unwrap()
                .iter()
                .filter(|event| event.kind == "daemon_restart_requested")
                .count(),
            1
        );
        drop(store);

        let mut reopened = Store::open(&path, 3_000).unwrap();
        assert!(reopened.restart_draining().unwrap());
        assert!(reopened.resume_scheduler(4_000).unwrap());
        assert!(!reopened.restart_draining().unwrap());
    }

    #[test]
    fn version_eleven_adds_restart_draining_state() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sloop.db");
        drop(Store::open(&path, 1_000).unwrap());
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "ALTER TABLE scheduler_state DROP COLUMN draining;
                 ALTER TABLE runs DROP COLUMN cleanup_eligible_at_ms;
                 ALTER TABLE runs DROP COLUMN cleaned_at_ms;
                 PRAGMA user_version = 11;",
            )
            .unwrap();
        drop(connection);

        let store = Store::open(&path, 2_000).unwrap();
        assert!(!store.restart_draining().unwrap());
        assert_eq!(
            store
                .connection
                .query_row("PRAGMA user_version", [], |row| row.get::<_, u32>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
    }
}
