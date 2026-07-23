use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use rusqlite::{Connection, TransactionBehavior, params};

pub const SCHEMA_VERSION: u32 = 13;

// `synchronous = NORMAL` is the standard WAL pairing: commits skip the
// per-transaction fsync (durability moves to checkpoints), which keeps the
// write lock short under contention. The busy timeout is generous because
// SQLite's busy handler has no fairness queue: under sustained multi-writer
// load a waiter can starve well past a "reasonable" wait before winning.
const CONNECTION_PRAGMAS: &str = "
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 30000;
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

#[derive(Clone)]
pub struct Db(Arc<Mutex<Connection>>);

impl Db {
    pub fn open(path: &Path, now_ms: i64) -> Result<Self, DbError> {
        let mut connection = Connection::open(path).map_err(|source| DbError::Open {
            path: path.to_path_buf(),
            source,
        })?;
        connection.execute_batch(CONNECTION_PRAGMAS)?;
        migrate(&mut connection, now_ms)?;
        Ok(Self(Arc::new(Mutex::new(connection))))
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, Connection> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn migrate(connection: &mut Connection, now_ms: i64) -> Result<(), DbError> {
    let version: u32 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    match version {
        0 => {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute_batch("ALTER TABLE runs ADD COLUMN worker_socket_path TEXT;")?;
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
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute_batch(EVENTS_SCHEMA)?;
            transaction.execute_batch(TICKET_SOURCE_COLUMNS)?;
            transaction.execute_batch(RESTART_DRAINING_COLUMN)?;
            transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
            transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            transaction.commit()?;
            Ok(())
        }
        10 => {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute_batch(TICKET_SOURCE_COLUMNS)?;
            transaction.execute_batch(RESTART_DRAINING_COLUMN)?;
            transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
            transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            transaction.commit()?;
            Ok(())
        }
        11 => {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute_batch(RESTART_DRAINING_COLUMN)?;
            transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
            transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            transaction.commit()?;
            Ok(())
        }
        12 => {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
            transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            transaction.commit()?;
            Ok(())
        }
        SCHEMA_VERSION => Ok(()),
        newer => Err(DbError::UnsupportedSchemaVersion(newer)),
    }
}

#[derive(Debug)]
pub enum DbError {
    Open {
        path: PathBuf,
        source: rusqlite::Error,
    },
    Sqlite(rusqlite::Error),
    UnsupportedSchemaVersion(u32),
}

impl From<rusqlite::Error> for DbError {
    fn from(source: rusqlite::Error) -> Self {
        Self::Sqlite(source)
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open { path, source } => {
                write!(formatter, "cannot open {}: {source}", path.display())
            }
            Self::Sqlite(source) => write!(formatter, "database error: {source}"),
            Self::UnsupportedSchemaVersion(version) => {
                write!(formatter, "unsupported database schema version {version}")
            }
        }
    }
}

impl std::error::Error for DbError {}
