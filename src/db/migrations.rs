//! Schema history.
//!
//! Every constant below is a migration step, and the arms of [`migrate`] replay
//! the steps a database at a given `user_version` has not yet seen. Strings
//! naming things Sloop no longer calls by those names survive here alone: a
//! database written by an older binary still holds them, and only a migration
//! may say so.

use rusqlite::{Connection, TransactionBehavior, params};

use super::{DbError, SCHEMA_VERSION};

/// The stage log's table name before [`UNIFORM_STAGE_DRIVER`] renamed it. Only
/// this module and the fixtures that plant a pre-migration database have any
/// business naming it, so it lives here rather than being spelled out at each
/// of those sites.
#[cfg(test)]
pub(crate) const LEGACY_STAGE_TABLE: &str = "aftercare_stages";

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

CREATE TABLE triggers (
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

CREATE TABLE trigger_filters (
    trigger_id      TEXT NOT NULL REFERENCES triggers(id) ON DELETE CASCADE,
    ticket_id       TEXT NOT NULL REFERENCES tickets(id),
    PRIMARY KEY (trigger_id, ticket_id)
);

CREATE TABLE runs (
    id                    TEXT PRIMARY KEY,
    trigger_id            TEXT NOT NULL REFERENCES triggers(id),
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
CREATE INDEX runs_by_trigger ON runs(trigger_id, created_at_ms);

-- A lease is time-bounded ownership of a ticket by the daemon, taken
-- atomically at claim time. `ticket_id` is the PRIMARY KEY and `run_id` is
-- UNIQUE, so the engine itself enforces at most one lease per ticket and per
-- run: the durable guard against double-spawn, backstopping the conditional
-- `UPDATE ... WHERE state='ready'` in `claim_ticket`.
--
-- Leases are held only by the daemon; `owner_id` stores the source's ownership
-- token, including the trigger needed to recover an interrupted claim.
-- Workers never hold, renew, or observe leases — a worker's
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

-- A run's stage-evidence log: append-only, and replayed by `next_step` to
-- derive where the walk stands. `seq` is per-run monotonic and assigned at
-- insert, so it — not `stage_index` — is the authoritative replay order once a
-- stage can hold more than one row. `attempt` counts a stage's executions and
-- `phase` separates an action's own evidence from that of the independent
-- `result_check` that judged it, so one execution may append two rows sharing
-- `(stage_index, attempt)`.
--
-- The resolved stage verdict rides on the last row of an execution and lives in
-- `state`; earlier rows hold only what their own actor produced and leave it
-- NULL. A row rewritten under the natural key keeps its original `seq`, so the
-- log's order is fixed by first insert.
CREATE TABLE stage_runs (
    run_id          TEXT NOT NULL REFERENCES runs(id),
    seq             INTEGER NOT NULL,
    stage_index     INTEGER NOT NULL,
    stage           TEXT NOT NULL,
    state           TEXT,
    attempt         INTEGER NOT NULL DEFAULT 1,
    phase           TEXT NOT NULL DEFAULT 'action',
    started_at_ms   INTEGER,
    finished_at_ms  INTEGER,
    exit_code       INTEGER,
    evidence_json   TEXT,
    PRIMARY KEY (run_id, seq),
    UNIQUE (run_id, stage_index, attempt, phase)
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
SELECT 'trigger', COALESCE(MAX(CAST(SUBSTR(id, 3) AS INTEGER)), 0) + 1 FROM triggers;
INSERT OR IGNORE INTO id_counters (kind, next_ordinal)
SELECT 'note', COALESCE(MAX(CAST(SUBSTR(id, 2) AS INTEGER)), 0) + 1 FROM notes;
";

/// The counter seed as it stood before [`TRIGGER_RENAME`]. Every arm that
/// replays it is opening a database written by a binary that still called the
/// table `activations` and minted one-letter `A<ordinal>` ids, so the seed has
/// to name them; the rename step later in the same transaction carries the row
/// and its ids forward.
const LEGACY_ID_COUNTER_SCHEMA: &str = "
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

// Turns the stage table into the ordered evidence log described above. The
// primary key moves from the natural key to `(run_id, seq)`, and `state`
// becomes nullable, so the table is rebuilt rather than altered in place.
//
// Every pre-existing row is one whole stage execution, judged and resolved, so
// each backfills as an `action` row carrying its verdict. Numbering them by
// `(stage_index, attempt)` reproduces the order the linear walk wrote them in,
// which is what makes a run queued before the migration replay to the same
// `Step` afterwards.
const STAGE_EVIDENCE_LOG: &str = "
CREATE TABLE stage_evidence_log (
    run_id          TEXT NOT NULL REFERENCES runs(id),
    seq             INTEGER NOT NULL,
    stage_index     INTEGER NOT NULL,
    stage           TEXT NOT NULL,
    state           TEXT,
    attempt         INTEGER NOT NULL DEFAULT 1,
    phase           TEXT NOT NULL DEFAULT 'action',
    started_at_ms   INTEGER,
    finished_at_ms  INTEGER,
    exit_code       INTEGER,
    evidence_json   TEXT,
    PRIMARY KEY (run_id, seq),
    UNIQUE (run_id, stage_index, attempt, phase)
);

INSERT INTO stage_evidence_log
    (run_id, seq, stage_index, stage, state, attempt, phase,
     started_at_ms, finished_at_ms, exit_code, evidence_json)
SELECT run_id,
       ROW_NUMBER() OVER (PARTITION BY run_id ORDER BY stage_index, attempt),
       stage_index, stage, state, attempt, 'action',
       started_at_ms, finished_at_ms, exit_code, evidence_json
FROM aftercare_stages;

DROP TABLE aftercare_stages;
ALTER TABLE stage_evidence_log RENAME TO aftercare_stages;
";

// Dissolves the aftercare regime: one driver now walks every stage, so nothing
// stored is named after the thread that used to walk the tail of a flow. The
// stage log becomes `stage_runs`, the run state that meant "no agent process
// left to identify" becomes `driving`, and the interrupted-stage process
// checkpoint becomes `stage_process`. These are the only strings left carrying
// the old spelling, and only because a database written by an older binary
// still holds them.
const UNIFORM_STAGE_DRIVER: &str = "
ALTER TABLE aftercare_stages RENAME TO stage_runs;

UPDATE runs SET state = 'driving' WHERE state = 'aftercare';

UPDATE run_evidence
   SET kind = 'stage_process',
       dedupe_key = 'settlement:' || run_id || ':stage_process'
 WHERE kind = 'aftercare_process';
";

// Renames the activation concept to `trigger`: the two tables, every column
// that points at them, the minted id prefix, and the counter row that hands out
// its ordinals. Nothing is dropped. A queued trigger is a durable record that
// demand exists, reconstructible from neither the committed ticket files nor
// Git, so `reindex` cannot put one back — the migration has to carry every row
// across or the pending work is simply gone.
//
// `RENAME TO` and `RENAME COLUMN` rewrite the `REFERENCES` clauses in other
// tables for us, but an `UPDATE` to a primary key does not propagate to the
// plain `REFERENCES` columns pointing at it, so `runs` and `trigger_filters`
// are rewritten by hand. `defer_foreign_keys` holds the constraint check until
// commit, by which point all three tables agree again.
//
// The prefix goes to `TR`, not `T`: `T91` reads as a ticket id beside
// `TICK-91`. Widening it by a character is why the id counter's seed moves from
// `SUBSTR(id, 2)` to `SUBSTR(id, 3)`.
//
// `leases.owner_id` is a JSON ownership token that embeds the claiming
// trigger's id, and recovery matches that id against `triggers` to re-find an
// interrupted claim. It is rewritten to the same shape and key order
// `lease_owner` now writes, so a lease planted before the upgrade still decodes
// to a trigger that exists.
const TRIGGER_RENAME: &str = "
PRAGMA defer_foreign_keys = ON;

ALTER TABLE activations RENAME TO triggers;
ALTER TABLE activation_filters RENAME TO trigger_filters;
ALTER TABLE trigger_filters RENAME COLUMN activation_id TO trigger_id;
ALTER TABLE runs RENAME COLUMN activation_id TO trigger_id;

DROP INDEX runs_by_activation;
CREATE INDEX runs_by_trigger ON runs(trigger_id, created_at_ms);

UPDATE triggers SET id = 'TR' || SUBSTR(id, 2) WHERE id GLOB 'A[0-9]*';
UPDATE runs SET trigger_id = 'TR' || SUBSTR(trigger_id, 2)
 WHERE trigger_id GLOB 'A[0-9]*';
UPDATE trigger_filters SET trigger_id = 'TR' || SUBSTR(trigger_id, 2)
 WHERE trigger_id GLOB 'A[0-9]*';

UPDATE leases
   SET owner_id = json_object(
           'owner', json_extract(owner_id, '$.owner'),
           'trigger', 'TR' || SUBSTR(json_extract(owner_id, '$.activation'), 2))
 WHERE json_valid(owner_id)
   AND json_extract(owner_id, '$.activation') GLOB 'A[0-9]*';

UPDATE id_counters SET kind = 'trigger' WHERE kind = 'activation';
";

/// The exact inverse of [`TRIGGER_RENAME`], for the fixtures that plant a
/// pre-migration database. They open a current-schema database and strip it
/// back, so without this the thing they plant is only half old and the arm they
/// exercise fails looking for `activations`.
///
/// The integration suite plants the same shape by hand, the way it already does
/// for the stage-log migration; keep the two in step.
///
/// Unlike the migration steps this opens its own transaction, because it is run
/// standalone rather than replayed by [`migrate`]. That is not cosmetic:
/// `defer_foreign_keys` lasts only until the end of the enclosing transaction,
/// so outside one it is undone by the very next statement's autocommit and the
/// rewrite trips the `runs` foreign key partway through.
#[cfg(test)]
pub(crate) const REVERT_TRIGGER_RENAME: &str = "
BEGIN IMMEDIATE;
PRAGMA defer_foreign_keys = ON;

UPDATE leases
   SET owner_id = json_object(
           'activation', 'A' || SUBSTR(json_extract(owner_id, '$.trigger'), 3),
           'owner', json_extract(owner_id, '$.owner'))
 WHERE json_valid(owner_id)
   AND json_extract(owner_id, '$.trigger') GLOB 'TR[0-9]*';

UPDATE trigger_filters SET trigger_id = 'A' || SUBSTR(trigger_id, 3)
 WHERE trigger_id GLOB 'TR[0-9]*';
UPDATE runs SET trigger_id = 'A' || SUBSTR(trigger_id, 3)
 WHERE trigger_id GLOB 'TR[0-9]*';
UPDATE triggers SET id = 'A' || SUBSTR(id, 3) WHERE id GLOB 'TR[0-9]*';

DROP INDEX runs_by_trigger;

ALTER TABLE runs RENAME COLUMN trigger_id TO activation_id;
ALTER TABLE trigger_filters RENAME COLUMN trigger_id TO activation_id;
ALTER TABLE trigger_filters RENAME TO activation_filters;
ALTER TABLE triggers RENAME TO activations;

CREATE INDEX runs_by_activation ON runs(activation_id, created_at_ms);

UPDATE id_counters SET kind = 'activation' WHERE kind = 'trigger';
COMMIT;
";

pub(super) fn migrate(connection: &mut Connection, now_ms: i64) -> Result<(), DbError> {
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
            transaction.execute_batch(LEGACY_ID_COUNTER_SCHEMA)?;
            transaction.execute_batch(EVENTS_SCHEMA)?;
            transaction.execute_batch(TICKET_SOURCE_COLUMNS)?;
            transaction.execute_batch(RESTART_DRAINING_COLUMN)?;
            transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
            transaction.execute_batch(STAGE_EVIDENCE_LOG)?;
            transaction.execute_batch(UNIFORM_STAGE_DRIVER)?;
            transaction.execute_batch(TRIGGER_RENAME)?;
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
            transaction.execute_batch(LEGACY_ID_COUNTER_SCHEMA)?;
            transaction.execute_batch(EVENTS_SCHEMA)?;
            transaction.execute_batch(TICKET_SOURCE_COLUMNS)?;
            transaction.execute_batch(RESTART_DRAINING_COLUMN)?;
            transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
            transaction.execute_batch(STAGE_EVIDENCE_LOG)?;
            transaction.execute_batch(UNIFORM_STAGE_DRIVER)?;
            transaction.execute_batch(TRIGGER_RENAME)?;
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
            transaction.execute_batch(LEGACY_ID_COUNTER_SCHEMA)?;
            transaction.execute_batch(EVENTS_SCHEMA)?;
            transaction.execute_batch(TICKET_SOURCE_COLUMNS)?;
            transaction.execute_batch(RESTART_DRAINING_COLUMN)?;
            transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
            transaction.execute_batch(STAGE_EVIDENCE_LOG)?;
            transaction.execute_batch(UNIFORM_STAGE_DRIVER)?;
            transaction.execute_batch(TRIGGER_RENAME)?;
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
            transaction.execute_batch(LEGACY_ID_COUNTER_SCHEMA)?;
            transaction.execute_batch(EVENTS_SCHEMA)?;
            transaction.execute_batch(TICKET_SOURCE_COLUMNS)?;
            transaction.execute_batch(RESTART_DRAINING_COLUMN)?;
            transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
            transaction.execute_batch(STAGE_EVIDENCE_LOG)?;
            transaction.execute_batch(UNIFORM_STAGE_DRIVER)?;
            transaction.execute_batch(TRIGGER_RENAME)?;
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
            transaction.execute_batch(LEGACY_ID_COUNTER_SCHEMA)?;
            transaction.execute_batch(EVENTS_SCHEMA)?;
            transaction.execute_batch(TICKET_SOURCE_COLUMNS)?;
            transaction.execute_batch(RESTART_DRAINING_COLUMN)?;
            transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
            transaction.execute_batch(STAGE_EVIDENCE_LOG)?;
            transaction.execute_batch(UNIFORM_STAGE_DRIVER)?;
            transaction.execute_batch(TRIGGER_RENAME)?;
            transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            transaction.commit()?;
            Ok(())
        }
        6 => {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute_batch("ALTER TABLE runs ADD COLUMN worker_socket_path TEXT;")?;
            transaction.execute_batch(RUN_SNAPSHOT_COLUMNS)?;
            transaction.execute_batch(LEGACY_ID_COUNTER_SCHEMA)?;
            transaction.execute_batch(EVENTS_SCHEMA)?;
            transaction.execute_batch(TICKET_SOURCE_COLUMNS)?;
            transaction.execute_batch(RESTART_DRAINING_COLUMN)?;
            transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
            transaction.execute_batch(STAGE_EVIDENCE_LOG)?;
            transaction.execute_batch(UNIFORM_STAGE_DRIVER)?;
            transaction.execute_batch(TRIGGER_RENAME)?;
            transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            transaction.commit()?;
            Ok(())
        }
        7 => {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute_batch(RUN_SNAPSHOT_COLUMNS)?;
            transaction.execute_batch(LEGACY_ID_COUNTER_SCHEMA)?;
            transaction.execute_batch(EVENTS_SCHEMA)?;
            transaction.execute_batch(TICKET_SOURCE_COLUMNS)?;
            transaction.execute_batch(RESTART_DRAINING_COLUMN)?;
            transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
            transaction.execute_batch(STAGE_EVIDENCE_LOG)?;
            transaction.execute_batch(UNIFORM_STAGE_DRIVER)?;
            transaction.execute_batch(TRIGGER_RENAME)?;
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
            transaction.execute_batch(STAGE_EVIDENCE_LOG)?;
            transaction.execute_batch(UNIFORM_STAGE_DRIVER)?;
            transaction.execute_batch(TRIGGER_RENAME)?;
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
            transaction.execute_batch(STAGE_EVIDENCE_LOG)?;
            transaction.execute_batch(UNIFORM_STAGE_DRIVER)?;
            transaction.execute_batch(TRIGGER_RENAME)?;
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
            transaction.execute_batch(STAGE_EVIDENCE_LOG)?;
            transaction.execute_batch(UNIFORM_STAGE_DRIVER)?;
            transaction.execute_batch(TRIGGER_RENAME)?;
            transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            transaction.commit()?;
            Ok(())
        }
        11 => {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute_batch(RESTART_DRAINING_COLUMN)?;
            transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
            transaction.execute_batch(STAGE_EVIDENCE_LOG)?;
            transaction.execute_batch(UNIFORM_STAGE_DRIVER)?;
            transaction.execute_batch(TRIGGER_RENAME)?;
            transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            transaction.commit()?;
            Ok(())
        }
        12 => {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute_batch(WORKTREE_CLEANUP_COLUMNS)?;
            transaction.execute_batch(STAGE_EVIDENCE_LOG)?;
            transaction.execute_batch(UNIFORM_STAGE_DRIVER)?;
            transaction.execute_batch(TRIGGER_RENAME)?;
            transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            transaction.commit()?;
            Ok(())
        }
        13 => {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute_batch(STAGE_EVIDENCE_LOG)?;
            transaction.execute_batch(UNIFORM_STAGE_DRIVER)?;
            transaction.execute_batch(TRIGGER_RENAME)?;
            transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            transaction.commit()?;
            Ok(())
        }
        14 => {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute_batch(UNIFORM_STAGE_DRIVER)?;
            transaction.execute_batch(TRIGGER_RENAME)?;
            transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            transaction.commit()?;
            Ok(())
        }
        15 => {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute_batch(TRIGGER_RENAME)?;
            transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            transaction.commit()?;
            Ok(())
        }
        SCHEMA_VERSION => Ok(()),
        newer => Err(DbError::UnsupportedSchemaVersion(newer)),
    }
}
