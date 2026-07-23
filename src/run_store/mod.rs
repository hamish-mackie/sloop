//! Concrete SQLite storage for run supervision state.
//!
//! `runs` owns run lifecycle records and their attached notes and activity
//! events, `evidence` owns verdict and aftercare facts, and `limits` owns
//! cooldown and budget state. Scheduler state and shared ID counters stay on
//! [`RunStore`] because they apply across those families.

pub(crate) mod evidence;
pub(crate) mod limits;
pub(crate) mod runs;

pub use evidence::{EvidenceRecord, StageRecord};
pub use limits::{CooldownRecord, CooldownUpdate};
pub use runs::{ActiveRun, EventRecord, ProjectNote, RunRecord, RunState, RunTimeline};
pub(crate) use runs::{NeedsReviewBranch, RecoverableRun, WorktreeCleanupCandidate};

use rusqlite::TransactionBehavior;

use crate::db::Db;

pub struct RunStore {
    db: Db,
}

impl RunStore {
    pub(crate) fn from_db(db: Db) -> Self {
        Self { db }
    }

    fn write<T>(
        &self,
        behavior: TransactionBehavior,
        operation: impl FnOnce(&rusqlite::Transaction<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        let mut connection = self.db.lock();
        let transaction = connection.transaction_with_behavior(behavior)?;
        let result = operation(&transaction)?;
        transaction.commit()?;
        Ok(result)
    }

    pub(crate) fn next_note_ordinal(&self) -> rusqlite::Result<i64> {
        self.reserve_ordinal("note", "notes")
    }

    fn reserve_ordinal(&self, kind: &str, table: &str) -> rusqlite::Result<i64> {
        self.write(TransactionBehavior::Immediate, |transaction| {
            tx::reserve_ordinal(transaction, kind, table)
        })
    }

    pub(crate) fn paused(&self) -> rusqlite::Result<bool> {
        paused(&self.db.lock())
    }

    pub(crate) fn clear_restart_draining(&self, now_ms: i64) -> rusqlite::Result<()> {
        self.write(TransactionBehavior::Deferred, |transaction| {
            tx::clear_restart_draining(transaction, now_ms)
        })
    }

    pub(crate) fn restart_draining(&self) -> rusqlite::Result<bool> {
        restart_draining(&self.db.lock())
    }

    pub(crate) fn set_paused(&self, paused: bool, now_ms: i64) -> rusqlite::Result<()> {
        self.write(TransactionBehavior::Deferred, |transaction| {
            tx::set_paused(transaction, paused, now_ms)
        })
    }

    pub(crate) fn begin_restart_draining(
        &self,
        active_runs: usize,
        now_ms: i64,
    ) -> rusqlite::Result<bool> {
        self.write(TransactionBehavior::Immediate, |transaction| {
            tx::begin_restart_draining(transaction, active_runs, now_ms)
        })
    }

    pub(crate) fn resume_scheduler(&self, now_ms: i64) -> rusqlite::Result<bool> {
        self.write(TransactionBehavior::Immediate, |transaction| {
            tx::resume_scheduler(transaction, now_ms)
        })
    }

    pub(crate) fn probe_writable(&self, now_ms: i64) -> rusqlite::Result<()> {
        self.write(TransactionBehavior::Deferred, |transaction| {
            tx::probe_writable(transaction, now_ms)
        })
    }
}

fn paused(connection: &rusqlite::Connection) -> rusqlite::Result<bool> {
    let paused: i64 = connection.query_row(
        "SELECT paused FROM scheduler_state WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?;
    Ok(paused != 0)
}

fn restart_draining(connection: &rusqlite::Connection) -> rusqlite::Result<bool> {
    let draining: i64 = connection.query_row(
        "SELECT draining FROM scheduler_state WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?;
    Ok(draining != 0)
}

pub(crate) mod tx {
    use rusqlite::{Transaction, params};

    pub(crate) fn reserve_ordinal(
        transaction: &Transaction<'_>,
        kind: &str,
        table: &str,
    ) -> rusqlite::Result<i64> {
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
        Ok(ordinal)
    }

    pub(crate) fn clear_restart_draining(
        transaction: &Transaction<'_>,
        now_ms: i64,
    ) -> rusqlite::Result<()> {
        transaction.execute(
            "UPDATE scheduler_state SET draining = 0, updated_at_ms = ?1 WHERE singleton = 1",
            params![now_ms],
        )?;
        Ok(())
    }

    pub(crate) fn set_paused(
        transaction: &Transaction<'_>,
        paused: bool,
        now_ms: i64,
    ) -> rusqlite::Result<()> {
        transaction.execute(
            "UPDATE scheduler_state SET paused = ?1, updated_at_ms = ?2 WHERE singleton = 1",
            params![i64::from(paused), now_ms],
        )?;
        Ok(())
    }

    pub(crate) fn begin_restart_draining(
        transaction: &Transaction<'_>,
        active_runs: usize,
        now_ms: i64,
    ) -> rusqlite::Result<bool> {
        let changed = transaction.execute(
            "UPDATE scheduler_state SET draining = 1, updated_at_ms = ?1
             WHERE singleton = 1 AND draining = 0",
            params![now_ms],
        )? != 0;
        if changed {
            super::runs::tx::record_event(
                transaction,
                now_ms,
                "daemon_restart_requested",
                None,
                None,
                &serde_json::json!({"active_runs": active_runs}).to_string(),
            )?;
        }
        Ok(changed)
    }

    pub(crate) fn resume_scheduler(
        transaction: &Transaction<'_>,
        now_ms: i64,
    ) -> rusqlite::Result<bool> {
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
        Ok(was_draining)
    }

    pub(crate) fn probe_writable(
        transaction: &Transaction<'_>,
        now_ms: i64,
    ) -> rusqlite::Result<()> {
        transaction.execute(
            "UPDATE scheduler_state SET updated_at_ms = ?1 WHERE singleton = 1",
            params![now_ms],
        )?;
        Ok(())
    }
}
