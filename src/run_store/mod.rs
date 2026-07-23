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
pub(crate) use runs::RunAdmission;
pub use runs::{ActiveRun, EventRecord, ProjectNote, RunRecord, RunState, RunTimeline};
pub(crate) use runs::{NeedsReviewBranch, RecoverableRun, WorktreeCleanupCandidate};

use rusqlite::{OptionalExtension, TransactionBehavior, params};

use crate::db::Db;
use crate::domain::ticket::TicketState;
use crate::domain::work::{OwnerId, WorkOutcome};
use crate::outcome::Outcome;
use crate::store::StoreError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedOutcome {
    pub work: WorkOutcome,
    pub not_before_ms: Option<i64>,
}

pub struct RunStore {
    db: Db,
}

impl RunStore {
    pub fn from_db(db: Db) -> Self {
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

    #[allow(clippy::too_many_arguments)]
    pub fn settle(
        &self,
        run_id: &str,
        exit_code: Option<i32>,
        outcome: Outcome,
        records: &[EvidenceRecord],
        cooldown: Option<&CooldownUpdate<'_>>,
        now_ms: i64,
    ) -> Result<(RecordedOutcome, bool), StoreError> {
        let mut connection = self.db.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let run_state = RunState::from(outcome);
        let changed = runs::tx::finish(&transaction, run_id, run_state, exit_code, now_ms)?;
        if changed == 0 {
            match runs::tx::state_and_exit(&transaction, run_id)? {
                Some((_, Some(_))) => {}
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
        } else {
            if let Some(cooldown) = cooldown {
                limits::tx::upsert_cooldown(&transaction, run_id, cooldown, now_ms)?;
            }
            evidence::tx::record_settlement(&transaction, run_id, records, now_ms)?;
            let ticket_id = runs::tx::ticket_id(&transaction, run_id)?;
            runs::tx::record_event(
                &transaction,
                now_ms,
                "run_finished",
                Some(run_id),
                Some(&ticket_id),
                &serde_json::json!({
                    "outcome": outcome.as_str(),
                    "exit_code": exit_code,
                    "ticket_state": TicketState::after_outcome(outcome).as_str(),
                })
                .to_string(),
            )?;
        }
        let recorded = recorded_outcome(&transaction, run_id)?.ok_or_else(|| {
            StoreError::RunStateConflict {
                run_id: run_id.into(),
                state: runs::tx::state(&transaction, run_id).ok().flatten(),
                requested: run_state.as_str().into(),
            }
        })?;
        transaction.commit()?;
        Ok((recorded, changed != 0))
    }

    pub fn abort(&self, run_id: &str, ticket_id: &str, now_ms: i64) -> Result<bool, StoreError> {
        let mut connection = self.db.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = runs::tx::abort(&transaction, run_id, now_ms)?;
        if changed != 0 {
            runs::tx::record_event(
                &transaction,
                now_ms,
                "run_aborted",
                Some(run_id),
                Some(ticket_id),
                "{}",
            )?;
        } else {
            let state = runs::tx::state(&transaction, run_id)?;
            if state.as_deref() != Some(RunState::Aborted.as_str()) {
                return Err(StoreError::RunStateConflict {
                    run_id: run_id.into(),
                    state,
                    requested: RunState::Aborted.as_str().into(),
                });
            }
        }
        transaction.commit()?;
        Ok(changed != 0)
    }

    pub(crate) fn record_external_merge(
        &self,
        run_id: &str,
        ticket_id: &str,
        branch: &str,
        branch_tip: &str,
        observed_default_tip: &str,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        self.write(TransactionBehavior::Immediate, |transaction| {
            runs::tx::mark_cleanup_eligible(transaction, run_id, now_ms)?;
            let data_json = serde_json::json!({
                "branch": branch,
                "branch_tip": branch_tip,
                "observed_default_tip": observed_default_tip,
            })
            .to_string();
            let inserted =
                evidence::tx::record_external_merge(transaction, run_id, &data_json, now_ms)?;
            if inserted {
                runs::tx::record_event(
                    transaction,
                    now_ms,
                    "external_merge_reconciled",
                    Some(run_id),
                    Some(ticket_id),
                    &data_json,
                )?;
            }
            Ok(inserted)
        })
        .map_err(StoreError::from)
    }

    pub fn recorded_outcome(&self, run_id: &str) -> Result<Option<RecordedOutcome>, StoreError> {
        recorded_outcome(&self.db.lock(), run_id).map_err(StoreError::from)
    }
}

fn recorded_outcome(
    connection: &rusqlite::Connection,
    run_id: &str,
) -> rusqlite::Result<Option<RecordedOutcome>> {
    let row = connection
        .query_row(
            "SELECT ticket_id, state, branch, attempt, exited_at_ms,
                    (SELECT data_json FROM run_evidence
                     WHERE run_id = runs.id AND kind = 'commits_observed'
                     ORDER BY sequence DESC LIMIT 1),
                    (SELECT data_json FROM run_evidence
                     WHERE run_id = runs.id AND kind = 'vendor_error_classified'
                     ORDER BY sequence DESC LIMIT 1),
                    (SELECT until_ms FROM cooldowns WHERE source_run_id = runs.id
                     ORDER BY until_ms DESC LIMIT 1),
                    EXISTS(SELECT 1 FROM run_evidence
                           WHERE run_id = runs.id AND kind = 'external_merge_observed')
             FROM runs WHERE id = ?1",
            params![run_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, RunState>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                    row.get::<_, bool>(8)?,
                ))
            },
        )
        .optional()?;
    let Some((
        ticket_id,
        state,
        branch,
        attempt,
        finished_at_ms,
        commits,
        vendor_error,
        cooldown,
        externally_merged,
    )) = row
    else {
        return Ok(None);
    };
    let Some(verdict) = externally_merged
        .then_some(Outcome::Merged)
        .or_else(|| state.outcome())
    else {
        return Ok(None);
    };
    let Some(finished_at_ms) = finished_at_ms else {
        return Ok(None);
    };
    let commit_count = commits
        .as_deref()
        .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
        .and_then(|value| value["oids"].as_array().map(Vec::len))
        .unwrap_or(0)
        .min(u32::MAX as usize) as u32;
    let not_before_ms = vendor_error
        .as_deref()
        .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
        .and_then(|value| value["cooldown_until_ms"].as_i64())
        .or(cooldown);
    Ok(Some(RecordedOutcome {
        work: WorkOutcome {
            ticket_id,
            owner: OwnerId(run_id.into()),
            verdict,
            branch,
            commit_count,
            attempt: attempt.clamp(0, i64::from(u32::MAX)) as u32,
            finished_at_ms,
        },
        not_before_ms,
    }))
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
