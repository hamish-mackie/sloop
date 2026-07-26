use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

use super::{ProjectCommitEvidence, RunStore};
use crate::db::StoreError;

/// One appended `run_evidence` row: a kind plus kind-specific JSON facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceRecord {
    pub kind: &'static str,
    pub data_json: String,
}

/// Where in a stage execution a log row was written. An execution whose
/// `result_check` runs an independent actor records both phases as two rows
/// sharing `stage_index` and `attempt`; every other execution is judged
/// without a second process and records its action alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagePhase {
    Action,
    Check,
}

impl StagePhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Action => "action",
            Self::Check => "check",
        }
    }

    /// Anything that is not the check phase is the action phase: rows written
    /// before the split carry no phase of their own and are all actions.
    fn parse(value: &str) -> Self {
        match value {
            "check" => Self::Check,
            _ => Self::Action,
        }
    }
}

/// One row of a run's ordered stage-evidence log.
///
/// Rows are produced and read in log order — the storage assigns each a
/// per-run `seq` at insert and hands them back in it — because the walk
/// replays them rather than looking them up by stage. The resolved stage
/// verdict rides on the last row of an execution; earlier rows carry only what
/// their own actor produced and leave `state` absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageRecord {
    pub stage_index: usize,
    pub stage: String,
    /// 1 for a stage's first execution, incrementing each time the walk
    /// re-enters it.
    pub attempt: u32,
    pub phase: StagePhase,
    /// `passed` or `failed` once the execution resolved; absent on a row the
    /// resolution does not ride on.
    pub state: Option<String>,
    pub started_at_ms: i64,
    pub finished_at_ms: i64,
    pub exit_code: Option<i32>,
    pub output_ref: String,
    pub verdict_source: Option<String>,
    pub reason: Option<String>,
}

/// One panel reviewer's report, as the storage takes it. The seat's key is
/// separate from the report's content because only the key is authoritative:
/// it comes from the reviewer's credential.
#[derive(Debug, Clone, Copy)]
pub struct PanelReportRecord<'a> {
    pub stage: &'a str,
    pub stage_index: usize,
    pub attempt: u32,
    pub reviewer_index: usize,
    pub verdict: &'a str,
    pub confidence: &'a str,
    pub reason: &'a str,
}

/// Durable watchdog intent, recorded before the agent process group is killed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputStallEvidence {
    pub stage: String,
    pub last_output_at_ms: i64,
    pub threshold_ms: i64,
    pub last_output_sequence: Option<u64>,
}

pub(crate) mod tx {
    use rusqlite::{Transaction, params};

    use super::{EvidenceRecord, OutputStallEvidence, PanelReportRecord, StageRecord};

    pub(crate) fn delete_for_run(
        transaction: &Transaction<'_>,
        run_id: &str,
    ) -> rusqlite::Result<usize> {
        let evidence = transaction.execute(
            "DELETE FROM run_evidence WHERE run_id = ?1",
            params![run_id],
        )?;
        let stages =
            transaction.execute("DELETE FROM stage_runs WHERE run_id = ?1", params![run_id])?;
        Ok(evidence + stages)
    }

    pub(crate) fn record_settlement(
        transaction: &Transaction<'_>,
        run_id: &str,
        evidence: &[EvidenceRecord],
        now_ms: i64,
    ) -> rusqlite::Result<()> {
        for record in evidence {
            transaction.execute(
                "INSERT OR IGNORE INTO run_evidence
                     (run_id, kind, observed_at_ms, dedupe_key, data_json)
                 VALUES (?1, ?2, ?3, 'settlement:' || ?1 || ':' || ?2, ?4)",
                params![run_id, record.kind, now_ms, record.data_json],
            )?;
        }
        Ok(())
    }

    /// Records what the run's primary agent stage did on its `attempt`-th
    /// execution.
    ///
    /// These rows are the run's exit *story*, not a log of exits: they carry
    /// the code the run settles on, the commits the ticket is credited with,
    /// and the rejection that puts a target on cooldown. A backward edge can
    /// re-enter the agent stage, so each execution overwrites the last rather
    /// than being ignored behind it — a resumed run must settle on what the
    /// most recent agent actually did, and `attempt` says which execution that
    /// was. Rows whose fact no longer holds (a vendor rejection the re-run did
    /// not repeat) are removed rather than left to speak for an exit that
    /// never had them.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_agent_exit(
        transaction: &Transaction<'_>,
        run_id: &str,
        attempt: u32,
        exit_code: Option<i32>,
        capture_complete: bool,
        commits_json: &str,
        vendor_error: Option<&crate::vendor::VendorErrorMatch>,
        cooldown_until_ms: Option<i64>,
        now_ms: i64,
    ) -> rusqlite::Result<()> {
        let evidence = [
            EvidenceRecord {
                kind: "exit_classified",
                data_json: serde_json::json!({"exit_code": exit_code, "attempt": attempt})
                    .to_string(),
            },
            EvidenceRecord {
                kind: "commits_observed",
                data_json: commits_json.to_owned(),
            },
        ];
        for record in evidence {
            record_stage_evidence(transaction, run_id, record.kind, &record.data_json, now_ms)?;
        }
        match vendor_error {
            Some(vendor_error) => record_stage_evidence(
                transaction,
                run_id,
                "vendor_error_classified",
                &vendor_error.evidence_json(cooldown_until_ms),
                now_ms,
            )?,
            None => delete_settlement(transaction, run_id, "vendor_error_classified")?,
        }
        if capture_complete {
            delete_settlement(transaction, run_id, "capture_incomplete")?;
        } else {
            record_stage_evidence(transaction, run_id, "capture_incomplete", "{}", now_ms)?;
        }
        Ok(())
    }

    fn delete_settlement(
        transaction: &Transaction<'_>,
        run_id: &str,
        kind: &str,
    ) -> rusqlite::Result<()> {
        transaction.execute(
            "DELETE FROM run_evidence
             WHERE run_id = ?1 AND dedupe_key = 'settlement:' || ?1 || ':' || ?2",
            params![run_id, kind],
        )?;
        Ok(())
    }

    /// Appends one stage execution's rows to the run's log, in the order
    /// given. `seq` is read off the run's own tail inside this transaction, so
    /// two executions racing to append can never claim the same position.
    pub(crate) fn append_stage_rows(
        transaction: &Transaction<'_>,
        run_id: &str,
        rows: &[StageRecord],
    ) -> rusqlite::Result<()> {
        for row in rows {
            let evidence_json = serde_json::json!({
                "output": row.output_ref,
                "verdict_source": row.verdict_source,
                "reason": row.reason,
            })
            .to_string();
            transaction.execute(
                "INSERT INTO stage_runs
                     (run_id, seq, stage_index, stage, state, attempt, phase,
                      started_at_ms, finished_at_ms, exit_code, evidence_json)
                 VALUES (?1,
                         (SELECT COALESCE(MAX(seq), 0) + 1
                          FROM stage_runs WHERE run_id = ?1),
                         ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                 ON CONFLICT(run_id, stage_index, attempt, phase) DO UPDATE SET
                     stage = excluded.stage,
                     state = excluded.state,
                     started_at_ms = excluded.started_at_ms,
                     finished_at_ms = excluded.finished_at_ms,
                     exit_code = excluded.exit_code,
                     evidence_json = excluded.evidence_json",
                params![
                    run_id,
                    row.stage_index as i64,
                    row.stage,
                    row.state,
                    row.attempt,
                    row.phase.as_str(),
                    row.started_at_ms,
                    row.finished_at_ms,
                    row.exit_code,
                    evidence_json,
                ],
            )?;
        }
        Ok(())
    }

    pub(crate) fn record_stage_evidence(
        transaction: &Transaction<'_>,
        run_id: &str,
        kind: &str,
        data_json: &str,
        now_ms: i64,
    ) -> rusqlite::Result<()> {
        transaction.execute(
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

    pub(crate) fn clear_stage_process(
        transaction: &Transaction<'_>,
        run_id: &str,
    ) -> rusqlite::Result<()> {
        transaction.execute(
            "DELETE FROM run_evidence
             WHERE run_id = ?1 AND dedupe_key = 'settlement:' || ?1 || ':stage_process'",
            params![run_id],
        )?;
        Ok(())
    }

    pub(crate) fn record_cancel_requested(
        transaction: &Transaction<'_>,
        run_id: &str,
        now_ms: i64,
    ) -> rusqlite::Result<()> {
        transaction.execute(
            "INSERT OR IGNORE INTO run_evidence
                 (run_id, kind, observed_at_ms, dedupe_key, data_json)
             VALUES (?1, 'cancel_requested', ?2, 'cancel_requested:' || ?1, '{}')",
            params![run_id, now_ms],
        )?;
        Ok(())
    }

    pub(crate) fn record_output_stall(
        transaction: &Transaction<'_>,
        run_id: &str,
        evidence: &OutputStallEvidence,
        now_ms: i64,
    ) -> rusqlite::Result<bool> {
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO run_evidence
                 (run_id, kind, observed_at_ms, dedupe_key, data_json)
             SELECT id, 'output_stall', ?2, 'output_stall:' || id, ?3
             FROM runs
             WHERE id = ?1 AND state = 'running' AND exited_at_ms IS NULL",
            params![run_id, now_ms, serde_json::to_string(evidence).unwrap()],
        )?;
        Ok(inserted == 1)
    }

    /// Records a worker's self-reported verdict for the execution it is
    /// running. The key carries the attempt as well as the stage: a stage a
    /// backward edge re-enters gets one report per execution, and without the
    /// attempt the second worker's report would be discarded as a duplicate of
    /// the first.
    ///
    /// `confidence` is stored beside the verdict and never consulted by it, so
    /// one worker's `--confidence` means exactly what a panel reviewer's does:
    /// evidence an operator reads, not a weight any decision uses.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_stage_verdict(
        transaction: &Transaction<'_>,
        run_id: &str,
        stage: &str,
        attempt: u32,
        verdict: &str,
        confidence: &str,
        reason: Option<&str>,
        now_ms: i64,
    ) -> rusqlite::Result<bool> {
        let dedupe_key = format!("verdict:{run_id}:{stage}:{attempt}");
        let data_json = serde_json::json!({
            "stage": stage,
            "attempt": attempt,
            "verdict": verdict,
            "confidence": confidence,
            "reason": reason,
        })
        .to_string();
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO run_evidence
                 (run_id, kind, observed_at_ms, dedupe_key, data_json)
             VALUES (?1, 'stage_verdict', ?2, ?3, ?4)",
            params![run_id, now_ms, dedupe_key, data_json],
        )?;
        Ok(inserted == 1)
    }

    /// Records one panel reviewer's report, append-only and one-shot.
    ///
    /// The key is the seat — `(run, stage_index, attempt, reviewer)` — and it
    /// comes from the reviewer's credential, never from its arguments. `INSERT
    /// OR IGNORE` is what makes the credential one-shot: a second `verdict`
    /// call from the same reviewer inserts nothing and is refused, so a
    /// reviewer cannot revise its own report once cast, and a `return_to`
    /// re-run reports onto fresh rows because its attempt differs.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_panel_report(
        transaction: &Transaction<'_>,
        run_id: &str,
        report: &PanelReportRecord<'_>,
        now_ms: i64,
    ) -> rusqlite::Result<bool> {
        let dedupe_key = format!(
            "panel:{run_id}:{}:{}:{}",
            report.stage_index, report.attempt, report.reviewer_index
        );
        let data_json = serde_json::json!({
            "stage": report.stage,
            "stage_index": report.stage_index,
            "attempt": report.attempt,
            "reviewer": report.reviewer_index,
            "verdict": report.verdict,
            "confidence": report.confidence,
            "reason": report.reason,
        })
        .to_string();
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO run_evidence
                 (run_id, kind, observed_at_ms, dedupe_key, data_json)
             VALUES (?1, 'panel_report', ?2, ?3, ?4)",
            params![run_id, now_ms, dedupe_key, data_json],
        )?;
        Ok(inserted == 1)
    }

    pub(crate) fn record_external_merge(
        transaction: &Transaction<'_>,
        run_id: &str,
        data_json: &str,
        now_ms: i64,
    ) -> rusqlite::Result<bool> {
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO run_evidence
                 (run_id, kind, observed_at_ms, dedupe_key, data_json)
             VALUES (?1, 'external_merge_observed', ?2, 'external_merge:' || ?1, ?3)",
            params![run_id, now_ms, data_json],
        )?;
        Ok(inserted == 1)
    }
}

/// The run's whole stage-evidence log, in `seq` order. Callers replay it; none
/// of them look a stage up by name.
fn stage_log(connection: &Connection, run_id: &str) -> rusqlite::Result<Vec<StageRecord>> {
    let mut statement = connection.prepare(
        "SELECT stage_index, stage, attempt, phase, state, started_at_ms, finished_at_ms,
                exit_code, evidence_json
         FROM stage_runs WHERE run_id = ?1 ORDER BY seq",
    )?;
    statement
        .query_map(params![run_id], |row| {
            let evidence = row
                .get::<_, Option<String>>(8)?
                .as_deref()
                .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok());
            let field = |name: &str| {
                evidence
                    .as_ref()
                    .and_then(|value| value[name].as_str())
                    .map(str::to_owned)
            };
            let state: Option<String> = row.get(4)?;
            Ok(StageRecord {
                stage_index: row.get::<_, i64>(0)? as usize,
                stage: row.get(1)?,
                attempt: row.get(2)?,
                phase: StagePhase::parse(&row.get::<_, String>(3)?),
                started_at_ms: row.get(5)?,
                finished_at_ms: row.get(6)?,
                exit_code: row.get(7)?,
                output_ref: field("output").unwrap_or_default(),
                verdict_source: field("verdict_source")
                    .or_else(|| state.is_some().then(|| "exit_code".to_owned())),
                reason: field("reason"),
                state,
            })
        })?
        .collect::<Result<Vec<_>, _>>()
}

fn cancellation_requested(connection: &Connection, run_id: &str) -> rusqlite::Result<bool> {
    let found: Option<i64> = connection
        .query_row(
            "SELECT 1 FROM run_evidence
             WHERE run_id = ?1 AND kind = 'cancel_requested'",
            params![run_id],
            |row| row.get(0),
        )
        .optional()?;
    Ok(found.is_some())
}

fn output_stall(
    connection: &Connection,
    run_id: &str,
) -> rusqlite::Result<Option<OutputStallEvidence>> {
    let data = connection
        .query_row(
            "SELECT data_json FROM run_evidence
             WHERE run_id = ?1 AND kind = 'output_stall'
             ORDER BY sequence DESC LIMIT 1",
            params![run_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    Ok(data.and_then(|data| serde_json::from_str(&data).ok()))
}

fn commit_evidence_for_project(
    connection: &Connection,
    project_id: &str,
) -> rusqlite::Result<Vec<ProjectCommitEvidence>> {
    let mut statement = connection.prepare(
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
}

fn run_evidence(connection: &Connection, run_id: &str) -> rusqlite::Result<Vec<(String, String)>> {
    let mut statement = connection
        .prepare("SELECT kind, data_json FROM run_evidence WHERE run_id = ?1 ORDER BY sequence")?;
    statement
        .query_map(params![run_id], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()
}

fn vendor_error_for_run(connection: &Connection, run_id: &str) -> rusqlite::Result<Option<String>> {
    connection
        .query_row(
            "SELECT data_json FROM run_evidence
             WHERE run_id = ?1 AND kind = 'vendor_error_classified'
             ORDER BY sequence DESC LIMIT 1",
            params![run_id],
            |row| row.get(0),
        )
        .optional()
}

fn latest_vendor_error_for_ticket(
    connection: &Connection,
    ticket_id: &str,
) -> rusqlite::Result<Option<String>> {
    connection
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
        .optional()
}

impl RunStore {
    /// Appends one stage execution's rows atomically: a crash can leave the
    /// execution unrecorded, never half-recorded.
    pub(crate) fn append_stage_rows(
        &self,
        run_id: &str,
        rows: &[StageRecord],
    ) -> Result<(), StoreError> {
        self.write(TransactionBehavior::Deferred, |transaction| {
            tx::append_stage_rows(transaction, run_id, rows)
        })
    }

    pub(crate) fn stage_log(&self, run_id: &str) -> Result<Vec<StageRecord>, StoreError> {
        stage_log(&self.db.lock(), run_id).map_err(StoreError::from)
    }

    pub(crate) fn record_stage_evidence(
        &self,
        run_id: &str,
        kind: &str,
        data_json: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.write(TransactionBehavior::Deferred, |transaction| {
            tx::record_stage_evidence(transaction, run_id, kind, data_json, now_ms)
        })
    }

    pub(crate) fn clear_stage_process(&self, run_id: &str) -> Result<(), StoreError> {
        self.write(TransactionBehavior::Deferred, |transaction| {
            tx::clear_stage_process(transaction, run_id)
        })
    }

    pub(crate) fn record_cancel_requested(
        &self,
        run_id: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.write(TransactionBehavior::Deferred, |transaction| {
            tx::record_cancel_requested(transaction, run_id, now_ms)
        })
    }

    pub(crate) fn cancellation_requested(&self, run_id: &str) -> Result<bool, StoreError> {
        cancellation_requested(&self.db.lock(), run_id).map_err(StoreError::from)
    }

    pub(crate) fn record_output_stall(
        &self,
        run_id: &str,
        evidence: &OutputStallEvidence,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        self.write(TransactionBehavior::Immediate, |transaction| {
            tx::record_output_stall(transaction, run_id, evidence, now_ms)
        })
    }

    pub(crate) fn output_stall(
        &self,
        run_id: &str,
    ) -> Result<Option<OutputStallEvidence>, StoreError> {
        output_stall(&self.db.lock(), run_id).map_err(StoreError::from)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_stage_verdict(
        &self,
        run_id: &str,
        stage: &str,
        attempt: u32,
        verdict: &str,
        confidence: &str,
        reason: Option<&str>,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        self.write(TransactionBehavior::Deferred, |transaction| {
            tx::record_stage_verdict(
                transaction,
                run_id,
                stage,
                attempt,
                verdict,
                confidence,
                reason,
                now_ms,
            )
        })
    }

    pub(crate) fn record_panel_report(
        &self,
        run_id: &str,
        report: &PanelReportRecord<'_>,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        self.write(TransactionBehavior::Deferred, |transaction| {
            tx::record_panel_report(transaction, run_id, report, now_ms)
        })
    }

    pub(crate) fn commit_evidence_for_project(
        &self,
        project_id: &str,
    ) -> Result<Vec<ProjectCommitEvidence>, StoreError> {
        commit_evidence_for_project(&self.db.lock(), project_id).map_err(StoreError::from)
    }

    pub(crate) fn run_evidence(&self, run_id: &str) -> Result<Vec<(String, String)>, StoreError> {
        run_evidence(&self.db.lock(), run_id).map_err(StoreError::from)
    }

    pub(crate) fn vendor_error_for_run(
        &self,
        run_id: &str,
    ) -> Result<Option<crate::vendor::VendorErrorMatch>, StoreError> {
        let data = vendor_error_for_run(&self.db.lock(), run_id)?;
        Ok(data.and_then(|data| serde_json::from_str(&data).ok()))
    }

    pub(crate) fn latest_vendor_error_for_ticket(
        &self,
        ticket_id: &str,
    ) -> Result<Option<crate::vendor::VendorErrorMatch>, StoreError> {
        let data = latest_vendor_error_for_ticket(&self.db.lock(), ticket_id)?;
        Ok(data.and_then(|data| serde_json::from_str(&data).ok()))
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::tempdir;

    use crate::db::{Db, LEGACY_STAGE_TABLE, REVERT_TRIGGER_RENAME, StoreError};
    use crate::outcome::Outcome;
    use crate::run_store::test_support::{claim_run, open_seeded, settle_run};
    use crate::run_store::{
        Exit, ExitClaim, ExitDenial, RunExit, RunStart, RunState, RunStore, StagePhase,
        StageRecord, Start,
    };
    use crate::vendor::{VendorErrorClass, VendorErrorMatch};

    fn stage_row(
        stage_index: usize,
        stage: &str,
        attempt: u32,
        phase: StagePhase,
        state: Option<&str>,
    ) -> StageRecord {
        StageRecord {
            stage_index,
            stage: stage.to_owned(),
            attempt,
            phase,
            state: state.map(str::to_owned),
            started_at_ms: 3_000,
            finished_at_ms: 3_100,
            exit_code: Some(0),
            output_ref: "runs/R1/output.ndjson".into(),
            verdict_source: state.map(|_| "exit_code".to_owned()),
            reason: None,
        }
    }

    /// `(stage_index, stage, attempt, phase, state)` for every row of the log,
    /// in the order the storage hands them back.
    fn log_shape(store: &RunStore) -> Vec<(usize, String, u32, StagePhase, Option<String>)> {
        store
            .stage_log("R1")
            .unwrap()
            .into_iter()
            .map(|row| {
                (
                    row.stage_index,
                    row.stage,
                    row.attempt,
                    row.phase,
                    row.state,
                )
            })
            .collect()
    }

    fn reopen(path: &Path) -> RunStore {
        RunStore::from_db(Db::open(path, 4_000).unwrap())
    }

    fn running_r1(store: &RunStore) {
        claim_run(store, "R1", "{}", "{}", 2_000);
        assert_eq!(
            store.begin("R1", "branch", "/worktree", 2_050).unwrap(),
            Start::Granted
        );
        assert_eq!(
            store
                .start(
                    &RunStart {
                        run_id: "R1",
                        branch: "branch",
                        worktree_path: "/worktree",
                        pid: 123,
                        pid_start_time: Some(456),
                        process_group_id: 123,
                        worker_token: "token",
                        worker_socket_path: "/runtime/R1.sock",
                    },
                    2_100,
                )
                .unwrap(),
            Start::Granted
        );
    }

    #[test]
    fn agent_exit_and_stage_results_are_checkpointed_idempotently() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));
        running_r1(&store);

        assert_eq!(
            store
                .record_agent_exit(
                    "R1",
                    1,
                    Some(0),
                    true,
                    r#"{"count":1,"oids":["abc"]}"#,
                    None,
                    None,
                    2_200,
                )
                .unwrap(),
            ExitClaim::Claimed
        );
        store
            .record_stage_evidence(
                "R1",
                "test_result",
                r#"{"passed":true,"exit_code":0}"#,
                2_300,
            )
            .unwrap();
        store
            .record_stage_evidence(
                "R1",
                "test_result",
                r#"{"passed":true,"exit_code":0}"#,
                2_400,
            )
            .unwrap();

        let run = store.run("R1").unwrap().unwrap();
        assert_eq!(run.state, "driving");
        assert_eq!(run.exit_code, Some(0));
        assert_eq!(
            store.recoverable_runs().unwrap()[0].state,
            RunState::Driving
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

    #[test]
    fn agent_exit_checkpoint_is_an_exclusive_ownership_handoff() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));
        running_r1(&store);

        let exit = RunExit {
            run_id: "R1",
            attempt: 1,
            exit_code: Some(0),
            capture_complete: true,
            commits_json: r#"{"oids":["abc"]}"#,
            vendor_error: None,
            cooldown_until_ms: None,
        };
        assert_eq!(store.record_exit(&exit, 2_200).unwrap(), Exit::Granted);
        assert_eq!(store.run("R1").unwrap().unwrap().state, "driving");
        let evidence = store.run_evidence("R1").unwrap();
        assert!(evidence.iter().any(|(kind, _)| kind == "exit_classified"));
        assert!(evidence.iter().any(|(kind, _)| kind == "commits_observed"));

        assert_eq!(
            store.record_exit(&exit, 2_300).unwrap(),
            Exit::Denied(ExitDenial::AlreadyClaimed {
                state: "driving".into(),
            })
        );
        let run = store.run("R1").unwrap().unwrap();
        assert_eq!(run.state, "driving");
        assert_eq!(run.exit_code, Some(0));
        assert_eq!(store.run_evidence("R1").unwrap(), evidence);
    }

    #[test]
    fn agent_exit_checkpoint_reports_terminal_and_missing_runs() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));
        running_r1(&store);
        settle_run(&store, "R1", Some(0), Outcome::Merged, &[], None, 2_200);

        let claim = store
            .record_agent_exit(
                "R1",
                1,
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
            1,
            Some(0),
            true,
            r#"{"count":0,"oids":[]}"#,
            None,
            None,
            2_300,
        );
        assert!(matches!(missing, Err(StoreError::RunNotFound { .. })));
    }

    /// The log's order is the order rows were appended, not the order of any
    /// key on them: a stage re-entered later reads back after the stages that
    /// ran in between, which is what makes the fold's replay faithful.
    #[test]
    fn the_stage_log_reads_back_in_append_order() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));
        running_r1(&store);

        store
            .append_stage_rows(
                "R1",
                &[stage_row(0, "build", 1, StagePhase::Action, Some("passed"))],
            )
            .unwrap();
        store
            .append_stage_rows(
                "R1",
                &[
                    stage_row(1, "verify", 1, StagePhase::Action, None),
                    stage_row(1, "verify", 1, StagePhase::Check, Some("failed")),
                ],
            )
            .unwrap();
        store
            .append_stage_rows(
                "R1",
                &[stage_row(0, "build", 2, StagePhase::Action, Some("passed"))],
            )
            .unwrap();

        assert_eq!(
            log_shape(&store),
            [
                (
                    0,
                    "build".into(),
                    1,
                    StagePhase::Action,
                    Some("passed".into())
                ),
                (1, "verify".into(), 1, StagePhase::Action, None),
                (
                    1,
                    "verify".into(),
                    1,
                    StagePhase::Check,
                    Some("failed".into())
                ),
                (
                    0,
                    "build".into(),
                    2,
                    StagePhase::Action,
                    Some("passed".into())
                ),
            ]
        );
        let unresolved = &store.stage_log("R1").unwrap()[1];
        assert_eq!(unresolved.verdict_source, None);
        assert_eq!(unresolved.output_ref, "runs/R1/output.ndjson");
    }

    /// Re-appending a row the natural key already holds corrects it in place.
    /// It must not take a new position: a rewrite that jumped to the tail
    /// would reorder history the fold has already replayed.
    #[test]
    fn rewriting_a_stage_row_keeps_the_position_it_first_took() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));
        running_r1(&store);

        store
            .append_stage_rows(
                "R1",
                &[
                    stage_row(0, "build", 1, StagePhase::Action, Some("passed")),
                    stage_row(1, "test", 1, StagePhase::Action, Some("failed")),
                ],
            )
            .unwrap();
        store
            .append_stage_rows(
                "R1",
                &[stage_row(0, "build", 1, StagePhase::Action, Some("failed"))],
            )
            .unwrap();

        assert_eq!(
            log_shape(&store),
            [
                (
                    0,
                    "build".into(),
                    1,
                    StagePhase::Action,
                    Some("failed".into())
                ),
                (
                    1,
                    "test".into(),
                    1,
                    StagePhase::Action,
                    Some("failed".into())
                ),
            ]
        );
    }

    /// Rows written before the table became a log are one whole execution
    /// each. They backfill in the order the linear walk produced them, so a
    /// run planted in the old shape replays to the same position afterwards.
    #[test]
    fn version_thirteen_backfills_the_stage_log_in_walk_order() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sloop.db");
        let store = open_seeded(&path);
        running_r1(&store);
        drop(store);

        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(&format!(
                "DROP TABLE stage_runs;
                 CREATE TABLE {LEGACY_STAGE_TABLE} (
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
                 INSERT INTO {LEGACY_STAGE_TABLE}
                     (run_id, stage_index, stage, state, attempt, started_at_ms,
                      finished_at_ms, exit_code, evidence_json)
                 VALUES
                     ('R1', 1, 'test', 'passed', 1, 3000, 3100, 0, NULL),
                     ('R1', 0, 'build', 'passed', 1, 2900, 3000, 0,
                      '{{\"output\":\"runs/R1/output.ndjson\",\"verdict_source\":\"reported\"}}');
                 PRAGMA user_version = 13;"
            ))
            .unwrap();
        connection.execute_batch(REVERT_TRIGGER_RENAME).unwrap();
        drop(connection);

        let store = reopen(&path);
        assert_eq!(
            log_shape(&store),
            [
                (
                    0,
                    "build".into(),
                    1,
                    StagePhase::Action,
                    Some("passed".into())
                ),
                (
                    1,
                    "test".into(),
                    1,
                    StagePhase::Action,
                    Some("passed".into())
                ),
            ]
        );
        let migrated = store.stage_log("R1").unwrap();
        assert_eq!(migrated[0].verdict_source.as_deref(), Some("reported"));
        assert_eq!(migrated[1].verdict_source.as_deref(), Some("exit_code"));

        store
            .append_stage_rows(
                "R1",
                &[stage_row(2, "merge", 1, StagePhase::Action, Some("passed"))],
            )
            .unwrap();
        assert_eq!(
            store
                .db()
                .lock()
                .query_row(
                    "SELECT seq FROM stage_runs WHERE run_id = 'R1' AND stage = 'merge'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            3
        );
    }

    #[test]
    fn run_store_deserializes_vendor_errors_and_returns_typed_project_evidence() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));
        running_r1(&store);
        let vendor_error = VendorErrorMatch {
            class: VendorErrorClass::RateLimited,
            vendor: "claude".into(),
            rule_id: "capacity".into(),
            diagnostic: "try later".into(),
        };

        store
            .record_agent_exit(
                "R1",
                1,
                Some(1),
                true,
                r#"{"oids":["abc"]}"#,
                Some(&vendor_error),
                Some(9_000),
                2_200,
            )
            .unwrap();

        assert_eq!(
            store.vendor_error_for_run("R1").unwrap(),
            Some(vendor_error.clone())
        );
        assert_eq!(
            store.latest_vendor_error_for_ticket("T1").unwrap(),
            Some(vendor_error)
        );
        let records = store.commit_evidence_for_project("default").unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].run_id, "R1");
        assert_eq!(records[0].ticket_id, "T1");
        assert_eq!(records[0].data_json, r#"{"oids":["abc"]}"#);
    }
}
