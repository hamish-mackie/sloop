use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::RunStore;

/// One appended `run_evidence` row: a kind plus kind-specific JSON facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceRecord {
    pub kind: &'static str,
    pub data_json: String,
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

pub(crate) mod tx {
    use rusqlite::{Transaction, params};

    use super::{EvidenceRecord, StageRecord};

    pub(crate) fn delete_for_run(
        transaction: &Transaction<'_>,
        run_id: &str,
    ) -> rusqlite::Result<usize> {
        let evidence = transaction.execute(
            "DELETE FROM run_evidence WHERE run_id = ?1",
            params![run_id],
        )?;
        let stages = transaction.execute(
            "DELETE FROM aftercare_stages WHERE run_id = ?1",
            params![run_id],
        )?;
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_agent_exit(
        transaction: &Transaction<'_>,
        run_id: &str,
        exit_code: Option<i32>,
        capture_complete: bool,
        commits_json: &str,
        vendor_error: Option<&crate::vendor_error::VendorErrorMatch>,
        cooldown_until_ms: Option<i64>,
        now_ms: i64,
    ) -> rusqlite::Result<()> {
        let evidence = [
            EvidenceRecord {
                kind: "exit_classified",
                data_json: serde_json::json!({"exit_code": exit_code}).to_string(),
            },
            EvidenceRecord {
                kind: "commits_observed",
                data_json: commits_json.to_owned(),
            },
        ];
        record_settlement(transaction, run_id, &evidence, now_ms)?;
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
        Ok(())
    }

    pub(crate) fn record_aftercare_stage(
        transaction: &Transaction<'_>,
        run_id: &str,
        stage: &StageRecord,
    ) -> rusqlite::Result<()> {
        let evidence_json = serde_json::json!({
            "output": stage.output_ref,
            "verdict_source": stage.verdict_source,
            "reason": stage.reason,
        })
        .to_string();
        transaction.execute(
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

    pub(crate) fn record_aftercare_evidence(
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

    pub(crate) fn clear_aftercare_process(
        transaction: &Transaction<'_>,
        run_id: &str,
    ) -> rusqlite::Result<()> {
        transaction.execute(
            "DELETE FROM run_evidence
             WHERE run_id = ?1 AND dedupe_key = 'settlement:' || ?1 || ':aftercare_process'",
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

    pub(crate) fn record_stage_verdict(
        transaction: &Transaction<'_>,
        run_id: &str,
        stage: &str,
        verdict: &str,
        reason: Option<&str>,
        now_ms: i64,
    ) -> rusqlite::Result<bool> {
        let dedupe_key = format!("verdict:{run_id}:{stage}");
        let data_json =
            serde_json::json!({"stage": stage, "verdict": verdict, "reason": reason}).to_string();
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO run_evidence
                 (run_id, kind, observed_at_ms, dedupe_key, data_json)
             VALUES (?1, 'stage_verdict', ?2, ?3, ?4)",
            params![run_id, now_ms, dedupe_key, data_json],
        )?;
        Ok(inserted == 1)
    }

    pub(crate) fn record_external_merge(
        transaction: &Transaction<'_>,
        run_id: &str,
        data_json: &str,
        now_ms: i64,
    ) -> rusqlite::Result<()> {
        transaction.execute(
            "INSERT OR IGNORE INTO run_evidence
                 (run_id, kind, observed_at_ms, dedupe_key, data_json)
             VALUES (?1, 'external_merge_observed', ?2, 'external_merge:' || ?1, ?3)",
            params![run_id, now_ms, data_json],
        )?;
        Ok(())
    }

    pub(crate) fn record_repair_attempt(
        transaction: &Transaction<'_>,
        run_id: &str,
        stage: &str,
        attempt: u32,
        data_json: &str,
        now_ms: i64,
    ) -> rusqlite::Result<()> {
        transaction.execute(
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
}

fn aftercare_stages(connection: &Connection, run_id: &str) -> rusqlite::Result<Vec<StageRecord>> {
    let mut statement = connection.prepare(
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

fn commit_evidence_for_project(
    connection: &Connection,
    project_id: &str,
) -> rusqlite::Result<Vec<(String, String, String)>> {
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
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
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
    pub(crate) fn record_aftercare_stage(
        &self,
        run_id: &str,
        stage: &StageRecord,
    ) -> rusqlite::Result<()> {
        self.write(TransactionBehavior::Deferred, |transaction| {
            tx::record_aftercare_stage(transaction, run_id, stage)
        })
    }

    pub(crate) fn aftercare_stages(&self, run_id: &str) -> rusqlite::Result<Vec<StageRecord>> {
        aftercare_stages(&self.db.lock(), run_id)
    }

    pub(crate) fn record_aftercare_evidence(
        &self,
        run_id: &str,
        kind: &str,
        data_json: &str,
        now_ms: i64,
    ) -> rusqlite::Result<()> {
        self.write(TransactionBehavior::Deferred, |transaction| {
            tx::record_aftercare_evidence(transaction, run_id, kind, data_json, now_ms)
        })
    }

    pub(crate) fn clear_aftercare_process(&self, run_id: &str) -> rusqlite::Result<()> {
        self.write(TransactionBehavior::Deferred, |transaction| {
            tx::clear_aftercare_process(transaction, run_id)
        })
    }

    pub(crate) fn record_cancel_requested(
        &self,
        run_id: &str,
        now_ms: i64,
    ) -> rusqlite::Result<()> {
        self.write(TransactionBehavior::Deferred, |transaction| {
            tx::record_cancel_requested(transaction, run_id, now_ms)
        })
    }

    pub(crate) fn cancellation_requested(&self, run_id: &str) -> rusqlite::Result<bool> {
        cancellation_requested(&self.db.lock(), run_id)
    }

    pub(crate) fn record_stage_verdict(
        &self,
        run_id: &str,
        stage: &str,
        verdict: &str,
        reason: Option<&str>,
        now_ms: i64,
    ) -> rusqlite::Result<bool> {
        self.write(TransactionBehavior::Deferred, |transaction| {
            tx::record_stage_verdict(transaction, run_id, stage, verdict, reason, now_ms)
        })
    }

    pub(crate) fn commit_evidence_for_project(
        &self,
        project_id: &str,
    ) -> rusqlite::Result<Vec<(String, String, String)>> {
        commit_evidence_for_project(&self.db.lock(), project_id)
    }

    pub(crate) fn run_evidence(&self, run_id: &str) -> rusqlite::Result<Vec<(String, String)>> {
        run_evidence(&self.db.lock(), run_id)
    }

    pub(crate) fn vendor_error_for_run(&self, run_id: &str) -> rusqlite::Result<Option<String>> {
        vendor_error_for_run(&self.db.lock(), run_id)
    }

    pub(crate) fn latest_vendor_error_for_ticket(
        &self,
        ticket_id: &str,
    ) -> rusqlite::Result<Option<String>> {
        latest_vendor_error_for_ticket(&self.db.lock(), ticket_id)
    }

    pub(crate) fn record_repair_attempt(
        &self,
        run_id: &str,
        stage: &str,
        attempt: u32,
        data_json: &str,
        now_ms: i64,
    ) -> rusqlite::Result<()> {
        self.write(TransactionBehavior::Deferred, |transaction| {
            tx::record_repair_attempt(transaction, run_id, stage, attempt, data_json, now_ms)
        })
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{EvidenceRecord, StageRecord};
    use crate::domain::ticket::TicketState;
    use crate::outcome::Outcome;
    use crate::store::{
        ActivationKind, ClaimRequest, ExitClaim, NewActivation, RunState, Store, StoreError,
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
    fn agent_exit_and_aftercare_results_are_checkpointed_idempotently() {
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
    fn finishing_a_run_settles_ticket_lease_and_evidence_atomically() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();
        store
            .record_aftercare_stage(
                "R1",
                &StageRecord {
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
                &[EvidenceRecord {
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
        assert!(store.renew_lease("T1", "R1", 60_000, 3_100).is_err());
    }

    #[test]
    fn finishing_a_run_is_idempotent() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();
        let evidence = [EvidenceRecord {
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
    fn a_cancelled_outcome_returns_the_ticket_to_ready() {
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

        let cancels = store
            .run_evidence("R1")
            .unwrap()
            .into_iter()
            .filter(|(kind, _)| kind == "cancel_requested")
            .count();
        assert_eq!(cancels, 1);
    }
}
