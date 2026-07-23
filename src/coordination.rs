//! Answers scheduling mutation intents with granted or denied decisions.
//!
//! This is the daemon's only path for mutating runtime scheduling state through
//! claims, leases, and run settlement. Read-only queries are not coordination
//! and remain on [`Store`]. Rust's sibling-module visibility cannot enforce the
//! boundary, so daemon code must not call the wrapped store methods directly.
//!
//! # Lease invariants
//!
//! A lease is time-bounded ownership of a ticket by the daemon, taken
//! atomically at claim time. In the `leases` table `ticket_id` is the PRIMARY
//! KEY and `run_id` is UNIQUE, so the database engine enforces at most one
//! lease per ticket and per run. That is the durable guard against
//! double-spawn, backstopping the conditional `UPDATE ... WHERE state='ready'`
//! inside [`Coordination::claim`].
//!
//! Leases are held only by the daemon. `owner_id` records which daemon process
//! took the claim. Workers never hold, renew, or observe leases: a worker's
//! only credential is a per-run capability token granting the worker verbs on
//! its own run. The daemon-to-worker relationship is delegation of access to a
//! run, never sub-leasing of ownership of a ticket.
//!
//! `expires_at_ms` gates renewal only. An expired lease cannot be renewed, so a
//! revived process cannot resurrect a claim recovery has decided is lost.
//! Liveness of a run is determined by process identity — pid, pid start time,
//! and process group id — never by lease expiry.
//!
//! A lease is released by deleting its row: on settlement (`settle`) or on
//! claim rollback (`abandon`). An expired-but-present lease row is evidence
//! of an owner that died mid-work.
//!
//! The daemon renews the lease of every run it actively supervises, from the
//! periodic reconcile pass, so `expires_at_ms` is truthful for as long as a run
//! is alive. Renewal is a statement of fact, not an authority: a renewal denial
//! is logged and changes no scheduling decision, and recovery still keys off
//! process identity rather than the clock. Because renewal is strict, a daemon
//! that was down past the TTL re-arms an adopted run's lapsed lease through
//! [`Coordination::readopt`] instead.

use std::collections::BTreeSet;

use crate::db::Db;
use crate::domain::ticket::TicketState;
use crate::outcome::Outcome;
use crate::run_store::{RunState, evidence, limits, runs};
use crate::store::{
    ClaimRequest, ClaimedRun, CooldownUpdate, EvidenceRecord, ExitClaim, Store, StoreError,
};
use crate::work_state::local::{self, LocalSqlite, ReindexResult, ReindexTicket};
use rusqlite::TransactionBehavior;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Claim {
    Granted(ClaimedRun),
    Denied(ClaimDenial),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimDenial {
    NotReady,
    ActivationNotQueued,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Renewal {
    Granted(i64),
    Denied(RenewalDenial),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenewalDenial {
    LeaseNotHeld,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Start {
    Granted,
    Denied(StartDenial),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartDenial {
    /// The run left `claimed` before its process was recorded — it was
    /// aborted, recovered, or already started. `state` is the run's state as
    /// stored, absent when the run itself is gone.
    NotClaimed { state: Option<String> },
}

/// Ownership of a run's exit processing. Whoever is granted the checkpoint
/// owns aftercare; the supervisor and crash recovery race for it deliberately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Exit {
    Granted,
    Denied(ExitDenial),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExitDenial {
    /// Another path checkpointed this exit first and owns aftercare.
    AlreadyClaimed { state: String },
}

/// Facts about a launched agent process, recorded when the run turns
/// `running`.
pub struct RunStart<'a> {
    pub run_id: &'a str,
    pub branch: &'a str,
    pub worktree_path: &'a str,
    pub pid: u32,
    pub pid_start_time: Option<i64>,
    pub process_group_id: u32,
    pub worker_token: &'a str,
    pub worker_socket_path: &'a str,
}

/// Facts about an agent's exit, recorded at the checkpoint that hands the run
/// to aftercare.
pub struct RunExit<'a> {
    pub run_id: &'a str,
    pub exit_code: Option<i32>,
    pub capture_complete: bool,
    pub commits_json: &'a str,
    pub vendor_error: Option<&'a crate::vendor_error::VendorErrorMatch>,
    pub cooldown_until_ms: Option<i64>,
}

pub struct Coordination(Store);

impl Coordination {
    pub fn new(store: &mut Store) -> Self {
        Self(Store::from_db(store.db()))
    }

    #[cfg(test)]
    pub(crate) fn from_shared(store: &Store) -> Self {
        Self(Store::from_db(store.db()))
    }

    pub fn claim(&mut self, claim: &ClaimRequest<'_>, now_ms: i64) -> Result<Claim, StoreError> {
        let result = self.claim_transaction(claim, now_ms);
        match result {
            Ok(claimed) => Ok(Claim::Granted(claimed)),
            Err(StoreError::TicketNotReady { .. }) => Ok(Claim::Denied(ClaimDenial::NotReady)),
            Err(StoreError::ActivationNotQueued { .. }) => {
                Ok(Claim::Denied(ClaimDenial::ActivationNotQueued))
            }
            Err(error) => Err(error),
        }
    }

    fn claim_transaction(
        &self,
        claim: &ClaimRequest<'_>,
        now_ms: i64,
    ) -> Result<ClaimedRun, StoreError> {
        let db = self.0.db();
        let mut connection = db.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;

        local::tx::claim_ticket(&transaction, claim, now_ms)?;
        local::tx::advance_activation(&transaction, claim, now_ms)?;

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

        let expires_at_ms = local::tx::insert_lease(&transaction, claim, now_ms)?;
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

    pub fn renew(
        &mut self,
        ticket_id: &str,
        run_id: &str,
        lease_ms: i64,
        now_ms: i64,
    ) -> Result<Renewal, StoreError> {
        match self.0.renew_lease(ticket_id, run_id, lease_ms, now_ms) {
            Ok(expires_at_ms) => Ok(Renewal::Granted(expires_at_ms)),
            Err(StoreError::LeaseNotHeld { .. }) => {
                Ok(Renewal::Denied(RenewalDenial::LeaseNotHeld))
            }
            Err(error) => Err(error),
        }
    }

    /// Re-arms the lease of a run recovery has just adopted, accepting a lease
    /// that expired while this daemon was down. Denied for a run that has
    /// already settled.
    pub fn readopt(
        &mut self,
        ticket_id: &str,
        run_id: &str,
        lease_ms: i64,
        now_ms: i64,
    ) -> Result<Renewal, StoreError> {
        match self.0.readopt_lease(ticket_id, run_id, lease_ms, now_ms) {
            Ok(expires_at_ms) => Ok(Renewal::Granted(expires_at_ms)),
            Err(StoreError::LeaseNotHeld { .. }) => {
                Ok(Renewal::Denied(RenewalDenial::LeaseNotHeld))
            }
            Err(error) => Err(error),
        }
    }

    /// Turns a claimed run `running` once its agent process exists.
    ///
    /// Takes the store by shared reference because the runner's stage hooks
    /// that call it hold only a shared borrow.
    pub fn start(store: &Store, start: &RunStart<'_>, now_ms: i64) -> Result<Start, StoreError> {
        let db = store.db();
        let mut connection = db.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = runs::tx::mark_running(
            &transaction,
            start.run_id,
            start.branch,
            start.worktree_path,
            start.pid,
            start.pid_start_time,
            start.process_group_id,
            start.worker_token,
            start.worker_socket_path,
            now_ms,
        )?;
        if changed != 1 {
            let state = runs::tx::state(&transaction, start.run_id)?;
            return Ok(Start::Denied(StartDenial::NotClaimed { state }));
        }
        let ticket_id = runs::tx::ticket_id(&transaction, start.run_id)?;
        runs::tx::record_event(
            &transaction,
            now_ms,
            "run_started",
            Some(start.run_id),
            Some(&ticket_id),
            "{}",
        )?;
        transaction.commit()?;
        Ok(Start::Granted)
    }

    /// Checkpoints an agent's exit, granting the caller ownership of aftercare.
    /// The supervisor and crash recovery may both attempt this; exactly one is
    /// granted.
    pub fn record_exit(&mut self, exit: &RunExit<'_>, now_ms: i64) -> Result<Exit, StoreError> {
        match self.0.record_agent_exit(
            exit.run_id,
            exit.exit_code,
            exit.capture_complete,
            exit.commits_json,
            exit.vendor_error,
            exit.cooldown_until_ms,
            now_ms,
        ) {
            Ok(ExitClaim::Claimed) => Ok(Exit::Granted),
            Ok(ExitClaim::AlreadyClaimed { state }) => {
                Ok(Exit::Denied(ExitDenial::AlreadyClaimed { state }))
            }
            Err(error) => Err(error),
        }
    }

    /// Rolls back a claim whose launch failed before a process existed: the
    /// lease is released, the run is closed, and the ticket returns to
    /// `ready`. The consumed attempt is kept as evidence of the try.
    pub fn abandon(
        &mut self,
        run_id: &str,
        ticket_id: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        let db = self.0.db();
        let mut connection = db.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        local::tx::delete_lease(&transaction, run_id)?;
        runs::tx::abort(&transaction, run_id, now_ms)?;
        local::tx::abort_ticket(&transaction, ticket_id, now_ms)?;
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

    /// Terminates a run in one transaction: the raw exit and derived outcome
    /// land on the run, evidence is appended, the lease is freed, and the
    /// ticket moves to its terminal state or back to `ready` when cancellation
    /// or recovery releases it.
    #[allow(clippy::too_many_arguments)]
    pub fn settle(
        &mut self,
        run_id: &str,
        ticket_id: &str,
        exit_code: Option<i32>,
        outcome: Outcome,
        evidence: &[EvidenceRecord],
        cooldown: Option<&CooldownUpdate<'_>>,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        let db = self.0.db();
        let mut connection = db.lock();
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
        local::tx::delete_lease(&transaction, run_id)?;

        let ticket_state = TicketState::after_outcome(outcome);
        local::tx::settle_ticket(&transaction, ticket_id, ticket_state, now_ms)?;
        if outcome == Outcome::RateLimited {
            let activation_id = runs::tx::activation_id(&transaction, run_id)?;
            local::tx::requeue_activation(&transaction, &activation_id, now_ms)?;
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

    /// Settles a `needs_review` ticket whose run branch an operator merged by
    /// hand. The ticket-state gate makes a repeated pass a no-op and the
    /// evidence dedupe key keeps the observation unique across restarts.
    #[allow(clippy::too_many_arguments)]
    pub fn settle_external_merge(
        &mut self,
        run_id: &str,
        ticket_id: &str,
        branch: &str,
        branch_tip: &str,
        observed_default_tip: &str,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        let db = self.0.db();
        let mut connection = db.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = local::tx::settle_external_merge(&transaction, ticket_id, now_ms)?;
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
}

pub(crate) fn apply_reindex(
    db: Db,
    project_ids: &[String],
    tickets: &[ReindexTicket],
    now_ms: i64,
) -> Result<ReindexResult, StoreError> {
    LocalSqlite::from_db(db).apply_reindex(
        project_ids,
        tickets,
        now_ms,
        |transaction, stale_tickets, doomed_activations| {
            let mut doomed_runs = BTreeSet::new();
            for ticket_id in stale_tickets {
                doomed_runs.extend(runs::tx::ids_for_ticket(transaction, ticket_id)?);
            }
            for activation_id in doomed_activations {
                doomed_runs.extend(runs::tx::ids_for_activation(transaction, activation_id)?);
            }

            let mut rows_dropped = 0;
            for run_id in &doomed_runs {
                limits::tx::detach_cooldowns_from_run(transaction, run_id)?;
                rows_dropped += local::tx::delete_lease(transaction, run_id)?;
                rows_dropped += evidence::tx::delete_for_run(transaction, run_id)?;
                rows_dropped += limits::tx::delete_budget_reservation_for_run(transaction, run_id)?;
                rows_dropped += runs::tx::delete_notes_for_run(transaction, run_id)?;
                rows_dropped += runs::tx::delete(transaction, run_id)?;
            }
            Ok(rows_dropped)
        },
        |transaction, ticket_id, now_ms| {
            runs::tx::mark_failed_or_review_runs_cleanup_eligible(transaction, ticket_id, now_ms)?;
            Ok(())
        },
    )
}

pub(crate) fn retry_ticket(db: Db, id: &str, now_ms: i64) -> Result<String, StoreError> {
    LocalSqlite::from_db(db).retry_ticket(id, now_ms, |transaction, id, now_ms| {
        runs::tx::mark_ticket_runs_cleanup_eligible(transaction, id, RunState::Failed, now_ms)?;
        Ok(())
    })
}

pub(crate) fn ticket_is_referenced(db: Db, id: &str) -> Result<bool, StoreError> {
    let connection = db.lock();
    Ok(runs::ticket_is_referenced(&connection, id)?
        || LocalSqlite::ticket_has_work_references_on(&connection, id)?)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use tempfile::tempdir;

    use super::{
        Claim, ClaimDenial, Coordination, Exit, ExitDenial, Renewal, RenewalDenial, RunExit,
        RunStart, Start, StartDenial,
    };
    use crate::domain::ticket::TicketState;
    use crate::outcome::Outcome;
    use crate::store::{
        ActivationKind, ClaimRequest, EvidenceRecord, NewActivation, StageRecord, Store, StoreError,
    };

    fn claim_t1(run_id: &str) -> ClaimRequest<'_> {
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

    fn seeded_store(directory: &TempDir) -> Store {
        let store = Store::open(&directory.path().join("sloop.db"), 1_000).unwrap();
        store
            .insert_local_project("default", "projects/default.md", "Default", 1_000)
            .unwrap();
        store
            .insert_local_ticket(
                "T1",
                "default",
                "tickets/T1.md",
                "Ticket one",
                &[],
                "sloop/T1",
                Some("opencode"),
                None,
                None,
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
    fn claiming_twice_is_denied_instead_of_failing() {
        let directory = tempdir().unwrap();
        let mut store = seeded_store(&directory);
        let mut coordination = Coordination::new(&mut store);

        let Claim::Granted(claimed) = coordination.claim(&claim_t1("R1"), 2_000).unwrap() else {
            panic!("first claim was denied");
        };
        assert_eq!(claimed.attempt, 1);
        assert_eq!(claimed.lease_expires_at_ms, 62_000);
        assert_eq!(
            coordination.claim(&claim_t1("R2"), 2_100).unwrap(),
            Claim::Denied(ClaimDenial::NotReady)
        );
        assert!(matches!(
            coordination.claim_transaction(&claim_t1("R3"), 2_200),
            Err(StoreError::TicketNotReady { state: Some(state), .. }) if state == "claimed"
        ));
    }

    #[test]
    fn claiming_an_unknown_ticket_reports_it_not_ready() {
        let directory = tempdir().unwrap();
        let store = seeded_store(&directory);
        let mut coordination = Coordination::from_shared(&store);

        let request = ClaimRequest {
            ticket_id: "missing",
            ..claim_t1("R1")
        };
        assert_eq!(
            coordination.claim(&request, 2_000).unwrap(),
            Claim::Denied(ClaimDenial::NotReady)
        );
        assert!(matches!(
            coordination.claim_transaction(&request, 2_000),
            Err(StoreError::TicketNotReady { state: None, .. })
        ));
    }

    #[test]
    fn missing_and_blocked_ticket_diagnostics_are_preserved() {
        let directory = tempdir().unwrap();
        let store = seeded_store(&directory);
        store.mark_ticket_missing("T1", 2_000).unwrap();
        let coordination = Coordination::from_shared(&store);

        assert!(matches!(
            coordination.claim_transaction(&claim_t1("R1"), 2_000),
            Err(StoreError::TicketNotReady { state: Some(state), .. }) if state == "missing"
        ));

        store.clear_ticket_missing("T1", 2_100).unwrap();
        store
            .insert_local_ticket(
                "T2",
                "default",
                "tickets/T2.md",
                "Ticket two",
                &["T1".into()],
                "sloop/T2",
                Some("opencode"),
                None,
                None,
                "default",
                TicketState::Ready,
                2_100,
            )
            .unwrap();
        store
            .db()
            .lock()
            .execute("UPDATE tickets SET state = 'failed' WHERE id = 'T1'", [])
            .unwrap();
        let request = ClaimRequest {
            ticket_id: "T2",
            run_id: "R2",
            ..claim_t1("R1")
        };
        assert!(matches!(
            coordination.claim_transaction(&request, 2_200),
            Err(StoreError::TicketNotReady { state: Some(state), .. }) if state == "blocked"
        ));
        assert_eq!(store.ticket("T2").unwrap().unwrap().attempts, 0);
    }

    #[test]
    fn concurrent_connections_cannot_both_claim_one_ticket() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sloop.db");
        drop(seeded_store(&directory));

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let claims: Vec<_> = ["R1", "R2"]
            .into_iter()
            .map(|run_id| {
                let path = path.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let store = Store::open(&path, 2_000).unwrap();
                    barrier.wait();
                    matches!(
                        Coordination::from_shared(&store)
                            .claim(&claim_t1(run_id), 2_000)
                            .unwrap(),
                        Claim::Granted(_)
                    )
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
    fn only_one_caller_is_granted_a_runs_exit_checkpoint() {
        let directory = tempdir().unwrap();
        let mut store = seeded_store(&directory);
        let mut coordination = Coordination::new(&mut store);
        coordination.claim(&claim_t1("R1"), 2_000).unwrap();
        let start = RunStart {
            run_id: "R1",
            branch: "sloop/T1",
            worktree_path: "/tmp/w",
            pid: 4_242,
            pid_start_time: Some(7),
            process_group_id: 4_242,
            worker_token: "token",
            worker_socket_path: "/tmp/w.sock",
        };
        assert_eq!(
            Coordination::start(&store, &start, 2_100).unwrap(),
            Start::Granted
        );

        let exit = RunExit {
            run_id: "R1",
            exit_code: Some(0),
            capture_complete: true,
            commits_json: "{}",
            vendor_error: None,
            cooldown_until_ms: None,
        };
        let mut coordination = Coordination::new(&mut store);
        assert_eq!(
            coordination.record_exit(&exit, 3_000).unwrap(),
            Exit::Granted
        );
        // The supervisor and crash recovery race here deliberately; the loser
        // is denied rather than failed, and does not own aftercare.
        assert_eq!(
            coordination.record_exit(&exit, 3_100).unwrap(),
            Exit::Denied(ExitDenial::AlreadyClaimed {
                state: "aftercare".into()
            })
        );
    }

    #[test]
    fn finish_run_settles_exactly_once() {
        let directory = tempdir().unwrap();
        let mut store = seeded_store(&directory);
        let mut coordination = Coordination::new(&mut store);
        coordination.claim(&claim_t1("R1"), 2_000).unwrap();
        assert_eq!(
            Coordination::start(
                &store,
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

        Coordination::new(&mut store)
            .settle("R1", "T1", Some(0), Outcome::Merged, &[], None, 2_300)
            .unwrap();
        assert_eq!(store.ticket_state("T1").unwrap().as_deref(), Some("merged"));
        assert_eq!(store.active_run_for_ticket("T1").unwrap(), None);
        let evidence = store.run_evidence("R1").unwrap();

        Coordination::new(&mut store)
            .settle("R1", "T1", Some(1), Outcome::Failed, &[], None, 2_400)
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
        let mut store = seeded_store(&directory);
        Coordination::new(&mut store)
            .claim(&claim_t1("R1"), 2_000)
            .unwrap();
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

        Coordination::new(&mut store)
            .settle(
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
        let mut store = seeded_store(&directory);
        Coordination::new(&mut store)
            .claim(&claim_t1("R1"), 2_000)
            .unwrap();
        let evidence = [EvidenceRecord {
            kind: "exit_classified",
            data_json: "{\"exit_code\":1}".into(),
        }];

        Coordination::new(&mut store)
            .settle("R1", "T1", Some(1), Outcome::Failed, &evidence, None, 3_000)
            .unwrap();
        Coordination::new(&mut store)
            .settle("R1", "T1", Some(1), Outcome::Failed, &evidence, None, 3_100)
            .unwrap();

        assert_eq!(store.run_evidence("R1").unwrap().len(), 1);
        assert_eq!(store.run("R1").unwrap().unwrap().exited_at_ms, Some(3_000));
    }

    #[test]
    fn a_cancelled_outcome_returns_the_ticket_to_ready() {
        let directory = tempdir().unwrap();
        let mut store = seeded_store(&directory);
        Coordination::new(&mut store)
            .claim(&claim_t1("R1"), 2_000)
            .unwrap();

        assert!(!store.cancellation_requested("R1").unwrap());
        store.record_cancel_requested("R1", 2_500).unwrap();
        store.record_cancel_requested("R1", 2_600).unwrap();
        assert!(store.cancellation_requested("R1").unwrap());

        Coordination::new(&mut store)
            .settle("R1", "T1", None, Outcome::Cancelled, &[], None, 3_000)
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

    #[test]
    fn aborted_claims_are_closed_and_no_longer_active() {
        let directory = tempdir().unwrap();
        let mut store = seeded_store(&directory);
        let mut coordination = Coordination::new(&mut store);
        coordination.claim(&claim_t1("R1"), 2_000).unwrap();

        coordination.abandon("R1", "T1", 2_100).unwrap();

        assert_eq!(store.run("R1").unwrap().unwrap().state, "aborted");
        assert_eq!(store.active_run_for_ticket("T1").unwrap(), None);
        assert_eq!(store.ticket_state("T1").unwrap().as_deref(), Some("ready"));
    }

    #[test]
    fn orphaning_a_run_releases_the_ticket_without_failing_it() {
        let directory = tempdir().unwrap();
        let mut store = seeded_store(&directory);
        let mut coordination = Coordination::new(&mut store);
        coordination.claim(&claim_t1("R1"), 2_000).unwrap();

        coordination
            .settle("R1", "T1", None, Outcome::Orphaned, &[], None, 3_000)
            .unwrap();

        assert_eq!(store.run("R1").unwrap().unwrap().state, "orphaned");
        assert_eq!(store.ticket_state("T1").unwrap().as_deref(), Some("ready"));
    }

    #[test]
    fn external_merge_settlement_is_atomic_and_idempotent() {
        let directory = tempdir().unwrap();
        let mut store = seeded_store(&directory);
        let mut coordination = Coordination::new(&mut store);
        coordination.claim(&claim_t1("R1"), 2_000).unwrap();
        coordination
            .settle("R1", "T1", Some(0), Outcome::NeedsReview, &[], None, 3_000)
            .unwrap();

        assert!(
            coordination
                .settle_external_merge("R1", "T1", "sloop/T1", "abc", "def", 4_000)
                .unwrap()
        );
        assert_eq!(store.ticket_state("T1").unwrap().as_deref(), Some("merged"));
        let evidence = store.run_evidence("R1").unwrap();
        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].0, "external_merge_observed");
        assert!(
            !coordination
                .settle_external_merge("R1", "T1", "sloop/T1", "abc", "def", 4_100)
                .unwrap()
        );
        assert_eq!(store.run_evidence("R1").unwrap(), evidence);
    }

    #[test]
    fn starting_a_run_that_left_claimed_is_denied() {
        let directory = tempdir().unwrap();
        let mut store = seeded_store(&directory);
        let mut coordination = Coordination::new(&mut store);
        coordination.claim(&claim_t1("R1"), 2_000).unwrap();
        coordination.abandon("R1", "T1", 2_050).unwrap();

        let start = RunStart {
            run_id: "R1",
            branch: "sloop/T1",
            worktree_path: "/tmp/w",
            pid: 4_242,
            pid_start_time: Some(7),
            process_group_id: 4_242,
            worker_token: "token",
            worker_socket_path: "/tmp/w.sock",
        };
        assert_eq!(
            Coordination::start(&store, &start, 2_100).unwrap(),
            Start::Denied(StartDenial::NotClaimed {
                state: Some("aborted".into())
            })
        );
    }

    #[test]
    fn readopting_re_arms_an_expired_lease_that_renewal_refuses() {
        let directory = tempdir().unwrap();
        let mut store = seeded_store(&directory);
        let mut coordination = Coordination::new(&mut store);
        coordination.claim(&claim_t1("R1"), 2_000).unwrap();

        // The claim's lease expired at 62_000.
        assert_eq!(
            coordination.renew("T1", "R1", 60_000, 90_000).unwrap(),
            Renewal::Denied(RenewalDenial::LeaseNotHeld)
        );
        assert_eq!(
            coordination.readopt("T1", "R1", 60_000, 90_000).unwrap(),
            Renewal::Granted(150_000)
        );
        assert_eq!(
            coordination.renew("T1", "R1", 60_000, 100_000).unwrap(),
            Renewal::Granted(160_000)
        );
    }
}
