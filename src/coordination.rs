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
//! inside `claim_ticket`.
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
//! A lease is released by deleting its row: on settlement (`finish_run`) or on
//! claim rollback (`abort_claim`). An expired-but-present lease row is evidence
//! of an owner that died mid-work.
//!
//! The daemon renews the lease of every run it actively supervises, from the
//! periodic reconcile pass, so `expires_at_ms` is truthful for as long as a run
//! is alive. Renewal is a statement of fact, not an authority: a renewal denial
//! is logged and changes no scheduling decision, and recovery still keys off
//! process identity rather than the clock. Because renewal is strict, a daemon
//! that was down past the TTL re-arms an adopted run's lapsed lease through
//! [`Coordination::readopt`] instead.

use crate::outcome::Outcome;
use crate::store::{
    ClaimRequest, ClaimedRun, CooldownUpdate, EvidenceRecord, ExitClaim, Store, StoreError,
};

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

pub struct Coordination<'a>(&'a mut Store);

impl<'a> Coordination<'a> {
    pub fn new(store: &'a mut Store) -> Self {
        Self(store)
    }

    pub fn claim(&mut self, claim: &ClaimRequest<'_>, now_ms: i64) -> Result<Claim, StoreError> {
        match self.0.claim_ticket(claim, now_ms) {
            Ok(claimed) => Ok(Claim::Granted(claimed)),
            Err(StoreError::TicketNotReady { .. }) => Ok(Claim::Denied(ClaimDenial::NotReady)),
            Err(StoreError::ActivationNotQueued { .. }) => {
                Ok(Claim::Denied(ClaimDenial::ActivationNotQueued))
            }
            Err(error) => Err(error),
        }
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
    /// Takes the store by shared reference: the transition needs no exclusive
    /// connection state, and the runner's stage hooks that call it hold only a
    /// shared borrow. It is an associated function for that reason alone —
    /// the coordination boundary is the same.
    pub fn start(store: &Store, start: &RunStart<'_>, now_ms: i64) -> Result<Start, StoreError> {
        match store.mark_run_running(
            start.run_id,
            start.branch,
            start.worktree_path,
            start.pid,
            start.pid_start_time,
            start.process_group_id,
            start.worker_token,
            start.worker_socket_path,
            now_ms,
        ) {
            Ok(()) => Ok(Start::Granted),
            Err(StoreError::RunStateConflict { state, .. }) => {
                Ok(Start::Denied(StartDenial::NotClaimed { state }))
            }
            Err(error) => Err(error),
        }
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

    pub fn abandon(
        &mut self,
        run_id: &str,
        ticket_id: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.0.abort_claim(run_id, ticket_id, now_ms)
    }

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
        self.0.finish_run(
            run_id, ticket_id, exit_code, outcome, evidence, cooldown, now_ms,
        )
    }
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
    use crate::store::{ActivationKind, ClaimRequest, NewActivation, Store};

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

        assert!(matches!(
            coordination.claim(&claim_t1("R1"), 2_000).unwrap(),
            Claim::Granted(_)
        ));
        assert_eq!(
            coordination.claim(&claim_t1("R2"), 2_100).unwrap(),
            Claim::Denied(ClaimDenial::NotReady)
        );
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
