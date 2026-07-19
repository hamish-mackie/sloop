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
//! and process group id — never by lease expiry. An expired lease on a live,
//! supervised run is currently normal.
//!
//! A lease is released by deleting its row: on settlement (`finish_run`) or on
//! claim rollback (`abort_claim`). An expired-but-present lease row is evidence
//! of an owner that died mid-work.
//!
//! [`Coordination::renew`] currently has no production caller, so any run
//! outliving its initial lease executes on an expired one and nothing depends
//! on that. The expiry vocabulary in the schema is ahead of the implementation.

use crate::outcome::Outcome;
use crate::store::{ClaimRequest, ClaimedRun, CooldownUpdate, EvidenceRecord, Store, StoreError};

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
    use tempfile::tempdir;

    use super::{Claim, ClaimDenial, Coordination};
    use crate::domain::ticket::TicketState;
    use crate::store::{ActivationKind, ClaimRequest, NewActivation, Store};

    #[test]
    fn claiming_twice_is_denied_instead_of_failing() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(&directory.path().join("sloop.db"), 1_000).unwrap();
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

        let claim = |run_id| ClaimRequest {
            ticket_id: "T1",
            run_id,
            activation_id: "A1",
            owner_id: "daemon-1",
            lease_ms: 60_000,
            next_activation_eligible_at_ms: None,
            flow_json: "{}",
            ticket_json: "{}",
        };
        let mut coordination = Coordination::new(&mut store);

        assert!(matches!(
            coordination.claim(&claim("R1"), 2_000).unwrap(),
            Claim::Granted(_)
        ));
        assert_eq!(
            coordination.claim(&claim("R2"), 2_100).unwrap(),
            Claim::Denied(ClaimDenial::NotReady)
        );
    }
}
