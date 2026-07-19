//! Model-based test of `Store` + `Coordination`.
//!
//! Random sequences of coordination verbs run against the real store on a
//! tempfile SQLite database, mirrored by a deliberately simple in-memory
//! reference model. After every operation the full database state — leases,
//! ticket states, run states, queued activations — must equal the model's
//! prediction, and every grant/denial must match what the model expected.
//!
//! Operations are valid-shaped but freely wrong-state: claiming a claimed
//! ticket, renewing an expired lease, settling a settled run. That region is
//! where scheduler bugs live. The one deliberate restriction is `Abandon`,
//! which is only generated for runs still in `claimed` — matching the single
//! call site (a claim whose spawn failed) — because `abort_claim` assumes the
//! caller owns the claim.

use std::collections::BTreeMap;
use std::path::PathBuf;

use proptest::prelude::*;
use rusqlite::Connection;
use sloop::coordination::{
    Claim, ClaimDenial, Coordination, Exit, Renewal, RunExit, RunStart, Start,
};
use sloop::domain::ticket::TicketState;
use sloop::outcome::Outcome;
use sloop::store::{ActivationKind, ClaimRequest, NewActivation, Store};
use tempfile::TempDir;

const TICKETS: [&str; 3] = ["T0", "T1", "T2"];
const CLAIM_LEASE_MS: i64 = 60_000;

#[derive(Debug, Clone)]
enum Op {
    /// Queue a fresh immediate activation for a ticket.
    Enqueue {
        ticket: usize,
    },
    /// Attempt to claim a ticket with its oldest queued activation, or a
    /// bogus activation id when none is queued.
    Claim {
        ticket: usize,
    },
    Start {
        run: usize,
    },
    RecordExit {
        run: usize,
        exit_code: i32,
    },
    Renew {
        run: usize,
        lease_ms: i64,
    },
    Readopt {
        run: usize,
        lease_ms: i64,
    },
    Abandon {
        run: usize,
    },
    Settle {
        run: usize,
        outcome: Outcome,
    },
    AdvanceClock {
        ms: i64,
    },
}

fn outcome() -> impl Strategy<Value = Outcome> {
    prop_oneof![
        Just(Outcome::Merged),
        Just(Outcome::Failed),
        Just(Outcome::NeedsReview),
        Just(Outcome::Cancelled),
        Just(Outcome::RateLimited),
        Just(Outcome::Orphaned),
    ]
}

fn op() -> impl Strategy<Value = Op> {
    let run = 0..16usize;
    prop_oneof![
        2 => (0..TICKETS.len()).prop_map(|ticket| Op::Enqueue { ticket }),
        3 => (0..TICKETS.len()).prop_map(|ticket| Op::Claim { ticket }),
        2 => run.clone().prop_map(|run| Op::Start { run }),
        2 => (run.clone(), -1i32..3).prop_map(|(run, exit_code)| Op::RecordExit { run, exit_code }),
        1 => (run.clone(), 1_000i64..120_000).prop_map(|(run, lease_ms)| Op::Renew { run, lease_ms }),
        1 => (run.clone(), 1_000i64..120_000).prop_map(|(run, lease_ms)| Op::Readopt { run, lease_ms }),
        1 => run.clone().prop_map(|run| Op::Abandon { run }),
        2 => (run, outcome()).prop_map(|(run, outcome)| Op::Settle { run, outcome }),
        1 => (1i64..90_000).prop_map(|ms| Op::AdvanceClock { ms }),
    ]
}

#[derive(Debug)]
struct ModelRun {
    ticket: String,
    activation: String,
    state: &'static str,
    exited: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct ModelLease {
    run: String,
    expires_at_ms: i64,
}

/// The reference model: small enough to be obviously correct.
#[derive(Debug, Default)]
struct Model {
    /// Ticket id → state string as stored.
    tickets: BTreeMap<String, &'static str>,
    /// Run id → run facts, in creation order via `run_order`.
    runs: BTreeMap<String, ModelRun>,
    run_order: Vec<String>,
    /// Ticket id → the lease it is under, if any.
    leases: BTreeMap<String, ModelLease>,
    /// Ticket id → queued activation ids, oldest first.
    queued: BTreeMap<String, Vec<String>>,
}

struct Harness {
    _directory: TempDir,
    db_path: PathBuf,
    store: Store,
    model: Model,
    now_ms: i64,
    run_counter: usize,
    activation_counter: usize,
}

impl Harness {
    fn new() -> Self {
        let directory = TempDir::new().expect("create tempdir");
        let db_path = directory.path().join("sloop.db");
        let now_ms = 1_000;
        let store = Store::open(&db_path, now_ms).expect("open store");
        store
            .insert_local_project("default", "projects/default.md", "Default", now_ms)
            .expect("insert project");

        let mut model = Model::default();
        for ticket in TICKETS {
            store
                .insert_local_ticket(
                    ticket,
                    "default",
                    &format!("tickets/{ticket}.md"),
                    &format!("Ticket {ticket}"),
                    &[],
                    &format!("sloop/{ticket}"),
                    Some("opencode"),
                    None,
                    None,
                    "default",
                    TicketState::Ready,
                    now_ms,
                )
                .expect("insert ticket");
            model.tickets.insert(ticket.into(), "ready");
            model.queued.insert(ticket.into(), Vec::new());
        }

        Self {
            _directory: directory,
            db_path,
            store,
            model,
            now_ms,
            run_counter: 0,
            activation_counter: 0,
        }
    }

    /// Picks an existing run for run-targeted ops, `None` when no run exists
    /// yet (the op is skipped: there is nothing valid-shaped to aim at).
    fn pick_run(&self, index: usize) -> Option<String> {
        if self.model.run_order.is_empty() {
            return None;
        }
        Some(self.model.run_order[index % self.model.run_order.len()].clone())
    }

    fn apply(&mut self, op: &Op) {
        match op {
            Op::AdvanceClock { ms } => self.now_ms += ms,
            Op::Enqueue { ticket } => self.enqueue(TICKETS[*ticket]),
            Op::Claim { ticket } => self.claim(TICKETS[*ticket]),
            Op::Start { run } => {
                if let Some(run) = self.pick_run(*run) {
                    self.start(&run);
                }
            }
            Op::RecordExit { run, exit_code } => {
                if let Some(run) = self.pick_run(*run) {
                    self.record_exit(&run, *exit_code);
                }
            }
            Op::Renew { run, lease_ms } => {
                if let Some(run) = self.pick_run(*run) {
                    self.renew(&run, *lease_ms, false);
                }
            }
            Op::Readopt { run, lease_ms } => {
                if let Some(run) = self.pick_run(*run) {
                    self.renew(&run, *lease_ms, true);
                }
            }
            Op::Abandon { run } => {
                if let Some(run) = self.pick_run(*run) {
                    self.abandon(&run);
                }
            }
            Op::Settle { run, outcome } => {
                if let Some(run) = self.pick_run(*run) {
                    self.settle(&run, *outcome);
                }
            }
        }
    }

    fn enqueue(&mut self, ticket: &str) {
        let id = format!("A{}", self.activation_counter);
        self.activation_counter += 1;
        self.store
            .insert_activation(
                &NewActivation {
                    id: &id,
                    kind: ActivationKind::Immediate,
                    ticket_id: Some(ticket),
                    project_id: None,
                    eligible_at_ms: None,
                    interval_ms: None,
                },
                self.now_ms,
            )
            .expect("insert activation");
        self.model
            .queued
            .get_mut(ticket)
            .expect("known ticket")
            .push(id);
    }

    fn claim(&mut self, ticket: &str) {
        let activation = self
            .model
            .queued
            .get(ticket)
            .expect("known ticket")
            .first()
            .cloned();
        let activation_id = activation.clone().unwrap_or_else(|| "A-none".into());
        let run_id = format!("R{}", self.run_counter);

        let request = ClaimRequest {
            ticket_id: ticket,
            run_id: &run_id,
            activation_id: &activation_id,
            owner_id: "daemon-prop",
            lease_ms: CLAIM_LEASE_MS,
            next_activation_eligible_at_ms: None,
            flow_json: "{}",
            ticket_json: "{}",
        };
        let claim = Coordination::new(&mut self.store)
            .claim(&request, self.now_ms)
            .expect("claim never fails structurally");

        let ticket_ready = self.model.tickets[ticket] == "ready";
        match (ticket_ready, activation) {
            (false, _) => assert_eq!(
                claim,
                Claim::Denied(ClaimDenial::NotReady),
                "{op:?}",
                op = ticket
            ),
            (true, None) => {
                assert_eq!(claim, Claim::Denied(ClaimDenial::ActivationNotQueued));
                // The denial happened mid-transaction; the ticket's move to
                // `claimed` must have rolled back. The post-op state
                // comparison verifies exactly that.
            }
            (true, Some(activation)) => {
                let Claim::Granted(granted) = claim else {
                    panic!("model expected a grant for {ticket}, got {claim:?}");
                };
                assert_eq!(granted.run_id, run_id);
                assert_eq!(granted.lease_expires_at_ms, self.now_ms + CLAIM_LEASE_MS);
                self.run_counter += 1;
                *self.model.tickets.get_mut(ticket).expect("known ticket") = "claimed";
                self.model
                    .queued
                    .get_mut(ticket)
                    .expect("known ticket")
                    .remove(0);
                self.model.leases.insert(
                    ticket.into(),
                    ModelLease {
                        run: run_id.clone(),
                        expires_at_ms: self.now_ms + CLAIM_LEASE_MS,
                    },
                );
                self.model.runs.insert(
                    run_id.clone(),
                    ModelRun {
                        ticket: ticket.into(),
                        activation,
                        state: "claimed",
                        exited: false,
                    },
                );
                self.model.run_order.push(run_id);
            }
        }
    }

    fn start(&mut self, run_id: &str) {
        let start = RunStart {
            run_id,
            branch: "sloop/branch",
            worktree_path: "/tmp/worktree",
            pid: 4_242,
            pid_start_time: Some(7),
            process_group_id: 4_242,
            worker_token: "token",
            worker_socket_path: "/tmp/worker.sock",
        };
        let result = Coordination::start(&self.store, &start, self.now_ms).expect("start");
        let run = self.model.runs.get_mut(run_id).expect("known run");
        if run.state == "claimed" {
            assert_eq!(result, Start::Granted);
            run.state = "running";
        } else {
            assert!(
                matches!(result, Start::Denied(_)),
                "model expected denial for {run_id} in {}, got {result:?}",
                run.state
            );
        }
    }

    fn record_exit(&mut self, run_id: &str, exit_code: i32) {
        let exit = RunExit {
            run_id,
            exit_code: Some(exit_code),
            capture_complete: true,
            commits_json: "{}",
            vendor_error: None,
            cooldown_until_ms: None,
        };
        let result = Coordination::new(&mut self.store)
            .record_exit(&exit, self.now_ms)
            .expect("record_exit");
        let run = self.model.runs.get_mut(run_id).expect("known run");
        if run.state == "running" {
            assert_eq!(result, Exit::Granted);
            run.state = "aftercare";
        } else {
            assert!(
                matches!(result, Exit::Denied(_)),
                "model expected denial for {run_id} in {}, got {result:?}",
                run.state
            );
        }
    }

    fn renew(&mut self, run_id: &str, lease_ms: i64, readopt: bool) {
        let ticket = self.model.runs[run_id].ticket.clone();
        let result = if readopt {
            Coordination::new(&mut self.store).readopt(&ticket, run_id, lease_ms, self.now_ms)
        } else {
            Coordination::new(&mut self.store).renew(&ticket, run_id, lease_ms, self.now_ms)
        }
        .expect("renewal");

        let lease = self.model.leases.get_mut(&ticket);
        let held = lease.as_ref().is_some_and(|lease| lease.run == run_id);
        // Ordinary renewal additionally requires the lease to be unexpired;
        // readopt re-arms a lapsed lease as long as the run has not settled
        // (a lease row's existence already implies that: settlement and
        // abandonment both delete the row).
        let granted = if readopt {
            held
        } else {
            held && lease
                .as_ref()
                .is_some_and(|lease| lease.expires_at_ms > self.now_ms)
        };
        if granted {
            assert_eq!(result, Renewal::Granted(self.now_ms + lease_ms));
            lease.expect("held lease").expires_at_ms = self.now_ms + lease_ms;
        } else {
            assert!(
                matches!(result, Renewal::Denied(_)),
                "model expected denial renewing {run_id}, got {result:?}"
            );
        }
    }

    fn abandon(&mut self, run_id: &str) {
        // Only a claim whose spawn failed is ever abandoned; mirror that.
        if self.model.runs[run_id].state != "claimed" {
            return;
        }
        let ticket = self.model.runs[run_id].ticket.clone();
        Coordination::new(&mut self.store)
            .abandon(run_id, &ticket, self.now_ms)
            .expect("abandon");

        self.model.leases.remove(&ticket);
        let run = self.model.runs.get_mut(run_id).expect("known run");
        run.state = "aborted";
        run.exited = true;
        if self.model.tickets[ticket.as_str()] == "claimed" {
            *self.model.tickets.get_mut(&ticket).expect("known ticket") = "ready";
        }
    }

    fn settle(&mut self, run_id: &str, outcome: Outcome) {
        let ticket = self.model.runs[run_id].ticket.clone();
        let settled = Coordination::new(&mut self.store)
            .settle(run_id, &ticket, Some(0), outcome, &[], None, self.now_ms)
            .expect("settle");

        if self.model.runs[run_id].exited {
            // Settling twice is an idempotent no-op, never an error.
            assert!(!settled, "settling an exited run must report false");
            return;
        }
        assert!(settled, "settling a live run must report true");

        let run = self.model.runs.get_mut(run_id).expect("known run");
        run.state = match outcome {
            Outcome::Merged => "merged",
            Outcome::Failed => "failed",
            Outcome::NeedsReview => "needs_review",
            Outcome::Cancelled => "cancelled",
            Outcome::RateLimited => "rate_limited",
            Outcome::Orphaned => "orphaned",
        };
        run.exited = true;
        let activation = run.activation.clone();
        if self
            .model
            .leases
            .get(&ticket)
            .is_some_and(|lease| lease.run == run_id)
        {
            self.model.leases.remove(&ticket);
        }
        if self.model.tickets[ticket.as_str()] == "claimed" {
            *self.model.tickets.get_mut(&ticket).expect("known ticket") =
                match TicketState::after_outcome(outcome) {
                    TicketState::Ready => "ready",
                    TicketState::Merged => "merged",
                    TicketState::Failed => "failed",
                    TicketState::NeedsReview => "needs_review",
                    state => panic!("unexpected post-outcome ticket state {state:?}"),
                };
        }
        if outcome == Outcome::RateLimited {
            // A rate-limited settlement re-queues the activation it consumed.
            self.model
                .queued
                .get_mut(&ticket)
                .expect("known ticket")
                .push(activation);
        }
    }

    /// Compares the entire database against the model over an independent
    /// read connection, so the check cannot lean on `Store`'s own accessors.
    fn assert_matches_model(&self) {
        let connection = Connection::open(&self.db_path).expect("open check connection");

        let mut db_leases = BTreeMap::new();
        let mut statement = connection
            .prepare("SELECT ticket_id, run_id, expires_at_ms FROM leases")
            .expect("prepare");
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    ModelLease {
                        run: row.get(1)?,
                        expires_at_ms: row.get(2)?,
                    },
                ))
            })
            .expect("query leases");
        for row in rows {
            let (ticket, lease) = row.expect("lease row");
            assert!(
                db_leases.insert(ticket.clone(), lease).is_none(),
                "database holds two leases for ticket {ticket}"
            );
        }
        assert_eq!(db_leases, self.model.leases, "lease table diverged");

        for (ticket, expected) in &self.model.tickets {
            let state: String = connection
                .query_row("SELECT state FROM tickets WHERE id = ?1", [ticket], |row| {
                    row.get(0)
                })
                .expect("ticket row");
            assert_eq!(&state, expected, "ticket {ticket} state diverged");
        }

        let mut statement = connection
            .prepare("SELECT id, state, exited_at_ms IS NOT NULL FROM runs")
            .expect("prepare");
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, bool>(2)?,
                ))
            })
            .expect("query runs");
        let mut seen = 0usize;
        for row in rows {
            let (run_id, state, exited) = row.expect("run row");
            let expected = self
                .model
                .runs
                .get(&run_id)
                .unwrap_or_else(|| panic!("database holds unknown run {run_id}"));
            assert_eq!(state, expected.state, "run {run_id} state diverged");
            assert_eq!(exited, expected.exited, "run {run_id} exit flag diverged");
            seen += 1;
        }
        assert_eq!(seen, self.model.runs.len(), "run count diverged");

        let mut queued: Vec<String> = Vec::new();
        let mut statement = connection
            .prepare("SELECT id FROM activations WHERE state = 'queued' ORDER BY id")
            .expect("prepare");
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query activations");
        for row in rows {
            queued.push(row.expect("activation row"));
        }
        let mut expected_queued: Vec<String> =
            self.model.queued.values().flatten().cloned().collect();
        expected_queued.sort();
        assert_eq!(queued, expected_queued, "queued activations diverged");

        extra_invariants(&connection);
    }
}

/// Cross-table structural invariants, independent of the reference model.
/// These hold for *every* reachable state, so they belong here rather than in
/// the model comparison: even if the model itself were wrong, these must
/// still pass. Each query counts violations, so a failure names the invariant
/// and the offending rows.
pub(crate) fn extra_invariants(connection: &Connection) {
    // 1. A lease is ownership of claimed work: every lease row must join a
    //    ticket that is actually in state `claimed`.
    let unclaimed_leased: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM leases l
             JOIN tickets t ON t.id = l.ticket_id
             WHERE t.state != 'claimed'",
            [],
            |row| row.get(0),
        )
        .expect("query leased-but-unclaimed tickets");
    assert_eq!(
        unclaimed_leased, 0,
        "a ticket holds a lease without being claimed"
    );

    // Companion direction: a lease row must never dangle without its ticket.
    let orphan_leases: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM leases l
             LEFT JOIN tickets t ON t.id = l.ticket_id
             WHERE t.id IS NULL",
            [],
            |row| row.get(0),
        )
        .expect("query orphan leases");
    assert_eq!(orphan_leases, 0, "a lease row references no ticket");

    // 2. Settlement and abandonment both delete the lease inside the same
    //    transaction that stamps `exited_at_ms`, so a lease may only ever
    //    join a run that has not exited.
    let leased_exited: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM leases l
             JOIN runs r ON r.id = l.run_id
             WHERE r.exited_at_ms IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .expect("query leases on exited runs");
    assert_eq!(leased_exited, 0, "an exited run still holds a lease");

    // 3. The anti-double-spawn guarantee itself: at no point may a ticket
    //    have two runs that are both still live.
    let double_spawned: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM (
                 SELECT ticket_id FROM runs
                 WHERE exited_at_ms IS NULL
                 GROUP BY ticket_id
                 HAVING COUNT(*) > 1
             )",
            [],
            |row| row.get(0),
        )
        .expect("query double-spawned tickets");
    assert_eq!(double_spawned, 0, "a ticket has two live runs");

    // 4. State and settlement must agree: `exited_at_ms` is stamped exactly
    //    when a run leaves the nonterminal ladder, so a terminal state with
    //    no exit time (or the reverse) is evidence of a transition that
    //    updated one half and lost the other.
    let torn_runs: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM runs
             WHERE (state IN ('claimed', 'running', 'aftercare'))
                != (exited_at_ms IS NULL)",
            [],
            |row| row.get(0),
        )
        .expect("query state/exit disagreements");
    assert_eq!(
        torn_runs, 0,
        "a run's state and exited_at_ms tell different stories"
    );
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        ..ProptestConfig::default()
    })]

    #[test]
    fn coordination_agrees_with_the_reference_model(
        ops in prop::collection::vec(op(), 0..40)
    ) {
        let mut harness = Harness::new();
        for op in &ops {
            harness.apply(op);
            harness.assert_matches_model();
        }

        // Reopening the database must reproduce the same state: settlement
        // and claims are durable, not artifacts of the live connection.
        drop(harness.store);
        harness.store = Store::open(&harness.db_path, harness.now_ms).expect("reopen");
        harness.assert_matches_model();
    }
}
