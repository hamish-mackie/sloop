//! True-simultaneity races against the store's durable guards.
//!
//! In production the database really is written from several connections at
//! once: the dispatcher holds one, every run supervisor opens its own to
//! checkpoint exits (`scheduler.rs`), and crash recovery opens another. The
//! design documents say the *database engine* is the durable backstop — the
//! `leases` primary key and the conditional `UPDATE ... WHERE state = ...`
//! guards — with the supervisor-vs-recovery race decided by whoever
//! checkpoints first. These tests hold those guards under real thread-level
//! simultaneity: N threads, each with its own `Store` connection, releasing
//! from a barrier at the same instant.
//!
//! Time stays injected even here: a shared atomic counter hands every
//! operation a distinct logical timestamp.

use std::sync::Barrier;
use std::sync::atomic::{AtomicI64, Ordering};

use rusqlite::Connection;
use sloop::coordination::{Claim, Coordination, Exit, RunExit, RunStart, Start};
use sloop::domain::ticket::TicketState;
use sloop::outcome::Outcome;
use sloop::store::{ActivationKind, ClaimRequest, NewActivation, Store};
use tempfile::TempDir;

use crate::model::extra_invariants;

const THREADS: usize = 8;

struct Arena {
    _directory: TempDir,
    db_path: std::path::PathBuf,
    clock: AtomicI64,
}

impl Arena {
    fn new() -> Self {
        let directory = TempDir::new().expect("create tempdir");
        let db_path = directory.path().join("sloop.db");
        let store = Store::open(&db_path, 1_000).expect("open store");
        store
            .insert_local_project("default", "projects/default.md", "Default", 1_000)
            .expect("insert project");
        Self {
            _directory: directory,
            db_path,
            clock: AtomicI64::new(2_000),
        }
    }

    fn now(&self) -> i64 {
        self.clock.fetch_add(1, Ordering::Relaxed)
    }

    fn open(&self) -> Store {
        Store::open(&self.db_path, self.now()).expect("open per-thread store")
    }

    fn add_ticket(&self, store: &Store, ticket: &str) {
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
                self.now(),
            )
            .expect("insert ticket");
    }

    fn add_activation(&self, store: &Store, id: &str, ticket: &str) {
        store
            .insert_activation(
                &NewActivation {
                    id,
                    kind: ActivationKind::Immediate,
                    ticket_id: Some(ticket),
                    project_id: None,
                    eligible_at_ms: None,
                    interval_ms: None,
                },
                self.now(),
            )
            .expect("insert activation");
    }

    fn check_invariants(&self) {
        let connection = Connection::open(&self.db_path).expect("open check connection");
        extra_invariants(&connection);
    }
}

fn claim_request<'a>(ticket: &'a str, run_id: &'a str, activation_id: &'a str) -> ClaimRequest<'a> {
    ClaimRequest {
        ticket_id: ticket,
        run_id,
        activation_id,
        owner_id: "daemon-race",
        lease_ms: 60_000,
        next_activation_eligible_at_ms: None,
        flow_json: "{}",
        ticket_json: "{}",
    }
}

fn run_start(run_id: &str) -> RunStart<'_> {
    RunStart {
        run_id,
        branch: "sloop/branch",
        worktree_path: "/tmp/worktree",
        pid: 4_242,
        pid_start_time: Some(7),
        process_group_id: 4_242,
        worker_token: "token",
        worker_socket_path: "/tmp/worker.sock",
    }
}

fn run_exit(run_id: &str) -> RunExit<'_> {
    RunExit {
        run_id,
        exit_code: Some(0),
        capture_complete: true,
        commits_json: "{}",
        vendor_error: None,
        cooldown_until_ms: None,
    }
}

/// Eight connections claim the same ready ticket at the same instant.
/// Exactly one may win; the conditional `UPDATE ... WHERE state='ready'` is
/// the only thing deciding it.
#[test]
fn simultaneous_claims_grant_exactly_one_winner() {
    let arena = Arena::new();
    let setup = arena.open();

    for round in 0..20 {
        let ticket = format!("T{round}");
        let activation = format!("A{round}");
        arena.add_ticket(&setup, &ticket);
        arena.add_activation(&setup, &activation, &ticket);

        let barrier = Barrier::new(THREADS);
        let grants: Vec<bool> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..THREADS)
                .map(|thread| {
                    let (arena, ticket, activation, barrier) =
                        (&arena, &ticket, &activation, &barrier);
                    scope.spawn(move || {
                        let mut store = arena.open();
                        let run_id = format!("{ticket}-R{thread}");
                        barrier.wait();
                        let claim = Coordination::new(&mut store)
                            .claim(&claim_request(ticket, &run_id, activation), arena.now())
                            .expect("claim must grant or deny, never fail");
                        matches!(claim, Claim::Granted(_))
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("join"))
                .collect()
        });

        let winners = grants.iter().filter(|granted| **granted).count();
        assert_eq!(winners, 1, "round {round}: ticket claimed {winners} times");
        arena.check_invariants();
    }
}

/// Eight connections race to checkpoint the same run's exit — the deliberate
/// supervisor-vs-recovery race, widened. Exactly one owns aftercare.
#[test]
fn simultaneous_exit_checkpoints_grant_exactly_one_owner() {
    let arena = Arena::new();
    let mut setup = arena.open();

    for round in 0..20 {
        let ticket = format!("T{round}");
        let activation = format!("A{round}");
        let run_id = format!("{ticket}-R0");
        arena.add_ticket(&setup, &ticket);
        arena.add_activation(&setup, &activation, &ticket);
        let claim = Coordination::new(&mut setup)
            .claim(&claim_request(&ticket, &run_id, &activation), arena.now())
            .expect("claim");
        assert!(matches!(claim, Claim::Granted(_)));
        let started = Coordination::start(&setup, &run_start(&run_id), arena.now()).expect("start");
        assert_eq!(started, Start::Granted);

        let barrier = Barrier::new(THREADS);
        let grants: Vec<bool> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..THREADS)
                .map(|_| {
                    let (arena, run_id, barrier) = (&arena, &run_id, &barrier);
                    scope.spawn(move || {
                        let mut store = arena.open();
                        barrier.wait();
                        let exit = Coordination::new(&mut store)
                            .record_exit(&run_exit(run_id), arena.now())
                            .expect("record_exit must grant or deny, never fail");
                        matches!(exit, Exit::Granted)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("join"))
                .collect()
        });

        let owners = grants.iter().filter(|granted| **granted).count();
        assert_eq!(owners, 1, "round {round}: {owners} threads own aftercare");
        arena.check_invariants();
    }
}

/// Eight connections race to settle the same run with *different* outcomes.
/// Exactly one settlement lands, and the database tells the winner's story —
/// never a blend of two.
#[test]
fn simultaneous_settlements_land_exactly_once() {
    const OUTCOMES: [Outcome; 4] = [
        Outcome::Merged,
        Outcome::Failed,
        Outcome::Cancelled,
        Outcome::RateLimited,
    ];

    let arena = Arena::new();
    let mut setup = arena.open();

    for round in 0..20 {
        let ticket = format!("T{round}");
        let activation = format!("A{round}");
        let run_id = format!("{ticket}-R0");
        arena.add_ticket(&setup, &ticket);
        arena.add_activation(&setup, &activation, &ticket);
        Coordination::new(&mut setup)
            .claim(&claim_request(&ticket, &run_id, &activation), arena.now())
            .expect("claim");
        Coordination::start(&setup, &run_start(&run_id), arena.now()).expect("start");
        Coordination::new(&mut setup)
            .record_exit(&run_exit(&run_id), arena.now())
            .expect("record exit");

        let barrier = Barrier::new(THREADS);
        let landed: Vec<Option<Outcome>> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..THREADS)
                .map(|thread| {
                    let (arena, ticket, run_id, barrier) = (&arena, &ticket, &run_id, &barrier);
                    scope.spawn(move || {
                        let mut store = arena.open();
                        let outcome = OUTCOMES[thread % OUTCOMES.len()];
                        barrier.wait();
                        let settled = Coordination::new(&mut store)
                            .settle(run_id, ticket, Some(0), outcome, &[], None, arena.now())
                            .expect("settle must land or no-op, never fail");
                        settled.then_some(outcome)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("join"))
                .collect()
        });

        let winners: Vec<Outcome> = landed.into_iter().flatten().collect();
        assert_eq!(winners.len(), 1, "round {round}: {winners:?} all landed");
        let winner = winners[0];

        let connection = Connection::open(&arena.db_path).expect("open check connection");
        let run_state: String = connection
            .query_row("SELECT state FROM runs WHERE id = ?1", [&run_id], |row| {
                row.get(0)
            })
            .expect("run row");
        assert_eq!(run_state, winner.as_str(), "run state is the winner's");
        let ticket_state: String = connection
            .query_row(
                "SELECT state FROM tickets WHERE id = ?1",
                [&ticket],
                |row| row.get(0),
            )
            .expect("ticket row");
        assert_eq!(
            ticket_state,
            TicketState::after_outcome(winner).as_str(),
            "ticket state is the winner's"
        );
        arena.check_invariants();
    }
}

/// Eight connections hammer a small shared ticket pool through whole run
/// lifecycles at once, with no coordination between threads beyond the
/// database itself. Whatever interleaving happens, every operation must
/// resolve to a grant or a denial — never a structural error — and the
/// cross-table invariants must hold at the end.
#[test]
fn uncoordinated_lifecycle_hammer_preserves_invariants() {
    const POOL: [&str; 4] = ["P0", "P1", "P2", "P3"];
    const ITERATIONS: usize = 60;

    let arena = Arena::new();
    let setup = arena.open();
    for ticket in POOL {
        arena.add_ticket(&setup, ticket);
    }

    let barrier = Barrier::new(THREADS);
    let completed: Vec<usize> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..THREADS)
            .map(|thread| {
                let (arena, barrier) = (&arena, &barrier);
                scope.spawn(move || {
                    let mut store = arena.open();
                    let mut completed = 0;
                    barrier.wait();
                    for iteration in 0..ITERATIONS {
                        let ticket = POOL[(thread + iteration) % POOL.len()];
                        let activation = format!("{ticket}-{thread}-{iteration}");
                        let run_id = format!("{ticket}-{thread}-{iteration}-run");
                        arena.add_activation(&store, &activation, ticket);

                        let claim = Coordination::new(&mut store)
                            .claim(&claim_request(ticket, &run_id, &activation), arena.now())
                            .expect("claim must grant or deny");
                        if !matches!(claim, Claim::Granted(_)) {
                            continue;
                        }
                        // Walk the whole lifecycle; every step must be
                        // granted, because this thread owns the run.
                        assert_eq!(
                            Coordination::start(&store, &run_start(&run_id), arena.now())
                                .expect("start"),
                            Start::Granted
                        );
                        assert_eq!(
                            Coordination::new(&mut store)
                                .record_exit(&run_exit(&run_id), arena.now())
                                .expect("record exit"),
                            Exit::Granted
                        );
                        let outcome = if iteration % 2 == 0 {
                            Outcome::Cancelled
                        } else {
                            Outcome::Merged
                        };
                        assert!(
                            Coordination::new(&mut store)
                                .settle(&run_id, ticket, Some(0), outcome, &[], None, arena.now())
                                .expect("settle"),
                            "the owner's settlement must land"
                        );
                        completed += 1;
                    }
                    completed
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("join"))
            .collect()
    });

    let total: usize = completed.iter().sum();
    assert!(total > 0, "contention must not starve every thread");
    arena.check_invariants();

    // Every granted claim left a settled run behind; nothing leaked.
    let connection = Connection::open(&arena.db_path).expect("open check connection");
    let live_runs: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM runs WHERE exited_at_ms IS NULL",
            [],
            |row| row.get(0),
        )
        .expect("count live runs");
    assert_eq!(live_runs, 0, "the hammer settles every run it starts");
    let leases: i64 = connection
        .query_row("SELECT COUNT(*) FROM leases", [], |row| row.get(0))
        .expect("count leases");
    assert_eq!(leases, 0, "no lease survives its settled run");
}
