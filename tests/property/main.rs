//! Property tests: mechanical exploration of state spaces and input spaces.
//!
//! `model` drives the real `Store` + `Coordination` over a tempfile SQLite
//! database with random operation sequences, checked against an in-memory
//! reference model. `flow_walk` and `roundtrip` cover the pure seams.
//!
//! Failing inputs are shrunk and persisted under
//! `tests/property/proptest-regressions/`; commit those files — each one is a
//! minimal regression test.

mod flow_gen;
mod flow_walk;
mod model;
mod races;
mod roundtrip;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use sloop::clock::Clock;
use sloop::domain::work::{Disposition, OwnerId, TicketRef};
use sloop::outcome::Outcome;
use sloop::run_store::{CooldownUpdate, RunStore};
use sloop::store::{ClaimRequest, ClaimedRun, Store};
use sloop::work_state::local::LocalSqlite;
use sloop::work_state::{ClaimResult, WorkState};

struct FixedClock(i64);

impl Clock for FixedClock {
    fn now_ms(&self) -> i64 {
        self.0
    }

    fn local_minute(&self, _timestamp_ms: i64) -> u16 {
        0
    }

    fn sleep_until(&self, _deadline_ms: i64) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(std::future::pending())
    }
}

fn claim(store: &Store, request: &ClaimRequest<'_>, now_ms: i64) -> Option<ClaimedRun> {
    let work_state = LocalSqlite::from_db_with_clock(store.db(), Arc::new(FixedClock(now_ms)));
    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(work_state.claim(
            &TicketRef {
                id: request.ticket_id.into(),
                source: "local".into(),
                source_ref: None,
            },
            &OwnerId(request.run_id.into()),
            Duration::from_millis(request.lease_ms as u64),
        ))
        .unwrap();
    match result {
        ClaimResult::Claimed { ticket } => {
            let activation_id = ticket
                .hints
                .activation_id
                .as_deref()
                .expect("local claims identify their activation");
            Some(
                store
                    .insert_claimed_run(
                        &ClaimRequest {
                            activation_id,
                            ..request.clone()
                        },
                        now_ms,
                    )
                    .unwrap(),
            )
        }
        ClaimResult::Lost { .. } => None,
    }
}

fn settle(store: &Store, run_id: &str, outcome: Outcome, now_ms: i64) -> bool {
    let cooldown = (outcome == Outcome::RateLimited).then_some(CooldownUpdate {
        target: "opencode",
        until_ms: now_ms + 60_000,
        reason: "property test",
    });
    let (recorded, applied) = RunStore::from_db(store.db())
        .settle(run_id, Some(0), outcome, &[], cooldown.as_ref(), now_ms)
        .expect("record settlement");
    let disposition = match recorded.work.verdict {
        Outcome::Merged => Disposition::Complete,
        Outcome::Failed => Disposition::Abandon,
        Outcome::NeedsReview => Disposition::Park {
            reason: "needs-review".into(),
        },
        Outcome::Cancelled | Outcome::Orphaned => Disposition::Retry {
            not_before_ms: None,
        },
        Outcome::RateLimited => Disposition::Retry {
            not_before_ms: recorded.not_before_ms,
        },
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(
            LocalSqlite::from_db_with_clock(store.db(), Arc::new(FixedClock(now_ms))).release(
                &TicketRef {
                    id: recorded.work.ticket_id.clone(),
                    source: "local".into(),
                    source_ref: None,
                },
                &recorded.work.owner,
                disposition,
            ),
        )
        .expect("release settled work");
    applied
}

fn abort(store: &Store, run_id: &str, ticket_id: &str, now_ms: i64) {
    RunStore::from_db(store.db())
        .abort(run_id, ticket_id, now_ms)
        .expect("record abort");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(
            LocalSqlite::from_db_with_clock(store.db(), Arc::new(FixedClock(now_ms))).release(
                &TicketRef {
                    id: ticket_id.into(),
                    source: "local".into(),
                    source_ref: None,
                },
                &OwnerId(run_id.into()),
                Disposition::Retry {
                    not_before_ms: Some(now_ms),
                },
            ),
        )
        .expect("release aborted work");
}
