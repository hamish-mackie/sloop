use std::time::Duration;

use async_trait::async_trait;

use crate::domain::work::{
    Disposition, OwnerId, SourceVersion, TicketRef, WorkOutcome, WorkTicket,
};

pub mod local;
pub mod markdown;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimStrength {
    Atomic,
    Optimistic,
    LocalOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum ClaimResult {
    Claimed { ticket: WorkTicket },
    Lost { held_by: Option<OwnerId> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceError {
    Unavailable { retry_after: Option<Duration> },
    Rejected { message: String },
    Corrupt { message: String },
}

/// The daemon-facing contract for reading and claiming work.
///
/// Implementations must uphold these invariants:
///
/// - Attempts increment only inside [`WorkState::claim`] or
///   [`WorkState::release`] with [`Disposition::Retry`], never elsewhere.
/// - A claim is committed at the source before any run is recorded against it.
/// - An outcome is durably recorded before [`WorkState::release`] or
///   [`WorkState::push_outcome`] is called.
#[async_trait]
pub trait WorkState: Send + Sync {
    /// What this source can promise about claims. The dispatcher never
    /// branches on this; it exists for status display and for refusing
    /// multi-writer setups on `LocalOnly` sources.
    fn claim_strength(&self) -> ClaimStrength;

    /// Refresh the daemon's read cache. Selection runs on the cache;
    /// pull is periodic, never per-tick.
    async fn pull_ready(&self) -> Result<Vec<WorkTicket>, SourceError>;

    /// The only authoritative check. Succeeds iff the ticket is still
    /// ready at the source. Lost is a normal, silent outcome.
    async fn claim(
        &self,
        ticket: &TicketRef,
        owner: &OwnerId,
        ttl: Duration,
    ) -> Result<ClaimResult, SourceError>;

    /// Heartbeat for long runs. An expired claim is reclaimable by peers.
    async fn renew(&self, ticket: &TicketRef, owner: &OwnerId) -> Result<ClaimResult, SourceError>;

    /// Give the ticket back, per policy. Increments attempts on `Retry`.
    async fn release(
        &self,
        ticket: &TicketRef,
        owner: &OwnerId,
        disposition: Disposition,
    ) -> Result<(), SourceError>;

    /// Terminal push. Fire-and-forget from the dispatcher's view.
    async fn push_outcome(&self, outcome: &WorkOutcome) -> Result<(), SourceError>;
}

#[async_trait]
pub trait WorkStateAuthor: Send + Sync {
    /// Create. The source assigns the ref; id authority lives behind
    /// the seam.
    async fn post(&self, ticket: &WorkTicket) -> Result<TicketRef, SourceError>;

    /// Repost-in-place, compare-and-swap on the version. A conflicting
    /// concurrent edit is a detected conflict, never a silent clobber.
    async fn update(
        &self,
        ticket: &TicketRef,
        content: &WorkTicket,
        expected: &SourceVersion,
    ) -> Result<SourceVersion, SourceError>;
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use crate::domain::work::{ExecutionHints, WorkTicketState};

    use super::*;

    struct FakeWorkState {
        ticket: Mutex<WorkTicket>,
        outcomes: Mutex<Vec<WorkOutcome>>,
    }

    #[async_trait]
    impl WorkState for FakeWorkState {
        fn claim_strength(&self) -> ClaimStrength {
            ClaimStrength::Atomic
        }

        async fn pull_ready(&self) -> Result<Vec<WorkTicket>, SourceError> {
            let ticket = self.ticket.lock().unwrap().clone();
            Ok((ticket.state == WorkTicketState::Ready)
                .then_some(ticket)
                .into_iter()
                .collect())
        }

        async fn claim(
            &self,
            ticket: &TicketRef,
            owner: &OwnerId,
            _ttl: Duration,
        ) -> Result<ClaimResult, SourceError> {
            let mut stored = self.ticket.lock().unwrap();
            if stored.id != ticket.id || stored.state != WorkTicketState::Ready {
                return Ok(ClaimResult::Lost { held_by: None });
            }

            stored.attempts += 1;
            stored.state = WorkTicketState::Claimed { by: owner.clone() };
            Ok(ClaimResult::Claimed {
                ticket: stored.clone(),
            })
        }

        async fn renew(
            &self,
            ticket: &TicketRef,
            owner: &OwnerId,
        ) -> Result<ClaimResult, SourceError> {
            let stored = self.ticket.lock().unwrap();
            if stored.id == ticket.id
                && stored.state == (WorkTicketState::Claimed { by: owner.clone() })
            {
                return Ok(ClaimResult::Claimed {
                    ticket: stored.clone(),
                });
            }

            Ok(ClaimResult::Lost { held_by: None })
        }

        async fn release(
            &self,
            _ticket: &TicketRef,
            _owner: &OwnerId,
            disposition: Disposition,
        ) -> Result<(), SourceError> {
            let mut stored = self.ticket.lock().unwrap();
            stored.state = match disposition {
                Disposition::Retry { .. } => {
                    stored.attempts += 1;
                    WorkTicketState::Ready
                }
                Disposition::Park { reason } => WorkTicketState::Held { reason },
                Disposition::Abandon => WorkTicketState::Failed,
            };
            Ok(())
        }

        async fn push_outcome(&self, outcome: &WorkOutcome) -> Result<(), SourceError> {
            self.outcomes.lock().unwrap().push(outcome.clone());
            Ok(())
        }
    }

    #[async_trait]
    impl WorkStateAuthor for FakeWorkState {
        async fn post(&self, ticket: &WorkTicket) -> Result<TicketRef, SourceError> {
            *self.ticket.lock().unwrap() = ticket.clone();
            Ok(ticket_ref(ticket))
        }

        async fn update(
            &self,
            ticket: &TicketRef,
            content: &WorkTicket,
            expected: &SourceVersion,
        ) -> Result<SourceVersion, SourceError> {
            let mut stored = self.ticket.lock().unwrap();
            if stored.id != ticket.id || &stored.version != expected {
                return Err(SourceError::Rejected {
                    message: "version conflict".into(),
                });
            }

            let version = SourceVersion(format!("{}-next", expected.0));
            *stored = content.clone();
            stored.version = version.clone();
            Ok(version)
        }
    }

    fn work_ticket() -> WorkTicket {
        WorkTicket {
            id: "T1".into(),
            project_id: "P1".into(),
            name: "test ticket".into(),
            body: String::new(),
            state: WorkTicketState::Ready,
            blocked_by: Vec::new(),
            attempts: 0,
            hints: ExecutionHints {
                target: None,
                model: None,
                effort: None,
                flow: None,
            },
            version: SourceVersion("1".into()),
        }
    }

    fn ticket_ref(ticket: &WorkTicket) -> TicketRef {
        TicketRef {
            id: ticket.id.clone(),
            source: "memory".into(),
            source_ref: None,
        }
    }

    #[test]
    fn work_state_is_object_safe() {
        let _: Option<Box<dyn WorkState>> = None;
    }

    #[tokio::test]
    async fn fake_implements_work_state_traits() {
        let ticket = work_ticket();
        let ticket_ref = ticket_ref(&ticket);
        let fake = FakeWorkState {
            ticket: Mutex::new(ticket.clone()),
            outcomes: Mutex::new(Vec::new()),
        };
        let author: &dyn WorkStateAuthor = &fake;
        let version = author
            .update(&ticket_ref, &ticket, &ticket.version)
            .await
            .unwrap();
        assert_eq!(version, SourceVersion("1-next".into()));

        let source: Box<dyn WorkState> = Box::new(fake);

        assert_eq!(source.claim_strength(), ClaimStrength::Atomic);
        let result = source
            .claim(
                &ticket_ref,
                &OwnerId("daemon-1".into()),
                Duration::from_secs(60),
            )
            .await
            .unwrap();

        assert!(matches!(
            result,
            ClaimResult::Claimed {
                ticket: WorkTicket { attempts: 1, .. }
            }
        ));
    }
}
