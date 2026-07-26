//! Authored ticket ingestion and daemon-owned work state.
//!
//! This module must never import the run-storage sibling; work-state
//! transitions and run evidence are separate storage boundaries.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;

use crate::domain::work::{
    Disposition, OwnerId, SourceVersion, TicketRef, WorkOutcome, WorkTicket,
};

use crate::frontmatter::Frontmatter;

pub(crate) mod exec;
pub mod local;
pub(crate) mod markdown;
mod reindex_evidence;
pub mod trigger;

use exec::ExecTicketSource;
use markdown::MarkdownTicketSource;

#[derive(Debug, Clone)]
pub(crate) struct AuthoredTicket {
    pub frontmatter: Frontmatter,
    pub body: String,
    pub source: String,
    pub source_ref: String,
    pub file_path: Option<PathBuf>,
    pub original_content: Option<String>,
    pub validation_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TicketFeedError(String);

impl TicketFeedError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for TicketFeedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for TicketFeedError {}

#[derive(Debug)]
pub(crate) struct ReindexError(pub(crate) String);

impl ReindexError {
    pub(crate) fn io(path: &Path, source: io::Error) -> Self {
        Self(format!("{}: {source}", path.display()))
    }
}

impl fmt::Display for ReindexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ReindexError {}

/// The configured authored-ticket feeder. The public extension seam is the
/// exec wire protocol, so server wiring uses this concrete enum rather than a
/// Rust implementation trait.
#[derive(Clone)]
pub(crate) enum TicketFeeder {
    Markdown(MarkdownTicketSource),
    Exec(ExecTicketSource),
}

impl TicketFeeder {
    pub(crate) fn markdown(root: impl Into<PathBuf>, ticket_dir: impl Into<PathBuf>) -> Self {
        Self::Markdown(MarkdownTicketSource::new(root, ticket_dir))
    }

    pub(crate) fn exec(root: impl Into<PathBuf>, argv: Vec<String>) -> Self {
        Self::Exec(ExecTicketSource::new(root, argv))
    }

    pub(crate) fn pull(&self) -> Result<Vec<AuthoredTicket>, TicketFeedError> {
        match self {
            Self::Markdown(source) => source.pull(),
            Self::Exec(source) => source.pull(),
        }
    }

    pub(crate) fn supports_authoring(&self) -> bool {
        matches!(self, Self::Markdown(_))
    }

    pub(crate) fn exec_reporter(&self) -> Option<ExecTicketSource> {
        match self {
            Self::Markdown(_) => None,
            Self::Exec(source) => Some(source.clone()),
        }
    }
}

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
pub struct ActiveClaim {
    pub ticket: TicketRef,
    pub owner: OwnerId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceError {
    Unavailable { retry_after: Option<Duration> },
    Rejected { message: String },
    Corrupt { message: String },
}

impl fmt::Display for SourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable { retry_after } => match retry_after {
                Some(retry_after) => write!(
                    formatter,
                    "work source is unavailable; retry after {} seconds",
                    retry_after.as_secs()
                ),
                None => formatter.write_str("work source is unavailable"),
            },
            Self::Rejected { message } | Self::Corrupt { message } => formatter.write_str(message),
        }
    }
}

impl std::error::Error for SourceError {}

/// The daemon-facing contract for reading and claiming work.
///
/// Implementations must uphold these invariants:
///
/// - Attempts increment only inside [`WorkState::claim`], never on release.
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

    /// Claims visible at the source. Recovery compares these with recorded
    /// runs and releases a claim whose second admission commit never landed.
    async fn active_claims(&self) -> Result<Vec<ActiveClaim>, SourceError>;

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

    /// Give the ticket back, per policy. Repeating a completed release is an
    /// idempotent success; it never consumes another attempt.
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

        async fn active_claims(&self) -> Result<Vec<ActiveClaim>, SourceError> {
            let ticket = self.ticket.lock().unwrap();
            let WorkTicketState::Claimed { by } = &ticket.state else {
                return Ok(Vec::new());
            };
            Ok(vec![ActiveClaim {
                ticket: ticket_ref(&ticket),
                owner: by.clone(),
            }])
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
                Disposition::Complete => WorkTicketState::Done,
                Disposition::Retry { .. } => WorkTicketState::Ready,
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
                worktree: None,
                trigger_id: None,
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
