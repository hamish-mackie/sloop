//! Outcome derivation. Evidence in, verdict out: this module is the only
//! place a run's terminal outcome is decided, and it is pure so policy can
//! be tested without a daemon, a process, or a repository.

use serde::Serialize;

use crate::vendor_error::VendorErrorClass;

/// Classification of how the agent process ended. This is the adapter-level
/// reading of the raw wait status; vendor-specific classifications such as
/// rate limits join in a later phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitClass {
    /// Exit status zero. Not proof of successful work on its own.
    Success,
    /// A nonzero exit status.
    Failure(i32),
    /// No exit code: the process was ended by a signal, including `cancel`.
    KilledBySignal,
}

pub fn classify_exit(exit_code: Option<i32>) -> ExitClass {
    match exit_code {
        Some(0) => ExitClass::Success,
        Some(code) => ExitClass::Failure(code),
        None => ExitClass::KilledBySignal,
    }
}

/// Result of an attempted merge of the run branch into the default branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeOutcome {
    Merged,
    /// The merge conflicted (or otherwise failed); a human must reconcile.
    /// A default branch that merely moved is handled with a merge commit.
    Diverged,
}

/// Everything observed about one finished run. Fields are facts gathered by
/// the supervisor; none of them are claims made by the agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunEvidence {
    /// An operator recorded cancellation intent before the exit was handled.
    pub cancelled: bool,
    pub exit: ExitClass,
    /// A rejection recognized from the adapter's captured output.
    pub vendor_error: Option<VendorErrorClass>,
    /// Commits are activity metadata except when aftercare fails: committed
    /// work is then preserved for review, while a known unchanged branch
    /// failed. `None` means commit enumeration was incomplete.
    pub commit_count: Option<usize>,
    /// Whether a flow stage after build failed.
    pub aftercare_failed: bool,
    /// `None` when a merge was never attempted.
    pub merge: Option<MergeOutcome>,
}

/// Terminal classification of a run. `Cancelled` and `Orphaned` release the
/// ticket back to `ready`; the other outcomes are also recorded on the ticket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Merged,
    Failed,
    NeedsReview,
    Cancelled,
    /// A retryable vendor rejection released the ticket under a cooldown.
    RateLimited,
    /// Recovery found no live process before the agent exit was checkpointed.
    /// The worktree stays available for inspection and the ticket is released.
    Orphaned,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Merged => "merged",
            Self::Failed => "failed",
            Self::NeedsReview => "needs_review",
            Self::Cancelled => "cancelled",
            Self::RateLimited => "rate_limited",
            Self::Orphaned => "orphaned",
        }
    }
}

/// Maps complete evidence to the run's terminal outcome.
///
/// Constraints fixed by the design documents:
/// - Cancellation always wins: the outcome is `Cancelled` regardless of
///   other evidence, so racing exit and cancel events stay idempotent.
/// - A run may only be `Merged` when the merge itself succeeded
///   (`Some(MergeOutcome::Merged)`).
///
/// Policy decisions taken here:
/// - A successful exit whose aftercare failed is `NeedsReview` when its run
///   branch has commits, otherwise `Failed`.
/// - A nonzero or killed exit is `Failed`; Git history does not upgrade or
///   downgrade the verdict.
/// - A merge attempt that conflicted is `NeedsReview`: the work passed its
///   preceding stages, only integration needs a human.
pub fn derive_outcome(evidence: &RunEvidence) -> Outcome {
    if evidence.cancelled {
        return Outcome::Cancelled;
    }
    if let Some(class) = evidence.vendor_error {
        return if class.requires_cooldown() {
            Outcome::RateLimited
        } else {
            Outcome::Failed
        };
    }
    if evidence.merge == Some(MergeOutcome::Merged) {
        return Outcome::Merged;
    }
    if evidence.exit != ExitClass::Success {
        return Outcome::Failed;
    }
    if evidence.aftercare_failed && evidence.commit_count == Some(0) {
        return Outcome::Failed;
    }
    Outcome::NeedsReview
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evidence() -> RunEvidence {
        RunEvidence {
            cancelled: false,
            exit: ExitClass::Success,
            vendor_error: None,
            commit_count: Some(0),
            aftercare_failed: false,
            merge: None,
        }
    }

    #[test]
    fn a_successful_exit_without_a_merge_needs_review() {
        let outcome = derive_outcome(&evidence());
        assert_eq!(outcome, Outcome::NeedsReview);
    }

    #[test]
    fn a_successful_merge_is_the_only_path_to_merged() {
        let merged = derive_outcome(&RunEvidence {
            commit_count: Some(1),
            merge: Some(MergeOutcome::Merged),
            ..evidence()
        });
        assert_eq!(merged, Outcome::Merged);

        let diverged = derive_outcome(&RunEvidence {
            commit_count: Some(1),
            merge: Some(MergeOutcome::Diverged),
            ..evidence()
        });
        assert_ne!(diverged, Outcome::Merged);
    }

    #[test]
    fn failed_aftercare_with_commits_needs_review() {
        let outcome = derive_outcome(&RunEvidence {
            commit_count: Some(1),
            aftercare_failed: true,
            ..evidence()
        });
        assert_eq!(outcome, Outcome::NeedsReview);
    }

    #[test]
    fn failed_aftercare_without_commits_fails() {
        let outcome = derive_outcome(&RunEvidence {
            aftercare_failed: true,
            ..evidence()
        });
        assert_eq!(outcome, Outcome::Failed);
    }

    #[test]
    fn failed_aftercare_with_unknown_commits_needs_review() {
        let outcome = derive_outcome(&RunEvidence {
            commit_count: None,
            aftercare_failed: true,
            ..evidence()
        });
        assert_eq!(outcome, Outcome::NeedsReview);
    }

    #[test]
    fn a_crashed_agent_fails_regardless_of_git_history() {
        let outcome = derive_outcome(&RunEvidence {
            exit: ExitClass::Failure(1),
            ..evidence()
        });
        assert_eq!(outcome, Outcome::Failed);
    }

    #[test]
    fn cancellation_wins_over_every_other_reading() {
        let outcome = derive_outcome(&RunEvidence {
            cancelled: true,
            exit: ExitClass::KilledBySignal,
            vendor_error: None,
            commit_count: Some(5),
            aftercare_failed: false,
            merge: Some(MergeOutcome::Merged),
        });
        assert_eq!(outcome, Outcome::Cancelled);
    }

    #[test]
    fn exit_classification_reads_the_wait_status() {
        assert_eq!(classify_exit(Some(0)), ExitClass::Success);
        assert_eq!(classify_exit(Some(2)), ExitClass::Failure(2));
        assert_eq!(classify_exit(None), ExitClass::KilledBySignal);
    }
    #[test]
    fn vendor_rejections_follow_code_owned_policy() {
        for class in [
            VendorErrorClass::AuthenticationRequired,
            VendorErrorClass::InvalidConfiguration,
        ] {
            assert_eq!(
                derive_outcome(&RunEvidence {
                    vendor_error: Some(class),
                    ..evidence()
                }),
                Outcome::Failed
            );
        }
        for class in [
            VendorErrorClass::RateLimited,
            VendorErrorClass::UnknownRejection,
        ] {
            assert_eq!(
                derive_outcome(&RunEvidence {
                    vendor_error: Some(class),
                    ..evidence()
                }),
                Outcome::RateLimited
            );
        }
    }
}
