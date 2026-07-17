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

/// Result of one executed aftercare stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageOutcome {
    Passed,
    Failed,
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
    /// `None` when no test stage ran: either the exit did not justify testing,
    /// or no test command is configured.
    pub tests: Option<StageOutcome>,
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

/// Whether the process exit justifies running the test stage. A recognized
/// vendor rejection means the "successful" exit is a refusal transcript, so
/// it never qualifies for testing.
pub fn wants_tests(exit: ExitClass, vendor_error: bool) -> bool {
    !vendor_error && exit == ExitClass::Success
}

/// Whether the evidence so far justifies attempting a merge. A test stage
/// that ran and failed blocks the merge; an unconfigured test stage does
/// not, because the operator chose auto-merge policy without one.
pub fn wants_merge(exit: ExitClass, tests: Option<StageOutcome>, vendor_error: bool) -> bool {
    wants_tests(exit, vendor_error) && tests != Some(StageOutcome::Failed)
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
/// - A successful exit whose tests or merge failed is `NeedsReview`; its run
///   branch is retained for inspection.
/// - A nonzero or killed exit is `Failed`; Git history does not upgrade or
///   downgrade the verdict.
/// - A merge attempt that conflicted is `NeedsReview`: the work passed
///   tests, only integration needs a human.
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
            tests: None,
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
            tests: Some(StageOutcome::Passed),
            merge: Some(MergeOutcome::Merged),
            ..evidence()
        });
        assert_eq!(merged, Outcome::Merged);

        let diverged = derive_outcome(&RunEvidence {
            tests: Some(StageOutcome::Passed),
            merge: Some(MergeOutcome::Diverged),
            ..evidence()
        });
        assert_ne!(diverged, Outcome::Merged);
    }

    #[test]
    fn failed_tests_never_reach_merged() {
        let outcome = derive_outcome(&RunEvidence {
            tests: Some(StageOutcome::Failed),
            ..evidence()
        });
        assert_ne!(outcome, Outcome::Merged);
        assert_ne!(outcome, Outcome::Cancelled);
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
            tests: Some(StageOutcome::Passed),
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
    fn test_and_merge_gates_follow_the_evidence() {
        assert!(wants_tests(ExitClass::Success, false));
        assert!(!wants_tests(ExitClass::Failure(1), false));
        assert!(!wants_tests(ExitClass::KilledBySignal, false));
        assert!(!wants_tests(ExitClass::Success, true));

        assert!(wants_merge(
            ExitClass::Success,
            Some(StageOutcome::Passed),
            false
        ));
        assert!(wants_merge(ExitClass::Success, None, false));
        assert!(!wants_merge(
            ExitClass::Success,
            Some(StageOutcome::Failed),
            false
        ));
        assert!(!wants_merge(ExitClass::Failure(1), None, false));
        assert!(!wants_merge(ExitClass::Success, None, true));
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
