//! Outcome derivation. Evidence in, verdict out: this module is the only
//! place a run's terminal outcome is decided, and it is pure so policy can
//! be tested without a daemon, a process, or a repository.

use serde::Serialize;

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
    /// Commits on the run branch that are not on the default branch.
    pub commit_count: u64,
    /// `None` when no test stage ran: either the exit and commit evidence
    /// never justified testing, or no test command is configured.
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
    /// Recovery found neither a live process nor committed work. The
    /// worktree stays available for inspection and the ticket is released.
    Orphaned,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Merged => "merged",
            Self::Failed => "failed",
            Self::NeedsReview => "needs_review",
            Self::Cancelled => "cancelled",
            Self::Orphaned => "orphaned",
        }
    }
}

/// Whether exit and commit evidence justify running the test stage. Tests
/// exist to qualify committed work for merging, so anything else skips them.
pub fn wants_tests(exit: ExitClass, commit_count: u64) -> bool {
    exit == ExitClass::Success && commit_count > 0
}

/// Whether the evidence so far justifies attempting a merge. A test stage
/// that ran and failed blocks the merge; an unconfigured test stage does
/// not, because the operator chose auto-merge policy without one.
pub fn wants_merge(exit: ExitClass, commit_count: u64, tests: Option<StageOutcome>) -> bool {
    wants_tests(exit, commit_count) && tests != Some(StageOutcome::Failed)
}

/// Maps complete evidence to the run's terminal outcome.
///
/// Constraints fixed by the design documents:
/// - Cancellation always wins: the outcome is `Cancelled` regardless of
///   other evidence, so racing exit and cancel events stay idempotent.
/// - Exit zero with no commits is NOT successful work.
/// - A run may only be `Merged` when the merge itself succeeded
///   (`Some(MergeOutcome::Merged)`).
/// - A nonzero or killed exit that left commits must preserve the evidence
///   for a human rather than merging or silently discarding it.
///
/// Policy decisions taken here:
/// - Committed work whose tests failed is `NeedsReview`, not `Failed`:
///   commits are evidence a human may want to salvage, and discarding them
///   silently would violate the preserve-the-work constraint above.
/// - A merge attempt that conflicted is `NeedsReview`: the work passed
///   tests, only integration needs a human.
/// - `Failed` is reserved for runs that produced no commits at all; there
///   is nothing to review.
pub fn derive_outcome(evidence: &RunEvidence) -> Outcome {
    if evidence.cancelled {
        return Outcome::Cancelled;
    }
    if evidence.merge == Some(MergeOutcome::Merged) {
        return Outcome::Merged;
    }
    if evidence.commit_count == 0 {
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
            commit_count: 0,
            tests: None,
            merge: None,
        }
    }

    #[test]
    fn exit_zero_without_commits_is_not_successful_work() {
        let outcome = derive_outcome(&evidence());
        assert_ne!(outcome, Outcome::Merged);
        assert_ne!(outcome, Outcome::Cancelled);
    }

    #[test]
    fn a_successful_merge_is_the_only_path_to_merged() {
        let merged = derive_outcome(&RunEvidence {
            commit_count: 2,
            tests: Some(StageOutcome::Passed),
            merge: Some(MergeOutcome::Merged),
            ..evidence()
        });
        assert_eq!(merged, Outcome::Merged);

        let diverged = derive_outcome(&RunEvidence {
            commit_count: 2,
            tests: Some(StageOutcome::Passed),
            merge: Some(MergeOutcome::Diverged),
            ..evidence()
        });
        assert_ne!(diverged, Outcome::Merged);
    }

    #[test]
    fn failed_tests_never_reach_merged() {
        let outcome = derive_outcome(&RunEvidence {
            commit_count: 1,
            tests: Some(StageOutcome::Failed),
            ..evidence()
        });
        assert_ne!(outcome, Outcome::Merged);
        assert_ne!(outcome, Outcome::Cancelled);
    }

    #[test]
    fn a_crashed_agent_with_commits_preserves_the_work_for_a_human() {
        let outcome = derive_outcome(&RunEvidence {
            exit: ExitClass::Failure(1),
            commit_count: 3,
            ..evidence()
        });
        assert_eq!(outcome, Outcome::NeedsReview);
    }

    #[test]
    fn cancellation_wins_over_every_other_reading() {
        let outcome = derive_outcome(&RunEvidence {
            cancelled: true,
            exit: ExitClass::KilledBySignal,
            commit_count: 5,
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
        assert!(wants_tests(ExitClass::Success, 1));
        assert!(!wants_tests(ExitClass::Success, 0));
        assert!(!wants_tests(ExitClass::Failure(1), 4));
        assert!(!wants_tests(ExitClass::KilledBySignal, 4));

        assert!(wants_merge(
            ExitClass::Success,
            1,
            Some(StageOutcome::Passed)
        ));
        assert!(wants_merge(ExitClass::Success, 1, None));
        assert!(!wants_merge(
            ExitClass::Success,
            1,
            Some(StageOutcome::Failed)
        ));
        assert!(!wants_merge(ExitClass::Failure(1), 1, None));
    }
}
