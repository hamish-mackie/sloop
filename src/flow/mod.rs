//! Flow definitions and the pure walk over them. Parsing turns a committed
//! YAML file into a validated `Flow`; `next_step` then replays the run's
//! ordered evidence log over that flow to derive the next stage to run or a
//! terminal reading. Neither half touches a clock, a process, or the store,
//! so policy can be tested without a daemon.
//!
//! The halves live in their own files and meet only here. This file is the
//! vocabulary: the types a stage is written in, and nothing that reads or
//! replays them. `parse` holds the YAML grammar and the `Raw*` wire types that
//! are its only readers; `walk` holds the replay, which reads exactly three
//! things from the vocabulary and never mentions the rest; `panel` holds the
//! panel slice — its types, its parse validation, and its aggregation — which
//! the walk does not know exists.

mod panel;
mod parse;
mod walk;

pub use panel::{
    Confidence, MAX_PANEL_REVIEWERS, MIN_PANEL_REVIEWERS, NO_VERDICT_REPORTED, PANEL_PROMPT_ROOT,
    PANEL_REVIEWER_INSTRUCTION, Panel, PanelOutcome, Reviewer, ReviewerReport, aggregate,
};
pub use parse::parse;
pub use walk::{
    HaltReason, Reported, StageEvidence, Step, Verdict, VerdictSource, next_step, resolve_verdict,
    return_trigger,
};

use serde::{Deserialize, Serialize};

pub const DEFAULT_FLOW_NAME: &str = "default";
pub const REVIEW_PROMPT_PATH: &str = ".agents/sloop/prompts/review.md";
pub const REVIEW_PROMPT_INSTRUCTION: &str = "Review the completed work for correctness and regressions. Run relevant tests, then report the verdict with `sloop verdict pass|fail --reason <text>` exactly once.";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Flow {
    pub name: String,
    pub stages: Vec<Stage>,
}

/// One stage: an untrusted `action`, an independent `result_check` that
/// judges it, and what to do when the reading is `Fail`.
///
/// The split is the point. The action is whatever produces the work and is
/// never trusted to grade itself; the check is a separate actor (or the
/// action's own exit, or a worker's report) that decides. Every judgement a
/// stage can carry is a configuration of this one shape rather than a policy
/// of its own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stage {
    pub name: String,
    pub action: Actor,
    pub result_check: Check,
    pub fail_action: FailAction,
    /// The merge builtin's fast-forward-only mode, written inside the action
    /// as `{ builtin: merge, ff_only: true }`. It is refused on every other
    /// action, so a `true` here always describes a merge stage.
    ///
    /// It rides on the stage rather than inside `Actor` so that a snapshot
    /// written before the option existed still reads: `Builtin::Merge` keeps
    /// its wire shape, and an absent key is the old behaviour exactly.
    #[serde(default, skip_serializing_if = "is_false")]
    pub ff_only: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// The inclusive upper bound on a `return_to` edge's attempt budget.
pub const MAX_RETURN_ATTEMPTS: u32 = 3;

/// The worst-case number of stage executions a flow may imply once every
/// `return_to` budget is spent. A flow that could exceed it is rejected at
/// parse time rather than discovered at runtime.
pub const MAX_FLOW_EXECUTIONS: u64 = 32;

/// Something that can be run within a stage. The same vocabulary serves
/// both positions: an `Actor` is either the stage's action or the
/// independent judge in its `result_check`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Actor {
    /// Spawns the ticket's agent target in the run worktree. The prompt
    /// comes from the ticket, not from the flow file.
    Agent,
    /// Runs an argv (no shell) in the run worktree.
    Exec { cmd: Vec<String> },
    /// A judgement Sloop makes itself, with no process to run.
    Builtin(Builtin),
}

/// The actors Sloop implements internally.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Builtin {
    /// Applies the run branch to the default branch using Sloop's merge
    /// policy. Only ever an action, never a check.
    Merge,
    /// Passes when Sloop observed at least one new commit on the run
    /// branch. Only ever a check.
    Commits,
    /// Integrates the default branch into the run branch, inside the run
    /// worktree. Only ever an action, never a check.
    ///
    /// It is the merge builtin's mirror image: the merge stage moves the
    /// default branch and reads the run branch, and this one moves the run
    /// branch and only ever reads the default branch. Putting it before the
    /// stages that judge the work is what makes those stages judge the tree
    /// the merge will actually land.
    Sync,
}

/// How a stage's action is judged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Check {
    /// The action's own exit status judges it: 0 passes.
    None,
    /// The worker must call `sloop verdict`; silence is a failure.
    Reported,
    /// An independent actor runs after the action and decides.
    Actor(Actor),
    /// Several independent reviewers each report, and a pure function over
    /// their reports decides.
    Panel(Panel),
}

/// What the walk does when a stage's reading is `Fail`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailAction {
    /// Stop the walk here, leaving the stages after it unrequested.
    Halt,
    /// Record the failure and carry on to the next stage.
    Continue,
    /// Re-run the span from `stage` through this one, up to `attempts`
    /// times.
    ReturnTo { stage: String, attempts: u32 },
}

pub(crate) fn built_in_default() -> Flow {
    let stages = vec![
        Stage {
            name: "build".into(),
            action: Actor::Agent,
            result_check: Check::Actor(Actor::Builtin(Builtin::Commits)),
            fail_action: FailAction::Halt,
            ff_only: false,
        },
        Stage {
            name: "merge".into(),
            action: Actor::Builtin(Builtin::Merge),
            result_check: Check::None,
            fail_action: FailAction::Halt,
            ff_only: false,
        },
    ];
    Flow {
        name: DEFAULT_FLOW_NAME.into(),
        stages,
    }
}
