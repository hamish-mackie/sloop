//! Flow definitions and the pure walk over them. Parsing turns a committed
//! YAML file into a validated `Flow`; `next_step` then replays the run's
//! ordered evidence log over that flow to derive the next stage to run or a
//! terminal reading. Neither half touches a clock, a process, or the store,
//! so policy can be tested without a daemon.

use std::collections::HashSet;
use std::path::Path;

use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};

pub const DEFAULT_FLOW_NAME: &str = "default";
pub const REVIEW_PROMPT_PATH: &str = ".agents/sloop/prompts/review.md";
pub const REVIEW_PROMPT_INSTRUCTION: &str = "Review the completed work for correctness and regressions. Run relevant tests, then report the verdict with `sloop verdict pass|fail --reason <text>` exactly once.";

/// The directory a panel's `prompt` path is resolved against. Panel prompts
/// are committed files like every other piece of flow configuration, so the
/// path is repository-relative under the Sloop directory rather than absolute
/// or relative to whatever the daemon's working directory happens to be.
pub const PANEL_PROMPT_ROOT: &str = ".agents/sloop";

/// The bootstrap prepended to a panel reviewer's prompt. A reviewer is not the
/// ticket's worker: it reads, it does not write, and its one job is the report.
pub const PANEL_REVIEWER_INSTRUCTION: &str = "You are one reviewer on a panel judging the work in this git worktree. Run `sloop brief` to read the assignment under review. Read and run whatever you need, but change nothing. Report your verdict with `sloop verdict pass|fail --reason <text> [--confidence low|medium|high]` exactly once; that call, not your prose, is your report, and finishing without it counts as a fail.";

/// The reason a reviewer that never reported is credited with.
pub const NO_VERDICT_REPORTED: &str = "no verdict reported";

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

/// A panel of independent reviewers.
///
/// One agentic reviewer is one uncalibrated opinion. A panel buys independence
/// with tokens: each reviewer examines the run alone and reports, and the stage
/// verdict is a *deterministic count* over the reports rather than anything a
/// reviewer decided. The opinions stay untrusted; the procedure over them is
/// kernel code, which is why [`aggregate`] lives here beside the walk and is
/// tested without an LLM anywhere near it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Panel {
    /// The prompt file every reviewer is handed, relative to
    /// [`PANEL_PROMPT_ROOT`]. One prompt for the whole panel: per-reviewer
    /// prompts would make the reports incomparable.
    pub prompt: String,
    pub reviewers: Vec<Reviewer>,
    /// How many `Pass` reports the stage needs. `1..=reviewers.len()`.
    pub quorum: u32,
}

/// One seat on a panel. Only the target and its model/effort vary: the point
/// of a panel is decorrelated failure modes, and the cheapest way to buy them
/// is to seat different vendors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reviewer {
    /// An entry under `agent.targets` in config.yaml. Existence is checked
    /// where the configured targets are known (see `config.rs`).
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
}

/// A panel smaller than this is not a panel, and one larger than this costs
/// more tokens than any v1 quorum rule can justify.
pub const MIN_PANEL_REVIEWERS: usize = 2;
pub const MAX_PANEL_REVIEWERS: usize = 5;

/// How sure a reviewer says it is. Recorded evidence only: v1 aggregation
/// counts reports and never weights them, so a confident wrong reviewer
/// outvotes nobody. Floats are deliberately absent — a scalar invites a
/// weighting rule that has not been designed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    Low,
    #[default]
    Medium,
    High,
}

impl Confidence {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "low" => Some(Self::Low),
            "medium" => Some(Self::Medium),
            "high" => Some(Self::High),
            _ => None,
        }
    }
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

/// The check a stage gets when it names none: an agent is never trusted to
/// grade itself by exiting cleanly, so it defaults to the commits gate;
/// everything else is judged by its own exit.
fn default_check(action: &Actor) -> Check {
    match action {
        Actor::Agent => Check::Actor(Actor::Builtin(Builtin::Commits)),
        Actor::Exec { .. } | Actor::Builtin(_) => Check::None,
    }
}

pub fn parse(name: &str, contents: &str) -> Result<Flow, String> {
    let file: RawFlowFile = serde_yaml::from_str(contents).map_err(|error| error.to_string())?;
    let raw_stages = match file {
        RawFlowFile::List(stages) => stages,
        RawFlowFile::Map { stages } => stages,
    };

    let mut stages = Vec::with_capacity(raw_stages.len());
    let mut names = HashSet::new();
    for raw in raw_stages {
        if !names.insert(raw.name.clone()) {
            return Err(format!("duplicate stage name `{}`", raw.name));
        }
        reject_removed_keys(&raw)?;
        let (action, ff_only) = parse_action(&raw.name, raw.action)?;
        let result_check = parse_result_check(&raw.name, &action, raw.result_check)?;
        let fail_action = parse_fail_action(&raw.name, raw.fail_action)?;
        validate_stage(&raw.name, &action, &result_check)?;
        stages.push(Stage {
            name: raw.name,
            action,
            result_check,
            fail_action,
            ff_only,
        });
    }

    validate_order(&stages)?;
    Ok(Flow {
        name: name.to_owned(),
        stages,
    })
}

/// Refuses every key the current grammar has dropped, by name. The keys are
/// still read off the stage (see [`RawStage`]) for exactly this: a flow file
/// written against an older grammar *does* say what its stages are, in a
/// spelling nothing reads any more, and the error that names the replacement is
/// the whole migration experience for whoever wrote it. Silence — or a generic
/// "must define an `action`" — would leave them to diff against a template.
fn reject_removed_keys(raw: &RawStage) -> Result<(), String> {
    let stage = &raw.name;
    if raw.kind.is_some() {
        return Err(format!(
            "stage `{stage}` uses the removed `kind` key; write `action: agent` instead \
             (see the 0.4.0 migration table in CHANGELOG.md)"
        ));
    }
    if raw.cmd.is_some() {
        return Err(format!(
            "stage `{stage}` uses the removed `cmd` key; write `action: {{ exec: [...] }}`"
        ));
    }
    if raw.verdict.is_some() {
        return Err(format!(
            "stage `{stage}` uses the removed `verdict` key; write `result_check: ...`"
        ));
    }
    if raw.on_fail.is_some() {
        return Err(format!(
            "stage `{stage}` uses the removed `on_fail` key; write \
             `fail_action: {{ return_to: <stage>, attempts: N }}` instead"
        ));
    }
    Ok(())
}

/// Resolves a stage's action, along with the `ff_only` option the merge
/// builtin alone accepts.
fn parse_action(stage: &str, action: Option<RawActor>) -> Result<(Actor, bool), String> {
    let Some(action) = action else {
        return Err(format!("stage `{stage}` must define an `action`"));
    };
    match action {
        RawActor::Agent { ff_only, .. } => {
            reject_ff_only(stage, ff_only)?;
            Ok((Actor::Agent, false))
        }
        RawActor::Exec { exec, ff_only } => {
            reject_ff_only(stage, ff_only)?;
            if exec.is_empty() {
                return Err(format!(
                    "stage `{stage}` exec action must define a non-empty command"
                ));
            }
            Ok((Actor::Exec { cmd: exec }, false))
        }
        RawActor::Builtin { builtin, ff_only } => match builtin.as_str() {
            // The one action `ff_only` means anything to, so the one that does
            // not refuse it.
            "merge" => Ok((Actor::Builtin(Builtin::Merge), ff_only.unwrap_or_default())),
            "sync" => {
                reject_ff_only(stage, ff_only)?;
                Ok((Actor::Builtin(Builtin::Sync), false))
            }
            "commits" => Err(format!(
                "stage `{stage}` builtin `commits` is a result_check, not an action"
            )),
            other => Err(format!("stage `{stage}` has unknown builtin `{other}`")),
        },
        RawActor::Name(name) if name == "agent" => Ok((Actor::Agent, false)),
        RawActor::Name(name) => Err(format!("stage `{stage}` has unknown action `{name}`")),
    }
}

/// `ff_only` describes what the merge stage does to the default branch, so it
/// says nothing anywhere else. Writing it elsewhere is far more likely to be a
/// misplaced expectation than a harmless extra key, and is refused as one.
fn reject_ff_only(stage: &str, ff_only: Option<bool>) -> Result<(), String> {
    match ff_only {
        None => Ok(()),
        Some(_) => Err(format!(
            "stage `{stage}` may only define `ff_only` on the `merge` builtin"
        )),
    }
}

/// Resolves a stage's result check, falling back to the check its action
/// implies when the stage names none.
fn parse_result_check(
    stage: &str,
    action: &Actor,
    result_check: Option<RawCheck>,
) -> Result<Check, String> {
    let Some(check) = result_check else {
        return Ok(default_check(action));
    };
    match check {
        RawCheck::Exec { exec } => {
            if exec.is_empty() {
                return Err(format!(
                    "stage `{stage}` exec result_check must define a non-empty command"
                ));
            }
            Ok(Check::Actor(Actor::Exec { cmd: exec }))
        }
        RawCheck::Builtin { builtin, ff_only } => {
            reject_ff_only(stage, ff_only)?;
            match builtin.as_str() {
                "commits" => Ok(Check::Actor(Actor::Builtin(Builtin::Commits))),
                // Both builtins that act on git refuse the check position:
                // a judge that moves a branch is not judging anything.
                "merge" | "sync" => Err(format!(
                    "stage `{stage}` result_check may not be the `{builtin}` builtin"
                )),
                other => Err(format!("stage `{stage}` has unknown builtin `{other}`")),
            }
        }
        RawCheck::Panel { panel } => Ok(Check::Panel(parse_panel(stage, panel)?)),
        RawCheck::Name(name) => match name.as_str() {
            "none" => Ok(Check::None),
            "reported" => Ok(Check::Reported),
            other => Err(format!(
                "stage `{stage}` has unknown result_check `{other}`"
            )),
        },
    }
}

/// Validates a panel block's own shape. Reviewer targets are checked later,
/// where the configured agent targets are known (see `config.rs`); everything
/// that can be decided from the flow file alone is decided here, so a flow
/// whose panel could never run is refused at parse time rather than at the
/// moment it would have spawned five agents.
fn parse_panel(stage: &str, raw: RawPanel) -> Result<Panel, String> {
    let prompt = raw
        .prompt
        .map(|prompt| prompt.trim().to_owned())
        .filter(|prompt| !prompt.is_empty())
        .ok_or_else(|| format!("stage `{stage}` panel must define a non-empty `prompt`"))?;
    // The path is joined onto the repository's Sloop directory, so anything
    // that could escape it is refused rather than resolved.
    if Path::new(&prompt).is_absolute() || prompt.split('/').any(|segment| segment == "..") {
        return Err(format!(
            "stage `{stage}` panel prompt must be a relative path under `{PANEL_PROMPT_ROOT}` \
             without `..`"
        ));
    }
    let reviewers = raw.reviewers.unwrap_or_default();
    if !(MIN_PANEL_REVIEWERS..=MAX_PANEL_REVIEWERS).contains(&reviewers.len()) {
        return Err(format!(
            "stage `{stage}` panel must define between {MIN_PANEL_REVIEWERS} and \
             {MAX_PANEL_REVIEWERS} reviewers; found {}",
            reviewers.len()
        ));
    }
    let reviewers = reviewers
        .into_iter()
        .map(|reviewer| {
            let target = reviewer.target.trim().to_owned();
            if target.is_empty() {
                return Err(format!(
                    "stage `{stage}` panel reviewer must name a non-empty `target`"
                ));
            }
            Ok(Reviewer {
                target,
                model: reviewer.model,
                effort: reviewer.effort,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    // An unstated quorum is unanimity: a panel whose rule was never written
    // down must not silently be the most permissive one it could have been.
    let quorum = raw
        .require
        .and_then(|require| require.quorum)
        .unwrap_or(reviewers.len() as u32);
    if quorum == 0 || quorum as usize > reviewers.len() {
        return Err(format!(
            "stage `{stage}` panel quorum must be between 1 and {}; found {quorum}",
            reviewers.len()
        ));
    }
    Ok(Panel {
        prompt,
        reviewers,
        quorum,
    })
}

fn parse_fail_action(stage: &str, raw: Option<RawFailAction>) -> Result<FailAction, String> {
    match raw {
        None => Ok(FailAction::Halt),
        Some(RawFailAction::ReturnTo {
            return_to,
            attempts,
        }) => {
            let attempts = attempts.unwrap_or(1);
            if attempts == 0 || attempts > MAX_RETURN_ATTEMPTS {
                return Err(format!(
                    "stage `{stage}` return_to attempts must be between 1 and {MAX_RETURN_ATTEMPTS}"
                ));
            }
            Ok(FailAction::ReturnTo {
                stage: return_to,
                attempts,
            })
        }
        Some(RawFailAction::Name(name)) => match name.as_str() {
            "fail" => Ok(FailAction::Halt),
            "continue" => Ok(FailAction::Continue),
            other => Err(format!("stage `{stage}` has unknown fail_action `{other}`")),
        },
    }
}

/// Checks that a stage's action and result check can stand together.
fn validate_stage(stage: &str, action: &Actor, result_check: &Check) -> Result<(), String> {
    if *action == Actor::Agent && *result_check == Check::None {
        return Err(format!(
            "stage `{stage}` is an agent action, so its result_check may not be `none`"
        ));
    }
    // Both git builtins are judged by what git did, so there is nothing for a
    // second opinion to add — and `reported` in particular could only ever
    // fail, since a builtin runs no worker to report with.
    for (builtin, name) in [(Builtin::Merge, "merge"), (Builtin::Sync, "sync")] {
        if *action == Actor::Builtin(builtin) && *result_check != Check::None {
            return Err(format!(
                "{name} stage `{stage}` must have `result_check: none`"
            ));
        }
    }
    Ok(())
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

/// Structural rules on a whole flow. Agent actions are deliberately absent:
/// one driver walks every stage the same way, so an agent action is legal in
/// any position and any number of times, each with its own supervised process.
fn validate_order(stages: &[Stage]) -> Result<(), String> {
    let merge = Actor::Builtin(Builtin::Merge);
    let sync = Actor::Builtin(Builtin::Sync);
    let merge_count = stages.iter().filter(|stage| stage.action == merge).count();
    if merge_count > 1 {
        return Err(format!(
            "flow may contain at most one merge stage; found {merge_count}"
        ));
    }
    // Any number of syncs, anywhere before the merge. Integrating the default
    // branch after it has already been moved would be answering a question the
    // walk has stopped asking.
    if let Some(merge_index) = stages.iter().position(|stage| stage.action == merge)
        && let Some(stray) = stages[merge_index..]
            .iter()
            .find(|stage| stage.action == sync)
    {
        return Err(format!(
            "sync stage `{}` must come before the merge stage",
            stray.name
        ));
    }
    if merge_count == 1 && stages.last().map(|stage| &stage.action) != Some(&merge) {
        return Err("merge stage must be last".into());
    }

    validate_return_edges(stages)?;
    Ok(())
}

/// Every `return_to` must point backwards, and the worst case they imply
/// together must stay bounded. Each edge re-runs the span from its target
/// through the stage that owns it, once per attempt; a flow whose total
/// executions could exceed `MAX_FLOW_EXECUTIONS` is refused rather than
/// left to spin.
fn validate_return_edges(stages: &[Stage]) -> Result<(), String> {
    let mut executions: u64 = stages.iter().map(stage_executions).sum();
    for (index, stage) in stages.iter().enumerate() {
        let FailAction::ReturnTo {
            stage: target,
            attempts,
        } = &stage.fail_action
        else {
            continue;
        };
        let Some(target_index) = stages[..index]
            .iter()
            .position(|candidate| candidate.name == *target)
        else {
            return Err(format!(
                "stage `{}` return_to must name an earlier stage; `{target}` is not one",
                stage.name
            ));
        };
        let span: u64 = stages[target_index..=index]
            .iter()
            .map(stage_executions)
            .sum();
        executions += u64::from(*attempts) * span;
    }
    if executions > MAX_FLOW_EXECUTIONS {
        return Err(format!(
            "flow may execute at most {MAX_FLOW_EXECUTIONS} stages in the worst case; \
             its return_to budgets imply {executions}"
        ));
    }
    Ok(())
}

/// What one execution of a stage costs the worst-case budget: the action
/// itself, plus one spawn for every seat on its panel. A panel is the only
/// check that spawns more than one process, and the budget exists precisely so
/// nobody discovers at runtime that a looping flow of five-seat panels implies
/// a hundred agent spawns.
fn stage_executions(stage: &Stage) -> u64 {
    1 + match &stage.result_check {
        Check::Panel(panel) => panel.reviewers.len() as u64,
        Check::None | Check::Reported | Check::Actor(_) => 0,
    }
}

/// A stage's pass/fail reading. Richer verdicts (e.g. `changes-requested`)
/// are a later phase; v1 is strictly binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Fail,
}

/// Where a stage's verdict came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerdictSource {
    /// The stage process's own exit status: 0 is `Pass`, anything else is
    /// `Fail`.
    ExitCode,
    /// A worker called `sloop verdict` over its stage's socket.
    Reported,
    /// A panel's reviewers reported and [`aggregate`] counted them.
    Panel,
}

impl VerdictSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ExitCode => "exit_code",
            Self::Reported => "reported",
            Self::Panel => "panel",
        }
    }
}

/// A worker's self-reported verdict for the stage it is running, gated to
/// at most one report per stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reported {
    pub verdict: Verdict,
    pub reason: Option<String>,
}

/// One stage execution's recorded result, and one entry in a run's evidence
/// log. Rows persist as they are produced and callers must supply them in
/// log order, because the walk is a replay of that order rather than a
/// lookup over a set: the same stage may hold several rows once a
/// `return_to` edge sends the walk back through it.
///
/// `stage_index` and `attempt` are the log key and are authoritative;
/// `stage` is carried for rendering only. A daemon crash mid-flow resumes
/// idempotently because `next_step` re-derives the same position from the
/// same rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageEvidence {
    pub stage: String,
    pub stage_index: usize,
    /// 1 for a stage's first execution, incrementing each time a `return_to`
    /// edge re-enters it.
    pub attempt: u32,
    pub verdict: Verdict,
    pub source: VerdictSource,
    pub reason: Option<String>,
}

/// Resolves a stage's verdict, source, and reason from the evidence selected
/// by its result check. Reports are authoritative only for `Reported`; every
/// other check ignores them.
pub fn resolve_verdict(
    check: &Check,
    exit: Verdict,
    reported: Option<Reported>,
) -> (Verdict, VerdictSource, Option<String>) {
    if *check != Check::Reported {
        return (exit, VerdictSource::ExitCode, None);
    }
    match reported {
        Some(reported) => (reported.verdict, VerdictSource::Reported, reported.reason),
        None => (
            Verdict::Fail,
            VerdictSource::Reported,
            Some("no verdict reported".into()),
        ),
    }
}

/// One reviewer's report, as the aggregation reads it back from evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewerReport {
    pub verdict: Verdict,
    /// Absent only for a reviewer that never reported: it has no confidence to
    /// record. Present reports default to [`Confidence::Medium`].
    pub confidence: Option<Confidence>,
    pub reason: String,
}

impl ReviewerReport {
    /// What a reviewer that exited without reporting counts as. Silence is not
    /// an abstention: a panel that could not be heard from has not approved
    /// anything, so the seat is filled with a `Fail` and says why.
    fn silent() -> Self {
        Self {
            verdict: Verdict::Fail,
            confidence: None,
            reason: NO_VERDICT_REPORTED.to_owned(),
        }
    }
}

/// A panel's derived reading: every seat filled, and the verdict the count
/// over them yields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PanelOutcome {
    pub verdict: Verdict,
    /// One entry per seat, in reviewer order, silent seats included.
    pub reports: Vec<ReviewerReport>,
    /// The tally, in one clause fit to quote back to an agent.
    pub reason: String,
}

/// Derives a panel's verdict from its reviewers' reports.
///
/// This is the whole aggregation, and it is deliberately dull: `Pass` iff at
/// least `quorum` seats reported `Pass`. Confidence is carried through as
/// evidence and never consulted; there is no veto and no weighting, so a panel
/// behaves the same way every time and an operator can predict it from the
/// config alone.
///
/// `reported` is indexed by reviewer; a `None` — or a short slice, which is
/// what a stage abandoned part-way through its panel leaves — fills that seat
/// with [`ReviewerReport::silent`]. Nothing here reads a clock, a process, or
/// the store, so the aggregate is *derived at read time* rather than stored:
/// a resumed run recomputes the identical verdict from the identical rows.
pub fn aggregate(panel: &Panel, reported: &[Option<ReviewerReport>]) -> PanelOutcome {
    let reports: Vec<ReviewerReport> = (0..panel.reviewers.len())
        .map(|seat| {
            reported
                .get(seat)
                .cloned()
                .flatten()
                .unwrap_or_else(ReviewerReport::silent)
        })
        .collect();
    let passed = reports
        .iter()
        .filter(|report| report.verdict == Verdict::Pass)
        .count();
    let verdict = if passed as u64 >= u64::from(panel.quorum) {
        Verdict::Pass
    } else {
        Verdict::Fail
    };
    PanelOutcome {
        verdict,
        reason: format!(
            "panel: {passed} of {} reviewers passed, quorum {}",
            reports.len(),
            panel.quorum,
        ),
        reports,
    }
}

/// What the walk does next, given a flow and the evidence log so far.
#[derive(Debug, PartialEq, Eq)]
pub enum Step<'a> {
    /// Where the replay stands with the log exhausted: this stage's
    /// `attempt`-th execution is the one that has not been recorded yet.
    Run { stage: &'a Stage, attempt: u32 },
    /// The replay stopped short of the end of the flow. Stages after it are
    /// never requested.
    Halted {
        failed_stage: String,
        reason: HaltReason,
    },
    /// The cursor walked off the end of the flow.
    Complete,
}

/// Why a replay stopped short of the end of the flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HaltReason {
    /// The stage failed and its `fail_action` is `Halt`.
    FailActionHalt,
    /// The stage failed with a `return_to`, but that edge's attempt budget
    /// was already spent earlier in this same replay.
    ReturnBudgetExhausted,
    /// The log does not describe a walk over this flow: a row arrived for a
    /// stage or attempt the replay was not standing on, or a `return_to`
    /// named a stage the bound flow no longer holds. Recovery treats this as
    /// needs-review rather than guessing at a history it cannot re-derive.
    CorruptLog,
}

/// The pure decision at the heart of a flow: a left fold over the run's
/// ordered evidence log. A cursor starts at stage 0 and consumes rows in
/// sequence: a `Pass` advances it, a `Fail` applies the failed stage's
/// `fail_action` — `Halt` stops, `Continue` advances, `ReturnTo` moves the
/// cursor back if that edge's budget is not yet spent. Wherever the cursor
/// stands once the log is exhausted is what runs next.
///
/// Position is therefore always recomputed from history; no stored cursor
/// exists to disagree with it. Because the fold reads only the rows — never
/// a clock, a process, or the store — resuming after a crash with the same
/// log yields the same `Step`: the walk is idempotent by construction.
///
/// Rows must arrive in log order. A row for any other stage or attempt than
/// the one the cursor expects is a corrupt log, not a hint to re-order.
pub fn next_step<'a>(flow: &'a Flow, evidence: &[StageEvidence]) -> Step<'a> {
    let mut cursor = 0usize;
    for (position, row) in evidence.iter().enumerate() {
        let Some(stage) = flow.stages.get(cursor) else {
            // The walk already ran off the end of the flow, so no execution
            // could have produced this row.
            return corrupt(row.stage.clone());
        };
        let consumed = &evidence[..position];
        if row.stage_index != cursor || row.attempt != attempt_at(consumed, cursor) {
            return corrupt(stage.name.clone());
        }
        match row.verdict {
            Verdict::Pass => cursor += 1,
            Verdict::Fail => match &stage.fail_action {
                FailAction::Halt => {
                    return Step::Halted {
                        failed_stage: stage.name.clone(),
                        reason: HaltReason::FailActionHalt,
                    };
                }
                FailAction::Continue => cursor += 1,
                FailAction::ReturnTo {
                    stage: target,
                    attempts,
                } => {
                    let Some(target_index) = flow
                        .stages
                        .iter()
                        .position(|candidate| candidate.name == *target)
                    else {
                        return corrupt(stage.name.clone());
                    };
                    if returns_taken(consumed, cursor) >= *attempts {
                        return Step::Halted {
                            failed_stage: stage.name.clone(),
                            reason: HaltReason::ReturnBudgetExhausted,
                        };
                    }
                    // Replay resumes from the target; the rows appended
                    // before the jump are behind the fold and superseded by
                    // whatever the re-run records.
                    cursor = target_index;
                }
            },
        }
    }
    match flow.stages.get(cursor) {
        Some(stage) => Step::Run {
            stage,
            attempt: attempt_at(evidence, cursor),
        },
        None => Step::Complete,
    }
}

/// The failure that most recently sent the walk backwards, as the `(stage
/// index, attempt)` key of its evidence row — the row a re-entered stage is
/// being re-run *because of*.
///
/// Every backward edge leaves a `Fail` row on the stage that owns it, so the
/// last such row in the log is the jump the walk is still inside. Reading it
/// from the persisted log rather than from a live counter is what makes a
/// re-run prompt reproducible: a resumed run derives the same trigger, and so
/// composes the same prompt, as the daemon that first took the jump.
pub fn return_trigger(flow: &Flow, evidence: &[StageEvidence]) -> Option<(usize, u32)> {
    evidence
        .iter()
        .rev()
        .find(|row| {
            row.verdict == Verdict::Fail
                && matches!(
                    flow.stages
                        .get(row.stage_index)
                        .map(|stage| &stage.fail_action),
                    Some(FailAction::ReturnTo { .. })
                )
        })
        .map(|row| (row.stage_index, row.attempt))
}

fn corrupt(failed_stage: String) -> Step<'static> {
    Step::Halted {
        failed_stage,
        reason: HaltReason::CorruptLog,
    }
}

/// Which attempt the next execution of `index` is, given the rows the fold
/// has already consumed. Every row records one completed execution of its
/// stage, so counting them is the attempt counter; recomputing it by scan
/// keeps the fold allocation-free and leaves nothing to fall out of step
/// with the log.
fn attempt_at(consumed: &[StageEvidence], index: usize) -> u32 {
    count(consumed.iter().filter(|row| row.stage_index == index)).saturating_add(1)
}

/// How much of the backward edge leaving `index` the replay has already
/// spent. A stage owns exactly one `fail_action`, so its outgoing edge is
/// identified by the stage alone, and every consumed `Fail` row there is one
/// jump the fold has taken.
fn returns_taken(consumed: &[StageEvidence], index: usize) -> u32 {
    count(
        consumed
            .iter()
            .filter(|row| row.stage_index == index && row.verdict == Verdict::Fail),
    )
}

fn count<'a>(rows: impl Iterator<Item = &'a StageEvidence>) -> u32 {
    u32::try_from(rows.count()).unwrap_or(u32::MAX)
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawFlowFile {
    List(Vec<RawStage>),
    Map { stages: Vec<RawStage> },
}

/// A stage exactly as written.
///
/// The removed keys are still read — as payloads nobody looks at — so that
/// `reject_removed_keys` can name them. Unknown fields are otherwise accepted
/// here, so dropping them outright would turn an older flow file into a stage
/// that appears to define nothing at all, or into one whose repair block was
/// quietly ignored.
#[derive(Debug, Deserialize)]
struct RawStage {
    name: String,
    action: Option<RawActor>,
    result_check: Option<RawCheck>,
    fail_action: Option<RawFailAction>,
    /// Removed in 0.4.0; `action` replaces it.
    kind: Option<IgnoredAny>,
    /// Removed in 0.4.0; `action: { exec: [...] }` replaces it.
    cmd: Option<IgnoredAny>,
    /// Removed in 0.4.0; `result_check` replaces it.
    verdict: Option<IgnoredAny>,
    /// Removed in 0.4.0; `fail_action: { return_to: ... }` replaces it.
    on_fail: Option<IgnoredAny>,
}

/// `action: agent` | `{ agent: <ignored> }` | `{ exec: [argv] }` |
/// `{ builtin: merge | sync }`. The `agent` payload is accepted and discarded:
/// an agent action's prompt comes from the ticket, not the flow file.
///
/// `ff_only` is carried on every mapping variant, not just the builtin one, so
/// that writing it on an action that cannot honour it is an error the author
/// sees rather than a key that is silently dropped.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawActor {
    Name(String),
    Agent {
        /// Present only so `{ agent: <prompt> }` matches this variant; the
        /// payload is deliberately discarded.
        #[allow(dead_code)]
        agent: IgnoredAny,
        ff_only: Option<bool>,
    },
    Exec {
        exec: Vec<String>,
        ff_only: Option<bool>,
    },
    Builtin {
        builtin: String,
        ff_only: Option<bool>,
    },
}

/// `result_check: none | reported` | `{ exec: [argv] }` |
/// `{ builtin: commits }` | `{ panel: {...} }`.
///
/// `Panel` is last because its payload is deliberately lenient — every field
/// inside it is optional so `parse_panel` can name what is missing instead of
/// serde reporting that nothing matched. Only the `panel` key itself is
/// required, which is what keeps the other shapes from falling into it.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawCheck {
    Name(String),
    Exec {
        exec: Vec<String>,
    },
    Builtin {
        builtin: String,
        ff_only: Option<bool>,
    },
    Panel {
        panel: RawPanel,
    },
}

#[derive(Debug, Deserialize)]
struct RawPanel {
    prompt: Option<String>,
    reviewers: Option<Vec<RawReviewer>>,
    require: Option<RawRequire>,
}

#[derive(Debug, Deserialize)]
struct RawReviewer {
    target: String,
    model: Option<String>,
    effort: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawRequire {
    quorum: Option<u32>,
}

/// `fail_action: fail | continue` | `{ return_to: <stage>, attempts: N }`.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawFailAction {
    Name(String),
    ReturnTo {
        return_to: String,
        attempts: Option<u32>,
    },
}

#[cfg(test)]
mod tests {
    use super::{
        Actor, Builtin, Check, Confidence, FailAction, Flow, HaltReason, NO_VERDICT_REPORTED,
        Panel, Reported, Reviewer, ReviewerReport, Stage, StageEvidence, Step, Verdict,
        VerdictSource, aggregate, next_step, parse, resolve_verdict, return_trigger,
    };

    fn error(yaml: &str) -> String {
        parse("example", yaml).unwrap_err()
    }

    fn commits() -> Check {
        Check::Actor(Actor::Builtin(Builtin::Commits))
    }

    fn exec_check(cmd: &[&str]) -> Check {
        Check::Actor(Actor::Exec {
            cmd: cmd.iter().map(|part| (*part).to_owned()).collect(),
        })
    }

    #[test]
    fn valid_multi_stage_flow_parses_in_order() {
        let flow = parse(
            "release",
            "stages:\n  - name: build\n    action: agent\n  - name: test\n    action: { exec: [cargo, test] }\n    result_check: { exec: [cargo, clippy] }\n  - name: merge\n    action: { builtin: merge }\n",
        )
        .unwrap();

        assert_eq!(
            flow,
            Flow {
                name: "release".into(),
                stages: vec![
                    Stage {
                        name: "build".into(),
                        action: Actor::Agent,
                        result_check: commits(),
                        fail_action: FailAction::Halt,
                        ff_only: false,
                    },
                    Stage {
                        name: "test".into(),
                        action: Actor::Exec {
                            cmd: vec!["cargo".into(), "test".into()],
                        },
                        result_check: exec_check(&["cargo", "clippy"]),
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
                ],
            }
        );
    }

    /// Every part a stage can leave unwritten defaults to the same stage the
    /// long form spells out, so the two forms are one grammar rather than two.
    #[test]
    fn a_fully_written_stage_matches_the_defaults_it_restates() {
        let terse = parse(
            "release",
            "stages:\n  - name: build\n    action: agent\n  - name: test\n    action: { exec: [cargo, test] }\n    result_check: { exec: [cargo, clippy] }\n  - name: merge\n    action: { builtin: merge }\n",
        )
        .unwrap();
        let explicit = parse(
            "release",
            "stages:\n  - name: build\n    action: { agent: ignored }\n    result_check: { builtin: commits }\n    fail_action: fail\n  - name: test\n    action: { exec: [cargo, test] }\n    result_check: { exec: [cargo, clippy] }\n    fail_action: fail\n  - name: merge\n    action: { builtin: merge }\n    result_check: none\n    fail_action: fail\n",
        )
        .unwrap();

        assert_eq!(terse, explicit);
    }

    /// An agent action's prompt comes from the ticket, so the mapping form's
    /// payload is accepted and discarded rather than read.
    #[test]
    fn a_bare_agent_action_needs_no_payload() {
        for yaml in [
            "- { name: build, action: agent }\n",
            "- { name: build, action: { agent: ignored } }\n",
        ] {
            let flow = parse("example", yaml).unwrap();
            assert_eq!(flow.stages[0].action, Actor::Agent);
            assert_eq!(flow.stages[0].result_check, commits());
        }
    }

    #[test]
    fn named_result_checks_parse() {
        let flow = parse(
            "example",
            "- { name: build, action: agent, result_check: reported }\n- { name: test, action: { exec: ['true'] }, result_check: none }\n- { name: gate, action: { exec: ['true'] }, result_check: { builtin: commits } }\n",
        )
        .unwrap();
        assert_eq!(flow.stages[0].result_check, Check::Reported);
        assert_eq!(flow.stages[1].result_check, Check::None);
        assert_eq!(flow.stages[2].result_check, commits());
    }

    /// The snapshot contract: a flow this binary writes onto a `runs` row is
    /// the flow this binary reads back when it recovers that run. Every field
    /// a stage can carry is exercised, because the one that is skipped when
    /// absent — `ff_only` — is exactly the one a missing serde default would
    /// silently break.
    #[test]
    fn snapshots_round_trip_through_the_new_vocabulary() {
        let flow = parse(
            "example",
            concat!(
                "- { name: build, action: agent, result_check: reported }\n",
                "- { name: test, action: { exec: [cargo, test] }, result_check: { exec: [cargo, fmt] }, fail_action: { return_to: build, attempts: 2 } }\n",
                "- name: review\n",
                "  action: agent\n",
                "  result_check:\n",
                "    panel:\n",
                "      prompt: prompts/review.md\n",
                "      reviewers: [{ target: claude }, { target: codex, model: gpt }]\n",
                "      require: { quorum: 2 }\n",
                "- { name: merge, action: { builtin: merge, ff_only: true }, result_check: none }\n",
            ),
        )
        .unwrap();
        let snapshot = serde_json::to_string(&flow).unwrap();

        assert!(snapshot.contains("result_check"), "{snapshot}");
        assert!(snapshot.contains("ff_only"), "{snapshot}");
        assert!(snapshot.contains("Panel"), "{snapshot}");
        assert!(!snapshot.contains("\"kind\""), "{snapshot}");
        assert!(!snapshot.contains("\"verdict\""), "{snapshot}");
        assert_eq!(serde_json::from_str::<Flow>(&snapshot).unwrap(), flow);
    }

    /// An omitted `result_check` is the one its action implies, and every
    /// check a stage can name binds what it says.
    #[test]
    fn result_checks_and_their_defaults_parse() {
        let flow = parse(
            "example",
            "- { name: build, action: agent, result_check: reported }\n- { name: test, action: { exec: ['true'] }, result_check: { builtin: commits } }\n- { name: review, action: { exec: ['true'] }, result_check: none }\n",
        )
        .unwrap();
        assert_eq!(flow.stages[0].result_check, Check::Reported);
        assert_eq!(flow.stages[1].result_check, commits());
        assert_eq!(flow.stages[2].result_check, Check::None);

        let defaults = parse(
            "example",
            "- { name: build, action: agent }\n- { name: test, action: { exec: ['true'] } }\n- { name: merge, action: { builtin: merge } }\n",
        )
        .unwrap();
        assert_eq!(defaults.stages[0].result_check, commits());
        assert_eq!(defaults.stages[1].result_check, Check::None);
        assert_eq!(defaults.stages[2].result_check, Check::None);
    }

    /// A flow file written for `0.3.0` is refused by name rather than by
    /// omission: the stage below does say what it is, in a spelling nothing
    /// reads any more, and the error is the only migration note its author
    /// gets.
    #[test]
    fn the_removed_grammar_is_rejected_by_name() {
        for (yaml, needle) in [
            (
                "- { name: build, kind: agent }\n",
                "uses the removed `kind` key; write `action: agent` instead",
            ),
            (
                "- { name: test, action: { exec: ['true'] }, cmd: ['true'] }\n",
                "uses the removed `cmd` key; write `action: { exec: [...] }`",
            ),
            (
                "- { name: test, action: agent, verdict: reported }\n",
                "uses the removed `verdict` key; write `result_check: ...`",
            ),
            (
                "- { name: test, action: { exec: ['true'] }, on_fail: { agent: fix it } }\n",
                "uses the removed `on_fail` key; write `fail_action: { return_to: <stage>, attempts: N }`",
            ),
        ] {
            let error = error(yaml);
            assert!(error.contains(needle), "{error}");
        }

        // The whole of a 0.3.0 stage, refused on the first removed key rather
        // than on the `action` it never had a chance to define.
        let legacy = error("- { name: test, kind: exec, cmd: ['true'], verdict: exit }\n");
        assert!(legacy.contains("stage `test`"), "{legacy}");
        assert!(legacy.contains("removed `kind` key"), "{legacy}");
        assert!(
            legacy.contains("see the 0.4.0 migration table in CHANGELOG.md"),
            "{legacy}"
        );
    }

    #[test]
    fn an_agent_action_may_not_go_unjudged() {
        let error = error("- { name: build, action: agent, result_check: none }\n");
        assert!(error.contains("stage `build`"), "{error}");
        assert!(
            error.contains("is an agent action, so its result_check may not be `none`"),
            "{error}"
        );
    }

    #[test]
    fn the_merge_builtin_is_an_action_and_never_a_check() {
        let as_check = error(
            "- { name: build, action: agent }\n- { name: gate, action: { exec: ['true'] }, result_check: { builtin: merge } }\n",
        );
        assert!(
            as_check.contains("result_check may not be the `merge` builtin"),
            "{as_check}"
        );

        let judged = error(
            "- { name: build, action: agent }\n- { name: merge, action: { builtin: merge }, result_check: reported }\n",
        );
        assert!(
            judged.contains("must have `result_check: none`"),
            "{judged}"
        );
    }

    #[test]
    fn the_sync_builtin_parses_as_an_action() {
        let flow = parse(
            "example",
            "- { name: build, action: agent }\n- { name: sync, action: { builtin: sync } }\n",
        )
        .unwrap();
        assert_eq!(flow.stages[1].action, Actor::Builtin(Builtin::Sync));
        assert_eq!(flow.stages[1].result_check, Check::None);
    }

    /// Sync moves the run branch. A judge that moves a branch is not judging
    /// anything, and a builtin that runs no worker could never report.
    #[test]
    fn the_sync_builtin_is_an_action_and_never_a_check() {
        let as_check = error(
            "- { name: build, action: agent }\n- { name: gate, action: { exec: ['true'] }, result_check: { builtin: sync } }\n",
        );
        assert!(
            as_check.contains("result_check may not be the `sync` builtin"),
            "{as_check}"
        );

        let judged = error(
            "- { name: build, action: agent }\n- { name: sync, action: { builtin: sync }, result_check: reported }\n",
        );
        assert!(
            judged.contains("sync stage `sync` must have `result_check: none`"),
            "{judged}"
        );
    }

    /// Any number of syncs, anywhere before the merge — but integrating the
    /// default branch after it has already been moved answers nothing.
    #[test]
    fn sync_stages_may_repeat_but_must_precede_the_merge() {
        let repeated = parse(
            "example",
            "- { name: build, action: agent }\n- { name: sync, action: { builtin: sync } }\n- { name: test, action: { exec: ['true'] } }\n- { name: resync, action: { builtin: sync } }\n- { name: merge, action: { builtin: merge } }\n",
        )
        .unwrap();
        assert_eq!(repeated.stages.len(), 5);

        let after = error(
            "- { name: build, action: agent }\n- { name: merge, action: { builtin: merge } }\n- { name: sync, action: { builtin: sync } }\n",
        );
        assert!(
            after.contains("sync stage `sync` must come before the merge stage"),
            "{after}"
        );
    }

    #[test]
    fn ff_only_binds_on_the_merge_stage_and_defaults_to_off() {
        let plain = parse(
            "example",
            "- { name: build, action: agent }\n- { name: merge, action: { builtin: merge } }\n",
        )
        .unwrap();
        assert!(!plain.stages[1].ff_only);

        let train = parse(
            "example",
            "- { name: build, action: agent }\n- { name: merge, action: { builtin: merge, ff_only: true }, result_check: none }\n",
        )
        .unwrap();
        assert!(train.stages[1].ff_only);

        // Written `false`, it is still the untouched merge policy.
        let explicit = parse(
            "example",
            "- { name: build, action: agent }\n- { name: merge, action: { builtin: merge, ff_only: false } }\n",
        )
        .unwrap();
        assert!(!explicit.stages[1].ff_only);
    }

    /// A queued run must recover the mode it was admitted with, and a stage
    /// that never asked for the mode must not start carrying it.
    #[test]
    fn ff_only_survives_a_snapshot_round_trip() {
        let flow = parse(
            "example",
            "- { name: build, action: agent }\n- { name: merge, action: { builtin: merge, ff_only: true } }\n",
        )
        .unwrap();
        let snapshot = serde_json::to_string(&flow).unwrap();
        assert!(snapshot.contains("ff_only"), "{snapshot}");
        assert_eq!(serde_json::from_str::<Flow>(&snapshot).unwrap(), flow);

        let plain = parse(
            "example",
            "- { name: build, action: agent }\n- { name: merge, action: { builtin: merge } }\n",
        )
        .unwrap();
        let snapshot = serde_json::to_string(&plain).unwrap();
        // Off is the absence of the key, so a flow that never asked for the
        // mode does not start carrying it.
        assert!(!snapshot.contains("ff_only"), "{snapshot}");
        assert_eq!(serde_json::from_str::<Flow>(&snapshot).unwrap(), plain);
    }

    /// `ff_only` describes what the merge does to the default branch, so
    /// anywhere else it is a misplaced expectation rather than an extra key
    /// worth dropping quietly.
    #[test]
    fn ff_only_is_refused_off_the_merge_builtin() {
        for yaml in [
            "- { name: build, action: agent }\n- { name: sync, action: { builtin: sync, ff_only: true } }\n",
            "- { name: test, action: { exec: ['true'], ff_only: true } }\n",
            "- { name: build, action: { agent: ignored, ff_only: true } }\n",
            "- { name: build, action: agent, result_check: { builtin: commits, ff_only: true } }\n",
        ] {
            let error = error(yaml);
            assert!(
                error.contains("may only define `ff_only` on the `merge` builtin"),
                "{error}"
            );
        }
    }

    #[test]
    fn the_commits_builtin_is_a_check_and_never_an_action() {
        let error = error("- { name: build, action: { builtin: commits } }\n");
        assert!(
            error.contains("`commits` is a result_check, not an action"),
            "{error}"
        );
    }

    #[test]
    fn unknown_words_are_rejected() {
        for (yaml, needle) in [
            (
                "- { name: build, action: wizard }\n",
                "unknown action `wizard`",
            ),
            (
                "- { name: build, action: { builtin: sparkle } }\n",
                "unknown builtin `sparkle`",
            ),
            (
                "- { name: build, action: agent, result_check: magic }\n",
                "unknown result_check `magic`",
            ),
            (
                "- { name: build, action: agent, fail_action: retry }\n",
                "unknown fail_action `retry`",
            ),
        ] {
            let error = error(yaml);
            assert!(error.contains(needle), "{error}");
        }
    }

    #[test]
    fn empty_commands_are_rejected() {
        let action = error("- { name: build, action: { exec: [] } }\n");
        assert!(
            action.contains("exec action must define a non-empty command"),
            "{action}"
        );

        let check = error("- { name: build, action: agent, result_check: { exec: [] } }\n");
        assert!(
            check.contains("exec result_check must define a non-empty command"),
            "{check}"
        );
    }

    /// Both advisory failures and backward edges are live: the walk honours
    /// what the fold returns, so the vocabulary binds rather than parsing into
    /// a rejection.
    #[test]
    fn continue_and_return_to_bind_the_edges_they_name() {
        let advisory = parse(
            "example",
            "- { name: build, action: agent }\n- { name: lint, action: { exec: ['true'] }, fail_action: continue }\n",
        )
        .unwrap();
        assert_eq!(advisory.stages[1].fail_action, FailAction::Continue);

        let looping = parse(
            "example",
            "- { name: build, action: agent }\n- { name: test, action: { exec: ['true'] }, fail_action: { return_to: build, attempts: 2 } }\n",
        )
        .unwrap();
        assert_eq!(
            looping.stages[1].fail_action,
            FailAction::ReturnTo {
                stage: "build".into(),
                attempts: 2,
            }
        );

        // An omitted budget is one attempt, not an unbounded loop.
        let defaulted = parse(
            "example",
            "- { name: build, action: agent }\n- { name: test, action: { exec: ['true'] }, fail_action: { return_to: build } }\n",
        )
        .unwrap();
        assert_eq!(
            defaulted.stages[1].fail_action,
            FailAction::ReturnTo {
                stage: "build".into(),
                attempts: 1,
            }
        );
    }

    #[test]
    fn return_to_must_name_an_earlier_stage() {
        for target in ["test", "later", "missing"] {
            let error = error(&format!(
                "- {{ name: build, action: agent }}\n- {{ name: test, action: {{ exec: ['true'] }}, fail_action: {{ return_to: {target} }} }}\n- {{ name: later, action: {{ exec: ['true'] }} }}\n",
            ));
            assert!(
                error.contains("return_to must name an earlier stage"),
                "{error}"
            );
        }
    }

    #[test]
    fn return_to_rejects_out_of_range_attempts() {
        for attempts in ["0", "4"] {
            let error = error(&format!(
                "- {{ name: build, action: agent }}\n- {{ name: test, action: {{ exec: ['true'] }}, fail_action: {{ return_to: build, attempts: {attempts} }} }}\n",
            ));
            assert!(error.contains("stage `test`"), "{error}");
            assert!(
                error.contains("return_to attempts must be between 1 and 3"),
                "{error}"
            );
        }
    }

    #[test]
    fn the_worst_case_execution_count_is_capped() {
        // Ten stages, each execution of the whole span costing ten: the base
        // walk plus three re-runs is forty, well over the cap.
        let mut yaml = String::from("- { name: build, action: agent }\n");
        for index in 1..9 {
            yaml.push_str(&format!(
                "- {{ name: s{index}, action: {{ exec: ['true'] }} }}\n"
            ));
        }
        yaml.push_str(
            "- { name: last, action: { exec: ['true'] }, fail_action: { return_to: build, attempts: 3 } }\n",
        );

        let error = error(&yaml);
        assert!(
            error.contains("at most 32 stages in the worst case"),
            "{error}"
        );
        assert!(error.contains("imply 40"), "{error}");
    }

    #[test]
    fn duplicate_stage_names_are_rejected() {
        let error = error(
            "- { name: build, action: agent }\n- { name: build, action: { builtin: merge } }\n",
        );
        assert!(error.contains("duplicate stage name `build`"), "{error}");
    }

    /// One driver walks every stage, so nothing about a flow's shape depends
    /// on where its agent actions sit or how many there are.
    #[test]
    fn agent_actions_are_legal_in_any_position_and_any_number() {
        let leading_exec = parse(
            "example",
            "- { name: check, action: { exec: ['true'] } }\n- { name: build, action: agent }\n",
        )
        .unwrap();
        assert_eq!(leading_exec.stages[1].action, Actor::Agent);

        let two_agents = parse(
            "example",
            "- { name: build, action: agent }\n- { name: review, action: agent, result_check: reported }\n",
        )
        .unwrap();
        assert_eq!(two_agents.stages[0].action, Actor::Agent);
        assert_eq!(two_agents.stages[1].action, Actor::Agent);

        let no_agent = parse("example", "- { name: check, action: { exec: ['true'] } }\n").unwrap();
        assert_eq!(no_agent.stages.len(), 1);
    }

    #[test]
    fn at_most_one_merge_stage_is_allowed() {
        let error = error(
            "- { name: build, action: agent }\n- { name: merge-one, action: { builtin: merge } }\n- { name: merge-two, action: { builtin: merge } }\n",
        );
        assert!(error.contains("at most one merge stage"), "{error}");
    }

    #[test]
    fn merge_stage_must_be_last() {
        let error = error(
            "- { name: build, action: agent }\n- { name: merge, action: { builtin: merge } }\n- { name: check, action: { exec: ['true'] } }\n",
        );
        assert!(error.contains("merge stage must be last"), "{error}");
    }

    fn build_review_merge() -> Flow {
        Flow {
            name: "example".into(),
            stages: vec![
                Stage {
                    name: "build".into(),
                    action: Actor::Agent,
                    result_check: commits(),
                    fail_action: FailAction::Halt,
                    ff_only: false,
                },
                Stage {
                    name: "review".into(),
                    action: Actor::Exec {
                        cmd: vec!["true".into()],
                    },
                    result_check: Check::None,
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
            ],
        }
    }

    /// A flow of exec stages with the given names and fail actions, built by
    /// hand so a fold test can name an edge without spelling out YAML.
    fn flow_with(stages: &[(&str, FailAction)]) -> Flow {
        Flow {
            name: "example".into(),
            stages: stages
                .iter()
                .map(|(name, fail_action)| Stage {
                    name: (*name).into(),
                    action: Actor::Exec {
                        cmd: vec!["true".into()],
                    },
                    result_check: Check::None,
                    fail_action: fail_action.clone(),
                    ff_only: false,
                })
                .collect(),
        }
    }

    fn return_to(stage: &str, attempts: u32) -> FailAction {
        FailAction::ReturnTo {
            stage: stage.into(),
            attempts,
        }
    }

    /// One log row, keyed the way the fold reads it.
    fn row(flow: &Flow, index: usize, attempt: u32, verdict: Verdict) -> StageEvidence {
        StageEvidence {
            stage: flow.stages[index].name.clone(),
            stage_index: index,
            attempt,
            verdict,
            source: VerdictSource::ExitCode,
            reason: None,
        }
    }

    fn pass(flow: &Flow, index: usize, attempt: u32) -> StageEvidence {
        row(flow, index, attempt, Verdict::Pass)
    }

    fn fail(flow: &Flow, index: usize, attempt: u32) -> StageEvidence {
        row(flow, index, attempt, Verdict::Fail)
    }

    fn passed(flow: &Flow, index: usize) -> StageEvidence {
        pass(flow, index, 1)
    }

    fn failed(flow: &Flow, index: usize) -> StageEvidence {
        fail(flow, index, 1)
    }

    fn run(flow: &Flow, index: usize, attempt: u32) -> Step<'_> {
        Step::Run {
            stage: &flow.stages[index],
            attempt,
        }
    }

    fn halted(stage: &str, reason: HaltReason) -> Step<'static> {
        Step::Halted {
            failed_stage: stage.into(),
            reason,
        }
    }

    #[test]
    fn a_linear_log_of_passes_advances_one_stage_per_row() {
        let flow = build_review_merge();

        assert_eq!(next_step(&flow, &[]), run(&flow, 0, 1));
        assert_eq!(next_step(&flow, &[passed(&flow, 0)]), run(&flow, 1, 1));
        assert_eq!(
            next_step(&flow, &[passed(&flow, 0), passed(&flow, 1)]),
            run(&flow, 2, 1)
        );
    }

    #[test]
    fn next_step_is_complete_only_when_the_cursor_runs_off_the_end() {
        let flow = build_review_merge();

        assert_eq!(
            next_step(
                &flow,
                &[passed(&flow, 0), passed(&flow, 1), passed(&flow, 2)]
            ),
            Step::Complete
        );
        assert_ne!(
            next_step(&flow, &[passed(&flow, 0), passed(&flow, 1)]),
            Step::Complete
        );
    }

    #[test]
    fn a_failed_row_halts_the_walk_and_later_stages_are_never_requested() {
        let flow = build_review_merge();

        // The fold stops at `review`, so a `merge` row could only be a
        // corrupt appendix — stages after a halting failure are never
        // requested, and evidence claiming otherwise is never believed.
        let evidence = [passed(&flow, 0), failed(&flow, 1), passed(&flow, 2)];

        assert_eq!(
            next_step(&flow, &evidence),
            halted("review", HaltReason::FailActionHalt)
        );
    }

    #[test]
    fn a_continue_fail_action_advances_past_the_failure() {
        let flow = flow_with(&[
            ("build", FailAction::Halt),
            ("lint", FailAction::Continue),
            ("merge", FailAction::Halt),
        ]);

        let evidence = [passed(&flow, 0), failed(&flow, 1)];
        assert_eq!(next_step(&flow, &evidence), run(&flow, 2, 1));

        let evidence = [passed(&flow, 0), failed(&flow, 1), passed(&flow, 2)];
        assert_eq!(next_step(&flow, &evidence), Step::Complete);
    }

    #[test]
    fn a_return_to_loop_converges_on_the_second_attempt() {
        let flow = flow_with(&[("build", FailAction::Halt), ("test", return_to("build", 2))]);

        // The failure sends the cursor back to `build`, which is now on its
        // second execution.
        let mut log = vec![pass(&flow, 0, 1), fail(&flow, 1, 1)];
        assert_eq!(next_step(&flow, &log), run(&flow, 0, 2));

        log.push(pass(&flow, 0, 2));
        assert_eq!(next_step(&flow, &log), run(&flow, 1, 2));

        log.push(pass(&flow, 1, 2));
        assert_eq!(next_step(&flow, &log), Step::Complete);
    }

    #[test]
    fn a_return_to_loop_halts_once_its_edge_budget_is_spent() {
        let flow = flow_with(&[("build", FailAction::Halt), ("test", return_to("build", 1))]);

        let log = vec![
            pass(&flow, 0, 1),
            fail(&flow, 1, 1),
            pass(&flow, 0, 2),
            fail(&flow, 1, 2),
        ];

        assert_eq!(
            next_step(&flow, &log),
            halted("test", HaltReason::ReturnBudgetExhausted)
        );
    }

    #[test]
    fn distinct_backward_edges_keep_independent_budgets() {
        // `test` and `review` each own one backward edge with one attempt.
        let flow = flow_with(&[
            ("build", FailAction::Halt),
            ("test", return_to("build", 1)),
            ("review", return_to("test", 1)),
        ]);

        // `test`'s edge is spent, but that says nothing about `review`'s:
        // its own failure still jumps, re-entering `test` for a third time.
        let mut log = vec![
            pass(&flow, 0, 1),
            fail(&flow, 1, 1),
            pass(&flow, 0, 2),
            pass(&flow, 1, 2),
            fail(&flow, 2, 1),
        ];
        assert_eq!(next_step(&flow, &log), run(&flow, 1, 3));

        // And the reverse: `review`'s untouched budget never refills
        // `test`'s, which is still exhausted.
        log.push(fail(&flow, 1, 3));
        assert_eq!(
            next_step(&flow, &log),
            halted("test", HaltReason::ReturnBudgetExhausted)
        );
    }

    #[test]
    fn a_jump_supersedes_the_passes_recorded_inside_its_span() {
        let flow = flow_with(&[
            ("build", FailAction::Halt),
            ("lint", FailAction::Halt),
            ("test", return_to("build", 1)),
        ]);

        // `lint` passed before the jump, but the jump puts that row behind
        // the fold: the span re-runs whole, so `lint` is requested again.
        let mut log = vec![pass(&flow, 0, 1), pass(&flow, 1, 1), fail(&flow, 2, 1)];
        assert_eq!(next_step(&flow, &log), run(&flow, 0, 2));

        log.push(pass(&flow, 0, 2));
        assert_eq!(next_step(&flow, &log), run(&flow, 1, 2));
    }

    /// A log that could not have been produced by any replay of this flow is
    /// never reinterpreted into one that could.
    #[test]
    fn a_row_the_cursor_did_not_expect_is_a_corrupt_log() {
        let flow = build_review_merge();

        // A row for a stage the cursor is not standing on.
        assert_eq!(
            next_step(&flow, &[passed(&flow, 1)]),
            halted("build", HaltReason::CorruptLog)
        );

        // The same row twice: the second arrives with the cursor already
        // past it.
        assert_eq!(
            next_step(&flow, &[passed(&flow, 0), passed(&flow, 0)]),
            halted("review", HaltReason::CorruptLog)
        );

        // The right stage on the wrong attempt.
        assert_eq!(
            next_step(&flow, &[pass(&flow, 0, 2)]),
            halted("build", HaltReason::CorruptLog)
        );

        // A row appended after the walk already ran off the end.
        assert_eq!(
            next_step(
                &flow,
                &[
                    passed(&flow, 0),
                    passed(&flow, 1),
                    passed(&flow, 2),
                    pass(&flow, 2, 2),
                ]
            ),
            halted("merge", HaltReason::CorruptLog)
        );
    }

    /// `parse` guarantees a `return_to` names an earlier stage, but a flow
    /// recovered from a snapshot has not been through it. An edge with
    /// nowhere to land is a history the fold cannot re-derive, not a licence
    /// to pick a landing site.
    #[test]
    fn a_return_to_with_no_such_stage_is_a_corrupt_log() {
        let flow = flow_with(&[
            ("build", FailAction::Halt),
            ("test", return_to("nowhere", 1)),
        ]);

        assert_eq!(
            next_step(&flow, &[passed(&flow, 0), failed(&flow, 1)]),
            halted("test", HaltReason::CorruptLog)
        );
    }

    /// A re-entered stage must be able to say *why*, and the answer is a row
    /// in the log rather than anything the driver remembers.
    #[test]
    fn the_return_trigger_is_the_failure_the_walk_jumped_on() {
        let flow = flow_with(&[
            ("build", FailAction::Halt),
            ("lint", FailAction::Halt),
            ("test", return_to("build", 2)),
        ]);

        // Nothing has jumped yet, so nothing triggered a re-entry.
        assert_eq!(return_trigger(&flow, &[]), None);
        assert_eq!(return_trigger(&flow, &[pass(&flow, 0, 1)]), None);

        // `lint` fails with a halting edge: a failure, but not a jump.
        assert_eq!(
            return_trigger(&flow, &[pass(&flow, 0, 1), fail(&flow, 1, 1)]),
            None
        );

        let mut log = vec![pass(&flow, 0, 1), pass(&flow, 1, 1), fail(&flow, 2, 1)];
        assert_eq!(return_trigger(&flow, &log), Some((2, 1)));

        // The whole span re-runs, and every stage inside it is re-entered
        // because of the same failure.
        log.push(pass(&flow, 0, 2));
        assert_eq!(return_trigger(&flow, &log), Some((2, 1)));

        // A second jump supersedes the first.
        log.extend([pass(&flow, 1, 2), fail(&flow, 2, 2)]);
        assert_eq!(return_trigger(&flow, &log), Some((2, 2)));
    }

    #[test]
    fn replaying_an_identical_log_yields_an_identical_step() {
        let flow = flow_with(&[("build", FailAction::Halt), ("test", return_to("build", 2))]);
        let log = [
            pass(&flow, 0, 1),
            fail(&flow, 1, 1),
            pass(&flow, 0, 2),
            fail(&flow, 1, 2),
        ];

        assert_eq!(next_step(&flow, &log), next_step(&flow, &log));
        assert_eq!(next_step(&flow, &log), run(&flow, 0, 3));
    }

    #[test]
    fn only_reported_policy_consults_reported_verdicts() {
        assert_eq!(
            resolve_verdict(&Check::None, Verdict::Pass, None),
            (Verdict::Pass, VerdictSource::ExitCode, None)
        );

        let reported = Reported {
            verdict: Verdict::Fail,
            reason: Some("changes requested".into()),
        };
        assert_eq!(
            resolve_verdict(&Check::Reported, Verdict::Pass, Some(reported)),
            (
                Verdict::Fail,
                VerdictSource::Reported,
                Some("changes requested".into())
            )
        );
    }

    #[test]
    fn non_reported_policies_ignore_reports() {
        let reported = Reported {
            verdict: Verdict::Pass,
            reason: Some("looks fine to me".into()),
        };

        assert_eq!(
            resolve_verdict(&commits(), Verdict::Fail, Some(reported)),
            (Verdict::Fail, VerdictSource::ExitCode, None)
        );
    }

    #[test]
    fn missing_report_is_a_failed_reported_verdict() {
        assert_eq!(
            resolve_verdict(&Check::Reported, Verdict::Pass, None),
            (
                Verdict::Fail,
                VerdictSource::Reported,
                Some("no verdict reported".into())
            )
        );
    }

    // ---- panels ---------------------------------------------------------

    fn panel_yaml(reviewers: &str, require: &str) -> String {
        format!(
            "- name: build\n  action: agent\n  result_check:\n    panel:\n      prompt: prompts/review.md\n      reviewers: {reviewers}\n{require}"
        )
    }

    fn panel_of(seats: usize, quorum: u32) -> Panel {
        Panel {
            prompt: "prompts/review.md".into(),
            reviewers: (0..seats)
                .map(|seat| Reviewer {
                    target: format!("target{seat}"),
                    model: None,
                    effort: None,
                })
                .collect(),
            quorum,
        }
    }

    fn report(verdict: Verdict) -> Option<ReviewerReport> {
        Some(ReviewerReport {
            verdict,
            confidence: Some(Confidence::Medium),
            reason: "considered".into(),
        })
    }

    #[test]
    fn a_panel_parses_with_its_seats_and_quorum() {
        let flow = parse(
            "example",
            &panel_yaml(
                "[{ target: claude }, { target: codex, model: gpt, effort: high }]",
                "      require: { quorum: 1 }\n",
            ),
        )
        .unwrap();

        assert_eq!(
            flow.stages[0].result_check,
            Check::Panel(Panel {
                prompt: "prompts/review.md".into(),
                reviewers: vec![
                    Reviewer {
                        target: "claude".into(),
                        model: None,
                        effort: None,
                    },
                    Reviewer {
                        target: "codex".into(),
                        model: Some("gpt".into()),
                        effort: Some("high".into()),
                    },
                ],
                quorum: 1,
            })
        );
    }

    /// A rule nobody wrote down must not silently be the most permissive one
    /// it could have been.
    #[test]
    fn an_unstated_quorum_is_unanimity() {
        let flow = parse(
            "example",
            &panel_yaml("[{ target: a }, { target: b }, { target: c }]", ""),
        )
        .unwrap();

        let Check::Panel(panel) = &flow.stages[0].result_check else {
            panic!("expected a panel");
        };
        assert_eq!(panel.quorum, 3);
    }

    #[test]
    fn a_panel_survives_a_snapshot_round_trip() {
        let flow = parse(
            "example",
            &panel_yaml(
                "[{ target: claude }, { target: codex }]",
                "      require: { quorum: 2 }\n",
            ),
        )
        .unwrap();

        let snapshot = serde_json::to_string(&flow).unwrap();
        assert_eq!(serde_json::from_str::<Flow>(&snapshot).unwrap(), flow);
    }

    #[test]
    fn a_panel_must_seat_between_two_and_five_reviewers() {
        for reviewers in [
            "[]",
            "[{ target: a }]",
            "[{target: a}, {target: b}, {target: c}, {target: d}, {target: e}, {target: f}]",
        ] {
            let error = error(&panel_yaml(reviewers, ""));
            assert!(
                error.contains("panel must define between 2 and 5 reviewers"),
                "{error}"
            );
        }
    }

    #[test]
    fn a_panel_quorum_must_fit_its_seats() {
        for quorum in ["0", "3"] {
            let error = error(&panel_yaml(
                "[{ target: a }, { target: b }]",
                &format!("      require: {{ quorum: {quorum} }}\n"),
            ));
            assert!(error.contains("stage `build`"), "{error}");
            assert!(
                error.contains("panel quorum must be between 1 and 2"),
                "{error}"
            );
        }
    }

    #[test]
    fn a_panel_prompt_must_be_a_relative_path_inside_the_sloop_directory() {
        let missing = error(
            "- name: build\n  action: agent\n  result_check:\n    panel:\n      reviewers: [{target: a}, {target: b}]\n",
        );
        assert!(
            missing.contains("panel must define a non-empty `prompt`"),
            "{missing}"
        );

        for prompt in ["/etc/passwd", "../../secrets.md"] {
            let escaping = error(&format!(
                "- name: build\n  action: agent\n  result_check:\n    panel:\n      prompt: {prompt}\n      reviewers: [{{target: a}}, {{target: b}}]\n",
            ));
            assert!(
                escaping.contains("must be a relative path under `.agents/sloop`"),
                "{escaping}"
            );
        }
    }

    /// A panel spawns one process per seat, and the execution cap exists so
    /// nobody discovers the total at runtime.
    #[test]
    fn panel_seats_count_towards_the_worst_case_execution_budget() {
        let flow = |seats: &str| {
            format!(
                "- name: build\n  action: agent\n  result_check:\n    panel:\n      prompt: prompts/review.md\n      reviewers: {seats}\n- {{ name: lint, action: {{ exec: ['true'] }} }}\n- {{ name: audit, action: {{ exec: ['true'] }} }}\n- {{ name: test, action: {{ exec: ['true'] }}, fail_action: {{ return_to: build, attempts: 3 }} }}\n",
            )
        };

        // Four stages whose panel seats three: the base walk costs seven
        // executions and each of the three re-runs costs the same seven, for
        // twenty-eight. Under the cap.
        let bounded = parse("example", &flow("[{target: a}, {target: b}, {target: c}]"));
        assert!(bounded.is_ok(), "{bounded:?}");

        // Widen the same panel to five seats and every one of those four
        // passes costs nine instead of seven — thirty-six in total, which the
        // cap refuses at parse time rather than at the thirty-third spawn.
        let error = error(&flow(
            "[{target: a}, {target: b}, {target: c}, {target: d}, {target: e}]",
        ));
        assert!(
            error.contains("at most 32 stages in the worst case"),
            "{error}"
        );
        assert!(error.contains("imply 36"), "{error}");
    }

    /// The whole aggregation, enumerated. Three states per seat — `Pass`,
    /// `Fail`, and the silence that counts as a `Fail` — across every quorum a
    /// two- and three-seat panel can name. Nothing here touches an LLM, a
    /// clock, or the store, which is the point: the opinions are untrusted and
    /// the procedure over them is not.
    #[test]
    fn aggregation_is_a_pass_count_against_the_quorum() {
        let states = [report(Verdict::Pass), report(Verdict::Fail), None];
        for seats in [2usize, 3] {
            for quorum in 1..=seats as u32 {
                let panel = panel_of(seats, quorum);
                // Every assignment of the three states to the seats.
                for combination in 0..states.len().pow(seats as u32) {
                    let reported: Vec<Option<ReviewerReport>> = (0..seats)
                        .map(|seat| states[combination / states.len().pow(seat as u32) % 3].clone())
                        .collect();
                    let passes = reported
                        .iter()
                        .filter(|report| {
                            report.as_ref().map(|report| report.verdict) == Some(Verdict::Pass)
                        })
                        .count();

                    let outcome = aggregate(&panel, &reported);

                    let expected = if passes as u32 >= quorum {
                        Verdict::Pass
                    } else {
                        Verdict::Fail
                    };
                    assert_eq!(
                        outcome.verdict, expected,
                        "seats {seats}, quorum {quorum}, reports {reported:?}"
                    );
                    // Every seat is accounted for, whether it spoke or not.
                    assert_eq!(outcome.reports.len(), seats);
                    assert_eq!(
                        outcome.reason,
                        format!("panel: {passes} of {seats} reviewers passed, quorum {quorum}")
                    );
                }
            }
        }
    }

    /// Silence is not an abstention. A seat nobody heard from has approved
    /// nothing, and says so in the words the rest of the system already uses
    /// for an unreported verdict.
    #[test]
    fn a_silent_reviewer_fills_its_seat_with_a_fail() {
        let panel = panel_of(3, 2);

        let outcome = aggregate(
            &panel,
            &[report(Verdict::Pass), None, report(Verdict::Pass)],
        );
        assert_eq!(outcome.verdict, Verdict::Pass);
        assert_eq!(outcome.reports[1].verdict, Verdict::Fail);
        assert_eq!(outcome.reports[1].confidence, None);
        assert_eq!(outcome.reports[1].reason, NO_VERDICT_REPORTED);

        // A stage abandoned before its last seats ran leaves a short slice,
        // which fills out the same way rather than shrinking the panel.
        let truncated = aggregate(&panel, &[report(Verdict::Pass)]);
        assert_eq!(truncated.verdict, Verdict::Fail);
        assert_eq!(truncated.reports.len(), 3);
    }

    /// Confidence rides along as evidence and is never weighted: three
    /// low-confidence passes beat two high-confidence fails, because the
    /// quorum says so and nothing else does.
    #[test]
    fn confidence_is_recorded_but_never_weighted() {
        let panel = panel_of(3, 2);
        let sure = |verdict, confidence| {
            Some(ReviewerReport {
                verdict,
                confidence: Some(confidence),
                reason: "considered".into(),
            })
        };

        let outcome = aggregate(
            &panel,
            &[
                sure(Verdict::Pass, Confidence::Low),
                sure(Verdict::Pass, Confidence::Low),
                sure(Verdict::Fail, Confidence::High),
            ],
        );

        assert_eq!(outcome.verdict, Verdict::Pass);
        assert_eq!(outcome.reports[2].confidence, Some(Confidence::High));
    }

    /// A panel is a check, never an action, and the merge stage keeps its
    /// standing prohibition on being judged at all.
    #[test]
    fn a_panel_may_not_judge_a_merge_stage() {
        let error = error(
            "- { name: build, action: agent }\n- name: merge\n  action: { builtin: merge }\n  result_check:\n    panel:\n      prompt: prompts/review.md\n      reviewers: [{target: a}, {target: b}]\n",
        );
        assert!(error.contains("must have `result_check: none`"), "{error}");
    }
}
