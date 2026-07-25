//! Flow definitions and the pure walk over them. Parsing turns a committed
//! YAML file into a validated `Flow`; `next_step` then replays the run's
//! ordered evidence log over that flow to derive the next stage to run or a
//! terminal reading. Neither half touches a clock, a process, or the store,
//! so policy can be tested without a daemon.

use std::collections::HashSet;

use serde::de::{Error as _, IgnoredAny};
use serde::{Deserialize, Deserializer, Serialize};

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
/// action's own exit, or a worker's report) that decides. The four verdict
/// policies of the old grammar are now configurations of this shape rather
/// than concepts of their own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
    /// Optional repair agent for non-agent stages. When the stage fails,
    /// this agent is spawned in the run worktree to fix the tree in place;
    /// the stage is then re-run and its own result check re-applied. The
    /// repair agent never produces the verdict.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_fail: Option<OnFail>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// A stage's optional repair configuration. It configures the repair worker
/// (prompt, attempt budget, and target/model/effort overrides) but can never
/// alter the stage's verdict policy, command, or ordering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnFail {
    /// The prompt handed to the repair agent.
    pub agent: String,
    /// How many repair-then-retry cycles are allowed per stage per run.
    pub attempts: u32,
    /// Agent target override; defaults to the ticket's target, then the
    /// configured default target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// Model override; defaults to the ticket's model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Effort override; defaults to the ticket's effort.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
}

/// The inclusive upper bound on `on_fail.attempts` and on a `return_to`
/// edge's attempt budget.
pub const MAX_ON_FAIL_ATTEMPTS: u32 = 3;

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

impl<'de> Deserialize<'de> for Stage {
    /// Reads both vocabularies. Snapshots written before the
    /// action/result_check split carry `kind`/`verdict` (including the
    /// long-deprecated `Build` spelling for an agent), and queued runs must
    /// recover from them unchanged; anything written since carries
    /// `action`/`result_check`/`fail_action`.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        /// The pre-split stage kind, kept only so old snapshots still read.
        #[derive(Deserialize)]
        enum SnapshotKind {
            #[serde(alias = "Build")]
            Agent,
            Merge,
            Exec {
                cmd: Vec<String>,
            },
        }

        /// The pre-split verdict policy, kept only so old snapshots still
        /// read.
        #[derive(Deserialize)]
        enum SnapshotVerdict {
            Exit,
            Commits,
            Check { cmd: Vec<String> },
            Reported,
        }

        #[derive(Deserialize)]
        struct SnapshotStage {
            name: String,
            #[serde(default)]
            action: Option<Actor>,
            #[serde(default)]
            result_check: Option<Check>,
            #[serde(default)]
            fail_action: Option<FailAction>,
            #[serde(default)]
            kind: Option<SnapshotKind>,
            #[serde(default)]
            verdict: Option<SnapshotVerdict>,
            #[serde(default)]
            ff_only: bool,
            #[serde(default)]
            on_fail: Option<OnFail>,
        }

        let stage = SnapshotStage::deserialize(deserializer)?;
        let action = match (stage.action, stage.kind) {
            (Some(action), _) => action,
            (None, Some(SnapshotKind::Agent)) => Actor::Agent,
            (None, Some(SnapshotKind::Merge)) => Actor::Builtin(Builtin::Merge),
            (None, Some(SnapshotKind::Exec { cmd })) => Actor::Exec { cmd },
            (None, None) => return Err(D::Error::missing_field("action")),
        };
        let result_check = match (stage.result_check, stage.verdict) {
            (Some(check), _) => check,
            (None, Some(SnapshotVerdict::Exit)) => Check::None,
            (None, Some(SnapshotVerdict::Commits)) => {
                Check::Actor(Actor::Builtin(Builtin::Commits))
            }
            (None, Some(SnapshotVerdict::Check { cmd })) => Check::Actor(Actor::Exec { cmd }),
            (None, Some(SnapshotVerdict::Reported)) => Check::Reported,
            (None, None) => default_check(&action),
        };
        Ok(Self {
            name: stage.name,
            action,
            result_check,
            fail_action: stage.fail_action.unwrap_or(FailAction::Halt),
            ff_only: stage.ff_only,
            on_fail: stage.on_fail,
        })
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
        let (action, ff_only) = parse_action(&raw.name, raw.action, raw.kind, raw.cmd)?;
        let result_check = parse_result_check(&raw.name, &action, raw.result_check, raw.verdict)?;
        let fail_action = parse_fail_action(&raw.name, raw.fail_action)?;
        validate_stage(&raw.name, &action, &result_check)?;
        let on_fail = match raw.on_fail {
            None => None,
            Some(_) if action == Actor::Agent => {
                return Err(format!(
                    "agent stage `{}` must not define `on_fail`",
                    raw.name
                ));
            }
            Some(on_fail) => Some(validate_on_fail(&raw.name, on_fail)?),
        };
        stages.push(Stage {
            name: raw.name,
            action,
            result_check,
            fail_action,
            ff_only,
            on_fail,
        });
    }

    validate_order(&stages)?;
    Ok(Flow {
        name: name.to_owned(),
        stages,
    })
}

/// Resolves a stage's action from either grammar, along with the `ff_only`
/// option the merge builtin alone accepts. The old `kind`/`cmd` pair is sugar
/// for the same `Actor`s the new `action` key names directly, so the two are
/// mutually exclusive rather than merged.
fn parse_action(
    stage: &str,
    action: Option<RawActor>,
    kind: Option<String>,
    cmd: Option<Vec<String>>,
) -> Result<(Actor, bool), String> {
    match (action, kind) {
        (Some(_), Some(_)) => Err(format!(
            "stage `{stage}` must not define both `action` and `kind`"
        )),
        (Some(action), None) => {
            if cmd.is_some() {
                return Err(format!(
                    "stage `{stage}` must not define `cmd` alongside `action`; \
                     write `action: {{ exec: [...] }}`"
                ));
            }
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
                    // The one action `ff_only` means anything to, so the one
                    // that does not refuse it.
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
        (None, Some(kind)) => match kind.as_str() {
            "agent" | "build" => {
                if cmd.is_some() {
                    return Err(format!("agent stage `{stage}` must not define `cmd`"));
                }
                Ok((Actor::Agent, false))
            }
            "merge" => {
                if cmd.is_some() {
                    return Err(format!("merge stage `{stage}` must not define `cmd`"));
                }
                Ok((Actor::Builtin(Builtin::Merge), false))
            }
            "sync" => {
                if cmd.is_some() {
                    return Err(format!("sync stage `{stage}` must not define `cmd`"));
                }
                Ok((Actor::Builtin(Builtin::Sync), false))
            }
            "exec" => {
                let cmd = cmd.unwrap_or_default();
                if cmd.is_empty() {
                    return Err(format!(
                        "exec stage `{stage}` must define a non-empty `cmd`"
                    ));
                }
                Ok((Actor::Exec { cmd }, false))
            }
            kind => Err(format!("stage `{stage}` has unknown kind `{kind}`")),
        },
        (None, None) => Err(format!("stage `{stage}` must define an `action`")),
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

/// Resolves a stage's result check from either grammar. Each of the four old
/// verdict policies names one configuration of `Check`.
fn parse_result_check(
    stage: &str,
    action: &Actor,
    result_check: Option<RawCheck>,
    verdict: Option<RawVerdict>,
) -> Result<Check, String> {
    match (result_check, verdict) {
        (Some(_), Some(_)) => Err(format!(
            "stage `{stage}` must not define both `result_check` and `verdict`"
        )),
        (Some(check), None) => match check {
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
            RawCheck::Name(name) => match name.as_str() {
                "none" => Ok(Check::None),
                "reported" => Ok(Check::Reported),
                other => Err(format!(
                    "stage `{stage}` has unknown result_check `{other}`"
                )),
            },
        },
        // A merge's own outcome is its verdict, so the old grammar refuses
        // the key outright rather than accepting a redundant `exit`.
        (None, Some(_)) if *action == Actor::Builtin(Builtin::Merge) => {
            Err(format!("merge stage `{stage}` must not define `verdict`"))
        }
        (None, Some(RawVerdict::Name(name))) => match name.as_str() {
            "exit" => Ok(Check::None),
            "commits" => Ok(Check::Actor(Actor::Builtin(Builtin::Commits))),
            "reported" => Ok(Check::Reported),
            _ => Err(format!(
                "stage `{stage}` has unknown verdict policy `{name}`"
            )),
        },
        (None, Some(RawVerdict::Check { check })) => {
            if check.is_empty() {
                return Err(format!(
                    "stage `{stage}` check verdict must define a non-empty command"
                ));
            }
            Ok(Check::Actor(Actor::Exec { cmd: check }))
        }
        (None, None) => Ok(default_check(action)),
    }
}

fn parse_fail_action(stage: &str, raw: Option<RawFailAction>) -> Result<FailAction, String> {
    match raw {
        None => Ok(FailAction::Halt),
        Some(RawFailAction::ReturnTo {
            return_to,
            attempts,
        }) => {
            let attempts = attempts.unwrap_or(1);
            if attempts == 0 || attempts > MAX_ON_FAIL_ATTEMPTS {
                return Err(format!(
                    "stage `{stage}` return_to attempts must be between 1 and {MAX_ON_FAIL_ATTEMPTS}"
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

/// Checks the action/check pairing, whichever grammar produced it.
fn validate_stage(stage: &str, action: &Actor, result_check: &Check) -> Result<(), String> {
    if *action == Actor::Agent && *result_check == Check::None {
        return Err(format!(
            "stage `{stage}`: agentic actions require a result_check or reported"
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

/// Validates an `on_fail` block's own shape. Target existence is checked
/// later, where the configured agent targets are known (see `config.rs`).
fn validate_on_fail(stage: &str, raw: RawOnFail) -> Result<OnFail, String> {
    if raw.agent.trim().is_empty() {
        return Err(format!(
            "stage `{stage}` on_fail must define a non-empty `agent` prompt"
        ));
    }
    let attempts = raw.attempts.unwrap_or(1);
    if attempts == 0 || attempts > MAX_ON_FAIL_ATTEMPTS {
        return Err(format!(
            "stage `{stage}` on_fail attempts must be between 1 and {MAX_ON_FAIL_ATTEMPTS}"
        ));
    }
    Ok(OnFail {
        agent: raw.agent,
        attempts,
        target: raw.target,
        model: raw.model,
        effort: raw.effort,
    })
}

pub(crate) fn built_in_default() -> Flow {
    let stages = vec![
        Stage {
            name: "build".into(),
            action: Actor::Agent,
            result_check: Check::Actor(Actor::Builtin(Builtin::Commits)),
            fail_action: FailAction::Halt,
            ff_only: false,
            on_fail: None,
        },
        Stage {
            name: "merge".into(),
            action: Actor::Builtin(Builtin::Merge),
            result_check: Check::None,
            fail_action: FailAction::Halt,
            ff_only: false,
            on_fail: None,
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
    let mut executions = stages.len() as u64;
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
        let span = (index - target_index + 1) as u64;
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

/// A stage exactly as written. Both grammars are optional here and sorted
/// out by `parse_action` / `parse_result_check`, so a file mixing them gets
/// an error naming the conflict rather than a serde message about a shape
/// nobody wrote.
#[derive(Debug, Deserialize)]
struct RawStage {
    name: String,
    action: Option<RawActor>,
    result_check: Option<RawCheck>,
    fail_action: Option<RawFailAction>,
    kind: Option<String>,
    cmd: Option<Vec<String>>,
    verdict: Option<RawVerdict>,
    on_fail: Option<RawOnFail>,
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
/// `{ builtin: commits }`.
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOnFail {
    agent: String,
    attempts: Option<u32>,
    target: Option<String>,
    model: Option<String>,
    effort: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawVerdict {
    Name(String),
    Check { check: Vec<String> },
}

#[cfg(test)]
mod tests {
    use super::{
        Actor, Builtin, Check, FailAction, Flow, HaltReason, Reported, Stage, StageEvidence, Step,
        Verdict, VerdictSource, next_step, parse, resolve_verdict, return_trigger,
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
            "stages:\n  - name: build\n    kind: agent\n  - name: test\n    kind: exec\n    cmd: [cargo, test]\n    verdict: { check: [cargo, clippy] }\n  - name: merge\n    kind: merge\n",
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
                        on_fail: None,
                    },
                    Stage {
                        name: "test".into(),
                        action: Actor::Exec {
                            cmd: vec!["cargo".into(), "test".into()],
                        },
                        result_check: exec_check(&["cargo", "clippy"]),
                        fail_action: FailAction::Halt,
                        ff_only: false,
                        on_fail: None,
                    },
                    Stage {
                        name: "merge".into(),
                        action: Actor::Builtin(Builtin::Merge),
                        result_check: Check::None,
                        fail_action: FailAction::Halt,
                        ff_only: false,
                        on_fail: None,
                    },
                ],
            }
        );
    }

    #[test]
    fn the_new_grammar_names_the_same_stages_directly() {
        let flow = parse(
            "release",
            "stages:\n  - name: build\n    action: { agent: ignored }\n    result_check: { builtin: commits }\n  - name: test\n    action: { exec: [cargo, test] }\n    result_check: { exec: [cargo, clippy] }\n    fail_action: fail\n  - name: merge\n    action: { builtin: merge }\n    result_check: none\n",
        )
        .unwrap();
        let sugared = parse(
            "release",
            "stages:\n  - name: build\n    kind: agent\n  - name: test\n    kind: exec\n    cmd: [cargo, test]\n    verdict: { check: [cargo, clippy] }\n  - name: merge\n    kind: merge\n",
        )
        .unwrap();

        // The whole point of the old grammar being sugar: both spellings
        // produce byte-for-byte the same flow.
        assert_eq!(flow, sugared);
    }

    #[test]
    fn a_bare_agent_action_needs_no_payload() {
        let flow = parse("example", "- { name: build, action: agent }\n").unwrap();
        assert_eq!(flow.stages[0].action, Actor::Agent);
        assert_eq!(flow.stages[0].result_check, commits());
    }

    #[test]
    fn new_grammar_result_checks_parse() {
        let flow = parse(
            "example",
            "- { name: build, action: agent, result_check: reported }\n- { name: test, action: { exec: ['true'] }, result_check: none }\n- { name: gate, action: { exec: ['true'] }, result_check: { builtin: commits } }\n",
        )
        .unwrap();
        assert_eq!(flow.stages[0].result_check, Check::Reported);
        assert_eq!(flow.stages[1].result_check, Check::None);
        assert_eq!(flow.stages[2].result_check, commits());
    }

    #[test]
    fn build_is_a_deprecated_alias_for_agent() {
        let flow = parse("example", "- { name: build, kind: build }\n").unwrap();
        assert_eq!(flow.stages[0].action, Actor::Agent);
        assert_eq!(flow.stages[0].result_check, commits());
    }

    #[test]
    fn old_build_snapshots_deserialize_with_the_agent_default() {
        let flow: Flow = serde_json::from_str(
            r#"{"name":"example","stages":[{"name":"build","kind":"Build"}]}"#,
        )
        .unwrap();
        assert_eq!(flow.stages[0].action, Actor::Agent);
        assert_eq!(flow.stages[0].result_check, commits());
        assert_eq!(flow.stages[0].fail_action, FailAction::Halt);
    }

    /// Every old `kind`/`verdict` pairing a snapshot can carry maps onto the
    /// new representation, so runs queued before the split still recover.
    #[test]
    fn old_snapshots_map_onto_the_new_representation() {
        let snapshot = r#"{"name":"example","stages":[
            {"name":"a","kind":"Agent","verdict":"Commits"},
            {"name":"b","kind":{"Exec":{"cmd":["true"]}},"verdict":"Exit"},
            {"name":"c","kind":{"Exec":{"cmd":["true"]}},"verdict":{"Check":{"cmd":["cargo","fmt"]}}},
            {"name":"d","kind":{"Exec":{"cmd":["true"]}},"verdict":"Reported"},
            {"name":"e","kind":"Merge"}
        ]}"#;
        let flow: Flow = serde_json::from_str(snapshot).unwrap();

        let actions: Vec<&Actor> = flow.stages.iter().map(|stage| &stage.action).collect();
        assert_eq!(
            actions,
            vec![
                &Actor::Agent,
                &Actor::Exec {
                    cmd: vec!["true".into()]
                },
                &Actor::Exec {
                    cmd: vec!["true".into()]
                },
                &Actor::Exec {
                    cmd: vec!["true".into()]
                },
                &Actor::Builtin(Builtin::Merge),
            ]
        );
        let checks: Vec<&Check> = flow
            .stages
            .iter()
            .map(|stage| &stage.result_check)
            .collect();
        assert_eq!(
            checks,
            vec![
                &commits(),
                &Check::None,
                &exec_check(&["cargo", "fmt"]),
                &Check::Reported,
                &Check::None,
            ]
        );
        assert!(
            flow.stages
                .iter()
                .all(|stage| stage.fail_action == FailAction::Halt)
        );
    }

    /// Snapshots are written in the new vocabulary and read back identically.
    #[test]
    fn new_snapshots_round_trip_through_the_new_vocabulary() {
        let flow = parse(
            "example",
            "- { name: build, action: agent, result_check: reported }\n- { name: test, action: { exec: [cargo, test] }, result_check: { exec: [cargo, fmt] } }\n- { name: merge, action: { builtin: merge }, result_check: none }\n",
        )
        .unwrap();
        let snapshot = serde_json::to_string(&flow).unwrap();

        assert!(snapshot.contains("result_check"), "{snapshot}");
        assert!(!snapshot.contains("\"kind\""), "{snapshot}");
        assert!(!snapshot.contains("\"verdict\""), "{snapshot}");
        assert_eq!(serde_json::from_str::<Flow>(&snapshot).unwrap(), flow);
    }

    #[test]
    fn verdict_policies_and_defaults_parse() {
        let flow = parse(
            "example",
            "- { name: build, kind: agent, verdict: reported }\n- { name: test, kind: exec, cmd: ['true'], verdict: commits }\n- { name: review, kind: exec, cmd: ['true'], verdict: exit }\n",
        )
        .unwrap();
        assert_eq!(flow.stages[0].result_check, Check::Reported);
        assert_eq!(flow.stages[1].result_check, commits());
        assert_eq!(flow.stages[2].result_check, Check::None);

        let defaults = parse(
            "example",
            "- { name: build, kind: agent }\n- { name: test, kind: exec, cmd: ['true'] }\n- { name: merge, kind: merge }\n",
        )
        .unwrap();
        assert_eq!(defaults.stages[0].result_check, commits());
        assert_eq!(defaults.stages[1].result_check, Check::None);
        assert_eq!(defaults.stages[2].result_check, Check::None);
    }

    #[test]
    fn the_two_grammars_may_not_be_mixed_on_one_stage() {
        let action_and_kind = error("- { name: build, action: agent, kind: agent }\n");
        assert!(
            action_and_kind.contains("both `action` and `kind`"),
            "{action_and_kind}"
        );

        let both_checks =
            error("- { name: build, action: agent, result_check: reported, verdict: reported }\n");
        assert!(
            both_checks.contains("both `result_check` and `verdict`"),
            "{both_checks}"
        );

        let stray_cmd = error("- { name: build, action: { exec: ['true'] }, cmd: ['true'] }\n");
        assert!(stray_cmd.contains("must not define `cmd`"), "{stray_cmd}");
    }

    #[test]
    fn an_agent_action_may_not_go_unjudged() {
        for yaml in [
            "- { name: build, kind: agent, verdict: exit }\n",
            "- { name: build, action: agent, result_check: none }\n",
        ] {
            let error = error(yaml);
            assert!(error.contains("stage `build`"), "{error}");
            assert!(
                error.contains("agentic actions require a result_check or reported"),
                "{error}"
            );
        }
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
    fn the_sync_builtin_parses_in_both_grammars() {
        let new = parse(
            "example",
            "- { name: build, action: agent }\n- { name: sync, action: { builtin: sync } }\n",
        )
        .unwrap();
        assert_eq!(new.stages[1].action, Actor::Builtin(Builtin::Sync));
        assert_eq!(new.stages[1].result_check, Check::None);

        let sugared = parse(
            "example",
            "- { name: build, kind: agent }\n- { name: sync, kind: sync }\n",
        )
        .unwrap();
        assert_eq!(new, sugared);
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

    /// A queued run must recover the mode it was admitted with, and a flow
    /// snapshotted before the option existed must still read as the merge
    /// policy it was written for.
    #[test]
    fn ff_only_survives_a_snapshot_round_trip_and_defaults_on_old_ones() {
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

        let old: Flow = serde_json::from_str(
            r#"{"name":"example","stages":[{"name":"merge","kind":"Merge"}]}"#,
        )
        .unwrap();
        assert!(!old.stages[0].ff_only);
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
    fn unknown_new_grammar_words_are_rejected() {
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
    fn empty_new_grammar_commands_are_rejected() {
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
    fn on_fail_parses_with_defaults_and_overrides() {
        let flow = parse(
            "example",
            "- { name: build, kind: agent }\n- name: test\n  kind: exec\n  cmd: [cargo, test]\n  on_fail:\n    agent: fix the tests\n- name: merge\n  kind: merge\n  on_fail:\n    agent: integrate the default branch\n    attempts: 2\n    target: claude\n    model: haiku\n    effort: low\n",
        )
        .unwrap();

        let test = flow.stages[1].on_fail.as_ref().unwrap();
        assert_eq!(test.agent, "fix the tests");
        assert_eq!(test.attempts, 1);
        assert_eq!(test.target, None);

        let merge = flow.stages[2].on_fail.as_ref().unwrap();
        assert_eq!(merge.attempts, 2);
        assert_eq!(merge.target.as_deref(), Some("claude"));
        assert_eq!(merge.model.as_deref(), Some("haiku"));
        assert_eq!(merge.effort.as_deref(), Some("low"));
    }

    #[test]
    fn on_fail_survives_a_snapshot_round_trip() {
        let flow = parse(
            "example",
            "- { name: build, kind: agent }\n- name: test\n  kind: exec\n  cmd: [cargo, test]\n  on_fail:\n    agent: fix the tests\n    attempts: 3\n    model: haiku\n",
        )
        .unwrap();
        let snapshot = serde_json::to_string(&flow).unwrap();
        let restored: Flow = serde_json::from_str(&snapshot).unwrap();
        assert_eq!(flow, restored);
        assert_eq!(restored.stages[1].on_fail.as_ref().unwrap().attempts, 3);
    }

    #[test]
    fn on_fail_is_rejected_on_agent_stages() {
        let error = error(
            "- name: build\n  kind: agent\n  on_fail:\n    agent: patch it\n- { name: merge, kind: merge }\n",
        );
        assert!(error.contains("agent stage `build`"), "{error}");
        assert!(error.contains("must not define `on_fail`"), "{error}");
    }

    #[test]
    fn on_fail_rejects_an_empty_prompt() {
        let error = error(
            "- { name: build, kind: agent }\n- name: test\n  kind: exec\n  cmd: ['true']\n  on_fail:\n    agent: '   '\n",
        );
        assert!(error.contains("stage `test`"), "{error}");
        assert!(error.contains("non-empty `agent` prompt"), "{error}");
    }

    #[test]
    fn on_fail_rejects_out_of_range_attempts() {
        for attempts in ["0", "4"] {
            let error = error(&format!(
                "- {{ name: build, kind: agent }}\n- name: test\n  kind: exec\n  cmd: ['true']\n  on_fail:\n    agent: fix it\n    attempts: {attempts}\n",
            ));
            assert!(error.contains("stage `test`"), "{error}");
            assert!(
                error.contains("attempts must be between 1 and 3"),
                "{error}"
            );
        }
    }

    #[test]
    fn merge_stages_reject_verdict_policies() {
        let error = error(
            "- { name: build, kind: agent }\n- { name: merge, kind: merge, verdict: exit }\n",
        );
        assert!(error.contains("must not define `verdict`"), "{error}");
    }

    #[test]
    fn unknown_kinds_are_rejected() {
        let error = error("- { name: build, kind: build }\n- { name: deploy, kind: magic }\n");
        assert!(error.contains("unknown kind `magic`"), "{error}");
    }

    #[test]
    fn duplicate_stage_names_are_rejected() {
        let error = error("- { name: build, kind: build }\n- { name: build, kind: merge }\n");
        assert!(error.contains("duplicate stage name `build`"), "{error}");
    }

    /// One driver walks every stage, so nothing about a flow's shape depends
    /// on where its agent actions sit or how many there are.
    #[test]
    fn agent_actions_are_legal_in_any_position_and_any_number() {
        let leading_exec = parse(
            "example",
            "- { name: check, kind: exec, cmd: ['true'] }\n- { name: build, kind: agent }\n",
        )
        .unwrap();
        assert_eq!(leading_exec.stages[1].action, Actor::Agent);

        let two_agents = parse(
            "example",
            "- { name: build, kind: agent }\n- { name: review, kind: agent, verdict: reported }\n",
        )
        .unwrap();
        assert_eq!(two_agents.stages[0].action, Actor::Agent);
        assert_eq!(two_agents.stages[1].action, Actor::Agent);

        let no_agent = parse("example", "- { name: check, kind: exec, cmd: ['true'] }\n").unwrap();
        assert_eq!(no_agent.stages.len(), 1);
    }

    #[test]
    fn at_most_one_merge_stage_is_allowed() {
        let error = error(
            "- { name: build, kind: build }\n- { name: merge-one, kind: merge }\n- { name: merge-two, kind: merge }\n",
        );
        assert!(error.contains("at most one merge stage"), "{error}");
    }

    #[test]
    fn merge_stage_must_be_last() {
        let error = error(
            "- { name: build, kind: build }\n- { name: merge, kind: merge }\n- { name: check, kind: exec, cmd: ['true'] }\n",
        );
        assert!(error.contains("merge stage must be last"), "{error}");
    }

    #[test]
    fn exec_stage_command_must_be_nonempty() {
        for yaml in [
            "- { name: build, kind: build }\n- { name: check, kind: exec }\n",
            "- { name: build, kind: build }\n- { name: check, kind: exec, cmd: [] }\n",
        ] {
            let error = error(yaml);
            assert!(error.contains("non-empty `cmd`"), "{error}");
        }
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
                    on_fail: None,
                },
                Stage {
                    name: "review".into(),
                    action: Actor::Exec {
                        cmd: vec!["true".into()],
                    },
                    result_check: Check::None,
                    fail_action: FailAction::Halt,
                    ff_only: false,
                    on_fail: None,
                },
                Stage {
                    name: "merge".into(),
                    action: Actor::Builtin(Builtin::Merge),
                    result_check: Check::None,
                    fail_action: FailAction::Halt,
                    ff_only: false,
                    on_fail: None,
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
                    on_fail: None,
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
}
