//! Flow definitions and the pure walk over them. Parsing turns a committed
//! YAML file into a validated `Flow`; `next_step` then turns a flow and the
//! evidence gathered so far into the next stage to run or a terminal
//! reading. Neither half touches a clock, a process, or the store, so
//! policy can be tested without a daemon.

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
    /// Optional repair agent for non-agent stages. When the stage fails,
    /// this agent is spawned in the run worktree to fix the tree in place;
    /// the stage is then re-run and its own result check re-applied. The
    /// repair agent never produces the verdict.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_fail: Option<OnFail>,
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
    /// Stop the walk here. The only variant the walk supports today.
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
        let action = parse_action(&raw.name, raw.action, raw.kind, raw.cmd)?;
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
            on_fail,
        });
    }

    validate_order(&stages)?;
    Ok(Flow {
        name: name.to_owned(),
        stages,
    })
}

/// Resolves a stage's action from either grammar. The old `kind`/`cmd` pair
/// is sugar for the same `Actor`s the new `action` key names directly, so
/// the two are mutually exclusive rather than merged.
fn parse_action(
    stage: &str,
    action: Option<RawActor>,
    kind: Option<String>,
    cmd: Option<Vec<String>>,
) -> Result<Actor, String> {
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
                RawActor::Agent { .. } => Ok(Actor::Agent),
                RawActor::Exec { exec } => {
                    if exec.is_empty() {
                        return Err(format!(
                            "stage `{stage}` exec action must define a non-empty command"
                        ));
                    }
                    Ok(Actor::Exec { cmd: exec })
                }
                RawActor::Builtin { builtin } => match builtin.as_str() {
                    "merge" => Ok(Actor::Builtin(Builtin::Merge)),
                    "commits" => Err(format!(
                        "stage `{stage}` builtin `commits` is a result_check, not an action"
                    )),
                    other => Err(format!("stage `{stage}` has unknown builtin `{other}`")),
                },
                RawActor::Name(name) if name == "agent" => Ok(Actor::Agent),
                RawActor::Name(name) => Err(format!("stage `{stage}` has unknown action `{name}`")),
            }
        }
        (None, Some(kind)) => match kind.as_str() {
            "agent" | "build" => {
                if cmd.is_some() {
                    return Err(format!("agent stage `{stage}` must not define `cmd`"));
                }
                Ok(Actor::Agent)
            }
            "merge" => {
                if cmd.is_some() {
                    return Err(format!("merge stage `{stage}` must not define `cmd`"));
                }
                Ok(Actor::Builtin(Builtin::Merge))
            }
            "exec" => {
                let cmd = cmd.unwrap_or_default();
                if cmd.is_empty() {
                    return Err(format!(
                        "exec stage `{stage}` must define a non-empty `cmd`"
                    ));
                }
                Ok(Actor::Exec { cmd })
            }
            kind => Err(format!("stage `{stage}` has unknown kind `{kind}`")),
        },
        (None, None) => Err(format!("stage `{stage}` must define an `action`")),
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
            RawCheck::Builtin { builtin } => match builtin.as_str() {
                "commits" => Ok(Check::Actor(Actor::Builtin(Builtin::Commits))),
                "merge" => Err(format!(
                    "stage `{stage}` result_check may not be the `merge` builtin"
                )),
                other => Err(format!("stage `{stage}` has unknown builtin `{other}`")),
            },
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
    if *action == Actor::Builtin(Builtin::Merge) && *result_check != Check::None {
        return Err(format!(
            "merge stage `{stage}` must have `result_check: none`"
        ));
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
            on_fail: None,
        },
        Stage {
            name: "merge".into(),
            action: Actor::Builtin(Builtin::Merge),
            result_check: Check::None,
            fail_action: FailAction::Halt,
            on_fail: None,
        },
    ];
    Flow {
        name: DEFAULT_FLOW_NAME.into(),
        stages,
    }
}

fn validate_order(stages: &[Stage]) -> Result<(), String> {
    if !stages
        .first()
        .is_some_and(|stage| stage.action == Actor::Agent)
    {
        return Err("the first stage must be an agent stage".into());
    }
    let agent_count = stages
        .iter()
        .filter(|stage| stage.action == Actor::Agent)
        .count();
    if agent_count > 1 {
        return Err(
            "only the first stage may be an agent stage; additional agent stages require runner support"
                .into(),
        );
    }

    let merge = Actor::Builtin(Builtin::Merge);
    let merge_count = stages.iter().filter(|stage| stage.action == merge).count();
    if merge_count > 1 {
        return Err(format!(
            "flow may contain at most one merge stage; found {merge_count}"
        ));
    }
    if merge_count == 1 && stages.last().map(|stage| &stage.action) != Some(&merge) {
        return Err("merge stage must be last".into());
    }

    validate_return_edges(stages)?;

    // The walk is still linear halt-on-fail (see `next_step`), so a flow may
    // not bind an edge the walk cannot honour. Parsing the vocabulary now
    // and refusing it here keeps flow files from depending on behaviour that
    // has not landed.
    for stage in stages {
        match &stage.fail_action {
            FailAction::Halt => {}
            FailAction::Continue => {
                return Err(format!(
                    "stage `{}` fail_action `continue` is not yet supported",
                    stage.name
                ));
            }
            FailAction::ReturnTo { .. } => {
                return Err(format!(
                    "stage `{}` fail_action `return_to` is not yet supported",
                    stage.name
                ));
            }
        }
    }
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

/// One stage's recorded result. Rows persist as they are produced, so a
/// daemon crash mid-flow resumes idempotently at the first stage without a
/// row: `next_step` re-derives the same answer from the same rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageEvidence {
    pub stage: String,
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

/// What the walk does next, given a flow and its evidence so far.
#[derive(Debug, PartialEq, Eq)]
pub enum Step<'a> {
    /// The first stage without an evidence row; every row before it is
    /// `Pass`.
    Run(&'a Stage),
    /// Some row is `Fail`; the walk stops there. Stages after it are never
    /// requested.
    Halted { failed_stage: String },
    /// Every stage has a `Pass` row.
    Complete,
}

/// The pure decision at the heart of a flow: given the flow and the
/// evidence recorded so far, what runs next. Linear and halt-on-fail, with
/// no notion of loops, branches, or retries (see `sloop-flows.md` §4).
///
/// Because this only reads persisted evidence rows and never a clock or a
/// process, resuming after a crash with the same rows yields the same
/// `Step`: the walk is idempotent by construction.
pub fn next_step<'a>(flow: &'a Flow, evidence: &[StageEvidence]) -> Step<'a> {
    for stage in &flow.stages {
        match evidence.iter().find(|row| row.stage == stage.name) {
            None => return Step::Run(stage),
            Some(row) if row.verdict == Verdict::Pass => continue,
            Some(row) => {
                return Step::Halted {
                    failed_stage: row.stage.clone(),
                };
            }
        }
    }
    Step::Complete
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
/// `{ builtin: merge }`. The `agent` payload is accepted and discarded: an
/// agent action's prompt comes from the ticket, not the flow file.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawActor {
    Name(String),
    Agent {
        /// Present only so `{ agent: <prompt> }` matches this variant; the
        /// payload is deliberately discarded.
        #[allow(dead_code)]
        agent: IgnoredAny,
    },
    Exec {
        exec: Vec<String>,
    },
    Builtin {
        builtin: String,
    },
}

/// `result_check: none | reported` | `{ exec: [argv] }` |
/// `{ builtin: commits }`.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawCheck {
    Name(String),
    Exec { exec: Vec<String> },
    Builtin { builtin: String },
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
        Actor, Builtin, Check, FailAction, Flow, Reported, Stage, StageEvidence, Step, Verdict,
        VerdictSource, next_step, parse, resolve_verdict,
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
                        on_fail: None,
                    },
                    Stage {
                        name: "test".into(),
                        action: Actor::Exec {
                            cmd: vec!["cargo".into(), "test".into()],
                        },
                        result_check: exec_check(&["cargo", "clippy"]),
                        fail_action: FailAction::Halt,
                        on_fail: None,
                    },
                    Stage {
                        name: "merge".into(),
                        action: Actor::Builtin(Builtin::Merge),
                        result_check: Check::None,
                        fail_action: FailAction::Halt,
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

    /// The walk is still linear halt-on-fail, so a flow may not bind an edge
    /// the walk cannot honour — even though the vocabulary parses.
    #[test]
    fn continue_and_return_to_parse_but_are_not_yet_supported() {
        let continues = error(
            "- { name: build, action: agent }\n- { name: test, action: { exec: ['true'] }, fail_action: continue }\n",
        );
        assert!(
            continues.contains("`continue` is not yet supported"),
            "{continues}"
        );

        let returns = error(
            "- { name: build, action: agent }\n- { name: test, action: { exec: ['true'] }, fail_action: { return_to: build, attempts: 2 } }\n",
        );
        assert!(
            returns.contains("`return_to` is not yet supported"),
            "{returns}"
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

    #[test]
    fn exactly_one_first_agent_stage_is_required() {
        let missing = error("- { name: check, kind: exec, cmd: ['true'] }\n");
        assert!(
            missing.contains("first stage must be an agent"),
            "{missing}"
        );

        let duplicate = error("- { name: build, kind: agent }\n- { name: rebuild, kind: agent }\n");
        assert!(duplicate.contains("require runner support"), "{duplicate}");
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
                    on_fail: None,
                },
                Stage {
                    name: "review".into(),
                    action: Actor::Exec {
                        cmd: vec!["true".into()],
                    },
                    result_check: Check::None,
                    fail_action: FailAction::Halt,
                    on_fail: None,
                },
                Stage {
                    name: "merge".into(),
                    action: Actor::Builtin(Builtin::Merge),
                    result_check: Check::None,
                    fail_action: FailAction::Halt,
                    on_fail: None,
                },
            ],
        }
    }

    fn passed(stage: &str) -> StageEvidence {
        StageEvidence {
            stage: stage.into(),
            verdict: Verdict::Pass,
            source: VerdictSource::ExitCode,
            reason: None,
        }
    }

    fn failed(stage: &str) -> StageEvidence {
        StageEvidence {
            stage: stage.into(),
            verdict: Verdict::Fail,
            source: VerdictSource::ExitCode,
            reason: None,
        }
    }

    #[test]
    fn next_step_selects_the_first_stage_without_a_row() {
        let flow = build_review_merge();

        assert_eq!(next_step(&flow, &[]), Step::Run(&flow.stages[0]));
        assert_eq!(
            next_step(&flow, &[passed("build")]),
            Step::Run(&flow.stages[1])
        );
        assert_eq!(
            next_step(&flow, &[passed("build"), passed("review")]),
            Step::Run(&flow.stages[2])
        );
    }

    #[test]
    fn next_step_is_complete_only_when_every_stage_passed() {
        let flow = build_review_merge();

        assert_eq!(
            next_step(&flow, &[passed("build"), passed("review"), passed("merge")]),
            Step::Complete
        );
        assert_ne!(
            next_step(&flow, &[passed("build"), passed("review")]),
            Step::Complete
        );
    }

    #[test]
    fn a_failed_row_halts_the_walk_and_later_stages_are_never_requested() {
        let flow = build_review_merge();

        // A `merge` row is present despite `review` failing first; the walk
        // must still halt at `review`, proving stages after a failure are
        // never requested even if evidence for them exists.
        let evidence = [passed("build"), failed("review"), passed("merge")];

        assert_eq!(
            next_step(&flow, &evidence),
            Step::Halted {
                failed_stage: "review".into()
            }
        );
    }

    #[test]
    fn resuming_with_identical_evidence_yields_an_identical_step() {
        let flow = build_review_merge();
        let evidence = [passed("build")];

        assert_eq!(next_step(&flow, &evidence), next_step(&flow, &evidence));
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
