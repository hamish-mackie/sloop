//! The YAML grammar: what a flow file may say, and the validation that turns
//! it into a `Flow`. Each `Raw*` wire type sits beside the function that is
//! its only reader, so a change to the grammar is a change to one adjacent
//! pair rather than to two halves of a file.

use std::collections::HashSet;

use serde::Deserialize;
use serde::de::IgnoredAny;

use super::panel::{RawPanel, parse_panel};
use super::{
    Actor, Builtin, Check, FailAction, Flow, MAX_FLOW_EXECUTIONS, MAX_RETURN_ATTEMPTS, Stage,
};

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

/// The check a stage gets when it names none: an agent is never trusted to
/// grade itself by exiting cleanly, so it defaults to the commits gate;
/// everything else is judged by its own exit.
fn default_check(action: &Actor) -> Check {
    match action {
        Actor::Agent => Check::Actor(Actor::Builtin(Builtin::Commits)),
        Actor::Exec { .. } | Actor::Builtin(_) => Check::None,
    }
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
    for (builtin, name) in [(Builtin::Merge, "merge"), (Builtin::Sync, "sync")] {
        if *action == Actor::Builtin(builtin) && *result_check != Check::None {
            return Err(format!(
                "{name} stage `{stage}` must have `result_check: none`"
            ));
        }
    }
    Ok(())
}

/// Structural rules on a whole flow. Agent actions are deliberately absent:
/// one driver walks every stage the same way, so an agent action is legal in
/// any position and any number of times, each with its own supervised process.
fn validate_order(stages: &[Stage]) -> Result<(), String> {
    if stages.is_empty() {
        return Err("flow must define at least one stage".into());
    }

    let merge = Actor::Builtin(Builtin::Merge);
    let sync = Actor::Builtin(Builtin::Sync);
    let merge_count = stages.iter().filter(|stage| stage.action == merge).count();
    if merge_count > 1 {
        return Err(format!(
            "flow may contain at most one merge stage; found {merge_count}"
        ));
    }
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

#[cfg(test)]
mod tests {
    use crate::flow::{Actor, Builtin, Check, FailAction, Flow, Stage, parse};

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

    /// Both spellings of "no stages at all" are refused at parse time rather
    /// than posted to and walked straight to complete.
    #[test]
    fn a_stageless_flow_is_rejected() {
        for yaml in ["[]\n", "stages: []\n"] {
            let error = error(yaml);
            assert!(error.contains("must define at least one stage"), "{error}");
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
}
