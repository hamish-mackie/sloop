//! Generates valid flow files together with the `Flow` they must parse to.
//!
//! The generator mirrors the validation rules in `flow::parse`: the first
//! stage is the only agent stage, at most one merge stage and only in last
//! position, exec actions carry a non-empty command, a merge stage's check is
//! either omitted or `none`, and an agent stage never writes
//! `result_check: none` because an agent may not go unjudged.

use proptest::prelude::*;
use sloop::flow::{Actor, Builtin, Check, FailAction, Flow, Stage};

/// A stage's action as written in YAML.
#[derive(Debug, Clone)]
pub enum WrittenAction {
    /// The bare `action: agent`.
    Agent,
    /// `action: { agent: <prompt> }`, whose payload the parser discards.
    AgentMapping(String),
    Exec(Vec<String>),
    Merge,
}

impl WrittenAction {
    fn expected(&self) -> Actor {
        match self {
            Self::Agent | Self::AgentMapping(_) => Actor::Agent,
            Self::Exec(cmd) => Actor::Exec { cmd: cmd.clone() },
            Self::Merge => Actor::Builtin(Builtin::Merge),
        }
    }

    fn render(&self) -> String {
        match self {
            Self::Agent => "agent".to_owned(),
            Self::AgentMapping(prompt) => format!("{{ agent: {} }}", quote(prompt)),
            Self::Exec(cmd) => format!("{{ exec: {} }}", render_command(cmd)),
            Self::Merge => "{ builtin: merge }".to_owned(),
        }
    }
}

/// A stage's result check as written in YAML, `Omitted` meaning "absent", in
/// which case the action decides the default.
#[derive(Debug, Clone)]
pub enum WrittenCheck {
    Omitted,
    None,
    Commits,
    Reported,
    Exec(Vec<String>),
}

impl WrittenCheck {
    fn expected(&self, action: &Actor) -> Check {
        match self {
            Self::Omitted => match action {
                Actor::Agent => Check::Actor(Actor::Builtin(Builtin::Commits)),
                Actor::Exec { .. } | Actor::Builtin(_) => Check::None,
            },
            Self::None => Check::None,
            Self::Commits => Check::Actor(Actor::Builtin(Builtin::Commits)),
            Self::Reported => Check::Reported,
            Self::Exec(cmd) => Check::Actor(Actor::Exec { cmd: cmd.clone() }),
        }
    }

    /// The value the `result_check` key takes, or `None` when the stage
    /// writes no key at all.
    fn render(&self) -> Option<String> {
        match self {
            Self::Omitted => None,
            Self::None => Some("none".to_owned()),
            Self::Commits => Some("{ builtin: commits }".to_owned()),
            Self::Reported => Some("reported".to_owned()),
            Self::Exec(cmd) => Some(format!("{{ exec: {} }}", render_command(cmd))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WrittenStage {
    pub action: WrittenAction,
    pub result_check: WrittenCheck,
}

fn command() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec("[a-zA-Z0-9./_-]{1,8}", 1..4)
}

fn result_check() -> impl Strategy<Value = WrittenCheck> {
    prop_oneof![
        Just(WrittenCheck::Omitted),
        Just(WrittenCheck::None),
        Just(WrittenCheck::Commits),
        Just(WrittenCheck::Reported),
        command().prop_map(WrittenCheck::Exec),
    ]
}

/// Every check an agent stage may write. `none` is excluded: an agent that
/// grades itself by exiting cleanly is rejected.
fn agent_check() -> impl Strategy<Value = WrittenCheck> {
    prop_oneof![
        Just(WrittenCheck::Omitted),
        Just(WrittenCheck::Commits),
        Just(WrittenCheck::Reported),
        command().prop_map(WrittenCheck::Exec),
    ]
}

fn agent_stage() -> impl Strategy<Value = WrittenStage> {
    let action = prop_oneof![
        Just(WrittenAction::Agent),
        "[a-z][a-z ]{0,15}".prop_map(WrittenAction::AgentMapping),
    ];
    (action, agent_check()).prop_map(|(action, result_check)| WrittenStage {
        action,
        result_check,
    })
}

fn exec_stage() -> impl Strategy<Value = WrittenStage> {
    (command(), result_check()).prop_map(|(cmd, result_check)| WrittenStage {
        action: WrittenAction::Exec(cmd),
        result_check,
    })
}

/// A merge stage's own outcome is its verdict, so the only checks it may
/// write are the absent one and the `none` that spells the same thing out.
fn merge_stage() -> impl Strategy<Value = WrittenStage> {
    let check = prop_oneof![Just(WrittenCheck::Omitted), Just(WrittenCheck::None)];
    check.prop_map(|result_check| WrittenStage {
        action: WrittenAction::Merge,
        result_check,
    })
}

/// Double-quotes a scalar so YAML reads it as exactly this string. The
/// generators emit no quotes, backslashes, or control characters, so no
/// escaping is needed — but without quoting, generated values like `true`,
/// `null`, `1e5`, or an agent prompt with a trailing space would parse as
/// booleans, numbers, or trimmed strings and diverge from the expected flow.
fn quote(value: &str) -> String {
    format!("\"{value}\"")
}

fn render_command(cmd: &[String]) -> String {
    let quoted: Vec<String> = cmd.iter().map(|part| quote(part)).collect();
    format!("[{}]", quoted.join(", "))
}

fn render_stage(name: &str, stage: &WrittenStage, indent: &str) -> String {
    let mut yaml = format!(
        "{indent}- name: {}\n{indent}  action: {}\n",
        quote(name),
        stage.action.render()
    );
    if let Some(check) = stage.result_check.render() {
        yaml.push_str(&format!("{indent}  result_check: {check}\n"));
    }
    yaml
}

fn expected_stage(name: &str, stage: &WrittenStage) -> Stage {
    let action = stage.action.expected();
    let result_check = stage.result_check.expected(&action);
    Stage {
        name: name.to_owned(),
        action,
        result_check,
        fail_action: FailAction::Halt,
        ff_only: false,
    }
}

/// A rendered flow file plus the `Flow` that `flow::parse` must produce.
pub fn flow_file() -> impl Strategy<Value = (String, Flow)> {
    (
        agent_stage(),
        prop::collection::vec(exec_stage(), 0..3),
        prop::option::of(merge_stage()),
        any::<bool>(),
    )
        .prop_flat_map(|(agent, execs, merge, as_map)| {
            let count = 1 + execs.len() + usize::from(merge.is_some());
            (
                Just((agent, execs, merge, as_map)),
                prop::collection::btree_set("[a-z][a-z0-9_]{0,7}", count),
            )
        })
        .prop_map(|((agent, execs, merge, as_map), names)| {
            let names: Vec<String> = names.into_iter().collect();
            let mut written: Vec<(&str, &WrittenStage)> = vec![(&names[0], &agent)];
            for (index, exec) in execs.iter().enumerate() {
                written.push((&names[1 + index], exec));
            }
            if let Some(merge) = &merge {
                written.push((names.last().expect("names cover every stage"), merge));
            }

            let indent = if as_map { "  " } else { "" };
            let mut yaml = if as_map {
                "stages:\n".to_owned()
            } else {
                String::new()
            };
            let mut stages = Vec::with_capacity(written.len());
            for (name, stage) in &written {
                yaml.push_str(&render_stage(name, stage, indent));
                stages.push(expected_stage(name, stage));
            }
            let flow = Flow {
                name: "generated".into(),
                stages,
            };
            (yaml, flow)
        })
}
