//! Generates valid flow files together with the `Flow` they must parse to.
//!
//! The generator mirrors the validation rules in `flow::parse`: the first
//! stage is the only agent stage, at most one merge stage and only in last
//! position, exec stages carry a non-empty `cmd`, merge stages define no
//! verdict, and agent stages define no `on_fail`.

use proptest::prelude::*;
use sloop::flow::{Flow, OnFail, Stage, StageKind, VerdictPolicy};

/// A stage's verdict as written in YAML, `None` meaning "omitted".
#[derive(Debug, Clone)]
pub enum WrittenVerdict {
    Omitted,
    Exit,
    Commits,
    Reported,
    Check(Vec<String>),
}

impl WrittenVerdict {
    fn expected(&self, kind: &StageKind) -> VerdictPolicy {
        match self {
            Self::Omitted => match kind {
                StageKind::Agent => VerdictPolicy::Commits,
                StageKind::Exec { .. } | StageKind::Merge => VerdictPolicy::Exit,
            },
            Self::Exit => VerdictPolicy::Exit,
            Self::Commits => VerdictPolicy::Commits,
            Self::Reported => VerdictPolicy::Reported,
            Self::Check(cmd) => VerdictPolicy::Check { cmd: cmd.clone() },
        }
    }
}

#[derive(Debug, Clone)]
pub struct WrittenStage {
    pub kind_word: &'static str,
    pub cmd: Option<Vec<String>>,
    pub verdict: WrittenVerdict,
    pub on_fail: Option<OnFail>,
}

fn command() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec("[a-zA-Z0-9./_-]{1,8}", 1..4)
}

fn verdict() -> impl Strategy<Value = WrittenVerdict> {
    prop_oneof![
        Just(WrittenVerdict::Omitted),
        Just(WrittenVerdict::Exit),
        Just(WrittenVerdict::Commits),
        Just(WrittenVerdict::Reported),
        command().prop_map(WrittenVerdict::Check),
    ]
}

fn on_fail() -> impl Strategy<Value = Option<OnFail>> {
    prop::option::of(
        (
            "[a-z][a-z ]{0,19}",
            1u32..=3,
            prop::option::of("[a-z]{1,8}"),
            prop::option::of("[a-z]{1,8}"),
            prop::option::of("[a-z]{1,8}"),
        )
            .prop_map(|(agent, attempts, target, model, effort)| OnFail {
                agent,
                attempts,
                target,
                model,
                effort,
            }),
    )
}

fn agent_stage() -> impl Strategy<Value = WrittenStage> {
    verdict().prop_map(|verdict| WrittenStage {
        kind_word: "agent",
        cmd: None,
        verdict,
        on_fail: None,
    })
}

fn exec_stage() -> impl Strategy<Value = WrittenStage> {
    (command(), verdict(), on_fail()).prop_map(|(cmd, verdict, on_fail)| WrittenStage {
        kind_word: "exec",
        cmd: Some(cmd),
        verdict,
        on_fail,
    })
}

fn merge_stage() -> impl Strategy<Value = WrittenStage> {
    on_fail().prop_map(|on_fail| WrittenStage {
        kind_word: "merge",
        cmd: None,
        verdict: WrittenVerdict::Omitted,
        on_fail,
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
        "{indent}- name: {}\n{indent}  kind: {}\n",
        quote(name),
        stage.kind_word
    );
    if let Some(cmd) = &stage.cmd {
        yaml.push_str(&format!("{indent}  cmd: {}\n", render_command(cmd)));
    }
    match &stage.verdict {
        WrittenVerdict::Omitted => {}
        WrittenVerdict::Exit => yaml.push_str(&format!("{indent}  verdict: exit\n")),
        WrittenVerdict::Commits => yaml.push_str(&format!("{indent}  verdict: commits\n")),
        WrittenVerdict::Reported => yaml.push_str(&format!("{indent}  verdict: reported\n")),
        WrittenVerdict::Check(cmd) => yaml.push_str(&format!(
            "{indent}  verdict: {{ check: {} }}\n",
            render_command(cmd)
        )),
    }
    if let Some(on_fail) = &stage.on_fail {
        yaml.push_str(&format!(
            "{indent}  on_fail:\n{indent}    agent: {}\n{indent}    attempts: {}\n",
            quote(&on_fail.agent),
            on_fail.attempts
        ));
        for (key, value) in [
            ("target", &on_fail.target),
            ("model", &on_fail.model),
            ("effort", &on_fail.effort),
        ] {
            if let Some(value) = value {
                yaml.push_str(&format!("{indent}    {key}: {}\n", quote(value)));
            }
        }
    }
    yaml
}

fn expected_stage(name: &str, stage: &WrittenStage) -> Stage {
    let kind = match stage.kind_word {
        "agent" => StageKind::Agent,
        "merge" => StageKind::Merge,
        "exec" => StageKind::Exec {
            cmd: stage.cmd.clone().expect("exec stages carry a cmd"),
        },
        other => unreachable!("generator produced kind {other}"),
    };
    let verdict = stage.verdict.expected(&kind);
    Stage {
        name: name.to_owned(),
        kind,
        verdict,
        on_fail: stage.on_fail.clone(),
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
