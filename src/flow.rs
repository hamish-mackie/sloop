//! Flow definitions and the pure walk over them. Parsing turns a committed
//! YAML file into a validated `Flow`; `next_step` then turns a flow and the
//! evidence gathered so far into the next stage to run or a terminal
//! reading. Neither half touches a clock, a process, or the store, so
//! policy can be tested without a daemon.

use std::collections::HashSet;

use serde::{Deserialize, Deserializer, Serialize};

pub const DEFAULT_FLOW_NAME: &str = "default";
pub const REVIEW_PROMPT_PATH: &str = ".agents/sloop/prompts/review.md";
pub const REVIEW_PROMPT_INSTRUCTION: &str = "Review the completed work for correctness and regressions. Run relevant tests, then report a pass or fail verdict with a concise reason.";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Flow {
    pub name: String,
    pub stages: Vec<Stage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Stage {
    pub name: String,
    pub kind: StageKind,
    pub verdict: VerdictPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StageKind {
    #[serde(alias = "Build")]
    Agent,
    Merge,
    Exec {
        cmd: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VerdictPolicy {
    Exit,
    Commits,
    Check { cmd: Vec<String> },
    Reported,
}

impl<'de> Deserialize<'de> for Stage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct SnapshotStage {
            name: String,
            kind: StageKind,
            verdict: Option<VerdictPolicy>,
        }

        let stage = SnapshotStage::deserialize(deserializer)?;
        let verdict = stage.verdict.unwrap_or(match &stage.kind {
            StageKind::Agent => VerdictPolicy::Commits,
            StageKind::Exec { .. } | StageKind::Merge => VerdictPolicy::Exit,
        });
        Ok(Self {
            name: stage.name,
            kind: stage.kind,
            verdict,
        })
    }
}

pub(crate) fn parse(name: &str, contents: &str) -> Result<Flow, String> {
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
        let kind = match raw.kind.as_str() {
            "agent" | "build" => {
                if raw.cmd.is_some() {
                    return Err(format!("agent stage `{}` must not define `cmd`", raw.name));
                }
                StageKind::Agent
            }
            "merge" => {
                if raw.cmd.is_some() {
                    return Err(format!("merge stage `{}` must not define `cmd`", raw.name));
                }
                StageKind::Merge
            }
            "exec" => {
                let cmd = raw.cmd.unwrap_or_default();
                if cmd.is_empty() {
                    return Err(format!(
                        "exec stage `{}` must define a non-empty `cmd`",
                        raw.name
                    ));
                }
                StageKind::Exec { cmd }
            }
            kind => return Err(format!("stage `{}` has unknown kind `{kind}`", raw.name)),
        };
        let verdict = match (&kind, raw.verdict) {
            (StageKind::Merge, Some(_)) => {
                return Err(format!(
                    "merge stage `{}` must not define `verdict`",
                    raw.name
                ));
            }
            (StageKind::Merge | StageKind::Exec { .. }, None) => VerdictPolicy::Exit,
            (StageKind::Agent, None) => VerdictPolicy::Commits,
            (_, Some(RawVerdict::Name(name))) => match name.as_str() {
                "exit" => VerdictPolicy::Exit,
                "commits" => VerdictPolicy::Commits,
                "reported" => VerdictPolicy::Reported,
                _ => {
                    return Err(format!(
                        "stage `{}` has unknown verdict policy `{name}`",
                        raw.name
                    ));
                }
            },
            (_, Some(RawVerdict::Check { check })) => {
                if check.is_empty() {
                    return Err(format!(
                        "stage `{}` check verdict must define a non-empty command",
                        raw.name
                    ));
                }
                VerdictPolicy::Check { cmd: check }
            }
        };
        stages.push(Stage {
            name: raw.name,
            kind,
            verdict,
        });
    }

    validate_order(&stages)?;
    Ok(Flow {
        name: name.to_owned(),
        stages,
    })
}

pub(crate) fn built_in_default() -> Flow {
    let stages = vec![
        Stage {
            name: "build".into(),
            kind: StageKind::Agent,
            verdict: VerdictPolicy::Commits,
        },
        Stage {
            name: "merge".into(),
            kind: StageKind::Merge,
            verdict: VerdictPolicy::Exit,
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
        .is_some_and(|stage| stage.kind == StageKind::Agent)
    {
        return Err("the first stage must be an agent stage".into());
    }
    let agent_count = stages
        .iter()
        .filter(|stage| stage.kind == StageKind::Agent)
        .count();
    if agent_count > 1 {
        return Err(
            "only the first stage may be an agent stage; additional agent stages require runner support"
                .into(),
        );
    }

    let merge_count = stages
        .iter()
        .filter(|stage| stage.kind == StageKind::Merge)
        .count();
    if merge_count > 1 {
        return Err(format!(
            "flow may contain at most one merge stage; found {merge_count}"
        ));
    }
    if merge_count == 1 && stages.last().map(|stage| &stage.kind) != Some(&StageKind::Merge) {
        return Err("merge stage must be last".into());
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
/// by its policy. Reports are authoritative only for `Reported`; other
/// policies ignore them.
pub fn resolve_verdict(
    policy: &VerdictPolicy,
    exit: Verdict,
    reported: Option<Reported>,
) -> (Verdict, VerdictSource, Option<String>) {
    if *policy != VerdictPolicy::Reported {
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

#[derive(Debug, Deserialize)]
struct RawStage {
    name: String,
    kind: String,
    cmd: Option<Vec<String>>,
    verdict: Option<RawVerdict>,
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
        Flow, Reported, Stage, StageEvidence, StageKind, Step, Verdict, VerdictPolicy,
        VerdictSource, next_step, parse, resolve_verdict,
    };

    fn error(yaml: &str) -> String {
        parse("example", yaml).unwrap_err()
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
                        kind: StageKind::Agent,
                        verdict: VerdictPolicy::Commits,
                    },
                    Stage {
                        name: "test".into(),
                        kind: StageKind::Exec {
                            cmd: vec!["cargo".into(), "test".into()],
                        },
                        verdict: VerdictPolicy::Check {
                            cmd: vec!["cargo".into(), "clippy".into()],
                        },
                    },
                    Stage {
                        name: "merge".into(),
                        kind: StageKind::Merge,
                        verdict: VerdictPolicy::Exit,
                    },
                ],
            }
        );
    }

    #[test]
    fn build_is_a_deprecated_alias_for_agent() {
        let flow = parse("example", "- { name: build, kind: build }\n").unwrap();
        assert_eq!(flow.stages[0].kind, StageKind::Agent);
        assert_eq!(flow.stages[0].verdict, VerdictPolicy::Commits);
    }

    #[test]
    fn old_build_snapshots_deserialize_with_the_agent_default() {
        let flow: Flow = serde_json::from_str(
            r#"{"name":"example","stages":[{"name":"build","kind":"Build"}]}"#,
        )
        .unwrap();
        assert_eq!(flow.stages[0].kind, StageKind::Agent);
        assert_eq!(flow.stages[0].verdict, VerdictPolicy::Commits);
    }

    #[test]
    fn verdict_policies_and_defaults_parse() {
        let flow = parse(
            "example",
            "- { name: build, kind: agent, verdict: exit }\n- { name: test, kind: exec, cmd: ['true'], verdict: commits }\n- { name: review, kind: exec, cmd: ['true'], verdict: reported }\n",
        )
        .unwrap();
        assert_eq!(flow.stages[0].verdict, VerdictPolicy::Exit);
        assert_eq!(flow.stages[1].verdict, VerdictPolicy::Commits);
        assert_eq!(flow.stages[2].verdict, VerdictPolicy::Reported);

        let defaults = parse(
            "example",
            "- { name: build, kind: agent }\n- { name: test, kind: exec, cmd: ['true'] }\n",
        )
        .unwrap();
        assert_eq!(defaults.stages[0].verdict, VerdictPolicy::Commits);
        assert_eq!(defaults.stages[1].verdict, VerdictPolicy::Exit);
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
                    kind: StageKind::Agent,
                    verdict: VerdictPolicy::Commits,
                },
                Stage {
                    name: "review".into(),
                    kind: StageKind::Exec {
                        cmd: vec!["true".into()],
                    },
                    verdict: VerdictPolicy::Exit,
                },
                Stage {
                    name: "merge".into(),
                    kind: StageKind::Merge,
                    verdict: VerdictPolicy::Exit,
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
            resolve_verdict(&VerdictPolicy::Exit, Verdict::Pass, None),
            (Verdict::Pass, VerdictSource::ExitCode, None)
        );

        let reported = Reported {
            verdict: Verdict::Fail,
            reason: Some("changes requested".into()),
        };
        assert_eq!(
            resolve_verdict(&VerdictPolicy::Reported, Verdict::Pass, Some(reported)),
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
            resolve_verdict(&VerdictPolicy::Commits, Verdict::Fail, Some(reported)),
            (Verdict::Fail, VerdictSource::ExitCode, None)
        );
    }

    #[test]
    fn missing_report_is_a_failed_reported_verdict() {
        assert_eq!(
            resolve_verdict(&VerdictPolicy::Reported, Verdict::Pass, None),
            (
                Verdict::Fail,
                VerdictSource::Reported,
                Some("no verdict reported".into())
            )
        );
    }
}
