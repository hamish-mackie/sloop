//! Flow definitions and the pure walk over them. Parsing turns a committed
//! YAML file into a validated `Flow`; `next_step` then turns a flow and the
//! evidence gathered so far into the next stage to run or a terminal
//! reading. Neither half touches a clock, a process, or the store, so
//! policy can be tested without a daemon.

use std::collections::HashSet;

use serde::Deserialize;

pub const DEFAULT_FLOW_NAME: &str = "default";
pub const REVIEW_PROMPT_PATH: &str = ".agents/sloop/prompts/review.md";
pub const REVIEW_PROMPT_INSTRUCTION: &str = "Review the completed work for correctness and regressions. Run relevant tests, then report a pass or fail verdict with a concise reason.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Flow {
    pub name: String,
    pub stages: Vec<Stage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stage {
    pub name: String,
    pub kind: StageKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StageKind {
    Build,
    Merge,
    Exec { cmd: Vec<String> },
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
            "build" => {
                if raw.cmd.is_some() {
                    return Err(format!("build stage `{}` must not define `cmd`", raw.name));
                }
                StageKind::Build
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
        stages.push(Stage {
            name: raw.name,
            kind,
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
            kind: StageKind::Build,
        },
        Stage {
            name: "merge".into(),
            kind: StageKind::Merge,
        },
    ];
    Flow {
        name: DEFAULT_FLOW_NAME.into(),
        stages,
    }
}

fn validate_order(stages: &[Stage]) -> Result<(), String> {
    let build_count = stages
        .iter()
        .filter(|stage| stage.kind == StageKind::Build)
        .count();
    if build_count != 1 {
        return Err(format!(
            "flow must contain exactly one build stage; found {build_count}"
        ));
    }
    if stages.first().map(|stage| &stage.kind) != Some(&StageKind::Build) {
        return Err("build stage must be first".into());
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

/// Resolves a stage's verdict, source, and reason from its exit-code
/// reading and an optional reported verdict.
///
/// Policy: a reported verdict is authoritative and overrides the exit code,
/// because agentic stages (e.g. review) exit 0 regardless of their
/// judgment — the CLI ran; that says nothing about the verdict. `build` is
/// the one exception: its evidence rule (exit 0 and commits > 0) is fixed
/// and not negotiable by the worker, so a reported verdict is ignored
/// entirely when the stage kind is `Build`.
pub fn resolve_verdict(
    kind: &StageKind,
    exit: Verdict,
    reported: Option<Reported>,
) -> (Verdict, VerdictSource, Option<String>) {
    if *kind != StageKind::Build {
        if let Some(reported) = reported {
            return (reported.verdict, VerdictSource::Reported, reported.reason);
        }
    }
    (exit, VerdictSource::ExitCode, None)
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
}

#[cfg(test)]
mod tests {
    use super::{
        Flow, Reported, Stage, StageEvidence, StageKind, Step, Verdict, VerdictSource, next_step,
        parse, resolve_verdict,
    };

    fn error(yaml: &str) -> String {
        parse("example", yaml).unwrap_err()
    }

    #[test]
    fn valid_multi_stage_flow_parses_in_order() {
        let flow = parse(
            "release",
            "stages:\n  - name: build\n    kind: build\n  - name: test\n    kind: exec\n    cmd: [cargo, test]\n  - name: merge\n    kind: merge\n",
        )
        .unwrap();

        assert_eq!(
            flow,
            Flow {
                name: "release".into(),
                stages: vec![
                    Stage {
                        name: "build".into(),
                        kind: StageKind::Build,
                    },
                    Stage {
                        name: "test".into(),
                        kind: StageKind::Exec {
                            cmd: vec!["cargo".into(), "test".into()],
                        },
                    },
                    Stage {
                        name: "merge".into(),
                        kind: StageKind::Merge,
                    },
                ],
            }
        );
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
    fn exactly_one_build_stage_is_required() {
        let missing = error("- { name: check, kind: exec, cmd: ['true'] }\n");
        assert!(missing.contains("exactly one build stage"), "{missing}");

        let duplicate = error("- { name: build, kind: build }\n- { name: rebuild, kind: build }\n");
        assert!(duplicate.contains("exactly one build stage"), "{duplicate}");
    }

    #[test]
    fn build_stage_must_be_first() {
        let error =
            error("- { name: check, kind: exec, cmd: ['true'] }\n- { name: build, kind: build }\n");
        assert!(error.contains("build stage must be first"), "{error}");
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
                    kind: StageKind::Build,
                },
                Stage {
                    name: "review".into(),
                    kind: StageKind::Exec {
                        cmd: vec!["true".into()],
                    },
                },
                Stage {
                    name: "merge".into(),
                    kind: StageKind::Merge,
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
    fn reported_verdicts_override_exit_code_for_ordinary_stages() {
        let exec = StageKind::Exec {
            cmd: vec!["true".into()],
        };

        assert_eq!(
            resolve_verdict(&exec, Verdict::Pass, None),
            (Verdict::Pass, VerdictSource::ExitCode, None)
        );

        let reported = Reported {
            verdict: Verdict::Fail,
            reason: Some("changes requested".into()),
        };
        assert_eq!(
            resolve_verdict(&exec, Verdict::Pass, Some(reported)),
            (
                Verdict::Fail,
                VerdictSource::Reported,
                Some("changes requested".into())
            )
        );
    }

    #[test]
    fn build_stage_ignores_reported_verdicts() {
        let reported = Reported {
            verdict: Verdict::Pass,
            reason: Some("looks fine to me".into()),
        };

        assert_eq!(
            resolve_verdict(&StageKind::Build, Verdict::Fail, Some(reported)),
            (Verdict::Fail, VerdictSource::ExitCode, None)
        );
    }
}
