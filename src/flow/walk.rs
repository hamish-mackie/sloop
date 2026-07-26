//! The replay: a left fold over a run's ordered evidence log that derives
//! where the walk stands. It reads exactly three things from the flow —
//! `Flow::stages`, `Stage::name`, and `Stage::fail_action` — and touches no
//! clock, process, or store, so the same log always yields the same step.

use super::{Check, FailAction, Flow, Stage};

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
    /// A panel's reviewers reported and [`aggregate`](super::aggregate)
    /// counted them.
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

#[cfg(test)]
mod tests {
    use crate::flow::{
        Actor, Builtin, Check, FailAction, Flow, HaltReason, Reported, Stage, StageEvidence, Step,
        Verdict, VerdictSource, next_step, resolve_verdict, return_trigger,
    };

    fn commits() -> Check {
        Check::Actor(Actor::Builtin(Builtin::Commits))
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
        let flow = flow_with(&[
            ("build", FailAction::Halt),
            ("test", return_to("build", 1)),
            ("review", return_to("test", 1)),
        ]);

        let mut log = vec![
            pass(&flow, 0, 1),
            fail(&flow, 1, 1),
            pass(&flow, 0, 2),
            pass(&flow, 1, 2),
            fail(&flow, 2, 1),
        ];
        assert_eq!(next_step(&flow, &log), run(&flow, 1, 3));

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

        assert_eq!(
            next_step(&flow, &[passed(&flow, 1)]),
            halted("build", HaltReason::CorruptLog)
        );

        assert_eq!(
            next_step(&flow, &[passed(&flow, 0), passed(&flow, 0)]),
            halted("review", HaltReason::CorruptLog)
        );

        assert_eq!(
            next_step(&flow, &[pass(&flow, 0, 2)]),
            halted("build", HaltReason::CorruptLog)
        );

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

        assert_eq!(return_trigger(&flow, &[]), None);
        assert_eq!(return_trigger(&flow, &[pass(&flow, 0, 1)]), None);

        assert_eq!(
            return_trigger(&flow, &[pass(&flow, 0, 1), fail(&flow, 1, 1)]),
            None
        );

        let mut log = vec![pass(&flow, 0, 1), pass(&flow, 1, 1), fail(&flow, 2, 1)];
        assert_eq!(return_trigger(&flow, &log), Some((2, 1)));

        log.push(pass(&flow, 0, 2));
        assert_eq!(return_trigger(&flow, &log), Some((2, 1)));

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
