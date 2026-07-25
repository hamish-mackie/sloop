//! Properties of the pure flow walk: `next_step` and `resolve_verdict`.

use proptest::prelude::*;
use sloop::flow::{
    Actor, Builtin, Check, Flow, HaltReason, Reported, StageEvidence, Step, Verdict, VerdictSource,
    next_step, resolve_verdict,
};

use crate::flow_gen::flow_file;

fn verdict() -> impl Strategy<Value = Verdict> {
    prop_oneof![Just(Verdict::Pass), Just(Verdict::Fail)]
}

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

/// A flow parsed from a generated file together with a log some replay of it
/// could actually have written, plus the index of the failing row if there
/// is one. Every parsed flow is all-`Halt` today — `parse` still refuses
/// `continue` and `return_to` — so a well-formed log is a run of first
/// attempts that pass, optionally ended by one that fails.
fn flow_with_log() -> impl Strategy<Value = (Flow, Vec<StageEvidence>, Option<usize>)> {
    flow_file()
        .prop_flat_map(|(yaml, _)| {
            let flow = sloop::flow::parse("generated", &yaml).expect("generated flows parse");
            let stage_count = flow.stages.len();
            (Just(flow), 0..=stage_count, any::<bool>())
        })
        .prop_map(|(flow, passes, fails)| {
            let failed = (fails && passes < flow.stages.len()).then_some(passes);
            let mut log: Vec<StageEvidence> = (0..passes)
                .map(|index| row(&flow, index, 1, Verdict::Pass))
                .collect();
            if let Some(index) = failed {
                log.push(row(&flow, index, 1, Verdict::Fail));
            }
            (flow, log, failed)
        })
}

proptest! {
    /// The walk is a pure function of (flow, log): replaying the same log
    /// yields the same step, which is what makes crash recovery idempotent.
    #[test]
    fn next_step_is_deterministic((flow, log, _) in flow_with_log()) {
        prop_assert_eq!(next_step(&flow, &log), next_step(&flow, &log));
    }

    /// The fold lands exactly where the log walked it: one stage per row, a
    /// halt at the failing row, and completion only off the end.
    #[test]
    fn the_fold_lands_where_the_log_walked_it((flow, log, failed) in flow_with_log()) {
        match next_step(&flow, &log) {
            Step::Run { stage, attempt } => {
                prop_assert_eq!(failed, None, "a halting failure never yields a run");
                prop_assert_eq!(&stage.name, &flow.stages[log.len()].name);
                prop_assert_eq!(attempt, 1, "no edge re-enters a stage in an all-halt flow");
            }
            Step::Halted { failed_stage, reason } => {
                let index = failed.expect("a halt requires a failing row");
                prop_assert_eq!(&failed_stage, &flow.stages[index].name);
                prop_assert_eq!(reason, HaltReason::FailActionHalt);
            }
            Step::Complete => {
                prop_assert_eq!(failed, None);
                prop_assert_eq!(log.len(), flow.stages.len());
            }
        }
    }

    /// Rows appended after a halting failure are never read: the fold stops
    /// at the failure, so nothing written afterwards can revive the walk.
    #[test]
    fn rows_after_a_halt_never_change_the_step(
        (flow, log, failed) in flow_with_log(),
        extra in prop::collection::vec((0..8usize, verdict()), 1..4),
    ) {
        prop_assume!(failed.is_some());
        let mut extended = log.clone();
        extended.extend(
            extra
                .into_iter()
                .map(|(index, verdict)| row(&flow, index % flow.stages.len(), 1, verdict)),
        );
        prop_assert_eq!(next_step(&flow, &log), next_step(&flow, &extended));
    }

    /// A row keyed to any stage but the one the cursor stands on is a
    /// corrupt log. The fold never re-orders or re-interprets it into a
    /// history that would have been legal.
    #[test]
    fn a_misplaced_row_is_a_corrupt_log(
        (flow, log, _) in flow_with_log(),
        choice in any::<prop::sample::Index>(),
        shift in 1..8usize,
    ) {
        prop_assume!(!log.is_empty());
        let mut corrupted = log.clone();
        let position = choice.index(corrupted.len());
        let stage_count = flow.stages.len();
        let moved = (corrupted[position].stage_index + shift) % stage_count;
        prop_assume!(moved != corrupted[position].stage_index);
        corrupted[position] = row(&flow, moved, 1, corrupted[position].verdict);

        let step = next_step(&flow, &corrupted);
        let corrupt = matches!(step, Step::Halted { reason: HaltReason::CorruptLog, .. });
        prop_assert!(corrupt, "misplaced row was not detected: {:?}", step);
    }

    /// Reports are authoritative only under the `Reported` policy; every
    /// other policy takes the exit verdict and discards the report entirely.
    #[test]
    fn reports_only_bind_under_the_reported_policy(
        exit in verdict(),
        reported in prop::option::of((verdict(), prop::option::of("[ -~]{0,20}"))),
        policy_choice in 0..3usize,
    ) {
        let policy = [
            Check::None,
            Check::Actor(Actor::Builtin(Builtin::Commits)),
            Check::Actor(Actor::Exec { cmd: vec!["true".into()] }),
        ][policy_choice].clone();
        let report = reported.map(|(verdict, reason)| Reported { verdict, reason });
        prop_assert_eq!(
            resolve_verdict(&policy, exit, report),
            (exit, VerdictSource::ExitCode, None)
        );
    }

    /// Under `Reported`, a report decides the verdict and a missing report
    /// is a `Fail` — silence is never success.
    #[test]
    fn a_missing_report_fails_a_reported_stage(
        exit in verdict(),
        reported in prop::option::of((verdict(), prop::option::of("[ -~]{0,20}"))),
    ) {
        let report = reported.clone().map(|(verdict, reason)| Reported { verdict, reason });
        let (verdict, source, reason) = resolve_verdict(&Check::Reported, exit, report);
        prop_assert_eq!(source, VerdictSource::Reported);
        match reported {
            Some((expected, expected_reason)) => {
                prop_assert_eq!(verdict, expected);
                prop_assert_eq!(reason, expected_reason);
            }
            None => {
                prop_assert_eq!(verdict, Verdict::Fail);
                prop_assert!(reason.is_some(), "a defaulted failure explains itself");
            }
        }
    }
}
