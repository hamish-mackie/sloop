//! Properties of the pure flow walk: `next_step` and `resolve_verdict`.

use proptest::prelude::*;
use sloop::flow::{
    Flow, Reported, StageEvidence, Step, Verdict, VerdictPolicy, VerdictSource, next_step,
    resolve_verdict,
};

use crate::flow_gen::flow_file;

fn verdict() -> impl Strategy<Value = Verdict> {
    prop_oneof![Just(Verdict::Pass), Just(Verdict::Fail)]
}

/// A flow parsed from a generated file plus evidence rows referencing its
/// stages — possibly several rows per stage, in arbitrary order, matching
/// what a crash-looped daemon could have persisted.
fn flow_with_evidence() -> impl Strategy<Value = (Flow, Vec<StageEvidence>)> {
    flow_file()
        .prop_flat_map(|(yaml, _)| {
            let flow = sloop::flow::parse("generated", &yaml).expect("generated flows parse");
            let stage_count = flow.stages.len();
            (
                Just(flow),
                prop::collection::vec((0..stage_count, verdict()), 0..stage_count * 2),
            )
        })
        .prop_map(|(flow, rows)| {
            let evidence = rows
                .into_iter()
                .map(|(index, verdict)| StageEvidence {
                    stage: flow.stages[index].name.clone(),
                    verdict,
                    source: VerdictSource::ExitCode,
                    reason: None,
                })
                .collect();
            (flow, evidence)
        })
}

/// The verdict `next_step` sees for a stage: its first evidence row.
fn first_row<'a>(evidence: &'a [StageEvidence], stage: &str) -> Option<&'a StageEvidence> {
    evidence.iter().find(|row| row.stage == stage)
}

proptest! {
    /// The walk is a pure function of (flow, evidence): calling it again
    /// yields the same step, which is what makes crash recovery idempotent.
    #[test]
    fn next_step_is_deterministic((flow, evidence) in flow_with_evidence()) {
        prop_assert_eq!(next_step(&flow, &evidence), next_step(&flow, &evidence));
    }

    /// Whatever the evidence, the returned step is consistent with the
    /// declared stage order: every stage before the answer passed, and the
    /// walk never skips a stage or runs one that already has evidence.
    #[test]
    fn next_step_respects_declaration_order((flow, evidence) in flow_with_evidence()) {
        match next_step(&flow, &evidence) {
            Step::Run(stage) => {
                let position = flow.stages.iter().position(|s| s.name == stage.name)
                    .expect("returned stage belongs to the flow");
                prop_assert!(first_row(&evidence, &stage.name).is_none(),
                    "ran a stage that already has evidence");
                for earlier in &flow.stages[..position] {
                    let row = first_row(&evidence, &earlier.name)
                        .expect("every stage before the answer has evidence");
                    prop_assert_eq!(row.verdict, Verdict::Pass);
                }
            }
            Step::Halted { failed_stage } => {
                let position = flow.stages.iter().position(|s| s.name == failed_stage)
                    .expect("failed stage belongs to the flow");
                let row = first_row(&evidence, &failed_stage).expect("halt cites evidence");
                prop_assert_eq!(row.verdict, Verdict::Fail);
                for earlier in &flow.stages[..position] {
                    let row = first_row(&evidence, &earlier.name)
                        .expect("every stage before the halt has evidence");
                    prop_assert_eq!(row.verdict, Verdict::Pass);
                }
            }
            Step::Complete => {
                for stage in &flow.stages {
                    let row = first_row(&evidence, &stage.name)
                        .expect("completion requires evidence for every stage");
                    prop_assert_eq!(row.verdict, Verdict::Pass);
                }
            }
        }
    }

    /// Only the first evidence row per stage matters; rows after it are
    /// ignored duplicates from re-executed stages.
    #[test]
    fn next_step_ignores_duplicate_rows((flow, evidence) in flow_with_evidence()) {
        let mut seen = std::collections::HashSet::new();
        let deduped: Vec<StageEvidence> = evidence
            .iter()
            .filter(|row| seen.insert(row.stage.clone()))
            .cloned()
            .collect();
        prop_assert_eq!(next_step(&flow, &evidence), next_step(&flow, &deduped));
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
            VerdictPolicy::Exit,
            VerdictPolicy::Commits,
            VerdictPolicy::Check { cmd: vec!["true".into()] },
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
        let (verdict, source, reason) = resolve_verdict(&VerdictPolicy::Reported, exit, report);
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
