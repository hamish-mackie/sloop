//! Panels: several independent reviewers judging one stage, and the pure
//! count over their reports that decides it. The whole slice lives here —
//! the types, the parse-time validation of a `panel:` block, and the
//! aggregation the driver derives at read time — because the walk does not
//! know panels exist and the grammar only needs the one entry point.

use std::path::Path;

use serde::{Deserialize, Serialize};

use super::walk::Verdict;

/// The directory a panel's `prompt` path is resolved against. Panel prompts
/// are committed files like every other piece of flow configuration, so the
/// path is repository-relative under the Sloop directory rather than absolute
/// or relative to whatever the daemon's working directory happens to be.
pub const PANEL_PROMPT_ROOT: &str = ".agents/sloop";

/// The reason a reviewer that never reported is credited with.
pub const NO_VERDICT_REPORTED: &str = "no verdict reported";

/// A panel smaller than this is not a panel, and one larger than this costs
/// more tokens than any v1 quorum rule can justify.
pub const MIN_PANEL_REVIEWERS: usize = 2;
pub const MAX_PANEL_REVIEWERS: usize = 5;

/// A panel of independent reviewers.
///
/// One agentic reviewer is one uncalibrated opinion. A panel buys independence
/// with tokens: each reviewer examines the run alone and reports, and the stage
/// verdict is a *deterministic count* over the reports rather than anything a
/// reviewer decided. The opinions stay untrusted; the procedure over them is
/// kernel code, which is why [`aggregate`] lives here beside the walk and is
/// tested without an LLM anywhere near it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Panel {
    /// The prompt file every reviewer is handed, relative to
    /// [`PANEL_PROMPT_ROOT`]. One prompt for the whole panel: per-reviewer
    /// prompts would make the reports incomparable.
    pub prompt: String,
    pub reviewers: Vec<Reviewer>,
    /// How many `Pass` reports the stage needs. `1..=reviewers.len()`.
    pub quorum: u32,
}

/// One seat on a panel. Only the target and its model/effort vary: the point
/// of a panel is decorrelated failure modes, and the cheapest way to buy them
/// is to seat different vendors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reviewer {
    /// An entry under `agent.targets` in config.yaml. Existence is checked
    /// where the configured targets are known (see `config.rs`).
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
}

/// How sure a reviewer says it is. Recorded evidence only: v1 aggregation
/// counts reports and never weights them, so a confident wrong reviewer
/// outvotes nobody. Floats are deliberately absent — a scalar invites a
/// weighting rule that has not been designed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    Low,
    #[default]
    Medium,
    High,
}

impl Confidence {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "low" => Some(Self::Low),
            "medium" => Some(Self::Medium),
            "high" => Some(Self::High),
            _ => None,
        }
    }
}

/// Validates a panel block's own shape. Reviewer targets are checked later,
/// where the configured agent targets are known (see `config.rs`); everything
/// that can be decided from the flow file alone is decided here, so a flow
/// whose panel could never run is refused at parse time rather than at the
/// moment it would have spawned five agents.
pub(super) fn parse_panel(stage: &str, raw: RawPanel) -> Result<Panel, String> {
    let prompt = raw
        .prompt
        .map(|prompt| prompt.trim().to_owned())
        .filter(|prompt| !prompt.is_empty())
        .ok_or_else(|| format!("stage `{stage}` panel must define a non-empty `prompt`"))?;
    if Path::new(&prompt).is_absolute() || prompt.split('/').any(|segment| segment == "..") {
        return Err(format!(
            "stage `{stage}` panel prompt must be a relative path under `{PANEL_PROMPT_ROOT}` \
             without `..`"
        ));
    }
    let reviewers = raw.reviewers.unwrap_or_default();
    if !(MIN_PANEL_REVIEWERS..=MAX_PANEL_REVIEWERS).contains(&reviewers.len()) {
        return Err(format!(
            "stage `{stage}` panel must define between {MIN_PANEL_REVIEWERS} and \
             {MAX_PANEL_REVIEWERS} reviewers; found {}",
            reviewers.len()
        ));
    }
    let reviewers = reviewers
        .into_iter()
        .map(|reviewer| {
            let target = reviewer.target.trim().to_owned();
            if target.is_empty() {
                return Err(format!(
                    "stage `{stage}` panel reviewer must name a non-empty `target`"
                ));
            }
            Ok(Reviewer {
                target,
                model: reviewer.model,
                effort: reviewer.effort,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let quorum = raw
        .require
        .and_then(|require| require.quorum)
        .unwrap_or(reviewers.len() as u32);
    if quorum == 0 || quorum as usize > reviewers.len() {
        return Err(format!(
            "stage `{stage}` panel quorum must be between 1 and {}; found {quorum}",
            reviewers.len()
        ));
    }
    Ok(Panel {
        prompt,
        reviewers,
        quorum,
    })
}

#[derive(Debug, Deserialize)]
pub(super) struct RawPanel {
    prompt: Option<String>,
    reviewers: Option<Vec<RawReviewer>>,
    require: Option<RawRequire>,
}

#[derive(Debug, Deserialize)]
struct RawReviewer {
    target: String,
    model: Option<String>,
    effort: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawRequire {
    quorum: Option<u32>,
}

/// One reviewer's report, as the aggregation reads it back from evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewerReport {
    pub verdict: Verdict,
    /// Absent only for a reviewer that never reported: it has no confidence to
    /// record. Present reports default to [`Confidence::Medium`].
    pub confidence: Option<Confidence>,
    pub reason: String,
}

impl ReviewerReport {
    /// What a reviewer that exited without reporting counts as. Silence is not
    /// an abstention: a panel that could not be heard from has not approved
    /// anything, so the seat is filled with a `Fail` and says why.
    fn silent() -> Self {
        Self {
            verdict: Verdict::Fail,
            confidence: None,
            reason: NO_VERDICT_REPORTED.to_owned(),
        }
    }
}

/// A panel's derived reading: every seat filled, and the verdict the count
/// over them yields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PanelOutcome {
    pub verdict: Verdict,
    /// One entry per seat, in reviewer order, silent seats included.
    pub reports: Vec<ReviewerReport>,
    /// The tally, in one clause fit to quote back to an agent.
    pub reason: String,
}

/// Derives a panel's verdict from its reviewers' reports.
///
/// This is the whole aggregation, and it is deliberately dull: `Pass` iff at
/// least `quorum` seats reported `Pass`. Confidence is carried through as
/// evidence and never consulted; there is no veto and no weighting, so a panel
/// behaves the same way every time and an operator can predict it from the
/// config alone.
///
/// `reported` is indexed by reviewer; a `None` — or a short slice, which is
/// what a stage abandoned part-way through its panel leaves — fills that seat
/// with [`ReviewerReport::silent`]. Nothing here reads a clock, a process, or
/// the store, so the aggregate is *derived at read time* rather than stored:
/// a resumed run recomputes the identical verdict from the identical rows.
pub fn aggregate(panel: &Panel, reported: &[Option<ReviewerReport>]) -> PanelOutcome {
    let reports: Vec<ReviewerReport> = (0..panel.reviewers.len())
        .map(|seat| {
            reported
                .get(seat)
                .cloned()
                .flatten()
                .unwrap_or_else(ReviewerReport::silent)
        })
        .collect();
    let passed = reports
        .iter()
        .filter(|report| report.verdict == Verdict::Pass)
        .count();
    let verdict = if passed as u64 >= u64::from(panel.quorum) {
        Verdict::Pass
    } else {
        Verdict::Fail
    };
    PanelOutcome {
        verdict,
        reason: format!(
            "panel: {passed} of {} reviewers passed, quorum {}",
            reports.len(),
            panel.quorum,
        ),
        reports,
    }
}

#[cfg(test)]
mod tests {
    use crate::flow::{
        Check, Confidence, Flow, NO_VERDICT_REPORTED, Panel, Reviewer, ReviewerReport, Verdict,
        aggregate, parse,
    };

    fn error(yaml: &str) -> String {
        parse("example", yaml).unwrap_err()
    }

    fn panel_yaml(reviewers: &str, require: &str) -> String {
        format!(
            "- name: build\n  action: agent\n  result_check:\n    panel:\n      prompt: prompts/review.md\n      reviewers: {reviewers}\n{require}"
        )
    }

    fn panel_of(seats: usize, quorum: u32) -> Panel {
        Panel {
            prompt: "prompts/review.md".into(),
            reviewers: (0..seats)
                .map(|seat| Reviewer {
                    target: format!("target{seat}"),
                    model: None,
                    effort: None,
                })
                .collect(),
            quorum,
        }
    }

    fn report(verdict: Verdict) -> Option<ReviewerReport> {
        Some(ReviewerReport {
            verdict,
            confidence: Some(Confidence::Medium),
            reason: "considered".into(),
        })
    }

    #[test]
    fn a_panel_parses_with_its_seats_and_quorum() {
        let flow = parse(
            "example",
            &panel_yaml(
                "[{ target: claude }, { target: codex, model: gpt, effort: high }]",
                "      require: { quorum: 1 }\n",
            ),
        )
        .unwrap();

        assert_eq!(
            flow.stages[0].result_check,
            Check::Panel(Panel {
                prompt: "prompts/review.md".into(),
                reviewers: vec![
                    Reviewer {
                        target: "claude".into(),
                        model: None,
                        effort: None,
                    },
                    Reviewer {
                        target: "codex".into(),
                        model: Some("gpt".into()),
                        effort: Some("high".into()),
                    },
                ],
                quorum: 1,
            })
        );
    }

    /// A rule nobody wrote down must not silently be the most permissive one
    /// it could have been.
    #[test]
    fn an_unstated_quorum_is_unanimity() {
        let flow = parse(
            "example",
            &panel_yaml("[{ target: a }, { target: b }, { target: c }]", ""),
        )
        .unwrap();

        let Check::Panel(panel) = &flow.stages[0].result_check else {
            panic!("expected a panel");
        };
        assert_eq!(panel.quorum, 3);
    }

    #[test]
    fn a_panel_survives_a_snapshot_round_trip() {
        let flow = parse(
            "example",
            &panel_yaml(
                "[{ target: claude }, { target: codex }]",
                "      require: { quorum: 2 }\n",
            ),
        )
        .unwrap();

        let snapshot = serde_json::to_string(&flow).unwrap();
        assert_eq!(serde_json::from_str::<Flow>(&snapshot).unwrap(), flow);
    }

    #[test]
    fn a_panel_must_seat_between_two_and_five_reviewers() {
        for reviewers in [
            "[]",
            "[{ target: a }]",
            "[{target: a}, {target: b}, {target: c}, {target: d}, {target: e}, {target: f}]",
        ] {
            let error = error(&panel_yaml(reviewers, ""));
            assert!(
                error.contains("panel must define between 2 and 5 reviewers"),
                "{error}"
            );
        }
    }

    #[test]
    fn a_panel_quorum_must_fit_its_seats() {
        for quorum in ["0", "3"] {
            let error = error(&panel_yaml(
                "[{ target: a }, { target: b }]",
                &format!("      require: {{ quorum: {quorum} }}\n"),
            ));
            assert!(error.contains("stage `build`"), "{error}");
            assert!(
                error.contains("panel quorum must be between 1 and 2"),
                "{error}"
            );
        }
    }

    #[test]
    fn a_panel_prompt_must_be_a_relative_path_inside_the_sloop_directory() {
        let missing = error(
            "- name: build\n  action: agent\n  result_check:\n    panel:\n      reviewers: [{target: a}, {target: b}]\n",
        );
        assert!(
            missing.contains("panel must define a non-empty `prompt`"),
            "{missing}"
        );

        for prompt in ["/etc/passwd", "../../secrets.md"] {
            let escaping = error(&format!(
                "- name: build\n  action: agent\n  result_check:\n    panel:\n      prompt: {prompt}\n      reviewers: [{{target: a}}, {{target: b}}]\n",
            ));
            assert!(
                escaping.contains("must be a relative path under `.agents/sloop`"),
                "{escaping}"
            );
        }
    }

    /// A panel spawns one process per seat, and the execution cap exists so
    /// nobody discovers the total at runtime.
    #[test]
    fn panel_seats_count_towards_the_worst_case_execution_budget() {
        let flow = |seats: &str| {
            format!(
                "- name: build\n  action: agent\n  result_check:\n    panel:\n      prompt: prompts/review.md\n      reviewers: {seats}\n- {{ name: lint, action: {{ exec: ['true'] }} }}\n- {{ name: audit, action: {{ exec: ['true'] }} }}\n- {{ name: test, action: {{ exec: ['true'] }}, fail_action: {{ return_to: build, attempts: 3 }} }}\n",
            )
        };

        let bounded = parse("example", &flow("[{target: a}, {target: b}, {target: c}]"));
        assert!(bounded.is_ok(), "{bounded:?}");

        let error = error(&flow(
            "[{target: a}, {target: b}, {target: c}, {target: d}, {target: e}]",
        ));
        assert!(
            error.contains("at most 32 stages in the worst case"),
            "{error}"
        );
        assert!(error.contains("imply 36"), "{error}");
    }

    /// The whole aggregation, enumerated. Three states per seat — `Pass`,
    /// `Fail`, and the silence that counts as a `Fail` — across every quorum a
    /// two- and three-seat panel can name. Nothing here touches an LLM, a
    /// clock, or the store, which is the point: the opinions are untrusted and
    /// the procedure over them is not.
    #[test]
    fn aggregation_is_a_pass_count_against_the_quorum() {
        let states = [report(Verdict::Pass), report(Verdict::Fail), None];
        for seats in [2usize, 3] {
            for quorum in 1..=seats as u32 {
                let panel = panel_of(seats, quorum);
                for combination in 0..states.len().pow(seats as u32) {
                    let reported: Vec<Option<ReviewerReport>> = (0..seats)
                        .map(|seat| states[combination / states.len().pow(seat as u32) % 3].clone())
                        .collect();
                    let passes = reported
                        .iter()
                        .filter(|report| {
                            report.as_ref().map(|report| report.verdict) == Some(Verdict::Pass)
                        })
                        .count();

                    let outcome = aggregate(&panel, &reported);

                    let expected = if passes as u32 >= quorum {
                        Verdict::Pass
                    } else {
                        Verdict::Fail
                    };
                    assert_eq!(
                        outcome.verdict, expected,
                        "seats {seats}, quorum {quorum}, reports {reported:?}"
                    );
                    assert_eq!(outcome.reports.len(), seats);
                    assert_eq!(
                        outcome.reason,
                        format!("panel: {passes} of {seats} reviewers passed, quorum {quorum}")
                    );
                }
            }
        }
    }

    /// Silence is not an abstention. A seat nobody heard from has approved
    /// nothing, and says so in the words the rest of the system already uses
    /// for an unreported verdict.
    #[test]
    fn a_silent_reviewer_fills_its_seat_with_a_fail() {
        let panel = panel_of(3, 2);

        let outcome = aggregate(
            &panel,
            &[report(Verdict::Pass), None, report(Verdict::Pass)],
        );
        assert_eq!(outcome.verdict, Verdict::Pass);
        assert_eq!(outcome.reports[1].verdict, Verdict::Fail);
        assert_eq!(outcome.reports[1].confidence, None);
        assert_eq!(outcome.reports[1].reason, NO_VERDICT_REPORTED);

        let truncated = aggregate(&panel, &[report(Verdict::Pass)]);
        assert_eq!(truncated.verdict, Verdict::Fail);
        assert_eq!(truncated.reports.len(), 3);
    }

    /// Confidence rides along as evidence and is never weighted: three
    /// low-confidence passes beat two high-confidence fails, because the
    /// quorum says so and nothing else does.
    #[test]
    fn confidence_is_recorded_but_never_weighted() {
        let panel = panel_of(3, 2);
        let sure = |verdict, confidence| {
            Some(ReviewerReport {
                verdict,
                confidence: Some(confidence),
                reason: "considered".into(),
            })
        };

        let outcome = aggregate(
            &panel,
            &[
                sure(Verdict::Pass, Confidence::Low),
                sure(Verdict::Pass, Confidence::Low),
                sure(Verdict::Fail, Confidence::High),
            ],
        );

        assert_eq!(outcome.verdict, Verdict::Pass);
        assert_eq!(outcome.reports[2].confidence, Some(Confidence::High));
    }

    /// A panel is a check, never an action, and the merge stage keeps its
    /// standing prohibition on being judged at all.
    #[test]
    fn a_panel_may_not_judge_a_merge_stage() {
        let error = error(
            "- { name: build, action: agent }\n- name: merge\n  action: { builtin: merge }\n  result_check:\n    panel:\n      prompt: prompts/review.md\n      reviewers: [{target: a}, {target: b}]\n",
        );
        assert!(error.contains("must have `result_check: none`"), "{error}");
    }
}
