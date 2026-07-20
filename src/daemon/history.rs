//! Run and stage history, projected for `show`.
//!
//! Everything here is a read: `runs`, `events`, `aftercare_stages`, and
//! `run_evidence` already hold a complete account of how every run reached its
//! outcome, and nothing rendered below is written back. The point of the module
//! is that the account is *derived from that evidence* rather than from
//! anything an agent claimed — an agent that exits 0 and says it is done does
//! not make a run successful if a later stage failed, and the `reason` line
//! built here says so in those words.

use serde_json::{Value, json};

use crate::store::{RunRecord, RunTimeline, StageRecord};

use super::commands::lookup;
use super::dispatcher::DispatcherState;
use crate::protocol::ErrorBody;

/// Run states past which no further stage can run. `merged` is the only
/// successful member; the rest all want a derived `reason` explaining how the
/// run got there.
const TERMINAL_STATES: &[&str] = &[
    "merged",
    "failed",
    "needs_review",
    "cancelled",
    "rate_limited",
    "orphaned",
    "aborted",
];

pub(super) fn is_terminal(state: &str) -> bool {
    TERMINAL_STATES.contains(&state)
}

/// One flow stage as `show` reports it: either a recorded verdict or a stage
/// the run's flow declares but has not reached.
struct Stage {
    name: String,
    /// `passed`, `failed`, `running`, or `pending`.
    state: &'static str,
    /// Total tries the stage cost, counting `on_fail` repair-then-retry cycles.
    attempts: u32,
    started_at_ms: Option<i64>,
    finished_at_ms: Option<i64>,
    exit_code: Option<i32>,
    verdict_source: Option<String>,
    reason: Option<String>,
}

impl Stage {
    fn to_json(&self) -> Value {
        json!({
            "stage": self.name,
            "state": self.state,
            "attempts": self.attempts,
            "started_at_ms": self.started_at_ms,
            "finished_at_ms": self.finished_at_ms,
            "duration_ms": self.duration_ms(),
            "exit_code": self.exit_code,
            "verdict_source": self.verdict_source,
            "reason": self.reason,
        })
    }

    fn duration_ms(&self) -> Option<i64> {
        let (start, finish) = (self.started_at_ms?, self.finished_at_ms?);
        Some((finish - start).max(0))
    }
}

/// Everything `show` needs about one run's history, gathered in one pass so
/// the ticket view and the run view cannot disagree about the same run.
pub(super) struct RunHistory {
    pub(super) timeline: RunTimeline,
    stages: Vec<Stage>,
    state: String,
    exit_code: Option<i64>,
    commits: usize,
}

/// Reads the history of several runs at once. The ticket view needs one row
/// per run, and batching the timeline read keeps that a single scan of the feed
/// rather than one per run.
pub(super) fn histories(
    state: &DispatcherState,
    runs: &[RunRecord],
) -> Result<Vec<RunHistory>, ErrorBody> {
    let ids = runs.iter().map(|run| run.id.as_str()).collect::<Vec<_>>();
    let mut timelines = lookup(state, |store| store.run_timelines(&ids))?;
    runs.iter()
        .map(|run| {
            let timeline = timelines.remove(&run.id).unwrap_or_default();
            history_with_timeline(state, run, timeline)
        })
        .collect()
}

/// Reads one run's history, including the timeline.
pub(super) fn history(state: &DispatcherState, run: &RunRecord) -> Result<RunHistory, ErrorBody> {
    let timeline = lookup(state, |store| store.run_timelines(&[run.id.as_str()]))?
        .remove(&run.id)
        .unwrap_or_default();
    history_with_timeline(state, run, timeline)
}

fn history_with_timeline(
    state: &DispatcherState,
    run: &RunRecord,
    timeline: RunTimeline,
) -> Result<RunHistory, ErrorBody> {
    let recorded = lookup(state, |store| store.aftercare_stages(&run.id))?;
    let evidence = lookup(state, |store| store.run_evidence(&run.id))?;
    Ok(RunHistory {
        stages: stages(run, &recorded, &evidence, is_terminal(&run.state)),
        state: run.state.clone(),
        exit_code: run.exit_code,
        commits: observed_commits(&evidence),
        timeline,
    })
}

impl RunHistory {
    pub(super) fn stages_json(&self) -> Vec<Value> {
        self.stages.iter().map(Stage::to_json).collect()
    }

    /// The compact per-run strip the ticket view prints: stage name plus its
    /// marker, nothing else. Kept as data rather than a rendered string so the
    /// JSON envelope stays structural.
    pub(super) fn strip_json(&self) -> Vec<Value> {
        self.stages
            .iter()
            .map(|stage| json!({"stage": stage.name, "state": stage.state}))
            .collect()
    }

    /// Why a run ended where it did, in one line, computed from stored stage
    /// and evidence rows.
    ///
    /// `merged` runs need no explanation and live runs have not earned one yet,
    /// so both return `None`. Everything else names the first stage that failed
    /// — the first, because later stages never ran and the first failure is the
    /// cause — and then, when the failure came after the agent, says what the
    /// agent itself did. That trailing clause is the whole point: the smoke
    /// test that motivated this feature saw `exit: 0` and concluded the run had
    /// succeeded, when in fact the agent had succeeded and a later stage had
    /// not.
    pub(super) fn derived_reason(&self) -> Option<String> {
        if self.state == "merged" || !is_terminal(&self.state) {
            return None;
        }
        let Some(failed) = self.stages.iter().find(|stage| stage.state == "failed") else {
            return Some(format!(
                "run ended as {} with no failing stage recorded",
                self.state
            ));
        };
        let mut reason = format!("stage `{}` failed", failed.name);
        if let Some(exit_code) = failed.exit_code {
            reason.push_str(&format!(" (exit {exit_code})"));
        }
        if let Some(detail) = failed.reason.as_deref().filter(|text| !text.is_empty()) {
            reason.push_str(&format!(": {detail}"));
        }
        if failed.attempts > 1 {
            reason.push_str(&format!(" after {} attempts", failed.attempts));
        }
        // Only worth saying when the agent is not itself the failure: if the
        // agent failed, the stage line above already carries its exit.
        if self
            .stages
            .first()
            .is_some_and(|first| first.name != failed.name && first.state == "passed")
        {
            reason.push_str(if self.commits > 0 {
                " after agent completed with commits"
            } else {
                " after agent completed with no commits"
            });
        }
        Some(reason)
    }

    /// The exit code of the agent stage specifically. `runs.exit_code` is that
    /// code and always has been, but named plainly it reads as the run's exit;
    /// callers surface it under a label that cannot.
    pub(super) fn agent_exit_code(&self) -> Option<i64> {
        self.exit_code
    }
}

/// Projects the recorded stage rows onto the run's admitted flow.
///
/// The flow snapshot is the source of stage *names*, so a ticket whose flow
/// file changed after the run still renders the stages that run actually had.
/// Recorded rows win wherever they exist; snapshot stages with no row are
/// `pending`, or — for the stage a live run is sitting in — `running`.
///
/// Recorded rows for stages the snapshot does not name (the implicit `test`
/// stage that `aftercare.test_cmd` splices in at index 1) are inserted at their
/// recorded index, which is where the flow driver actually put them.
fn stages(
    run: &RunRecord,
    recorded: &[StageRecord],
    evidence: &[(String, String)],
    terminal: bool,
) -> Vec<Stage> {
    let mut names = run
        .flow_json
        .as_deref()
        .and_then(|json| serde_json::from_str::<crate::flow::Flow>(json).ok())
        .map(|flow| {
            flow.stages
                .into_iter()
                .map(|stage| stage.name)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    for row in recorded {
        if !names.contains(&row.stage) {
            names.insert(row.stage_index.min(names.len()), row.stage.clone());
        }
    }

    let mut running_claimed = false;
    names
        .into_iter()
        .map(|name| {
            let Some(row) = recorded.iter().find(|row| row.stage == name) else {
                // The first unrecorded stage of a run still in flight is the
                // one executing now; the rest genuinely have not started. A
                // terminal run has no running stage at all — its unrecorded
                // stages were skipped when an earlier one failed.
                let state = if !terminal && !running_claimed {
                    running_claimed = true;
                    "running"
                } else {
                    "pending"
                };
                return Stage {
                    name,
                    state,
                    attempts: 0,
                    started_at_ms: None,
                    finished_at_ms: None,
                    exit_code: None,
                    verdict_source: None,
                    reason: None,
                };
            };
            Stage {
                state: if row.state == "passed" {
                    "passed"
                } else {
                    "failed"
                },
                attempts: 1 + repair_attempts(evidence, &name),
                started_at_ms: positive(row.started_at_ms),
                finished_at_ms: positive(row.finished_at_ms),
                exit_code: row.exit_code,
                verdict_source: Some(row.verdict_source.clone()),
                reason: row.reason.clone(),
                name,
            }
        })
        .collect()
}

/// Repair cycles a stage consumed, counted from the durable `repair_attempt`
/// evidence rather than any in-memory counter, so a resumed run reports the
/// same total a straight-through one does.
fn repair_attempts(evidence: &[(String, String)], stage: &str) -> u32 {
    evidence
        .iter()
        .filter(|(kind, _)| kind == "repair_attempt")
        .filter_map(|(_, data)| serde_json::from_str::<Value>(data).ok())
        .filter(|data| data["stage"] == stage)
        .filter_map(|data| data["attempt"].as_u64())
        .max()
        .unwrap_or(0) as u32
}

fn observed_commits(evidence: &[(String, String)]) -> usize {
    evidence
        .iter()
        .filter(|(kind, _)| kind == "commits_observed")
        .filter_map(|(_, data)| serde_json::from_str::<Value>(data).ok())
        .filter_map(|data| data["oids"].as_array().map(Vec::len))
        .max()
        .unwrap_or(0)
}

/// Stage rows store `0` for a boundary that was never observed. Rendering that
/// as an instant in 1970 would be a lie dressed as data, so it becomes absent.
fn positive(timestamp_ms: i64) -> Option<i64> {
    (timestamp_ms > 0).then_some(timestamp_ms)
}

/// One line of the ticket view's runs section.
pub(super) fn run_summary_json(run: &RunRecord, history: &RunHistory) -> Value {
    json!({
        "id": run.id,
        "alias": crate::run_ref::alias(&run.ticket_id, run.attempt),
        "attempt": run.attempt,
        "state": run.state,
        "terminal": is_terminal(&run.state),
        "started_at_ms": history.timeline.started_at_ms.or(history.timeline.claimed_at_ms),
        "finished_at_ms": history.timeline.finished_at_ms,
        "reason": history.derived_reason(),
        "stages": history.strip_json(),
    })
}

/// Timeline plus stages for the run view, merged into the run's own object.
pub(super) fn extend_run_detail(value: &mut Value, history: &RunHistory) {
    value["claimed_at_ms"] = json!(history.timeline.claimed_at_ms);
    value["started_at_ms"] = json!(history.timeline.started_at_ms);
    value["finished_at_ms"] = json!(history.timeline.finished_at_ms);
    value["agent_exit_code"] = json!(history.agent_exit_code());
    value["stages"] = json!(history.stages_json());
}
