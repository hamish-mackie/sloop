//! Run and stage history, projected for `show`.
//!
//! Everything here is a read: `runs`, `events`, `stage_runs`, and
//! `run_evidence` already hold a complete account of how every run reached its
//! outcome, and nothing rendered below is written back. The point of the module
//! is that the account is *derived from that evidence* rather than from
//! anything an agent claimed — an agent that exits 0 and says it is done does
//! not make a run successful if a later stage failed, and the `reason` line
//! built here says so in those words.

use serde_json::{Value, json};

use crate::run_store::{OutputStallEvidence, RunRecord, RunTimeline, StageRecord};

use super::commands::run_lookup;
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
    /// Which execution of the stage this row is. A `return_to` edge re-enters
    /// a stage, and each re-entry is a row of its own rather than an overwrite
    /// — a loop that converged and one that never ran twice must not read the
    /// same. `0` on a stage the walk has not reached.
    attempt: u32,
    /// Total tries the execution cost, counting `on_fail` repair-then-retry
    /// cycles. Distinct from `attempt`: a repair retries a stage *within* one
    /// execution and records no row of its own.
    attempts: u32,
    started_at_ms: Option<i64>,
    finished_at_ms: Option<i64>,
    exit_code: Option<i32>,
    verdict_source: Option<String>,
    reason: Option<String>,
    silent_for_ms: Option<i64>,
    /// One entry per seat when this execution was judged by a panel, in
    /// reviewer order and including seats that never reported. Empty for every
    /// other check.
    reviewers: Vec<Value>,
    /// Where the execution sits in the run's log. Rows are rendered in flow
    /// order, but "which failure ended the run" is a question about log order,
    /// and a loop makes the two differ. `0` on a stage with no row.
    log_position: usize,
}

impl Stage {
    fn to_json(&self) -> Value {
        json!({
            "stage": self.name,
            "state": self.state,
            "attempt": self.attempt,
            "attempts": self.attempts,
            "started_at_ms": self.started_at_ms,
            "finished_at_ms": self.finished_at_ms,
            "duration_ms": self.duration_ms(),
            "exit_code": self.exit_code,
            "verdict_source": self.verdict_source,
            "reason": self.reason,
            "silent_for_ms": self.silent_for_ms,
            "reviewers": self.reviewers,
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
    stall: Option<OutputStallEvidence>,
}

/// Reads the history of several runs at once. The ticket view needs one row
/// per run, and batching the timeline read keeps that a single scan of the feed
/// rather than one per run.
pub(super) fn histories(
    state: &DispatcherState,
    runs: &[RunRecord],
) -> Result<Vec<RunHistory>, ErrorBody> {
    let ids = runs.iter().map(|run| run.id.as_str()).collect::<Vec<_>>();
    let mut timelines = run_lookup(state, |run_store| run_store.run_timelines(&ids))?;
    runs.iter()
        .map(|run| {
            let timeline = timelines.remove(&run.id).unwrap_or_default();
            history_with_timeline(state, run, timeline)
        })
        .collect()
}

/// Reads one run's history, including the timeline.
pub(super) fn history(state: &DispatcherState, run: &RunRecord) -> Result<RunHistory, ErrorBody> {
    let timeline = run_lookup(state, |run_store| {
        run_store.run_timelines(&[run.id.as_str()])
    })?
    .remove(&run.id)
    .unwrap_or_default();
    history_with_timeline(state, run, timeline)
}

fn history_with_timeline(
    state: &DispatcherState,
    run: &RunRecord,
    timeline: RunTimeline,
) -> Result<RunHistory, ErrorBody> {
    let recorded = run_lookup(state, |run_store| run_store.stage_log(&run.id))?;
    let evidence = run_lookup(state, |run_store| run_store.run_evidence(&run.id))?;
    let stall = evidence.iter().rev().find_map(|(kind, data)| {
        (kind == "output_stall")
            .then(|| serde_json::from_str::<OutputStallEvidence>(data).ok())
            .flatten()
    });
    let mut stages = stages(run, &recorded, &evidence, is_terminal(&run.state));
    if run.state == "running"
        && state.supervised.contains(&run.id)
        && !state.cancelling.contains(&run.id)
        && !state.suspected_dead.contains(&run.id)
        && !state.recovering.contains(&run.id)
        && !state.pending_exits.contains_key(&run.id)
        && let Some(staleness) =
            super::scheduler::running_output_staleness(state, run, state.clock.now_ms())
        && staleness.stalled
        && let Some(stage) = stages.iter_mut().find(|stage| stage.state == "running")
    {
        stage.silent_for_ms = Some(staleness.silent_for_ms);
    }
    Ok(RunHistory {
        stages,
        state: run.state.clone(),
        exit_code: run.exit_code,
        commits: observed_commits(&evidence),
        stall,
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
            .map(|stage| {
                json!({
                    "stage": stage.name,
                    "state": stage.state,
                    "attempt": stage.attempt,
                    "silent_for_ms": stage.silent_for_ms,
                })
            })
            .collect()
    }

    /// Why a run ended where it did, in one line, computed from stored stage
    /// and evidence rows.
    ///
    /// `merged` runs need no explanation and live runs have not earned one yet,
    /// so both return `None`. Everything else names the failure the run ended
    /// on — the *last* one recorded, because a walk stops on the failure it
    /// cannot get past, and any earlier one was either advisory or superseded
    /// by a re-run that followed it — and then, when the failure came after
    /// the agent, says what the agent itself did. That trailing clause is the
    /// whole point: the smoke test that motivated this feature saw `exit: 0`
    /// and concluded the run had succeeded, when in fact the agent had
    /// succeeded and a later stage had not.
    pub(super) fn derived_reason(&self) -> Option<String> {
        if self.state == "merged" || !is_terminal(&self.state) {
            return None;
        }
        if let Some(stall) = &self.stall {
            return Some(format!(
                "stalled: no output for {}",
                format_duration(stall.threshold_ms)
            ));
        }
        let Some(failed) = self
            .stages
            .iter()
            .filter(|stage| stage.state == "failed")
            .max_by_key(|stage| stage.log_position)
        else {
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
        // Two different budgets can be spent on one stage: `return_to` gives
        // it whole re-entries, `on_fail` retries within one. Each is only
        // worth saying when it was actually used.
        if failed.attempt > 1 {
            reason.push_str(&format!(" on attempt {}", failed.attempt));
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

    pub(super) fn stalled(&self) -> bool {
        self.stall.is_some()
    }

    fn stall_json(&self) -> Value {
        json!(self.stall)
    }
}

fn format_duration(milliseconds: i64) -> String {
    let seconds = milliseconds / 1_000;
    match (seconds / 3_600, (seconds % 3_600) / 60, seconds % 60) {
        (0, 0, seconds) => format!("{seconds}s"),
        (0, minutes, 0) => format!("{minutes}m"),
        (0, minutes, seconds) => format!("{minutes}m{seconds}s"),
        (hours, 0, _) => format!("{hours}h"),
        (hours, minutes, _) => format!("{hours}h{minutes}m"),
    }
}

/// Projects the recorded stage rows onto the run's admitted flow.
///
/// The flow snapshot is the source of stage *names*, so a ticket whose flow
/// file changed after the run still renders the stages that run actually had.
/// Recorded rows win wherever they exist; snapshot stages with no row are
/// `pending`, or — for the stage a live run is sitting in — `running`.
///
/// A stage a backward edge re-entered gets one row per execution, in attempt
/// order under its name. Collapsing them onto the last would erase exactly the
/// thing worth reading: that the walk went round, and what it saw the first
/// time.
///
/// Recorded rows for stages the snapshot does not name (the implicit `test`
/// stage that `flow.test_cmd` splices in at index 1) are inserted at their
/// recorded index, which is where the flow driver actually put them.
fn stages(
    run: &RunRecord,
    recorded: &[StageRecord],
    evidence: &[(String, String)],
    terminal: bool,
) -> Vec<Stage> {
    let flow = run
        .flow_json
        .as_deref()
        .and_then(|json| serde_json::from_str::<crate::flow::Flow>(json).ok());
    let mut names = flow
        .as_ref()
        .map(|flow| {
            flow.stages
                .iter()
                .map(|stage| stage.name.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    for row in recorded {
        if !names.contains(&row.stage) {
            names.insert(row.stage_index.min(names.len()), row.stage.clone());
        }
    }

    // Where the walk stands is the fold's answer, not a guess: with loops, an
    // already-recorded stage can be the one running right now, so "the first
    // stage with no row" no longer finds it. Only a run still in flight has a
    // running stage at all.
    let running =
        flow.as_ref().filter(|_| !terminal).and_then(|flow| {
            match crate::flow::next_step(flow, &super::driver::replayable(recorded)) {
                crate::flow::Step::Run { stage, attempt } => Some((stage.name.clone(), attempt)),
                _ => None,
            }
        });
    let mut running_claimed = false;

    let mut stages = Vec::with_capacity(names.len());
    for name in names {
        // Rows carrying no verdict are an execution's action, already answered
        // for by the check row behind it.
        let executions = recorded
            .iter()
            .enumerate()
            .filter(|(_, row)| row.stage == name && row.state.is_some());
        for (log_position, row) in executions {
            stages.push(Stage {
                log_position,
                state: if row.state.as_deref() == Some("passed") {
                    "passed"
                } else {
                    "failed"
                },
                attempt: row.attempt,
                attempts: 1 + repair_attempts(evidence, &name),
                started_at_ms: positive(row.started_at_ms),
                finished_at_ms: positive(row.finished_at_ms),
                exit_code: row.exit_code,
                verdict_source: row.verdict_source.clone(),
                reason: row.reason.clone(),
                silent_for_ms: None,
                reviewers: panel_reviewers(flow.as_ref(), evidence, row),
                name: name.clone(),
            });
        }
        let executed = stages.iter().any(|stage| stage.name == name);
        let running_here = match &running {
            Some((stage, attempt)) if *stage == name => Some(*attempt),
            // Without a readable flow snapshot the fold cannot say, so the
            // first unrecorded stage of a live run stands in as it always did.
            None if !terminal && !executed && !running_claimed => Some(1),
            _ => None,
        };
        if let Some(attempt) = running_here {
            running_claimed = true;
            stages.push(pending(name, "running", attempt));
        } else if !executed {
            stages.push(pending(name, "pending", 0));
        }
    }
    stages
}

/// A stage the walk has not resolved: either the one executing now or one it
/// has not reached.
fn pending(name: String, state: &'static str, attempt: u32) -> Stage {
    Stage {
        name,
        state,
        attempt,
        attempts: 0,
        started_at_ms: None,
        finished_at_ms: None,
        exit_code: None,
        verdict_source: None,
        reason: None,
        silent_for_ms: None,
        reviewers: Vec::new(),
        log_position: 0,
    }
}

/// One panel's seats as `show` reports them: who sat, what they said, and how
/// sure they were.
///
/// The list is built by running the *same* aggregation the walk ran, over the
/// same rows, so a seat that never reported appears here as the `Fail` the
/// verdict actually counted rather than as an absence the reader has to infer.
/// Nothing about the panel is stored, so this is the only way `show` and the
/// driver can be made to agree — and running the shared function is what makes
/// that agreement structural rather than a convention.
fn panel_reviewers(
    flow: Option<&crate::flow::Flow>,
    evidence: &[(String, String)],
    row: &StageRecord,
) -> Vec<Value> {
    // The panel is found by *name*: a configured `flow.test_cmd` splices a
    // stage into the flow the driver walks but not into the snapshot, so the
    // recorded index need not be an index into the snapshot. The evidence rows
    // are still keyed by the driver's index, which is what the row carries.
    let Some(crate::flow::Check::Panel(panel)) = flow
        .and_then(|flow| flow.stages.iter().find(|stage| stage.name == row.stage))
        .map(|stage| &stage.result_check)
    else {
        return Vec::new();
    };
    let reported = super::driver::panel_reports(
        evidence,
        row.stage_index,
        row.attempt,
        panel.reviewers.len(),
    );
    crate::flow::aggregate(panel, &reported)
        .reports
        .into_iter()
        .zip(&panel.reviewers)
        .enumerate()
        .map(|(seat, (report, reviewer))| {
            json!({
                "reviewer": seat,
                "target": reviewer.target,
                "verdict": match report.verdict {
                    crate::flow::Verdict::Pass => "pass",
                    crate::flow::Verdict::Fail => "fail",
                },
                "confidence": report.confidence.map(crate::flow::Confidence::as_str),
                "reason": report.reason,
            })
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
        "stall": history.stall_json(),
        "stages": history.strip_json(),
    })
}

/// Timeline plus stages for the run view, merged into the run's own object.
pub(super) fn extend_run_detail(value: &mut Value, history: &RunHistory) {
    value["claimed_at_ms"] = json!(history.timeline.claimed_at_ms);
    value["started_at_ms"] = json!(history.timeline.started_at_ms);
    value["finished_at_ms"] = json!(history.timeline.finished_at_ms);
    value["agent_exit_code"] = json!(history.agent_exit_code());
    value["stall"] = history.stall_json();
    value["stages"] = json!(history.stages_json());
}
