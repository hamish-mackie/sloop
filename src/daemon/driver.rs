//! The per-run driver: one loop that executes every stage of a flow.
//!
//! A driver owns a FlowRun from admission to settlement. It loads the run's
//! ordered evidence log, asks [`next_step`] where the walk stands, executes the
//! stage it names, appends the resulting row (or rows), and repeats until the
//! walk completes or halts. Every stage goes through this loop — the agent
//! stage included — so nothing about a flow's shape depends on which stage is
//! executed by whom.
//!
//! The driver decides *what* runs and *when*; `src/runner` decides *how* a
//! process is supervised. The one exception is the merge builtin, which the
//! daemon performs itself under a global lock because it touches the shared
//! default-branch checkout.

use std::ffi::OsString;
use std::io::Write;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde_json::json;
use tokio::sync::{Notify, mpsc};

use crate::clock::Clock;
use crate::config::{AgentConfig, expand_agent_cmd};
use crate::db::{Db, StoreError};
use crate::domain::ticket::TicketSnapshot;
use crate::flow::{
    Actor, Builtin, Check, Confidence, FailAction, Flow, Panel, PanelOutcome, Reported, Reviewer,
    ReviewerReport, Stage, StageEvidence, Step, Verdict, VerdictSource, aggregate, next_step,
    resolve_verdict, return_trigger,
};
use crate::outcome::{ExitClass, FlowHalt, MergeOutcome, classify_exit};
use crate::run_log::stage_output_tail;
use crate::run_store::{
    Exit, ExitDenial, RunExit, RunStart, RunState, RunStore, StagePhase, StageRecord, Start,
    StartDenial,
};
use crate::runner::local::{
    create_run_worktree, launch_agent, mint_worker_credentials, process_start_time, run_exec_stage,
    run_output_path, wait_for_test_hook, worker_socket_path,
};
use crate::runner::{
    AgentLaunch, AgentProcessCheckpoint, ExecLaunch, ExecProcessCheckpoint, ExecutionEvidence,
    ProcessIdentity, RunnerError, StageExecution, StageHooks, StageOrder, WorkerCredentials,
    WorkerScope,
};
use crate::vendor::{VendorErrorClassifier, VendorErrorMatch};
use crate::worker::{
    BACKWARD_CONTEXT_LINES, FailureContext, compose_worker_prompt, panel_prompt,
    previous_attempt_block,
};

use super::dispatcher::{DispatcherState, RunEvent};
use super::logging::{LogLevel, OperationalLog};
use super::recovery::{
    MergeProcessCheckpoint, PersistedProcessStop, RecoveryClassification, classify_run_output,
    inspect_interrupted_merge, stop_interrupted_process,
};
use super::scheduler::VENDOR_COOLDOWN_MS;

static MERGE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// The evidence kind under which the driver checkpoints the process of the
/// stage it is executing right now. Exactly one is live per run, so cancel and
/// crash recovery both read the currently executing stage from it.
pub(super) const STAGE_PROCESS: &str = "stage_process";

pub(super) struct StoreStageHooks<'a> {
    run_store: &'a RunStore,
    log: &'a OperationalLog,
}

impl<'a> StoreStageHooks<'a> {
    pub(super) fn new(run_store: &'a RunStore, log: &'a OperationalLog) -> Self {
        Self { run_store, log }
    }

    fn record_stage_process(
        &self,
        run_id: &str,
        stage: &str,
        attempt: u32,
        process: ProcessIdentity,
        started_at_ms: i64,
    ) -> Result<(), StoreError> {
        self.run_store.record_stage_evidence(
            run_id,
            STAGE_PROCESS,
            &json!({
                "stage": stage,
                "attempt": attempt,
                "pid": process.pid,
                "pid_start_time": process.start_time,
                "process_group_id": process.process_group_id,
            })
            .to_string(),
            started_at_ms,
        )
    }
}

impl StageHooks for StoreStageHooks<'_> {
    type Error = StoreError;

    fn cancellation_requested(&self, run_id: &str) -> bool {
        cancelled(self.run_store, run_id, self.log)
    }

    fn record_agent_process(&self, checkpoint: &AgentProcessCheckpoint) -> Result<(), Self::Error> {
        let worktree_path = checkpoint.worktree.to_string_lossy();
        let worker_socket_path = checkpoint.worker.socket.to_string_lossy();
        let start = RunStart {
            run_id: &checkpoint.run_id,
            branch: &checkpoint.branch,
            worktree_path: &worktree_path,
            pid: checkpoint.process.pid,
            pid_start_time: checkpoint.process.start_time,
            process_group_id: checkpoint.process.process_group_id,
            worker_token: &checkpoint.worker.token,
            worker_socket_path: &worker_socket_path,
        };
        match self.run_store.start(&start, checkpoint.started_at_ms)? {
            Start::Granted => {}
            // The launch raced a rollback or a recovery that already closed
            // the run. Surfacing it as the same conflict error keeps the
            // caller's abort path unchanged.
            Start::Denied(StartDenial::NotClaimed { state }) => {
                return Err(StoreError::RunStateConflict {
                    run_id: checkpoint.run_id.clone(),
                    state,
                    requested: RunState::Running.as_str().into(),
                });
            }
        }
        // An agent stage checkpoints its process the same way every other
        // stage does, so "which stage is executing" has one answer.
        self.record_stage_process(
            &checkpoint.run_id,
            &checkpoint.stage,
            checkpoint.attempt,
            checkpoint.process,
            checkpoint.started_at_ms,
        )
    }

    fn record_exec_process(&self, checkpoint: &ExecProcessCheckpoint) -> Result<(), Self::Error> {
        self.record_stage_process(
            &checkpoint.run_id,
            &checkpoint.stage,
            checkpoint.attempt,
            checkpoint.process,
            checkpoint.started_at_ms,
        )
    }
}

/// One stage execution the driver is about to perform, as the walk named it.
///
/// `index` and `attempt` together are the log key the execution will record
/// under; `context` is present only on a re-entry and says which failure sent
/// the walk back here.
struct StageRun {
    stage: Stage,
    index: usize,
    attempt: u32,
    context: Option<FailureContext>,
}

/// One thing the driver ran and watched: a stage's action, or the independent
/// check that judges it. Each is evidence in its own right, and each appends
/// its own row to the run's stage log.
struct StageResult {
    verdict: Verdict,
    exit_code: Option<i32>,
    started_at_ms: i64,
    finished_at_ms: i64,
}

/// Facts about the run's agent, gathered once at its exit and thereafter read
/// from durable evidence. They are the run-level exit story: what the process
/// returned, what it committed, and whether the vendor rejected it.
///
/// The *first* agent stage in a flow owns them. A flow may hold several agent
/// stages, but only one of them is the run's attempt at its ticket; the rest
/// are later steps whose verdicts speak for themselves.
#[derive(Debug, Clone)]
struct AgentFacts {
    /// `Some(0)` until an agent has actually exited: with no process observed,
    /// nothing has been observed to fail.
    exit_code: Option<i32>,
    capture_complete: bool,
    commits: Vec<String>,
    commit_observation_complete: bool,
    vendor_error: Option<VendorErrorMatch>,
    cooldown_until_ms: Option<i64>,
    /// The execution of the primary agent stage whose exit checkpoint is
    /// already durable. That stage resolves from the checkpoint rather than
    /// launching a second process for the *same* execution — but a backward
    /// edge re-entering it is a different execution, and does launch.
    checkpointed_attempt: Option<u32>,
}

impl Default for AgentFacts {
    fn default() -> Self {
        Self {
            exit_code: Some(0),
            capture_complete: true,
            commits: Vec::new(),
            commit_observation_complete: true,
            vendor_error: None,
            cooldown_until_ms: None,
            checkpointed_attempt: None,
        }
    }
}

impl AgentFacts {
    /// Rebuilds the run-level exit story from durable evidence. A resumed run
    /// must settle on exactly the facts the daemon that observed the exit
    /// recorded, never on a fresh reading of a process that is long gone.
    fn from_evidence(rows: &[(String, String)], run_exit_code: Option<i64>) -> Self {
        let value = |kind: &str| {
            rows.iter()
                .find(|(candidate, _)| candidate == kind)
                .and_then(|(_, data)| serde_json::from_str::<serde_json::Value>(data).ok())
        };
        let Some(commits) = value("commits_observed") else {
            return Self::default();
        };
        let commit_observation_complete = commits["complete"].as_bool().unwrap_or(true);
        let commits = commits["oids"]
            .as_array()
            .map(|oids| {
                oids.iter()
                    .filter_map(|oid| oid.as_str().map(str::to_owned))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Self {
            exit_code: run_exit_code.and_then(|code| i32::try_from(code).ok()),
            capture_complete: !rows.iter().any(|(kind, _)| kind == "capture_incomplete"),
            commits,
            commit_observation_complete,
            vendor_error: value("vendor_error_classified")
                .and_then(|data| serde_json::from_value::<VendorErrorMatch>(data).ok()),
            cooldown_until_ms: value("vendor_error_classified")
                .and_then(|data| data["cooldown_until_ms"].as_i64()),
            // Checkpoints written before re-entries existed name no attempt,
            // and such a run only ever had a first.
            checkpointed_attempt: Some(
                value("exit_classified")
                    .and_then(|data| data["attempt"].as_u64())
                    .and_then(|attempt| u32::try_from(attempt).ok())
                    .unwrap_or(1),
            ),
        }
    }
}

/// Daemon-wide services a driver needs, cloned out of the dispatcher once so
/// the driver can run detached from it.
pub(super) struct DriverEnvironment {
    root: PathBuf,
    state_dir: PathBuf,
    runtime_dir: PathBuf,
    db_path: PathBuf,
    test_cmd: Option<Vec<String>>,
    agent: Option<AgentConfig>,
    clock: Arc<dyn Clock>,
    classifier: Arc<VendorErrorClassifier>,
    log: OperationalLog,
    output_notify: Arc<Notify>,
    shutdown: Arc<AtomicBool>,
}

impl DriverEnvironment {
    pub(super) fn from_state(state: &DispatcherState) -> Self {
        Self {
            root: state.root.clone(),
            state_dir: state.state_dir.clone(),
            runtime_dir: state.runtime_dir.clone(),
            db_path: state.state_dir.join("sloop.db"),
            test_cmd: state.flow_test_cmd.clone(),
            agent: state.agent.clone(),
            clock: state.clock.clone(),
            classifier: state.classifier.clone(),
            log: state.log.clone(),
            output_notify: state.output_notify.clone(),
            shutdown: state.shutdown_flag.clone(),
        }
    }
}

/// One run's identity and workspace: everything a driver needs that is not a
/// daemon-wide service. Built at admission, and rebuilt from the run row when a
/// restarted daemon resumes the walk.
pub(super) struct DriverPlan {
    pub(super) run_id: String,
    pub(super) ticket_id: String,
    /// The agent target the run's cooldowns and rate limits are attributed to.
    pub(super) target: String,
    pub(super) branch: String,
    pub(super) worktree: PathBuf,
    pub(super) flow: Flow,
    pub(super) ticket: Option<TicketSnapshot>,
    pub(super) recovery: Option<RecoveryClassification>,
}

/// Starts a run's driver on a blocking thread and returns immediately. The
/// driver reports back over `events`: worker sockets as they are minted, and
/// one final `Exited` when the walk is over.
pub(super) fn start_driver(
    environment: DriverEnvironment,
    plan: DriverPlan,
    events: mpsc::Sender<RunEvent>,
) {
    tokio::task::spawn_blocking(move || {
        // A driver needs its own connection. Nothing about the run has been
        // touched yet, so failing to open one is worth retrying rather than
        // settling: the daemon is still up, and storage may well come back.
        let db = loop {
            if environment.shutdown.load(Ordering::Acquire) {
                return;
            }
            match Db::open(&environment.db_path, environment.clock.now_ms()) {
                Ok(db) => break db,
                Err(error) => {
                    environment.log.emit_with_fields(
                        LogLevel::Error,
                        "sloop::driver",
                        "driver_store_open_failed",
                        json!({"run_id": plan.run_id, "error": error.to_string()}),
                    );
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        };
        let mut driver = RunDriver {
            run_store: RunStore::from_db(db),
            output_path: run_output_path(&environment.state_dir, &plan.run_id),
            flow: plan.flow.clone(),
            environment: &environment,
            plan: &plan,
            events: &events,
            agent: AgentFacts::default(),
            merge: None,
        };
        if let Some(event) = driver.walk() {
            let _ = events.blocking_send(event);
        }
    });
}

/// Whether a driver took ownership of the run it was started for.
enum Preparation {
    Ready,
    /// Something else already owns the run, so this driver settles nothing.
    NotOurs,
}

/// Why a walk stopped without reaching a verdict.
enum WalkError {
    /// The run never got off the ground; nothing about it is recorded.
    Admission(String),
    /// The driver could not carry the walk past a stage.
    Stage(String),
}

struct RunDriver<'a> {
    environment: &'a DriverEnvironment,
    plan: &'a DriverPlan,
    events: &'a mpsc::Sender<RunEvent>,
    run_store: RunStore,
    output_path: PathBuf,
    /// The run's flow with the configured implicit `test` stage spliced in.
    /// This is the flow the walk is over; nothing else sees it.
    flow: Flow,
    agent: AgentFacts,
    merge: Option<MergeOutcome>,
}

impl RunDriver<'_> {
    fn run_id(&self) -> &str {
        &self.plan.run_id
    }

    fn clock(&self) -> &dyn Clock {
        self.environment.clock.as_ref()
    }

    fn log(&self) -> &OperationalLog {
        &self.environment.log
    }

    /// Walks the run's flow to completion. Returns the settlement event, or
    /// `None` when this driver does not own the run's settlement — because
    /// another path claimed the agent exit first, or the run was already gone.
    fn walk(&mut self) -> Option<RunEvent> {
        match self.prepare() {
            Ok(Preparation::Ready) => {}
            Ok(Preparation::NotOurs) => return None,
            Err(error) => return Some(self.abandoned(error, 0)),
        }
        loop {
            if cancelled(&self.run_store, self.run_id(), self.log()) {
                return Some(self.exited(None));
            }
            let rows = match self.run_store.stage_log(self.run_id()) {
                Ok(rows) => rows,
                Err(error) => {
                    return Some(self.abandoned(WalkError::Stage(error.to_string()), usize::MAX));
                }
            };
            let log = replayable(&rows);
            let run = match next_step(&self.flow, &log) {
                Step::Run { stage, attempt } => {
                    let index = self
                        .flow
                        .stages
                        .iter()
                        .position(|candidate| candidate.name == stage.name)
                        .expect("next_step returned a stage from this flow");
                    StageRun {
                        // Only a re-entry has a failure behind it; a stage's
                        // first execution answers for nothing.
                        context: (attempt > 1)
                            .then(|| self.failure_context(&rows, &log))
                            .flatten(),
                        stage: stage.clone(),
                        index,
                        attempt,
                    }
                }
                // Every halt lands the ticket where the same failure would
                // have without an edge: a spent `return_to` budget is the
                // failure it could not repair, and says so through the stage
                // it stopped on.
                Step::Halted { failed_stage, .. } => {
                    let halt = self
                        .flow
                        .stages
                        .iter()
                        .position(|candidate| candidate.name == failed_stage)
                        .map_or(FlowHalt::LaterStage, FlowHalt::at_stage);
                    return Some(self.exited(Some(halt)));
                }
                Step::Complete => return Some(self.exited(None)),
            };
            let stage_name = run.stage.name.clone();
            let index = run.index;
            match self.execute(&run) {
                Ok(true) => {}
                // The stage resolved into another owner's hands: the agent
                // exit was claimed elsewhere, so that owner settles the run.
                Ok(false) => return None,
                Err(error) => return Some(self.abandoned(error, index)),
            }
            wait_for_test_hook(&format!("after-stage-{stage_name}"));
        }
    }

    /// Reports a walk that stopped without a verdict.
    ///
    /// A run that never got off the ground rolls its claim back and returns the
    /// ticket to the queue, because nothing about it was recorded and a retry
    /// costs nothing. A walk that stopped part-way halts instead: earlier stages
    /// really ran, and whatever they produced is preserved for review.
    fn abandoned(&self, error: WalkError, stage_index: usize) -> RunEvent {
        match error {
            WalkError::Admission(error) => {
                self.log().emit_with_fields(
                    LogLevel::Error,
                    "sloop::driver",
                    "driver_start_failed",
                    json!({"run_id": self.run_id(), "error": error}),
                );
                RunEvent::AdmissionFailed {
                    run_id: self.plan.run_id.clone(),
                    ticket_id: self.plan.ticket_id.clone(),
                    error,
                }
            }
            WalkError::Stage(error) => {
                self.log().emit_with_fields(
                    LogLevel::Error,
                    "sloop::driver",
                    "stage_execution_failed",
                    json!({
                        "run_id": self.run_id(),
                        "stage_index": stage_index,
                        "error": error,
                    }),
                );
                self.exited(Some(FlowHalt::at_stage(stage_index)))
            }
        }
    }

    /// Opens the run's workspace and takes ownership of the walk. A run this
    /// driver is resuming already has both, so only its recorded facts are
    /// read back.
    fn prepare(&mut self) -> Result<Preparation, WalkError> {
        self.flow = flow_with_implicit_test(&self.plan.flow, self.environment.test_cmd.as_deref())
            .map_err(WalkError::Admission)?;
        let run = self
            .run_store
            .run(self.run_id())
            .map_err(|error| WalkError::Admission(error.to_string()))?
            .ok_or_else(|| {
                WalkError::Admission(format!("run `{}` no longer exists", self.run_id()))
            })?;
        let state =
            RunState::parse(&run.state).map_err(|error| WalkError::Admission(error.to_string()))?;
        if state.is_terminal() {
            return Ok(Preparation::NotOurs);
        }
        if state == RunState::Claimed {
            create_run_worktree(
                &self.environment.root,
                &self.plan.worktree,
                &self.plan.branch,
            )
            .map_err(WalkError::Admission)?;
            let worktree = self.plan.worktree.to_string_lossy();
            let begun = self
                .run_store
                .begin(
                    self.run_id(),
                    &self.plan.branch,
                    &worktree,
                    self.clock().now_ms(),
                )
                .map_err(|error| WalkError::Admission(error.to_string()))?;
            // Losing the handoff means something else already moved the run on;
            // it owns the walk and this driver has nothing to settle.
            if begun != Start::Granted {
                return Ok(Preparation::NotOurs);
            }
            return Ok(Preparation::Ready);
        }
        // A resumed run settles on the exit facts the daemon that observed
        // them recorded, never on a fresh reading.
        let evidence = self
            .run_store
            .run_evidence(self.run_id())
            .map_err(|error| WalkError::Stage(error.to_string()))?;
        self.agent = AgentFacts::from_evidence(&evidence, run.exit_code);
        self.merge = self.recorded_merge();
        Ok(Preparation::Ready)
    }

    /// The merge outcome a resumed run already reached, read from the log's
    /// last resolved row for the merge stage. Any earlier one was superseded by
    /// the execution that followed it.
    fn recorded_merge(&self) -> Option<MergeOutcome> {
        let index = self
            .flow
            .stages
            .iter()
            .position(|stage| stage.action == Actor::Builtin(Builtin::Merge))?;
        let rows = self.run_store.stage_log(self.run_id()).ok()?;
        replayable(&rows)
            .iter()
            .rev()
            .find(|row| row.stage_index == index)
            .map(|row| {
                if row.verdict == Verdict::Pass {
                    MergeOutcome::Merged
                } else {
                    MergeOutcome::Diverged
                }
            })
    }

    /// Why the walk came back to a stage, read entirely out of the run's
    /// persisted log.
    ///
    /// `return_trigger` names the failure the fold jumped on; the log row and
    /// the captured output under that same `(stage, attempt)` key supply the
    /// rest. Nothing here consults live state, so a resumed run re-derives the
    /// identical context — which is what lets a re-run prompt survive a
    /// daemon restart unchanged.
    fn failure_context(
        &self,
        rows: &[StageRecord],
        log: &[StageEvidence],
    ) -> Option<FailureContext> {
        let (stage_index, attempt) = return_trigger(&self.flow, log)?;
        let record = rows.iter().rev().find(|row| {
            row.stage_index == stage_index && row.attempt == attempt && row.state.is_some()
        })?;
        let output = stage_output_tail(
            &self.output_path,
            &record.stage,
            attempt,
            BACKWARD_CONTEXT_LINES,
        )
        .unwrap_or_default();
        Some(FailureContext {
            stage: record.stage.clone(),
            attempt,
            reason: failure_reason(record),
            output,
        })
    }

    /// Executes one stage: its action, then the independent check that judges
    /// it, and finally the row (or rows) the execution earned. A stage that
    /// fails gets no second chance here — retrying is the walk's business, and
    /// it re-enters the stage through a `return_to` edge with an attempt number
    /// of its own. `false` means another owner claimed the run mid-stage.
    fn execute(&mut self, run: &StageRun) -> Result<bool, WalkError> {
        let stage = &run.stage;
        let interrupted = self
            .run_store
            .run_evidence(self.run_id())
            .map_err(|error| WalkError::Stage(error.to_string()))?;
        let merge_recovery = self
            .recover_interrupted_stage(&interrupted, stage)
            .map_err(WalkError::Stage)?;

        let Some(action) = self.run_action(run, merge_recovery)? else {
            return Ok(false);
        };
        // The action's own reading, before the result check has a say. Only an
        // independent actor that actually runs produces evidence of its own,
        // and so a second log row; the rest judge in place.
        let mut reading = action.verdict;
        let mut check = None;
        let mut panel = None;
        match &stage.result_check {
            Check::None | Check::Reported => {}
            Check::Actor(Actor::Builtin(Builtin::Commits)) => {
                if reading != Verdict::Pass
                    || !self.agent.commit_observation_complete
                    || self.agent.commits.is_empty()
                {
                    reading = Verdict::Fail;
                }
            }
            Check::Actor(Actor::Exec { cmd }) if reading == Verdict::Pass => {
                let judged = self.run_exec(&stage.name, run.attempt, cmd, None);
                reading = judged.verdict;
                check = Some(judged);
            }
            Check::Actor(Actor::Exec { .. }) => {}
            Check::Panel(configured) if reading == Verdict::Pass => {
                let (outcome, judged) =
                    self.run_panel(run, configured).map_err(WalkError::Stage)?;
                reading = outcome.verdict;
                panel = Some(outcome);
                check = Some(judged);
            }
            // The action failed on its own terms, so there is nothing left
            // for a panel to judge — and a panel is the most expensive
            // check there is. Seating five reviewers to confirm a verdict
            // already reached is tokens spent on nothing.
            Check::Panel(_) => {}
            // Parsing refuses an agent judge and both git builtins as a
            // check, so none of them can reach a run. Fail closed rather
            // than pass a stage nothing actually judged.
            Check::Actor(Actor::Agent)
            | Check::Actor(Actor::Builtin(Builtin::Merge | Builtin::Sync)) => {
                reading = Verdict::Fail;
            }
        }
        let reported = if stage.result_check == Check::Reported {
            reported_verdict(&self.run_store, self.run_id(), &stage.name, run.attempt)
                .map_err(WalkError::Stage)?
        } else {
            None
        };
        // A panel's aggregate is derived, never stored: what persists is the
        // reviewers' reports, and this reading is recomputed from them every
        // time — including by a daemon that resumed mid-walk.
        let (verdict, source, reason) = match &panel {
            Some(outcome) => (
                outcome.verdict,
                VerdictSource::Panel,
                Some(outcome.reason.clone()),
            ),
            None => resolve_verdict(&stage.result_check, reading, reported),
        };
        self.append_rows(run, verdict, source, reason, &action, check.as_ref())
            .map_err(WalkError::Stage)?;
        Ok(true)
    }

    /// Kills whatever the previous daemon left running for this stage and, for
    /// an interrupted merge, works out how much of it landed.
    fn recover_interrupted_stage(
        &self,
        interrupted: &[(String, String)],
        stage: &Stage,
    ) -> Result<Option<super::recovery::MergeRecovery>, String> {
        let stopped = stop_interrupted_process(interrupted, &stage.name)?;
        let Some((identity, disposition)) = stopped else {
            return Ok(None);
        };
        if disposition == PersistedProcessStop::LeaderMissing {
            self.log().emit_with_fields(
                LogLevel::Info,
                "sloop::recovery",
                "stale_stage_group_not_signalled",
                json!({
                    "run_id": self.run_id(),
                    "stage": stage.name,
                    "process_group_id": identity.group,
                }),
            );
        }
        let recovery = if stage.action == Actor::Builtin(Builtin::Merge) && identity.merge.is_some()
        {
            match inspect_interrupted_merge(&self.environment.root, &self.plan.branch, &identity) {
                Ok(recovery) => Some(recovery),
                Err(error) => {
                    self.log().emit_with_fields(
                        LogLevel::Error,
                        "sloop::recovery",
                        "merge_recovery_inspection_failed",
                        json!({"run_id": self.run_id(), "error": error}),
                    );
                    Some(super::recovery::MergeRecovery::UnsafePartial)
                }
            }
        } else {
            None
        };
        self.run_store
            .clear_stage_process(self.run_id())
            .map_err(|error| error.to_string())?;
        Ok(recovery)
    }

    /// Runs a stage's action. `None` means the run left this driver's hands.
    fn run_action(
        &mut self,
        run: &StageRun,
        merge_recovery: Option<super::recovery::MergeRecovery>,
    ) -> Result<Option<StageResult>, WalkError> {
        let stage = &run.stage;
        let now = || self.environment.clock.now_ms();
        Ok(Some(match &stage.action {
            Actor::Agent => match self.run_agent(run)? {
                Some(result) => result,
                None => return Ok(None),
            },
            Actor::Exec { cmd } => {
                let worker = if stage.result_check == Check::Reported {
                    Some(
                        self.issue_worker_credentials(run)
                            .map_err(WalkError::Stage)?,
                    )
                } else {
                    None
                };
                self.run_exec(&stage.name, run.attempt, cmd, worker)
            }
            // `Commits` never reaches here: parsing refuses it as an action,
            // so the builtins that act are `Merge` and `Sync`.
            Actor::Builtin(Builtin::Commits) => StageResult {
                verdict: Verdict::Fail,
                exit_code: Some(1),
                started_at_ms: now(),
                finished_at_ms: now(),
            },
            Actor::Builtin(Builtin::Merge) => self.run_merge(stage, merge_recovery),
            Actor::Builtin(Builtin::Sync) => self.run_sync(run),
        }))
    }

    /// Executes an agent stage: a supervised process in the run worktree with
    /// its own worker credentials, followed by the evidence its exit earned.
    ///
    /// The run's *first* agent stage owns the run-level exit facts, so its exit
    /// goes through the durable checkpoint that hands ownership of the rest of
    /// the walk to exactly one caller. Once that checkpoint exists the stage is
    /// resolved from it: a daemon that crashed between the exit and the log row
    /// must not run the agent twice for one execution.
    fn run_agent(&mut self, run: &StageRun) -> Result<Option<StageResult>, WalkError> {
        let stage = &run.stage;
        let primary = self.is_primary_agent_stage(run.index);
        // The checkpoint speaks for one execution. A re-entry is a different
        // execution, so an earlier attempt's exit never stands in for it.
        if primary && self.agent.checkpointed_attempt == Some(run.attempt) {
            self.agent.checkpointed_attempt = None;
            let now = self.clock().now_ms();
            return Ok(Some(StageResult {
                verdict: self.agent_verdict(),
                exit_code: self.agent.exit_code,
                started_at_ms: now,
                finished_at_ms: now,
            }));
        }
        let worker = self
            .issue_worker_credentials(run)
            .map_err(WalkError::Stage)?;
        let order = self
            .agent_stage_order(run, worker)
            .map_err(WalkError::Stage)?;
        let started_at_ms = self.clock().now_ms();
        let hooks = StoreStageHooks::new(&self.run_store, self.log());
        let notify = self.environment.output_notify.clone();
        let launched = launch_agent(
            order,
            &hooks,
            self.environment.clock.clone(),
            Arc::new(move || notify.notify_one()),
        );
        let launched = match launched {
            Ok(launched) => launched,
            Err(error) => {
                // A first stage that cannot launch has recorded nothing, so
                // the claim can still roll back and the ticket be retried whole.
                if run.index == 0 {
                    return Err(WalkError::Admission(error.to_string()));
                }
                self.log().emit_with_fields(
                    LogLevel::Error,
                    "sloop::driver",
                    "agent_stage_launch_failed",
                    json!({"run_id": self.run_id(), "stage": stage.name, "error": error.to_string()}),
                );
                return Ok(Some(StageResult {
                    verdict: Verdict::Fail,
                    exit_code: None,
                    started_at_ms,
                    finished_at_ms: self.clock().now_ms(),
                }));
            }
        };
        let pid = launched.process().pid;
        self.log().emit_with_fields(
            LogLevel::Info,
            "sloop::driver",
            "agent_stage_started",
            json!({"run_id": self.run_id(), "stage": stage.name, "pid": pid}),
        );
        let completion = launched.wait(self.clock());
        let exit_code = completion.evidence.exit_code;
        if let Some(error) = completion.wait_error {
            self.log().emit_with_fields(
                LogLevel::Error,
                "sloop::driver",
                "agent_wait_failed",
                json!({"run_id": self.run_id(), "stage": stage.name, "error": error}),
            );
        }
        if completion.evidence.stragglers_killed {
            self.log().emit_with_fields(
                LogLevel::Info,
                "sloop::driver",
                "stragglers_killed",
                json!({"run_id": self.run_id(), "stage": stage.name, "process_group_id": pid}),
            );
        }
        let mut capture_complete = completion.evidence.output_capture_complete;
        let vendor_error = match classify_run_output(
            &self.environment.classifier,
            &self.environment.state_dir,
            self.run_id(),
            exit_code,
        ) {
            Ok(classification) => classification,
            Err(error) => {
                capture_complete = false;
                self.log().emit_with_fields(
                    LogLevel::Error,
                    "sloop::driver",
                    "vendor_error_classification_failed",
                    json!({"run_id": self.run_id(), "error": error}),
                );
                None
            }
        };
        let cooldown_until_ms = vendor_error
            .as_ref()
            .filter(|error| error.class.requires_cooldown())
            .map(|_| self.clock().now_ms() + VENDOR_COOLDOWN_MS);
        let commit_observation = try_commits_on_branch(&self.environment.root, &self.plan.branch);
        let commit_observation_complete = commit_observation.is_ok();
        let commits = commit_observation.unwrap_or_default();

        if primary {
            self.agent = AgentFacts {
                exit_code,
                capture_complete,
                commits,
                commit_observation_complete,
                vendor_error,
                cooldown_until_ms,
                checkpointed_attempt: None,
            };
            if !self
                .checkpoint_agent_exit(run.attempt)
                .map_err(WalkError::Stage)?
            {
                return Ok(None);
            }
        } else {
            self.agent.commits = commits;
            self.agent.commit_observation_complete = commit_observation_complete;
            if !self.release_agent_stage().map_err(WalkError::Stage)? {
                return Ok(None);
            }
        }
        let verdict = if primary {
            self.agent_verdict()
        } else if capture_complete && classify_exit(exit_code) == ExitClass::Success {
            Verdict::Pass
        } else {
            Verdict::Fail
        };
        Ok(Some(StageResult {
            verdict,
            exit_code,
            started_at_ms: completion.evidence.started_at_ms,
            finished_at_ms: completion.evidence.finished_at_ms,
        }))
    }

    /// The first agent stage a flow declares. Its exit is the run's exit: the
    /// code recorded on the run row, the commits the ticket is credited with,
    /// and the vendor rejection that puts its target on cooldown.
    fn is_primary_agent_stage(&self, stage_index: usize) -> bool {
        self.flow
            .stages
            .iter()
            .position(|stage| stage.action == Actor::Agent)
            == Some(stage_index)
    }

    /// The agent's own reading of its run, before its result check has a say.
    /// The watchdog's durable kill intent and a vendor rejection both override
    /// the exit status, which a killed or rejected process cannot speak for.
    fn agent_verdict(&self) -> Verdict {
        let stalled = self
            .run_store
            .output_stall(self.run_id())
            .ok()
            .flatten()
            .is_some();
        if !stalled
            && self.agent.vendor_error.is_none()
            && classify_exit(self.agent.exit_code) == ExitClass::Success
        {
            Verdict::Pass
        } else {
            Verdict::Fail
        }
    }

    /// Checkpoints the run's agent exit, which is also the handoff that grants
    /// exactly one caller ownership of the rest of the walk. `false` means
    /// another one already holds it.
    fn checkpoint_agent_exit(&mut self, attempt: u32) -> Result<bool, String> {
        wait_for_test_hook("before-agent-exit-checkpoint");
        let commits_json = json!({
            "complete": self.agent.commit_observation_complete,
            "oids": self.agent.commits,
        })
        .to_string();
        let exit = RunExit {
            run_id: self.run_id(),
            attempt,
            exit_code: self.agent.exit_code,
            capture_complete: self.agent.capture_complete,
            commits_json: &commits_json,
            vendor_error: self.agent.vendor_error.as_ref(),
            cooldown_until_ms: self.agent.cooldown_until_ms,
        };
        match self.run_store.record_exit(&exit, self.clock().now_ms()) {
            Ok(Exit::Granted) => {
                wait_for_test_hook("after-agent-exit-checkpoint");
                Ok(true)
            }
            Ok(Exit::Denied(ExitDenial::AlreadyClaimed { state })) => {
                self.log().emit_with_fields(
                    LogLevel::Info,
                    "sloop::driver",
                    "exit_checkpoint_already_claimed",
                    json!({"run_id": self.run_id(), "state": state}),
                );
                Ok(false)
            }
            Err(error) => Err(error.to_string()),
        }
    }

    /// Returns a run to its driver after a non-primary agent stage. Nothing
    /// about the run's exit story changes: that belongs to the first agent
    /// stage alone.
    fn release_agent_stage(&self) -> Result<bool, String> {
        self.run_store
            .release_agent(self.run_id(), self.clock().now_ms())
            .map(|released| released == Exit::Granted)
            .map_err(|error| error.to_string())
    }

    /// Builds the launch order for an agent stage from the run's snapshotted
    /// ticket, so a resumed run spawns exactly what the original would have.
    fn agent_stage_order(
        &self,
        run: &StageRun,
        worker: WorkerCredentials,
    ) -> Result<StageOrder, String> {
        let ticket = self
            .plan
            .ticket
            .as_ref()
            .ok_or_else(|| format!("run `{}` has no ticket snapshot", self.run_id()))?;
        let agent = self
            .environment
            .agent
            .as_ref()
            .ok_or_else(|| "no agent targets configured".to_owned())?;
        let target = ticket
            .target
            .as_deref()
            .unwrap_or(agent.default_target.as_str());
        let template = agent.targets.get(target).ok_or_else(|| {
            format!(
                "ticket `{}` names unknown agent target `{target}`",
                ticket.id
            )
        })?;
        // The failure block lands after the bootstrap and the repository's
        // own instructions: it is the most recent thing that happened, not a
        // standing rule, and the ticket still frames the work.
        let prompt = match run.context.as_ref() {
            Some(context) => format!(
                "{}\n\n{}",
                compose_worker_prompt(&self.environment.root)?,
                previous_attempt_block(context),
            ),
            None => compose_worker_prompt(&self.environment.root)?,
        };
        let argv = expand_agent_cmd(
            template,
            ticket.model.as_deref(),
            ticket.effort.as_deref(),
            &prompt,
        )
        .map_err(|message| format!("ticket `{}` {message}", ticket.id))?;
        Ok(StageOrder {
            run_id: self.plan.run_id.clone(),
            stage: run.stage.name.clone(),
            attempt: run.attempt,
            execution: StageExecution::Agent(AgentLaunch {
                argv,
                environment: agent_environment(&ticket.id, self.run_id())?,
                worker,
            }),
            worktree: self.plan.worktree.clone(),
            branch: self.plan.branch.clone(),
            output_path: self.output_path.clone(),
        })
    }

    /// Runs an argv in the run worktree under the same supervision every stage
    /// process gets.
    fn run_exec(
        &self,
        stage: &str,
        attempt: u32,
        cmd: &[String],
        worker: Option<WorkerCredentials>,
    ) -> StageResult {
        let order = StageOrder {
            run_id: self.plan.run_id.clone(),
            stage: stage.into(),
            attempt,
            execution: StageExecution::Exec(ExecLaunch {
                argv: cmd.to_vec(),
                worker,
                environment: Vec::new(),
            }),
            worktree: self.plan.worktree.clone(),
            branch: self.plan.branch.clone(),
            output_path: self.output_path.clone(),
        };
        let hooks = StoreStageHooks::new(&self.run_store, self.log());
        let evidence = match run_exec_stage(&order, &hooks, self.clock()) {
            Ok(evidence) => evidence,
            Err(failure) => {
                if let RunnerError::Hook(error) = failure.error {
                    self.log().emit_with_fields(
                        LogLevel::Error,
                        "sloop::driver",
                        "stage_process_checkpoint_failed",
                        json!({"run_id": self.run_id(), "stage": stage, "error": error.to_string()}),
                    );
                }
                failure.evidence
            }
        };
        self.stage_result_from_execution(stage, evidence)
    }

    fn stage_result_from_execution(&self, stage: &str, evidence: ExecutionEvidence) -> StageResult {
        if evidence.stragglers_killed {
            self.log().emit_with_fields(
                LogLevel::Info,
                "sloop::driver",
                "stage_stragglers_killed",
                json!({
                    "run_id": self.run_id(),
                    "stage": stage,
                    "process_group_id": evidence.process.map(|process| process.process_group_id),
                }),
            );
        }
        StageResult {
            verdict: if evidence.output_capture_complete && evidence.exit_code == Some(0) {
                Verdict::Pass
            } else {
                Verdict::Fail
            },
            exit_code: evidence.exit_code,
            started_at_ms: evidence.started_at_ms,
            finished_at_ms: evidence.finished_at_ms,
        }
    }

    /// The merge builtin. It is driven as a stage like any other, but never
    /// dispatched to a runner: it touches the shared default-branch checkout,
    /// so the daemon performs it itself under the global merge lock.
    fn run_merge(
        &mut self,
        stage: &Stage,
        merge_recovery: Option<super::recovery::MergeRecovery>,
    ) -> StageResult {
        let now = self.clock().now_ms();
        match merge_recovery {
            Some(super::recovery::MergeRecovery::AlreadyCompleted) => {
                self.merge = Some(MergeOutcome::Merged);
                return StageResult {
                    verdict: Verdict::Pass,
                    exit_code: Some(0),
                    started_at_ms: now,
                    finished_at_ms: now,
                };
            }
            Some(super::recovery::MergeRecovery::UnsafePartial) => {
                self.merge = Some(MergeOutcome::Diverged);
                return StageResult {
                    verdict: Verdict::Fail,
                    exit_code: Some(1),
                    started_at_ms: now,
                    finished_at_ms: now,
                };
            }
            Some(super::recovery::MergeRecovery::Retry) | None => {}
        }
        let outcome = attempt_merge(
            &self.environment.root,
            &self.plan.branch,
            self.agent.commit_observation_complete && self.agent.commits.is_empty(),
            stage.ff_only,
            &stage.name,
            &self.run_store,
            self.run_id(),
            self.clock(),
            self.log(),
        );
        self.merge = Some(outcome);
        StageResult {
            verdict: if outcome == MergeOutcome::Merged {
                Verdict::Pass
            } else {
                Verdict::Fail
            },
            exit_code: Some(i32::from(outcome != MergeOutcome::Merged)),
            started_at_ms: now,
            finished_at_ms: self.clock().now_ms(),
        }
    }

    /// The sync builtin: integrates the default branch into the run branch,
    /// inside the run worktree, so the stages after it judge the tree the
    /// merge will land rather than one that was never assembled.
    ///
    /// It is the merge builtin's mirror image and its opposite in cost. A
    /// merge writes to the shared default-branch checkout, so the daemon
    /// performs it itself under the global lock; a sync writes only to the
    /// run's own worktree, and reads the shared checkout for exactly as long
    /// as it takes to pin one commit. The integration itself is an ordinary
    /// supervised stage process, which is what puts git's conflict output in
    /// the run log — and so in the prompt of whatever a `return_to` re-enters.
    fn run_sync(&self, run: &StageRun) -> StageResult {
        let started_at_ms = self.clock().now_ms();
        let failed = |code: i32| StageResult {
            verdict: Verdict::Fail,
            exit_code: Some(code),
            started_at_ms,
            finished_at_ms: self.clock().now_ms(),
        };
        let integrated = || StageResult {
            verdict: Verdict::Pass,
            exit_code: Some(0),
            started_at_ms,
            finished_at_ms: self.clock().now_ms(),
        };
        let worktree = self.plan.worktree.as_path();

        // Whatever the default branch is at this instant is what this sync
        // integrates. The lock is held only for the read: a sync writes
        // nothing the merge stage contends for, and holding it across the
        // integration would let one run's conflicts stall another's merge.
        let default_head = {
            let Ok(_guard) = MERGE_LOCK.lock() else {
                return failed(1);
            };
            match git_stdout(&self.environment.root, &["rev-parse", "HEAD"]) {
                Ok(head) => head,
                Err(error) => {
                    self.log().emit_with_fields(
                        LogLevel::Error,
                        "sloop::driver",
                        "sync_default_branch_unreadable",
                        json!({"run_id": self.run_id(), "error": error}),
                    );
                    return failed(1);
                }
            }
        };

        // A sync owns merge state in the run worktree: it promises to leave
        // none behind, and starts by making that true. A daemon that died
        // mid-integration is the usual author of what is found here, and the
        // branch tip it restores is a state the walk can always re-enter.
        match shared_checkout_has_git_operation(worktree) {
            Ok(true) => {
                self.log().emit_with_fields(
                    LogLevel::Info,
                    "sloop::driver",
                    "sync_aborted_leftover_merge",
                    json!({"run_id": self.run_id(), "stage": run.stage.name}),
                );
                abort_in_progress_merge(worktree);
            }
            Ok(false) => {}
            Err(error) => {
                self.log().emit_with_fields(
                    LogLevel::Error,
                    "sloop::driver",
                    "sync_worktree_unreadable",
                    json!({"run_id": self.run_id(), "error": error}),
                );
                return failed(1);
            }
        }
        if !matches!(merge_checkout_ready(worktree), Ok(true)) {
            self.log().emit_with_fields(
                LogLevel::Warn,
                "sloop::driver",
                "sync_worktree_not_ready",
                json!({"run_id": self.run_id(), "stage": run.stage.name}),
            );
            return failed(1);
        }
        // Already up to date is a pass with nothing to run: the run branch
        // holds the default branch, which is all the stage promises.
        match git_is_ancestor(worktree, &default_head, &self.plan.branch) {
            Ok(true) => return integrated(),
            Ok(false) => {}
            Err(error) => {
                self.log().emit_with_fields(
                    LogLevel::Error,
                    "sloop::driver",
                    "sync_ancestry_unreadable",
                    json!({"run_id": self.run_id(), "error": error}),
                );
                return failed(1);
            }
        }

        // The merge commit is Sloop's own action, so it carries Sloop's
        // identity — the same one `attempt_merge` signs with.
        let argv = vec![
            "git".to_owned(),
            "-c".to_owned(),
            "user.name=sloop".to_owned(),
            "-c".to_owned(),
            "user.email=sloop@sloop.invalid".to_owned(),
            "merge".to_owned(),
            "--no-edit".to_owned(),
            "-m".to_owned(),
            format!(
                "Merge the default branch into run branch '{}'",
                self.plan.branch
            ),
            default_head,
        ];
        let result = self.run_exec(&run.stage.name, run.attempt, &argv, None);
        if result.verdict == Verdict::Pass {
            return result;
        }
        // The conflict has been captured in the run log by now, so the tree
        // holding it has nothing left to say. Restoring the branch tip is what
        // lets a `return_to` target start from a clean worktree.
        abort_in_progress_merge(worktree);
        result
    }

    /// Runs a stage's panel and derives its verdict.
    ///
    /// Reviewers run **one at a time**. A panel is the only check that spawns
    /// more than one process, and running them in sequence is what keeps that
    /// from being a way around `max_parallel_tasks`: at any instant the run
    /// holds exactly the one child a single-agent stage would have held, so a
    /// five-seat panel never occupies more of the daemon than a one-seat one.
    /// It also preserves the run's single-live-worker-socket invariant, which
    /// is what lets each seat's credential be the only one that validates
    /// while that seat is speaking.
    ///
    /// A reviewer that cannot be launched at all is not fatal: its seat simply
    /// goes unreported, and the aggregation counts an unreported seat as a
    /// `Fail`. Failing closed is the point — a panel that could not be heard
    /// from has approved nothing.
    fn run_panel(
        &self,
        run: &StageRun,
        panel: &Panel,
    ) -> Result<(PanelOutcome, StageResult), String> {
        let started_at_ms = self.clock().now_ms();
        let prompt = panel_prompt(&self.environment.root, panel)?;
        // A crash part-way through a panel re-runs the stage, but the seats
        // that already reported are append-only and one-shot: their reports
        // stand, and re-spawning those reviewers could only burn tokens to be
        // refused. Silent seats are the ones still owed a hearing.
        let filed = self.panel_reports(run, panel.reviewers.len())?;
        for (index, reviewer) in panel.reviewers.iter().enumerate() {
            if cancelled(&self.run_store, self.run_id(), self.log()) {
                break;
            }
            if filed[index].is_some() {
                self.log().emit_with_fields(
                    LogLevel::Info,
                    "sloop::driver",
                    "panel_reviewer_already_reported",
                    json!({
                        "run_id": self.run_id(),
                        "stage": run.stage.name,
                        "attempt": run.attempt,
                        "reviewer": index,
                    }),
                );
                continue;
            }
            self.log().emit_with_fields(
                LogLevel::Info,
                "sloop::driver",
                "panel_reviewer_spawned",
                json!({
                    "run_id": self.run_id(),
                    "stage": run.stage.name,
                    "attempt": run.attempt,
                    "reviewer": index,
                    "target": reviewer.target,
                }),
            );
            if let Err(error) = self.run_reviewer(run, &prompt, index, reviewer) {
                self.log().emit_with_fields(
                    LogLevel::Error,
                    "sloop::driver",
                    "panel_reviewer_failed",
                    json!({
                        "run_id": self.run_id(),
                        "stage": run.stage.name,
                        "reviewer": index,
                        "error": error,
                    }),
                );
            }
        }
        let reported = self.panel_reports(run, panel.reviewers.len())?;
        let outcome = aggregate(panel, &reported);
        let judged = StageResult {
            verdict: outcome.verdict,
            // No single process spoke for the panel, and inventing a code for
            // the aggregate would read as one that did.
            exit_code: None,
            started_at_ms,
            finished_at_ms: self.clock().now_ms(),
        };
        Ok((outcome, judged))
    }

    /// Spawns one reviewer in the run worktree, holding a credential that
    /// authorises exactly one report against its own seat.
    ///
    /// A seat's model and effort default to its *target's*, not the ticket's:
    /// the ticket says how the work should be done, and a panel is about who
    /// judges it.
    fn run_reviewer(
        &self,
        run: &StageRun,
        prompt: &str,
        index: usize,
        reviewer: &Reviewer,
    ) -> Result<(), String> {
        let agent = self
            .environment
            .agent
            .as_ref()
            .ok_or_else(|| "no agent targets configured".to_owned())?;
        let ticket = self
            .plan
            .ticket
            .as_ref()
            .ok_or_else(|| "the run has no ticket snapshot".to_owned())?;
        let template = agent.targets.get(&reviewer.target).ok_or_else(|| {
            format!(
                "panel reviewer target `{}` is not a configured agent target",
                reviewer.target
            )
        })?;
        let argv = expand_agent_cmd(
            template,
            reviewer.model.as_deref(),
            reviewer.effort.as_deref(),
            prompt,
        )
        .map_err(|message| format!("panel reviewer target `{}` {message}", reviewer.target))?;
        let worker = self.issue_credentials(WorkerScope::PanelReviewer {
            stage: run.stage.name.clone(),
            stage_index: run.index,
            attempt: run.attempt,
            reviewer_index: index,
        })?;
        let order = StageOrder {
            run_id: self.plan.run_id.clone(),
            stage: run.stage.name.clone(),
            attempt: run.attempt,
            execution: StageExecution::Exec(ExecLaunch {
                argv,
                worker: Some(worker),
                environment: agent_environment(&ticket.id, self.run_id())?,
            }),
            worktree: self.plan.worktree.clone(),
            branch: self.plan.branch.clone(),
            output_path: self.output_path.clone(),
        };
        let hooks = StoreStageHooks::new(&self.run_store, self.log());
        // The exit is deliberately ignored. A reviewer's report is its verdict
        // and its exit says nothing: `claude --print` exits 0 whatever it
        // concluded, which is exactly why a panel is not judged by exit codes.
        match run_exec_stage(&order, &hooks, self.clock()) {
            Ok(_) => Ok(()),
            Err(failure) => Err(failure.error.to_string()),
        }
    }

    /// The reports this execution's seats have filed, indexed by seat.
    ///
    /// Read back out of durable evidence rather than collected in memory as
    /// the reviewers exit: that is what makes the aggregate reproducible after
    /// a restart, and what keeps `(stage_index, attempt)` — not "the reviewers
    /// this driver happened to run" — the thing a report belongs to.
    fn panel_reports(
        &self,
        run: &StageRun,
        seats: usize,
    ) -> Result<Vec<Option<ReviewerReport>>, String> {
        let rows = self
            .run_store
            .run_evidence(self.run_id())
            .map_err(|error| error.to_string())?;
        Ok(panel_reports(&rows, run.index, run.attempt, seats))
    }

    /// Mints the worker credentials for the stage about to execute and hands
    /// the dispatcher the socket to serve them on. A fresh token per stage
    /// execution is what scopes a worker's authority to the stage it is
    /// running: an earlier stage's token stops validating the moment this one
    /// is issued.
    ///
    /// The socket path stays per-run — macOS caps Unix socket paths at 104
    /// bytes and the run's short id is already most of the budget.
    fn issue_worker_credentials(&self, run: &StageRun) -> Result<WorkerCredentials, String> {
        self.issue_credentials(WorkerScope::Stage {
            stage: run.stage.name.clone(),
            attempt: run.attempt,
        })
    }

    fn issue_credentials(&self, scope: WorkerScope) -> Result<WorkerCredentials, String> {
        let socket = worker_socket_path(&self.environment.runtime_dir, self.run_id());
        let (worker, listener) = mint_worker_credentials(&socket, scope)?;
        self.events
            .blocking_send(RunEvent::WorkerReady {
                run_id: self.plan.run_id.clone(),
                worker: worker.clone(),
                listener,
            })
            .map_err(|_| "the dispatcher stopped accepting worker sockets".to_owned())?;
        Ok(worker)
    }

    /// The execution's rows, in the order it produced them. The resolved
    /// verdict rides on the last of them, so a check that ran carries it and
    /// the action it judged keeps only its own exit; a stage judged without a
    /// second process records one row that carries both.
    #[allow(clippy::too_many_arguments)]
    fn append_rows(
        &self,
        run: &StageRun,
        verdict: Verdict,
        source: VerdictSource,
        reason: Option<String>,
        action: &StageResult,
        check: Option<&StageResult>,
    ) -> Result<(), String> {
        let output_ref = format!("runs/{}/output.ndjson", self.run_id());
        let row = |phase: StagePhase, result: &StageResult, resolved: bool| StageRecord {
            stage_index: run.index,
            stage: run.stage.name.clone(),
            attempt: run.attempt,
            phase,
            state: resolved.then(|| match verdict {
                Verdict::Pass => "passed".to_owned(),
                Verdict::Fail => "failed".to_owned(),
            }),
            started_at_ms: result.started_at_ms,
            finished_at_ms: result.finished_at_ms,
            exit_code: result.exit_code,
            output_ref: output_ref.clone(),
            verdict_source: resolved.then(|| source.as_str().to_owned()),
            reason: resolved.then(|| reason.clone()).flatten(),
        };
        let rows = match check {
            Some(check) => vec![
                row(StagePhase::Action, action, false),
                row(StagePhase::Check, check, true),
            ],
            None => vec![row(StagePhase::Action, action, true)],
        };
        self.run_store
            .append_stage_rows(self.run_id(), &rows)
            .map_err(|error| format!("cannot record stage rows: {error}"))?;
        self.run_store
            .clear_stage_process(self.run_id())
            .map_err(|error| format!("cannot clear the stage process checkpoint: {error}"))
    }

    fn exited(&self, halt: Option<FlowHalt>) -> RunEvent {
        RunEvent::Exited {
            run_id: self.plan.run_id.clone(),
            target: self.plan.target.clone(),
            exit_code: self.agent.exit_code,
            capture_complete: self.agent.capture_complete,
            commits: self.agent.commits.clone(),
            commit_observation_complete: self.agent.commit_observation_complete,
            halt,
            merge: self.merge,
            vendor_error: self.agent.vendor_error.clone(),
            cooldown_until_ms: self.agent.cooldown_until_ms,
            recovery: self.plan.recovery,
        }
    }
}

/// The environment every process Sloop spawns on a run's behalf shares: the run
/// and ticket it belongs to, and a PATH that finds the `sloop` binary the
/// daemon is running from.
fn agent_environment(ticket_id: &str, run_id: &str) -> Result<Vec<(OsString, OsString)>, String> {
    let executable = std::env::current_exe()
        .map_err(|source| format!("cannot locate sloop executable: {source}"))?;
    let executable_dir = executable
        .parent()
        .ok_or_else(|| "sloop executable has no parent directory".to_owned())?;
    let mut path_entries = vec![executable_dir.to_path_buf()];
    if let Some(path) = std::env::var_os("PATH") {
        path_entries.extend(std::env::split_paths(&path));
    }
    let path = std::env::join_paths(path_entries)
        .map_err(|source| format!("cannot construct agent PATH: {source}"))?;
    Ok(vec![
        (OsString::from("SLOOP_RUN_ID"), OsString::from(run_id)),
        (OsString::from("SLOOP_TICKET_ID"), OsString::from(ticket_id)),
        (
            OsString::from("SLOOP_BIN"),
            executable.as_os_str().to_owned(),
        ),
        (OsString::from("PATH"), path),
    ])
}

pub(super) fn cancelled(run_store: &RunStore, run_id: &str, log: &OperationalLog) -> bool {
    match run_store.cancellation_requested(run_id) {
        Ok(cancelled) => cancelled,
        Err(error) => {
            log.emit_with_fields(
                LogLevel::Error,
                "sloop::driver",
                "cancellation_read_failed",
                json!({"run_id": run_id, "error": error.to_string()}),
            );
            true
        }
    }
}

/// How a resolved stage row failed, in one clause fit to quote back to an
/// agent. A reported verdict carries the reviewer's own words; everything else
/// is judged by an exit status, and the status is what there is to say.
fn failure_reason(row: &StageRecord) -> String {
    match row.reason.as_deref().filter(|text| !text.trim().is_empty()) {
        Some(reason) => reason.to_owned(),
        None => match row.exit_code {
            Some(code) => format!("exit {code}"),
            None => "the process was killed before it exited".to_owned(),
        },
    }
}

/// The fold's view of a run's stage-evidence log: the rows that resolved an
/// execution, in log order.
///
/// A row without a resolved verdict is an execution's earlier phase — its
/// action, with the check that judges it still to come — so the fold skips it
/// and the stage re-runs whole. That is exactly what a crash between an action
/// and its check should cost: the check never judged anything, so the walk
/// cannot stand past it.
pub(super) fn replayable(log: &[StageRecord]) -> Vec<StageEvidence> {
    log.iter()
        .filter_map(|row| {
            Some(StageEvidence {
                stage: row.stage.clone(),
                stage_index: row.stage_index,
                attempt: row.attempt,
                verdict: if row.state.as_deref()? == "passed" {
                    Verdict::Pass
                } else {
                    Verdict::Fail
                },
                source: match row.verdict_source.as_deref() {
                    Some("reported") => VerdictSource::Reported,
                    Some("panel") => VerdictSource::Panel,
                    _ => VerdictSource::ExitCode,
                },
                reason: row.reason.clone(),
            })
        })
        .collect()
}

/// The panel reports one stage execution's seats have filed, indexed by seat.
///
/// The one place the `panel_report` evidence shape is read. The driver derives
/// a verdict from it and `show` renders it, and they must agree exactly — the
/// aggregate is never stored, so a divergence here would show an operator a
/// tally the walk never used.
///
/// Rows are matched on `(stage_index, attempt)`: a `return_to` re-run reports
/// onto its own attempt, so an earlier round's reports can never be counted
/// towards a later one.
pub(super) fn panel_reports(
    evidence: &[(String, String)],
    stage_index: usize,
    attempt: u32,
    seats: usize,
) -> Vec<Option<ReviewerReport>> {
    let mut reports = vec![None; seats];
    for (_, data) in evidence.iter().filter(|(kind, _)| kind == "panel_report") {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(data) else {
            continue;
        };
        if value["stage_index"].as_u64() != Some(stage_index as u64)
            || value["attempt"].as_u64() != Some(u64::from(attempt))
        {
            continue;
        }
        let Some(slot) = value["reviewer"]
            .as_u64()
            .and_then(|seat| usize::try_from(seat).ok())
            .and_then(|seat| reports.get_mut(seat))
        else {
            continue;
        };
        let verdict = match value["verdict"].as_str() {
            Some("pass") => Verdict::Pass,
            Some("fail") => Verdict::Fail,
            _ => continue,
        };
        *slot = Some(ReviewerReport {
            verdict,
            confidence: value["confidence"].as_str().and_then(Confidence::parse),
            reason: value["reason"].as_str().unwrap_or_default().to_owned(),
        });
    }
    reports
}

fn flow_with_implicit_test(flow: &Flow, test_cmd: Option<&[String]>) -> Result<Flow, String> {
    let mut flow = flow.clone();
    if let Some(cmd) = test_cmd {
        if flow.stages.iter().any(|stage| stage.name == "test") {
            return Err("flow.test_cmd conflicts with flow stage `test`".into());
        }
        flow.stages.insert(
            1.min(flow.stages.len()),
            Stage {
                name: "test".into(),
                action: Actor::Exec { cmd: cmd.to_vec() },
                result_check: Check::None,
                fail_action: FailAction::Halt,
                ff_only: false,
            },
        );
    }
    Ok(flow)
}

fn reported_verdict(
    run_store: &RunStore,
    run_id: &str,
    stage: &str,
    attempt: u32,
) -> Result<Option<Reported>, String> {
    let rows = run_store
        .run_evidence(run_id)
        .map_err(|error| error.to_string())?;
    let Some(data) = rows
        .iter()
        .rev()
        .filter(|(kind, _)| kind == "stage_verdict")
        .find_map(|(_, data)| {
            let value = serde_json::from_str::<serde_json::Value>(data).ok()?;
            // A report written before re-entries existed names no attempt and
            // belongs to the only execution its stage had.
            let reported = value["attempt"].as_u64().unwrap_or(1);
            (value["stage"] == stage && reported == u64::from(attempt)).then_some(value)
        })
    else {
        return Ok(None);
    };
    let verdict = match data["verdict"].as_str() {
        Some("pass") => Verdict::Pass,
        Some("fail") => Verdict::Fail,
        _ => {
            return Err(format!(
                "stage `{stage}` has invalid reported verdict evidence"
            ));
        }
    };
    Ok(Some(Reported {
        verdict,
        reason: data["reason"].as_str().map(str::to_owned),
    }))
}

/// Commits made since the run branch was created. The branch's own reflog is
/// the stable baseline, so rewriting the default branch cannot change this
/// activity metadata.
pub(super) fn try_commits_on_branch(root: &Path, branch: &str) -> Result<Vec<String>, String> {
    let start = git_stdout(root, &["reflog", "show", "--format=%H", branch])?
        .lines()
        .last()
        .map(str::to_owned)
        .ok_or_else(|| format!("branch `{branch}` has no reflog"))?;
    git_stdout(
        root,
        &["rev-list", "--reverse", &format!("{start}..{branch}")],
    )
    .map(|output| output.lines().map(str::to_owned).collect())
}

/// Attempts the policy merge into the default branch: fast-forward when
/// possible, otherwise a merge commit. Failed merges leave the exact checkout
/// state for human review; Sloop never guesses which post-merge edits it owns.
///
/// With `ff_only` the merge commit is off the table: the default branch either
/// fast-forwards to the run branch or the stage fails, and git leaves the
/// checkout untouched in the second case. That refusal is the point. A
/// fast-forward can only succeed while the default branch is still the one an
/// earlier sync integrated, so the run branch a flow verified is provably the
/// tree that lands — and a default branch that moved in between trips the
/// stage instead of quietly merging something no stage ever tested.
#[allow(clippy::too_many_arguments)]
pub(super) fn attempt_merge(
    root: &Path,
    branch: &str,
    branch_unchanged: bool,
    ff_only: bool,
    stage: &str,
    run_store: &RunStore,
    run_id: &str,
    clock: &dyn Clock,
    operational_log: &OperationalLog,
) -> MergeOutcome {
    if branch_unchanged {
        return MergeOutcome::Merged;
    }
    let Ok(_guard) = MERGE_LOCK.lock() else {
        return MergeOutcome::Diverged;
    };
    let Ok(true) = merge_checkout_ready(root) else {
        return MergeOutcome::Diverged;
    };
    let Ok(target_head) = git_stdout(root, &["rev-parse", "HEAD"]) else {
        return MergeOutcome::Diverged;
    };
    let Ok(branch_tip) = git_stdout(root, &["rev-parse", branch]) else {
        return MergeOutcome::Diverged;
    };
    let message = format!("Merge run branch '{branch}'");
    // The merge commit is sloop's own action, not the operator's or the
    // agent's, so it carries sloop's identity; a fast-forward creates no
    // commit and ignores these.
    let mut command = Command::new("sh");
    command
        .args([
            "-c",
            "IFS= read -r _ || exit 125; exec git \"$@\"",
            "sloop-merge",
        ])
        .args([
            "-c",
            "user.name=sloop",
            "-c",
            "user.email=sloop@sloop.invalid",
            "merge",
            "--quiet",
        ]);
    if ff_only {
        // No commit is ever created here, so there is no message to write.
        command.arg("--ff-only");
    } else {
        command.args(["-m", &message]);
    }
    command
        .arg(&branch_tip)
        .current_dir(root)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0);
    let Ok(mut child) = command.spawn() else {
        return MergeOutcome::Diverged;
    };
    let pid = child.id();
    let mut gate = child.stdin.take().expect("merge gate stdin was piped");
    let Some(pid_start_time) = process_start_time(pid) else {
        unsafe {
            libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
        }
        let _ = child.wait();
        return MergeOutcome::Diverged;
    };
    let checkpoint = MergeProcessCheckpoint {
        target_head,
        branch_tip,
        completed_target: None,
    };
    if let Err(error) = record_merge_process_checkpoint(
        run_store,
        run_id,
        stage,
        pid,
        pid_start_time,
        &checkpoint,
        clock.now_ms(),
    ) {
        operational_log.emit_with_fields(
            LogLevel::Error,
            "sloop::driver",
            "stage_process_checkpoint_failed",
            json!({"run_id": run_id, "stage": stage, "error": error.to_string()}),
        );
        unsafe {
            libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
        }
        let _ = child.wait();
        return MergeOutcome::Diverged;
    }
    wait_for_test_hook(&format!("after-stage-process-checkpoint-{stage}"));
    if cancelled(run_store, run_id, operational_log) {
        unsafe {
            libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
        }
        let _ = child.wait();
        return MergeOutcome::Diverged;
    }
    if gate.write_all(b"run\n").is_err() {
        unsafe {
            libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
        }
        let _ = child.wait();
        return MergeOutcome::Diverged;
    }
    drop(gate);
    match child.wait() {
        Ok(status) if status.success() => {
            if let Ok(completed_target) = git_stdout(root, &["rev-parse", "HEAD"]) {
                let completed = MergeProcessCheckpoint {
                    completed_target: Some(completed_target),
                    ..checkpoint
                };
                if let Err(error) = record_merge_process_checkpoint(
                    run_store,
                    run_id,
                    stage,
                    pid,
                    pid_start_time,
                    &completed,
                    clock.now_ms(),
                ) {
                    operational_log.emit_with_fields(
                        LogLevel::Error,
                        "sloop::driver",
                        "merge_completion_checkpoint_failed",
                        json!({"run_id": run_id, "stage": stage, "error": error.to_string()}),
                    );
                }
            }
            wait_for_test_hook("after-successful-merge-process-exit");
            MergeOutcome::Merged
        }
        _ => {
            wait_for_test_hook("after-failed-merge-process-exit");
            MergeOutcome::Diverged
        }
    }
}

/// Restores a checkout after a conflicting merge so whatever runs next is not
/// wedged by the leftover `MERGE_HEAD`.
///
/// Only the sync builtin uses it, and only in the run worktree, because there
/// the conflict has already been captured in the run log and the tree holding
/// it is nobody's evidence. The shared default-branch checkout is deliberately
/// left alone: a conflicted merge stage preserves its conflict for review.
fn abort_in_progress_merge(checkout: &Path) {
    let _ = Command::new("git")
        .args(["merge", "--abort"])
        .current_dir(checkout)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

#[allow(clippy::too_many_arguments)]
fn record_merge_process_checkpoint(
    run_store: &RunStore,
    run_id: &str,
    stage: &str,
    pid: u32,
    pid_start_time: i64,
    checkpoint: &MergeProcessCheckpoint,
    now_ms: i64,
) -> Result<(), StoreError> {
    run_store.record_stage_evidence(
        run_id,
        STAGE_PROCESS,
        &json!({
            "stage": stage,
            "pid": pid,
            "pid_start_time": pid_start_time,
            "process_group_id": pid,
            "merge": {
                "target_head": checkpoint.target_head,
                "branch_tip": checkpoint.branch_tip,
                "completed_target": checkpoint.completed_target,
            },
        })
        .to_string(),
        now_ms,
    )
}

pub(super) fn git_stdout(root: &Path, args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(root)
        .output()
        .map_err(|error| error.to_string())?;
    match output {
        output if output.status.success() => {
            Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
        }
        output => Err(format!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        )),
    }
}

fn merge_checkout_ready(root: &Path) -> Result<bool, String> {
    Ok(!shared_checkout_has_git_operation(root)?
        && !git_index_lock_path(root)?.exists()
        && git_index_matches_head(root)?)
}

pub(super) fn git_is_ancestor(
    root: &Path,
    ancestor: &str,
    descendant: &str,
) -> Result<bool, String> {
    let status = Command::new("git")
        .args(["merge-base", "--is-ancestor", ancestor, descendant])
        .current_dir(root)
        .status()
        .map_err(|error| error.to_string())?;
    match status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => Err(format!(
            "git merge-base --is-ancestor {ancestor} {descendant} failed: {status}"
        )),
    }
}

pub(super) fn git_index_matches_head(root: &Path) -> Result<bool, String> {
    let status = Command::new("git")
        .args(["diff", "--cached", "--quiet", "--no-ext-diff", "HEAD", "--"])
        .current_dir(root)
        .status()
        .map_err(|error| error.to_string())?;
    match status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => Err(format!("git diff --cached --quiet failed: {status}")),
    }
}

pub(super) fn git_index_lock_path(root: &Path) -> Result<PathBuf, String> {
    git_path(root, "index.lock")
}

fn git_path(root: &Path, name: &str) -> Result<PathBuf, String> {
    let path = git_stdout(root, &["rev-parse", "--git-path", name])?;
    let path = PathBuf::from(path);
    Ok(if path.is_absolute() {
        path
    } else {
        root.join(path)
    })
}

pub(super) fn shared_checkout_has_git_operation(root: &Path) -> Result<bool, String> {
    for state in [
        "MERGE_HEAD",
        "AUTO_MERGE",
        "MERGE_MODE",
        "CHERRY_PICK_HEAD",
        "REVERT_HEAD",
        "REBASE_HEAD",
        "rebase-merge",
        "rebase-apply",
        "sequencer",
    ] {
        if git_path(root, state)?.exists() {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::{FlowHalt, StagePhase, StageRecord, failure_reason, flow_with_implicit_test};
    use crate::flow::{Actor, Flow, Stage};

    fn flow(names: &[&str]) -> Flow {
        Flow {
            name: "example".into(),
            stages: names
                .iter()
                .map(|name| Stage {
                    name: (*name).into(),
                    action: Actor::Agent,
                    result_check: crate::flow::Check::Reported,
                    fail_action: crate::flow::FailAction::Halt,
                    ff_only: false,
                })
                .collect(),
        }
    }

    #[test]
    fn the_implicit_test_stage_lands_at_index_one() {
        let spliced =
            flow_with_implicit_test(&flow(&["build", "merge"]), Some(&["true".to_owned()]))
                .unwrap();
        assert_eq!(
            spliced
                .stages
                .iter()
                .map(|stage| stage.name.as_str())
                .collect::<Vec<_>>(),
            ["build", "test", "merge"]
        );
    }

    /// A one-stage flow has no index 1 to splice at; the test stage appends
    /// rather than panicking on an out-of-range insert.
    #[test]
    fn the_implicit_test_stage_appends_to_a_single_stage_flow() {
        let spliced =
            flow_with_implicit_test(&flow(&["build"]), Some(&["true".to_owned()])).unwrap();
        assert_eq!(spliced.stages.len(), 2);
        assert_eq!(spliced.stages[1].name, "test");
    }

    #[test]
    fn the_implicit_test_stage_refuses_to_shadow_an_explicit_one() {
        let error = flow_with_implicit_test(&flow(&["build", "test"]), Some(&["true".to_owned()]))
            .unwrap_err();
        assert!(error.contains("flow.test_cmd conflicts"), "{error}");
    }

    /// The outcome mapping turns on first-versus-later, and a driver that
    /// cannot name the stage it stopped on must not claim the first.
    #[test]
    fn an_unknown_halt_position_counts_as_a_later_stage() {
        assert_eq!(FlowHalt::at_stage(usize::MAX), FlowHalt::LaterStage);
    }

    fn resolved(exit_code: Option<i32>, reason: Option<&str>) -> StageRecord {
        StageRecord {
            stage_index: 1,
            stage: "test".into(),
            attempt: 1,
            phase: StagePhase::Action,
            state: Some("failed".into()),
            started_at_ms: 0,
            finished_at_ms: 0,
            exit_code,
            output_ref: String::new(),
            verdict_source: Some("exit_code".into()),
            reason: reason.map(str::to_owned),
        }
    }

    /// A reviewer's words outrank an exit status, but a stage judged only by
    /// its exit still has something to say — and a killed process says so
    /// rather than passing off a missing code as success.
    #[test]
    fn a_failure_reason_falls_back_to_what_the_exit_said() {
        assert_eq!(
            failure_reason(&resolved(Some(0), Some("changes requested"))),
            "changes requested"
        );
        assert_eq!(failure_reason(&resolved(Some(1), None)), "exit 1");
        assert_eq!(failure_reason(&resolved(Some(1), Some("  "))), "exit 1");
        assert_eq!(
            failure_reason(&resolved(None, None)),
            "the process was killed before it exited"
        );
    }
}
