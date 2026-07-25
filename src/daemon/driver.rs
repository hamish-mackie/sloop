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
use crate::config::{AgentConfig, RunningHours, expand_agent_cmd};
use crate::db::{Db, StoreError};
use crate::domain::ticket::TicketSnapshot;
use crate::flow::{
    Actor, Builtin, Check, FailAction, Flow, OnFail, Reported, Stage, StageEvidence, Step, Verdict,
    VerdictSource, next_step, resolve_verdict,
};
use crate::logging::{LogLevel, OperationalLog};
use crate::outcome::{ExitClass, FlowHalt, MergeOutcome, classify_exit};
use crate::run_store::{
    Exit, ExitDenial, RunExit, RunStart, RunState, RunStore, StagePhase, StageRecord, Start,
    StartDenial,
};
use crate::runner::local::{
    compose_worker_prompt, create_run_worktree, launch_agent, mint_worker_credentials,
    process_start_time, run_exec_stage, run_output_path, wait_for_test_hook, worker_socket_path,
};
use crate::runner::{
    AgentLaunch, AgentProcessCheckpoint, ExecLaunch, ExecProcessCheckpoint, ExecutionEvidence,
    ProcessIdentity, RunnerError, StageExecution, StageHooks, StageOrder, WorkerCredentials,
};
use crate::vendor_error::{VendorErrorClassifier, VendorErrorMatch};
use crate::work_state::local::LocalSqlite;

use super::dispatcher::{DispatcherState, RunEvent};
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
        process: ProcessIdentity,
        started_at_ms: i64,
    ) -> Result<(), StoreError> {
        self.run_store.record_stage_evidence(
            run_id,
            STAGE_PROCESS,
            &json!({
                "stage": stage,
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
            checkpoint.process,
            checkpoint.started_at_ms,
        )
    }

    fn record_exec_process(&self, checkpoint: &ExecProcessCheckpoint) -> Result<(), Self::Error> {
        self.record_stage_process(
            &checkpoint.run_id,
            &checkpoint.stage,
            checkpoint.process,
            checkpoint.started_at_ms,
        )
    }
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
/// are review or repair steps whose verdicts speak for themselves.
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
    /// The exit checkpoint is already durable, so the agent stage resolves
    /// from it rather than launching a second process for the same execution.
    checkpointed: bool,
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
            checkpointed: false,
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
            checkpointed: true,
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
    running_hours: Option<RunningHours>,
    max_parallel_tasks: usize,
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
            running_hours: state.running_hours.clone(),
            max_parallel_tasks: state.max_agents,
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
            run_store: RunStore::from_db(db.clone()),
            local_work_state: LocalSqlite::from_db_with_clock(db, environment.clock.clone()),
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
    local_work_state: LocalSqlite,
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
            let log = match self.run_store.stage_log(self.run_id()) {
                Ok(rows) => replayable(&rows),
                Err(error) => {
                    return Some(self.abandoned(WalkError::Stage(error.to_string()), usize::MAX));
                }
            };
            let (stage, attempt, stage_index) = match next_step(&self.flow, &log) {
                Step::Run { stage, attempt } => {
                    let index = self
                        .flow
                        .stages
                        .iter()
                        .position(|candidate| candidate.name == stage.name)
                        .expect("next_step returned a stage from this flow");
                    (stage.clone(), attempt, index)
                }
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
            match self.execute(&stage, stage_index, attempt) {
                Ok(true) => {}
                // The stage resolved into another owner's hands: the agent
                // exit was claimed elsewhere, so that owner settles the run.
                Ok(false) => return None,
                Err(error) => return Some(self.abandoned(error, stage_index)),
            }
            wait_for_test_hook(&format!("after-stage-{}", stage.name));
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

    /// Executes one stage: its action, then the independent check that judges
    /// it, then any repair-and-retry cycles its `on_fail` allows, and finally
    /// the row (or rows) the execution earned. `false` means another owner
    /// claimed the run mid-stage.
    fn execute(
        &mut self,
        stage: &Stage,
        stage_index: usize,
        attempt: u32,
    ) -> Result<bool, WalkError> {
        let interrupted = self
            .run_store
            .run_evidence(self.run_id())
            .map_err(|error| WalkError::Stage(error.to_string()))?;
        let mut merge_recovery = self
            .recover_interrupted_stage(&interrupted, stage)
            .map_err(WalkError::Stage)?;

        // Each `on_fail` stage may run up to `attempts` repair-then-retry
        // cycles. The repair agent never produces the verdict: after it exits
        // the stage is re-run and its own verdict policy re-applied, and that
        // re-run is the only evidence.
        let mut repair_used = repair_attempts_used(&interrupted, &stage.name);
        let mut pending_repair: Option<(u32, ProcessIdentity, String)> = None;
        let (verdict, source, reason, action, check) = loop {
            let Some(action) = self.run_action(stage, stage_index, merge_recovery)? else {
                return Ok(false);
            };
            // The action's own reading, before the result check has a say.
            // Only an independent actor that actually runs produces evidence
            // of its own, and so a second log row; the rest judge in place.
            let mut reading = action.verdict;
            let mut check = None;
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
                    let judged = self.run_exec(&stage.name, cmd, None);
                    reading = judged.verdict;
                    check = Some(judged);
                }
                Check::Actor(Actor::Exec { .. }) => {}
                // Parsing refuses an agent judge and the merge builtin as a
                // check, so neither can reach a run. Fail closed rather than
                // pass a stage nothing actually judged.
                Check::Actor(Actor::Agent) | Check::Actor(Actor::Builtin(Builtin::Merge)) => {
                    reading = Verdict::Fail;
                }
            }
            let reported = if stage.result_check == Check::Reported {
                reported_verdict(&self.run_store, self.run_id(), &stage.name)
                    .map_err(WalkError::Stage)?
            } else {
                None
            };
            let (verdict, source, reason) = resolve_verdict(&stage.result_check, reading, reported);
            // Fill in the verdict of the re-run that followed the last repair.
            if let Some((repair_attempt, identity, target)) = pending_repair.take() {
                let _ = self.run_store.record_repair_attempt(
                    self.run_id(),
                    &stage.name,
                    repair_attempt,
                    &repair_attempt_json(
                        &stage.name,
                        repair_attempt,
                        &target,
                        Some(identity),
                        Some(verdict),
                    ),
                    self.clock().now_ms(),
                );
            }
            if verdict == Verdict::Pass {
                break (verdict, source, reason, action, check);
            }
            match self.repair(stage, repair_used).map_err(WalkError::Stage)? {
                Some((repair_attempt, identity, target)) => {
                    repair_used = repair_attempt;
                    pending_repair = Some((repair_attempt, identity, target));
                    // A fresh retry: any interrupted-merge recovery from a
                    // crash applied only to the first execution.
                    merge_recovery = None;
                }
                None => break (verdict, source, reason, action, check),
            }
        };
        self.append_rows(
            stage,
            stage_index,
            attempt,
            verdict,
            source,
            reason,
            &action,
            check.as_ref(),
        )
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
        stage: &Stage,
        stage_index: usize,
        merge_recovery: Option<super::recovery::MergeRecovery>,
    ) -> Result<Option<StageResult>, WalkError> {
        let now = || self.environment.clock.now_ms();
        Ok(Some(match &stage.action {
            Actor::Agent => match self.run_agent(stage, stage_index)? {
                Some(result) => result,
                None => return Ok(None),
            },
            Actor::Exec { cmd } => {
                let worker = if stage.result_check == Check::Reported {
                    Some(self.issue_worker_credentials().map_err(WalkError::Stage)?)
                } else {
                    None
                };
                self.run_exec(&stage.name, cmd, worker)
            }
            // `Commits` never reaches here: parsing refuses it as an action,
            // so the only builtin that acts is `Merge`.
            Actor::Builtin(Builtin::Commits) => StageResult {
                verdict: Verdict::Fail,
                exit_code: Some(1),
                started_at_ms: now(),
                finished_at_ms: now(),
            },
            Actor::Builtin(Builtin::Merge) => self.run_merge(stage, merge_recovery),
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
    fn run_agent(
        &mut self,
        stage: &Stage,
        stage_index: usize,
    ) -> Result<Option<StageResult>, WalkError> {
        let primary = self.is_primary_agent_stage(stage_index);
        if primary && self.agent.checkpointed {
            self.agent.checkpointed = false;
            let now = self.clock().now_ms();
            return Ok(Some(StageResult {
                verdict: self.agent_verdict(),
                exit_code: self.agent.exit_code,
                started_at_ms: now,
                finished_at_ms: now,
            }));
        }
        let worker = self.issue_worker_credentials().map_err(WalkError::Stage)?;
        let order = self
            .agent_stage_order(stage, worker)
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
                if stage_index == 0 {
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
                checkpointed: false,
            };
            if !self.checkpoint_agent_exit().map_err(WalkError::Stage)? {
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
    fn checkpoint_agent_exit(&mut self) -> Result<bool, String> {
        wait_for_test_hook("before-agent-exit-checkpoint");
        let commits_json = json!({
            "complete": self.agent.commit_observation_complete,
            "oids": self.agent.commits,
        })
        .to_string();
        let exit = RunExit {
            run_id: self.run_id(),
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
        stage: &Stage,
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
        let prompt = compose_worker_prompt(&self.environment.root)?;
        let argv = expand_agent_cmd(
            template,
            ticket.model.as_deref(),
            ticket.effort.as_deref(),
            &prompt,
        )
        .map_err(|message| format!("ticket `{}` {message}", ticket.id))?;
        Ok(StageOrder {
            run_id: self.plan.run_id.clone(),
            stage: stage.name.clone(),
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
        cmd: &[String],
        worker: Option<WorkerCredentials>,
    ) -> StageResult {
        let order = StageOrder {
            run_id: self.plan.run_id.clone(),
            stage: stage.into(),
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

    /// Repairs a failed stage in place when it has a repair worker, attempts
    /// remain, and every spawn gate is open. Returns the attempt that ran, so
    /// the caller can re-run the stage and record what the retry decided.
    fn repair(
        &self,
        stage: &Stage,
        repair_used: u32,
    ) -> Result<Option<(u32, ProcessIdentity, String)>, String> {
        let (Some(on_fail), Some(agent)) =
            (stage.on_fail.as_ref(), self.environment.agent.as_ref())
        else {
            return Ok(None);
        };
        let Some(ticket) = self.plan.ticket.as_ref() else {
            return Ok(None);
        };
        if repair_used >= on_fail.attempts {
            return Ok(None);
        }
        let target = on_fail
            .target
            .clone()
            .or_else(|| ticket.target.clone())
            .unwrap_or_else(|| agent.default_target.clone());
        if !self.repair_gates_open(&target) {
            self.log().emit_with_fields(
                LogLevel::Info,
                "sloop::driver",
                "repair_gate_closed",
                json!({"run_id": self.run_id(), "stage": stage.name, "target": target}),
            );
            return Ok(None);
        }
        // A conflicting merge left the default checkout mid-merge. Restore it
        // now — only because a repair will run — so the repair's integration
        // and the retried merge start clean. An exhausted merge that never
        // reaches here keeps the conflict for review.
        if stage.action == Actor::Builtin(Builtin::Merge) {
            abort_conflicted_merge(&self.environment.root);
        }
        let attempt = repair_used + 1;
        // Record the attempt before spawning so a crash mid-repair still
        // counts it: recovery re-runs the stage, never the repair, so the
        // attempt is neither repeated nor lost.
        self.run_store
            .record_repair_attempt(
                self.run_id(),
                &stage.name,
                attempt,
                &repair_attempt_json(&stage.name, attempt, &target, None, None),
                self.clock().now_ms(),
            )
            .map_err(|error| error.to_string())?;
        match self.run_repair_agent(on_fail, &target, &stage.name, attempt) {
            Ok(identity) => Ok(Some((attempt, identity, target))),
            Err(error) => {
                self.log().emit_with_fields(
                    LogLevel::Error,
                    "sloop::driver",
                    "repair_agent_failed",
                    json!({"run_id": self.run_id(), "stage": stage.name, "error": error}),
                );
                Ok(None)
            }
        }
    }

    /// Whether a repair spawn for `target` clears the same gates a normal spawn
    /// would: running hours, the per-target cooldown, and capacity. Budget
    /// reservations are not yet enforced for any spawn, so that gate is open. A
    /// database read error closes the gate rather than risk an ungated spawn.
    fn repair_gates_open(&self, target: &str) -> bool {
        let now_ms = self.clock().now_ms();
        let hours_open = self
            .environment
            .running_hours
            .as_ref()
            .is_none_or(|hours| hours.is_open(self.clock().local_minute(now_ms)));
        if !hours_open {
            return false;
        }
        if !matches!(
            self.run_store.active_cooldown_for_target(target, now_ms),
            Ok(None)
        ) {
            return false;
        }
        // The repair runs inside an already-leased run, so that run's own lease
        // is counted here; an over-subscribed database still closes the gate.
        matches!(
            self.local_work_state.active_lease_count(),
            Ok(count) if count <= self.environment.max_parallel_tasks
        )
    }

    /// Spawns the stage's repair agent in the run worktree, captures its output
    /// to the run log, checkpoints its process for crash recovery, and waits for
    /// it to exit. The agent works in place; the caller re-runs the stage
    /// afterwards. The repair agent never reports a verdict — the retried stage
    /// is the only evidence.
    fn run_repair_agent(
        &self,
        on_fail: &OnFail,
        target: &str,
        stage: &str,
        attempt: u32,
    ) -> Result<ProcessIdentity, String> {
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
        let template = agent
            .targets
            .get(target)
            .ok_or_else(|| format!("repair target `{target}` is not a configured agent target"))?;
        let model = on_fail.model.as_deref().or(ticket.model.as_deref());
        let effort = on_fail.effort.as_deref().or(ticket.effort.as_deref());
        let argv = expand_agent_cmd(template, model, effort, &on_fail.agent)
            .map_err(|message| format!("repair target `{target}` {message}"))?;
        let order = StageOrder {
            run_id: self.plan.run_id.clone(),
            stage: stage.into(),
            execution: StageExecution::Exec(ExecLaunch {
                argv,
                worker: None,
                environment: agent_environment(&ticket.id, self.run_id())?,
            }),
            worktree: self.plan.worktree.clone(),
            branch: self.plan.branch.clone(),
            output_path: self.output_path.clone(),
        };
        self.log().emit_with_fields(
            LogLevel::Info,
            "sloop::driver",
            "repair_agent_spawned",
            json!({"run_id": self.run_id(), "stage": stage, "attempt": attempt, "target": target}),
        );
        let hooks = StoreStageHooks::new(&self.run_store, self.log());
        let evidence = match run_exec_stage(&order, &hooks, self.clock()) {
            Ok(evidence) => evidence,
            Err(failure) => failure.evidence,
        };
        evidence
            .process
            .ok_or_else(|| format!("repair agent for stage `{stage}` produced no process identity"))
    }

    /// Mints the worker credentials for the stage about to execute and hands
    /// the dispatcher the socket to serve them on. A fresh token per stage
    /// execution is what scopes a worker's authority to the stage it is
    /// running: an earlier stage's token stops validating the moment this one
    /// is issued.
    ///
    /// The socket path stays per-run — macOS caps Unix socket paths at 104
    /// bytes and the run's short id is already most of the budget.
    fn issue_worker_credentials(&self) -> Result<WorkerCredentials, String> {
        let socket = worker_socket_path(&self.environment.runtime_dir, self.run_id());
        let (worker, listener) = mint_worker_credentials(&socket)?;
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
        stage: &Stage,
        stage_index: usize,
        attempt: u32,
        verdict: Verdict,
        source: VerdictSource,
        reason: Option<String>,
        action: &StageResult,
        check: Option<&StageResult>,
    ) -> Result<(), String> {
        let output_ref = format!("runs/{}/output.ndjson", self.run_id());
        let row = |phase: StagePhase, result: &StageResult, resolved: bool| StageRecord {
            stage_index,
            stage: stage.name.clone(),
            attempt,
            phase,
            state: resolved.then(|| match verdict {
                Verdict::Pass => "passed".to_owned(),
                Verdict::Fail => "failed".to_owned(),
            }),
            started_at_ms: result.started_at_ms,
            finished_at_ms: result.finished_at_ms,
            exit_code: result.exit_code,
            output_ref: output_ref.clone(),
            verdict_source: resolved.then(|| {
                match source {
                    VerdictSource::ExitCode => "exit_code",
                    VerdictSource::Reported => "reported",
                }
                .to_owned()
            }),
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

/// The fold's view of a run's stage-evidence log: the rows that resolved an
/// execution, in log order.
///
/// A row without a resolved verdict is an execution's earlier phase — its
/// action, with the check that judges it still to come — so the fold skips it
/// and the stage re-runs whole. That is exactly what a crash between an action
/// and its check should cost: the check never judged anything, so the walk
/// cannot stand past it.
fn replayable(log: &[StageRecord]) -> Vec<StageEvidence> {
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
                source: if row.verdict_source.as_deref() == Some("reported") {
                    VerdictSource::Reported
                } else {
                    VerdictSource::ExitCode
                },
                reason: row.reason.clone(),
            })
        })
        .collect()
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
                on_fail: None,
            },
        );
    }
    Ok(flow)
}

/// Repair attempts already consumed for `stage`, recovered from durable
/// evidence so a restart never repeats or loses one.
fn repair_attempts_used(evidence: &[(String, String)], stage: &str) -> u32 {
    evidence
        .iter()
        .filter(|(kind, _)| kind == "repair_attempt")
        .filter_map(|(_, data)| serde_json::from_str::<serde_json::Value>(data).ok())
        .filter(|value| value["stage"].as_str() == Some(stage))
        .count() as u32
}

fn repair_attempt_json(
    stage: &str,
    attempt: u32,
    target: &str,
    identity: Option<ProcessIdentity>,
    retry_verdict: Option<Verdict>,
) -> String {
    json!({
        "stage": stage,
        "attempt": attempt,
        "target": target,
        "pid": identity.map(|id| id.pid),
        "pid_start_time": identity.and_then(|id| id.start_time),
        "retry_verdict": retry_verdict.map(|verdict| match verdict {
            Verdict::Pass => "pass",
            Verdict::Fail => "fail",
        }),
    })
    .to_string()
}

fn reported_verdict(
    run_store: &RunStore,
    run_id: &str,
    stage: &str,
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
            (value["stage"] == stage).then_some(value)
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
#[allow(clippy::too_many_arguments)]
pub(super) fn attempt_merge(
    root: &Path,
    branch: &str,
    branch_unchanged: bool,
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
            "-m",
            &message,
            &branch_tip,
        ])
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

/// Restores the default checkout after a conflicting merge so a following
/// `on_fail` retry is not wedged by the leftover `MERGE_HEAD`. Only used before
/// a repair actually runs: an exhausted merge preserves the conflict for review.
fn abort_conflicted_merge(root: &Path) {
    let _ = Command::new("git")
        .args(["merge", "--abort"])
        .current_dir(root)
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
    use super::{FlowHalt, flow_with_implicit_test};
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
                    on_fail: None,
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
}
