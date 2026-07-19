use std::ffi::OsString;
use std::io::Write;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde_json::json;

use crate::clock::Clock;
use crate::config::{AgentConfig, RunningHours, expand_agent_cmd};
use crate::domain::ticket::TicketSnapshot;
use crate::flow::{
    Flow, OnFail, Reported, Stage, StageEvidence, StageKind, Step, Verdict, VerdictPolicy,
    VerdictSource, next_step, resolve_verdict,
};
use crate::logging::{LogLevel, OperationalLog};
use crate::outcome::{ExitClass, MergeOutcome, classify_exit};
use crate::runner::local::{process_start_time, run_exec_stage, wait_for_test_hook};
use crate::runner::{
    AgentProcessCheckpoint, ExecLaunch, ExecProcessCheckpoint, ExecutionEvidence, ProcessIdentity,
    RunnerError, StageExecution, StageHooks, StageOrder, WorkerCredentials,
};
use crate::store::{ExitClaim, StageRecord, Store, StoreError};
use crate::vendor_error::VendorErrorMatch;

use super::recovery::{
    MergeProcessCheckpoint, PersistedProcessStop, inspect_interrupted_merge,
    stop_interrupted_process,
};

static MERGE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub(super) struct StoreStageHooks<'a> {
    store: &'a Store,
    log: &'a OperationalLog,
}

impl<'a> StoreStageHooks<'a> {
    pub(super) fn new(store: &'a Store, log: &'a OperationalLog) -> Self {
        Self { store, log }
    }
}

impl StageHooks for StoreStageHooks<'_> {
    type Error = StoreError;

    fn cancellation_requested(&self, run_id: &str) -> bool {
        aftercare_cancelled(self.store, run_id, self.log)
    }

    fn record_agent_process(&self, checkpoint: &AgentProcessCheckpoint) -> Result<(), Self::Error> {
        self.store.mark_run_running(
            &checkpoint.run_id,
            &checkpoint.branch,
            &checkpoint.worktree.to_string_lossy(),
            checkpoint.process.pid,
            checkpoint.process.start_time,
            checkpoint.process.process_group_id,
            &checkpoint.worker.token,
            &checkpoint.worker.socket.to_string_lossy(),
            checkpoint.started_at_ms,
        )
    }

    fn record_exec_process(&self, checkpoint: &ExecProcessCheckpoint) -> Result<(), Self::Error> {
        self.store.record_aftercare_evidence(
            &checkpoint.run_id,
            "aftercare_process",
            &json!({
                "stage": checkpoint.stage,
                "pid": checkpoint.process.pid,
                "pid_start_time": checkpoint.process.start_time,
                "process_group_id": checkpoint.process.process_group_id,
            })
            .to_string(),
            checkpoint.started_at_ms,
        )
    }
}

/// One executed flow stage as observed by the supervisor.
pub(super) struct StageResult {
    pub(super) verdict: Verdict,
    pub(super) exit_code: Option<i32>,
    pub(super) started_at_ms: i64,
    pub(super) finished_at_ms: i64,
}

/// Gathers post-exit evidence in the supervisor thread, keeping slow Git and
/// flow execution out of the dispatcher.
#[allow(clippy::too_many_arguments)]
pub(super) fn gather_exit_evidence(
    run_id: &str,
    root: &Path,
    worktree: &Path,
    branch: &str,
    flow: &Flow,
    worker: &WorkerCredentials,
    test_cmd: Option<&[String]>,
    clock: &dyn Clock,
    output_path: &Path,
    exit_code: Option<i32>,
    capture_complete: bool,
    vendor_error: Option<&VendorErrorMatch>,
    cooldown_until_ms: Option<i64>,
    mut checkpoint_store: Option<&mut Store>,
    repair: Option<&RepairContext>,
    operational_log: &OperationalLog,
) -> Option<(Vec<String>, bool, bool, Option<MergeOutcome>)> {
    let commit_observation = try_commits_on_branch(root, branch);
    let commit_observation_complete = commit_observation.is_ok();
    let commits = commit_observation.unwrap_or_default();
    wait_for_test_hook("before-agent-exit-checkpoint");
    let checkpointed = if let Some(store) = checkpoint_store.as_deref_mut() {
        match store.record_agent_exit(
            run_id,
            exit_code,
            capture_complete,
            &json!({"complete": commit_observation_complete, "oids": commits}).to_string(),
            vendor_error,
            cooldown_until_ms,
            clock.now_ms(),
        ) {
            Ok(ExitClaim::Claimed) => true,
            Ok(ExitClaim::AlreadyClaimed { state }) => {
                operational_log.emit_with_fields(
                    LogLevel::Info,
                    "sloop::supervisor",
                    "exit_checkpoint_already_claimed",
                    json!({"run_id": run_id, "state": state}),
                );
                return None;
            }
            Err(error) => {
                operational_log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::supervisor",
                    "agent_exit_checkpoint_failed",
                    json!({"run_id": run_id, "error": error.to_string()}),
                );
                false
            }
        }
    } else {
        false
    };
    if checkpointed {
        wait_for_test_hook("after-agent-exit-checkpoint");
    }
    // Tests and merge can have side effects. Without the pre-aftercare
    // checkpoint, preserve the run branch for review rather than performing
    // an action that recovery could no longer prove or resume.
    if !checkpointed
        || checkpoint_store
            .as_deref()
            .is_some_and(|store| aftercare_cancelled(store, run_id, operational_log))
    {
        return Some((commits, commit_observation_complete, false, None));
    }
    let store = checkpoint_store.as_deref()?;
    match drive_flow(
        root,
        worktree,
        branch,
        flow,
        worker,
        test_cmd,
        exit_code,
        vendor_error.is_some(),
        &commits,
        commit_observation_complete,
        output_path,
        clock,
        store,
        run_id,
        repair,
        operational_log,
    ) {
        Ok(result) => Some((
            commits,
            commit_observation_complete,
            result.aftercare_failed,
            result.merge,
        )),
        Err(error) => {
            operational_log.emit_with_fields(
                LogLevel::Error,
                "sloop::supervisor",
                "aftercare_failed",
                json!({"run_id": run_id, "error": error}),
            );
            Some((commits, commit_observation_complete, true, None))
        }
    }
}

pub(super) fn aftercare_cancelled(store: &Store, run_id: &str, log: &OperationalLog) -> bool {
    match store.cancellation_requested(run_id) {
        Ok(cancelled) => cancelled,
        Err(error) => {
            log.emit_with_fields(
                LogLevel::Error,
                "sloop::supervisor",
                "cancellation_read_failed",
                json!({"run_id": run_id, "error": error.to_string()}),
            );
            true
        }
    }
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

#[allow(clippy::too_many_arguments)]
fn execute_stage_order(
    worktree: &Path,
    branch: &str,
    stage: &str,
    cmd: &[String],
    output_path: &Path,
    clock: &dyn Clock,
    store: &Store,
    run_id: &str,
    log: &OperationalLog,
    worker: Option<&WorkerCredentials>,
) -> StageResult {
    let order = StageOrder {
        run_id: run_id.into(),
        stage: stage.into(),
        execution: StageExecution::Exec(ExecLaunch {
            argv: cmd.to_vec(),
            worker: worker.cloned(),
            environment: Vec::new(),
        }),
        worktree: worktree.into(),
        branch: branch.into(),
        output_path: output_path.into(),
    };
    let hooks = StoreStageHooks::new(store, log);
    let evidence = match run_exec_stage(&order, &hooks, clock) {
        Ok(evidence) => evidence,
        Err(failure) => {
            if let RunnerError::Hook(error) = failure.error {
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::supervisor",
                    "aftercare_process_checkpoint_failed",
                    json!({"run_id": run_id, "stage": stage, "error": error.to_string()}),
                );
            }
            failure.evidence
        }
    };
    stage_result_from_execution(run_id, stage, evidence, log)
}

fn stage_result_from_execution(
    run_id: &str,
    stage: &str,
    evidence: ExecutionEvidence,
    log: &OperationalLog,
) -> StageResult {
    if evidence.stragglers_killed {
        log.emit_with_fields(
            LogLevel::Info,
            "sloop::supervisor",
            "aftercare_stragglers_killed",
            json!({
                "run_id": run_id,
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

/// Attempts the policy merge into the default branch: fast-forward when
/// possible, otherwise a merge commit. Failed merges leave the exact checkout
/// state for human review; Sloop never guesses which post-merge edits it owns.
#[allow(clippy::too_many_arguments)]
pub(super) fn attempt_merge(
    root: &Path,
    branch: &str,
    branch_unchanged: bool,
    stage: &str,
    checkpoint_store: &Store,
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
        checkpoint_store,
        run_id,
        stage,
        pid,
        pid_start_time,
        &checkpoint,
        clock.now_ms(),
    ) {
        operational_log.emit_with_fields(
            LogLevel::Error,
            "sloop::supervisor",
            "aftercare_process_checkpoint_failed",
            json!({"run_id": run_id, "stage": stage, "error": error.to_string()}),
        );
        unsafe {
            libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
        }
        let _ = child.wait();
        return MergeOutcome::Diverged;
    }
    wait_for_test_hook(&format!("after-aftercare-process-checkpoint-{stage}"));
    if aftercare_cancelled(checkpoint_store, run_id, operational_log) {
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
                    checkpoint_store,
                    run_id,
                    stage,
                    pid,
                    pid_start_time,
                    &completed,
                    clock.now_ms(),
                ) {
                    operational_log.emit_with_fields(
                        LogLevel::Error,
                        "sloop::supervisor",
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

pub(super) struct FlowRunResult {
    pub(super) aftercare_failed: bool,
    pub(super) merge: Option<MergeOutcome>,
}

/// Everything the detached aftercare thread needs to spawn a stage's repair
/// agent and pass it through the same gates as a normal spawn. Built from
/// dispatcher state before aftercare detaches; `None` disables repair, so a
/// stage without a resolvable repair worker settles exactly as it does today.
pub(super) struct RepairContext {
    pub(super) agent: AgentConfig,
    pub(super) ticket_id: String,
    pub(super) ticket_target: Option<String>,
    pub(super) ticket_model: Option<String>,
    pub(super) ticket_effort: Option<String>,
    pub(super) running_hours: Option<RunningHours>,
    pub(super) max_parallel_tasks: usize,
}

impl RepairContext {
    /// Builds a repair context from the run's snapshotted ticket and the
    /// live spawn gates. The `on_fail` block itself rides in the flow
    /// snapshot, so a repair's behavior is frozen at post time like the flow.
    pub(super) fn new(
        agent: AgentConfig,
        ticket: &TicketSnapshot,
        running_hours: Option<RunningHours>,
        max_parallel_tasks: usize,
    ) -> Self {
        Self {
            agent,
            ticket_id: ticket.id.clone(),
            ticket_target: ticket.target.clone(),
            ticket_model: ticket.model.clone(),
            ticket_effort: ticket.effort.clone(),
            running_hours,
            max_parallel_tasks,
        }
    }
}

/// Resolves the repair worker's target: the `on_fail` override, then the
/// ticket's snapshotted target, then the configured default target.
fn resolve_repair_target(ctx: &RepairContext, on_fail: &OnFail) -> String {
    on_fail
        .target
        .clone()
        .or_else(|| ctx.ticket_target.clone())
        .unwrap_or_else(|| ctx.agent.default_target.clone())
}

/// Whether a repair spawn for `target` clears the same gates a normal spawn
/// would: running hours, the per-target cooldown, and capacity. Budget
/// reservations are not yet enforced for any spawn, so that gate is open. A
/// store read error closes the gate rather than risk an ungated spawn.
fn repair_gates_open(
    ctx: &RepairContext,
    store: &Store,
    target: &str,
    clock: &dyn Clock,
    now_ms: i64,
) -> bool {
    let hours_open = ctx
        .running_hours
        .as_ref()
        .is_none_or(|hours| hours.is_open(clock.local_minute(now_ms)));
    if !hours_open {
        return false;
    }
    match store.active_cooldown_for_target(target, now_ms) {
        Ok(None) => {}
        _ => return false,
    }
    // The repair runs inside an already-leased run, so that run's own lease is
    // counted here; an over-subscribed store still closes the gate.
    matches!(store.active_lease_count(), Ok(count) if count <= ctx.max_parallel_tasks)
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

fn repair_agent_environment(
    ctx: &RepairContext,
    run_id: &str,
) -> Result<Vec<(OsString, OsString)>, String> {
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
        (
            OsString::from("SLOOP_TICKET_ID"),
            OsString::from(&ctx.ticket_id),
        ),
        (
            OsString::from("SLOOP_BIN"),
            executable.as_os_str().to_owned(),
        ),
        (OsString::from("PATH"), path),
    ])
}

/// Spawns the stage's repair agent in the run worktree, captures its output to
/// the run log, checkpoints its process for crash recovery, and waits for it to
/// exit. The agent works in place; the caller re-runs the stage afterwards. The
/// repair agent never reports a verdict — the retried stage is the only
/// evidence. Returns the repair process identity for the attempt record.
#[allow(clippy::too_many_arguments)]
fn run_repair_agent(
    ctx: &RepairContext,
    on_fail: &OnFail,
    target: &str,
    worktree: &Path,
    output_path: &Path,
    store: &Store,
    run_id: &str,
    stage: &str,
    attempt: u32,
    clock: &dyn Clock,
    log: &OperationalLog,
) -> Result<ProcessIdentity, String> {
    let template = ctx
        .agent
        .targets
        .get(target)
        .ok_or_else(|| format!("repair target `{target}` is not a configured agent target"))?;
    let model = on_fail.model.as_deref().or(ctx.ticket_model.as_deref());
    let effort = on_fail.effort.as_deref().or(ctx.ticket_effort.as_deref());
    let argv = expand_agent_cmd(template, model, effort, &on_fail.agent)
        .map_err(|message| format!("repair target `{target}` {message}"))?;
    let environment = repair_agent_environment(ctx, run_id)?;
    // A repair agent works in the run worktree only; it never touches the
    // default-branch checkout. `branch` is unused because no worktree is
    // created here — the run's worktree already exists.
    let order = StageOrder {
        run_id: run_id.into(),
        stage: stage.into(),
        execution: StageExecution::Exec(ExecLaunch {
            argv,
            worker: None,
            environment,
        }),
        worktree: worktree.into(),
        branch: String::new(),
        output_path: output_path.into(),
    };
    log.emit_with_fields(
        LogLevel::Info,
        "sloop::supervisor",
        "repair_agent_spawned",
        json!({"run_id": run_id, "stage": stage, "attempt": attempt, "target": target}),
    );
    let hooks = StoreStageHooks::new(store, log);
    let evidence = match run_exec_stage(&order, &hooks, clock) {
        Ok(evidence) => evidence,
        Err(failure) => failure.evidence,
    };
    evidence
        .process
        .ok_or_else(|| format!("repair agent for stage `{stage}` produced no process identity"))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn drive_flow(
    root: &Path,
    worktree: &Path,
    branch: &str,
    bound_flow: &Flow,
    worker: &WorkerCredentials,
    test_cmd: Option<&[String]>,
    exit_code: Option<i32>,
    vendor_rejected: bool,
    commits: &[String],
    commit_observation_complete: bool,
    output_path: &Path,
    clock: &dyn Clock,
    store: &Store,
    run_id: &str,
    repair: Option<&RepairContext>,
    log: &OperationalLog,
) -> Result<FlowRunResult, String> {
    let flow = flow_with_implicit_test(bound_flow, test_cmd)?;
    let rows = store
        .aftercare_stages(run_id)
        .map_err(|error| error.to_string())?;
    let mut evidence = rows
        .iter()
        .map(|row| StageEvidence {
            stage: row.stage.clone(),
            verdict: if row.state == "passed" {
                Verdict::Pass
            } else {
                Verdict::Fail
            },
            source: if row.verdict_source == "reported" {
                VerdictSource::Reported
            } else {
                VerdictSource::ExitCode
            },
            reason: row.reason.clone(),
        })
        .collect::<Vec<_>>();
    let mut merge = flow.stages.iter().find_map(|stage| {
        if stage.kind != StageKind::Merge {
            return None;
        }
        evidence
            .iter()
            .find(|row| row.stage == stage.name)
            .map(|row| {
                if row.verdict == Verdict::Pass {
                    MergeOutcome::Merged
                } else {
                    MergeOutcome::Diverged
                }
            })
    });
    let interrupted = store
        .run_evidence(run_id)
        .map_err(|error| error.to_string())?;

    loop {
        if aftercare_cancelled(store, run_id, log) {
            return Ok(FlowRunResult {
                aftercare_failed: false,
                merge,
            });
        }
        let stage = match next_step(&flow, &evidence) {
            Step::Run(stage) => stage,
            Step::Halted { failed_stage } => {
                let first_stage_failed = flow
                    .stages
                    .first()
                    .is_some_and(|stage| stage.name == failed_stage);
                return Ok(FlowRunResult {
                    aftercare_failed: !first_stage_failed,
                    merge,
                });
            }
            Step::Complete => {
                return Ok(FlowRunResult {
                    aftercare_failed: false,
                    merge,
                });
            }
        };
        let stage_index = flow
            .stages
            .iter()
            .position(|candidate| candidate.name == stage.name)
            .expect("next_step returned a stage from this flow");
        let interrupted_process = stop_interrupted_process(&interrupted, &stage.name)?;
        if let Some((identity, PersistedProcessStop::LeaderMissing)) = &interrupted_process {
            log.emit_with_fields(
                LogLevel::Info,
                "sloop::recovery",
                "stale_aftercare_group_not_signalled",
                json!({
                    "run_id": run_id,
                    "stage": stage.name,
                    "process_group_id": identity.group,
                }),
            );
        }
        let mut merge_recovery = if let Some((identity, _)) = &interrupted_process
            && stage.kind == StageKind::Merge
            && identity.merge.is_some()
        {
            match inspect_interrupted_merge(root, branch, identity) {
                Ok(recovery) => Some(recovery),
                Err(error) => {
                    log.emit_with_fields(
                        LogLevel::Error,
                        "sloop::recovery",
                        "merge_recovery_inspection_failed",
                        json!({"run_id": run_id, "error": error}),
                    );
                    Some(super::recovery::MergeRecovery::UnsafePartial)
                }
            }
        } else {
            None
        };
        if interrupted_process.is_some() {
            store
                .clear_aftercare_process(run_id)
                .map_err(|error| error.to_string())?;
        }
        // Each `on_fail` stage may run up to `attempts` repair-then-retry
        // cycles. The repair agent never produces the verdict: after it exits
        // the stage is re-run and its own verdict policy re-applied, and that
        // re-run is the only evidence.
        let mut repair_used = repair_attempts_used(&interrupted, &stage.name);
        let mut pending_repair: Option<(u32, ProcessIdentity, String)> = None;
        let (verdict, source, reason, result) = loop {
            let mut result = match &stage.kind {
                StageKind::Agent => {
                    let now = clock.now_ms();
                    StageResult {
                        verdict: if !vendor_rejected
                            && classify_exit(exit_code) == ExitClass::Success
                        {
                            Verdict::Pass
                        } else {
                            Verdict::Fail
                        },
                        exit_code,
                        started_at_ms: now,
                        finished_at_ms: now,
                    }
                }
                StageKind::Exec { cmd } => execute_stage_order(
                    worktree,
                    branch,
                    &stage.name,
                    cmd,
                    output_path,
                    clock,
                    store,
                    run_id,
                    log,
                    (stage.verdict == VerdictPolicy::Reported).then_some(worker),
                ),
                StageKind::Merge
                    if merge_recovery == Some(super::recovery::MergeRecovery::AlreadyCompleted) =>
                {
                    let now = clock.now_ms();
                    merge = Some(MergeOutcome::Merged);
                    StageResult {
                        verdict: Verdict::Pass,
                        exit_code: Some(0),
                        started_at_ms: now,
                        finished_at_ms: now,
                    }
                }
                StageKind::Merge
                    if merge_recovery == Some(super::recovery::MergeRecovery::UnsafePartial) =>
                {
                    let now = clock.now_ms();
                    merge = Some(MergeOutcome::Diverged);
                    StageResult {
                        verdict: Verdict::Fail,
                        exit_code: Some(1),
                        started_at_ms: now,
                        finished_at_ms: now,
                    }
                }
                StageKind::Merge => {
                    let started_at_ms = clock.now_ms();
                    let outcome = attempt_merge(
                        root,
                        branch,
                        commit_observation_complete && commits.is_empty(),
                        &stage.name,
                        store,
                        run_id,
                        clock,
                        log,
                    );
                    merge = Some(outcome);
                    StageResult {
                        verdict: if outcome == MergeOutcome::Merged {
                            Verdict::Pass
                        } else {
                            Verdict::Fail
                        },
                        exit_code: Some(if outcome == MergeOutcome::Merged {
                            0
                        } else {
                            1
                        }),
                        started_at_ms,
                        finished_at_ms: clock.now_ms(),
                    }
                }
            };
            match &stage.verdict {
                VerdictPolicy::Exit | VerdictPolicy::Reported => {}
                VerdictPolicy::Commits => {
                    if result.verdict != Verdict::Pass
                        || !commit_observation_complete
                        || commits.is_empty()
                    {
                        result.verdict = Verdict::Fail;
                    }
                }
                VerdictPolicy::Check { cmd } if result.verdict == Verdict::Pass => {
                    result = execute_stage_order(
                        worktree,
                        branch,
                        &stage.name,
                        cmd,
                        output_path,
                        clock,
                        store,
                        run_id,
                        log,
                        None,
                    );
                }
                VerdictPolicy::Check { .. } => {}
            }
            let reported = if stage.verdict == VerdictPolicy::Reported {
                reported_verdict(store, run_id, &stage.name)?
            } else {
                None
            };
            let (verdict, source, reason) =
                resolve_verdict(&stage.verdict, result.verdict, reported);
            // Fill in the verdict of the re-run that followed the last repair.
            if let Some((attempt, identity, target)) = pending_repair.take() {
                let _ = store.record_repair_attempt(
                    run_id,
                    &stage.name,
                    attempt,
                    &repair_attempt_json(
                        &stage.name,
                        attempt,
                        &target,
                        Some(identity),
                        Some(verdict),
                    ),
                    clock.now_ms(),
                );
            }
            if verdict == Verdict::Pass {
                break (verdict, source, reason, result);
            }
            // The stage failed. If it has a repair worker, attempts remain, and
            // every spawn gate is open, repair in place and re-run the stage.
            if let (Some(on_fail), Some(ctx)) = (stage.on_fail.as_ref(), repair)
                && repair_used < on_fail.attempts
            {
                let target = resolve_repair_target(ctx, on_fail);
                if repair_gates_open(ctx, store, &target, clock, clock.now_ms()) {
                    let attempt = repair_used + 1;
                    // Record the attempt before spawning so a crash mid-repair
                    // still counts it: recovery re-runs the stage, never the
                    // repair, so the attempt is neither repeated nor lost.
                    store
                        .record_repair_attempt(
                            run_id,
                            &stage.name,
                            attempt,
                            &repair_attempt_json(&stage.name, attempt, &target, None, None),
                            clock.now_ms(),
                        )
                        .map_err(|error| error.to_string())?;
                    match run_repair_agent(
                        ctx,
                        on_fail,
                        &target,
                        worktree,
                        output_path,
                        store,
                        run_id,
                        &stage.name,
                        attempt,
                        clock,
                        log,
                    ) {
                        Ok(identity) => {
                            repair_used = attempt;
                            pending_repair = Some((attempt, identity, target));
                            // A fresh retry: any interrupted-merge recovery from
                            // a crash applied only to the first execution.
                            merge_recovery = None;
                            continue;
                        }
                        Err(error) => {
                            log.emit_with_fields(
                                LogLevel::Error,
                                "sloop::supervisor",
                                "repair_agent_failed",
                                json!({"run_id": run_id, "stage": stage.name, "error": error}),
                            );
                        }
                    }
                } else {
                    log.emit_with_fields(
                        LogLevel::Info,
                        "sloop::supervisor",
                        "repair_gate_closed",
                        json!({"run_id": run_id, "stage": stage.name, "target": target}),
                    );
                }
            }
            break (verdict, source, reason, result);
        };
        if let Err(error) = store.record_aftercare_stage(
            run_id,
            &StageRecord {
                stage_index,
                stage: stage.name.clone(),
                state: if verdict == Verdict::Pass {
                    "passed".into()
                } else {
                    "failed".into()
                },
                started_at_ms: result.started_at_ms,
                finished_at_ms: result.finished_at_ms,
                exit_code: result.exit_code,
                output_ref: format!("runs/{run_id}/output.ndjson"),
                verdict_source: match source {
                    VerdictSource::ExitCode => "exit_code",
                    VerdictSource::Reported => "reported",
                }
                .into(),
                reason: reason.clone(),
            },
        ) {
            log.emit_with_fields(
                LogLevel::Error,
                "sloop::supervisor",
                "aftercare_stage_persist_failed",
                json!({"run_id": run_id, "stage": stage.name, "error": error.to_string()}),
            );
            return Ok(FlowRunResult {
                aftercare_failed: true,
                merge,
            });
        }
        if let Err(error) = store.clear_aftercare_process(run_id) {
            log.emit_with_fields(
                LogLevel::Error,
                "sloop::supervisor",
                "aftercare_process_clear_failed",
                json!({"run_id": run_id, "stage": stage.name, "error": error.to_string()}),
            );
            return Ok(FlowRunResult {
                aftercare_failed: true,
                merge,
            });
        }
        evidence.push(StageEvidence {
            stage: stage.name.clone(),
            verdict,
            source,
            reason,
        });
        wait_for_test_hook(&format!("after-aftercare-stage-{}", stage.name));
    }
}

fn flow_with_implicit_test(flow: &Flow, test_cmd: Option<&[String]>) -> Result<Flow, String> {
    let mut flow = flow.clone();
    if let Some(cmd) = test_cmd {
        if flow.stages.iter().any(|stage| stage.name == "test") {
            return Err("aftercare.test_cmd conflicts with flow stage `test`".into());
        }
        flow.stages.insert(
            1,
            Stage {
                name: "test".into(),
                kind: StageKind::Exec { cmd: cmd.to_vec() },
                verdict: VerdictPolicy::Exit,
                on_fail: None,
            },
        );
    }
    Ok(flow)
}

fn reported_verdict(store: &Store, run_id: &str, stage: &str) -> Result<Option<Reported>, String> {
    let rows = store
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

#[allow(clippy::too_many_arguments)]
fn record_merge_process_checkpoint(
    store: &Store,
    run_id: &str,
    stage: &str,
    pid: u32,
    pid_start_time: i64,
    checkpoint: &MergeProcessCheckpoint,
    now_ms: i64,
) -> Result<(), StoreError> {
    store.record_aftercare_evidence(
        run_id,
        "aftercare_process",
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
