use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use serde_json::json;
use tokio::net::UnixListener;
use tokio::sync::mpsc;

use crate::clock::Clock;
use crate::db::{Db, StoreError};
use crate::domain::work::{Disposition, TicketRef};
use crate::flow::Flow;
use crate::logging::{LogLevel, OperationalLog};
use crate::outcome::Outcome;
use crate::run_log::{OutputStream, output_staleness};
use crate::run_store::{
    EvidenceRecord, Exit, ExitDenial, OutputStallEvidence, RecoverableRun, RunExit, RunState,
    RunStore,
};
use crate::runner::local::{
    process_start_time, run_output_path, wait_for_test_hook, worker_socket_path,
};
use crate::vendor_error::{VendorErrorClassifier, VendorErrorMatch};

use super::dispatcher::{
    DispatcherState, RunEvent, close_worker_socket, disposition_for_outcome, mark_storage_full,
    push_work_outcome,
};
use super::driver::{
    DriverEnvironment, DriverPlan, STAGE_PROCESS, git_index_lock_path, git_index_matches_head,
    git_is_ancestor, git_stdout, shared_checkout_has_git_operation, start_driver,
    try_commits_on_branch,
};
use super::scheduler::{DEFAULT_LEASE_MS, VENDOR_COOLDOWN_MS};
use super::server::{DaemonError, serve_worker_socket};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RecoveryClassification {
    /// A restarted daemon replayed the run's evidence log and resumed its
    /// driver at the derived step.
    Resumed,
    Orphaned,
}

/// Classifies every durable lease before normal dispatch. Processes that are
/// live or cannot be disproved consume capacity and are monitored by identity;
/// dead or reused PIDs are settled from the work preserved in their branches.
pub(super) async fn recover_inflight_runs(
    state: &mut DispatcherState,
    events: &mpsc::Sender<RunEvent>,
    log: &OperationalLog,
) -> Result<(), DaemonError> {
    release_interrupted_claims(state, log).await?;
    let runs = state
        .run_store
        .recoverable_runs()
        .map_err(DaemonError::Store)?;
    for run in runs {
        // Every durable lease consumes capacity until adoption or settlement
        // succeeds; a transient database error must never permit double-spawn.
        state.active.insert(run.id.clone());
        let process_identity = recoverable_process_identity(&run);
        let cancellation_requested = state
            .run_store
            .cancellation_requested(&run.id)
            .map_err(DaemonError::Store)?;
        if cancellation_requested {
            state.cancelling.insert(run.id.clone());
        }
        let mut stall = state
            .run_store
            .output_stall(&run.id)
            .map_err(DaemonError::Store)?;
        if run.state == RunState::Running && !cancellation_requested && stall.is_none() {
            let started_at_ms = state
                .run_store
                .run_timelines(&[run.id.as_str()])
                .map_err(DaemonError::Store)?
                .remove(&run.id)
                .and_then(|timeline| timeline.started_at_ms);
            if let Some(started_at_ms) = started_at_ms
                && let Ok(staleness) = output_staleness(
                    &run_output_path(&state.state_dir, &run.id),
                    started_at_ms,
                    state.clock.now_ms(),
                    state.stall_after_ms,
                )
                && staleness.stalled
            {
                let evidence = OutputStallEvidence {
                    stage: executing_stage(&state.run_store, &run.id, run.flow_json.as_deref()),
                    last_output_at_ms: staleness.last_output_at_ms,
                    threshold_ms: state.stall_after_ms,
                    last_output_sequence: staleness.last_sequence,
                };
                state
                    .run_store
                    .record_output_stall(&run.id, &evidence, state.clock.now_ms())
                    .map_err(DaemonError::Store)?;
                stall = state
                    .run_store
                    .output_stall(&run.id)
                    .map_err(DaemonError::Store)?;
            }
        }
        if let Some(stall) =
            stall.filter(|_| !cancellation_requested && run.state == RunState::Running)
        {
            if state.reported_stalls.get(&run.id) != Some(&stall.last_output_sequence) {
                log.emit_with_fields(
                    LogLevel::Warn,
                    "sloop::dispatcher",
                    "run_output_stalled",
                    json!({
                        "run_id": run.id,
                        "ticket_id": run.ticket_id,
                        "stage": stall.stage,
                        "silent_for_ms": state.clock.now_ms().saturating_sub(stall.last_output_at_ms),
                        "last_output_sequence": stall.last_output_sequence,
                    }),
                );
                state
                    .reported_stalls
                    .insert(run.id.clone(), stall.last_output_sequence);
            }
            state.stalling.insert(run.id.clone());
            match process_identity {
                ProcessIdentity::Matches | ProcessIdentity::Unverifiable => {
                    state.supervised.insert(run.id.clone());
                    if let Err(error) =
                        stop_agent_process_group(run.pid, run.pid_start_time, run.process_group_id)
                    {
                        log.emit_with_fields(
                            LogLevel::Error,
                            "sloop::recovery",
                            "output_stall_signal_refused",
                            json!({"run_id": run.id, "error": error}),
                        );
                    }
                    monitor_recovered_run(state, events.clone(), run.clone());
                }
                ProcessIdentity::GoneOrReused => {
                    state.recovering.insert(run.id.clone());
                    spawn_dead_run_recovery(state, events.clone(), run.clone(), log.clone());
                }
            }
            log.emit_with_fields(
                LogLevel::Warn,
                "sloop::recovery",
                "stalled_run_recovered",
                json!({"run_id": run.id, "ticket_id": run.ticket_id}),
            );
            continue;
        }
        match process_identity {
            ProcessIdentity::Matches | ProcessIdentity::Unverifiable => {
                state.supervised.insert(run.id.clone());
                rearm_adopted_lease(state, &run, log);
                if cancellation_requested {
                    if let Err(error) =
                        stop_agent_process_group(run.pid, run.pid_start_time, run.process_group_id)
                    {
                        log.emit_with_fields(
                            LogLevel::Error,
                            "sloop::recovery",
                            "agent_cancel_signal_refused",
                            json!({"run_id": run.id, "error": error}),
                        );
                    }
                }
                if let Err(error) = restore_worker_socket(state, &run) {
                    log.emit_with_fields(
                        LogLevel::Error,
                        "sloop::recovery",
                        "worker_socket_restore_failed",
                        json!({"run_id": run.id, "error": error}),
                    );
                }
                monitor_recovered_run(state, events.clone(), run.clone());
                log.emit_with_fields(
                    LogLevel::Info,
                    "sloop::recovery",
                    "run_readopted",
                    json!({"run_id": run.id, "ticket_id": run.ticket_id}),
                );
            }
            ProcessIdentity::GoneOrReused => {
                state.recovering.insert(run.id.clone());
                if run.state == RunState::Driving {
                    // A run whose flow has not reached an agent stage yet has
                    // no credentials to restore; its driver mints them when a
                    // stage needs them.
                    if run.worker_token.is_some()
                        && let Err(error) = restore_worker_socket(state, &run)
                    {
                        log.emit_with_fields(
                            LogLevel::Error,
                            "sloop::recovery",
                            "worker_socket_restore_failed",
                            json!({"run_id": run.id, "error": error}),
                        );
                    }
                    if let Err(error) = resume_run_driver(state, events.clone(), run.clone()) {
                        park_unresumable_run(state, &run, &error, log).await;
                    }
                } else {
                    spawn_dead_run_recovery(state, events.clone(), run, log.clone());
                }
            }
        }
    }
    Ok(())
}

/// The stage a run is executing right now, read from the process checkpoint the
/// driver writes for every stage. A run between stages has none, so the flow's
/// first agent stage stands in — that is the only stage the output watchdog can
/// be waiting on.
pub(super) fn executing_stage(
    run_store: &RunStore,
    run_id: &str,
    flow_json: Option<&str>,
) -> String {
    if let Ok(rows) = run_store.run_evidence(run_id)
        && let Some(stage) = rows
            .iter()
            .find(|(kind, _)| kind == STAGE_PROCESS)
            .and_then(|(_, data)| serde_json::from_str::<serde_json::Value>(data).ok())
            .and_then(|data| data["stage"].as_str().map(str::to_owned))
    {
        return stage;
    }
    flow_json
        .and_then(|snapshot| serde_json::from_str::<Flow>(snapshot).ok())
        .and_then(|flow| {
            flow.stages
                .into_iter()
                .find(|stage| stage.action == crate::flow::Actor::Agent)
        })
        .map_or_else(|| "agent".to_owned(), |stage| stage.name)
}

/// Which execution of the executing stage the driver last checkpointed. The
/// checkpoint is the only durable record of where the interrupted daemon
/// stood, so an exit recovery credits the execution it was actually watching.
pub(super) fn executing_attempt(run_store: &RunStore, run_id: &str) -> u32 {
    run_store
        .run_evidence(run_id)
        .ok()
        .and_then(|rows| {
            rows.iter()
                .find(|(kind, _)| kind == STAGE_PROCESS)
                .and_then(|(_, data)| serde_json::from_str::<serde_json::Value>(data).ok())
                .and_then(|data| data["attempt"].as_u64())
        })
        .and_then(|attempt| u32::try_from(attempt).ok())
        .unwrap_or(1)
}

async fn release_interrupted_claims(
    state: &mut DispatcherState,
    log: &OperationalLog,
) -> Result<(), DaemonError> {
    for claim in state
        .work_state
        .active_claims()
        .await
        .map_err(DaemonError::WorkState)?
    {
        let run = state
            .run_store
            .run(&claim.owner.0)
            .map_err(DaemonError::Store)?;
        let (disposition, outcome) = match run {
            None => (
                Disposition::Retry {
                    not_before_ms: Some(state.clock.now_ms()),
                },
                None,
            ),
            Some(run) => {
                let run_state = RunState::parse(&run.state).map_err(DaemonError::Store)?;
                if !run_state.is_terminal() {
                    continue;
                }
                if run_state == RunState::Aborted {
                    (
                        Disposition::Retry {
                            not_before_ms: Some(state.clock.now_ms()),
                        },
                        None,
                    )
                } else {
                    let recorded = state
                        .run_store
                        .recorded_outcome(&claim.owner.0)
                        .map_err(DaemonError::Store)?
                        .ok_or_else(|| {
                            DaemonError::Store(StoreError::RunStateConflict {
                                run_id: claim.owner.0.clone(),
                                state: Some(run.state),
                                requested: "recorded outcome".into(),
                            })
                        })?;
                    (
                        disposition_for_outcome(recorded.work.verdict, recorded.not_before_ms),
                        Some(recorded.work),
                    )
                }
            }
        };
        state
            .work_state
            .release(&claim.ticket, &claim.owner, disposition)
            .await
            .map_err(DaemonError::WorkState)?;
        if let Some(outcome) = outcome {
            push_work_outcome(state, outcome, log, "sloop::recovery");
        }
        log.emit_with_fields(
            LogLevel::Info,
            "sloop::recovery",
            "interrupted_claim_released",
            json!({"run_id": claim.owner.0, "ticket_id": claim.ticket.id}),
        );
    }
    Ok(())
}

/// Re-arms the lease of a run this daemon has just adopted.
///
/// Strict renewal cannot lift a lease that lapsed while the daemon was down,
/// so without this an adopted long-lived run would advertise an expired lease
/// for the rest of its life. Adoption is the one moment where resetting expiry
/// is sound: the run's process identity has just been verified alive, and a
/// run whose identity failed is settled rather than adopted, so its lease
/// stays expired and is then deleted by settlement.
pub(super) fn rearm_adopted_lease(
    state: &mut DispatcherState,
    run: &RecoverableRun,
    log: &OperationalLog,
) {
    let now_ms = state.clock.now_ms();
    let readopted =
        state
            .local_work_state
            .readopt_lease(&run.ticket_id, &run.id, DEFAULT_LEASE_MS, now_ms);
    match readopted {
        Ok(_) => {}
        Err(StoreError::LeaseNotHeld { .. }) => log.emit_with_fields(
            LogLevel::Warn,
            "sloop::recovery",
            "lease_rearm_denied",
            json!({"run_id": run.id, "ticket_id": run.ticket_id}),
        ),
        Err(error) => log.emit_with_fields(
            LogLevel::Error,
            "sloop::recovery",
            "lease_rearm_failed",
            json!({"run_id": run.id, "ticket_id": run.ticket_id, "error": error.to_string()}),
        ),
    }
}

pub(super) fn restore_worker_socket(
    state: &mut DispatcherState,
    run: &RecoverableRun,
) -> Result<(), String> {
    let token = run
        .worker_token
        .as_ref()
        .ok_or_else(|| "the persisted run has no worker token".to_owned())?;
    let socket_path = run
        .worker_socket_path
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(|| worker_socket_path(&state.runtime_dir, &run.id));
    fs::create_dir_all(socket_path.parent().expect("worker sockets have a parent"))
        .map_err(|error| error.to_string())?;
    let _ = fs::remove_file(&socket_path);
    let listener = UnixListener::bind(&socket_path).map_err(|error| error.to_string())?;
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))
        .map_err(|error| error.to_string())?;
    // The persisted token is the *agent stage's*: only an agent launch writes
    // one to the run row. A panel reviewer's credential is minted per seat and
    // never persisted, so nothing here can restore one — which is right, since
    // recovery re-runs the stage and its panel from the top.
    state.worker_tokens.insert(
        run.id.clone(),
        super::dispatcher::IssuedWorker {
            token: token.clone(),
            scope: crate::runner::WorkerScope::Stage,
        },
    );
    state
        .worker_socket_paths
        .insert(run.id.clone(), socket_path.clone());
    let accept_loop = tokio::spawn(serve_worker_socket(
        listener,
        run.id.clone(),
        state.requests_tx.clone(),
        state.log.clone(),
    ));
    state.worker_listeners.insert(run.id.clone(), accept_loop);
    Ok(())
}

pub(super) fn monitor_recovered_run(
    state: &DispatcherState,
    events: mpsc::Sender<RunEvent>,
    run: RecoverableRun,
) {
    let root = state.root.clone();
    let state_dir = state.state_dir.clone();
    let classifier = state.classifier.clone();
    let clock = state.clock.clone();
    let db_path = state.state_dir.join("sloop.db");
    let log = state.log.clone();
    let shutdown = state.shutdown_flag.clone();
    tokio::task::spawn_blocking(move || {
        loop {
            if shutdown.load(Ordering::Acquire) {
                return;
            }
            match recoverable_process_identity(&run) {
                ProcessIdentity::Matches | ProcessIdentity::Unverifiable => {
                    std::thread::sleep(Duration::from_millis(100));
                }
                ProcessIdentity::GoneOrReused => break,
            }
        }
        while !shutdown.load(Ordering::Acquire) {
            match recovered_exit_event(&root, &state_dir, &classifier, clock.now_ms(), &run) {
                Ok(event) => match claim_recovered_exit(&db_path, clock.as_ref(), &event, &log) {
                    Ok(claimed) => {
                        if claimed {
                            let _ = events.blocking_send(event);
                        }
                        break;
                    }
                    Err(()) => std::thread::sleep(Duration::from_secs(1)),
                },
                Err(error) => {
                    log.emit_with_fields(
                        LogLevel::Error,
                        "sloop::recovery",
                        "run_observation_failed",
                        json!({"run_id": run.id, "error": error}),
                    );
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        }
    });
}

pub(super) fn spawn_dead_run_recovery(
    state: &DispatcherState,
    events: mpsc::Sender<RunEvent>,
    run: RecoverableRun,
    log: OperationalLog,
) {
    let root = state.root.clone();
    let state_dir = state.state_dir.clone();
    let classifier = state.classifier.clone();
    let clock = state.clock.clone();
    let db_path = state.state_dir.join("sloop.db");
    let shutdown = state.shutdown_flag.clone();
    tokio::task::spawn_blocking(move || {
        while !shutdown.load(Ordering::Acquire) {
            match recovered_exit_event(&root, &state_dir, &classifier, clock.now_ms(), &run) {
                Ok(event) => {
                    let claim = if run.state == RunState::Running {
                        claim_recovered_exit(&db_path, clock.as_ref(), &event, &log)
                    } else {
                        Ok(true)
                    };
                    match claim {
                        Ok(claimed) => {
                            if claimed {
                                let _ = events.blocking_send(event);
                            }
                            break;
                        }
                        Err(()) => std::thread::sleep(Duration::from_secs(1)),
                    }
                }
                Err(error) => {
                    log.emit_with_fields(
                        LogLevel::Error,
                        "sloop::recovery",
                        "run_observation_failed",
                        json!({"run_id": run.id, "error": error}),
                    );
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        }
    });
}

pub(super) fn recovered_exit_event(
    root: &Path,
    state_dir: &Path,
    classifier: &VendorErrorClassifier,
    now_ms: i64,
    run: &RecoverableRun,
) -> Result<RunEvent, String> {
    let commits = run
        .branch
        .as_deref()
        .map(|branch| try_commits_on_branch(root, branch))
        .transpose()?
        .unwrap_or_default();
    let exit_code = run.exit_code.and_then(|code| i32::try_from(code).ok());
    let vendor_error = classify_run_output(classifier, state_dir, &run.id, exit_code)?;
    let cooldown_until_ms = vendor_error
        .as_ref()
        .filter(|error| error.class.requires_cooldown())
        .map(|_| now_ms + VENDOR_COOLDOWN_MS);
    Ok(RunEvent::Exited {
        run_id: run.id.clone(),
        target: run.target.clone(),
        exit_code,
        capture_complete: false,
        commits,
        commit_observation_complete: true,
        halt: None,
        merge: None,
        vendor_error,
        cooldown_until_ms,
        recovery: Some(RecoveryClassification::Orphaned),
    })
}

/// Resumes a run whose daemon died: replays its evidence log and restarts its
/// driver at the derived step.
///
/// Nothing here depends on how far the run got. The driver re-derives its own
/// position from the log, stops whatever process the previous daemon left
/// behind, and carries on — so a run interrupted in its first stage and one
/// interrupted in its last resume through exactly the same path.
pub(super) fn resume_run_driver(
    state: &DispatcherState,
    events: mpsc::Sender<RunEvent>,
    run: RecoverableRun,
) -> Result<(), DaemonError> {
    let flow = match run.flow_json.as_deref() {
        Some(snapshot) => serde_json::from_str::<Flow>(snapshot).map_err(|error| {
            DaemonError::InvalidResponse(format!(
                "run `{}` has an invalid flow snapshot: {error}",
                run.id
            ))
        })?,
        None => {
            let ticket = state
                .local_work_state
                .ticket(&run.ticket_id)
                .map_err(DaemonError::Store)?
                .ok_or_else(|| {
                    DaemonError::InvalidResponse(format!(
                        "ticket `{}` no longer exists",
                        run.ticket_id
                    ))
                })?;
            let flow_name = ticket.flow.as_deref().ok_or_else(|| {
                DaemonError::InvalidResponse(format!(
                    "ticket `{}` has no bound flow",
                    run.ticket_id
                ))
            })?;
            state.flows.get(flow_name).cloned().ok_or_else(|| {
                DaemonError::InvalidResponse(format!(
                    "ticket `{}` names unknown bound flow `{flow_name}`",
                    run.ticket_id
                ))
            })?
        }
    };
    let branch = run.branch.clone().ok_or_else(|| {
        DaemonError::InvalidResponse(format!("run `{}` has no run branch", run.id))
    })?;
    let worktree = run
        .worktree_path
        .clone()
        .ok_or_else(|| DaemonError::InvalidResponse(format!("run `{}` has no worktree", run.id)))?;
    let plan = DriverPlan {
        run_id: run.id.clone(),
        ticket_id: run.ticket_id.clone(),
        target: run.target.clone(),
        branch,
        worktree: PathBuf::from(worktree),
        flow,
        ticket: run
            .ticket_json
            .as_deref()
            .and_then(|json| serde_json::from_str(json).ok()),
        recovery: Some(RecoveryClassification::Resumed),
    };
    start_driver(DriverEnvironment::from_state(state), plan, events);
    Ok(())
}

/// Settles a run whose driver refused to resume — an unreadable flow snapshot,
/// a row with no branch or worktree.
///
/// The failure belongs to the one run, so it is parked for a human with its
/// branch and evidence intact rather than retried against a flow the daemon
/// cannot read, and the recovery pass carries on to the next run. One bad row
/// must not cost every other run its recovery.
async fn park_unresumable_run(
    state: &mut DispatcherState,
    run: &RecoverableRun,
    error: &DaemonError,
    log: &OperationalLog,
) {
    let error = error.to_string();
    log.emit_with_fields(
        LogLevel::Error,
        "sloop::recovery",
        "driver_resume_failed",
        json!({"run_id": run.id, "ticket_id": run.ticket_id, "error": error}),
    );
    let records = [EvidenceRecord {
        kind: "driver_resume_failed",
        data_json: json!({"error": error}).to_string(),
    }];
    let recorded = match state.run_store.settle(
        &run.id,
        None,
        Outcome::NeedsReview,
        &records,
        None,
        state.clock.now_ms(),
    ) {
        Ok((recorded, _)) => recorded,
        Err(store_error) => {
            mark_storage_full(state, &store_error);
            log.emit_with_fields(
                LogLevel::Error,
                "sloop::recovery",
                "unresumable_run_park_failed",
                json!({"run_id": run.id, "error": store_error.to_string()}),
            );
            return;
        }
    };
    match state.local_work_state.ticket(&run.ticket_id) {
        Ok(Some(ticket)) => {
            let ticket_ref = TicketRef {
                id: ticket.id,
                source: ticket.source,
                source_ref: ticket.source_ref,
            };
            if let Err(release_error) = state
                .work_state
                .release(
                    &ticket_ref,
                    &recorded.work.owner,
                    disposition_for_outcome(recorded.work.verdict, recorded.not_before_ms),
                )
                .await
            {
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::recovery",
                    "claim_release_failed",
                    json!({
                        "run_id": run.id,
                        "ticket_id": run.ticket_id,
                        "error": release_error.to_string(),
                    }),
                );
            }
        }
        Ok(None) => {}
        Err(store_error) => {
            mark_storage_full(state, &store_error);
            log.emit_with_fields(
                LogLevel::Error,
                "sloop::recovery",
                "claim_release_failed",
                json!({
                    "run_id": run.id,
                    "ticket_id": run.ticket_id,
                    "error": store_error.to_string(),
                }),
            );
        }
    }
    push_work_outcome(state, recorded.work, log, "sloop::recovery");
    state.active.remove(&run.id);
    state.supervised.remove(&run.id);
    state.suspected_dead.remove(&run.id);
    state.recovering.remove(&run.id);
    state.cancelling.remove(&run.id);
    state.stalling.remove(&run.id);
    state.reported_stalls.remove(&run.id);
    close_worker_socket(state, &run.id);
}

pub(super) fn stop_interrupted_process(
    rows: &[(String, String)],
    stage: &str,
) -> Result<Option<(StageProcessIdentity, PersistedProcessStop)>, String> {
    let Some(identity) = stage_process_identity(rows, Some(stage))? else {
        return Ok(None);
    };
    if identity.group <= 0 {
        return Err("the interrupted stage has an invalid process group".into());
    }
    let stopped = stop_persisted_process_group(&identity)?;
    Ok(Some((identity, stopped)))
}

pub(super) fn stage_process_identity(
    rows: &[(String, String)],
    stage: Option<&str>,
) -> Result<Option<StageProcessIdentity>, String> {
    let Some(data) = rows
        .iter()
        .find(|(candidate, _)| candidate == STAGE_PROCESS)
        .and_then(|(_, data)| serde_json::from_str::<serde_json::Value>(data).ok())
    else {
        return Ok(None);
    };
    if stage.is_some_and(|stage| data["stage"].as_str() != Some(stage)) {
        return Ok(None);
    }
    let pid = data["pid"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .ok_or_else(|| "the interrupted stage has no valid pid".to_owned())?;
    let start_time = data["pid_start_time"]
        .as_i64()
        .ok_or_else(|| "the interrupted stage has no valid start time".to_owned())?;
    let group = data["process_group_id"]
        .as_i64()
        .ok_or_else(|| "the interrupted stage has no valid process group".to_owned())?;
    let merge = data
        .get("merge")
        .map(|merge| -> Result<_, String> {
            Ok(MergeProcessCheckpoint {
                target_head: merge["target_head"]
                    .as_str()
                    .ok_or_else(|| "the interrupted merge has no target HEAD".to_owned())?
                    .to_owned(),
                branch_tip: merge["branch_tip"]
                    .as_str()
                    .ok_or_else(|| "the interrupted merge has no branch tip".to_owned())?
                    .to_owned(),
                completed_target: merge["completed_target"].as_str().map(str::to_owned),
            })
        })
        .transpose()?;
    Ok(Some(StageProcessIdentity {
        pid,
        start_time,
        group,
        merge,
    }))
}

#[cfg(test)]
pub(super) fn recoverable_process_matches(run: &RecoverableRun) -> bool {
    recoverable_process_identity(run) == ProcessIdentity::Matches
}

/// Claims the same durable exit handoff used by the normal supervisor. A
/// racing loser emits no settlement event and leaves the walk to the winner.
fn claim_recovered_exit(
    db_path: &Path,
    clock: &dyn Clock,
    event: &RunEvent,
    log: &OperationalLog,
) -> Result<bool, ()> {
    let RunEvent::Exited {
        run_id,
        exit_code,
        capture_complete,
        commits,
        commit_observation_complete,
        vendor_error,
        cooldown_until_ms,
        ..
    } = event
    else {
        return Ok(false);
    };
    let now_ms = clock.now_ms();
    let result = Db::open(db_path, now_ms)
        .map_err(StoreError::from)
        .and_then(|db| {
            let run_store = RunStore::from_db(db);
            let exit = RunExit {
                run_id,
                // The interrupted execution is the one the driver last
                // checkpointed a process for; a run with none behind it only
                // ever had a first.
                attempt: executing_attempt(&run_store, run_id),
                exit_code: *exit_code,
                capture_complete: *capture_complete,
                commits_json: &json!({"complete": commit_observation_complete, "oids": commits})
                    .to_string(),
                vendor_error: vendor_error.as_ref(),
                cooldown_until_ms: *cooldown_until_ms,
            };
            run_store.record_exit(&exit, now_ms)
        });
    match result {
        Ok(Exit::Granted) => Ok(true),
        Ok(Exit::Denied(ExitDenial::AlreadyClaimed { state })) => {
            log.emit_with_fields(
                LogLevel::Info,
                "sloop::recovery",
                "exit_checkpoint_already_claimed",
                json!({"run_id": run_id, "state": state}),
            );
            Ok(false)
        }
        Err(error) => {
            log.emit_with_fields(
                LogLevel::Error,
                "sloop::recovery",
                "agent_exit_checkpoint_failed",
                json!({"run_id": run_id, "error": error.to_string()}),
            );
            Err(())
        }
    }
}

/// Repairs in-memory capacity from durable leases and recovers runs whose
/// recorded process identity is provably gone or reused.
pub(super) async fn reconcile_run_liveness(
    state: &mut DispatcherState,
    events: &mpsc::Sender<RunEvent>,
    log: &OperationalLog,
) {
    if let Err(error) = release_interrupted_claims(state, log).await {
        state.reconciliation_blocked = true;
        log.emit_with_fields(
            LogLevel::Error,
            "sloop::recovery",
            "claim_reconciliation_failed",
            json!({"error": error.to_string()}),
        );
        return;
    }
    let runs = match state.run_store.recoverable_runs() {
        Ok(runs) => {
            state.reconciliation_blocked = false;
            runs
        }
        Err(error) => {
            state.reconciliation_blocked = true;
            log.emit_with_fields(
                LogLevel::Error,
                "sloop::recovery",
                "run_reconciliation_failed",
                json!({"error": error.to_string()}),
            );
            return;
        }
    };
    for run in runs {
        // A run this daemon has not seen before is being adopted now, so its
        // lease is re-armed once its identity checks out below.
        let adopted = state.active.insert(run.id.clone());
        if state.recovering.contains(&run.id) {
            continue;
        }
        if run.state == RunState::Driving && state.supervised.contains(&run.id) {
            continue;
        }
        match recoverable_process_identity(&run) {
            ProcessIdentity::Matches => {
                state.suspected_dead.remove(&run.id);
                if adopted {
                    rearm_adopted_lease(state, &run, log);
                }
                continue;
            }
            ProcessIdentity::Unverifiable => {
                state.suspected_dead.remove(&run.id);
                if adopted {
                    rearm_adopted_lease(state, &run, log);
                }
                log.emit_with_fields(
                    LogLevel::Info,
                    "sloop::recovery",
                    "run_identity_unverifiable",
                    json!({"run_id": run.id}),
                );
                continue;
            }
            ProcessIdentity::GoneOrReused => {}
        }

        if state.supervised.contains(&run.id) && state.suspected_dead.insert(run.id.clone()) {
            log.emit_with_fields(
                LogLevel::Info,
                "sloop::recovery",
                "supervised_run_exit_observed",
                json!({"run_id": run.id}),
            );
            continue;
        }

        state.recovering.insert(run.id.clone());
        if run.state == RunState::Driving {
            if let Err(error) = resume_run_driver(state, events.clone(), run.clone()) {
                park_unresumable_run(state, &run, &error, log).await;
            }
        } else {
            spawn_dead_run_recovery(state, events.clone(), run, log.clone());
        }
    }
    wait_for_test_hook("after-run-liveness-reconciliation");
}

#[derive(Debug, Clone)]
pub(super) struct StageProcessIdentity {
    pub(super) pid: u32,
    pub(super) start_time: i64,
    pub(super) group: i64,
    pub(super) merge: Option<MergeProcessCheckpoint>,
}

#[derive(Debug, Clone)]
pub(super) struct MergeProcessCheckpoint {
    pub(super) target_head: String,
    pub(super) branch_tip: String,
    pub(super) completed_target: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistedProcessState {
    OriginalLeader,
    ReusedLeader,
    LeaderMissing,
    UnverifiableLeader,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PersistedProcessStop {
    StoppedOriginal,
    LeaderMissing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MergeRecovery {
    Retry,
    AlreadyCompleted,
    UnsafePartial,
}

#[cfg(target_os = "linux")]
fn process_group_alive(group: i64) -> bool {
    let Ok(processes) = fs::read_dir("/proc") else {
        return unsafe { libc::kill(-(group as libc::pid_t), 0) == 0 };
    };
    processes.filter_map(Result::ok).any(|process| {
        let Ok(pid) = process.file_name().to_string_lossy().parse::<u32>() else {
            return false;
        };
        let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) else {
            return false;
        };
        let Some(after_command) = stat.rfind(')').map(|index| &stat[index + 1..]) else {
            return false;
        };
        let mut fields = after_command.split_whitespace();
        let state = fields.next();
        let _parent = fields.next();
        let process_group = fields.next().and_then(|value| value.parse::<i64>().ok());
        state != Some("Z") && process_group == Some(group)
    })
}

#[cfg(not(target_os = "linux"))]
fn process_group_alive(group: i64) -> bool {
    unsafe { libc::kill(-(group as libc::pid_t), 0) == 0 }
}

pub(super) fn inspect_interrupted_merge(
    root: &Path,
    branch: &str,
    identity: &StageProcessIdentity,
) -> Result<MergeRecovery, String> {
    let checkpoint = identity
        .merge
        .as_ref()
        .ok_or_else(|| "the interrupted merge has no baseline checkpoint".to_owned())?;
    if shared_checkout_has_git_operation(root)? || git_index_lock_path(root)?.exists() {
        return Ok(MergeRecovery::UnsafePartial);
    }
    if !git_stdout(root, &["ls-files", "--unmerged"])?.is_empty() {
        return Ok(MergeRecovery::UnsafePartial);
    }
    let branch_tip = git_stdout(root, &["rev-parse", branch])?;
    if branch_tip != checkpoint.branch_tip {
        return Ok(MergeRecovery::UnsafePartial);
    }
    let target_head = git_stdout(root, &["rev-parse", "HEAD"])?;
    if checkpoint.completed_target.is_some() {
        return if git_is_ancestor(root, &checkpoint.branch_tip, &target_head)? {
            Ok(MergeRecovery::AlreadyCompleted)
        } else {
            Ok(MergeRecovery::UnsafePartial)
        };
    }
    if target_head == checkpoint.target_head {
        return if git_index_matches_head(root)? {
            Ok(MergeRecovery::Retry)
        } else {
            Ok(MergeRecovery::UnsafePartial)
        };
    }
    if git_is_ancestor(root, &checkpoint.branch_tip, &target_head)? {
        return Ok(MergeRecovery::AlreadyCompleted);
    }
    Ok(MergeRecovery::UnsafePartial)
}

fn persisted_process_state(identity: &StageProcessIdentity) -> PersistedProcessState {
    let observed_start_time = process_start_time(identity.pid);
    classify_persisted_process(
        identity.start_time,
        observed_start_time,
        observed_start_time.is_some() || process_exists(identity.pid),
    )
}

fn classify_persisted_process(
    expected_start_time: i64,
    observed_start_time: Option<i64>,
    leader_exists: bool,
) -> PersistedProcessState {
    match observed_start_time {
        Some(actual) if actual == expected_start_time => PersistedProcessState::OriginalLeader,
        Some(_) => PersistedProcessState::ReusedLeader,
        None if leader_exists => PersistedProcessState::UnverifiableLeader,
        None => PersistedProcessState::LeaderMissing,
    }
}

pub(super) fn stop_persisted_process_group(
    identity: &StageProcessIdentity,
) -> Result<PersistedProcessStop, String> {
    if identity.group <= 0
        || identity.group != i64::from(identity.pid)
        || libc::pid_t::try_from(identity.group).is_err()
    {
        return Err("the persisted process group is not its recorded leader".into());
    }
    match persisted_process_state(identity) {
        PersistedProcessState::ReusedLeader => {
            return Err("the process group ID was reused; refusing to signal it".into());
        }
        PersistedProcessState::UnverifiableLeader => {
            return Err("cannot verify the persisted process leader".into());
        }
        // The group may still exist, but without the recorded leader its
        // identity is unverifiable and signaling it is unsafe.
        PersistedProcessState::LeaderMissing => return Ok(PersistedProcessStop::LeaderMissing),
        PersistedProcessState::OriginalLeader => {}
    }
    unsafe {
        libc::kill(-(identity.group as libc::pid_t), libc::SIGKILL);
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while process_group_alive(identity.group) {
        if Instant::now() >= deadline {
            return Err("the persisted process group did not exit".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Ok(PersistedProcessStop::StoppedOriginal)
}

pub(super) fn stop_agent_process_group(
    pid: Option<i64>,
    start_time: Option<i64>,
    group: Option<i64>,
) -> Result<PersistedProcessStop, String> {
    let identity = StageProcessIdentity {
        pid: pid
            .and_then(|pid| u32::try_from(pid).ok())
            .ok_or_else(|| "the persisted agent PID is invalid".to_owned())?,
        start_time: start_time
            .ok_or_else(|| "the persisted agent start time is missing".to_owned())?,
        group: group.ok_or_else(|| "the persisted agent process group is missing".to_owned())?,
        merge: None,
    };
    stop_persisted_process_group(&identity)
}

fn process_exists(pid: u32) -> bool {
    let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
    result == 0 || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProcessIdentity {
    Matches,
    GoneOrReused,
    Unverifiable,
}

pub(super) fn recoverable_process_identity(run: &RecoverableRun) -> ProcessIdentity {
    if run.state != RunState::Running {
        return ProcessIdentity::GoneOrReused;
    }
    let Some(pid) = run.pid.and_then(|pid| u32::try_from(pid).ok()) else {
        return ProcessIdentity::GoneOrReused;
    };
    match (run.pid_start_time, process_start_time(pid)) {
        (Some(expected), Some(actual)) if expected == actual => ProcessIdentity::Matches,
        (Some(_), Some(_)) => ProcessIdentity::GoneOrReused,
        (_, None) if process_exists(pid) => ProcessIdentity::Unverifiable,
        (_, None) => ProcessIdentity::GoneOrReused,
        (None, Some(_)) => ProcessIdentity::Unverifiable,
    }
}

pub(super) fn classify_run_output(
    classifier: &VendorErrorClassifier,
    state_dir: &Path,
    run_id: &str,
    exit_code: Option<i32>,
) -> Result<Option<VendorErrorMatch>, String> {
    let mut scanner = classifier.scanner(exit_code);
    crate::run_log::visit_agent_output(&run_output_path(state_dir, run_id), |stream, bytes| {
        match stream {
            OutputStream::Stdout => scanner.feed_stdout(bytes),
            OutputStream::Stderr => scanner.feed_stderr(bytes),
        }
    })
    .map_err(|error| format!("cannot read captured agent output: {error}"))?;
    Ok(scanner.finish())
}

#[cfg(test)]
mod tests {
    use super::{
        PersistedProcessState, ProcessIdentity, classify_persisted_process,
        recoverable_process_identity, recoverable_process_matches,
    };
    use crate::run_store::{RecoverableRun, RunState};
    use crate::runner::local::process_start_time;

    fn recoverable_current_process(start_time: Option<i64>) -> RecoverableRun {
        RecoverableRun {
            id: "R1".into(),
            ticket_id: "T1".into(),
            target: "fake".into(),
            state: RunState::Running,
            branch: None,
            worktree_path: None,
            pid: Some(i64::from(std::process::id())),
            pid_start_time: start_time,
            process_group_id: None,
            worker_token: None,
            worker_socket_path: None,
            exit_code: None,
            lease_expires_at_ms: 1,
            flow_json: None,
            ticket_json: None,
        }
    }

    #[test]
    fn recovery_requires_both_pid_and_start_time_to_match() {
        let Some(start_time) = process_start_time(std::process::id()) else {
            return;
        };
        assert!(recoverable_process_matches(&recoverable_current_process(
            Some(start_time)
        )));
        assert!(!recoverable_process_matches(&recoverable_current_process(
            Some(start_time + 1)
        )));
        assert!(!recoverable_process_matches(&recoverable_current_process(
            None
        )));
        assert_eq!(
            recoverable_process_identity(&recoverable_current_process(None)),
            ProcessIdentity::Unverifiable
        );
    }

    #[test]
    fn persisted_process_identity_requires_the_recorded_leader_to_signal() {
        assert_eq!(
            classify_persisted_process(10, Some(10), true),
            PersistedProcessState::OriginalLeader
        );
        assert_eq!(
            classify_persisted_process(10, Some(11), true),
            PersistedProcessState::ReusedLeader
        );
        assert_eq!(
            classify_persisted_process(10, None, false),
            PersistedProcessState::LeaderMissing
        );
        assert_eq!(
            classify_persisted_process(10, None, true),
            PersistedProcessState::UnverifiableLeader
        );
    }
}
