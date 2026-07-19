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
use crate::flow::Flow;
use crate::logging::{LogLevel, OperationalLog};
use crate::run_log::OutputStream;
use crate::runner::WorkerCredentials;
use crate::runner::local::{
    process_start_time, run_output_path, wait_for_test_hook, worker_socket_path,
};
use crate::store::{ExitClaim, RecoverableRun, Store};
use crate::vendor_error::{VendorErrorClassifier, VendorErrorMatch};

use super::aftercare::{
    aftercare_cancelled, drive_flow, git_index_lock_path, git_index_matches_head, git_is_ancestor,
    git_stdout, shared_checkout_has_git_operation, try_commits_on_branch,
};
use super::dispatcher::{DispatcherState, RunEvent};
use super::scheduler::{VENDOR_COOLDOWN_MS, bound_flow};
use super::server::{DaemonError, serve_worker_socket};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RecoveryClassification {
    Aftercare,
    Orphaned,
}

/// Classifies every durable lease before normal dispatch. Processes that are
/// live or cannot be disproved consume capacity and are monitored by identity;
/// dead or reused PIDs are settled from the work preserved in their branches.
pub(super) fn recover_inflight_runs(
    state: &mut DispatcherState,
    events: &mpsc::Sender<RunEvent>,
    log: &OperationalLog,
) -> Result<(), DaemonError> {
    let runs = state.store.recoverable_runs().map_err(DaemonError::Store)?;
    for run in runs {
        // Every durable lease consumes capacity until adoption or settlement
        // succeeds; a transient database error must never permit double-spawn.
        state.active.insert(run.id.clone());
        match recoverable_process_identity(&run) {
            ProcessIdentity::Matches | ProcessIdentity::Unverifiable => {
                state.supervised.insert(run.id.clone());
                let cancellation_requested = state
                    .store
                    .cancellation_requested(&run.id)
                    .map_err(DaemonError::Store)?;
                if cancellation_requested {
                    state.cancelling.insert(run.id.clone());
                    if recoverable_process_matches(&run)
                        && let Some(group) = run.process_group_id
                    {
                        unsafe {
                            libc::kill(-(group as libc::pid_t), libc::SIGKILL);
                        }
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
                if run.state == "aftercare" {
                    if let Err(error) = restore_worker_socket(state, &run) {
                        log.emit_with_fields(
                            LogLevel::Error,
                            "sloop::recovery",
                            "worker_socket_restore_failed",
                            json!({"run_id": run.id, "error": error}),
                        );
                    }
                    spawn_aftercare_recovery(state, events.clone(), run, log.clone())?;
                } else {
                    spawn_dead_run_recovery(state, events.clone(), run, log.clone());
                }
            }
        }
    }
    Ok(())
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
    state.worker_tokens.insert(run.id.clone(), token.clone());
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
                    let claim = if run.state == "running" {
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
        aftercare_failed: false,
        merge: None,
        vendor_error,
        cooldown_until_ms,
        recovery: Some(RecoveryClassification::Orphaned),
    })
}

pub(super) fn spawn_aftercare_recovery(
    state: &DispatcherState,
    events: mpsc::Sender<RunEvent>,
    run: RecoverableRun,
    log: OperationalLog,
) -> Result<(), DaemonError> {
    let root = state.root.clone();
    let state_dir = state.state_dir.clone();
    let test_cmd = state.aftercare_test_cmd.clone();
    let flow = match run.flow_json.as_deref() {
        Some(snapshot) => serde_json::from_str::<Flow>(snapshot).map_err(|error| {
            DaemonError::InvalidResponse(format!(
                "run `{}` has an invalid flow snapshot: {error}",
                run.id
            ))
        })?,
        None => bound_flow(&state.store, &state.flows, &run.ticket_id)
            .map_err(DaemonError::InvalidResponse)?,
    };
    let clock = state.clock.clone();
    let db_path = state.state_dir.join("sloop.db");
    let shutdown = state.shutdown_flag.clone();
    let worker = WorkerCredentials {
        socket: run
            .worker_socket_path
            .as_deref()
            .map(PathBuf::from)
            .unwrap_or_else(|| worker_socket_path(&state.runtime_dir, &run.id)),
        token: run.worker_token.clone().unwrap_or_default(),
    };
    tokio::task::spawn_blocking(move || {
        while !shutdown.load(Ordering::Acquire) {
            let result = Store::open(&db_path, clock.now_ms())
                .map_err(|error| error.to_string())
                .and_then(|store| {
                    resume_aftercare(
                        &root,
                        &state_dir,
                        &flow,
                        &worker,
                        test_cmd.as_deref(),
                        clock.as_ref(),
                        &store,
                        &run,
                        &log,
                    )
                });
            match result {
                Ok(event) => {
                    let _ = events.blocking_send(event);
                    break;
                }
                Err(error) => {
                    log.emit_with_fields(
                        LogLevel::Error,
                        "sloop::recovery",
                        "aftercare_resume_failed",
                        json!({"run_id": run.id, "error": error}),
                    );
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        }
    });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn resume_aftercare(
    root: &Path,
    state_dir: &Path,
    flow: &Flow,
    worker: &WorkerCredentials,
    test_cmd: Option<&[String]>,
    clock: &dyn Clock,
    store: &Store,
    run: &RecoverableRun,
    log: &OperationalLog,
) -> Result<RunEvent, String> {
    let rows = store
        .run_evidence(&run.id)
        .map_err(|error| error.to_string())?;
    let value = |kind: &str| {
        rows.iter()
            .find(|(candidate, _)| candidate == kind)
            .and_then(|(_, data)| serde_json::from_str::<serde_json::Value>(data).ok())
    };
    let commit_observation = value("commits_observed")
        .and_then(|data| {
            data["oids"].as_array().map(|oids| {
                let complete = data["complete"].as_bool().unwrap_or(true);
                let commits = oids
                    .iter()
                    .filter_map(|oid| oid.as_str().map(str::to_owned))
                    .collect::<Vec<_>>();
                (commits, complete)
            })
        })
        .ok_or_else(|| "the aftercare checkpoint has no valid commit evidence".to_owned())?;
    let (commits, commit_observation_complete) = commit_observation;
    let exit_code = run.exit_code.and_then(|code| i32::try_from(code).ok());
    let vendor_error = value("vendor_error_classified")
        .and_then(|data| serde_json::from_value::<VendorErrorMatch>(data).ok());
    let cooldown_until_ms =
        value("vendor_error_classified").and_then(|data| data["cooldown_until_ms"].as_i64());
    let output_path = run_output_path(state_dir, &run.id);
    if aftercare_cancelled(store, &run.id, log) {
        return Ok(RunEvent::Exited {
            run_id: run.id.clone(),
            target: run.target.clone(),
            exit_code,
            capture_complete: !rows.iter().any(|(kind, _)| kind == "capture_incomplete"),
            commits,
            commit_observation_complete,
            aftercare_failed: false,
            merge: None,
            vendor_error,
            cooldown_until_ms,
            recovery: Some(RecoveryClassification::Aftercare),
        });
    }
    let worktree = run
        .worktree_path
        .as_deref()
        .ok_or_else(|| "the aftercare checkpoint has no worktree".to_owned())?;
    let branch = run
        .branch
        .as_deref()
        .ok_or_else(|| "the aftercare checkpoint has no branch".to_owned())?;
    let result = drive_flow(
        root,
        Path::new(worktree),
        branch,
        flow,
        worker,
        test_cmd,
        exit_code,
        vendor_error.is_some(),
        &commits,
        commit_observation_complete,
        &output_path,
        clock,
        store,
        &run.id,
        log,
    )?;

    Ok(RunEvent::Exited {
        run_id: run.id.clone(),
        target: run.target.clone(),
        exit_code,
        capture_complete: !rows.iter().any(|(kind, _)| kind == "capture_incomplete"),
        commits,
        commit_observation_complete,
        aftercare_failed: result.aftercare_failed,
        merge: result.merge,
        vendor_error,
        cooldown_until_ms,
        recovery: Some(RecoveryClassification::Aftercare),
    })
}

pub(super) fn stop_interrupted_process(
    rows: &[(String, String)],
    stage: &str,
) -> Result<Option<(AftercareProcessIdentity, PersistedProcessStop)>, String> {
    let Some(identity) = aftercare_process_identity(rows, Some(stage))? else {
        return Ok(None);
    };
    if identity.group <= 0 {
        return Err("the interrupted aftercare stage has an invalid process group".into());
    }
    let stopped = stop_persisted_process_group(&identity)?;
    Ok(Some((identity, stopped)))
}

pub(super) fn aftercare_process_identity(
    rows: &[(String, String)],
    stage: Option<&str>,
) -> Result<Option<AftercareProcessIdentity>, String> {
    let Some(data) = rows
        .iter()
        .find(|(candidate, _)| candidate == "aftercare_process")
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
        .ok_or_else(|| "the interrupted aftercare stage has no valid pid".to_owned())?;
    let start_time = data["pid_start_time"]
        .as_i64()
        .ok_or_else(|| "the interrupted aftercare stage has no valid start time".to_owned())?;
    let group = data["process_group_id"]
        .as_i64()
        .ok_or_else(|| "the interrupted aftercare stage has no valid process group".to_owned())?;
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
    Ok(Some(AftercareProcessIdentity {
        pid,
        start_time,
        group,
        merge,
    }))
}

pub(super) fn recoverable_process_matches(run: &RecoverableRun) -> bool {
    recoverable_process_identity(run) == ProcessIdentity::Matches
}

/// Claims the same durable exit handoff used by the normal supervisor. A
/// racing loser emits no settlement event and leaves aftercare to the winner.
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
    } = event;
    let now_ms = clock.now_ms();
    let result = Store::open(db_path, now_ms).and_then(|mut store| {
        store.record_agent_exit(
            run_id,
            *exit_code,
            *capture_complete,
            &json!({"complete": commit_observation_complete, "oids": commits}).to_string(),
            vendor_error.as_ref(),
            *cooldown_until_ms,
            now_ms,
        )
    });
    match result {
        Ok(ExitClaim::Claimed) => Ok(true),
        Ok(ExitClaim::AlreadyClaimed { state }) => {
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
pub(super) fn reconcile_run_liveness(
    state: &mut DispatcherState,
    events: &mpsc::Sender<RunEvent>,
    log: &OperationalLog,
) {
    let runs = match state.store.recoverable_runs() {
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
        state.active.insert(run.id.clone());
        if state.recovering.contains(&run.id) {
            continue;
        }
        if run.state == "aftercare" && state.supervised.contains(&run.id) {
            continue;
        }
        match recoverable_process_identity(&run) {
            ProcessIdentity::Matches => {
                state.suspected_dead.remove(&run.id);
                continue;
            }
            ProcessIdentity::Unverifiable => {
                state.suspected_dead.remove(&run.id);
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
        if run.state == "aftercare" {
            if let Err(error) =
                spawn_aftercare_recovery(state, events.clone(), run.clone(), log.clone())
            {
                state.recovering.remove(&run.id);
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::recovery",
                    "aftercare_recovery_start_failed",
                    json!({"run_id": run.id, "error": error.to_string()}),
                );
            }
        } else {
            spawn_dead_run_recovery(state, events.clone(), run, log.clone());
        }
    }
    wait_for_test_hook("after-run-liveness-reconciliation");
}

#[derive(Debug, Clone)]
pub(super) struct AftercareProcessIdentity {
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
    identity: &AftercareProcessIdentity,
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

fn persisted_process_state(identity: &AftercareProcessIdentity) -> PersistedProcessState {
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
    identity: &AftercareProcessIdentity,
) -> Result<PersistedProcessStop, String> {
    if identity.group <= 0
        || identity.group != i64::from(identity.pid)
        || libc::pid_t::try_from(identity.group).is_err()
    {
        return Err("the persisted aftercare process group is not its recorded leader".into());
    }
    match persisted_process_state(identity) {
        PersistedProcessState::ReusedLeader => {
            return Err("the aftercare process group ID was reused; refusing to signal it".into());
        }
        PersistedProcessState::UnverifiableLeader => {
            return Err("cannot verify the persisted aftercare process leader".into());
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
            return Err("the interrupted aftercare process group did not exit".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Ok(PersistedProcessStop::StoppedOriginal)
}

fn process_exists(pid: u32) -> bool {
    let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
    result == 0 || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessIdentity {
    Matches,
    GoneOrReused,
    Unverifiable,
}

fn recoverable_process_identity(run: &RecoverableRun) -> ProcessIdentity {
    if run.state != "running" {
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
    use crate::runner::local::process_start_time;
    use crate::store::RecoverableRun;

    fn recoverable_current_process(start_time: Option<i64>) -> RecoverableRun {
        RecoverableRun {
            id: "R1".into(),
            ticket_id: "T1".into(),
            target: "fake".into(),
            state: "running".into(),
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
