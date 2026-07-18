use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::Duration;

use serde_json::json;
use tokio::net::UnixListener;
use tokio::sync::mpsc;

use crate::clock::Clock;
use crate::flow::Flow;
use crate::logging::{LogLevel, OperationalLog};
use crate::run_log::RunLogWriter;
use crate::store::{RecoverableRun, Store};
use crate::vendor_error::{VendorErrorClassifier, VendorErrorMatch};

use super::aftercare::{aftercare_cancelled, try_commits_on_branch};
use super::runner::{process_start_time, run_output_path, worker_socket_path};
use super::{
    AftercareProcessIdentity, DaemonError, DispatcherState, MergeProcessCheckpoint,
    PersistedProcessStop, ProcessIdentity, RunEvent, VENDOR_COOLDOWN_MS, bound_flow,
    claim_recovered_exit, classify_run_output, drive_flow, recoverable_process_identity,
    serve_worker_socket, stop_persisted_process_group,
};

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
    let flow = bound_flow(&state.store, &state.flows, &run.ticket_id)
        .map_err(DaemonError::InvalidResponse)?;
    let clock = state.clock.clone();
    let db_path = state.state_dir.join("sloop.db");
    let shutdown = state.shutdown_flag.clone();
    tokio::task::spawn_blocking(move || {
        while !shutdown.load(Ordering::Acquire) {
            let result = Store::open(&db_path, clock.now_ms())
                .map_err(|error| error.to_string())
                .and_then(|store| {
                    resume_aftercare(
                        &root,
                        &state_dir,
                        &flow,
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
    let output_log = RunLogWriter::open(&run_output_path(state_dir, &run.id))
        .map_err(|error| format!("cannot open run output: {error}"))?;
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
        test_cmd,
        exit_code,
        vendor_error.is_some(),
        &commits,
        commit_observation_complete,
        &output_log,
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

pub(super) fn process_identity_matches(pid: u32, expected_start_time: Option<i64>) -> bool {
    matches!(
        (expected_start_time, process_start_time(pid)),
        (Some(expected), Some(actual)) if expected == actual
    )
}
