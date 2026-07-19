use std::cell::Cell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use serde_json::json;
use tokio::sync::{mpsc, oneshot};

use crate::clock::{Clock, format_timestamp, next_local_minute_ms};
use crate::config::{AgentConfig, RunningHours, parse_local_time};
use crate::coordination::Coordination;
use crate::flow::Flow;
use crate::logging::{LogLevel, OperationalLog};
use crate::outcome::{MergeOutcome, RunEvidence, classify_exit, derive_outcome};
use crate::protocol::{ErrorBody, ErrorCode, Request, RequestId, ResponseEnvelope};
use crate::runner::local::worker_socket_path;
use crate::sources::TicketSource;
use crate::store::{CooldownUpdate, EvidenceRecord, Store, StoreError};
use crate::vendor_error::{VendorErrorClassifier, VendorErrorMatch};

use super::commands::{
    handle_cancel, handle_events, handle_hold, handle_list, handle_logs, handle_operator_show,
    handle_ready, handle_reindex, handle_retry, handle_run, handle_stop, handle_wait,
};
use super::recovery::{RecoveryClassification, reconcile_run_liveness};
use super::scheduler::{next_dispatch_deadline, reconcile};
use super::worker_api::dispatch_worker;

pub(super) const LOGS_PAGE_LIMIT: usize = 64;

pub(super) enum DispatcherMessage {
    Request {
        id: RequestId,
        request: Request,
        origin: RequestOrigin,
        reply: oneshot::Sender<ResponseEnvelope>,
    },
    RestartAcknowledged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DaemonControl {
    Stop,
    Restart,
}

/// Which socket a request arrived on. Worker requests carry the run whose
/// socket accepted the connection plus the token the caller presented; the
/// dispatcher owns the comparison against the run's issued token.
pub(super) enum RequestOrigin {
    Operator,
    Worker {
        run_id: String,
        token: Option<String>,
    },
}

pub(super) struct DispatcherState {
    pub(super) pid: u32,
    pub(super) paused: bool,
    pub(super) draining: bool,
    pub(super) restart_acknowledged: bool,
    pub(super) restart_signalled: bool,
    pub(super) max_agents: usize,
    pub(super) ticket_prefix: String,
    pub(super) project_prefix: String,
    pub(super) running_hours: Option<RunningHours>,
    pub(super) agent: Option<AgentConfig>,
    pub(super) flows: BTreeMap<String, Flow>,
    pub(super) default_flow: String,
    pub(super) aftercare_test_cmd: Option<Vec<String>>,
    pub(super) root: PathBuf,
    pub(super) project_dir: PathBuf,
    pub(super) ticket_dir: PathBuf,
    pub(super) ticket_source: Arc<dyn TicketSource>,
    pub(super) worktree_dir: PathBuf,
    pub(super) state_dir: PathBuf,
    pub(super) runtime_dir: PathBuf,
    pub(super) socket: PathBuf,
    pub(super) daemon_log: PathBuf,
    pub(super) store: Store,
    /// `SQLITE_FULL` is a dispatcher gate. The daemon retains active and
    /// pending run evidence in memory until a committed probe succeeds.
    pub(super) storage_full: Cell<bool>,
    /// A failed durable liveness scan closes the spawn gate until a later scan
    /// succeeds, so incomplete capacity information cannot over-dispatch.
    pub(super) reconciliation_blocked: bool,
    /// Run IDs with a durable nonterminal lease; its size is the capacity gate.
    pub(super) active: HashSet<String>,
    /// Run IDs whose normal or re-adopted supervisor still owns execution.
    pub(super) supervised: HashSet<String>,
    /// Supervised run IDs observed dead once. A second consecutive observation
    /// starts recovery, leaving the normal supervisor one interval to finish
    /// draining output and claim the durable exit handoff.
    pub(super) suspected_dead: HashSet<String>,
    /// Run IDs with a recovery task in flight. The entry remains until final
    /// settlement so a normal supervisor racing recovery cannot duplicate it.
    pub(super) recovering: HashSet<String>,
    /// Run IDs whose cancellation was requested but whose exit has not been
    /// resolved yet; mirrors the durable `cancel_requested` evidence.
    pub(super) cancelling: HashSet<String>,
    /// Tokens issued to live runs; a worker request must present its run's
    /// token exactly. Entries die with the run.
    pub(super) worker_tokens: HashMap<String, String>,
    /// Accept-loop tasks for live per-run worker sockets, aborted at settle.
    pub(super) worker_listeners: HashMap<String, tokio::task::JoinHandle<()>>,
    pub(super) worker_socket_paths: HashMap<String, PathBuf>,
    /// Exit evidence remains here until its atomic store transaction commits.
    /// The dispatcher retries these records on every reconciliation pass.
    pub(super) pending_exits: HashMap<String, RunEvent>,
    /// The dispatcher's own request channel, cloned into each worker
    /// accept loop so every request funnels through the single owner.
    pub(super) requests_tx: mpsc::Sender<DispatcherMessage>,
    pub(super) log: OperationalLog,
    pub(super) clock: Arc<dyn Clock>,
    pub(super) classifier: Arc<VendorErrorClassifier>,
    /// Signals the accept loop to end the process; used by daemon-side
    /// exits such as the project-root liveness check.
    pub(super) shutdown: mpsc::Sender<DaemonControl>,
    pub(super) shutdown_flag: Arc<AtomicBool>,
}

/// Internal dispatcher events reported by effect tasks, never by clients.
pub(super) enum RunEvent {
    Exited {
        run_id: String,
        target: String,
        exit_code: Option<i32>,
        /// False when a pipe reader failed to durably record every chunk;
        /// the loss becomes explicit run evidence instead of silence.
        capture_complete: bool,
        /// Commits made after the run branch was created. This is activity
        /// metadata only; it does not determine the run's outcome.
        commits: Vec<String>,
        commit_observation_complete: bool,
        aftercare_failed: bool,
        merge: Option<MergeOutcome>,
        vendor_error: Option<VendorErrorMatch>,
        cooldown_until_ms: Option<i64>,
        recovery: Option<RecoveryClassification>,
    },
}

pub(super) async fn run_dispatcher(
    mut state: DispatcherState,
    mut requests: mpsc::Receiver<DispatcherMessage>,
    mut events: mpsc::Receiver<RunEvent>,
    events_tx: mpsc::Sender<RunEvent>,
    log: OperationalLog,
) {
    let mut liveness_tick = tokio::time::interval(Duration::from_secs(2));
    liveness_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Tokio intervals fire immediately once; consume that tick because startup
    // recovery already classified every durable lease.
    liveness_tick.tick().await;
    reconcile(&mut state, &events_tx, &log);
    loop {
        let deadline = next_dispatch_deadline(&state);
        let clock = state.clock.clone();
        tokio::select! {
            message = requests.recv() => {
                let Some(message) = message else { break };
                match message {
                    DispatcherMessage::Request { id, request, origin, reply } => {
                        let response = match origin {
                            RequestOrigin::Operator => dispatch(&mut state, id, request),
                            RequestOrigin::Worker { run_id, token } => dispatch_worker(
                                &mut state,
                                id,
                                request,
                                &run_id,
                                token.as_deref(),
                            ),
                        };
                        let _ = reply.send(response);
                        log.emit(LogLevel::Info, "sloop::dispatcher", "request_handled");
                    }
                    DispatcherMessage::RestartAcknowledged => {
                        state.restart_acknowledged = true;
                    }
                }
            }
            event = events.recv() => {
                let Some(event) = event else { break };
                settle_run_exit(&mut state, event, &log);
            }
            () = wait_for_deadline(clock, deadline) => {
                log.emit(LogLevel::Info, "sloop::dispatcher", "timer_fired");
            }
            // Wall-clock is deliberate: this is a liveness probe, not
            // decision logic, so the manual test clock must not gate it.
            _ = liveness_tick.tick() => {
                if !state.root.join(".git").exists() {
                    log.emit(LogLevel::Error, "sloop::dispatcher", "project_root_missing");
                    let _ = state.shutdown.send(DaemonControl::Stop).await;
                    break;
                }
                reconcile_run_liveness(&mut state, &events_tx, &log);
            }
        }
        reconcile(&mut state, &events_tx, &log);
        if complete_restart_if_ready(&mut state).await {
            break;
        }
    }
}

async fn complete_restart_if_ready(state: &mut DispatcherState) -> bool {
    if !state.draining
        || !state.restart_acknowledged
        || state.restart_signalled
        || !state.active.is_empty()
    {
        return false;
    }
    state.restart_signalled = true;
    state
        .log
        .emit(LogLevel::Info, "sloop::daemon", "restart_drain_complete");
    let _ = state.shutdown.send(DaemonControl::Restart).await;
    true
}

async fn wait_for_deadline(clock: Arc<dyn Clock>, deadline: Option<i64>) {
    match deadline {
        Some(deadline) => clock.sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Resolves one finished run: derives the outcome from the gathered evidence
/// and commits the whole settlement in one store transaction. Cancellation
/// intent recorded before the exit wins over every other reading, keeping a
/// racing `cancel` and natural exit idempotent.
fn settle_run_exit(state: &mut DispatcherState, event: RunEvent, log: &OperationalLog) {
    let run_id = match &event {
        RunEvent::Exited { run_id, .. } => run_id.clone(),
    };
    state.pending_exits.insert(run_id, event);
    if !state.storage_full.get() {
        settle_pending_exits(state, log);
    }
}

pub(super) fn settle_pending_exits(state: &mut DispatcherState, log: &OperationalLog) {
    let run_ids: Vec<String> = state.pending_exits.keys().cloned().collect();
    for run_id in run_ids {
        let Some(event) = state.pending_exits.remove(&run_id) else {
            continue;
        };
        match try_settle_run_exit(state, &event) {
            Ok((ticket_id, outcome, applied)) => {
                state.cancelling.remove(&run_id);
                state.active.remove(&run_id);
                state.supervised.remove(&run_id);
                state.suspected_dead.remove(&run_id);
                state.recovering.remove(&run_id);
                close_worker_socket(state, &run_id);
                log.emit_with_fields(
                    LogLevel::Info,
                    "sloop::dispatcher",
                    "run_exited",
                    json!({"run_id": run_id, "outcome": outcome.as_str()}),
                );
                if applied {
                    let ticket_source = state.ticket_source.clone();
                    let report_log = log.clone();
                    tokio::task::spawn_blocking(move || {
                        if let Err(error) = ticket_source.report(&ticket_id, &outcome) {
                            report_log.emit_with_fields(
                                LogLevel::Warn,
                                "sloop::dispatcher",
                                "ticket_source_report_failed",
                                json!({"ticket_id": ticket_id, "outcome": outcome.as_str(), "error": error.to_string()}),
                            );
                        }
                    });
                }
            }
            Err(error) => {
                let disk_full = error.is_disk_full();
                mark_storage_full(state, &error);
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::dispatcher",
                    "run_exit_persist_failed",
                    json!({"run_id": run_id, "error": error.to_string()}),
                );
                state.pending_exits.insert(run_id, event);
                if disk_full {
                    break;
                }
            }
        }
    }
}

fn try_settle_run_exit(
    state: &mut DispatcherState,
    event: &RunEvent,
) -> Result<(String, crate::outcome::Outcome, bool), StoreError> {
    let RunEvent::Exited {
        run_id,
        target,
        exit_code,
        capture_complete,
        commits,
        commit_observation_complete,
        aftercare_failed,
        merge,
        vendor_error,
        cooldown_until_ms,
        recovery,
    } = event;

    let cancelled =
        state.cancelling.contains(run_id) || state.store.cancellation_requested(run_id)?;
    let evidence = RunEvidence {
        cancelled,
        exit: classify_exit(*exit_code),
        vendor_error: vendor_error.as_ref().map(|error| error.class),
        commit_count: commit_observation_complete.then_some(commits.len()),
        aftercare_failed: *aftercare_failed,
        merge: *merge,
    };
    let outcome = if *recovery == Some(RecoveryClassification::Orphaned)
        && !cancelled
        && vendor_error.is_none()
    {
        crate::outcome::Outcome::Orphaned
    } else {
        derive_outcome(&evidence)
    };

    let mut records = vec![
        EvidenceRecord {
            kind: "exit_classified",
            data_json: json!({"exit_code": exit_code}).to_string(),
        },
        EvidenceRecord {
            kind: "commits_observed",
            data_json: json!({"complete": commit_observation_complete, "oids": commits})
                .to_string(),
        },
    ];
    if let Some(classification) = recovery {
        records.push(EvidenceRecord {
            kind: "recovery_classified",
            data_json: json!({
                "classification": match classification {
                    RecoveryClassification::Aftercare => "aftercare",
                    RecoveryClassification::Orphaned => "orphaned",
                }
            })
            .to_string(),
        });
    }
    if let Some(merge) = *merge {
        records.push(EvidenceRecord {
            kind: "merge_result",
            data_json: json!({"merged": merge == MergeOutcome::Merged}).to_string(),
        });
    }
    if !capture_complete {
        records.push(EvidenceRecord {
            kind: "capture_incomplete",
            data_json: json!({}).to_string(),
        });
    }
    if let Some(vendor_error) = vendor_error {
        records.push(EvidenceRecord {
            kind: "vendor_error_classified",
            data_json: vendor_error.evidence_json(*cooldown_until_ms),
        });
    }
    let ticket_id = state
        .store
        .run(run_id)?
        .ok_or_else(|| StoreError::RunNotFound {
            run_id: run_id.clone(),
        })?
        .ticket_id;
    let cooldown = vendor_error
        .as_ref()
        .filter(|error| error.class.requires_cooldown() && !cancelled)
        .and_then(|error| cooldown_until_ms.map(|until_ms| (error, until_ms)))
        .map(|(error, until_ms)| CooldownUpdate {
            target,
            until_ms,
            reason: &error.diagnostic,
        });
    let applied = Coordination::new(&mut state.store).settle(
        run_id,
        &ticket_id,
        *exit_code,
        outcome,
        &records,
        cooldown.as_ref(),
        state.clock.now_ms(),
    )?;
    Ok((ticket_id, outcome, applied))
}

/// Tears down a run's worker boundary: the token stops validating, the
/// accept loop ends, and the socket file disappears. Idempotent, so crash
/// recovery and racing settlements can call it freely.
pub(super) fn close_worker_socket(state: &mut DispatcherState, run_id: &str) {
    state.worker_tokens.remove(run_id);
    if let Some(listener) = state.worker_listeners.remove(run_id) {
        listener.abort();
    }
    let socket_path = state
        .worker_socket_paths
        .remove(run_id)
        .unwrap_or_else(|| worker_socket_path(&state.runtime_dir, run_id));
    let _ = fs::remove_file(socket_path);
}

pub(super) fn mark_storage_full(state: &DispatcherState, error: &StoreError) {
    if error.is_disk_full() && !state.storage_full.replace(true) {
        state.log.emit_with_fields(
            LogLevel::Error,
            "sloop::dispatcher",
            "storage_full",
            json!({"error": error.to_string()}),
        );
    }
}

pub(super) fn recover_storage(state: &DispatcherState, now_ms: i64) -> bool {
    if !state.storage_full.get() {
        return true;
    }
    match state.store.probe_writable(now_ms) {
        Ok(()) => {
            state.storage_full.set(false);
            state
                .log
                .emit(LogLevel::Info, "sloop::dispatcher", "storage_recovered");
            true
        }
        Err(error) => {
            mark_storage_full(state, &error);
            false
        }
    }
}

fn dispatch(state: &mut DispatcherState, id: RequestId, request: Request) -> ResponseEnvelope {
    let data = match request {
        Request::Show(args) => match handle_operator_show(state, &args.reference) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Run(args) => match handle_run(state, &args) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Daemon(_) => json!({
            "pid": state.pid,
            "socket": state.socket.to_string_lossy(),
            "state_dir": state.state_dir.to_string_lossy(),
            "log": state.daemon_log.to_string_lossy(),
            "version": env!("CARGO_PKG_VERSION"),
            "started": false
        }),
        Request::Restart(_) => {
            let active_runs = state.active.len();
            let changed = match state
                .store
                .begin_restart_draining(active_runs, state.clock.now_ms())
            {
                Ok(changed) => changed,
                Err(error) => {
                    mark_storage_full(state, &error);
                    return ResponseEnvelope::failure(
                        Some(id),
                        internal(&format!("cannot begin daemon restart: {error}")),
                    );
                }
            };
            state.draining = true;
            state.restart_acknowledged = false;
            if changed {
                state.log.emit_with_fields(
                    LogLevel::Info,
                    "sloop::daemon",
                    "restart_drain_started",
                    json!({"active_runs": active_runs}),
                );
            }
            json!({
                "draining": true,
                "active_runs": active_runs,
                "pid": state.pid,
            })
        }
        Request::Post(args) => {
            let now_ms = state.clock.now_ms();
            let at_eligible_ms = match &args.activation {
                crate::protocol::PostActivation::At { time } => {
                    let Some(minute) = parse_local_time(time) else {
                        return ResponseEnvelope::failure(
                            Some(id),
                            invalid_arguments(&format!(
                                "time `{time}` must use a valid HH:MM value"
                            )),
                        );
                    };
                    let Some(eligible_at_ms) =
                        next_local_minute_ms(state.clock.as_ref(), now_ms, minute)
                    else {
                        return ResponseEnvelope::failure(
                            Some(id),
                            invalid_arguments("the requested local time is out of range"),
                        );
                    };
                    Some(eligible_at_ms)
                }
                _ => None,
            };
            match crate::post::handle(
                &state.root,
                &state.ticket_dir,
                &state.store,
                &args,
                now_ms,
                at_eligible_ms,
                &state.ticket_prefix,
                state.agent.as_ref(),
                &state.flows,
                &state.default_flow,
            ) {
                Ok(data) => data,
                Err(error) => {
                    if let crate::post::PostError::Store(store_error) = &error {
                        mark_storage_full(state, store_error);
                    }
                    return ResponseEnvelope::failure(Some(id), post_error_body(&error));
                }
            }
        }
        Request::List(_) => match handle_list(state) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Status(_) => {
            let tickets = match state.store.ticket_counts() {
                Ok(counts) => counts,
                Err(error) => {
                    return ResponseEnvelope::failure(
                        Some(id),
                        internal(&format!("cannot read ticket counts: {error}")),
                    );
                }
            };
            let runs: Vec<_> = match state.store.active_runs() {
                Ok(runs) => runs
                    .into_iter()
                    .map(|run| {
                        json!({
                            "id": run.id,
                            "project": run.project_id,
                            "ticket": run.ticket_id,
                            "state": run.state,
                        })
                    })
                    .collect(),
                Err(error) => {
                    return ResponseEnvelope::failure(
                        Some(id),
                        internal(&format!("cannot read active runs: {error}")),
                    );
                }
            };
            let active_agents = runs.len();
            let queued: Vec<_> = match state.store.queued_activations() {
                Ok(activations) => activations
                    .into_iter()
                    .map(|activation| {
                        json!({
                            "id": activation.id,
                            "ticket": activation.ticket_id,
                            "project": activation.project_id,
                            "state": "queued",
                        })
                    })
                    .collect(),
                Err(error) => {
                    return ResponseEnvelope::failure(
                        Some(id),
                        internal(&format!("cannot read queued activations: {error}")),
                    );
                }
            };
            let now_ms = state.clock.now_ms();
            let mut gate = json!({
                "active_agents": active_agents,
                "max_agents": state.max_agents,
                "capacity_reconciled": !state.reconciliation_blocked,
                "storage": {
                    "writable": !state.storage_full.get(),
                    "reason": state.storage_full.get().then_some("database_full"),
                },
            });
            if let Some(hours) = &state.running_hours {
                gate["running_hours"] = json!({
                    "start": hours.start,
                    "end": hours.end,
                    "open": hours.is_open(state.clock.local_minute(now_ms)),
                });
            }
            let cooldowns = match state.store.active_cooldowns(now_ms) {
                Ok(cooldowns) => cooldowns
                    .into_iter()
                    .map(|cooldown| {
                        json!({
                            "target": cooldown.target,
                            "until_ms": cooldown.until_ms,
                            "reason": cooldown.reason,
                        })
                    })
                    .collect::<Vec<_>>(),
                Err(error) => {
                    return ResponseEnvelope::failure(
                        Some(id),
                        internal(&format!("cannot read cooldowns: {error}")),
                    );
                }
            };
            gate["cooldowns"] = json!(cooldowns);
            let mut snapshot = json!({
                "daemon": {
                    "pid": state.pid,
                    "paused": state.paused,
                    "draining": state.draining,
                },
                "gate": gate,
                "runs": runs,
                "queued_activations": queued,
                "tickets": {
                    "ready": tickets.ready,
                    "held": tickets.held,
                    "blocked": tickets.blocked,
                    "claimed": tickets.claimed,
                    "merged": tickets.merged,
                    "failed": tickets.failed,
                    "needs_review": tickets.needs_review
                }
            });
            if let Some(deadline) = next_dispatch_deadline(state)
                && let Some(formatted) = format_timestamp(deadline)
            {
                snapshot["next_wake"] = json!(formatted);
            }
            snapshot
        }
        Request::Pause(_) => {
            if let Err(error) = state.store.set_paused(true, state.clock.now_ms()) {
                mark_storage_full(state, &error);
                return ResponseEnvelope::failure(
                    Some(id),
                    internal(&format!("cannot pause scheduler: {error}")),
                );
            }
            state.paused = true;
            json!({"paused": true})
        }
        Request::Resume(_) => {
            let cancelled_restart = match state.store.resume_scheduler(state.clock.now_ms()) {
                Ok(cancelled) => cancelled,
                Err(error) => {
                    mark_storage_full(state, &error);
                    return ResponseEnvelope::failure(
                        Some(id),
                        internal(&format!("cannot resume scheduler: {error}")),
                    );
                }
            };
            state.paused = false;
            state.draining = false;
            state.restart_acknowledged = false;
            state.restart_signalled = false;
            if cancelled_restart {
                state
                    .log
                    .emit(LogLevel::Info, "sloop::daemon", "restart_cancelled");
            }
            json!({"paused": false, "restart_cancelled": cancelled_restart})
        }
        Request::Hold(args) => match handle_hold(state, &args) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Ready(args) => match handle_ready(state, &args) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Retry(args) => match handle_retry(state, &args) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Logs(args) => match handle_logs(state, &args) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Events(args) => match handle_events(state, &args) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Cancel(args) => match handle_cancel(state, &args) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Reindex(_) => match handle_reindex(state) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Stop(args) => match handle_stop(state, &args) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Wait(args) => match handle_wait(state, &args) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        request => {
            return ResponseEnvelope::failure(
                Some(id),
                ErrorBody {
                    code: ErrorCode::InvalidRequest,
                    message: format!("verb `{}` is not implemented by the daemon", request.verb()),
                    details: json!({"verb": request.verb()}),
                },
            );
        }
    };
    ResponseEnvelope::success(Some(id), data)
}

pub(super) fn invalid_arguments(message: &str) -> ErrorBody {
    ErrorBody {
        code: ErrorCode::InvalidArguments,
        message: message.into(),
        details: json!({}),
    }
}

pub(super) fn not_found(message: &str) -> ErrorBody {
    ErrorBody {
        code: ErrorCode::NotFound,
        message: message.into(),
        details: json!({}),
    }
}

pub(super) fn conflict(message: &str) -> ErrorBody {
    ErrorBody {
        code: ErrorCode::Conflict,
        message: message.into(),
        details: json!({}),
    }
}

fn post_error_body(error: &crate::post::PostError) -> ErrorBody {
    use crate::post::PostError;
    let code = match error {
        PostError::TicketFileNotFound(_)
        | PostError::UnknownProject(_)
        | PostError::UnknownFlow { .. }
        | PostError::UnknownBlockedBy { .. } => ErrorCode::NotFound,
        PostError::OutsideRepository(_)
        | PostError::OutsideTicketDirectory { .. }
        | PostError::InvalidTicket { .. }
        | PostError::MissingName { .. }
        | PostError::MissingBlockedBy { .. }
        | PostError::InvalidBlockedBy { .. }
        | PostError::EmptyBody { .. }
        | PostError::UnknownTarget(_)
        | PostError::MissingTargetValue { .. } => ErrorCode::InvalidArguments,
        PostError::ProjectConflict { .. }
        | PostError::FlowConflict { .. }
        | PostError::TicketIdTaken { .. }
        | PostError::DependencyCycle(_) => ErrorCode::Conflict,
        PostError::Io { .. } | PostError::Store(_) | PostError::IdAllocation(_) => {
            ErrorCode::Internal
        }
    };
    ErrorBody {
        code,
        message: error.to_string(),
        details: json!({}),
    }
}

pub(super) fn protocol_error(message: &str) -> ErrorBody {
    ErrorBody {
        code: ErrorCode::InvalidRequest,
        message: message.into(),
        details: json!({}),
    }
}

pub(super) fn unauthorized(message: &str) -> ErrorBody {
    ErrorBody {
        code: ErrorCode::Unauthorized,
        message: message.into(),
        details: json!({}),
    }
}

pub(super) fn internal(message: &str) -> ErrorBody {
    ErrorBody {
        code: ErrorCode::Internal,
        message: message.into(),
        details: json!({}),
    }
}
