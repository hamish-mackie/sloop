use std::cell::Cell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use serde_json::json;
use tokio::sync::{Notify, mpsc, oneshot};

use crate::clock::{Clock, next_local_minute_ms};
use crate::config::{AgentConfig, RunningHours, parse_local_time};
use crate::db::StoreError;
use crate::domain::work::{Disposition, TicketRef, WorkOutcome};
use crate::flow::Flow;
use crate::outcome::{FlowHalt, MergeOutcome, RunEvidence, classify_exit, derive_outcome};
use crate::protocol::{ErrorBody, ErrorCode, Request, RequestId, ResponseEnvelope};
use crate::run_ref::RunIdSource;
use crate::run_store::{CooldownUpdate, EvidenceRecord, RunStore};
use crate::runner::local::worker_socket_path;
use crate::runner::{WorkerCredentials, WorkerScope};
use crate::vendor::{VendorErrorClassifier, VendorErrorMatch};
use crate::work_state::local::LocalSqlite;
use crate::work_state::{SourceError, TicketFeeder, WorkState};

use super::commands::{
    handle_cancel, handle_events, handle_hold, handle_list, handle_logs, handle_operator_show,
    handle_ready, handle_reindex, handle_retry, handle_run, handle_status, handle_stop,
    handle_wait,
};
use super::logging::{LogLevel, OperationalLog};
use super::recovery::{RecoveryClassification, reconcile_run_liveness};
use super::scheduler::{next_dispatch_deadline, reconcile};
use super::server::serve_worker_socket;
use super::worker_api::dispatch_worker;

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
    pub(super) stall_report_after_ms: i64,
    pub(super) stall_after_ms: i64,
    pub(super) ticket_prefix: String,
    pub(super) project_prefix: String,
    pub(super) running_hours: Option<RunningHours>,
    pub(super) agent: Option<AgentConfig>,
    pub(super) flows: BTreeMap<String, Flow>,
    pub(super) default_flow: String,
    pub(super) flow_test_cmd: Option<Vec<String>>,
    pub(super) root: PathBuf,
    pub(super) project_dir: PathBuf,
    pub(super) ticket_dir: PathBuf,
    pub(super) ticket_source: TicketFeeder,
    pub(super) work_state_author_enabled: bool,
    pub(super) worktree_dir: PathBuf,
    pub(super) worktree_retention_ms: Option<i64>,
    pub(super) state_dir: PathBuf,
    pub(super) runtime_dir: PathBuf,
    pub(super) socket: PathBuf,
    pub(super) daemon_log: PathBuf,
    pub(super) local_work_state: LocalSqlite,
    pub(super) work_state: Arc<dyn WorkState>,
    pub(super) run_store: RunStore,
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
    /// Run IDs with durable output-stall intent awaiting final settlement.
    pub(super) stalling: HashSet<String>,
    /// Credentials issued to live runs; a worker request must present its
    /// run's token exactly. Entries die with the run.
    pub(super) worker_tokens: HashMap<String, IssuedWorker>,
    /// Accept-loop tasks for live per-run worker sockets, aborted at settle.
    pub(super) worker_listeners: HashMap<String, tokio::task::JoinHandle<()>>,
    pub(super) worker_socket_paths: HashMap<String, PathBuf>,
    /// Exit evidence remains here until its atomic store transaction commits.
    /// The dispatcher retries these records on every reconciliation pass.
    pub(super) pending_exits: HashMap<String, RunEvent>,
    /// Last output sequence warned for each run. A different sequence is a
    /// later silence episode and may warn again.
    pub(super) reported_stalls: HashMap<String, Option<u64>>,
    /// Output pumps notify the dispatcher after a durable append so its one
    /// existing deadline can be recomputed without a polling loop.
    pub(super) output_notify: Arc<Notify>,
    /// The dispatcher's own request channel, cloned into each worker
    /// accept loop so every request funnels through the single owner.
    pub(super) requests_tx: mpsc::Sender<DispatcherMessage>,
    pub(super) log: OperationalLog,
    pub(super) clock: Arc<dyn Clock>,
    /// Mints internal run ids at claim time. Injected so claim-time logic can
    /// be driven with predictable identities in tests.
    pub(super) run_ids: Arc<dyn RunIdSource>,
    pub(super) classifier: Arc<VendorErrorClassifier>,
    /// Signals the accept loop to end the process; used by daemon-side
    /// exits such as the project-root liveness check.
    pub(super) shutdown: mpsc::Sender<DaemonControl>,
    pub(super) shutdown_flag: Arc<AtomicBool>,
}

/// The live credential for a run's one worker socket: the token a request must
/// present, and what presenting it authorises. The scope is stored here rather
/// than derived per request because it is minted with the token — the daemon
/// decides what a worker may do at the moment it hands over the secret, and
/// nothing the worker sends afterwards can change the answer.
#[derive(Debug, Clone)]
pub(super) struct IssuedWorker {
    pub(super) token: String,
    pub(super) scope: WorkerScope,
}

/// Internal dispatcher events reported by drivers and recovery tasks, never by
/// clients.
pub(super) enum RunEvent {
    /// A driver minted worker credentials for the stage it is about to run.
    /// The dispatcher registers the token *before* serving the socket, so no
    /// worker request can be answered against a token it has not yet issued.
    WorkerReady {
        run_id: String,
        worker: WorkerCredentials,
        listener: tokio::net::UnixListener,
    },
    /// A driver could not open its run's workspace. Nothing was recorded, so
    /// the claim rolls back and the ticket is queued again.
    AdmissionFailed {
        run_id: String,
        ticket_id: String,
        error: String,
    },
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
        /// Where the run's flow walk stopped short, if it did.
        halt: Option<FlowHalt>,
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
    reconcile(&mut state, &events_tx, &log).await;
    loop {
        let deadline = next_dispatch_deadline(&state);
        let clock = state.clock.clone();
        let output_notify = state.output_notify.clone();
        tokio::select! {
            message = requests.recv() => {
                let Some(message) = message else { break };
                match message {
                    DispatcherMessage::Request { id, request, origin, reply } => {
                        let response = match origin {
                            RequestOrigin::Operator => dispatch(&mut state, id, request).await,
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
                settle_run_exit(&mut state, event, &log).await;
            }
            () = wait_for_deadline(clock, deadline) => {
                log.emit(LogLevel::Info, "sloop::dispatcher", "timer_fired");
            }
            () = output_notify.notified() => {}
            // Wall-clock is deliberate: this is a liveness probe, not
            // decision logic, so the manual test clock must not gate it.
            _ = liveness_tick.tick() => {
                if !state.root.join(".git").exists() {
                    log.emit(LogLevel::Error, "sloop::dispatcher", "project_root_missing");
                    let _ = state.shutdown.send(DaemonControl::Stop).await;
                    break;
                }
                reconcile_run_liveness(&mut state, &events_tx, &log).await;
            }
        }
        reconcile(&mut state, &events_tx, &log).await;
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

/// Resolves one finished run: durable outcome evidence lands first, then the
/// work source releases its claim. Cancellation intent recorded before the exit
/// wins over every other reading.
async fn settle_run_exit(state: &mut DispatcherState, event: RunEvent, log: &OperationalLog) {
    let run_id = match event {
        RunEvent::WorkerReady {
            run_id,
            worker,
            listener,
        } => {
            register_worker_socket(state, &run_id, &worker, listener);
            return;
        }
        RunEvent::AdmissionFailed {
            run_id,
            ticket_id,
            error,
        } => {
            super::scheduler::roll_back_admission(state, &run_id, &ticket_id, &error, log).await;
            return;
        }
        RunEvent::Exited { ref run_id, .. } => run_id.clone(),
    };
    state.pending_exits.insert(run_id, event);
    if !state.storage_full.get() {
        settle_pending_exits(state, log).await;
    }
}

/// Issues a stage's worker credentials and then, and only then, starts serving
/// the socket they authenticate against. The ordering is the whole guarantee:
/// a worker's connection waits in the listen backlog until the accept loop
/// exists, and that loop exists only once the token is registered, so a request
/// can never race its own credential.
///
/// A run holds at most one live worker socket, so a new one replaces the
/// previous stage's and that stage's token stops validating.
fn register_worker_socket(
    state: &mut DispatcherState,
    run_id: &str,
    worker: &WorkerCredentials,
    listener: tokio::net::UnixListener,
) {
    state.worker_tokens.insert(
        run_id.to_owned(),
        IssuedWorker {
            token: worker.token.clone(),
            scope: worker.scope.clone(),
        },
    );
    state
        .worker_socket_paths
        .insert(run_id.to_owned(), worker.socket.clone());
    let accept_loop = tokio::spawn(serve_worker_socket(
        listener,
        run_id.to_owned(),
        state.requests_tx.clone(),
        state.log.clone(),
    ));
    if let Some(previous) = state
        .worker_listeners
        .insert(run_id.to_owned(), accept_loop)
    {
        previous.abort();
    }
}

pub(super) async fn settle_pending_exits(state: &mut DispatcherState, log: &OperationalLog) {
    let run_ids: Vec<String> = state.pending_exits.keys().cloned().collect();
    for run_id in run_ids {
        let Some(event) = state.pending_exits.remove(&run_id) else {
            continue;
        };
        match try_settle_run_exit(state, &event, log).await {
            Ok((_ticket_id, outcome, _applied)) => {
                state.cancelling.remove(&run_id);
                state.stalling.remove(&run_id);
                state.active.remove(&run_id);
                state.supervised.remove(&run_id);
                state.suspected_dead.remove(&run_id);
                state.recovering.remove(&run_id);
                state.reported_stalls.remove(&run_id);
                close_worker_socket(state, &run_id);
                log.emit_with_fields(
                    LogLevel::Info,
                    "sloop::dispatcher",
                    "run_exited",
                    json!({"run_id": run_id, "outcome": outcome.as_str()}),
                );
            }
            Err(SettleError::Store(error)) => {
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
            Err(SettleError::WorkState(error)) => {
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::dispatcher",
                    "run_exit_release_failed",
                    json!({"run_id": run_id, "error": error.to_string()}),
                );
                state.pending_exits.insert(run_id, event);
            }
        }
    }
}

enum SettleError {
    Store(StoreError),
    WorkState(SourceError),
}

impl From<StoreError> for SettleError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

async fn try_settle_run_exit(
    state: &mut DispatcherState,
    event: &RunEvent,
    log: &OperationalLog,
) -> Result<(String, crate::outcome::Outcome, bool), SettleError> {
    let RunEvent::Exited {
        run_id,
        target,
        exit_code,
        capture_complete,
        commits,
        commit_observation_complete,
        halt,
        merge,
        vendor_error,
        cooldown_until_ms,
        recovery,
    } = event
    else {
        unreachable!("only settlement events are queued as pending exits")
    };

    let cancelled =
        state.cancelling.contains(run_id) || state.run_store.cancellation_requested(run_id)?;
    let stalled =
        state.stalling.contains(run_id) || state.run_store.output_stall(run_id)?.is_some();
    let evidence = RunEvidence {
        cancelled,
        stalled,
        exit: classify_exit(*exit_code),
        vendor_error: vendor_error.as_ref().map(|error| error.class),
        commit_count: commit_observation_complete.then_some(commits.len()),
        halt: *halt,
        merge: *merge,
    };
    let outcome = if *recovery == Some(RecoveryClassification::Orphaned)
        && !cancelled
        && !stalled
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
                    RecoveryClassification::Resumed => "resumed",
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
    let cooldown = vendor_error
        .as_ref()
        .filter(|error| error.class.requires_cooldown() && !cancelled && !stalled)
        .and_then(|error| cooldown_until_ms.map(|until_ms| (error, until_ms)))
        .map(|(error, until_ms)| CooldownUpdate {
            target,
            until_ms,
            reason: &error.diagnostic,
        });
    let (recorded, applied) = state.run_store.settle(
        run_id,
        *exit_code,
        outcome,
        &records,
        cooldown.as_ref(),
        state.clock.now_ms(),
    )?;
    let ticket_id = recorded.work.ticket_id.clone();
    let ticket =
        state
            .local_work_state
            .ticket(&ticket_id)?
            .ok_or_else(|| StoreError::TicketNotFound {
                ticket_id: ticket_id.clone(),
            })?;
    let ticket_ref = TicketRef {
        id: ticket.id,
        source: ticket.source,
        source_ref: ticket.source_ref,
    };
    state
        .work_state
        .release(
            &ticket_ref,
            &recorded.work.owner,
            disposition_for_outcome(recorded.work.verdict, recorded.not_before_ms),
        )
        .await
        .map_err(SettleError::WorkState)?;
    push_work_outcome(state, recorded.work.clone(), log, "sloop::dispatcher");
    Ok((ticket_id, recorded.work.verdict, applied))
}

pub(super) fn push_work_outcome(
    state: &DispatcherState,
    outcome: WorkOutcome,
    log: &OperationalLog,
    target: &'static str,
) {
    let work_state = state.work_state.clone();
    let log = log.clone();
    let runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        if let Err(error) = runtime.block_on(work_state.push_outcome(&outcome)) {
            log.emit_with_fields(
                LogLevel::Warn,
                target,
                "work_outcome_push_failed",
                json!({
                    "run_id": outcome.owner.0,
                    "ticket_id": outcome.ticket_id,
                    "error": error.to_string(),
                }),
            );
        }
    });
}

pub(super) fn disposition_for_outcome(
    outcome: crate::outcome::Outcome,
    not_before_ms: Option<i64>,
) -> Disposition {
    match outcome {
        crate::outcome::Outcome::Merged => Disposition::Complete,
        crate::outcome::Outcome::Failed => Disposition::Abandon,
        crate::outcome::Outcome::NeedsReview => Disposition::Park {
            reason: "needs-review".into(),
        },
        crate::outcome::Outcome::Cancelled | crate::outcome::Outcome::Orphaned => {
            Disposition::Retry {
                not_before_ms: None,
            }
        }
        crate::outcome::Outcome::RateLimited => Disposition::Retry { not_before_ms },
    }
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
    match state.run_store.probe_writable(now_ms) {
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

async fn dispatch(
    state: &mut DispatcherState,
    id: RequestId,
    request: Request,
) -> ResponseEnvelope {
    let data = match request {
        Request::Show(args) => match handle_operator_show(state, &args) {
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
                .run_store
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
            if !state.work_state_author_enabled {
                return ResponseEnvelope::failure(
                    Some(id),
                    conflict("the configured ticket source does not support authoring"),
                );
            }
            let now_ms = state.clock.now_ms();
            let at_eligible_ms = match &args.trigger {
                crate::protocol::PostTrigger::At { time } => {
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
                &state.local_work_state,
                &args,
                now_ms,
                at_eligible_ms,
                &state.ticket_prefix,
                state.agent.as_ref(),
                &state.flows,
                &state.default_flow,
            )
            .await
            {
                Ok(data) => data,
                Err(error) => {
                    if let crate::post::PostError::Store(store_error) = &error {
                        mark_storage_full(state, store_error);
                    } else if matches!(
                        &error,
                        crate::post::PostError::Source(
                            crate::work_state::SourceError::Unavailable { .. }
                        )
                    ) && !state.storage_full.replace(true)
                    {
                        state.log.emit_with_fields(
                            LogLevel::Error,
                            "sloop::dispatcher",
                            "storage_full",
                            json!({"error": error.to_string()}),
                        );
                    }
                    return ResponseEnvelope::failure(Some(id), post_error_body(&error));
                }
            }
        }
        Request::List(args) => match handle_list(state, &args) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Status(_) => match handle_status(state) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Pause(_) => {
            if let Err(error) = state.run_store.set_paused(true, state.clock.now_ms()) {
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
            let cancelled_restart = match state.run_store.resume_scheduler(state.clock.now_ms()) {
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
        | PostError::InvalidTicketFields { .. }
        | PostError::InvalidWorktreeStem { .. }
        | PostError::UnknownTarget(_)
        | PostError::MissingTargetValue { .. } => ErrorCode::InvalidArguments,
        PostError::ProjectConflict { .. }
        | PostError::FlowConflict { .. }
        | PostError::TicketIdTaken { .. }
        | PostError::DependencyCycle(_)
        | PostError::Source(crate::work_state::SourceError::Rejected { .. }) => ErrorCode::Conflict,
        PostError::Io { .. }
        | PostError::Source(_)
        | PostError::Store(_)
        | PostError::IdAllocation(_) => ErrorCode::Internal,
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
