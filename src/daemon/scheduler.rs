use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde_json::json;
use tokio::sync::mpsc;

use crate::config::expand_agent_cmd;
use crate::coordination::{Claim, Coordination};
use crate::domain::ticket::TicketSnapshot;
use crate::flow::Flow;
use crate::frontmatter::Frontmatter;
use crate::ids::next_id;
use crate::logging::{LogLevel, OperationalLog};
use crate::runner::local::{
    compose_worker_prompt, launch_agent, run_output_path, wait_for_test_hook, worker_socket_path,
};
use crate::runner::{AgentLaunch, RunnerError, StageExecution, StageOrder};
use crate::store::{ClaimRequest, QueuedActivation, Store, TicketRecord};

use super::aftercare::{StoreStageHooks, gather_exit_evidence};
use super::dispatcher::{
    DispatcherState, RunEvent, close_worker_socket, mark_storage_full, recover_storage,
    settle_pending_exits,
};
use super::recovery::{classify_run_output, reconcile_run_liveness};
use super::server::{DaemonError, serve_worker_socket};

pub(super) const DEFAULT_LEASE_MS: i64 = 10 * 60 * 1000;
pub(super) const VENDOR_COOLDOWN_MS: i64 = 5 * 60 * 1000;

fn agent_stage_order(
    state: &DispatcherState,
    ticket: &TicketRecord,
    flow: &Flow,
    run_id: &str,
    attempt: i64,
) -> Result<(StageOrder, String), RunnerError<crate::store::StoreError>> {
    let error = |message| RunnerError::Execution(message);
    let agent = state
        .agent
        .as_ref()
        .ok_or_else(|| error("no agent targets configured".into()))?;
    let target = ticket.target.as_deref().ok_or_else(|| {
        error(format!(
            "ticket `{}` does not specify an agent target",
            ticket.id
        ))
    })?;
    let template = agent.targets.get(target).ok_or_else(|| {
        error(format!(
            "ticket `{}` names unknown agent target `{target}`",
            ticket.id
        ))
    })?;
    let prompt = compose_worker_prompt(&state.root).map_err(error)?;
    let argv = expand_agent_cmd(
        template,
        ticket.model.as_deref(),
        ticket.effort.as_deref(),
        &prompt,
    )
    .map_err(|message| error(format!("ticket `{}` {message}", ticket.id)))?;
    let executable = std::env::current_exe()
        .map_err(|source| error(format!("cannot locate sloop executable: {source}")))?;
    let executable_dir = executable
        .parent()
        .ok_or_else(|| error("sloop executable has no parent directory".into()))?;
    let mut path_entries = vec![executable_dir.to_path_buf()];
    if let Some(path) = std::env::var_os("PATH") {
        path_entries.extend(std::env::split_paths(&path));
    }
    let path = std::env::join_paths(path_entries)
        .map_err(|source| error(format!("cannot construct agent PATH: {source}")))?;
    let branch = format!("sloop/{}-a{attempt}-{run_id}", ticket.id);
    let worktree = state.worktree_dir.join(run_id);
    let stage = flow
        .stages
        .iter()
        .find(|stage| matches!(stage.kind, crate::flow::StageKind::Agent))
        .map_or_else(|| "agent".into(), |stage| stage.name.clone());
    let environment = vec![
        (OsString::from("SLOOP_RUN_ID"), OsString::from(run_id)),
        (
            OsString::from("SLOOP_TICKET_ID"),
            OsString::from(&ticket.id),
        ),
        (
            OsString::from("SLOOP_BIN"),
            executable.as_os_str().to_owned(),
        ),
        (OsString::from("PATH"), path),
    ];
    Ok((
        StageOrder {
            run_id: run_id.into(),
            stage,
            execution: StageExecution::Agent(AgentLaunch {
                argv,
                environment,
                repository: state.root.clone(),
                worker_socket_path: worker_socket_path(&state.runtime_dir, run_id),
            }),
            worktree,
            branch,
            output_path: run_output_path(&state.state_dir, run_id),
        },
        target.into(),
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OrphanDisposition {
    MarkMissing,
    Delete,
    Keep,
}

/// Policy for a DB ticket whose file has disappeared: stamp first, delete
/// once the stamp outlives the grace window, and never delete a row
/// something else still references — such rows just stay stamped.
fn orphan_disposition(
    missing_since_ms: Option<i64>,
    is_referenced: bool,
    now_ms: i64,
    delete_missing_after_ms: i64,
) -> OrphanDisposition {
    match missing_since_ms {
        None => OrphanDisposition::MarkMissing,
        Some(since) if now_ms - since >= delete_missing_after_ms && !is_referenced => {
            OrphanDisposition::Delete
        }
        Some(_) => OrphanDisposition::Keep,
    }
}

/// Reconciles local ticket rows against their committed files. Runs at
/// startup; `reindex` will share it.
pub(super) fn reconcile_tickets(
    root: &Path,
    store: &Store,
    now_ms: i64,
    delete_missing_after_ms: i64,
) -> Result<(), DaemonError> {
    for ticket in store.local_ticket_files().map_err(DaemonError::Store)? {
        if root.join(&ticket.file_path).is_file() {
            if ticket.missing_at_ms.is_some() {
                store
                    .clear_ticket_missing(&ticket.id, now_ms)
                    .map_err(DaemonError::Store)?;
            }
            continue;
        }
        let is_referenced = store
            .ticket_is_referenced(&ticket.id)
            .map_err(DaemonError::Store)?;
        match orphan_disposition(
            ticket.missing_at_ms,
            is_referenced,
            now_ms,
            delete_missing_after_ms,
        ) {
            OrphanDisposition::MarkMissing => {
                store
                    .mark_ticket_missing(&ticket.id, now_ms)
                    .map_err(DaemonError::Store)?;
            }
            OrphanDisposition::Delete => {
                store
                    .delete_ticket(&ticket.id)
                    .map_err(DaemonError::Store)?;
            }
            OrphanDisposition::Keep => {}
        }
    }
    Ok(())
}

/// Indexes committed project Markdown files into the store so ticket membership
/// can be validated. Runs at startup; `reindex` will share it.
pub(super) fn index_projects(
    root: &Path,
    project_dir: &Path,
    store: &Store,
    now_ms: i64,
    project_prefix: &str,
) -> Result<Vec<String>, DaemonError> {
    let directory = root.join(project_dir);
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(DaemonError::Io {
                path: directory,
                source,
            });
        }
    };

    let mut paths = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|source| DaemonError::Io {
                path: directory.clone(),
                source,
            })?
            .path();
        if path.extension().and_then(|extension| extension.to_str()) == Some("md") {
            paths.push(path);
        }
    }
    paths.sort();

    struct ProjectFile {
        path: PathBuf,
        content: String,
        stem: String,
        frontmatter: Frontmatter,
    }

    let mut projects = Vec::new();
    for path in paths {
        let content = fs::read_to_string(&path).map_err(|source| DaemonError::Io {
            path: path.clone(),
            source,
        })?;
        let stem = path
            .file_stem()
            .map(|stem| stem.to_string_lossy().into_owned())
            .unwrap_or_default();
        // A malformed project file must not keep the daemon from starting;
        // it is simply not indexed until fixed.
        let Ok(frontmatter) = crate::frontmatter::parse(&content) else {
            continue;
        };
        projects.push(ProjectFile {
            path,
            content,
            stem,
            frontmatter,
        });
    }

    // Explicit IDs in every file establish the high-water mark before sorted
    // idless files are assigned, regardless of where those explicit files sort.
    let mut ids: Vec<String> = projects
        .iter()
        .filter_map(|project| project.frontmatter.id.clone())
        .collect();
    let mut indexed = Vec::with_capacity(projects.len());
    for project in projects {
        let id = match project.frontmatter.id {
            Some(id) => id,
            None => {
                let id = next_id(project_prefix, ids.iter().map(String::as_str))?;
                let updated =
                    crate::frontmatter::stamp_id(&project.content, &id).map_err(|error| {
                        DaemonError::Frontmatter {
                            path: project.path.clone(),
                            error,
                        }
                    })?;
                fs::write(
                    &project.path,
                    updated.expect("idless project always needs an ID stamp"),
                )
                .map_err(|source| DaemonError::Io {
                    path: project.path.clone(),
                    source,
                })?;
                ids.push(id.clone());
                id
            }
        };
        let title = project.frontmatter.title.unwrap_or(project.stem);
        let relative = project
            .path
            .strip_prefix(root)
            .unwrap_or(&project.path)
            .to_string_lossy()
            .into_owned();
        store
            .upsert_local_project(&id, &relative, &title, now_ms)
            .map_err(DaemonError::Store)?;
        indexed.push(id);
    }
    Ok(indexed)
}

/// The single spawn decision point: every queued activation passes the same
/// pause and capacity gates, selects deterministically, claims conditionally,
/// and only then touches Git and processes.
pub(super) fn reconcile(
    state: &mut DispatcherState,
    events: &mpsc::Sender<RunEvent>,
    log: &OperationalLog,
) {
    let now_ms = state.clock.now_ms();
    if !recover_storage(state, now_ms) {
        return;
    }
    settle_pending_exits(state, log);
    if state.storage_full.get()
        || state.reconciliation_blocked
        || state.paused
        || state.agent.is_none()
        || !running_hours_open(state, now_ms)
    {
        return;
    }
    let activations = match state.store.dispatchable_activations(now_ms) {
        Ok(activations) => activations,
        Err(error) => {
            log.emit_with_fields(
                LogLevel::Error,
                "sloop::dispatcher",
                "activation_scan_failed",
                json!({"error": error.to_string()}),
            );
            return;
        }
    };

    // A durable lease missing from memory must consume capacity before the
    // dispatcher can use an apparently free slot. The periodic pass remains
    // responsible for idle reconciliation; this extra scan only runs when a
    // queued activation could otherwise spawn now.
    if !activations.is_empty() && state.active.len() < state.max_agents {
        wait_for_test_hook("before-spawn-capacity-reconciliation");
        reconcile_run_liveness(state, events, log);
        if state.reconciliation_blocked {
            return;
        }
    }

    for activation in activations {
        if state.active.len() >= state.max_agents {
            break;
        }
        let Some(ticket_id) = eligible_ticket(&state.store, &activation, now_ms) else {
            continue;
        };

        let ticket = match state.store.ticket(&ticket_id) {
            Ok(Some(ticket)) => ticket,
            Ok(None) => {
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::dispatcher",
                    "bound_flow_resolution_failed",
                    json!({"ticket_id": ticket_id, "error": format!("ticket `{ticket_id}` no longer exists")}),
                );
                continue;
            }
            Err(error) => {
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::dispatcher",
                    "bound_flow_resolution_failed",
                    json!({"ticket_id": ticket_id, "error": format!("cannot read ticket `{ticket_id}`: {error}")}),
                );
                continue;
            }
        };
        let flow = match bound_flow_for_ticket(&state.flows, &ticket) {
            Ok(flow) => flow,
            Err(error) => {
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::dispatcher",
                    "bound_flow_resolution_failed",
                    json!({"ticket_id": ticket_id, "error": error}),
                );
                continue;
            }
        };
        let body = ticket.body.clone().unwrap_or_else(|| {
            ticket
                .file_path
                .as_ref()
                .and_then(|file_path| fs::read_to_string(state.root.join(file_path)).ok())
                .unwrap_or_default()
        });
        let ticket_snapshot = TicketSnapshot {
            id: ticket.id.clone(),
            name: ticket.name.clone(),
            blocked_by: ticket.blocked_by.clone(),
            worktree: ticket.worktree.clone(),
            target: ticket.target.clone(),
            model: ticket.model.clone(),
            effort: ticket.effort.clone(),
            body,
        };
        let flow_json = serde_json::to_string(&flow).expect("flow snapshots serialize to JSON");
        let ticket_json =
            serde_json::to_string(&ticket_snapshot).expect("ticket snapshots serialize to JSON");

        let now_ms = state.clock.now_ms();
        let run_ordinal = match state.store.next_run_ordinal() {
            Ok(ordinal) => ordinal,
            Err(error) => {
                mark_storage_full(state, &error);
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::dispatcher",
                    "run_id_reservation_failed",
                    json!({"error": error.to_string()}),
                );
                if error.is_disk_full() {
                    break;
                }
                continue;
            }
        };
        let run_id = format!("R{run_ordinal}");
        let owner = format!("daemon-{}", state.pid);
        let claim = ClaimRequest {
            ticket_id: &ticket_id,
            run_id: &run_id,
            activation_id: &activation.id,
            owner_id: &owner,
            lease_ms: DEFAULT_LEASE_MS,
            flow_json: &flow_json,
            ticket_json: &ticket_json,
            next_activation_eligible_at_ms: if activation.kind == "every" {
                match (activation.eligible_at_ms, activation.interval_ms) {
                    (Some(eligible_at_ms), Some(interval_ms)) => {
                        rearm_every_at(eligible_at_ms, interval_ms, now_ms)
                    }
                    _ => None,
                }
            } else {
                None
            },
        };
        if activation.kind == "every" && claim.next_activation_eligible_at_ms.is_none() {
            log.emit_with_fields(
                LogLevel::Error,
                "sloop::dispatcher",
                "invalid_recurring_activation",
                json!({"activation_id": activation.id}),
            );
            continue;
        }
        let claimed = match Coordination::new(&mut state.store).claim(&claim, now_ms) {
            Ok(Claim::Granted(claimed)) => claimed,
            // Not ready right now; the activation stays queued for later.
            Ok(Claim::Denied(_)) => continue,
            Err(error) => {
                mark_storage_full(state, &error);
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::dispatcher",
                    "claim_failed",
                    json!({
                        "activation_id": activation.id,
                        "ticket_id": ticket_id,
                        "run_id": run_id,
                        "error": error.to_string(),
                    }),
                );
                if error.is_disk_full() {
                    break;
                }
                continue;
            }
        };
        let launch = agent_stage_order(state, &ticket, &flow, &run_id, claimed.attempt).and_then(
            |(order, target)| {
                let worktree = order.worktree.clone();
                let branch = order.branch.clone();
                let output_path = order.output_path.clone();
                let hooks = StoreStageHooks::new(&state.store, log);
                launch_agent(order, &hooks, state.clock.as_ref())
                    .map(|launched| (launched, target, worktree, branch, output_path))
            },
        );
        match launch {
            Ok((mut launched, target, worktree, branch, output_path)) => {
                state.active.insert(run_id.clone());
                let events = events.clone();
                let exited_run = run_id.clone();
                let root = state.root.clone();
                let test_cmd = state.aftercare_test_cmd.clone();
                let clock = state.clock.clone();
                let classifier = state.classifier.clone();
                let supervisor_log = log.clone();
                let state_dir = state.state_dir.clone();
                let db_path = state.state_dir.join("sloop.db");
                let worker = launched.worker().clone();
                state
                    .worker_tokens
                    .insert(run_id.clone(), worker.token.clone());
                state
                    .worker_socket_paths
                    .insert(run_id.clone(), worker.socket.clone());
                let accept_loop = tokio::spawn(serve_worker_socket(
                    launched.take_worker_listener(),
                    run_id.clone(),
                    state.requests_tx.clone(),
                    state.log.clone(),
                ));
                state.worker_listeners.insert(run_id.clone(), accept_loop);
                state.supervised.insert(run_id.clone());
                let pid = launched.process().pid;
                tokio::task::spawn_blocking(move || {
                    let completion = launched.wait(clock.as_ref());
                    let exit_code = completion.evidence.exit_code;
                    if let Some(error) = completion.wait_error {
                        supervisor_log.emit_with_fields(
                            LogLevel::Error,
                            "sloop::supervisor",
                            "agent_wait_failed",
                            json!({"run_id": exited_run, "error": error}),
                        );
                    }
                    if completion.evidence.stragglers_killed {
                        supervisor_log.emit_with_fields(
                            LogLevel::Info,
                            "sloop::supervisor",
                            "stragglers_killed",
                            json!({"run_id": exited_run, "process_group_id": pid}),
                        );
                    }
                    let mut capture_complete = completion.evidence.output_capture_complete;
                    let vendor_error = match classify_run_output(
                        &classifier,
                        &state_dir,
                        &exited_run,
                        exit_code,
                    ) {
                        Ok(classification) => classification,
                        Err(error) => {
                            capture_complete = false;
                            supervisor_log.emit_with_fields(
                                LogLevel::Error,
                                "sloop::supervisor",
                                "vendor_error_classification_failed",
                                json!({"run_id": exited_run, "error": error}),
                            );
                            None
                        }
                    };
                    let cooldown_until_ms = vendor_error
                        .as_ref()
                        .filter(|error| error.class.requires_cooldown())
                        .map(|_| clock.now_ms() + VENDOR_COOLDOWN_MS);
                    let mut checkpoint_store = match Store::open(&db_path, clock.now_ms()) {
                        Ok(store) => Some(store),
                        Err(error) => {
                            supervisor_log.emit_with_fields(
                                LogLevel::Error,
                                "sloop::supervisor",
                                "aftercare_checkpoint_open_failed",
                                json!({"run_id": exited_run, "error": error.to_string()}),
                            );
                            None
                        }
                    };
                    let Some((commits, commit_observation_complete, aftercare_failed, merge)) =
                        gather_exit_evidence(
                            &exited_run,
                            &root,
                            &worktree,
                            &branch,
                            &flow,
                            &worker,
                            test_cmd.as_deref(),
                            clock.as_ref(),
                            &output_path,
                            exit_code,
                            capture_complete,
                            vendor_error.as_ref(),
                            cooldown_until_ms,
                            checkpoint_store.as_mut(),
                            &supervisor_log,
                        )
                    else {
                        return;
                    };
                    let _ = events.blocking_send(RunEvent::Exited {
                        run_id: exited_run,
                        target,
                        exit_code,
                        capture_complete,
                        commits,
                        commit_observation_complete,
                        aftercare_failed,
                        merge,
                        vendor_error,
                        cooldown_until_ms,
                        recovery: None,
                    });
                });
                log.emit_with_fields(
                    LogLevel::Info,
                    "sloop::dispatcher",
                    "run_started",
                    json!({"run_id": run_id, "ticket_id": ticket_id, "pid": pid}),
                );
            }
            Err(error) => {
                if let RunnerError::Hook(store_error) = &error {
                    mark_storage_full(state, store_error);
                }
                if let Err(abort_error) = Coordination::new(&mut state.store).abandon(
                    &run_id,
                    &ticket_id,
                    state.clock.now_ms(),
                ) {
                    mark_storage_full(state, &abort_error);
                    log.emit_with_fields(
                        LogLevel::Error,
                        "sloop::dispatcher",
                        "claim_abort_failed",
                        json!({
                            "run_id": run_id,
                            "ticket_id": ticket_id,
                            "error": abort_error.to_string(),
                        }),
                    );
                }
                // A launch can fail after the worker socket was bound.
                close_worker_socket(state, &run_id);
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::dispatcher",
                    "run_launch_failed",
                    json!({
                        "run_id": run_id,
                        "ticket_id": ticket_id,
                        "error": error.to_string(),
                    }),
                );
            }
        }
    }
}

pub(super) fn running_hours_open(state: &DispatcherState, now_ms: i64) -> bool {
    state
        .running_hours
        .as_ref()
        .is_none_or(|hours| hours.is_open(state.clock.local_minute(now_ms)))
}

pub(super) fn next_dispatch_deadline(state: &DispatcherState) -> Option<i64> {
    let now_ms = state.clock.now_ms();
    let cooldown_deadline = state.store.next_active_cooldown(now_ms).ok().flatten();
    let next_eligible = state
        .store
        .next_activation_eligible_at_ms(now_ms)
        .ok()
        .flatten();
    let hours_deadline = 'hours: {
        let Some(hours) = state.running_hours.as_ref() else {
            break 'hours next_eligible;
        };
        if hours.is_open(state.clock.local_minute(now_ms)) {
            break 'hours next_eligible;
        }
        let opening = hours.next_opening_ms(state.clock.as_ref(), now_ms);
        let has_due_demand = state
            .store
            .dispatchable_activations(now_ms)
            .is_ok_and(|activations| !activations.is_empty());
        if has_due_demand || next_eligible.is_some_and(|deadline| deadline <= opening) {
            Some(opening)
        } else {
            next_eligible
        }
    };
    [hours_deadline, cooldown_deadline]
        .into_iter()
        .flatten()
        .min()
}

/// Advances a recurring cadence to its first future slot. Missed slots are
/// skipped deterministically so reopening a dispatch window cannot cause a
/// burst of catch-up runs.
fn rearm_every_at(eligible_at_ms: i64, interval_ms: i64, now_ms: i64) -> Option<i64> {
    if interval_ms <= 0 || eligible_at_ms > now_ms {
        return None;
    }
    let missed = now_ms.checked_sub(eligible_at_ms)?.div_euclid(interval_ms);
    let steps = missed.checked_add(1)?;
    eligible_at_ms.checked_add(interval_ms.checked_mul(steps)?)
}

fn eligible_ticket(store: &Store, activation: &QueuedActivation, now_ms: i64) -> Option<String> {
    match &activation.ticket_id {
        Some(ticket) if store.ticket_is_dispatchable(ticket).unwrap_or(false) => {
            let record = store.ticket(ticket).ok().flatten()?;
            let target = record.target.as_deref()?;
            store
                .active_cooldown_for_target(target, now_ms)
                .ok()
                .flatten()
                .is_none()
                .then(|| ticket.clone())
        }
        Some(_) => None,
        None => store.select_ready_ticket(activation, now_ms).ok().flatten(),
    }
}

pub(super) fn bound_flow(
    store: &Store,
    flows: &BTreeMap<String, Flow>,
    ticket_id: &str,
) -> Result<Flow, String> {
    let ticket = store
        .ticket(ticket_id)
        .map_err(|error| format!("cannot read ticket `{ticket_id}`: {error}"))?
        .ok_or_else(|| format!("ticket `{ticket_id}` no longer exists"))?;
    bound_flow_for_ticket(flows, &ticket)
}

fn bound_flow_for_ticket(
    flows: &BTreeMap<String, Flow>,
    ticket: &TicketRecord,
) -> Result<Flow, String> {
    let flow_name = ticket
        .flow
        .as_ref()
        .ok_or_else(|| format!("ticket `{}` has no bound flow", ticket.id))?;
    flows.get(flow_name).cloned().ok_or_else(|| {
        format!(
            "ticket `{}` names unknown bound flow `{flow_name}`",
            ticket.id
        )
    })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{
        OrphanDisposition::{Delete, Keep, MarkMissing},
        index_projects, orphan_disposition, rearm_every_at, reconcile_tickets,
    };
    use crate::domain::ticket::TicketState;
    use crate::store::Store;

    #[test]
    fn recurring_rearm_preserves_cadence_and_skips_missed_slots() {
        assert_eq!(rearm_every_at(1_000, 500, 1_000), Some(1_500));
        assert_eq!(rearm_every_at(1_000, 500, 2_200), Some(2_500));
        assert_eq!(rearm_every_at(1_000, 0, 1_000), None);
        assert_eq!(rearm_every_at(2_000, 500, 1_000), None);
    }

    #[test]
    fn orphan_disposition_stamps_waits_then_deletes_unreferenced_rows() {
        assert_eq!(orphan_disposition(None, false, 1_000, 100), MarkMissing);
        assert_eq!(orphan_disposition(Some(950), false, 1_000, 100), Keep);
        assert_eq!(orphan_disposition(Some(900), false, 1_000, 100), Delete);
        assert_eq!(orphan_disposition(Some(900), true, 1_000, 100), Keep);
    }

    #[test]
    fn reconcile_stamps_deletes_and_restores_tickets() {
        let root = tempdir().unwrap();
        let tickets = root.path().join(".agents/sloop/tickets");
        fs::create_dir_all(&tickets).unwrap();
        fs::write(tickets.join("present.md"), "# Present\n").unwrap();
        let store = Store::open(&root.path().join("sloop.db"), 1_000).unwrap();
        store
            .insert_local_project(
                "default",
                ".agents/sloop/projects/default.md",
                "Default",
                1_000,
            )
            .unwrap();
        let insert = |id: &str, file: &str, blocked_by: &[String]| {
            store
                .insert_local_ticket(
                    id,
                    "default",
                    &format!(".agents/sloop/tickets/{file}"),
                    id,
                    blocked_by,
                    &format!("sloop/{id}"),
                    None,
                    None,
                    None,
                    "default",
                    TicketState::Ready,
                    1_000,
                )
                .unwrap();
        };
        insert("T1", "present.md", &[]);
        insert("T2", "gone.md", &[]);
        insert("T3", "blocked-gone.md", &[]);
        insert("T4", "dependent.md", &["T3".into()]);
        fs::write(tickets.join("dependent.md"), "# Dependent\n").unwrap();

        let window = 100;
        let stamps = |store: &Store| -> Vec<(String, Option<i64>)> {
            store
                .local_ticket_files()
                .unwrap()
                .into_iter()
                .map(|ticket| (ticket.id, ticket.missing_at_ms))
                .collect()
        };

        // First pass stamps the two tickets whose files are gone.
        reconcile_tickets(root.path(), &store, 2_000, window).unwrap();
        assert_eq!(
            stamps(&store),
            vec![
                ("T1".into(), None),
                ("T2".into(), Some(2_000)),
                ("T3".into(), Some(2_000)),
                ("T4".into(), None),
            ]
        );

        // Within the window nothing is deleted and stamps keep their origin.
        reconcile_tickets(root.path(), &store, 2_050, window).unwrap();
        assert_eq!(stamps(&store)[1], ("T2".into(), Some(2_000)));

        // Past the window the unreferenced orphan is deleted; T3 survives
        // because T4 still names it as a blocker.
        reconcile_tickets(root.path(), &store, 2_100, window).unwrap();
        assert_eq!(
            stamps(&store),
            vec![
                ("T1".into(), None),
                ("T3".into(), Some(2_000)),
                ("T4".into(), None),
            ]
        );

        // The file coming back clears the stamp even after the window.
        fs::write(tickets.join("blocked-gone.md"), "# Returned\n").unwrap();
        reconcile_tickets(root.path(), &store, 3_000, window).unwrap();
        assert_eq!(stamps(&store)[1], ("T3".into(), None));
    }

    #[test]
    fn project_allocation_uses_sorted_paths_after_explicit_high_water_marks() {
        let root = tempdir().unwrap();
        let projects = root.path().join(".agents/sloop/projects");
        fs::create_dir_all(&projects).unwrap();
        fs::write(projects.join("zeta.md"), "# Zeta\n").unwrap();
        fs::write(projects.join("alpha.md"), "# Alpha\n").unwrap();
        fs::write(
            projects.join("middle.md"),
            "---\nid: PROJ-7\ntitle: Explicit\n---\n",
        )
        .unwrap();
        let store = Store::open(&root.path().join("sloop.db"), 1_000).unwrap();

        index_projects(
            root.path(),
            std::path::Path::new(".agents/sloop/projects"),
            &store,
            1_000,
            "PROJ",
        )
        .unwrap();

        assert!(
            fs::read_to_string(projects.join("alpha.md"))
                .unwrap()
                .contains("id: PROJ-8")
        );
        assert!(
            fs::read_to_string(projects.join("zeta.md"))
                .unwrap()
                .contains("id: PROJ-9")
        );
    }
}
