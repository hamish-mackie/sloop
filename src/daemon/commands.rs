use std::collections::HashMap;
use std::path::Path;
use std::process::Command;

use serde_json::json;

use crate::clock::{format_timestamp, next_local_minute_ms};
use crate::config::parse_local_time;
use crate::logging::LogLevel;
use crate::protocol::ErrorBody;
use crate::store::{ActivationKind, NewActivation, Store, StoreError, TicketState};

use super::dispatcher::{
    DispatcherState, LOGS_PAGE_LIMIT, conflict, internal, invalid_arguments, mark_storage_full,
    not_found,
};
use super::recovery::{
    PersistedProcessStop, aftercare_process_identity, process_identity_matches,
    stop_persisted_process_group,
};
use super::runner::run_output_path;
use super::scheduler::{index_projects, running_hours_open};
use super::worker_api::{current_ticket_vendor_error, ticket_show};

pub(super) fn handle_operator_show(
    state: &DispatcherState,
    reference: &str,
) -> Result<serde_json::Value, ErrorBody> {
    if let Some(ticket) = lookup(state, |store| store.ticket(reference))? {
        let vendor_error = current_ticket_vendor_error(state, &ticket)?;
        return Ok(ticket_show(reference, &ticket, vendor_error.as_ref()));
    }
    let project = lookup(state, |store| store.project(reference))?
        .ok_or_else(|| not_found(&format!("reference `{reference}` is not indexed")))?;
    let tickets = lookup(state, |store| store.tickets_for_project(reference))?;
    let mut vendor_errors = HashMap::new();
    for ticket in &tickets {
        if let Some(error) = current_ticket_vendor_error(state, ticket)? {
            vendor_errors.insert(ticket.id.clone(), error);
        }
    }

    let mut notes: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
    for note in lookup(state, |store| store.notes_for_project(reference))? {
        notes.entry(note.ticket_id).or_default().push(json!({
            "id": note.id,
            "run": note.run_id,
            "text": note.text,
            "recorded_at_ms": note.recorded_at_ms,
        }));
    }

    let mut commits: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
    for evidence in lookup(state, |store| store.commit_evidence_for_project(reference))? {
        let data: serde_json::Value = serde_json::from_str(&evidence.data_json)
            .map_err(|error| internal(&format!("cannot decode commit evidence: {error}")))?;
        for oid in data["oids"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or_default()
            .iter()
            .filter_map(serde_json::Value::as_str)
        {
            let (short_hash, message) = git_commit(&state.root, oid)?;
            commits
                .entry(evidence.ticket_id.clone())
                .or_default()
                .push(json!({
                    "run": evidence.run_id.clone(),
                    "hash": short_hash,
                    "message": message,
                }));
        }
    }

    let activity = tickets
        .into_iter()
        .map(|ticket| {
            let ticket_notes = notes.remove(&ticket.id).unwrap_or_default();
            let ticket_commits = commits.remove(&ticket.id).unwrap_or_default();
            let vendor_error = vendor_errors.remove(&ticket.id);
            json!({
                "id": ticket.id,
                "name": ticket.name,
                "state": ticket.state,
                "notes": ticket_notes,
                "commits": ticket_commits,
                "reason": vendor_error.as_ref().map(|error| error.diagnostic.as_str()),
                "classification": vendor_error,
            })
        })
        .collect::<Vec<_>>();

    Ok(json!({
        "ref": reference,
        "kind": "project",
        "value": {
            "id": project.id,
            "title": project.title,
            "file": project.file_path,
            "tickets": activity,
        },
    }))
}

fn git_commit(root: &Path, oid: &str) -> Result<(String, String), ErrorBody> {
    let output = Command::new("git")
        .args(["show", "--no-patch", "--format=%h%x00%s", oid, "--"])
        .current_dir(root)
        .output()
        .map_err(|error| internal(&format!("cannot read commit `{oid}`: {error}")))?;
    if !output.status.success() {
        return Err(internal(&format!(
            "cannot read commit `{oid}`: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let rendered = String::from_utf8_lossy(&output.stdout);
    let (hash, message) = rendered
        .trim_end()
        .split_once('\0')
        .ok_or_else(|| internal(&format!("Git returned malformed data for commit `{oid}`")))?;
    Ok((hash.to_owned(), message.to_owned()))
}

pub(super) fn handle_list(state: &DispatcherState) -> Result<serde_json::Value, ErrorBody> {
    let now_ms = state.clock.now_ms();
    let at_capacity = lookup(state, Store::active_runs)?.len() >= state.max_agents;
    let gates = crate::eligibility::Gates {
        paused: state.paused,
        storage_writable: !state.storage_full.get() && !state.reconciliation_blocked,
        agent_configured: state.agent.is_some(),
        hours_open: running_hours_open(state, now_ms),
        at_capacity,
        has_queued_activation: !lookup(state, Store::queued_activations)?.is_empty(),
    };
    let mut rows = Vec::new();
    for ticket in lookup(state, Store::tickets)? {
        let active_run = lookup(state, |store| store.active_run_for_ticket(&ticket.id))?;
        let blockers = lookup(state, |store| store.unmerged_blockers(&ticket.id))?;
        let mut vendor_error = lookup(state, |store| {
            store.latest_vendor_error_for_ticket(&ticket.id)
        })?;
        let cooldown = match ticket.target.as_deref() {
            Some(target) => lookup(state, |store| {
                store.active_cooldown_for_target(target, now_ms)
            })?,
            None => None,
        };
        if ticket.state == "ready"
            && !vendor_error
                .as_ref()
                .is_some_and(|error| error.class.requires_cooldown() && cooldown.is_some())
        {
            vendor_error = None;
        }
        let ineligibility = crate::eligibility::ticket_ineligibility(
            &ticket.state,
            ticket.attempts,
            active_run.as_deref(),
            &blockers,
            &gates,
        );
        let display_state =
            crate::eligibility::display_state(&ticket.state, ineligibility.as_ref());
        let mut reason = ineligibility.map(|reason| reason.describe());
        if ticket.state == "failed"
            && let Some(error) = &vendor_error
        {
            reason = Some(format!(
                "{}; failed after {} attempt(s); requeue with `sloop retry`",
                error.diagnostic, ticket.attempts
            ));
        } else if ticket.state == "ready"
            && let (Some(target), Some(cooldown)) = (ticket.target.as_deref(), cooldown.as_ref())
        {
            reason = Some(format!(
                "target `{target}` is cooling down until {}: {}",
                format_timestamp(cooldown.until_ms)
                    .unwrap_or_else(|| cooldown.until_ms.to_string()),
                cooldown.reason
            ));
        }
        rows.push(json!({
            "id": ticket.id,
            "name": ticket.name,
            "project": ticket.project_id,
            "state": display_state,
            "run": active_run,
            "reason": reason,
            "classification": vendor_error,
        }));
    }
    Ok(json!({"tickets": rows}))
}

/// Validates a `run` request and persists one queued activation. Acceptance
/// never implies a spawn; reconciliation decides that separately.
pub(super) fn handle_run(
    state: &mut DispatcherState,
    args: &crate::protocol::RunArgs,
) -> Result<serde_json::Value, ErrorBody> {
    use crate::protocol::RunActivation;

    if args.ticket.is_some() && args.project.is_some() {
        return Err(invalid_arguments(
            "a run may target a ticket or a project, not both",
        ));
    }
    if let Some(ticket_id) = &args.ticket {
        let Some(ticket) = lookup(state, |store| store.ticket(ticket_id))? else {
            return Err(not_found(&format!(
                "ticket `{ticket_id}` is not registered"
            )));
        };
        if ticket.state == TicketState::Held.as_str() {
            return Err(conflict(&format!(
                "ticket `{ticket_id}` is held; release it with `sloop ready {ticket_id}`"
            )));
        }
    }
    if let Some(project) = &args.project
        && !lookup(state, |store| store.project_exists(project))?
    {
        return Err(not_found(&format!("project `{project}` is not indexed")));
    }
    for only in &args.only {
        let Some(ticket) = lookup(state, |store| store.ticket(only))? else {
            return Err(not_found(&format!("ticket `{only}` is not registered")));
        };
        if let Some(project) = &args.project
            && &ticket.project_id != project
        {
            return Err(invalid_arguments(&format!(
                "ticket `{only}` belongs to project `{}`, not `{project}`",
                ticket.project_id
            )));
        }
    }

    let now_ms = state.clock.now_ms();
    let (kind, echo_kind, eligible_at_ms, interval_ms) = match &args.activation {
        RunActivation::Now => (ActivationKind::Immediate, "now", None, None),
        RunActivation::At { local_time } => {
            let minute = parse_local_time(local_time).ok_or_else(|| {
                invalid_arguments(&format!("time `{local_time}` must use a valid HH:MM value"))
            })?;
            let eligible_at_ms = next_local_minute_ms(state.clock.as_ref(), now_ms, minute)
                .ok_or_else(|| invalid_arguments("the requested local time is out of range"))?;
            (ActivationKind::At, "at", Some(eligible_at_ms), None)
        }
        RunActivation::Every { interval_ms } => {
            let interval_ms = i64::try_from(*interval_ms)
                .ok()
                .filter(|interval_ms| *interval_ms > 0)
                .ok_or_else(|| invalid_arguments("--every requires a positive interval"))?;
            let eligible_at_ms = now_ms
                .checked_add(interval_ms)
                .ok_or_else(|| invalid_arguments("--every interval is too large"))?;
            (
                ActivationKind::Every,
                "every",
                Some(eligible_at_ms),
                Some(interval_ms),
            )
        }
        RunActivation::Overnight => {
            let eligible_at_ms = state.running_hours.as_ref().map_or(now_ms, |hours| {
                if hours.is_open(state.clock.local_minute(now_ms)) {
                    now_ms
                } else {
                    hours.next_opening_ms(state.clock.as_ref(), now_ms)
                }
            });
            (
                ActivationKind::Overnight,
                "overnight",
                Some(eligible_at_ms),
                None,
            )
        }
    };
    let activation_id = format!(
        "A{}",
        lookup(state, |store| store.next_activation_ordinal())?
    );
    lookup(state, |store| {
        store.insert_activation(
            &NewActivation {
                id: &activation_id,
                kind,
                ticket_id: args.ticket.as_deref(),
                project_id: args.project.as_deref(),
                eligible_at_ms,
                interval_ms,
            },
            now_ms,
        )
    })?;
    for only in &args.only {
        lookup(state, |store| {
            store.insert_activation_filter(&activation_id, only)
        })?;
    }

    let mut activation = json!({
        "id": activation_id,
        "kind": echo_kind,
        "state": "queued",
    });
    if let Some(ticket) = &args.ticket {
        activation["ticket"] = json!(ticket);
    }
    if let Some(project) = &args.project {
        activation["project"] = json!(project);
    }
    if let Some(eligible_at_ms) = eligible_at_ms {
        activation["eligible_at_ms"] = json!(eligible_at_ms);
    }
    match &args.activation {
        RunActivation::At { local_time } => activation["local_time"] = json!(local_time),
        RunActivation::Every { .. } => activation["interval_ms"] = json!(interval_ms),
        RunActivation::Now | RunActivation::Overnight => {}
    }
    Ok(json!({"activation": activation}))
}

pub(super) fn handle_hold(
    state: &mut DispatcherState,
    args: &crate::protocol::TicketReferenceArgs,
) -> Result<serde_json::Value, ErrorBody> {
    let requested = TicketState::Held;
    let previous = state
        .store
        .set_ticket_hold(&args.ticket, requested, state.clock.now_ms())
        .map_err(|error| match error {
            StoreError::TicketNotFound { .. } => not_found(&error.to_string()),
            StoreError::TicketStateConflict { .. } => conflict(&error.to_string()),
            _ => {
                mark_storage_full(state, &error);
                internal(&error.to_string())
            }
        })?;
    Ok(json!({
        "ticket": args.ticket,
        "previous_state": previous,
        "state": requested.as_str(),
        "overridden": previous != requested.as_str(),
    }))
}

pub(super) fn handle_ready(
    state: &mut DispatcherState,
    args: &crate::protocol::TicketReferenceArgs,
) -> Result<serde_json::Value, ErrorBody> {
    let requested = TicketState::Ready;
    let previous = state
        .store
        .set_ticket_hold(&args.ticket, requested, state.clock.now_ms())
        .map_err(|error| match error {
            StoreError::TicketNotFound { .. } => not_found(&error.to_string()),
            StoreError::TicketStateConflict { .. } => conflict(&error.to_string()),
            _ => {
                mark_storage_full(state, &error);
                internal(&error.to_string())
            }
        })?;
    Ok(json!({
        "ticket": args.ticket,
        "previous_state": previous,
        "state": requested.as_str(),
        "overridden": previous != requested.as_str(),
    }))
}

pub(super) fn handle_retry(
    state: &mut DispatcherState,
    args: &crate::protocol::TicketReferenceArgs,
) -> Result<serde_json::Value, ErrorBody> {
    let previous = state
        .store
        .retry_ticket(&args.ticket, state.clock.now_ms())
        .map_err(|error| match error {
            StoreError::TicketNotFound { .. } => not_found(&error.to_string()),
            StoreError::TicketStateConflict { .. } => conflict(&error.to_string()),
            _ => {
                mark_storage_full(state, &error);
                internal(&error.to_string())
            }
        })?;
    Ok(json!({
        "ticket": args.ticket,
        "previous_state": previous,
        "state": TicketState::Ready.as_str(),
    }))
}

/// One non-blocking snapshot of a run's state; the client loops. Launch and
/// recovery closures are terminal alongside ordinary derived outcomes.
pub(super) fn handle_wait(
    state: &DispatcherState,
    args: &crate::protocol::RunReferenceArgs,
) -> Result<serde_json::Value, ErrorBody> {
    let Some(run) = lookup(state, |store| store.run(&args.run))? else {
        return Err(not_found(&format!("run `{}` does not exist", args.run)));
    };
    let terminal = matches!(
        run.state.as_str(),
        "merged"
            | "failed"
            | "needs_review"
            | "cancelled"
            | "rate_limited"
            | "orphaned"
            | "aborted"
    );
    let vendor_error = lookup(state, |store| store.vendor_error_for_run(&run.id))?;
    Ok(json!({
        "run": run.id,
        "state": run.state,
        "terminal": terminal,
        "exit_code": run.exit_code,
        "reason": vendor_error.as_ref().map(|error| error.diagnostic.as_str()),
        "classification": vendor_error,
    }))
}

/// Returns one finite page of captured run output. Records are stored
/// escaped inside the response; raw agent bytes never reach Sloop's stdout.
pub(super) fn handle_logs(
    state: &DispatcherState,
    args: &crate::protocol::RunReferenceArgs,
) -> Result<serde_json::Value, ErrorBody> {
    if lookup(state, |store| store.run(&args.run))?.is_none() {
        return Err(not_found(&format!("run `{}` does not exist", args.run)));
    }
    let page = crate::run_log::read_page(
        &run_output_path(&state.state_dir, &args.run),
        0,
        LOGS_PAGE_LIMIT,
    )
    .map_err(|error| internal(&format!("cannot read run log: {error}")))?;
    let entries = page
        .entries
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| internal(&format!("cannot encode run log: {error}")))?;
    Ok(json!({
        "run": args.run,
        "entries": entries,
        "next_cursor": page.next_cursor,
        "complete": page.complete,
    }))
}

/// Records cancellation intent durably, then kills the run's whole process
/// group. Termination is confirmed by the exit event, which reads the intent
/// and settles the outcome as `Cancelled`; the worktree, branch, and captured
/// logs are preserved as evidence.
pub(super) fn handle_cancel(
    state: &mut DispatcherState,
    args: &crate::protocol::RunReferenceArgs,
) -> Result<serde_json::Value, ErrorBody> {
    let Some(run) = lookup(state, |store| store.run(&args.run))? else {
        return Err(not_found(&format!("run `{}` does not exist", args.run)));
    };
    if !matches!(run.state.as_str(), "running" | "aftercare") || run.exited_at_ms.is_some() {
        return Err(conflict(&format!(
            "run `{}` is `{}` and cannot be cancelled",
            run.id, run.state
        )));
    }

    // Intent must be durable before any signal: if the daemon dies between
    // the kill and the exit event, recovery still reads the cancellation.
    lookup(state, |store| {
        store.record_cancel_requested(&run.id, state.clock.now_ms())
    })?;
    state.cancelling.insert(run.id.clone());

    if run.state == "aftercare" {
        let rows = lookup(state, |store| store.run_evidence(&run.id))?;
        if let Some(identity) =
            aftercare_process_identity(&rows, None).map_err(|error| internal(&error))?
        {
            if identity.group <= 0 {
                return Err(internal(
                    "the active aftercare stage has an invalid process group",
                ));
            }
            match stop_persisted_process_group(&identity) {
                Ok(PersistedProcessStop::LeaderMissing) => state.log.emit_with_fields(
                    LogLevel::Info,
                    "sloop::supervisor",
                    "stale_aftercare_group_not_signalled",
                    json!({"run_id": run.id, "process_group_id": identity.group}),
                ),
                Ok(PersistedProcessStop::StoppedOriginal) => {}
                Err(error) => {
                    state.log.emit_with_fields(
                        LogLevel::Error,
                        "sloop::supervisor",
                        "aftercare_cancel_signal_refused",
                        json!({"run_id": run.id, "error": error}),
                    );
                }
            }
        }
    } else {
        let process_matches = run
            .pid
            .and_then(|pid| u32::try_from(pid).ok())
            .is_some_and(|pid| process_identity_matches(pid, run.pid_start_time));
        if process_matches && let Some(group) = run.process_group_id {
            // A negative PID signals the whole group, so grandchildren die too.
            // ESRCH means the group already exited; the race resolves through
            // the recorded intent.
            unsafe {
                libc::kill(-(group as libc::pid_t), libc::SIGKILL);
            }
        }
    }

    Ok(json!({
        "run": run.id,
        "state": "cancelling",
        "worktree": run.worktree_path,
        "preserved": true,
    }))
}

/// Validates a stop request and, when forced, cancels every active run
/// through the same durable-intent path as `cancel`. The connection handler
/// owns the actual exit so the reply always reaches the caller first.
pub(super) fn handle_stop(
    state: &mut DispatcherState,
    args: &crate::protocol::StopArgs,
) -> Result<serde_json::Value, ErrorBody> {
    let mut active: Vec<String> = state.active.iter().cloned().collect();
    active.sort();
    if !active.is_empty() && !args.force {
        return Err(conflict(&format!(
            "{} active run(s): {}; stop --force cancels them",
            active.len(),
            active.join(", "),
        )));
    }
    let mut cancelled = Vec::new();
    for run_id in active {
        if handle_cancel(
            state,
            &crate::protocol::RunReferenceArgs {
                run: run_id.clone(),
            },
        )
        .is_ok()
        {
            cancelled.push(run_id);
        }
    }
    Ok(json!({
        "stopping": true,
        "pid": state.pid,
        "cancelled_runs": cancelled,
    }))
}

pub(super) fn handle_reindex(state: &mut DispatcherState) -> Result<serde_json::Value, ErrorBody> {
    let mut active: Vec<String> = state.active.iter().cloned().collect();
    active.sort();
    if !active.is_empty() {
        return Err(conflict(&format!(
            "{} active run(s): {}; reindex requires an idle daemon",
            active.len(),
            active.join(", "),
        )));
    }
    let now_ms = state.clock.now_ms();
    let project_ids = index_projects(
        &state.root,
        &state.project_dir,
        &state.store,
        now_ms,
        &state.project_prefix,
    )
    .map_err(|error| internal(&format!("cannot reindex projects: {error}")))?;
    crate::reindex::run(
        &state.root,
        &state.ticket_dir,
        &state.worktree_dir,
        &state.state_dir,
        &state.store,
        now_ms,
        &state.ticket_prefix,
        &project_ids,
        state.agent.as_ref(),
        &state.flows,
        &state.default_flow,
    )
    .map_err(|error| internal(&format!("cannot reindex tickets: {error}")))
}

pub(super) fn lookup<T>(
    state: &DispatcherState,
    query: impl FnOnce(&Store) -> Result<T, StoreError>,
) -> Result<T, ErrorBody> {
    query(&state.store).map_err(|error| {
        mark_storage_full(state, &error);
        internal(&error.to_string())
    })
}
