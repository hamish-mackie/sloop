use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::process::Command;

use serde_json::json;

use crate::clock::{format_timestamp, next_local_minute_ms};
use crate::config::parse_local_time;
use crate::domain::ticket::TicketState;
use crate::frontmatter;
use crate::logging::LogLevel;
use crate::protocol::ErrorBody;
use crate::runner::local::{process_identity_matches, run_output_path};
use crate::store::{ActivationKind, NewActivation, Store, StoreError};

use super::dispatcher::{
    DispatcherState, LOGS_PAGE_LIMIT, conflict, internal, invalid_arguments, mark_storage_full,
    not_found,
};
use super::recovery::{
    PersistedProcessStop, aftercare_process_identity, stop_persisted_process_group,
};
use super::scheduler::{index_projects, running_hours_open};
use super::worker_api::{current_ticket_vendor_error, ticket_show};

/// The operator read view for `show`. Resolution is ordered by how specific a
/// reference is: an exact ticket id, then any run reference, then a ticket
/// name, and finally a project id. Every branch is read-only; nothing here
/// transitions state. Workers reach `show` through a separate, run-scoped
/// handler and never gain these resolutions.
///
/// Tickets win over runs so that a bare ticket id still shows the ticket; the
/// shared run resolver is consulted for the alias, prefix, and legacy-id forms.
pub(super) fn handle_operator_show(
    state: &DispatcherState,
    reference: &str,
) -> Result<serde_json::Value, ErrorBody> {
    if let Some(ticket) = lookup(state, |store| store.ticket(reference))? {
        return ticket_detail(state, reference, &ticket);
    }
    if let Some(run) = lookup(state, |store| store.run(reference))? {
        return run_detail(state, reference, &ResolvedRun::only(run));
    }
    if let Some(ticket) = lookup(state, |store| store.ticket_by_name(reference))? {
        return ticket_detail(state, reference, &ticket);
    }
    if let Some((ticket_id, attempt)) = crate::run_ref::parse_alias(reference)
        && let Some(run) = lookup(state, |store| {
            store.run_for_ticket_attempt(ticket_id, attempt)
        })?
    {
        return run_detail(state, reference, &ResolvedRun::only(run));
    }
    if let Some(prefix) = crate::run_ref::as_id_prefix(reference) {
        let mut candidates = lookup(state, |store| store.runs_with_id_prefix(&prefix))?;
        if candidates.len() == 1 {
            let resolved = ResolvedRun::only(candidates.remove(0));
            return run_detail(state, reference, &resolved);
        }
        if candidates.len() > 1 {
            return Err(ambiguous_run_prefix(reference, &candidates));
        }
    }
    let Some(project) = lookup(state, |store| store.project(reference))? else {
        return Err(not_found(&format!(
            "reference `{reference}` is not a known ticket, run, or project; `show` accepts a \
             ticket id or name, a project id, or a run named by {} — run `sloop list` to see \
             tickets",
            crate::run_ref::ACCEPTED_RUN_REFERENCES
        )));
    };
    project_activity(state, reference, &project)
}

/// A ticket's frontmatter summary plus its committed Markdown body. The body is
/// read through the same committed-file path the daemon trusts for `brief` and
/// claim-time snapshots, then stripped of the frontmatter the summary already
/// renders.
fn ticket_detail(
    state: &DispatcherState,
    reference: &str,
    ticket: &crate::store::TicketRecord,
) -> Result<serde_json::Value, ErrorBody> {
    let vendor_error = current_ticket_vendor_error(state, ticket)?;
    let mut detail = ticket_show(reference, ticket, vendor_error.as_ref());
    let body = ticket
        .file_path
        .as_ref()
        .and_then(|file_path| fs::read_to_string(state.root.join(file_path)).ok())
        .map(|content| {
            frontmatter::body(&content)
                .unwrap_or(content.as_str())
                .trim()
                .to_owned()
        })
        .unwrap_or_default();
    detail["value"]["body"] = json!(body);
    Ok(detail)
}

/// A run's identity and settled evidence: which ticket it served, whether it
/// reached a terminal state, its branch and worktree, and the exit summary
/// (code plus any classified vendor error). Mirrors the facts `wait` exposes,
/// framed as a detail view.
fn run_detail(
    state: &DispatcherState,
    reference: &str,
    resolved: &ResolvedRun,
) -> Result<serde_json::Value, ErrorBody> {
    let run = &resolved.run;
    let ticket = lookup(state, |store| store.ticket(&run.ticket_id))?;
    let vendor_error = lookup(state, |store| store.vendor_error_for_run(&run.id))?;
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
    Ok(json!({
        "ref": reference,
        "kind": "run",
        "value": {
            "id": run.id,
            "alias": resolved.alias,
            "note": resolved.note(),
            "ticket": run.ticket_id,
            "ticket_name": ticket.as_ref().map(|ticket| ticket.name.as_str()),
            "state": run.state,
            "terminal": terminal,
            "branch": run.branch,
            "worktree": run.worktree_path,
            "exit_code": run.exit_code,
            "reason": vendor_error.as_ref().map(|error| error.diagnostic.as_str()),
            "classification": vendor_error,
        },
    }))
}

/// A project's tickets with their runtime activity: recent notes and the Git
/// commits observed against each run. Rendered from state and Git only; no
/// generated activity is written back into committed source.
fn project_activity(
    state: &DispatcherState,
    reference: &str,
    project: &crate::store::ProjectRecord,
) -> Result<serde_json::Value, ErrorBody> {
    let tickets = lookup(state, |store| store.tickets_for_project(reference))?;
    let mut vendor_errors = HashMap::new();
    for ticket in &tickets {
        if let Some(error) = current_ticket_vendor_error(state, ticket)? {
            vendor_errors.insert(ticket.id.clone(), error);
        }
    }

    // Notes and commits are attributed to a run in output a human reads, so
    // they are attributed by alias. One lookup per ticket covers every run the
    // project's activity can mention.
    let mut aliases: HashMap<String, String> = HashMap::new();
    for ticket in &tickets {
        for run in lookup(state, |store| store.runs_for_ticket(&ticket.id))? {
            aliases.insert(run.id, crate::run_ref::alias(&run.ticket_id, run.attempt));
        }
    }
    let alias_of = |run_id: &str| {
        aliases
            .get(run_id)
            .cloned()
            .unwrap_or_else(|| crate::run_ref::short(run_id).to_owned())
    };

    let mut notes: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
    for note in lookup(state, |store| store.notes_for_project(reference))? {
        notes.entry(note.ticket_id).or_default().push(json!({
            "id": note.id,
            "run": alias_of(&note.run_id),
            "run_id": note.run_id,
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
                    "run": alias_of(&evidence.run_id),
                    "run_id": evidence.run_id.clone(),
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
        draining: state.draining,
        storage_writable: !state.storage_full.get() && !state.reconciliation_blocked,
        agent_configured: state.agent.is_some(),
        hours_open: running_hours_open(state, now_ms),
        at_capacity,
        has_queued_activation: !lookup(state, Store::queued_activations)?.is_empty(),
    };
    let mut rows = Vec::new();
    for ticket in lookup(state, Store::tickets)? {
        let active_run = lookup(state, |store| store.active_run_for_ticket(&ticket.id))?;
        // Every ineligibility reason and list row names the run by alias; the
        // internal id rides alongside for machine consumers.
        let active_alias = active_run
            .as_ref()
            .map(|(_, attempt)| crate::run_ref::alias(&ticket.id, *attempt));
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
            active_alias.as_deref(),
            &blockers,
            &gates,
        );
        let display_state =
            crate::eligibility::display_state(&ticket.state, ineligibility.as_ref());
        let mut reason = ineligibility.map(|reason| reason.describe());
        if ticket.state == "held"
            && let Some(held_reason) = ticket.held_reason
        {
            reason = Some(held_reason);
        }
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
            "run": active_alias,
            "run_id": active_run.map(|(id, _)| id),
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
                "ticket `{ticket_id}` is not registered; run `sloop list` to see registered ticket ids"
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
            return Err(not_found(&format!(
                "ticket `{only}` is not registered; run `sloop list` to see registered ticket ids"
            )));
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
    let resolved = resolve_run(state, &args.run)?;
    let run = &resolved.run;
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
        "id": run.id,
        "alias": resolved.alias,
        "note": resolved.note(),
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
    let resolved = resolve_run(state, &args.run)?;
    // The log lives under the resolved run's own id, never under whatever
    // shorthand the caller happened to type.
    let page = crate::run_log::read_page(
        &run_output_path(&state.state_dir, &resolved.run.id),
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
        "id": resolved.run.id,
        "alias": resolved.alias,
        "note": resolved.note(),
        "entries": entries,
        "next_cursor": page.next_cursor,
        "complete": page.complete,
    }))
}

/// One page of the activity feed. Reads are cursor-based and stateless, so a
/// watcher streams by polling with the cursor from the previous response and
/// the daemon keeps no per-client state.
pub(super) fn handle_events(
    state: &DispatcherState,
    args: &crate::protocol::EventsArgs,
) -> Result<serde_json::Value, ErrorBody> {
    const DEFAULT_LIMIT: u32 = 64;
    const MAX_LIMIT: u32 = 256;
    let limit = args.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT) as usize;
    let latest = lookup(state, |store| store.latest_event_sequence())?;
    let after = match (args.after, args.tail) {
        (Some(after), _) => after,
        (None, Some(tail)) => latest.saturating_sub(i64::from(tail)),
        (None, None) => 0,
    };
    let events = lookup(state, |store| store.events_after(after, limit))?;
    let next_cursor = events.last().map_or(after.max(0), |event| event.sequence);
    let events = events
        .iter()
        .map(|event| {
            json!({
                "sequence": event.sequence,
                "occurred_at_ms": event.occurred_at_ms,
                "kind": event.kind,
                "run": event.run_id,
                "ticket": event.ticket_id,
                "data": serde_json::from_str::<serde_json::Value>(&event.data_json)
                    .unwrap_or_else(|_| json!({})),
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "events": events,
        "next_cursor": next_cursor,
        "latest": latest,
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
    let resolved = resolve_run(state, &args.run)?;
    let run = resolved.run.clone();
    if !matches!(run.state.as_str(), "running" | "aftercare") || run.exited_at_ms.is_some() {
        return Err(conflict(&format!(
            "run `{}` is `{}` and cannot be cancelled",
            resolved.alias, run.state
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
        "id": run.id,
        "alias": resolved.alias,
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
    let active = active_run_aliases(state)?;
    if !active.is_empty() && !args.force {
        return Err(conflict(&format!(
            "{} active run(s): {}; stop --force cancels them",
            active.len(),
            aliases_of(&active).join(", "),
        )));
    }
    let mut cancelled = Vec::new();
    for (run_id, alias) in active {
        if handle_cancel(state, &crate::protocol::RunReferenceArgs { run: run_id }).is_ok() {
            cancelled.push(alias);
        }
    }
    Ok(json!({
        "stopping": true,
        "pid": state.pid,
        "cancelled_runs": cancelled,
    }))
}

/// The daemon's live runs as `(internal id, alias)`, alias-ordered. Messages
/// name runs by alias; the id stays alongside for the verbs that act on one.
fn active_run_aliases(state: &DispatcherState) -> Result<Vec<(String, String)>, ErrorBody> {
    let mut active = Vec::new();
    for run_id in &state.active {
        let alias = lookup(state, |store| store.run(run_id))?
            .map(|run| crate::run_ref::alias(&run.ticket_id, run.attempt))
            .unwrap_or_else(|| crate::run_ref::short(run_id).to_owned());
        active.push((run_id.clone(), alias));
    }
    active.sort_by(|left, right| left.1.cmp(&right.1));
    Ok(active)
}

fn aliases_of(active: &[(String, String)]) -> Vec<&str> {
    active.iter().map(|(_, alias)| alias.as_str()).collect()
}

pub(super) fn handle_reindex(state: &mut DispatcherState) -> Result<serde_json::Value, ErrorBody> {
    let active = active_run_aliases(state)?;
    if !active.is_empty() {
        return Err(conflict(&format!(
            "{} active run(s): {}; reindex requires an idle daemon — wait for them to finish or cancel with `sloop cancel <run>`",
            active.len(),
            aliases_of(&active).join(", "),
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
        state.ticket_source.as_ref(),
        &state.worktree_dir,
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

/// A run named by a reference, together with the alias every human-facing
/// surface shows it by. `earlier_attempts` is populated only when a bare ticket
/// reference selected the latest of several runs, so the caller can say which
/// attempts it passed over.
pub(super) struct ResolvedRun {
    pub(super) run: crate::store::RunRecord,
    pub(super) alias: String,
    pub(super) earlier_attempts: Vec<i64>,
}

impl ResolvedRun {
    fn only(run: crate::store::RunRecord) -> Self {
        Self {
            alias: crate::run_ref::alias(&run.ticket_id, run.attempt),
            run,
            earlier_attempts: Vec::new(),
        }
    }

    /// The `showing TICK-16-r3; earlier attempts: r1, r2` note, or nothing when
    /// the reference named the only run there was.
    pub(super) fn note(&self) -> Option<String> {
        if self.earlier_attempts.is_empty() {
            return None;
        }
        let attempts = self
            .earlier_attempts
            .iter()
            .map(|attempt| format!("r{attempt}"))
            .collect::<Vec<_>>()
            .join(", ");
        Some(format!(
            "showing {}; earlier attempts: {attempts}",
            self.alias
        ))
    }
}

/// The single resolution used by every verb that takes a run reference.
///
/// Ordering is by specificity, and exact-id first is what keeps legacy `R<n>`
/// ids working without a compatibility branch of their own. A bare ticket is
/// the most forgiving form, so it comes before the prefix search that could
/// otherwise claim a short hexadecimal ticket name.
pub(super) fn resolve_run(
    state: &DispatcherState,
    reference: &str,
) -> Result<ResolvedRun, ErrorBody> {
    if let Some(run) = lookup(state, |store| store.run(reference))? {
        return Ok(ResolvedRun::only(run));
    }
    if let Some((ticket_id, attempt)) = crate::run_ref::parse_alias(reference)
        && let Some(run) = lookup(state, |store| {
            store.run_for_ticket_attempt(ticket_id, attempt)
        })?
    {
        return Ok(ResolvedRun::only(run));
    }
    if let Some(ticket_id) = ticket_id_for(state, reference)? {
        let mut runs = lookup(state, |store| store.runs_for_ticket(&ticket_id))?;
        if runs.is_empty() {
            return Err(not_found(&format!(
                "ticket `{ticket_id}` has no runs yet; start one with `sloop run {ticket_id}`"
            )));
        }
        let latest = runs.remove(0);
        let mut earlier_attempts: Vec<i64> = runs.iter().map(|run| run.attempt).collect();
        earlier_attempts.sort_unstable();
        return Ok(ResolvedRun {
            earlier_attempts,
            ..ResolvedRun::only(latest)
        });
    }
    if let Some(prefix) = crate::run_ref::as_id_prefix(reference) {
        let mut candidates = lookup(state, |store| store.runs_with_id_prefix(&prefix))?;
        if candidates.len() == 1 {
            return Ok(ResolvedRun::only(candidates.remove(0)));
        }
        if candidates.len() > 1 {
            return Err(ambiguous_run_prefix(reference, &candidates));
        }
    }
    Err(run_not_found(reference))
}

fn ticket_id_for(state: &DispatcherState, reference: &str) -> Result<Option<String>, ErrorBody> {
    if let Some(ticket) = lookup(state, |store| store.ticket(reference))? {
        return Ok(Some(ticket.id));
    }
    Ok(lookup(state, |store| store.ticket_by_name(reference))?.map(|ticket| ticket.id))
}

/// Git's ambiguous-object error, in Sloop's terms: name the candidates so the
/// operator can retype one character more rather than guess.
fn ambiguous_run_prefix(reference: &str, candidates: &[crate::store::RunRecord]) -> ErrorBody {
    let listed = candidates
        .iter()
        .map(|run| {
            format!(
                "\n  {} {}",
                crate::run_ref::short(&run.id),
                crate::run_ref::alias(&run.ticket_id, run.attempt)
            )
        })
        .collect::<String>();
    invalid_arguments(&format!(
        "run reference `{reference}` is ambiguous; it matches {} runs:{listed}\nuse more \
         characters of a run id, or name a run by its alias",
        candidates.len()
    ))
}

/// The `logs`, `wait`, and `cancel` verbs all address a run by reference; a
/// dead end here names every accepted form so the caller has a next move.
fn run_not_found(run: &str) -> ErrorBody {
    not_found(&format!(
        "run `{run}` does not exist; pass {} — run `sloop list` to see each ticket's runs",
        crate::run_ref::ACCEPTED_RUN_REFERENCES
    ))
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
