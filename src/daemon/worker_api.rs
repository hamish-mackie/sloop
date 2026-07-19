use std::fs;

use serde_json::json;

use crate::domain::ticket::TicketSnapshot;
use crate::protocol::{ErrorBody, Request, RequestId, ResponseEnvelope};
use crate::vendor_error::VendorErrorMatch;

use super::commands::lookup;
use super::dispatcher::{DispatcherState, internal, mark_storage_full, unauthorized};

/// Serves a worker verb after proving the caller holds the run's token.
/// Everything an agent can reach flows through here: `brief` and `show` are
/// scoped reads, `note` is the only write and moves nothing.
pub(super) fn dispatch_worker(
    state: &mut DispatcherState,
    id: RequestId,
    request: Request,
    run_id: &str,
    token: Option<&str>,
) -> ResponseEnvelope {
    let valid = token.is_some_and(|presented| {
        state
            .worker_tokens
            .get(run_id)
            .is_some_and(|issued| issued == presented)
    });
    if !valid {
        return ResponseEnvelope::failure(
            Some(id),
            unauthorized("the presented token is not valid for this run"),
        );
    }

    let data = match request {
        Request::Brief(_) => handle_brief(state, run_id),
        Request::Show(args) => handle_show(state, run_id, &args.reference),
        Request::Note(args) => handle_note(state, run_id, &args.text),
        // The connection handler already rejected operator verbs.
        _ => Err(unauthorized(
            "operator verbs are not available on a worker socket",
        )),
    };
    match data {
        Ok(data) => ResponseEnvelope::success(Some(id), data),
        Err(error) => ResponseEnvelope::failure(Some(id), error),
    }
}

/// Everything the agent needs to work, re-readable after a compaction: the
/// ticket body from its committed file, the isolated workspace, and the
/// evidence-based definition of done.
fn handle_brief(state: &DispatcherState, run_id: &str) -> Result<serde_json::Value, ErrorBody> {
    let run = lookup(state, |store| store.run(run_id))?
        .ok_or_else(|| internal("the run for this token no longer exists"))?;
    let ticket = match run.ticket_json.as_deref() {
        Some(snapshot) => serde_json::from_str::<TicketSnapshot>(snapshot)
            .map_err(|error| internal(&format!("the run's ticket snapshot is invalid: {error}")))?,
        None => {
            let ticket = lookup(state, |store| store.ticket(&run.ticket_id))?
                .ok_or_else(|| internal("the ticket for this run no longer exists"))?;
            let body = ticket
                .file_path
                .as_ref()
                .and_then(|file_path| fs::read_to_string(state.root.join(file_path)).ok())
                .unwrap_or_default();
            TicketSnapshot {
                id: ticket.id,
                name: ticket.name,
                blocked_by: ticket.blocked_by,
                worktree: ticket.worktree,
                target: ticket.target,
                model: ticket.model,
                effort: ticket.effort,
                body,
            }
        }
    };

    let mut definition_of_done = vec!["Commit your work to the run branch".to_owned()];
    if state.aftercare_test_cmd.is_some() {
        definition_of_done.push("The configured test command passes".to_owned());
    }

    Ok(json!({
        "run": run_id,
        "ticket": {
            "id": ticket.id,
            "name": ticket.name,
            "blocked_by": ticket.blocked_by,
            "worktree": ticket.worktree,
            "body": ticket.body,
            "acceptance": [],
            "target": ticket.target,
            "model": ticket.model,
            "effort": ticket.effort,
        },
        "worktree": run.worktree_path,
        "branch": run.branch,
        "definition_of_done": definition_of_done,
    }))
}

/// Read-only lookup, scoped to the run's own ticket. Whether a foreign
/// reference exists is not the worker's to learn: everything else is
/// uniformly unauthorized.
fn handle_show(
    state: &DispatcherState,
    run_id: &str,
    reference: &str,
) -> Result<serde_json::Value, ErrorBody> {
    let run = lookup(state, |store| store.run(run_id))?
        .ok_or_else(|| internal("the run for this token no longer exists"))?;
    if reference != run.ticket_id {
        return Err(unauthorized("workers may only show their own run's ticket"));
    }
    let ticket = lookup(state, |store| store.ticket(&run.ticket_id))?
        .ok_or_else(|| internal("the ticket for this run no longer exists"))?;
    let vendor_error = current_ticket_vendor_error(state, &ticket)?;
    Ok(ticket_show(reference, &ticket, vendor_error.as_ref()))
}

pub(super) fn ticket_show(
    reference: &str,
    ticket: &crate::store::TicketRecord,
    vendor_error: Option<&VendorErrorMatch>,
) -> serde_json::Value {
    json!({
        "ref": reference,
        "kind": "ticket",
        "value": {
            "id": ticket.id,
            "project": ticket.project_id,
            "state": ticket.state,
            "file": ticket.file_path,
            "name": ticket.name,
            "blocked_by": ticket.blocked_by,
            "worktree": ticket.worktree,
            "target": ticket.target,
            "model": ticket.model,
            "effort": ticket.effort,
            "reason": vendor_error.map(|error| error.diagnostic.as_str()),
            "classification": vendor_error,
        },
    })
}

pub(super) fn current_ticket_vendor_error(
    state: &DispatcherState,
    ticket: &crate::store::TicketRecord,
) -> Result<Option<VendorErrorMatch>, ErrorBody> {
    let vendor_error = lookup(state, |store| {
        store.latest_vendor_error_for_ticket(&ticket.id)
    })?;
    if ticket.state != "ready" {
        return Ok(vendor_error);
    }
    let cooldown_active = match ticket.target.as_deref() {
        Some(target) => lookup(state, |store| {
            store.active_cooldown_for_target(target, state.clock.now_ms())
        })?
        .is_some(),
        None => false,
    };
    Ok(vendor_error.filter(|error| error.class.requires_cooldown() && cooldown_active))
}

/// The agent's only write: an advisory note recorded against its run. It
/// transitions nothing.
fn handle_note(
    state: &DispatcherState,
    run_id: &str,
    text: &str,
) -> Result<serde_json::Value, ErrorBody> {
    let ordinal = lookup(state, |store| store.next_note_ordinal())?;
    let note_id = format!("N{ordinal}");
    state
        .store
        .insert_note(&note_id, run_id, text, state.clock.now_ms())
        .map_err(|error| {
            mark_storage_full(state, &error);
            internal(&format!("cannot record note: {error}"))
        })?;
    Ok(json!({"note": {"id": note_id, "run": run_id, "text": text}}))
}
