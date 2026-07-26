use std::fs;

use serde_json::json;

use crate::domain::ticket::TicketSnapshot;
use crate::flow::{Check, Confidence, Flow};
use crate::protocol::{ErrorBody, Request, RequestId, ResponseEnvelope, VerdictArgs, VerdictValue};
use crate::run_store::{PanelReportRecord, RunRecord};
use crate::runner::WorkerScope;
use crate::vendor_error::VendorErrorMatch;
use crate::worker::{WorkerRole, check_label, definition_of_done};

use crate::work_state::local::TicketRecord;

use super::commands::{local_lookup, mark_storage_full, run_lookup};
use super::dispatcher::{DispatcherState, conflict, internal, invalid_arguments, unauthorized};

/// Serves a worker verb after proving the caller holds the run's token.
/// Everything an agent can reach flows through here: reads and writes are
/// scoped to the authenticated run, and only a configured reported-verdict
/// stage can affect flow evidence.
pub(super) fn dispatch_worker(
    state: &mut DispatcherState,
    id: RequestId,
    request: Request,
    run_id: &str,
    token: Option<&str>,
) -> ResponseEnvelope {
    // The scope is read from the issued credential, not from the request: what
    // a token may do was decided when it was minted.
    let scope = token.and_then(|presented| {
        state
            .worker_tokens
            .get(run_id)
            .filter(|issued| issued.token == presented)
            .map(|issued| issued.scope.clone())
    });
    let Some(scope) = scope else {
        return ResponseEnvelope::failure(
            Some(id),
            unauthorized("the presented token is not valid for this run"),
        );
    };

    let data = match request {
        Request::Brief(_) => handle_brief(state, run_id, &scope),
        Request::Show(args) => match args.reference.as_deref() {
            Some(reference) if args.limit.is_none() => handle_show(state, run_id, reference),
            _ => Err(unauthorized(
                "workers may only show their own run's ticket by exact id",
            )),
        },
        Request::Note(args) => handle_note(state, run_id, &args.text),
        Request::Verdict(args) => match &scope {
            WorkerScope::Stage { .. } => handle_verdict(state, run_id, &scope, &args),
            WorkerScope::PanelReviewer {
                stage_index,
                reviewer_index,
                ..
            } => handle_panel_report(
                state,
                run_id,
                &scope,
                PanelSeat {
                    stage_index: *stage_index,
                    reviewer_index: *reviewer_index,
                },
                &args,
            ),
        },
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

/// The stage execution a worker's credential serves: which stage, which
/// execution of it, and what its result turns on.
struct ExecutingStage {
    name: String,
    /// A `return_to` edge re-enters a stage, and each execution is a separate
    /// assignment: the attempt is part of what a brief or a report is *for*.
    attempt: u32,
    check: Check,
}

/// Resolves the stage the caller is executing. Every scope answers from its own
/// credential, minted for one stage execution and never read off a request, so
/// a worker's authority is exactly the stage its token names.
///
/// Nothing here consults the checkpointed `stage_process` row. That is
/// load-bearing rather than incidental: the driver clears that row between
/// stages and writes the next one only after the stage's child is spawned, so
/// a worker quick enough to report inside the window found no row and was
/// refused — leaving the `reported` check it was answering waiting for a report
/// that had already been thrown away. The flow snapshot is still read, but only
/// to look up what the named stage turns on.
fn executing_stage(run: &RunRecord, scope: &WorkerScope) -> Result<ExecutingStage, ErrorBody> {
    if !matches!(run.state.as_str(), "running" | "driving") {
        return Err(conflict("the run has no stage currently executing"));
    }
    let snapshot = run
        .flow_json
        .as_deref()
        .ok_or_else(|| internal("the run has no flow snapshot"))?;
    let flow: Flow = serde_json::from_str(snapshot)
        .map_err(|error| internal(&format!("the run's flow snapshot is invalid: {error}")))?;
    let (name, attempt) = match scope {
        WorkerScope::Stage { stage, attempt } => (stage.clone(), *attempt),
        WorkerScope::PanelReviewer { stage, attempt, .. } => (stage.clone(), *attempt),
    };
    let stage = flow
        .stages
        .iter()
        .find(|stage| stage.name == name)
        .ok_or_else(|| internal("the executing stage is not in the run's flow snapshot"))?;
    Ok(ExecutingStage {
        name,
        attempt,
        check: stage.result_check.clone(),
    })
}

/// Everything the agent needs to work, re-readable after a compaction: the
/// ticket body from its committed file, the isolated workspace, the stage it
/// is executing, and what that stage turns on.
fn handle_brief(
    state: &DispatcherState,
    run_id: &str,
    scope: &WorkerScope,
) -> Result<serde_json::Value, ErrorBody> {
    let run = run_lookup(state, |run_store| run_store.run(run_id))?
        .ok_or_else(|| internal("the run for this token no longer exists"))?;
    let ticket = match run.ticket_json.as_deref() {
        Some(snapshot) => serde_json::from_str::<TicketSnapshot>(snapshot)
            .map_err(|error| internal(&format!("the run's ticket snapshot is invalid: {error}")))?,
        None => {
            let ticket = local_lookup(state, |work_state| work_state.ticket(&run.ticket_id))?
                .ok_or_else(|| internal("the ticket for this run no longer exists"))?;
            let body = ticket.body.unwrap_or_else(|| {
                ticket
                    .file_path
                    .as_ref()
                    .and_then(|file_path| fs::read_to_string(state.root.join(file_path)).ok())
                    .unwrap_or_default()
            });
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

    // The assignment is this stage execution, not the run: what a worker owes
    // is a property of the stage it is running, and the role its credential
    // gives it there.
    let executing = executing_stage(&run, scope)?;
    let role = match scope {
        WorkerScope::Stage { .. } => WorkerRole::Stage,
        WorkerScope::PanelReviewer { .. } => WorkerRole::PanelReviewer,
    };
    let definition_of_done = definition_of_done(role, &executing.check);

    Ok(json!({
        "run": run_id,
        "ticket": {
            "id": ticket.id,
            "name": ticket.name,
            "blocked_by": ticket.blocked_by,
            "worktree": ticket.worktree,
            "body": ticket.body,
            "target": ticket.target,
            "model": ticket.model,
            "effort": ticket.effort,
        },
        "worktree": run.worktree_path,
        "branch": run.branch,
        // The stage's identity only. A panel seat reads its stage from its own
        // credential and learns nothing here about the seats beside it.
        "stage": {
            "name": executing.name,
            "attempt": executing.attempt,
            "result_check": check_label(&executing.check),
        },
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
    let run = run_lookup(state, |run_store| run_store.run(run_id))?
        .ok_or_else(|| internal("the run for this token no longer exists"))?;
    if reference != run.ticket_id {
        return Err(unauthorized("workers may only show their own run's ticket"));
    }
    let ticket = local_lookup(state, |work_state| work_state.ticket(&run.ticket_id))?
        .ok_or_else(|| internal("the ticket for this run no longer exists"))?;
    let vendor_error = current_ticket_vendor_error(state, &ticket)?;
    Ok(ticket_show(reference, &ticket, vendor_error.as_ref()))
}

pub(super) fn ticket_show(
    reference: &str,
    ticket: &TicketRecord,
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
    ticket: &TicketRecord,
) -> Result<Option<VendorErrorMatch>, ErrorBody> {
    let vendor_error = run_lookup(state, |run_store| {
        run_store.latest_vendor_error_for_ticket(&ticket.id)
    })?;
    if ticket.state != "ready" {
        return Ok(vendor_error);
    }
    let cooldown_active = match ticket.target.as_deref() {
        Some(target) => run_lookup(state, |run_store| {
            run_store.active_cooldown_for_target(target, state.clock.now_ms())
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
    let ordinal = run_lookup(state, |run_store| run_store.next_note_ordinal())?;
    let note_id = format!("N{ordinal}");
    state
        .run_store
        .insert_note(&note_id, run_id, text, state.clock.now_ms())
        .map_err(|error| {
            mark_storage_full(state, &error);
            internal(&format!("cannot record note: {error}"))
        })?;
    Ok(json!({"note": {"id": note_id, "run": run_id, "text": text}}))
}

/// Where in the panel a reviewer's credential seats it. Copied out of the
/// scope so the handler cannot accidentally read anything else off the
/// request; the stage and attempt it belongs to come from the same credential,
/// through [`executing_stage`].
struct PanelSeat {
    stage_index: usize,
    reviewer_index: usize,
}

/// Records one panel reviewer's report against the seat its credential names.
///
/// Nothing in `args` says where the report lands: the run comes from the
/// socket, and the stage, attempt, and seat come from the token. A reviewer
/// therefore cannot report for another run, another stage, another attempt, or
/// another reviewer even if it fabricates every argument it sends.
///
/// The report is one-shot. The storage refuses a second insert on the same
/// seat, so the first report a reviewer makes is the one the panel counts;
/// there is no revising a vote after casting it.
fn handle_panel_report(
    state: &DispatcherState,
    run_id: &str,
    scope: &WorkerScope,
    seat: PanelSeat,
    args: &VerdictArgs,
) -> Result<serde_json::Value, ErrorBody> {
    let run = run_lookup(state, |run_store| run_store.run(run_id))?
        .ok_or_else(|| internal("the run for this token no longer exists"))?;
    let executing = executing_stage(&run, scope)?;
    // A panel report is what the whole stage turns on, and an unexplained one
    // is worth nothing to the operator reading `sloop show` afterwards.
    let reason = args
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|reason| !reason.is_empty())
        .ok_or_else(|| invalid_arguments("a panel reviewer must report a non-empty `--reason`"))?;
    let verdict = match args.verdict {
        VerdictValue::Pass => "pass",
        VerdictValue::Fail => "fail",
    };
    let confidence = args
        .confidence
        .map_or(Confidence::default(), Confidence::from);
    let record = PanelReportRecord {
        stage: &executing.name,
        stage_index: seat.stage_index,
        attempt: executing.attempt,
        reviewer_index: seat.reviewer_index,
        verdict,
        confidence: confidence.as_str(),
        reason,
    };
    let inserted = state
        .run_store
        .record_panel_report(run_id, &record, state.clock.now_ms())
        .map_err(|error| {
            mark_storage_full(state, &error);
            internal(&format!("cannot record panel report: {error}"))
        })?;
    if !inserted {
        return Err(conflict(&format!(
            "reviewer {} of stage `{}` has already reported",
            seat.reviewer_index, executing.name
        )));
    }
    Ok(json!({
        "verdict": {
            "run": run_id,
            "stage": executing.name,
            "reviewer": seat.reviewer_index,
            "verdict": verdict,
            "confidence": confidence.as_str(),
            "reason": reason,
        }
    }))
}

fn handle_verdict(
    state: &DispatcherState,
    run_id: &str,
    scope: &WorkerScope,
    args: &VerdictArgs,
) -> Result<serde_json::Value, ErrorBody> {
    let run = run_lookup(state, |run_store| run_store.run(run_id))?
        .ok_or_else(|| internal("the run for this token no longer exists"))?;
    // A worker can only ever report for the stage it is running, and the
    // resolver is the only thing that decides which stage that is.
    let executing = executing_stage(&run, scope)?;
    let stage_name = executing.name;
    let attempt = executing.attempt;
    if executing.check != Check::Reported {
        return Err(unauthorized(&format!(
            "stage `{stage_name}` does not use `result_check: reported`"
        )));
    }

    let verdict = match args.verdict {
        VerdictValue::Pass => "pass",
        VerdictValue::Fail => "fail",
    };
    // Stored, never consulted: the same rule a panel's seats live under, so
    // `--confidence` means one thing wherever a worker reports from.
    let confidence = args
        .confidence
        .map_or(Confidence::default(), Confidence::from);
    let inserted = state
        .run_store
        .record_stage_verdict(
            run_id,
            &stage_name,
            attempt,
            verdict,
            confidence.as_str(),
            args.reason.as_deref(),
            state.clock.now_ms(),
        )
        .map_err(|error| {
            mark_storage_full(state, &error);
            internal(&format!("cannot record stage verdict: {error}"))
        })?;
    if !inserted {
        return Err(conflict(&format!(
            "stage `{stage_name}` has already reported a verdict"
        )));
    }
    Ok(json!({
        "verdict": {
            "run": run_id,
            "stage": stage_name,
            "attempt": attempt,
            "verdict": verdict,
            "confidence": confidence.as_str(),
            "reason": args.reason,
        }
    }))
}
