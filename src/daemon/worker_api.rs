use std::fs;

use serde_json::json;

use crate::domain::ticket::TicketSnapshot;
use crate::flow::{Check, Confidence, Flow};
use crate::protocol::{ErrorBody, Request, RequestId, ResponseEnvelope, VerdictArgs, VerdictValue};
use crate::run_store::PanelReportRecord;
use crate::runner::WorkerScope;
use crate::vendor_error::VendorErrorMatch;

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
        Request::Brief(_) => handle_brief(state, run_id),
        Request::Show(args) => match args.reference.as_deref() {
            Some(reference) if args.limit.is_none() => handle_show(state, run_id, reference),
            _ => Err(unauthorized(
                "workers may only show their own run's ticket by exact id",
            )),
        },
        Request::Note(args) => handle_note(state, run_id, &args.text),
        Request::Verdict(args) => match &scope {
            WorkerScope::Stage => handle_verdict(state, run_id, &args),
            WorkerScope::PanelReviewer {
                stage,
                stage_index,
                attempt,
                reviewer_index,
            } => handle_panel_report(
                state,
                run_id,
                PanelSeat {
                    stage,
                    stage_index: *stage_index,
                    attempt: *attempt,
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

/// Everything the agent needs to work, re-readable after a compaction: the
/// ticket body from its committed file, the isolated workspace, and the
/// evidence-based definition of done.
fn handle_brief(state: &DispatcherState, run_id: &str) -> Result<serde_json::Value, ErrorBody> {
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

    let mut definition_of_done = vec!["Commit your work to the run branch".to_owned()];
    if state.flow_test_cmd.is_some() {
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

/// The seat a panel reviewer's credential names. Copied out of the scope so
/// the handler cannot accidentally read anything else off the request.
struct PanelSeat<'a> {
    stage: &'a str,
    stage_index: usize,
    attempt: u32,
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
    seat: PanelSeat<'_>,
    args: &VerdictArgs,
) -> Result<serde_json::Value, ErrorBody> {
    let run = run_lookup(state, |run_store| run_store.run(run_id))?
        .ok_or_else(|| internal("the run for this token no longer exists"))?;
    if !matches!(run.state.as_str(), "running" | "driving") {
        return Err(conflict("the run has no stage currently executing"));
    }
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
        stage: seat.stage,
        stage_index: seat.stage_index,
        attempt: seat.attempt,
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
            seat.reviewer_index, seat.stage
        )));
    }
    Ok(json!({
        "verdict": {
            "run": run_id,
            "stage": seat.stage,
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
    args: &VerdictArgs,
) -> Result<serde_json::Value, ErrorBody> {
    let run = run_lookup(state, |run_store| run_store.run(run_id))?
        .ok_or_else(|| internal("the run for this token no longer exists"))?;
    let snapshot = run
        .flow_json
        .as_deref()
        .ok_or_else(|| internal("the run has no flow snapshot"))?;
    let flow: Flow = serde_json::from_str(snapshot)
        .map_err(|error| internal(&format!("the run's flow snapshot is invalid: {error}")))?;
    // The executing stage is whatever the driver last checkpointed a process
    // for — one answer, whatever kind of stage it is and wherever it sits in
    // the flow. A worker can only ever report for the stage it is running.
    if !matches!(run.state.as_str(), "running" | "driving") {
        return Err(conflict("the run has no stage currently executing"));
    }
    let rows = run_lookup(state, |run_store| run_store.run_evidence(run_id))?;
    let executing = rows
        .iter()
        .find(|(kind, _)| kind == super::driver::STAGE_PROCESS)
        .and_then(|(_, data)| serde_json::from_str::<serde_json::Value>(data).ok())
        .ok_or_else(|| conflict("the run has no stage process currently executing"))?;
    let stage_name = executing["stage"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| conflict("the run has no stage process currently executing"))?;
    // A backward edge can re-enter a reported stage, and each execution is
    // owed its own report: the attempt is part of what the report is *for*.
    let attempt = executing["attempt"]
        .as_u64()
        .and_then(|attempt| u32::try_from(attempt).ok())
        .unwrap_or(1);
    let stage = flow
        .stages
        .iter()
        .find(|stage| stage.name == stage_name)
        .ok_or_else(|| internal("the executing stage is not in the run's flow snapshot"))?;
    if stage.result_check != Check::Reported {
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
