use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use serde_json::json;
use tokio::sync::mpsc;

use crate::db::StoreError;
use crate::domain::ticket::TicketSnapshot;
use crate::domain::work::{Disposition, OwnerId, TicketRef, WorkOutcome, WorkTicket};
use crate::flow::Flow;
use crate::frontmatter::Frontmatter;
use crate::ids::next_id;
use crate::logging::{LogLevel, OperationalLog};
use crate::run_log::output_staleness;
use crate::run_store::{
    NeedsReviewBranch, OutputStallEvidence, RunAdmission, RunState, RunStore,
    WorktreeCleanupCandidate,
};
use crate::runner::local::{run_output_path, wait_for_test_hook};
use crate::work_state::ClaimResult;
use crate::work_state::local::LocalSqlite;
use crate::work_state::trigger::QueuedTrigger;

use super::dispatcher::{
    DispatcherState, RunEvent, close_worker_socket, disposition_for_outcome, mark_storage_full,
    push_work_outcome, recover_storage, settle_pending_exits,
};
use super::driver::{DriverEnvironment, DriverPlan, git_is_ancestor, git_stdout, start_driver};
use super::recovery::{
    ProcessIdentity, executing_stage, reconcile_run_liveness, recoverable_process_identity,
    stop_agent_process_group,
};
use super::server::DaemonError;

pub(super) const DEFAULT_LEASE_MS: i64 = 10 * 60 * 1000;
pub(super) const VENDOR_COOLDOWN_MS: i64 = 5 * 60 * 1000;

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
    local_work_state: &LocalSqlite,
    run_store: &RunStore,
    now_ms: i64,
    delete_missing_after_ms: i64,
) -> Result<(), DaemonError> {
    for ticket in local_work_state
        .local_ticket_files()
        .map_err(DaemonError::Store)?
    {
        if root.join(&ticket.file_path).is_file() {
            if ticket.missing_at_ms.is_some() {
                local_work_state
                    .clear_ticket_missing(&ticket.id, now_ms)
                    .map_err(DaemonError::Store)?;
            }
            continue;
        }
        let is_referenced = ticket_is_referenced(local_work_state, run_store, &ticket.id)
            .map_err(DaemonError::Store)?;
        match orphan_disposition(
            ticket.missing_at_ms,
            is_referenced,
            now_ms,
            delete_missing_after_ms,
        ) {
            OrphanDisposition::MarkMissing => {
                local_work_state
                    .mark_ticket_missing(&ticket.id, now_ms)
                    .map_err(DaemonError::Store)?;
            }
            OrphanDisposition::Delete => {
                local_work_state
                    .delete_ticket(&ticket.id)
                    .map_err(DaemonError::Store)?;
            }
            OrphanDisposition::Keep => {}
        }
    }
    Ok(())
}

fn ticket_is_referenced(
    local_work_state: &LocalSqlite,
    run_store: &RunStore,
    ticket_id: &str,
) -> Result<bool, StoreError> {
    Ok(run_store.ticket_is_referenced(ticket_id)?
        || local_work_state.ticket_has_work_references(ticket_id)?)
}

/// Indexes committed project Markdown files into the store so ticket membership
/// can be validated. Runs at startup; `reindex` will share it.
pub(super) fn index_projects(
    root: &Path,
    project_dir: &Path,
    local_work_state: &LocalSqlite,
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
        local_work_state
            .upsert_local_project(&id, &relative, &title, now_ms)
            .map_err(DaemonError::Store)?;
        indexed.push(id);
    }
    Ok(indexed)
}

/// Whether the daemon should renew this run's lease.
///
/// Renewal states a fact the daemon can vouch for — *I am supervising a live
/// run* — so it is refused whenever that cannot be shown. A run whose process
/// identity check failed keeps its expiring lease: letting a dead run's lease
/// live on is exactly what strict renewal exists to prevent. This decides
/// nothing about scheduling; process identity remains the only liveness
/// authority.
pub(super) fn renews_lease(
    run_state: RunState,
    supervised: bool,
    identity: ProcessIdentity,
) -> bool {
    if run_state.is_terminal() {
        return false;
    }
    if run_state == RunState::Driving {
        // A driving run has no agent process left to identify. The driver
        // walking its stages is the liveness evidence, the same reading
        // run-liveness reconciliation takes.
        return supervised;
    }
    identity != ProcessIdentity::GoneOrReused
}

async fn release_unrecorded_claim(
    state: &mut DispatcherState,
    ticket: &TicketRef,
    owner: &OwnerId,
    log: &OperationalLog,
) {
    if let Err(error) = state
        .work_state
        .release(
            ticket,
            owner,
            Disposition::Retry {
                not_before_ms: Some(state.clock.now_ms()),
            },
        )
        .await
    {
        log.emit_with_fields(
            LogLevel::Error,
            "sloop::dispatcher",
            "unrecorded_claim_release_failed",
            json!({
                "run_id": owner.0,
                "ticket_id": ticket.id,
                "error": format!("{error:?}"),
            }),
        );
    }
}

/// Keeps `expires_at_ms` truthful for every run this daemon supervises, so a
/// run outliving the initial TTL no longer executes on an expired lease.
async fn renew_supervised_leases(state: &mut DispatcherState, log: &OperationalLog) {
    let runs = match state.run_store.recoverable_runs() {
        Ok(runs) => runs,
        Err(error) => {
            log.emit_with_fields(
                LogLevel::Error,
                "sloop::dispatcher",
                "lease_renewal_scan_failed",
                json!({"error": error.to_string()}),
            );
            return;
        }
    };
    for run in runs {
        if state.recovering.contains(&run.id) {
            continue;
        }
        let supervised = state.supervised.contains(&run.id);
        if !renews_lease(run.state, supervised, recoverable_process_identity(&run)) {
            continue;
        }
        let ticket = match state.local_work_state.ticket(&run.ticket_id) {
            Ok(Some(ticket)) => TicketRef {
                id: ticket.id,
                source: ticket.source,
                source_ref: ticket.source_ref,
            },
            Ok(None) => continue,
            Err(error) => {
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::dispatcher",
                    "lease_renewal_failed",
                    json!({"run_id": run.id, "ticket_id": run.ticket_id, "error": error.to_string()}),
                );
                continue;
            }
        };
        let renewed = state
            .work_state
            .renew(&ticket, &OwnerId(run.id.clone()))
            .await;
        match renewed {
            Ok(ClaimResult::Claimed { .. }) => {}
            // The daemon believes it supervises this run yet does not hold a
            // renewable lease on it. That is an anomaly worth surfacing, but
            // recovery keys off process identity, so nothing changes here.
            Ok(ClaimResult::Lost { .. }) => log.emit_with_fields(
                LogLevel::Warn,
                "sloop::dispatcher",
                "lease_renewal_denied",
                json!({"run_id": run.id, "ticket_id": run.ticket_id}),
            ),
            Err(error) => log.emit_with_fields(
                LogLevel::Error,
                "sloop::dispatcher",
                "lease_renewal_failed",
                json!({"run_id": run.id, "ticket_id": run.ticket_id, "error": format!("{error:?}")}),
            ),
        }
    }
}

/// The single spawn decision point: every queued trigger passes the same
/// pause and capacity gates, selects deterministically, claims conditionally,
/// and only then touches Git and processes.
pub(super) async fn reconcile(
    state: &mut DispatcherState,
    events: &mpsc::Sender<RunEvent>,
    log: &OperationalLog,
) {
    let now_ms = state.clock.now_ms();
    if !recover_storage(state, now_ms) {
        return;
    }
    settle_pending_exits(state, log).await;
    reconcile_output_stalls(state, log);
    reconcile_stall_watchdog(state, log);
    // Settling externally merged review branches is independent of the dispatch
    // gates below: it releases blocked dependents even while paused, at
    // capacity, or outside running hours.
    reconcile_external_merges(state, log).await;
    reconcile_worktree_cleanup(state, log);
    // Supervised runs keep their leases truthful regardless of the dispatch
    // gates: a run outliving the TTL while the daemon is paused is still alive.
    renew_supervised_leases(state, log).await;
    if state.storage_full.get()
        || state.reconciliation_blocked
        || state.paused
        || state.draining
        || state.agent.is_none()
        || !running_hours_open(state, now_ms)
    {
        return;
    }
    let triggers = match state.local_work_state.dispatchable_triggers(now_ms) {
        Ok(triggers) => triggers,
        Err(error) => {
            log.emit_with_fields(
                LogLevel::Error,
                "sloop::dispatcher",
                "trigger_scan_failed",
                json!({"error": error.to_string()}),
            );
            return;
        }
    };

    // A durable lease missing from memory must consume capacity before the
    // dispatcher can use an apparently free slot. The periodic pass remains
    // responsible for idle reconciliation; this extra scan only runs when a
    // queued trigger could otherwise spawn now.
    if !triggers.is_empty() && state.active.len() < state.max_agents {
        wait_for_test_hook("before-spawn-capacity-reconciliation");
        reconcile_run_liveness(state, events, log).await;
        if state.reconciliation_blocked {
            return;
        }
    }

    for trigger in triggers {
        if state.active.len() >= state.max_agents {
            break;
        }
        let Some(ticket_id) =
            eligible_ticket(&state.local_work_state, &state.run_store, &trigger, now_ms)
        else {
            continue;
        };

        let ticket_record = match state.local_work_state.ticket(&ticket_id) {
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
        let fallback_body = ticket_record.body.clone().unwrap_or_else(|| {
            ticket_record
                .file_path
                .as_ref()
                .and_then(|file_path| fs::read_to_string(state.root.join(file_path)).ok())
                .unwrap_or_default()
        });
        let ticket_ref = TicketRef {
            id: ticket_record.id,
            source: ticket_record.source,
            source_ref: ticket_record.source_ref,
        };

        let now_ms = state.clock.now_ms();
        // Minting reads the OS CSPRNG, so it happens here at the effect
        // boundary; everything downstream just carries the id it was handed.
        let run_id = match state.run_ids.mint() {
            Ok(run_id) => run_id,
            Err(error) => {
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::dispatcher",
                    "run_id_minting_failed",
                    json!({"error": error}),
                );
                continue;
            }
        };
        let owner = OwnerId(run_id.clone());
        let ticket = match state
            .work_state
            .claim(
                &ticket_ref,
                &owner,
                Duration::from_millis(DEFAULT_LEASE_MS as u64),
            )
            .await
        {
            Ok(ClaimResult::Claimed { ticket }) => ticket,
            // Losing a conditional source claim is expected under contention.
            Ok(ClaimResult::Lost { .. }) => continue,
            Err(error) => {
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::dispatcher",
                    "claim_failed",
                    json!({
                        "trigger_id": trigger.id,
                        "ticket_id": ticket_id,
                        "run_id": run_id,
                        "error": format!("{error:?}"),
                    }),
                );
                continue;
            }
        };
        let flow = match bound_flow_for_work_ticket(&state.flows, &ticket) {
            Ok(flow) => flow,
            Err(error) => {
                release_unrecorded_claim(state, &ticket_ref, &owner, log).await;
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::dispatcher",
                    "bound_flow_resolution_failed",
                    json!({"ticket_id": ticket_id, "error": error}),
                );
                continue;
            }
        };
        // `on_fail` predates `fail_action` and does a narrower version of the
        // same job: a failing stage that gets another chance. It still works,
        // and is not being removed here, but a flow that uses it is admitted
        // with a note so the operator learns there is a replacement before the
        // mechanism goes.
        let repaired: Vec<&str> = flow
            .stages
            .iter()
            .filter(|stage| stage.on_fail.is_some())
            .map(|stage| stage.name.as_str())
            .collect();
        if !repaired.is_empty() {
            log.emit_with_fields(
                LogLevel::Warn,
                "sloop::dispatcher",
                "flow_on_fail_deprecated",
                json!({
                    "ticket_id": ticket_id,
                    "run_id": run_id,
                    "flow": flow.name,
                    "stages": repaired,
                    "replacement": "fail_action: { return_to: <stage>, attempts: N }",
                }),
            );
        }
        let Some(trigger_id) = ticket.hints.trigger_id.as_deref() else {
            release_unrecorded_claim(state, &ticket_ref, &owner, log).await;
            log.emit_with_fields(
                LogLevel::Error,
                "sloop::dispatcher",
                "claim_missing_trigger",
                json!({"ticket_id": ticket_id, "run_id": run_id}),
            );
            continue;
        };
        let ticket_snapshot = TicketSnapshot {
            id: ticket.id.clone(),
            name: ticket.name.clone(),
            blocked_by: ticket.blocked_by.clone(),
            worktree: ticket.hints.worktree.clone(),
            target: ticket.hints.target.clone(),
            model: ticket.hints.model.clone(),
            effort: ticket.hints.effort.clone(),
            body: if ticket.body.is_empty() {
                fallback_body
            } else {
                ticket.body.clone()
            },
        };
        let flow_json = serde_json::to_string(&flow).expect("flow snapshots serialize to JSON");
        let ticket_json =
            serde_json::to_string(&ticket_snapshot).expect("ticket snapshots serialize to JSON");
        let admission = RunAdmission {
            ticket_id: &ticket_id,
            run_id: &run_id,
            trigger_id,
            flow_json: &flow_json,
            ticket_json: &ticket_json,
        };
        let claimed = match state.run_store.insert_claimed_run(&admission, now_ms) {
            Ok(claimed) => claimed,
            Err(error) => {
                mark_storage_full(state, &error);
                release_unrecorded_claim(state, &ticket_ref, &owner, log).await;
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::dispatcher",
                    "claim_failed",
                    json!({
                        "trigger_id": trigger.id,
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
        // Admission ends here. Everything after it — the workspace, every
        // stage, the merge, and the settlement event — belongs to the run's
        // driver, which owns the run from this moment until it settles.
        let target = ticket
            .hints
            .target
            .clone()
            .or_else(|| {
                state
                    .agent
                    .as_ref()
                    .map(|agent| agent.default_target.clone())
            })
            .unwrap_or_default();
        // Paths and branches carry the short id: filesystem names are internal
        // plumbing, and a 32-character suffix would drown the readable part.
        let short_id = crate::run_ref::short(&run_id);
        let plan = DriverPlan {
            run_id: run_id.clone(),
            ticket_id: ticket_id.clone(),
            target,
            branch: format!("sloop/{}-a{}-{short_id}", ticket.id, claimed.attempt),
            worktree: state.worktree_dir.join(short_id),
            flow,
            ticket: Some(ticket_snapshot),
            recovery: None,
        };
        state.active.insert(run_id.clone());
        state.supervised.insert(run_id.clone());
        start_driver(DriverEnvironment::from_state(state), plan, events.clone());
        log.emit_with_fields(
            LogLevel::Info,
            "sloop::dispatcher",
            "run_dispatched",
            json!({"run_id": run_id, "ticket_id": ticket_id, "attempt": claimed.attempt}),
        );
    }
}

/// Undoes an admission whose driver could not open the run's workspace. Nothing
/// of the run was recorded, so the claim rolls back and the ticket is queued
/// again — exactly what a failed spawn did when the dispatcher owned it.
pub(super) async fn roll_back_admission(
    state: &mut DispatcherState,
    run_id: &str,
    ticket_id: &str,
    error: &str,
    log: &OperationalLog,
) {
    state.active.remove(run_id);
    state.supervised.remove(run_id);
    match state
        .run_store
        .abort(run_id, ticket_id, state.clock.now_ms())
    {
        Ok(_) => {
            let ticket = match state.local_work_state.ticket(ticket_id) {
                Ok(Some(ticket)) => Some(TicketRef {
                    id: ticket.id,
                    source: ticket.source,
                    source_ref: ticket.source_ref,
                }),
                Ok(None) => None,
                Err(store_error) => {
                    mark_storage_full(state, &store_error);
                    None
                }
            };
            if let Some(ticket) = ticket
                && let Err(release_error) = state
                    .work_state
                    .release(
                        &ticket,
                        &OwnerId(run_id.to_owned()),
                        Disposition::Retry {
                            not_before_ms: Some(state.clock.now_ms()),
                        },
                    )
                    .await
            {
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::dispatcher",
                    "claim_release_failed",
                    json!({
                        "run_id": run_id,
                        "ticket_id": ticket_id,
                        "error": release_error.to_string(),
                    }),
                );
            }
        }
        Err(abort_error) => {
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
    }
    // A driver can fail after the worker socket was bound.
    close_worker_socket(state, run_id);
    log.emit_with_fields(
        LogLevel::Error,
        "sloop::dispatcher",
        "run_launch_failed",
        json!({"run_id": run_id, "ticket_id": ticket_id, "error": error}),
    );
}

/// Notices review branches an operator merged into the default branch by hand.
/// A run branch whose tip is a strict ancestor of the default branch tip has
/// been integrated, so its `needs_review` ticket settles to `merged` without a
/// reindex. Refs are resolved freshly every pass; a missing branch or any git
/// failure is a no-op for that ticket, since unprovable evidence is not
/// evidence. Squash- and rebase-merges rewrite the commits and are invisible to
/// ancestry, so they still require `sloop reindex`.
async fn reconcile_external_merges(state: &mut DispatcherState, log: &OperationalLog) {
    let branches = match state.run_store.needs_review_branches() {
        Ok(branches) => branches,
        Err(error) => {
            log.emit_with_fields(
                LogLevel::Error,
                "sloop::dispatcher",
                "needs_review_scan_failed",
                json!({"error": error.to_string()}),
            );
            return;
        }
    };
    if branches.is_empty() {
        return;
    }
    let default_tip = git_stdout(&state.root, &["rev-parse", "HEAD"]).ok();
    for branch in branches {
        match state.run_store.recorded_outcome(&branch.run_id) {
            Ok(Some(outcome)) if outcome.work.verdict == crate::outcome::Outcome::Merged => {
                release_external_merge(state, &branch, outcome.work, log).await;
                continue;
            }
            Ok(_) => {}
            Err(error) => {
                mark_storage_full(state, &error);
                continue;
            }
        }
        let Some(default_tip) = default_tip.as_deref() else {
            // Without the default branch tip nothing new can be proven.
            continue;
        };
        let Ok(branch_tip) = git_stdout(&state.root, &["rev-parse", &branch.branch]) else {
            // A deleted branch ref or any git failure leaves the ticket alone.
            continue;
        };
        // A tip equal to the default tip is not a strict ancestor: an untouched
        // branch coinciding with the default branch proves no external merge.
        if branch_tip == default_tip {
            continue;
        }
        if !matches!(
            git_is_ancestor(&state.root, &branch_tip, default_tip),
            Ok(true)
        ) {
            continue;
        }
        let now_ms = state.clock.now_ms();
        match state.run_store.record_external_merge(
            &branch.run_id,
            &branch.ticket_id,
            &branch.branch,
            &branch_tip,
            default_tip,
            now_ms,
        ) {
            Ok(applied) => {
                let outcome = match state.run_store.recorded_outcome(&branch.run_id) {
                    Ok(Some(outcome)) => outcome.work,
                    Ok(None) => continue,
                    Err(error) => {
                        mark_storage_full(state, &error);
                        continue;
                    }
                };
                if !release_external_merge(state, &branch, outcome, log).await || !applied {
                    continue;
                }
                log.emit_with_fields(
                    LogLevel::Info,
                    "sloop::dispatcher",
                    "external_merge_reconciled",
                    json!({
                        "ticket_id": branch.ticket_id,
                        "run_id": branch.run_id,
                        "branch": branch.branch,
                        "branch_tip": branch_tip,
                        "observed_default_tip": default_tip,
                    }),
                );
            }
            Err(error) => {
                mark_storage_full(state, &error);
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::dispatcher",
                    "external_merge_settle_failed",
                    json!({"ticket_id": branch.ticket_id, "run_id": branch.run_id, "error": error.to_string()}),
                );
            }
        }
    }
}

async fn release_external_merge(
    state: &mut DispatcherState,
    branch: &NeedsReviewBranch,
    outcome: WorkOutcome,
    log: &OperationalLog,
) -> bool {
    let ticket = match state.local_work_state.ticket(&branch.ticket_id) {
        Ok(Some(ticket)) => TicketRef {
            id: ticket.id,
            source: ticket.source,
            source_ref: ticket.source_ref,
        },
        Ok(None) => return false,
        Err(error) => {
            mark_storage_full(state, &error);
            return false;
        }
    };
    if let Err(error) = state
        .work_state
        .release(
            &ticket,
            &outcome.owner,
            disposition_for_outcome(outcome.verdict, None),
        )
        .await
    {
        log.emit_with_fields(
            LogLevel::Error,
            "sloop::dispatcher",
            "external_merge_release_failed",
            json!({"ticket_id": branch.ticket_id, "run_id": branch.run_id, "error": error.to_string()}),
        );
        return false;
    }
    push_work_outcome(state, outcome, log, "sloop::dispatcher");
    true
}

fn worktree_cleanup_due(
    cleanup_eligible_at_ms: i64,
    retention_ms: i64,
    now_ms: i64,
    is_live: bool,
) -> bool {
    !is_live && now_ms >= cleanup_eligible_at_ms.saturating_add(retention_ms)
}

fn reconcile_worktree_cleanup(state: &mut DispatcherState, log: &OperationalLog) {
    if state.draining {
        return;
    }
    let Some(retention_ms) = state.worktree_retention_ms else {
        return;
    };
    let candidates = match state.run_store.worktree_cleanup_candidates() {
        Ok(candidates) => candidates,
        Err(error) => {
            log.emit_with_fields(
                LogLevel::Error,
                "sloop::dispatcher",
                "worktree_cleanup_scan_failed",
                json!({"error": error.to_string()}),
            );
            return;
        }
    };
    let now_ms = state.clock.now_ms();
    for candidate in candidates {
        let is_live = state.active.contains(&candidate.run_id);
        let worktree_missing = !Path::new(&candidate.worktree_path).exists();
        if is_live
            || (!worktree_missing
                && !worktree_cleanup_due(
                    candidate.cleanup_eligible_at_ms,
                    retention_ms,
                    now_ms,
                    false,
                ))
        {
            continue;
        }
        if let Err(error) = remove_run_worktree(&state.root, &candidate) {
            log.emit_with_fields(
                LogLevel::Warn,
                "sloop::dispatcher",
                "run_worktree_cleanup_failed",
                json!({
                    "run_id": candidate.run_id,
                    "ticket_id": candidate.ticket_id,
                    "branch": candidate.branch,
                    "worktree": candidate.worktree_path,
                    "error": error,
                }),
            );
            continue;
        }
        match state
            .run_store
            .mark_run_worktree_cleaned(&candidate, state.clock.now_ms())
        {
            Ok(true) => log.emit_with_fields(
                LogLevel::Info,
                "sloop::dispatcher",
                "run_worktree_cleaned",
                json!({
                    "run_id": candidate.run_id,
                    "ticket_id": candidate.ticket_id,
                    "branch": candidate.branch,
                    "worktree": candidate.worktree_path,
                }),
            ),
            Ok(false) => {}
            Err(error) => {
                mark_storage_full(state, &error);
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::dispatcher",
                    "run_worktree_cleanup_record_failed",
                    json!({"run_id": candidate.run_id, "error": error.to_string()}),
                );
            }
        }
    }
}

fn remove_run_worktree(root: &Path, candidate: &WorktreeCleanupCandidate) -> Result<(), String> {
    let worktree = Path::new(&candidate.worktree_path);
    if worktree.exists() {
        git_status(root, "worktree remove", |command| {
            command
                .args(["worktree", "remove", "--force"])
                .arg(worktree);
        })?;
    }
    git_status(root, "worktree prune", |command| {
        command.args(["worktree", "prune"]);
    })?;

    let branch_ref = format!("refs/heads/{}", candidate.branch);
    let branch_exists = Command::new("git")
        .args(["show-ref", "--verify", "--quiet", &branch_ref])
        .current_dir(root)
        .status()
        .map_err(|error| format!("git show-ref failed: {error}"))?;
    match branch_exists.code() {
        Some(0) => git_status(root, "branch deletion", |command| {
            command.args(["branch", "-D", &candidate.branch]);
        }),
        Some(1) => Ok(()),
        _ => Err(format!("git show-ref failed: {branch_exists}")),
    }
}

fn git_status(
    root: &Path,
    operation: &str,
    configure: impl FnOnce(&mut Command),
) -> Result<(), String> {
    let mut command = Command::new("git");
    command.current_dir(root);
    configure(&mut command);
    let output = command
        .output()
        .map_err(|error| format!("git {operation} failed: {error}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "git {operation} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))
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
    let cooldown_deadline = state.run_store.next_active_cooldown(now_ms).ok().flatten();
    let next_eligible = state
        .local_work_state
        .next_trigger_eligible_at_ms(now_ms)
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
            .local_work_state
            .dispatchable_triggers(now_ms)
            .is_ok_and(|triggers| !triggers.is_empty());
        if has_due_demand || next_eligible.is_some_and(|deadline| deadline <= opening) {
            Some(opening)
        } else {
            next_eligible
        }
    };
    let cleanup_deadline = state.worktree_retention_ms.and_then(|retention_ms| {
        state
            .run_store
            .next_worktree_cleanup_at_ms(retention_ms, now_ms)
            .ok()
            .flatten()
    });
    let stall_report_deadline = next_output_stall_deadline(state, state.stall_report_after_ms);
    let stall_kill_deadline = next_output_stall_deadline(state, state.stall_after_ms);
    [
        hours_deadline,
        cooldown_deadline,
        cleanup_deadline,
        stall_report_deadline,
        stall_kill_deadline,
    ]
    .into_iter()
    .flatten()
    .min()
}

fn supervised_running_runs(
    state: &DispatcherState,
) -> impl Iterator<Item = crate::run_store::RunRecord> + '_ {
    state
        .run_store
        .recoverable_runs()
        .unwrap_or_default()
        .into_iter()
        .filter(|run| run.state == RunState::Running)
        .filter(|run| state.supervised.contains(&run.id))
        .filter(|run| !state.cancelling.contains(&run.id))
        .filter(|run| !state.stalling.contains(&run.id))
        .filter(|run| !state.suspected_dead.contains(&run.id))
        .filter(|run| !state.recovering.contains(&run.id))
        .filter(|run| !state.pending_exits.contains_key(&run.id))
        .filter_map(|run| state.run_store.run(&run.id).ok().flatten())
}

pub(super) fn running_output_staleness(
    state: &DispatcherState,
    run: &crate::run_store::RunRecord,
    now_ms: i64,
) -> Option<crate::run_log::OutputStaleness> {
    running_output_staleness_after(state, run, now_ms, state.stall_report_after_ms)
}

fn running_output_staleness_after(
    state: &DispatcherState,
    run: &crate::run_store::RunRecord,
    now_ms: i64,
    threshold_ms: i64,
) -> Option<crate::run_log::OutputStaleness> {
    let started_at_ms = state
        .run_store
        .run_timelines(&[run.id.as_str()])
        .ok()?
        .remove(&run.id)?
        .started_at_ms?;
    output_staleness(
        &run_output_path(&state.state_dir, &run.id),
        started_at_ms,
        now_ms,
        threshold_ms,
    )
    .ok()
}

fn next_output_stall_deadline(state: &DispatcherState, threshold_ms: i64) -> Option<i64> {
    let now_ms = state.clock.now_ms();
    supervised_running_runs(state)
        .filter_map(|run| running_output_staleness_after(state, &run, now_ms, threshold_ms))
        .filter(|staleness| !staleness.stalled)
        .map(|staleness| staleness.deadline_ms)
        .min()
}

fn reconcile_output_stalls(state: &mut DispatcherState, log: &OperationalLog) {
    let now_ms = state.clock.now_ms();
    let running = supervised_running_runs(state).collect::<Vec<_>>();
    for run in running {
        let Some(staleness) = running_output_staleness(state, &run, now_ms) else {
            continue;
        };
        if !staleness.stalled
            || state.reported_stalls.get(&run.id) == Some(&staleness.last_sequence)
        {
            continue;
        }
        let stage = executing_stage(&state.run_store, &run.id, run.flow_json.as_deref());
        log.emit_with_fields(
            LogLevel::Warn,
            "sloop::dispatcher",
            "run_output_stalled",
            json!({
                "run_id": run.id,
                "ticket_id": run.ticket_id,
                "stage": stage,
                "silent_for_ms": staleness.silent_for_ms,
                "last_output_sequence": staleness.last_sequence,
            }),
        );
        state
            .reported_stalls
            .insert(run.id, staleness.last_sequence);
    }
}

fn reconcile_stall_watchdog(state: &mut DispatcherState, log: &OperationalLog) {
    let now_ms = state.clock.now_ms();
    let running = supervised_running_runs(state).collect::<Vec<_>>();
    for run in running {
        let Some(staleness) =
            running_output_staleness_after(state, &run, now_ms, state.stall_after_ms)
        else {
            continue;
        };
        if !staleness.stalled {
            continue;
        }
        let evidence = OutputStallEvidence {
            stage: executing_stage(&state.run_store, &run.id, run.flow_json.as_deref()),
            last_output_at_ms: staleness.last_output_at_ms,
            threshold_ms: state.stall_after_ms,
            last_output_sequence: staleness.last_sequence,
        };
        let recorded = match state
            .run_store
            .record_output_stall(&run.id, &evidence, now_ms)
        {
            Ok(recorded) => recorded,
            Err(error) => {
                mark_storage_full(state, &error);
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::dispatcher",
                    "output_stall_persist_failed",
                    json!({"run_id": run.id, "error": error.to_string()}),
                );
                continue;
            }
        };
        if !recorded
            && !state
                .run_store
                .output_stall(&run.id)
                .is_ok_and(|evidence| evidence.is_some())
        {
            continue;
        }
        state.stalling.insert(run.id.clone());
        log.emit_with_fields(
            LogLevel::Warn,
            "sloop::dispatcher",
            "run_output_stall_terminated",
            json!({
                "run_id": run.id,
                "ticket_id": run.ticket_id,
                "stage": evidence.stage,
                "last_output_at_ms": evidence.last_output_at_ms,
                "threshold_ms": evidence.threshold_ms,
            }),
        );
        if let Err(error) =
            stop_agent_process_group(run.pid, run.pid_start_time, run.process_group_id)
        {
            log.emit_with_fields(
                LogLevel::Error,
                "sloop::dispatcher",
                "output_stall_signal_refused",
                json!({"run_id": run.id, "error": error}),
            );
        }
    }
}

/// Restores warning episode markers from the append-only operational log.
/// This keeps a daemon restart from warning twice for unchanged output while
/// requiring no runtime schema change.
pub(super) fn restore_reported_output_stalls(state: &mut DispatcherState) {
    let active = state
        .run_store
        .recoverable_runs()
        .unwrap_or_default()
        .into_iter()
        .map(|run| run.id)
        .collect::<std::collections::HashSet<_>>();
    let contents = match fs::read_to_string(&state.daemon_log) {
        Ok(contents) => contents,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return,
        Err(_) => return,
    };
    for line in contents.lines() {
        let Ok(record) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if record["event"] != "run_output_stalled" {
            continue;
        }
        let Some(fields) = record["fields"].as_object() else {
            continue;
        };
        let (Some(run_id), Some(sequence)) = (
            fields.get("run_id").and_then(serde_json::Value::as_str),
            fields.get("last_output_sequence"),
        ) else {
            continue;
        };
        if !active.contains(run_id) {
            continue;
        }
        state
            .reported_stalls
            .insert(run_id.to_owned(), sequence.as_u64());
    }
}

/// Retires triggers pinned to a ticket that is already `merged`. Settling
/// now completes them in the same transaction as the merge, but that rule is
/// not retroactive and a merged ticket never settles again, so rows stranded
/// before it existed need this one-off sweep. Without it they stay `queued`
/// forever, unfireable but still counted as pending demand.
///
/// Runs once per daemon lifetime, and only reports what it actually changed: a
/// clean database selects nothing, writes nothing, and logs nothing.
pub(super) fn reconcile_merged_ticket_triggers(state: &DispatcherState, log: &OperationalLog) {
    let now_ms = state.clock.now_ms();
    match state
        .local_work_state
        .complete_merged_ticket_triggers(now_ms)
    {
        Ok(completed) => {
            for (trigger_id, ticket_id) in completed {
                log.emit_with_fields(
                    LogLevel::Info,
                    "sloop::daemon",
                    "trigger_completed_on_merged_ticket",
                    json!({"trigger_id": trigger_id, "ticket_id": ticket_id}),
                );
            }
        }
        Err(error) => {
            mark_storage_full(state, &error);
            log.emit_with_fields(
                LogLevel::Error,
                "sloop::daemon",
                "merged_ticket_trigger_sweep_failed",
                json!({"error": error.to_string()}),
            );
        }
    }
}

fn eligible_ticket(
    local_work_state: &LocalSqlite,
    run_store: &RunStore,
    trigger: &QueuedTrigger,
    now_ms: i64,
) -> Option<String> {
    match &trigger.ticket_id {
        Some(ticket)
            if local_work_state
                .ticket_is_dispatchable(ticket)
                .unwrap_or(false) =>
        {
            let record = local_work_state.ticket(ticket).ok().flatten()?;
            let target = record.target.as_deref()?;
            run_store
                .active_cooldown_for_target(target, now_ms)
                .ok()
                .flatten()
                .is_none()
                .then(|| ticket.clone())
        }
        Some(_) => None,
        None => local_work_state
            .select_ready_ticket(trigger.project_id.as_deref(), &trigger.id, now_ms)
            .ok()
            .flatten(),
    }
}

fn bound_flow_for_work_ticket(
    flows: &BTreeMap<String, Flow>,
    ticket: &WorkTicket,
) -> Result<Flow, String> {
    let flow_name = ticket
        .hints
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
        ProcessIdentity, RunState, index_projects, orphan_disposition, reconcile_tickets,
        renews_lease, worktree_cleanup_due,
    };
    use crate::db::Db;
    use crate::domain::ticket::TicketState;
    use crate::run_store::RunStore;
    use crate::work_state::local::LocalSqlite;

    #[test]
    fn only_runs_verified_alive_this_pass_renew_their_lease() {
        // A supervised run whose process is still identifiable renews.
        assert!(renews_lease(
            RunState::Running,
            true,
            ProcessIdentity::Matches
        ));
        // So does one whose identity cannot be disproved: recovery already
        // treats unverifiable as alive.
        assert!(renews_lease(
            RunState::Running,
            true,
            ProcessIdentity::Unverifiable
        ));
        // A failed identity check must leave the lease expiring, supervised or
        // not — that is the whole point of strict renewal.
        assert!(!renews_lease(
            RunState::Running,
            true,
            ProcessIdentity::GoneOrReused
        ));
        // A driving run has no agent process left; the driver walking it is
        // the liveness evidence, so supervision alone decides.
        assert!(renews_lease(
            RunState::Driving,
            true,
            ProcessIdentity::GoneOrReused
        ));
        assert!(!renews_lease(
            RunState::Driving,
            false,
            ProcessIdentity::GoneOrReused
        ));
        // Terminal runs hold no lease worth keeping truthful.
        for state in [RunState::Merged, RunState::Failed, RunState::Aborted] {
            assert!(!renews_lease(state, true, ProcessIdentity::Matches));
        }
    }

    #[test]
    fn cleanup_eligibility_uses_retention_and_excludes_live_runs() {
        assert!(!worktree_cleanup_due(1_000, 500, 1_499, false));
        assert!(worktree_cleanup_due(1_000, 500, 1_500, false));
        assert!(!worktree_cleanup_due(1_000, 500, 2_000, true));
        assert!(worktree_cleanup_due(i64::MIN, i64::MAX, 0, false));
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
        let db = Db::open(&root.path().join("sloop.db"), 1_000).unwrap();
        let local_work_state = LocalSqlite::from_db(db.clone());
        let run_store = RunStore::from_db(db);
        local_work_state
            .insert_local_project(
                "default",
                ".agents/sloop/projects/default.md",
                "Default",
                1_000,
            )
            .unwrap();
        let insert = |id: &str, file: &str, blocked_by: &[String]| {
            local_work_state
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
        let stamps = |local_work_state: &LocalSqlite| -> Vec<(String, Option<i64>)> {
            local_work_state
                .local_ticket_files()
                .unwrap()
                .into_iter()
                .map(|ticket| (ticket.id, ticket.missing_at_ms))
                .collect()
        };

        // First pass stamps the two tickets whose files are gone.
        reconcile_tickets(root.path(), &local_work_state, &run_store, 2_000, window).unwrap();
        assert_eq!(
            stamps(&local_work_state),
            vec![
                ("T1".into(), None),
                ("T2".into(), Some(2_000)),
                ("T3".into(), Some(2_000)),
                ("T4".into(), None),
            ]
        );

        // Within the window nothing is deleted and stamps keep their origin.
        reconcile_tickets(root.path(), &local_work_state, &run_store, 2_050, window).unwrap();
        assert_eq!(stamps(&local_work_state)[1], ("T2".into(), Some(2_000)));

        // Past the window the unreferenced orphan is deleted; T3 survives
        // because T4 still names it as a blocker.
        reconcile_tickets(root.path(), &local_work_state, &run_store, 2_100, window).unwrap();
        assert_eq!(
            stamps(&local_work_state),
            vec![
                ("T1".into(), None),
                ("T3".into(), Some(2_000)),
                ("T4".into(), None),
            ]
        );

        // The file coming back clears the stamp even after the window.
        fs::write(tickets.join("blocked-gone.md"), "# Returned\n").unwrap();
        reconcile_tickets(root.path(), &local_work_state, &run_store, 3_000, window).unwrap();
        assert_eq!(stamps(&local_work_state)[1], ("T3".into(), None));
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
        let local_work_state =
            LocalSqlite::from_db(Db::open(&root.path().join("sloop.db"), 1_000).unwrap());

        index_projects(
            root.path(),
            std::path::Path::new(".agents/sloop/projects"),
            &local_work_state,
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
