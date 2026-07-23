use std::fmt;
use std::path::{Path, PathBuf};

use rusqlite::TransactionBehavior;

pub use crate::db::SCHEMA_VERSION;
use crate::db::{Db, DbError};
use crate::domain::ticket::TicketState;
pub use crate::run_store::{
    ActiveRun, CooldownRecord, CooldownUpdate, EventRecord, EvidenceRecord, ProjectNote, RunRecord,
    RunState, RunTimeline, StageRecord,
};
pub(crate) use crate::run_store::{NeedsReviewBranch, RecoverableRun, WorktreeCleanupCandidate};
use crate::run_store::{RunStore, evidence, runs};
use crate::work_state::local::LocalSqlite;
pub use crate::work_state::local::{
    ActivationKind, ActivationState, LocalTicketFile, NewActivation, ProjectRecord,
    QueuedActivation, ReindexResult, ReindexStateChange, ReindexTicket, TicketCounts, TicketRecord,
};

impl RunState {
    /// Reads a state written by an older or newer binary. An unrecognized
    /// value is an error rather than a fallback: silently treating it as
    /// nonterminal would let the daemon act on a run it cannot classify.
    pub fn parse(value: &str) -> Result<Self, StoreError> {
        Self::from_stored(value).ok_or_else(|| StoreError::UnknownRunState {
            state: value.into(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimRequest<'a> {
    pub ticket_id: &'a str,
    pub run_id: &'a str,
    pub activation_id: &'a str,
    pub owner_id: &'a str,
    pub lease_ms: i64,
    pub next_activation_eligible_at_ms: Option<i64>,
    pub flow_json: &'a str,
    pub ticket_json: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedRun {
    pub run_id: String,
    pub attempt: i64,
    pub lease_expires_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectCommitEvidence {
    pub run_id: String,
    pub ticket_id: String,
    pub data_json: String,
}

pub struct Store {
    db: Db,
}

impl Store {
    /// Opens (creating if needed) the database and migrates it to the current
    /// schema version. The daemon is the only writer; `now_ms` is injected so
    /// decision-adjacent timestamps never read the wall clock here.
    pub fn open(path: &Path, now_ms: i64) -> Result<Self, StoreError> {
        Db::open(path, now_ms)
            .map(Self::from_db)
            .map_err(StoreError::from)
    }

    pub fn from_db(db: Db) -> Self {
        Self { db }
    }

    pub fn db(&self) -> Db {
        self.db.clone()
    }

    fn run_store(&self) -> RunStore {
        RunStore::from_db(self.db.clone())
    }

    fn local_sqlite(&self) -> LocalSqlite {
        LocalSqlite::from_db(self.db.clone())
    }

    pub fn insert_local_project(
        &self,
        id: &str,
        file_path: &str,
        title: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.local_sqlite()
            .insert_local_project(id, file_path, title, now_ms)
    }

    /// Inserts or refreshes a project indexed from a committed file. Startup
    /// and reindex call this for every configured project file, so it must tolerate
    /// rows that already exist.
    pub fn upsert_local_project(
        &self,
        id: &str,
        file_path: &str,
        title: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.local_sqlite()
            .upsert_local_project(id, file_path, title, now_ms)
    }

    pub fn project_exists(&self, id: &str) -> Result<bool, StoreError> {
        self.local_sqlite().project_exists(id)
    }

    pub fn project(&self, id: &str) -> Result<Option<ProjectRecord>, StoreError> {
        self.local_sqlite().project(id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_local_ticket(
        &self,
        id: &str,
        project_id: &str,
        file_path: &str,
        name: &str,
        blocked_by: &[String],
        worktree: &str,
        target: Option<&str>,
        model: Option<&str>,
        effort: Option<&str>,
        flow: &str,
        state: TicketState,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.local_sqlite().insert_local_ticket(
            id, project_id, file_path, name, blocked_by, worktree, target, model, effort, flow,
            state, now_ms,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_local_ticket(
        &self,
        id: &str,
        name: &str,
        blocked_by: &[String],
        worktree: &str,
        target: Option<&str>,
        model: Option<&str>,
        effort: Option<&str>,
        flow: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.local_sqlite().update_local_ticket(
            id, name, blocked_by, worktree, target, model, effort, flow, now_ms,
        )
    }

    /// Applies a complete authored ticket snapshot without disturbing runtime
    /// history for IDs that remain present. Missing local rows and everything
    /// that depends on them are removed explicitly so the operation can report
    /// how much non-derivable state was discarded.
    pub fn apply_reindex(
        &self,
        project_ids: &[String],
        tickets: &[ReindexTicket],
        now_ms: i64,
    ) -> Result<ReindexResult, StoreError> {
        crate::coordination::apply_reindex(self.db(), project_ids, tickets, now_ms)
    }

    pub fn update_ticket_execution(
        &self,
        id: &str,
        target: Option<&str>,
        model: Option<&str>,
        effort: Option<&str>,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.local_sqlite()
            .update_ticket_execution(id, target, model, effort, now_ms)
    }

    pub fn update_ticket_body(&self, id: &str, body: &str, now_ms: i64) -> Result<(), StoreError> {
        self.local_sqlite().update_ticket_body(id, body, now_ms)
    }

    /// Version-two rows predate target snapshots. Once a repository has a
    /// target configuration, persist its default before dispatch can observe
    /// those rows.
    pub fn backfill_ticket_targets(
        &self,
        default_target: &str,
        now_ms: i64,
    ) -> Result<usize, StoreError> {
        self.local_sqlite()
            .backfill_ticket_targets(default_target, now_ms)
    }

    pub fn ticket(&self, id: &str) -> Result<Option<TicketRecord>, StoreError> {
        self.local_sqlite().ticket(id)
    }

    /// Resolves a ticket by its human-facing name. Names are not guaranteed
    /// unique across projects, so the lowest id wins deterministically; `show`
    /// tries this only after an exact id match fails.
    pub fn ticket_by_name(&self, name: &str) -> Result<Option<TicketRecord>, StoreError> {
        self.local_sqlite().ticket_by_name(name)
    }

    pub fn ticket_by_file(&self, file_path: &str) -> Result<Option<TicketRecord>, StoreError> {
        self.local_sqlite().ticket_by_file(file_path)
    }

    pub fn ticket_by_source_ref(
        &self,
        source: &str,
        source_ref: &str,
    ) -> Result<Option<TicketRecord>, StoreError> {
        self.local_sqlite().ticket_by_source_ref(source, source_ref)
    }

    /// Every ticket, newest registration first. `sloop list` answers "what is
    /// going on right now?", so recency leads; SQL settles the coarse order and
    /// a stable pass re-breaks ties on the id's numeric ordinal, which string
    /// comparison gets wrong (`TICK-9` sorts above `TICK-38`). Ids with no
    /// ordinal keep the deterministic `id DESC` order SQL gave them.
    pub fn tickets(&self) -> Result<Vec<TicketRecord>, StoreError> {
        self.local_sqlite().tickets()
    }

    pub fn tickets_for_project(&self, project_id: &str) -> Result<Vec<TicketRecord>, StoreError> {
        self.local_sqlite().tickets_for_project(project_id)
    }

    pub fn ticket_dependencies(
        &self,
    ) -> Result<std::collections::BTreeMap<String, Vec<String>>, StoreError> {
        self.local_sqlite().ticket_dependencies()
    }

    pub fn ticket_ids(&self) -> Result<Vec<String>, StoreError> {
        self.local_sqlite().ticket_ids()
    }

    pub fn local_ticket_files(&self) -> Result<Vec<LocalTicketFile>, StoreError> {
        self.local_sqlite().local_ticket_files()
    }

    /// Whether run history, a lease, an activation, or another ticket's
    /// blocker list still points at this row; deleting it would then violate
    /// a foreign key or orphan run evidence.
    pub fn ticket_is_referenced(&self, id: &str) -> Result<bool, StoreError> {
        crate::coordination::ticket_is_referenced(self.db(), id)
    }

    pub fn delete_ticket(&self, id: &str) -> Result<(), StoreError> {
        self.local_sqlite().delete_ticket(id)
    }

    /// Stamps a ticket whose committed file has disappeared. The stamp keeps
    /// the row out of selection without disturbing its state; an existing
    /// stamp is preserved so the deletion clock starts at the first pass.
    pub fn mark_ticket_missing(&self, id: &str, now_ms: i64) -> Result<(), StoreError> {
        self.local_sqlite().mark_ticket_missing(id, now_ms)
    }

    pub fn clear_ticket_missing(&self, id: &str, now_ms: i64) -> Result<(), StoreError> {
        self.local_sqlite().clear_ticket_missing(id, now_ms)
    }

    pub fn ticket_state(&self, id: &str) -> Result<Option<String>, StoreError> {
        self.local_sqlite().ticket_state(id)
    }

    /// Applies the operator-controlled ready/held side-state transition. The
    /// conditional update prevents an override from stealing a live claim or
    /// rewriting an evidence-derived outcome.
    pub fn set_ticket_hold(
        &self,
        id: &str,
        state: TicketState,
        now_ms: i64,
    ) -> Result<String, StoreError> {
        self.local_sqlite().set_ticket_hold(id, state, now_ms)
    }

    /// Returns a failed ticket to the ready queue and starts its attempt
    /// counter over. Other states remain evidence-derived and immutable here.
    pub fn retry_ticket(&self, id: &str, now_ms: i64) -> Result<String, StoreError> {
        crate::coordination::retry_ticket(self.db(), id, now_ms)
    }

    pub fn insert_activation(
        &self,
        activation: &NewActivation<'_>,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.local_sqlite().insert_activation(activation, now_ms)
    }

    pub fn insert_activation_filter(
        &self,
        activation_id: &str,
        ticket_id: &str,
    ) -> Result<(), StoreError> {
        self.local_sqlite()
            .insert_activation_filter(activation_id, ticket_id)
    }

    pub fn queued_activations(&self) -> Result<Vec<QueuedActivation>, StoreError> {
        self.local_sqlite().queued_activations()
    }

    /// Queued activations whose time gate is open, oldest first.
    pub fn dispatchable_activations(
        &self,
        now_ms: i64,
    ) -> Result<Vec<QueuedActivation>, StoreError> {
        self.local_sqlite().dispatchable_activations(now_ms)
    }

    pub fn next_activation_eligible_at_ms(&self, now_ms: i64) -> Result<Option<i64>, StoreError> {
        self.local_sqlite().next_activation_eligible_at_ms(now_ms)
    }

    /// Deterministic ready-work selection within an activation's scope:
    /// oldest registration first, ticket ID as the tiebreak. `--only` filters
    /// apply when the activation has filter rows.
    pub fn select_ready_ticket(
        &self,
        activation: &QueuedActivation,
        now_ms: i64,
    ) -> Result<Option<String>, StoreError> {
        self.local_sqlite().select_ready_ticket(
            activation.project_id.as_deref(),
            &activation.id,
            now_ms,
        )
    }

    pub fn ticket_is_dispatchable(&self, ticket_id: &str) -> Result<bool, StoreError> {
        self.local_sqlite().ticket_is_dispatchable(ticket_id)
    }

    pub fn unmerged_blockers(&self, ticket_id: &str) -> Result<Vec<String>, StoreError> {
        self.local_sqlite().unmerged_blockers(ticket_id)
    }

    /// Records one completed flow stage. The flow index is the idempotency
    /// key, so recovery can re-derive the first stage still lacking a verdict.
    pub(crate) fn record_aftercare_stage(
        &self,
        run_id: &str,
        stage: &StageRecord,
    ) -> Result<(), StoreError> {
        self.run_store()
            .record_aftercare_stage(run_id, stage)
            .map_err(StoreError::from)
    }

    pub(crate) fn aftercare_stages(&self, run_id: &str) -> Result<Vec<StageRecord>, StoreError> {
        self.run_store()
            .aftercare_stages(run_id)
            .map_err(StoreError::from)
    }

    /// Checkpoints the agent's exit before aftercare starts. The lease and
    /// ticket remain claimed until final settlement, but recovery can now
    /// resume with the exact exit and branch-activity facts. Only the caller that
    /// wins this transition owns exit processing and aftercare for the run.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_agent_exit(
        &mut self,
        run_id: &str,
        exit_code: Option<i32>,
        capture_complete: bool,
        commits_json: &str,
        vendor_error: Option<&crate::vendor_error::VendorErrorMatch>,
        cooldown_until_ms: Option<i64>,
        now_ms: i64,
    ) -> Result<ExitClaim, StoreError> {
        let mut connection = self.db.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = runs::tx::claim_agent_exit(&transaction, run_id, exit_code, now_ms)?;
        if changed == 0 {
            let state = runs::tx::state(&transaction, run_id)?;
            return match state {
                Some(state) => Ok(ExitClaim::AlreadyClaimed { state }),
                None => Err(StoreError::RunNotFound {
                    run_id: run_id.into(),
                }),
            };
        }
        evidence::tx::record_agent_exit(
            &transaction,
            run_id,
            exit_code,
            capture_complete,
            commits_json,
            vendor_error,
            cooldown_until_ms,
            now_ms,
        )?;
        transaction.commit()?;
        Ok(ExitClaim::Claimed)
    }

    pub(crate) fn record_aftercare_evidence(
        &self,
        run_id: &str,
        kind: &str,
        data_json: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.run_store()
            .record_aftercare_evidence(run_id, kind, data_json, now_ms)
            .map_err(StoreError::from)
    }

    pub(crate) fn clear_aftercare_process(&self, run_id: &str) -> Result<(), StoreError> {
        self.run_store()
            .clear_aftercare_process(run_id)
            .map_err(StoreError::from)
    }

    /// Durably records an operator's cancellation intent, idempotently: the
    /// dedupe key makes a repeated `cancel` a no-op rather than new evidence.
    pub fn record_cancel_requested(&self, run_id: &str, now_ms: i64) -> Result<(), StoreError> {
        self.run_store()
            .record_cancel_requested(run_id, now_ms)
            .map_err(StoreError::from)
    }

    /// Whether cancellation intent was recorded for the run, so an exit event
    /// racing the cancel still resolves to `Cancelled`.
    pub fn cancellation_requested(&self, run_id: &str) -> Result<bool, StoreError> {
        self.run_store()
            .cancellation_requested(run_id)
            .map_err(StoreError::from)
    }

    /// Appends a worker's advisory note. The agent's only write: it records
    /// text against the run and moves nothing.
    pub fn insert_note(
        &self,
        id: &str,
        run_id: &str,
        text: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.run_store()
            .insert_note(id, run_id, text, now_ms)
            .map_err(StoreError::from)
    }

    /// Notes recorded against one run, in the order they arrived.
    pub fn notes_for_run(&self, run_id: &str) -> Result<Vec<String>, StoreError> {
        self.run_store()
            .notes_for_run(run_id)
            .map_err(StoreError::from)
    }

    /// Records the first worker-reported verdict for one stage. The unique
    /// dedupe key is the at-most-once gate; later reports cannot overwrite it.
    pub(crate) fn record_stage_verdict(
        &self,
        run_id: &str,
        stage: &str,
        verdict: &str,
        reason: Option<&str>,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        self.run_store()
            .record_stage_verdict(run_id, stage, verdict, reason, now_ms)
            .map_err(StoreError::from)
    }

    pub fn notes_for_project(&self, project_id: &str) -> Result<Vec<ProjectNote>, StoreError> {
        self.run_store()
            .notes_for_project(project_id)
            .map_err(StoreError::from)
    }

    pub fn commit_evidence_for_project(
        &self,
        project_id: &str,
    ) -> Result<Vec<ProjectCommitEvidence>, StoreError> {
        self.run_store()
            .commit_evidence_for_project(project_id)?
            .into_iter()
            .map(|(run_id, ticket_id, data_json)| {
                Ok(ProjectCommitEvidence {
                    run_id,
                    ticket_id,
                    data_json,
                })
            })
            .collect()
    }

    pub fn next_note_ordinal(&self) -> Result<i64, StoreError> {
        self.run_store()
            .next_note_ordinal()
            .map_err(StoreError::from)
    }

    /// Evidence rows for one run in observation order, as (kind, data_json).
    pub fn run_evidence(&self, run_id: &str) -> Result<Vec<(String, String)>, StoreError> {
        self.run_store()
            .run_evidence(run_id)
            .map_err(StoreError::from)
    }

    pub fn vendor_error_for_run(
        &self,
        run_id: &str,
    ) -> Result<Option<crate::vendor_error::VendorErrorMatch>, StoreError> {
        let data = self.run_store().vendor_error_for_run(run_id)?;
        Ok(data.and_then(|data| serde_json::from_str(&data).ok()))
    }

    pub fn latest_vendor_error_for_ticket(
        &self,
        ticket_id: &str,
    ) -> Result<Option<crate::vendor_error::VendorErrorMatch>, StoreError> {
        let data = self.run_store().latest_vendor_error_for_ticket(ticket_id)?;
        Ok(data.and_then(|data| serde_json::from_str(&data).ok()))
    }

    /// Reads activity-feed rows with `sequence > after`, oldest first. The
    /// last row's sequence is the caller's next cursor.
    pub fn events_after(&self, after: i64, limit: usize) -> Result<Vec<EventRecord>, StoreError> {
        self.run_store()
            .events_after(after, limit)
            .map_err(StoreError::from)
    }

    /// The claim/start/finish instants of each named run, read back out of the
    /// activity feed. The feed is written in the same transaction as the
    /// transition it narrates, so these are the authoritative wall-clock
    /// boundaries of a run — nothing extra is stored to render them.
    ///
    /// A run with no finish row is still in flight; callers render it
    /// open-ended rather than inventing an end. `run_aborted` counts as a
    /// finish because it is the terminal row for runs that never settled.
    pub fn run_timelines(
        &self,
        run_ids: &[&str],
    ) -> Result<std::collections::HashMap<String, RunTimeline>, StoreError> {
        self.run_store()
            .run_timelines(run_ids)
            .map_err(StoreError::from)
    }

    pub fn latest_event_sequence(&self) -> Result<i64, StoreError> {
        self.run_store()
            .latest_event_sequence()
            .map_err(StoreError::from)
    }

    /// Drops all but the newest `keep` activity-feed rows. Sequences are never
    /// reused after a trim, so cursors held by watchers stay valid.
    pub fn trim_events(&self, keep: i64) -> Result<(), StoreError> {
        self.run_store().trim_events(keep).map_err(StoreError::from)
    }

    pub fn run(&self, id: &str) -> Result<Option<RunRecord>, StoreError> {
        self.run_store().run(id).map_err(StoreError::from)
    }

    /// The run a `<ticket>-r<attempt>` alias names. The pair is unique because
    /// attempts are allocated once per ticket at claim time.
    pub fn run_for_ticket_attempt(
        &self,
        ticket_id: &str,
        attempt: i64,
    ) -> Result<Option<RunRecord>, StoreError> {
        self.run_store()
            .run_for_ticket_attempt(ticket_id, attempt)
            .map_err(StoreError::from)
    }

    /// Every run a ticket has produced, newest attempt first, so a bare ticket
    /// reference can name the latest run and still report the earlier ones.
    pub fn runs_for_ticket(&self, ticket_id: &str) -> Result<Vec<RunRecord>, StoreError> {
        self.run_store()
            .runs_for_ticket(ticket_id)
            .map_err(StoreError::from)
    }

    /// Runs whose internal id starts with `prefix`. More than one row means the
    /// reference is ambiguous, so the caller needs the candidates, not a pick.
    pub fn runs_with_id_prefix(&self, prefix: &str) -> Result<Vec<RunRecord>, StoreError> {
        // `LIKE` would treat `%` and `_` in a prefix as wildcards; run ids are
        // hexadecimal, but comparing on the substring keeps that beyond doubt.
        self.run_store()
            .runs_with_id_prefix(prefix)
            .map_err(StoreError::from)
    }

    /// Every `needs_review` ticket paired with the branch of the run that
    /// produced it, so the daemon can freshly test each branch tip for external
    /// integration. Only the newest `needs_review` run with a branch is
    /// returned per ticket; the tip itself is never cached here.
    pub(crate) fn needs_review_branches(&self) -> Result<Vec<NeedsReviewBranch>, StoreError> {
        self.run_store()
            .needs_review_branches()
            .map_err(StoreError::from)
    }

    pub(crate) fn worktree_cleanup_candidates(
        &self,
    ) -> Result<Vec<WorktreeCleanupCandidate>, StoreError> {
        self.run_store()
            .worktree_cleanup_candidates()
            .map_err(StoreError::from)
    }

    pub(crate) fn next_worktree_cleanup_at_ms(
        &self,
        retention_ms: i64,
        now_ms: i64,
    ) -> Result<Option<i64>, StoreError> {
        self.run_store()
            .next_worktree_cleanup_at_ms(retention_ms, now_ms)
            .map_err(StoreError::from)
    }

    pub(crate) fn mark_run_worktree_cleaned(
        &self,
        candidate: &WorktreeCleanupCandidate,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        self.run_store()
            .mark_run_worktree_cleaned(candidate, now_ms)
            .map_err(StoreError::from)
    }

    /// The ticket's live run as `(id, attempt)`. The attempt travels with the
    /// id so callers can name the run by alias without re-reading the row.
    pub fn active_run_for_ticket(
        &self,
        ticket_id: &str,
    ) -> Result<Option<(String, i64)>, StoreError> {
        self.run_store()
            .active_run_for_ticket(ticket_id)
            .map_err(StoreError::from)
    }

    /// Leased nonterminal runs that consume capacity, oldest first.
    pub fn active_runs(&self) -> Result<Vec<ActiveRun>, StoreError> {
        self.run_store().active_runs().map_err(StoreError::from)
    }

    /// Every nonterminal run that still owns a lease, oldest first. Startup
    /// must classify all of these before making another spawn decision.
    pub(crate) fn recoverable_runs(&self) -> Result<Vec<RecoverableRun>, StoreError> {
        self.run_store()
            .recoverable_runs()
            .map_err(StoreError::from)
    }

    /// Finds a still-queued activation of `kind` scoped to one ticket, used
    /// to keep reposting the same file idempotent.
    pub fn queued_ticket_activation(
        &self,
        ticket_id: &str,
        kind: ActivationKind,
    ) -> Result<Option<String>, StoreError> {
        self.local_sqlite()
            .queued_ticket_activation(ticket_id, kind)
    }

    /// Moves a still-queued timed activation to a new eligibility instant,
    /// so reposting a ticket with a different `--at` time reschedules the
    /// existing activation instead of queueing a duplicate.
    pub fn reschedule_activation(
        &self,
        id: &str,
        eligible_at_ms: i64,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.local_sqlite()
            .reschedule_activation(id, eligible_at_ms, now_ms)
    }

    /// Reserves the next activation ordinal without reusing IDs removed by
    /// reindex.
    pub fn next_activation_ordinal(&self) -> Result<i64, StoreError> {
        self.local_sqlite().next_activation_ordinal()
    }

    /// Re-arms the lease of a run this daemon has just adopted, returning the
    /// new expiry. Unlike [`Store::renew_lease`] this accepts an already
    /// expired lease: a daemon down longer than the TTL comes back to leases
    /// that lapsed while nobody was renewing them, and ordinary renewal could
    /// never lift them again. It is not a weaker renewal — the guard moves
    /// from the clock to the run itself, so only a run that has not settled
    /// can be re-armed, and a dead run's lease stays expired because recovery
    /// settles it instead of adopting it.
    pub(crate) fn readopt_lease(
        &mut self,
        ticket_id: &str,
        run_id: &str,
        lease_ms: i64,
        now_ms: i64,
    ) -> Result<i64, StoreError> {
        self.local_sqlite()
            .readopt_lease(ticket_id, run_id, lease_ms, now_ms)
    }

    /// Renews the lease that `run_id` holds on `ticket_id`, returning the new
    /// expiry. Renewal is strict: an expired lease cannot be renewed, so once
    /// recovery treats expiry as "run is lost" a revived run can never
    /// resurrect a lease that recovery may be reclaiming. Re-arming an expired
    /// lease is a separate, adoption-only verb ([`Store::readopt_lease`]).
    pub(crate) fn renew_lease(
        &mut self,
        ticket_id: &str,
        run_id: &str,
        lease_ms: i64,
        now_ms: i64,
    ) -> Result<i64, StoreError> {
        self.local_sqlite()
            .renew_lease(ticket_id, run_id, lease_ms, now_ms)
    }

    pub fn paused(&self) -> Result<bool, StoreError> {
        self.run_store().paused().map_err(StoreError::from)
    }

    pub fn clear_restart_draining(&self, now_ms: i64) -> Result<(), StoreError> {
        self.run_store()
            .clear_restart_draining(now_ms)
            .map_err(StoreError::from)
    }

    pub fn restart_draining(&self) -> Result<bool, StoreError> {
        self.run_store()
            .restart_draining()
            .map_err(StoreError::from)
    }

    pub fn active_cooldown_for_target(
        &self,
        target: &str,
        now_ms: i64,
    ) -> Result<Option<CooldownRecord>, StoreError> {
        self.run_store()
            .active_cooldown_for_target(target, now_ms)
            .map_err(StoreError::from)
    }

    /// Number of runs currently holding a durable lease. Used as the capacity
    /// gate for repair spawns, which run inside an already-leased run.
    pub(crate) fn active_lease_count(&self) -> Result<usize, StoreError> {
        self.local_sqlite().active_lease_count()
    }

    /// Persists one repair attempt for a stage. The dedupe key is per
    /// (run, stage, attempt), so recovery counts consumed attempts without
    /// repeating or losing one, and the retry verdict can be filled in later
    /// by upserting the same key.
    pub(crate) fn record_repair_attempt(
        &self,
        run_id: &str,
        stage: &str,
        attempt: u32,
        data_json: &str,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.run_store()
            .record_repair_attempt(run_id, stage, attempt, data_json, now_ms)
            .map_err(StoreError::from)
    }

    pub fn active_cooldowns(&self, now_ms: i64) -> Result<Vec<CooldownRecord>, StoreError> {
        self.run_store()
            .active_cooldowns(now_ms)
            .map_err(StoreError::from)
    }

    pub fn next_active_cooldown(&self, now_ms: i64) -> Result<Option<i64>, StoreError> {
        self.run_store()
            .next_active_cooldown(now_ms)
            .map_err(StoreError::from)
    }

    pub fn set_paused(&self, paused: bool, now_ms: i64) -> Result<(), StoreError> {
        self.run_store()
            .set_paused(paused, now_ms)
            .map_err(StoreError::from)
    }

    pub fn begin_restart_draining(
        &mut self,
        active_runs: usize,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        self.run_store()
            .begin_restart_draining(active_runs, now_ms)
            .map_err(StoreError::from)
    }

    /// Resuming cancels both scheduler holds in one durable transition.
    pub fn resume_scheduler(&mut self, now_ms: i64) -> Result<bool, StoreError> {
        self.run_store()
            .resume_scheduler(now_ms)
            .map_err(StoreError::from)
    }

    /// Performs a small committed write used to detect when SQLite can make
    /// progress again after returning `SQLITE_FULL`.
    pub(crate) fn probe_writable(&self, now_ms: i64) -> Result<(), StoreError> {
        self.run_store()
            .probe_writable(now_ms)
            .map_err(StoreError::from)
    }

    pub fn ticket_counts(&self) -> Result<TicketCounts, StoreError> {
        self.local_sqlite().ticket_counts()
    }
}

/// Whether the caller won the `running` → `aftercare` transition and with it
/// ownership of exit processing and aftercare for the run.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ExitClaim {
    Claimed,
    AlreadyClaimed { state: String },
}

#[derive(Debug)]
pub enum StoreError {
    Open {
        path: PathBuf,
        source: rusqlite::Error,
    },
    Sqlite(rusqlite::Error),
    UnsupportedSchemaVersion(u32),
    TicketNotReady {
        ticket_id: String,
        state: Option<String>,
    },
    TicketNotFound {
        ticket_id: String,
    },
    TicketStateConflict {
        ticket_id: String,
        state: String,
        requested: String,
    },
    ActivationNotQueued {
        activation_id: String,
    },
    LeaseNotHeld {
        ticket_id: String,
        run_id: String,
    },
    RunNotFound {
        run_id: String,
    },
    RunStateConflict {
        run_id: String,
        state: Option<String>,
        requested: String,
    },
    UnknownRunState {
        state: String,
    },
}

impl StoreError {
    pub(crate) fn is_disk_full(&self) -> bool {
        let source = match self {
            Self::Open { source, .. } | Self::Sqlite(source) => source,
            _ => return false,
        };
        matches!(
            source,
            rusqlite::Error::SqliteFailure(error, _)
                if error.code == rusqlite::ffi::ErrorCode::DiskFull
        )
    }
}

impl From<rusqlite::Error> for StoreError {
    fn from(source: rusqlite::Error) -> Self {
        Self::Sqlite(source)
    }
}

impl From<DbError> for StoreError {
    fn from(source: DbError) -> Self {
        match source {
            DbError::Open { path, source } => Self::Open { path, source },
            DbError::Sqlite(source) => Self::Sqlite(source),
            DbError::UnsupportedSchemaVersion(version) => Self::UnsupportedSchemaVersion(version),
        }
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open { path, source } => {
                write!(formatter, "cannot open {}: {source}", path.display())
            }
            Self::Sqlite(source) => write!(formatter, "database error: {source}"),
            Self::UnsupportedSchemaVersion(version) => {
                write!(formatter, "unsupported database schema version {version}")
            }
            Self::TicketNotReady { ticket_id, state } => match state {
                Some(state) => write!(formatter, "ticket `{ticket_id}` is `{state}`, not `ready`"),
                None => write!(formatter, "ticket `{ticket_id}` does not exist"),
            },
            Self::TicketNotFound { ticket_id } => {
                write!(formatter, "ticket `{ticket_id}` does not exist")
            }
            Self::TicketStateConflict {
                ticket_id,
                state,
                requested,
            } => write!(
                formatter,
                "ticket `{ticket_id}` is `{state}` and cannot be changed to `{requested}`"
            ),
            Self::ActivationNotQueued { activation_id } => write!(
                formatter,
                "activation `{activation_id}` is not queued for dispatch"
            ),
            Self::LeaseNotHeld { ticket_id, run_id } => write!(
                formatter,
                "run `{run_id}` does not hold the lease on ticket `{ticket_id}`"
            ),
            Self::RunNotFound { run_id } => write!(formatter, "run `{run_id}` does not exist"),
            Self::RunStateConflict {
                run_id,
                state,
                requested,
            } => match state {
                Some(state) => write!(
                    formatter,
                    "run `{run_id}` is `{state}` and cannot be changed to `{requested}`"
                ),
                None => write!(formatter, "run `{run_id}` does not exist"),
            },
            Self::UnknownRunState { state } => {
                write!(formatter, "unrecognized run state `{state}`")
            }
        }
    }
}

impl std::error::Error for StoreError {}

#[cfg(test)]
mod tests {
    use super::StoreError;

    #[test]
    fn sqlite_full_errors_are_classified_for_backpressure() {
        let sqlite = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_FULL),
            None,
        );
        assert!(StoreError::from(sqlite).is_disk_full());
        assert!(
            !StoreError::TicketNotFound {
                ticket_id: "T1".into()
            }
            .is_disk_full()
        );
    }
}
