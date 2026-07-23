use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::RunStore;

/// Every value the `runs.state` column can hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    Claimed,
    Running,
    Aftercare,
    /// A claim rolled back before a process existed.
    Aborted,
    Merged,
    Failed,
    NeedsReview,
    Cancelled,
    RateLimited,
    Orphaned,
}

const NONTERMINAL_RUN_STATES: [RunState; 3] =
    [RunState::Claimed, RunState::Running, RunState::Aftercare];

impl RunState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Claimed => "claimed",
            Self::Running => "running",
            Self::Aftercare => "aftercare",
            Self::Aborted => "aborted",
            Self::Merged => "merged",
            Self::Failed => "failed",
            Self::NeedsReview => "needs_review",
            Self::Cancelled => "cancelled",
            Self::RateLimited => "rate_limited",
            Self::Orphaned => "orphaned",
        }
    }

    pub(crate) fn from_stored(value: &str) -> Option<Self> {
        match value {
            "claimed" => Some(Self::Claimed),
            "running" => Some(Self::Running),
            "aftercare" => Some(Self::Aftercare),
            "aborted" => Some(Self::Aborted),
            "merged" => Some(Self::Merged),
            "failed" => Some(Self::Failed),
            "needs_review" => Some(Self::NeedsReview),
            "cancelled" => Some(Self::Cancelled),
            "rate_limited" => Some(Self::RateLimited),
            "orphaned" => Some(Self::Orphaned),
            _ => None,
        }
    }

    pub fn is_terminal(self) -> bool {
        !NONTERMINAL_RUN_STATES.contains(&self)
    }
}

impl rusqlite::types::FromSql for RunState {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let text = value.as_str()?;
        Self::from_stored(text).ok_or_else(|| {
            rusqlite::types::FromSqlError::Other(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unrecognized run state `{text}`"),
            )))
        })
    }
}

impl From<crate::outcome::Outcome> for RunState {
    fn from(outcome: crate::outcome::Outcome) -> Self {
        use crate::outcome::Outcome;
        match outcome {
            Outcome::Merged => Self::Merged,
            Outcome::Failed => Self::Failed,
            Outcome::NeedsReview => Self::NeedsReview,
            Outcome::Cancelled => Self::Cancelled,
            Outcome::RateLimited => Self::RateLimited,
            Outcome::Orphaned => Self::Orphaned,
        }
    }
}

fn nonterminal_state_params() -> [&'static str; 3] {
    [
        NONTERMINAL_RUN_STATES[0].as_str(),
        NONTERMINAL_RUN_STATES[1].as_str(),
        NONTERMINAL_RUN_STATES[2].as_str(),
    ]
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveRun {
    pub id: String,
    pub ticket_id: String,
    pub attempt: i64,
    pub ticket_name: String,
    pub project_id: String,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRecord {
    pub id: String,
    pub ticket_id: String,
    pub attempt: i64,
    pub state: String,
    pub branch: Option<String>,
    pub worktree_path: Option<String>,
    pub pid: Option<i64>,
    pub pid_start_time: Option<i64>,
    pub process_group_id: Option<i64>,
    pub exit_code: Option<i64>,
    pub exited_at_ms: Option<i64>,
    pub flow_json: Option<String>,
    pub ticket_json: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NeedsReviewBranch {
    pub(crate) ticket_id: String,
    pub(crate) run_id: String,
    pub(crate) branch: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorktreeCleanupCandidate {
    pub(crate) run_id: String,
    pub(crate) ticket_id: String,
    pub(crate) branch: String,
    pub(crate) worktree_path: String,
    pub(crate) cleanup_eligible_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecoverableRun {
    pub(crate) id: String,
    pub(crate) ticket_id: String,
    pub(crate) target: String,
    pub(crate) state: RunState,
    pub(crate) branch: Option<String>,
    pub(crate) worktree_path: Option<String>,
    pub(crate) pid: Option<i64>,
    pub(crate) pid_start_time: Option<i64>,
    pub(crate) process_group_id: Option<i64>,
    pub(crate) worker_token: Option<String>,
    pub(crate) worker_socket_path: Option<String>,
    pub(crate) exit_code: Option<i64>,
    pub(crate) lease_expires_at_ms: i64,
    pub(crate) flow_json: Option<String>,
    pub(crate) ticket_json: Option<String>,
}

const RUN_RECORD_SELECT: &str = "SELECT id, ticket_id, attempt, state, branch, worktree_path, pid,
            pid_start_time, process_group_id, exit_code, exited_at_ms,
            flow_json, ticket_json
     FROM runs";

fn run_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<RunRecord> {
    Ok(RunRecord {
        id: row.get(0)?,
        ticket_id: row.get(1)?,
        attempt: row.get(2)?,
        state: row.get(3)?,
        branch: row.get(4)?,
        worktree_path: row.get(5)?,
        pid: row.get(6)?,
        pid_start_time: row.get(7)?,
        process_group_id: row.get(8)?,
        exit_code: row.get(9)?,
        exited_at_ms: row.get(10)?,
        flow_json: row.get(11)?,
        ticket_json: row.get(12)?,
    })
}

/// One row of the activity feed, ordered by `sequence`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRecord {
    pub sequence: i64,
    pub occurred_at_ms: i64,
    pub kind: String,
    pub run_id: Option<String>,
    pub ticket_id: Option<String>,
    pub data_json: String,
}

/// One run's wall-clock boundaries, derived from the activity feed. Every
/// field is optional because a run is observable at each stage of its life:
/// claimed but not started, started but not finished.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RunTimeline {
    pub claimed_at_ms: Option<i64>,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectNote {
    pub id: String,
    pub run_id: String,
    pub ticket_id: String,
    pub text: String,
    pub recorded_at_ms: i64,
}

pub(crate) mod tx {
    use rusqlite::{Transaction, params};

    use super::{RunState, WorktreeCleanupCandidate};

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn mark_running(
        transaction: &Transaction<'_>,
        run_id: &str,
        branch: &str,
        worktree_path: &str,
        pid: u32,
        pid_start_time: Option<i64>,
        process_group_id: u32,
        worker_token: &str,
        worker_socket_path: &str,
        now_ms: i64,
    ) -> rusqlite::Result<usize> {
        transaction.execute(
            "UPDATE runs
             SET state = ?10, branch = ?2, worktree_path = ?3, pid = ?4,
                 pid_start_time = ?5, process_group_id = ?6, worker_token = ?7,
                 worker_socket_path = ?8, started_at_ms = ?9, updated_at_ms = ?9
             WHERE id = ?1 AND state = ?11 AND exited_at_ms IS NULL",
            params![
                run_id,
                branch,
                worktree_path,
                i64::from(pid),
                pid_start_time,
                i64::from(process_group_id),
                worker_token,
                worker_socket_path,
                now_ms,
                RunState::Running.as_str(),
                RunState::Claimed.as_str(),
            ],
        )
    }

    pub(crate) fn state(
        transaction: &Transaction<'_>,
        run_id: &str,
    ) -> rusqlite::Result<Option<String>> {
        use rusqlite::OptionalExtension;

        transaction
            .query_row(
                "SELECT state FROM runs WHERE id = ?1",
                params![run_id],
                |row| row.get(0),
            )
            .optional()
    }

    pub(crate) fn ticket_id(
        transaction: &Transaction<'_>,
        run_id: &str,
    ) -> rusqlite::Result<String> {
        transaction.query_row(
            "SELECT ticket_id FROM runs WHERE id = ?1",
            params![run_id],
            |row| row.get(0),
        )
    }

    pub(crate) fn finish(
        transaction: &Transaction<'_>,
        run_id: &str,
        state: RunState,
        exit_code: Option<i32>,
        now_ms: i64,
    ) -> rusqlite::Result<usize> {
        transaction.execute(
            "UPDATE runs
             SET state = ?2, exited_at_ms = ?3, exit_code = ?4, updated_at_ms = ?3,
                 cleanup_eligible_at_ms = CASE WHEN ?2 = ?5 THEN ?3 ELSE NULL END
             WHERE id = ?1 AND exited_at_ms IS NULL",
            params![
                run_id,
                state.as_str(),
                now_ms,
                exit_code,
                RunState::Merged.as_str(),
            ],
        )
    }

    pub(crate) fn state_and_exit(
        transaction: &Transaction<'_>,
        run_id: &str,
    ) -> rusqlite::Result<Option<(String, Option<i64>)>> {
        use rusqlite::OptionalExtension;

        transaction
            .query_row(
                "SELECT state, exited_at_ms FROM runs WHERE id = ?1",
                params![run_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
    }

    pub(crate) fn activation_id(
        transaction: &Transaction<'_>,
        run_id: &str,
    ) -> rusqlite::Result<String> {
        transaction.query_row(
            "SELECT activation_id FROM runs WHERE id = ?1",
            params![run_id],
            |row| row.get(0),
        )
    }

    pub(crate) fn abort(
        transaction: &Transaction<'_>,
        run_id: &str,
        now_ms: i64,
    ) -> rusqlite::Result<usize> {
        transaction.execute(
            "UPDATE runs
             SET state = ?3, exited_at_ms = ?2, updated_at_ms = ?2
             WHERE id = ?1 AND exited_at_ms IS NULL",
            params![run_id, now_ms, RunState::Aborted.as_str()],
        )
    }

    pub(crate) fn mark_cleanup_eligible(
        transaction: &Transaction<'_>,
        run_id: &str,
        now_ms: i64,
    ) -> rusqlite::Result<usize> {
        transaction.execute(
            "UPDATE runs SET cleanup_eligible_at_ms = ?2
             WHERE id = ?1 AND cleanup_eligible_at_ms IS NULL AND cleaned_at_ms IS NULL",
            params![run_id, now_ms],
        )
    }

    pub(crate) fn ids_for_ticket(
        transaction: &Transaction<'_>,
        ticket_id: &str,
    ) -> rusqlite::Result<Vec<String>> {
        let mut statement = transaction.prepare("SELECT id FROM runs WHERE ticket_id = ?1")?;
        statement
            .query_map(params![ticket_id], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()
    }

    pub(crate) fn ids_for_activation(
        transaction: &Transaction<'_>,
        activation_id: &str,
    ) -> rusqlite::Result<Vec<String>> {
        let mut statement = transaction.prepare("SELECT id FROM runs WHERE activation_id = ?1")?;
        statement
            .query_map(params![activation_id], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()
    }

    pub(crate) fn delete(transaction: &Transaction<'_>, run_id: &str) -> rusqlite::Result<usize> {
        transaction.execute("DELETE FROM runs WHERE id = ?1", params![run_id])
    }

    pub(crate) fn mark_ticket_runs_cleanup_eligible(
        transaction: &Transaction<'_>,
        ticket_id: &str,
        state: RunState,
        now_ms: i64,
    ) -> rusqlite::Result<usize> {
        transaction.execute(
            "UPDATE runs SET cleanup_eligible_at_ms = ?3
             WHERE ticket_id = ?1 AND state = ?2
               AND cleanup_eligible_at_ms IS NULL AND cleaned_at_ms IS NULL",
            params![ticket_id, state.as_str(), now_ms],
        )
    }

    pub(crate) fn mark_failed_or_review_runs_cleanup_eligible(
        transaction: &Transaction<'_>,
        ticket_id: &str,
        now_ms: i64,
    ) -> rusqlite::Result<usize> {
        transaction.execute(
            "UPDATE runs SET cleanup_eligible_at_ms = ?2
             WHERE ticket_id = ?1 AND state IN ('failed', 'needs_review')
               AND cleanup_eligible_at_ms IS NULL AND cleaned_at_ms IS NULL",
            params![ticket_id, now_ms],
        )
    }

    pub(crate) fn claim_agent_exit(
        transaction: &Transaction<'_>,
        run_id: &str,
        exit_code: Option<i32>,
        now_ms: i64,
    ) -> rusqlite::Result<usize> {
        transaction.execute(
            "UPDATE runs
             SET state = ?4, exit_code = ?2, updated_at_ms = ?3
             WHERE id = ?1 AND state = ?5 AND exited_at_ms IS NULL",
            params![
                run_id,
                exit_code,
                now_ms,
                RunState::Aftercare.as_str(),
                RunState::Running.as_str(),
            ],
        )
    }

    pub(crate) fn next_attempt(
        transaction: &Transaction<'_>,
        ticket_id: &str,
    ) -> rusqlite::Result<i64> {
        transaction.query_row(
            "SELECT COALESCE(MAX(attempt), 0) + 1 FROM runs WHERE ticket_id = ?1",
            params![ticket_id],
            |row| row.get(0),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn insert_claimed(
        transaction: &Transaction<'_>,
        run_id: &str,
        activation_id: &str,
        ticket_id: &str,
        attempt: i64,
        flow_json: &str,
        ticket_json: &str,
        now_ms: i64,
    ) -> rusqlite::Result<usize> {
        transaction.execute(
            "INSERT INTO runs
                 (id, activation_id, ticket_id, state, attempt, flow_json, ticket_json,
                  created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
            params![
                run_id,
                activation_id,
                ticket_id,
                RunState::Claimed.as_str(),
                attempt,
                flow_json,
                ticket_json,
                now_ms,
            ],
        )
    }

    /// Appends one activity-feed row inside the transaction performing the
    /// transition, so the event commits or rolls back with it.
    pub(crate) fn record_event(
        transaction: &Transaction<'_>,
        now_ms: i64,
        kind: &str,
        run_id: Option<&str>,
        ticket_id: Option<&str>,
        data_json: &str,
    ) -> rusqlite::Result<()> {
        transaction.execute(
            "INSERT INTO events (occurred_at_ms, kind, run_id, ticket_id, data_json)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![now_ms, kind, run_id, ticket_id, data_json],
        )?;
        Ok(())
    }

    pub(crate) fn insert_note(
        transaction: &Transaction<'_>,
        id: &str,
        run_id: &str,
        text: &str,
        now_ms: i64,
    ) -> rusqlite::Result<()> {
        transaction.execute(
            "INSERT INTO notes (id, run_id, text, recorded_at_ms)
             VALUES (?1, ?2, ?3, ?4)",
            params![id, run_id, text, now_ms],
        )?;
        Ok(())
    }

    pub(crate) fn delete_notes_for_run(
        transaction: &Transaction<'_>,
        run_id: &str,
    ) -> rusqlite::Result<usize> {
        transaction.execute("DELETE FROM notes WHERE run_id = ?1", params![run_id])
    }

    pub(crate) fn trim_events(transaction: &Transaction<'_>, keep: i64) -> rusqlite::Result<()> {
        transaction.execute(
            "DELETE FROM events
             WHERE sequence <= (SELECT COALESCE(MAX(sequence), 0) FROM events) - ?1",
            params![keep],
        )?;
        Ok(())
    }

    pub(crate) fn mark_worktree_cleaned(
        transaction: &Transaction<'_>,
        candidate: &WorktreeCleanupCandidate,
        now_ms: i64,
    ) -> rusqlite::Result<bool> {
        let changed = transaction.execute(
            "UPDATE runs SET cleaned_at_ms = ?2, updated_at_ms = ?2
             WHERE id = ?1 AND cleanup_eligible_at_ms IS NOT NULL
               AND cleaned_at_ms IS NULL
               AND NOT EXISTS (SELECT 1 FROM leases l WHERE l.run_id = runs.id)",
            params![candidate.run_id, now_ms],
        )?;
        if changed == 1 {
            record_event(
                transaction,
                now_ms,
                "run_worktree_cleaned",
                Some(&candidate.run_id),
                Some(&candidate.ticket_id),
                &serde_json::json!({
                    "branch": candidate.branch,
                    "worktree": candidate.worktree_path,
                })
                .to_string(),
            )?;
        }
        Ok(changed == 1)
    }
}

pub(crate) fn notes_for_run(
    connection: &Connection,
    run_id: &str,
) -> rusqlite::Result<Vec<String>> {
    let mut statement = connection
        .prepare("SELECT text FROM notes WHERE run_id = ?1 ORDER BY recorded_at_ms, id")?;
    statement
        .query_map(params![run_id], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()
}

pub(crate) fn notes_for_project(
    connection: &Connection,
    project_id: &str,
) -> rusqlite::Result<Vec<ProjectNote>> {
    let mut statement = connection.prepare(
        "SELECT n.id, n.run_id, r.ticket_id, n.text, n.recorded_at_ms
         FROM notes n
         JOIN runs r ON r.id = n.run_id
         JOIN tickets t ON t.id = r.ticket_id
         WHERE t.project_id = ?1
         ORDER BY r.ticket_id, n.recorded_at_ms, n.id",
    )?;
    statement
        .query_map(params![project_id], |row| {
            Ok(ProjectNote {
                id: row.get(0)?,
                run_id: row.get(1)?,
                ticket_id: row.get(2)?,
                text: row.get(3)?,
                recorded_at_ms: row.get(4)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()
}

pub(crate) fn events_after(
    connection: &Connection,
    after: i64,
    limit: usize,
) -> rusqlite::Result<Vec<EventRecord>> {
    let mut statement = connection.prepare(
        "SELECT sequence, occurred_at_ms, kind, run_id, ticket_id, data_json
         FROM events WHERE sequence > ?1 ORDER BY sequence LIMIT ?2",
    )?;
    statement
        .query_map(params![after, limit as i64], |row| {
            Ok(EventRecord {
                sequence: row.get(0)?,
                occurred_at_ms: row.get(1)?,
                kind: row.get(2)?,
                run_id: row.get(3)?,
                ticket_id: row.get(4)?,
                data_json: row.get(5)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()
}

pub(crate) fn run_timelines(
    connection: &Connection,
    run_ids: &[&str],
) -> rusqlite::Result<HashMap<String, RunTimeline>> {
    if run_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let placeholders = std::iter::repeat_n("?", run_ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let mut statement = connection.prepare(&format!(
        "SELECT run_id,
                MIN(CASE WHEN kind = 'run_claimed' THEN occurred_at_ms END),
                MIN(CASE WHEN kind = 'run_started' THEN occurred_at_ms END),
                MAX(CASE WHEN kind IN ('run_finished', 'run_aborted')
                         THEN occurred_at_ms END)
         FROM events
         WHERE run_id IN ({placeholders})
         GROUP BY run_id"
    ))?;
    statement
        .query_map(rusqlite::params_from_iter(run_ids), |row| {
            Ok((
                row.get::<_, String>(0)?,
                RunTimeline {
                    claimed_at_ms: row.get(1)?,
                    started_at_ms: row.get(2)?,
                    finished_at_ms: row.get(3)?,
                },
            ))
        })?
        .collect::<Result<HashMap<_, _>, _>>()
}

pub(crate) fn latest_event_sequence(connection: &Connection) -> rusqlite::Result<i64> {
    connection.query_row("SELECT COALESCE(MAX(sequence), 0) FROM events", [], |row| {
        row.get(0)
    })
}

pub(crate) fn ticket_is_referenced(
    connection: &Connection,
    ticket_id: &str,
) -> rusqlite::Result<bool> {
    connection.query_row(
        "SELECT EXISTS (SELECT 1 FROM runs WHERE ticket_id = ?1)
             OR EXISTS (SELECT 1 FROM leases WHERE ticket_id = ?1)
             OR EXISTS (SELECT 1 FROM activations WHERE ticket_id = ?1)
             OR EXISTS (SELECT 1 FROM activation_filters WHERE ticket_id = ?1)
             OR EXISTS (SELECT 1 FROM ticket_blockers WHERE blocker_id = ?1)",
        params![ticket_id],
        |row| row.get(0),
    )
}

pub(crate) fn readopt_lease(
    connection: &Connection,
    ticket_id: &str,
    run_id: &str,
    now_ms: i64,
    expires_at_ms: i64,
) -> rusqlite::Result<usize> {
    connection.execute(
        "UPDATE leases
         SET renewed_at_ms = ?3, expires_at_ms = ?4
         WHERE ticket_id = ?1 AND run_id = ?2
           AND EXISTS (SELECT 1 FROM runs
                       WHERE id = ?2 AND exited_at_ms IS NULL)",
        params![ticket_id, run_id, now_ms, expires_at_ms],
    )
}

pub(crate) fn run(connection: &Connection, id: &str) -> rusqlite::Result<Option<RunRecord>> {
    connection
        .query_row(
            &format!("{RUN_RECORD_SELECT} WHERE id = ?1"),
            params![id],
            run_record,
        )
        .optional()
}

pub(crate) fn run_for_ticket_attempt(
    connection: &Connection,
    ticket_id: &str,
    attempt: i64,
) -> rusqlite::Result<Option<RunRecord>> {
    connection
        .query_row(
            &format!(
                "{RUN_RECORD_SELECT} WHERE ticket_id = ?1 AND attempt = ?2
                 ORDER BY created_at_ms DESC LIMIT 1"
            ),
            params![ticket_id, attempt],
            run_record,
        )
        .optional()
}

pub(crate) fn runs_for_ticket(
    connection: &Connection,
    ticket_id: &str,
) -> rusqlite::Result<Vec<RunRecord>> {
    let mut statement = connection.prepare(&format!(
        "{RUN_RECORD_SELECT} WHERE ticket_id = ?1 ORDER BY attempt DESC, created_at_ms DESC"
    ))?;
    statement
        .query_map(params![ticket_id], run_record)?
        .collect::<Result<Vec<_>, _>>()
}

pub(crate) fn runs_with_id_prefix(
    connection: &Connection,
    prefix: &str,
) -> rusqlite::Result<Vec<RunRecord>> {
    let mut statement = connection.prepare(&format!(
        "{RUN_RECORD_SELECT} WHERE SUBSTR(id, 1, ?2) = ?1 ORDER BY created_at_ms, id"
    ))?;
    statement
        .query_map(params![prefix, prefix.len() as i64], run_record)?
        .collect::<Result<Vec<_>, _>>()
}

pub(crate) fn needs_review_branches(
    connection: &Connection,
) -> rusqlite::Result<Vec<NeedsReviewBranch>> {
    let mut statement = connection.prepare(
        "SELECT t.id, r.id, r.branch
         FROM tickets t
         JOIN runs r ON r.id = (
             SELECT r2.id FROM runs r2
             WHERE r2.ticket_id = t.id
               AND r2.state = 'needs_review'
               AND r2.branch IS NOT NULL
             ORDER BY r2.created_at_ms DESC, r2.id DESC
             LIMIT 1
         )
         WHERE t.state = 'needs_review'",
    )?;
    statement
        .query_map([], |row| {
            Ok(NeedsReviewBranch {
                ticket_id: row.get(0)?,
                run_id: row.get(1)?,
                branch: row.get(2)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()
}

pub(crate) fn worktree_cleanup_candidates(
    connection: &Connection,
) -> rusqlite::Result<Vec<WorktreeCleanupCandidate>> {
    let mut statement = connection.prepare(
        "SELECT r.id, r.ticket_id, r.branch, r.worktree_path, r.cleanup_eligible_at_ms
         FROM runs r
         WHERE r.cleanup_eligible_at_ms IS NOT NULL
           AND r.cleaned_at_ms IS NULL
           AND r.branch IS NOT NULL
           AND r.worktree_path IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM leases l WHERE l.run_id = r.id)
         ORDER BY r.cleanup_eligible_at_ms, r.id",
    )?;
    statement
        .query_map([], |row| {
            Ok(WorktreeCleanupCandidate {
                run_id: row.get(0)?,
                ticket_id: row.get(1)?,
                branch: row.get(2)?,
                worktree_path: row.get(3)?,
                cleanup_eligible_at_ms: row.get(4)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()
}

pub(crate) fn next_worktree_cleanup_at_ms(
    connection: &Connection,
    retention_ms: i64,
    now_ms: i64,
) -> rusqlite::Result<Option<i64>> {
    let eligible_at: Option<i64> = connection.query_row(
        "SELECT MIN(r.cleanup_eligible_at_ms)
         FROM runs r
         WHERE r.cleanup_eligible_at_ms IS NOT NULL
           AND r.cleaned_at_ms IS NULL
           AND r.branch IS NOT NULL
           AND r.worktree_path IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM leases l WHERE l.run_id = r.id)",
        [],
        |row| row.get(0),
    )?;
    Ok(eligible_at.and_then(|value| {
        let deadline = value.saturating_add(retention_ms);
        (deadline > now_ms).then_some(deadline)
    }))
}

pub(crate) fn active_run_for_ticket(
    connection: &Connection,
    ticket_id: &str,
) -> rusqlite::Result<Option<(String, i64)>> {
    connection
        .query_row(
            "SELECT r.id, r.attempt FROM runs r
             JOIN leases l ON l.run_id = r.id
             WHERE r.ticket_id = ?1
               AND r.state IN (?2, ?3, ?4)
               AND r.exited_at_ms IS NULL
             ORDER BY r.created_at_ms DESC, r.id DESC LIMIT 1",
            params![
                ticket_id,
                NONTERMINAL_RUN_STATES[0].as_str(),
                NONTERMINAL_RUN_STATES[1].as_str(),
                NONTERMINAL_RUN_STATES[2].as_str(),
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
}

pub(crate) fn active_runs(connection: &Connection) -> rusqlite::Result<Vec<ActiveRun>> {
    let mut statement = connection.prepare(
        "SELECT r.id, r.ticket_id, r.attempt, t.name, t.project_id, r.state FROM runs r
         JOIN leases l ON l.run_id = r.id
         JOIN tickets t ON t.id = r.ticket_id
         WHERE r.exited_at_ms IS NULL
           AND r.state IN (?1, ?2, ?3)
         ORDER BY r.created_at_ms, r.id",
    )?;
    statement
        .query_map(nonterminal_state_params(), |row| {
            Ok(ActiveRun {
                id: row.get(0)?,
                ticket_id: row.get(1)?,
                attempt: row.get(2)?,
                ticket_name: row.get(3)?,
                project_id: row.get(4)?,
                state: row.get(5)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()
}

pub(crate) fn recoverable_runs(connection: &Connection) -> rusqlite::Result<Vec<RecoverableRun>> {
    let mut statement = connection.prepare(
        "SELECT r.id, r.ticket_id, t.target, r.state, r.branch, r.worktree_path,
                r.pid, r.pid_start_time, r.process_group_id, r.worker_token,
                r.worker_socket_path, r.exit_code, l.expires_at_ms, r.flow_json,
                r.ticket_json
         FROM runs r
         JOIN leases l ON l.run_id = r.id
         JOIN tickets t ON t.id = r.ticket_id
         WHERE r.exited_at_ms IS NULL
           AND r.state IN (?1, ?2, ?3)
         ORDER BY r.created_at_ms, r.id",
    )?;
    statement
        .query_map(nonterminal_state_params(), |row| {
            Ok(RecoverableRun {
                id: row.get(0)?,
                ticket_id: row.get(1)?,
                target: row.get(2)?,
                state: row.get(3)?,
                branch: row.get(4)?,
                worktree_path: row.get(5)?,
                pid: row.get(6)?,
                pid_start_time: row.get(7)?,
                process_group_id: row.get(8)?,
                worker_token: row.get(9)?,
                worker_socket_path: row.get(10)?,
                exit_code: row.get(11)?,
                lease_expires_at_ms: row.get(12)?,
                flow_json: row.get(13)?,
                ticket_json: row.get(14)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()
}

impl RunStore {
    pub(crate) fn insert_note(
        &self,
        id: &str,
        run_id: &str,
        text: &str,
        now_ms: i64,
    ) -> rusqlite::Result<()> {
        self.write(TransactionBehavior::Deferred, |transaction| {
            tx::insert_note(transaction, id, run_id, text, now_ms)
        })
    }

    pub(crate) fn notes_for_run(&self, run_id: &str) -> rusqlite::Result<Vec<String>> {
        notes_for_run(&self.db.lock(), run_id)
    }

    pub(crate) fn notes_for_project(&self, project_id: &str) -> rusqlite::Result<Vec<ProjectNote>> {
        notes_for_project(&self.db.lock(), project_id)
    }

    pub(crate) fn events_after(
        &self,
        after: i64,
        limit: usize,
    ) -> rusqlite::Result<Vec<EventRecord>> {
        events_after(&self.db.lock(), after, limit)
    }

    pub(crate) fn run_timelines(
        &self,
        run_ids: &[&str],
    ) -> rusqlite::Result<HashMap<String, RunTimeline>> {
        if run_ids.is_empty() {
            return Ok(HashMap::new());
        }
        run_timelines(&self.db.lock(), run_ids)
    }

    pub(crate) fn latest_event_sequence(&self) -> rusqlite::Result<i64> {
        latest_event_sequence(&self.db.lock())
    }

    pub(crate) fn trim_events(&self, keep: i64) -> rusqlite::Result<()> {
        self.write(TransactionBehavior::Deferred, |transaction| {
            tx::trim_events(transaction, keep)
        })
    }

    pub(crate) fn ticket_is_referenced(&self, ticket_id: &str) -> rusqlite::Result<bool> {
        ticket_is_referenced(&self.db.lock(), ticket_id)
    }

    pub(crate) fn readopt_lease(
        &self,
        ticket_id: &str,
        run_id: &str,
        now_ms: i64,
        expires_at_ms: i64,
    ) -> rusqlite::Result<usize> {
        readopt_lease(&self.db.lock(), ticket_id, run_id, now_ms, expires_at_ms)
    }

    pub(crate) fn run(&self, id: &str) -> rusqlite::Result<Option<RunRecord>> {
        run(&self.db.lock(), id)
    }

    pub(crate) fn run_for_ticket_attempt(
        &self,
        ticket_id: &str,
        attempt: i64,
    ) -> rusqlite::Result<Option<RunRecord>> {
        run_for_ticket_attempt(&self.db.lock(), ticket_id, attempt)
    }

    pub(crate) fn runs_for_ticket(&self, ticket_id: &str) -> rusqlite::Result<Vec<RunRecord>> {
        runs_for_ticket(&self.db.lock(), ticket_id)
    }

    pub(crate) fn runs_with_id_prefix(&self, prefix: &str) -> rusqlite::Result<Vec<RunRecord>> {
        runs_with_id_prefix(&self.db.lock(), prefix)
    }

    pub(crate) fn needs_review_branches(&self) -> rusqlite::Result<Vec<NeedsReviewBranch>> {
        needs_review_branches(&self.db.lock())
    }

    pub(crate) fn worktree_cleanup_candidates(
        &self,
    ) -> rusqlite::Result<Vec<WorktreeCleanupCandidate>> {
        worktree_cleanup_candidates(&self.db.lock())
    }

    pub(crate) fn next_worktree_cleanup_at_ms(
        &self,
        retention_ms: i64,
        now_ms: i64,
    ) -> rusqlite::Result<Option<i64>> {
        next_worktree_cleanup_at_ms(&self.db.lock(), retention_ms, now_ms)
    }

    pub(crate) fn mark_run_worktree_cleaned(
        &self,
        candidate: &WorktreeCleanupCandidate,
        now_ms: i64,
    ) -> rusqlite::Result<bool> {
        self.write(TransactionBehavior::Immediate, |transaction| {
            tx::mark_worktree_cleaned(transaction, candidate, now_ms)
        })
    }

    pub(crate) fn active_run_for_ticket(
        &self,
        ticket_id: &str,
    ) -> rusqlite::Result<Option<(String, i64)>> {
        active_run_for_ticket(&self.db.lock(), ticket_id)
    }

    pub(crate) fn active_runs(&self) -> rusqlite::Result<Vec<ActiveRun>> {
        active_runs(&self.db.lock())
    }

    pub(crate) fn recoverable_runs(&self) -> rusqlite::Result<Vec<RecoverableRun>> {
        recoverable_runs(&self.db.lock())
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use tempfile::tempdir;

    use super::RunState;
    use crate::db::SCHEMA_VERSION;
    use crate::domain::ticket::{TicketSnapshot, TicketState};
    use crate::flow::{Flow, Stage, StageKind, VerdictPolicy};
    use crate::outcome::Outcome;
    use crate::store::{ActivationKind, ClaimRequest, NewActivation, Store, StoreError};

    fn open_seeded(path: &std::path::Path) -> Store {
        let store = Store::open(path, 1_000).unwrap();
        store
            .insert_local_project(
                "default",
                ".agents/sloop/projects/default.md",
                "Default",
                1_000,
            )
            .unwrap();
        store
            .insert_local_ticket(
                "T1",
                "default",
                ".agents/sloop/tickets/t1.md",
                "Ticket one",
                &[],
                "sloop/T1",
                Some("claude"),
                Some("sonnet"),
                Some("medium"),
                "default",
                TicketState::Ready,
                1_000,
            )
            .unwrap();
        store
            .insert_activation(
                &NewActivation {
                    id: "A1",
                    kind: ActivationKind::Immediate,
                    ticket_id: Some("T1"),
                    project_id: None,
                    eligible_at_ms: None,
                    interval_ms: None,
                },
                1_000,
            )
            .unwrap();
        store
    }

    fn claim_t1<'a>(run_id: &'a str) -> ClaimRequest<'a> {
        ClaimRequest {
            ticket_id: "T1",
            run_id,
            activation_id: "A1",
            owner_id: "daemon-1",
            lease_ms: 60_000,
            next_activation_eligible_at_ms: None,
            flow_json: "{}",
            ticket_json: "{}",
        }
    }

    fn running_r1(store: &mut Store) {
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();
        store
            .mark_run_running(
                "R1",
                "branch",
                "/worktree",
                123,
                Some(456),
                123,
                "token",
                "/runtime/R1.sock",
                2_100,
            )
            .unwrap();
    }

    #[test]
    fn every_run_state_round_trips_through_its_stored_string() {
        let states = [
            RunState::Claimed,
            RunState::Running,
            RunState::Aftercare,
            RunState::Aborted,
            RunState::Merged,
            RunState::Failed,
            RunState::NeedsReview,
            RunState::Cancelled,
            RunState::RateLimited,
            RunState::Orphaned,
        ];
        for state in states {
            assert_eq!(RunState::parse(state.as_str()).unwrap(), state);
        }
        for outcome in [
            Outcome::Merged,
            Outcome::Failed,
            Outcome::NeedsReview,
            Outcome::Cancelled,
            Outcome::RateLimited,
            Outcome::Orphaned,
        ] {
            assert_eq!(RunState::from(outcome).as_str(), outcome.as_str());
            assert!(RunState::from(outcome).is_terminal());
        }
        for state in [RunState::Claimed, RunState::Running, RunState::Aftercare] {
            assert!(!state.is_terminal());
        }
        assert!(RunState::Aborted.is_terminal());
    }

    #[test]
    fn an_unknown_stored_run_state_is_an_error_not_a_fallback() {
        let error = RunState::parse("half_running").unwrap_err();
        assert!(matches!(error, StoreError::UnknownRunState { state } if state == "half_running"));
    }

    #[test]
    fn claims_persist_flow_and_ticket_snapshots() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        let flow = Flow {
            name: "default".into(),
            stages: vec![
                Stage {
                    name: "build".into(),
                    kind: StageKind::Agent,
                    verdict: VerdictPolicy::Commits,
                    on_fail: None,
                },
                Stage {
                    name: "check".into(),
                    kind: StageKind::Exec {
                        cmd: vec!["cargo".into(), "test".into()],
                    },
                    verdict: VerdictPolicy::Exit,
                    on_fail: None,
                },
            ],
        };
        let ticket = TicketSnapshot {
            id: "T1".into(),
            name: "Ticket one".into(),
            blocked_by: vec![],
            worktree: Some("sloop/T1".into()),
            target: Some("claude".into()),
            model: Some("sonnet".into()),
            effort: Some("medium".into()),
            body: "# Original body\n".into(),
        };
        let flow_json = serde_json::to_string(&flow).unwrap();
        let ticket_json = serde_json::to_string(&ticket).unwrap();

        store
            .claim_ticket(
                &ClaimRequest {
                    flow_json: &flow_json,
                    ticket_json: &ticket_json,
                    ..claim_t1("R1")
                },
                2_000,
            )
            .unwrap();

        let run = store.run("R1").unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<Flow>(run.flow_json.as_deref().unwrap()).unwrap(),
            flow
        );
        assert_eq!(
            serde_json::from_str::<TicketSnapshot>(run.ticket_json.as_deref().unwrap()).unwrap(),
            ticket
        );
    }

    #[test]
    fn active_run_for_ticket_tracks_claimed_and_running_runs_only() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        assert_eq!(store.active_run_for_ticket("T1").unwrap(), None);

        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();
        assert_eq!(
            store.active_run_for_ticket("T1").unwrap(),
            Some(("R1".into(), 1))
        );
        store
            .mark_run_running(
                "R1",
                "branch",
                "/tmp/worktree",
                1,
                Some(1),
                1,
                "token",
                "/runtime/R1.sock",
                2_100,
            )
            .unwrap();
        assert_eq!(
            store.active_run_for_ticket("T1").unwrap(),
            Some(("R1".into(), 1))
        );

        store
            .finish_run("R1", "T1", Some(1), Outcome::Failed, &[], None, 2_200)
            .unwrap();
        assert_eq!(store.active_run_for_ticket("T1").unwrap(), None);
    }

    #[test]
    fn recoverable_runs_round_trip_process_identity_and_lease() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sloop.db");
        let mut store = open_seeded(&path);
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();
        store
            .mark_run_running(
                "R1",
                "sloop/T1-a1-R1",
                "/worktrees/R1",
                123,
                Some(456),
                123,
                "worker-token",
                "/runtime/R1.sock",
                2_100,
            )
            .unwrap();
        drop(store);

        let store = Store::open(&path, 3_000).unwrap();
        let runs = store.recoverable_runs().unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].id, "R1");
        assert_eq!(runs[0].ticket_id, "T1");
        assert_eq!(runs[0].pid, Some(123));
        assert_eq!(runs[0].pid_start_time, Some(456));
        assert_eq!(runs[0].process_group_id, Some(123));
        assert_eq!(runs[0].worker_token.as_deref(), Some("worker-token"));
        assert_eq!(
            runs[0].worker_socket_path.as_deref(),
            Some("/runtime/R1.sock")
        );
        assert_eq!(runs[0].exit_code, None);
        assert_eq!(runs[0].lease_expires_at_ms, 62_000);
    }

    #[test]
    fn writable_probe_commits_without_changing_pause_state() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));

        store.probe_writable(2_000).unwrap();

        assert!(!store.paused().unwrap());
        let updated_at_ms: i64 = store
            .db()
            .lock()
            .query_row(
                "SELECT updated_at_ms FROM scheduler_state WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(updated_at_ms, 2_000);
    }

    #[test]
    fn lifecycle_transitions_append_ordered_events() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        running_r1(&mut store);
        store
            .finish_run("R1", "T1", Some(0), Outcome::Merged, &[], None, 2_300)
            .unwrap();

        let events = store.events_after(0, 10).unwrap();
        let kinds: Vec<&str> = events.iter().map(|event| event.kind.as_str()).collect();
        assert_eq!(kinds, ["run_claimed", "run_started", "run_finished"]);
        assert!(events.iter().all(|event| {
            event.run_id.as_deref() == Some("R1") && event.ticket_id.as_deref() == Some("T1")
        }));
        let finished: serde_json::Value = serde_json::from_str(&events[2].data_json).unwrap();
        assert_eq!(finished["outcome"], "merged");
        assert_eq!(finished["ticket_state"], "merged");

        store
            .finish_run("R1", "T1", Some(1), Outcome::Failed, &[], None, 2_400)
            .unwrap();
        assert_eq!(store.latest_event_sequence().unwrap(), events[2].sequence);

        let rest = store.events_after(events[0].sequence, 10).unwrap();
        assert_eq!(rest.len(), 2);
        assert_eq!(rest[0].kind, "run_started");

        store.trim_events(1).unwrap();
        let kept = store.events_after(0, 10).unwrap();
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].sequence, events[2].sequence);
    }

    #[test]
    fn abandoned_claims_append_an_abort_event() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();
        store.abort_claim("R1", "T1", 2_100).unwrap();

        let kinds: Vec<String> = store
            .events_after(0, 10)
            .unwrap()
            .into_iter()
            .map(|event| event.kind)
            .collect();
        assert_eq!(kinds, ["run_claimed", "run_aborted"]);
    }

    #[test]
    fn notes_round_trip_in_arrival_order() {
        let directory = tempdir().unwrap();
        let mut store = open_seeded(&directory.path().join("sloop.db"));
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();

        assert_eq!(store.next_note_ordinal().unwrap(), 1);
        store.insert_note("N1", "R1", "first", 3_000).unwrap();
        store.insert_note("N2", "R1", "second", 3_000).unwrap();
        assert_eq!(store.next_note_ordinal().unwrap(), 3);

        assert_eq!(
            store.notes_for_run("R1").unwrap(),
            vec!["first".to_owned(), "second".to_owned()]
        );
        assert!(store.notes_for_run("R2").unwrap().is_empty());
    }

    #[test]
    fn version_three_migrates_ticket_metadata_and_newer_schemas_are_rejected() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sloop.db");
        drop(Store::open(&path, 1_000).unwrap());

        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "DROP TABLE ticket_blockers;
                 ALTER TABLE tickets DROP COLUMN name;
                 ALTER TABLE tickets DROP COLUMN worktree;
                 ALTER TABLE tickets DROP COLUMN flow;
                 ALTER TABLE tickets DROP COLUMN body;
                 ALTER TABLE tickets DROP COLUMN held_reason;
                 ALTER TABLE tickets DROP COLUMN missing_at_ms;
                 ALTER TABLE scheduler_state DROP COLUMN draining;
                 ALTER TABLE runs DROP COLUMN worker_socket_path;
                 ALTER TABLE runs DROP COLUMN flow_json;
                 ALTER TABLE runs DROP COLUMN ticket_json;
                 ALTER TABLE runs DROP COLUMN cleanup_eligible_at_ms;
                 ALTER TABLE runs DROP COLUMN cleaned_at_ms;",
            )
            .unwrap();
        connection.pragma_update(None, "user_version", 3).unwrap();
        drop(connection);

        let store = Store::open(&path, 2_000).unwrap();
        assert!(!store.paused().unwrap());
        store
            .insert_local_project(
                "default",
                ".agents/sloop/projects/default.md",
                "Default",
                2_000,
            )
            .unwrap();
        store
            .insert_local_ticket(
                "T1",
                "default",
                ".agents/sloop/tickets/t1.md",
                "Ticket one",
                &[],
                "sloop/T1",
                Some("codex"),
                None,
                None,
                "default",
                TicketState::Ready,
                2_000,
            )
            .unwrap();
        assert_eq!(
            store.ticket("T1").unwrap().unwrap().target.as_deref(),
            Some("codex")
        );
        drop(store);

        let connection = Connection::open(&path).unwrap();
        connection.pragma_update(None, "user_version", 99).unwrap();
        drop(connection);

        assert!(matches!(
            Store::open(&path, 3_000),
            Err(StoreError::UnsupportedSchemaVersion(99))
        ));
    }

    #[test]
    fn version_eight_migrates_existing_runs_with_null_snapshots() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sloop.db");
        let mut store = open_seeded(&path);
        store.claim_ticket(&claim_t1("R1"), 2_000).unwrap();
        drop(store);

        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "ALTER TABLE runs DROP COLUMN flow_json;
                 ALTER TABLE runs DROP COLUMN ticket_json;
                 ALTER TABLE tickets DROP COLUMN body;
                 ALTER TABLE tickets DROP COLUMN held_reason;
                 ALTER TABLE scheduler_state DROP COLUMN draining;
                 ALTER TABLE runs DROP COLUMN cleanup_eligible_at_ms;
                 ALTER TABLE runs DROP COLUMN cleaned_at_ms;",
            )
            .unwrap();
        connection.pragma_update(None, "user_version", 8).unwrap();
        drop(connection);

        let store = Store::open(&path, 3_000).unwrap();
        let run = store.run("R1").unwrap().unwrap();
        assert_eq!(run.flow_json, None);
        assert_eq!(run.ticket_json, None);
    }

    #[test]
    fn version_ten_adds_source_metadata_without_disturbing_ticket_state() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sloop.db");
        let store = open_seeded(&path);
        store
            .db()
            .lock()
            .execute(
                "UPDATE tickets SET state = 'held', attempts = 3 WHERE id = 'T1'",
                [],
            )
            .unwrap();
        drop(store);

        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "ALTER TABLE tickets DROP COLUMN body;
                 ALTER TABLE tickets DROP COLUMN held_reason;
                 ALTER TABLE scheduler_state DROP COLUMN draining;
                 ALTER TABLE runs DROP COLUMN cleanup_eligible_at_ms;
                 ALTER TABLE runs DROP COLUMN cleaned_at_ms;",
            )
            .unwrap();
        connection.pragma_update(None, "user_version", 10).unwrap();
        drop(connection);

        let store = Store::open(&path, 3_000).unwrap();
        let ticket = store.ticket("T1").unwrap().unwrap();
        assert_eq!(ticket.state, "held");
        assert_eq!(ticket.attempts, 3);
        assert_eq!(ticket.body, None);
        assert_eq!(ticket.held_reason, None);
    }

    #[test]
    fn paused_state_persists() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sloop.db");

        let store = Store::open(&path, 1_000).unwrap();
        store.set_paused(true, 2_000).unwrap();
        drop(store);

        assert!(Store::open(&path, 3_000).unwrap().paused().unwrap());
    }

    #[test]
    fn restart_draining_is_durable_idempotent_and_cancelled_by_resume() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sloop.db");
        let mut store = Store::open(&path, 1_000).unwrap();

        assert!(store.begin_restart_draining(2, 2_000).unwrap());
        assert!(!store.begin_restart_draining(2, 2_100).unwrap());
        assert!(store.restart_draining().unwrap());
        assert_eq!(
            store
                .events_after(0, 10)
                .unwrap()
                .iter()
                .filter(|event| event.kind == "daemon_restart_requested")
                .count(),
            1
        );
        drop(store);

        let mut reopened = Store::open(&path, 3_000).unwrap();
        assert!(reopened.restart_draining().unwrap());
        assert!(reopened.resume_scheduler(4_000).unwrap());
        assert!(!reopened.restart_draining().unwrap());
    }

    #[test]
    fn version_eleven_adds_restart_draining_state() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sloop.db");
        drop(Store::open(&path, 1_000).unwrap());
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "ALTER TABLE scheduler_state DROP COLUMN draining;
                 ALTER TABLE runs DROP COLUMN cleanup_eligible_at_ms;
                 ALTER TABLE runs DROP COLUMN cleaned_at_ms;
                 PRAGMA user_version = 11;",
            )
            .unwrap();
        drop(connection);

        let store = Store::open(&path, 2_000).unwrap();
        assert!(!store.restart_draining().unwrap());
        assert_eq!(
            store
                .db()
                .lock()
                .query_row("PRAGMA user_version", [], |row| row.get::<_, u32>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
    }
}
