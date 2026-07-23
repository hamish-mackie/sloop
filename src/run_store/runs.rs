use std::collections::HashMap;

use rusqlite::{Connection, TransactionBehavior, params};

use super::RunStore;

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
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use tempfile::tempdir;

    use crate::db::SCHEMA_VERSION;
    use crate::domain::ticket::TicketState;
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
