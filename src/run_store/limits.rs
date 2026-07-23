use rusqlite::{Connection, OptionalExtension, params};

use super::RunStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CooldownUpdate<'a> {
    pub target: &'a str,
    pub until_ms: i64,
    pub reason: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CooldownRecord {
    pub target: String,
    pub until_ms: i64,
    pub reason: String,
}

pub(crate) mod tx {
    use rusqlite::{Transaction, params};

    use super::CooldownUpdate;

    pub(crate) fn detach_cooldowns_from_run(
        transaction: &Transaction<'_>,
        run_id: &str,
    ) -> rusqlite::Result<usize> {
        transaction.execute(
            "UPDATE cooldowns SET source_run_id = NULL WHERE source_run_id = ?1",
            params![run_id],
        )
    }

    pub(crate) fn delete_budget_reservation_for_run(
        transaction: &Transaction<'_>,
        run_id: &str,
    ) -> rusqlite::Result<usize> {
        transaction.execute(
            "DELETE FROM budget_reservations WHERE run_id = ?1",
            params![run_id],
        )
    }

    pub(crate) fn upsert_cooldown(
        transaction: &Transaction<'_>,
        run_id: &str,
        cooldown: &CooldownUpdate<'_>,
        now_ms: i64,
    ) -> rusqlite::Result<()> {
        transaction.execute(
            "INSERT INTO cooldowns (key, until_ms, reason, source_run_id, updated_at_ms)
             VALUES ('agent_target:' || ?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(key) DO UPDATE SET
                 until_ms = MAX(cooldowns.until_ms, excluded.until_ms),
                 reason = CASE WHEN excluded.until_ms >= cooldowns.until_ms
                               THEN excluded.reason ELSE cooldowns.reason END,
                 source_run_id = CASE WHEN excluded.until_ms >= cooldowns.until_ms
                                      THEN excluded.source_run_id ELSE cooldowns.source_run_id END,
                 updated_at_ms = excluded.updated_at_ms",
            params![
                cooldown.target,
                cooldown.until_ms,
                cooldown.reason,
                run_id,
                now_ms
            ],
        )?;
        Ok(())
    }
}

fn active_cooldown_for_target(
    connection: &Connection,
    target: &str,
    now_ms: i64,
) -> rusqlite::Result<Option<CooldownRecord>> {
    connection
        .query_row(
            "SELECT ?1, until_ms, reason FROM cooldowns
             WHERE key = 'agent_target:' || ?1 AND until_ms > ?2",
            params![target, now_ms],
            |row| {
                Ok(CooldownRecord {
                    target: row.get(0)?,
                    until_ms: row.get(1)?,
                    reason: row.get(2)?,
                })
            },
        )
        .optional()
}

fn active_cooldowns(connection: &Connection, now_ms: i64) -> rusqlite::Result<Vec<CooldownRecord>> {
    let mut statement = connection.prepare(
        "SELECT SUBSTR(key, 14), until_ms, reason FROM cooldowns
         WHERE key LIKE 'agent_target:%' AND until_ms > ?1
         ORDER BY key",
    )?;
    statement
        .query_map(params![now_ms], |row| {
            Ok(CooldownRecord {
                target: row.get(0)?,
                until_ms: row.get(1)?,
                reason: row.get(2)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()
}

fn next_active_cooldown(connection: &Connection, now_ms: i64) -> rusqlite::Result<Option<i64>> {
    connection.query_row(
        "SELECT MIN(until_ms) FROM cooldowns WHERE until_ms > ?1",
        params![now_ms],
        |row| row.get(0),
    )
}

impl RunStore {
    pub(crate) fn active_cooldown_for_target(
        &self,
        target: &str,
        now_ms: i64,
    ) -> rusqlite::Result<Option<CooldownRecord>> {
        active_cooldown_for_target(&self.db.lock(), target, now_ms)
    }

    pub(crate) fn active_cooldowns(&self, now_ms: i64) -> rusqlite::Result<Vec<CooldownRecord>> {
        active_cooldowns(&self.db.lock(), now_ms)
    }

    pub(crate) fn next_active_cooldown(&self, now_ms: i64) -> rusqlite::Result<Option<i64>> {
        next_active_cooldown(&self.db.lock(), now_ms)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::CooldownUpdate;
    use crate::coordination::{Claim, Coordination};
    use crate::domain::ticket::TicketState;
    use crate::outcome::Outcome;
    use crate::store::{ActivationKind, ClaimRequest, NewActivation, QueuedActivation, Store};

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
                None,
                None,
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

    fn claim<'a>(run_id: &'a str) -> ClaimRequest<'a> {
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

    fn claim_run(store: &Store, run_id: &str, now_ms: i64) {
        assert!(matches!(
            Coordination::from_shared(store)
                .claim(&claim(run_id), now_ms)
                .unwrap(),
            Claim::Granted(_)
        ));
    }

    #[test]
    fn cooldowns_extend_without_shortening_and_gate_ready_work() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));
        let activation = QueuedActivation {
            id: "A1".into(),
            kind: "immediate".into(),
            ticket_id: Some("T1".into()),
            project_id: None,
            eligible_at_ms: None,
            interval_ms: None,
        };

        claim_run(&store, "R1", 2_000);
        Coordination::from_shared(&store)
            .settle(
                "R1",
                "T1",
                Some(1),
                Outcome::RateLimited,
                &[],
                Some(&CooldownUpdate {
                    target: "claude",
                    until_ms: 10_000,
                    reason: "rate limit",
                }),
                2_100,
            )
            .unwrap();

        assert_eq!(store.select_ready_ticket(&activation, 5_000).unwrap(), None);
        assert_eq!(store.next_active_cooldown(5_000).unwrap(), Some(10_000));
        assert_eq!(store.active_cooldowns(5_000).unwrap()[0].target, "claude");

        claim_run(&store, "R2", 5_000);
        Coordination::from_shared(&store)
            .settle(
                "R2",
                "T1",
                Some(1),
                Outcome::RateLimited,
                &[],
                Some(&CooldownUpdate {
                    target: "claude",
                    until_ms: 8_000,
                    reason: "shorter retry",
                }),
                5_100,
            )
            .unwrap();

        let cooldown = store
            .active_cooldown_for_target("claude", 6_000)
            .unwrap()
            .unwrap();
        assert_eq!(cooldown.until_ms, 10_000);
        assert_eq!(cooldown.reason, "rate limit");
        assert_eq!(
            store
                .select_ready_ticket(&activation, 10_000)
                .unwrap()
                .as_deref(),
            Some("T1")
        );
    }

    #[test]
    fn reindex_detaches_cooldowns_and_deletes_budget_reservations() {
        let directory = tempdir().unwrap();
        let store = open_seeded(&directory.path().join("sloop.db"));
        claim_run(&store, "R1", 2_000);
        Coordination::from_shared(&store)
            .settle(
                "R1",
                "T1",
                Some(1),
                Outcome::Failed,
                &[],
                Some(&CooldownUpdate {
                    target: "claude",
                    until_ms: 10_000,
                    reason: "rate limit",
                }),
                2_100,
            )
            .unwrap();
        store
            .db()
            .lock()
            .execute(
                "INSERT INTO budget_reservations
                     (run_id, reserved_tokens, actual_tokens, state, created_at_ms,
                      reconciled_at_ms)
                 VALUES ('R1', 1000, NULL, 'reserved', 2000, NULL)",
                [],
            )
            .unwrap();

        store
            .apply_reindex(&["default".into()], &[], 3_000)
            .unwrap();

        let database = store.db();
        let connection = database.lock();
        let source_run_id: Option<String> = connection
            .query_row(
                "SELECT source_run_id FROM cooldowns WHERE key = 'agent_target:claude'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let reservations: i64 = connection
            .query_row("SELECT COUNT(*) FROM budget_reservations", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(source_run_id, None);
        assert_eq!(reservations, 0);
    }
}
