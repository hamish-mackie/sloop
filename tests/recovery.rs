//! Integration coverage for the recovery pass that reconciles run state a
//! previous daemon left half-written.
//!
//! Each case reproduces one crash window by hand — a claim recorded without its
//! run, a terminal run whose source release never landed, an external merge
//! observed against a branch that is gone — and asserts that a fresh daemon
//! settles it from the durable evidence alone.

mod support;

use serde_json::Value;
use support::{World, wait_until};

fn post(world: &World, name: &str) -> String {
    let ticket = world.write_ticket(name, "# Recovery scenario\n");
    let output = world.sloop(&["post", ticket.to_str().unwrap(), "--manual"]);
    assert!(
        output.status.success(),
        "post failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    World::json_stdout(&output)["data"]["ticket"]["id"]
        .as_str()
        .expect("ticket id")
        .to_owned()
}

fn status(world: &World) -> Value {
    let output = world.sloop(&["status"]);
    assert!(output.status.success());
    World::json_stdout(&output)["data"].clone()
}

#[test]
fn recovery_releases_a_claim_whose_run_commit_never_landed() {
    let world = World::configured();
    world.commit_all("initial");
    let first = World::json_stdout(&world.sloop(&["daemon"]))["data"]["pid"]
        .as_u64()
        .unwrap() as u32;
    let ticket = post(&world, "claim-window.md");
    assert!(world.sloop(&["pause"]).status.success());
    world.kill_daemon(first);

    let now_ms = world.now_ms();
    let connection = rusqlite::Connection::open(world.db_path()).expect("open state database");
    connection
        .pragma_update(None, "foreign_keys", false)
        .unwrap();
    connection
        .execute(
            "INSERT INTO triggers
                 (id, kind, state, ticket_id, created_at_ms, updated_at_ms)
             VALUES ('A-window', 'immediate', 'completed', ?1, ?2, ?2)",
            rusqlite::params![ticket, now_ms],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO triggers
                 (id, kind, state, ticket_id, created_at_ms, updated_at_ms)
             VALUES ('A-other', 'immediate', 'completed', ?1, ?2, ?3)",
            rusqlite::params![ticket, now_ms + 1, now_ms + 1],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE tickets
             SET state = 'claimed', attempts = 1, updated_at_ms = ?2
             WHERE id = ?1",
            rusqlite::params![ticket, now_ms],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO leases
                 (ticket_id, run_id, owner_id, acquired_at_ms, renewed_at_ms, expires_at_ms)
             VALUES (?1, 'R-window', ?2, ?3, ?3, ?4)",
            rusqlite::params![
                ticket,
                r#"{"trigger":"A-window","owner":"R-window"}"#,
                now_ms,
                now_ms + 60_000
            ],
        )
        .unwrap();
    drop(connection);

    world.start_daemon();
    wait_until("the unrecorded claim is released", || {
        status(&world)["tickets"]["ready"] == 1
    });

    let connection = rusqlite::Connection::open(world.db_path()).expect("open recovered database");
    let (state, attempts): (String, i64) = connection
        .query_row(
            "SELECT state, attempts FROM tickets WHERE id = ?1",
            [&ticket],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, "ready");
    assert_eq!(attempts, 1);
    assert_eq!(
        connection
            .query_row(
                "SELECT state FROM triggers WHERE id = 'A-window'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "queued"
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT state FROM triggers WHERE id = 'A-other'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "completed"
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM leases", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM runs", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        connection
            .prepare("PRAGMA foreign_key_check")
            .unwrap()
            .query_map([], |_| Ok(()))
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn recovery_releases_a_terminal_run_whose_source_release_never_landed() {
    let world = World::configured();
    world.commit_all("initial");
    let first = World::json_stdout(&world.sloop(&["daemon"]))["data"]["pid"]
        .as_u64()
        .unwrap() as u32;
    let ticket = post(&world, "settlement-window.md");
    assert!(world.sloop(&["pause"]).status.success());
    world.kill_daemon(first);

    let now_ms = world.now_ms();
    let connection = rusqlite::Connection::open(world.db_path()).expect("open state database");
    connection
        .execute(
            "INSERT INTO triggers
                 (id, kind, state, ticket_id, created_at_ms, updated_at_ms)
             VALUES ('A-settlement-window', 'immediate', 'completed', ?1, ?2, ?2)",
            rusqlite::params![ticket, now_ms],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE tickets
             SET state = 'claimed', attempts = 1, updated_at_ms = ?2
             WHERE id = ?1",
            rusqlite::params![ticket, now_ms],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO runs
                 (id, trigger_id, ticket_id, state, attempt, branch, exit_code,
                  exited_at_ms, created_at_ms, updated_at_ms)
             VALUES ('R-settlement-window', 'A-settlement-window', ?1, 'failed', 1,
                     'sloop/settlement-window', 1, ?2, ?2, ?2)",
            rusqlite::params![ticket, now_ms],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO run_evidence
                 (run_id, kind, observed_at_ms, dedupe_key, data_json)
             VALUES ('R-settlement-window', 'commits_observed', ?1,
                     'settlement:R-settlement-window:commits_observed',
                     '{\"complete\":true,\"oids\":[\"abc\"]}')",
            [now_ms],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO leases
                 (ticket_id, run_id, owner_id, acquired_at_ms, renewed_at_ms, expires_at_ms)
             VALUES (?1, 'R-settlement-window', ?2, ?3, ?3, ?4)",
            rusqlite::params![
                ticket,
                r#"{"trigger":"A-settlement-window","owner":"R-settlement-window"}"#,
                now_ms,
                now_ms + 60_000
            ],
        )
        .unwrap();
    drop(connection);

    world.start_daemon();
    wait_until("the recorded outcome releases its source claim", || {
        status(&world)["tickets"]["failed"] == 1
    });

    let connection = rusqlite::Connection::open(world.db_path()).expect("open recovered database");
    let (state, attempts): (String, i64) = connection
        .query_row(
            "SELECT state, attempts FROM tickets WHERE id = ?1",
            [&ticket],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, "failed");
    assert_eq!(attempts, 1);
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM leases", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT state FROM triggers WHERE id = 'A-settlement-window'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "completed"
    );
}

#[test]
fn recovery_applies_recorded_external_merge_without_the_run_branch() {
    let world = World::configured();
    world.commit_all("initial");
    let first = World::json_stdout(&world.sloop(&["daemon"]))["data"]["pid"]
        .as_u64()
        .unwrap() as u32;
    let ticket = post(&world, "external-merge-window.md");
    assert!(world.sloop(&["pause"]).status.success());
    world.kill_daemon(first);

    let now_ms = world.now_ms();
    let connection = rusqlite::Connection::open(world.db_path()).expect("open state database");
    connection
        .execute(
            "INSERT INTO triggers
                 (id, kind, state, ticket_id, created_at_ms, updated_at_ms)
             VALUES ('A-external-window', 'immediate', 'completed', ?1, ?2, ?2)",
            rusqlite::params![ticket, now_ms],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE tickets SET state = 'needs_review', attempts = 1, updated_at_ms = ?2
             WHERE id = ?1",
            rusqlite::params![ticket, now_ms],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO runs
                 (id, trigger_id, ticket_id, state, attempt, branch, exit_code,
                  exited_at_ms, created_at_ms, updated_at_ms)
             VALUES ('R-external-window', 'A-external-window', ?1, 'needs_review', 1,
                     'sloop/deleted-external-window', 0, ?2, ?2, ?2)",
            rusqlite::params![ticket, now_ms],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO run_evidence
                 (run_id, kind, observed_at_ms, dedupe_key, data_json)
             VALUES ('R-external-window', 'external_merge_observed', ?1,
                     'external_merge:R-external-window',
                     '{\"branch\":\"sloop/deleted-external-window\"}')",
            [now_ms],
        )
        .unwrap();
    drop(connection);

    world.start_daemon();
    wait_until("the durable external merge releases the ticket", || {
        status(&world)["tickets"]["merged"] == 1
    });
}
