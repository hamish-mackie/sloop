mod support;

use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use rusqlite::Connection;

use support::{FakeAgent, World, wait_until};

fn configure(world: &World, retention: &str, agent: FakeAgent) {
    world.configure_fake_agent(agent);
    let path = world.root().join(".agents/sloop/config.yaml");
    let config = fs::read_to_string(&path).unwrap();
    fs::write(
        path,
        config.replacen(
            "version: 1\n",
            &format!("version: 1\nworktree_retention: {retention}\n"),
            1,
        ),
    )
    .unwrap();
}

fn post_and_run(world: &World, name: &str) -> String {
    let ticket_path = world.write_ticket(name, "# Retention test\n");
    let posted = world.sloop(&["post", ticket_path.to_str().unwrap(), "--manual"]);
    assert!(
        posted.status.success(),
        "{}",
        String::from_utf8_lossy(&posted.stderr)
    );
    let ticket = World::json_stdout(&posted)["data"]["ticket"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let run = world.sloop(&["run", &ticket]);
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    ticket
}

fn run_value<T: rusqlite::types::FromSql>(world: &World, column: &str) -> T {
    let connection = Connection::open(world.db_path()).unwrap();
    connection
        .query_row(
            &format!("SELECT {column} FROM runs ORDER BY created_at_ms LIMIT 1"),
            [],
            |row| row.get(0),
        )
        .unwrap()
}

fn branch_exists(root: &Path, branch: &str) -> bool {
    Command::new("git")
        .args([
            "show-ref",
            "--verify",
            "--quiet",
            &format!("refs/heads/{branch}"),
        ])
        .current_dir(root)
        .status()
        .unwrap()
        .success()
}

fn trigger_reconcile(world: &World) {
    assert!(world.sloop(&["status"]).status.success());
}

fn wait_for_merged(world: &World) {
    wait_until("run settles as merged", || {
        let connection = Connection::open(world.db_path()).unwrap();
        connection
            .query_row("SELECT state = 'merged' FROM runs LIMIT 1", [], |row| {
                row.get(0)
            })
            .unwrap_or(false)
    });
}

#[test]
fn merged_worktree_and_branch_expire_after_retention() {
    let world = World::configured();
    configure(&world, "1h", FakeAgent::new().commit("work").exit(0));
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "expire.md");
    wait_for_merged(&world);
    let worktree = world.run_worktree(1);
    let branch: String = run_value(&world, "branch");

    world.tick(Duration::from_secs(59 * 60));
    trigger_reconcile(&world);
    assert!(worktree.exists());
    assert!(branch_exists(world.root(), &branch));

    world.tick(Duration::from_secs(2 * 60));
    wait_until("expired worktree is cleaned", || !worktree.exists());
    assert!(!branch_exists(world.root(), &branch));
    assert!(run_value::<Option<i64>>(&world, "cleaned_at_ms").is_some());
    let connection = Connection::open(world.db_path()).unwrap();
    let events: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE kind = 'run_worktree_cleaned'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(events, 1);
}

#[test]
fn never_retains_settled_worktrees() {
    let world = World::configured();
    configure(&world, "never", FakeAgent::new().commit("work").exit(0));
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "never.md");
    wait_for_merged(&world);
    let worktree = world.run_worktree(1);

    world.tick(Duration::from_secs(365 * 24 * 60 * 60));
    trigger_reconcile(&world);
    assert!(worktree.exists());
    assert!(run_value::<Option<i64>>(&world, "cleaned_at_ms").is_none());
}

#[test]
fn failed_worktree_expires_only_after_retry_resolves_it() {
    let world = World::configured();
    configure(&world, "1h", FakeAgent::new().exit(1));
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_and_run(&world, "retry.md");
    wait_until("first run fails", || {
        let connection = Connection::open(world.db_path()).unwrap();
        connection
            .query_row("SELECT state = 'failed' FROM runs LIMIT 1", [], |row| {
                row.get(0)
            })
            .unwrap_or(false)
    });
    let failed_worktree = world.run_worktree(1);

    world.tick(Duration::from_secs(365 * 24 * 60 * 60));
    trigger_reconcile(&world);
    assert!(failed_worktree.exists());

    fs::write(
        world.root().join("fake-agent.sh"),
        "#!/bin/sh\nset -eu\ngit -c user.name=sloop-test-agent -c user.email=sloop-test-agent@example.invalid commit --quiet --allow-empty -m replacement\n",
    )
    .unwrap();
    assert!(world.sloop(&["retry", &ticket]).status.success());
    assert!(world.sloop(&["run", &ticket]).status.success());
    wait_until("replacement run merges", || {
        let connection = Connection::open(world.db_path()).unwrap();
        connection
            .query_row(
                "SELECT COUNT(*) = 1 FROM runs WHERE state = 'merged'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(false)
    });
    world.tick(Duration::from_secs(61 * 60));
    wait_until("failed worktree expires from retry time", || {
        !failed_worktree.exists()
    });
}

#[test]
fn manually_removed_worktree_is_pruned_and_recorded_before_expiry() {
    let world = World::configured();
    configure(&world, "7d", FakeAgent::new().commit("work").exit(0));
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "manual.md");
    wait_for_merged(&world);
    let worktree = world.run_worktree(1);
    let branch: String = run_value(&world, "branch");
    fs::remove_dir_all(&worktree).unwrap();

    trigger_reconcile(&world);
    wait_until("manual cleanup is recorded", || {
        run_value::<Option<i64>>(&world, "cleaned_at_ms").is_some()
    });
    assert!(!branch_exists(world.root(), &branch));
}

#[test]
fn active_worktree_is_never_removed() {
    let world = World::configured();
    configure(
        &world,
        "1s",
        FakeAgent::new()
            .block_until_released("active")
            .commit("work")
            .exit(0),
    );
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "active.md");
    wait_until("agent is active", || world.fake_agent_reached("active"));
    let worktree = world.run_worktree(1);

    world.tick(Duration::from_secs(30 * 24 * 60 * 60));
    trigger_reconcile(&world);
    assert!(worktree.exists());
    world.release("active");
}

#[test]
fn restart_after_expiry_cleans_on_startup_reconcile() {
    let world = World::configured();
    configure(&world, "1h", FakeAgent::new().commit("work").exit(0));
    world.commit_all("initial");
    let started = world.start_daemon();
    let pid = started["data"]["pid"].as_u64().unwrap() as u32;
    post_and_run(&world, "restart.md");
    wait_for_merged(&world);
    let worktree = world.run_worktree(1);
    assert!(world.sloop(&["stop"]).status.success());
    wait_until("daemon stops", || {
        !Path::new(&world.operator_socket()).exists()
    });
    world.tick(Duration::from_secs(2 * 60 * 60));

    let restarted = world.start_daemon();
    assert_ne!(restarted["data"]["pid"].as_u64().unwrap() as u32, pid);
    wait_until("startup reconcile cleans expired worktree", || {
        !worktree.exists()
    });
}

#[test]
fn invalid_retention_prevents_daemon_startup_with_a_remedy() {
    let world = World::configured();
    let path = world.root().join(".agents/sloop/config.yaml");
    fs::write(&path, "version: 1\nworktree_retention: eventually\n").unwrap();

    let output = world.sloop(&["daemon"]);
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("worktree_retention"), "{error}");
    assert!(error.contains("use a positive duration"), "{error}");
    assert!(error.contains("never"), "{error}");
}
