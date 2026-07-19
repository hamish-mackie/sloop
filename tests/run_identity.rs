//! Run identity end to end: internal ids are random, humans see aliases, and
//! one resolver accepts every reasonable way to name a run.

mod support;

use std::fs;

use serde_json::json;
use support::{World, wait_until};

fn configure_agent_script(world: &World, script_body: &str) {
    fs::create_dir_all(world.root().join(".agents/sloop/flows")).unwrap();
    fs::write(
        world.root().join(".agents/sloop/flows/default.yaml"),
        "stages:\n  - { name: build, kind: build }\n  - { name: merge, kind: merge }\n",
    )
    .unwrap();
    let script = world.root().join("fake-agent.sh");
    fs::write(&script, format!("#!/bin/sh\n{script_body}")).expect("write fake agent script");
    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        format!(
            "version: 1\nscheduler:\n  max_parallel_tasks: 1\nagent:\n  default_target: fake\n  targets:\n    fake:\n      cmd: [\"sh\", \"{}\", \"{{prompt}}\"]\n",
            script.display()
        ),
    )
    .expect("write agent config");
}

fn post(world: &World, name: &str) -> String {
    let ticket = world.write_ticket(name, "# Work\n");
    let output = world.sloop(&["post", ticket.to_str().unwrap(), "--manual"]);
    assert!(output.status.success());
    World::json_stdout(&output)["data"]["ticket"]["id"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn post_and_run(world: &World, name: &str) -> String {
    let id = post(world, name);
    assert!(world.sloop(&["run", &id]).status.success());
    id
}

fn wait_for_idle(world: &World) {
    wait_until("the run exits", || {
        let output = world.sloop(&["status"]);
        World::json_stdout(&output)["data"]["gate"]["active_agents"] == 0
    });
}

/// A world with one settled run, returned with its ticket id.
fn world_with_one_settled_run(name: &str) -> (World, String) {
    let world = World::configured();
    configure_agent_script(&world, "echo working\nexit 0\n");
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_and_run(&world, name);
    wait_for_idle(&world);
    (world, ticket)
}

#[test]
fn minted_run_ids_are_random_hex_and_never_sequential_ordinals() {
    let world = World::configured();
    configure_agent_script(&world, "echo working\nexit 0\n");
    world.commit_all("initial");
    world.start_daemon();
    for name in ["first.md", "second.md", "third.md"] {
        post_and_run(&world, name);
        wait_for_idle(&world);
    }

    let ids = world.run_ids();
    assert_eq!(ids.len(), 3, "{ids:?}");
    for id in &ids {
        assert_eq!(id.len(), 32, "`{id}` is not a 128-bit hexadecimal id");
        assert!(
            id.bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "`{id}` is not lowercase hexadecimal"
        );
    }
    // Sequential minting would make these share every character but the last.
    assert_eq!(
        ids.iter().collect::<std::collections::HashSet<_>>().len(),
        3,
        "minted ids collided: {ids:?}"
    );
}

#[test]
fn status_and_list_name_runs_by_ticket_derived_alias() {
    let world = World::configured();
    configure_agent_script(&world, "sleep 30\n");
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_and_run(&world, "alias.md");

    wait_until("the run starts", || {
        let output = world.sloop(&["status"]);
        World::json_stdout(&output)["data"]["gate"]["active_agents"] == 1
    });
    let alias = format!("{ticket}-r1");

    let status = World::json_stdout(&world.sloop(&["status"]))["data"].clone();
    let run = &status["runs"][0];
    assert_eq!(run["alias"], json!(alias));
    assert_eq!(run["id"], json!(world.run_id(1)));
    assert_eq!(run["ticket"], json!(ticket));

    // The human line is ticket-first and carries the ticket's name.
    let human = String::from_utf8_lossy(&world.sloop_plain(&["status"]).stdout).into_owned();
    assert!(
        human.contains(&format!("{alias} running — alias")),
        "{human}"
    );
    assert!(!human.contains(&world.run_id(1)), "{human}");

    let list = World::json_stdout(&world.sloop(&["list"]))["data"].clone();
    let row = list["tickets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == json!(ticket))
        .expect("the ticket is listed");
    assert_eq!(row["run"], json!(alias));
    assert_eq!(row["run_id"], json!(world.run_id(1)));
    assert_eq!(row["reason"], json!(format!("claimed by run {alias}")));

    assert!(world.sloop(&["stop", "--force"]).status.success());
}

#[test]
fn a_run_resolves_by_alias_and_the_json_envelope_carries_id_and_alias() {
    let (world, ticket) = world_with_one_settled_run("by-alias.md");
    let alias = format!("{ticket}-r1");

    let data = World::json_stdout_or_stderr(&world.sloop(&["wait", &alias]))["data"].clone();
    assert_eq!(data["alias"], json!(alias));
    assert_eq!(data["id"], json!(world.run_id(1)));
    assert_eq!(data["terminal"], json!(true));
    // An alias names exactly one run, so there is nothing to disambiguate.
    assert_eq!(data["note"], json!(null));

    let logs = World::json_stdout(&world.sloop(&["logs", &alias]))["data"].clone();
    assert_eq!(logs["alias"], json!(alias));
    assert_eq!(logs["id"], json!(world.run_id(1)));
}

#[test]
fn a_bare_ticket_resolves_to_the_latest_attempt_and_names_earlier_ones() {
    let world = World::configured();
    // Failing runs leave the ticket retryable, so a second attempt is possible.
    configure_agent_script(&world, "exit 3\n");
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_and_run(&world, "attempts.md");
    wait_for_idle(&world);
    assert!(world.sloop(&["retry", &ticket]).status.success());
    assert!(world.sloop(&["run", &ticket]).status.success());
    wait_until("the second attempt settles", || world.run_ids().len() == 2);
    wait_for_idle(&world);

    // A bare ticket id picks the newest attempt...
    let data = World::json_stdout_or_stderr(&world.sloop(&["wait", &ticket]))["data"].clone();
    assert_eq!(data["alias"], json!(format!("{ticket}-r2")));
    assert_eq!(data["id"], json!(world.run_id(2)));
    assert_eq!(
        data["note"],
        json!(format!("showing {ticket}-r2; earlier attempts: r1"))
    );

    // ...and the earlier attempt stays reachable by its own alias.
    let first =
        World::json_stdout_or_stderr(&world.sloop(&["wait", &format!("{ticket}-r1")]))["data"]
            .clone();
    assert_eq!(first["id"], json!(world.run_id(1)));
    assert_eq!(first["note"], json!(null));

    // A ticket that never ran says so rather than reporting a missing run.
    let unrun = post(&world, "unrun.md");
    let error = World::json_stdout_or_stderr(&world.sloop(&["wait", &unrun]));
    assert_eq!(error["error"]["code"], json!("not_found"));
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("has no runs yet"),
        "{error}"
    );
}

#[test]
fn a_unique_hex_prefix_resolves_and_an_ambiguous_one_lists_the_candidates() {
    let (world, ticket) = world_with_one_settled_run("prefix.md");
    let id = world.run_id(1);

    let data = World::json_stdout_or_stderr(&world.sloop(&["wait", &id[..4]]))["data"].clone();
    assert_eq!(data["id"], json!(id));
    assert_eq!(data["alias"], json!(format!("{ticket}-r1")));

    // Two runs sharing a prefix cannot be told apart, so the error must name
    // both rather than silently picking one.
    let store = rusqlite::Connection::open(world.db_path()).expect("open state database");
    let twin = format!("{}ffffffffffffffffffffffffffff", &id[..4]);
    store
        .execute(
            "INSERT INTO runs (id, activation_id, ticket_id, state, attempt, created_at_ms,
                               updated_at_ms)
             SELECT ?1, activation_id, ticket_id, state, 2, created_at_ms, updated_at_ms
             FROM runs WHERE id = ?2",
            rusqlite::params![twin, id],
        )
        .expect("insert a colliding run");
    drop(store);

    let error = World::json_stdout_or_stderr(&world.sloop(&["wait", &id[..4]]));
    let message = error["error"]["message"].as_str().unwrap().to_owned();
    assert!(message.contains("is ambiguous"), "{message}");
    assert!(message.contains(&id[..8]), "{message}");
    assert!(message.contains(&twin[..8]), "{message}");
    assert!(message.contains(&format!("{ticket}-r1")), "{message}");
    assert!(message.contains(&format!("{ticket}-r2")), "{message}");
}

#[test]
fn an_unresolvable_reference_names_every_accepted_form() {
    let (world, _) = world_with_one_settled_run("unknown.md");

    let error = World::json_stdout_or_stderr(&world.sloop(&["wait", "not-a-run"]));
    assert_eq!(error["error"]["code"], json!("not_found"));
    let message = error["error"]["message"].as_str().unwrap().to_owned();
    for expected in ["an alias like", "a ticket id or name", "4 characters"] {
        assert!(message.contains(expected), "{message}");
    }
}

#[test]
fn legacy_ordinal_run_ids_keep_resolving_in_a_pre_existing_store() {
    let (world, ticket) = world_with_one_settled_run("legacy.md");

    // Rewrite the run to the `R<n>` shape a store written before this change
    // would hold. Nothing on disk is renamed, exactly as an upgrade would find.
    let store = rusqlite::Connection::open(world.db_path()).expect("open state database");
    store
        .execute("PRAGMA foreign_keys = OFF", [])
        .expect("relax foreign keys");
    store
        .execute(
            "UPDATE runs SET id = 'R1' WHERE id = ?1",
            rusqlite::params![world.run_id(1)],
        )
        .expect("rewrite the run id");
    drop(store);

    let data = World::json_stdout_or_stderr(&world.sloop(&["wait", "R1"]))["data"].clone();
    assert_eq!(data["id"], json!("R1"));
    // The alias still derives from stored ticket and attempt columns.
    assert_eq!(data["alias"], json!(format!("{ticket}-r1")));
}

#[test]
fn reindex_preserves_run_identity() {
    let (world, ticket) = world_with_one_settled_run("reindex.md");
    let before = world.run_ids();
    assert_eq!(before.len(), 1);

    assert!(world.sloop(&["reindex"]).status.success());

    assert_eq!(world.run_ids(), before, "reindex re-minted a run id");
    let data =
        World::json_stdout_or_stderr(&world.sloop(&["wait", &format!("{ticket}-r1")]))["data"]
            .clone();
    assert_eq!(data["id"], json!(before[0]));
}
