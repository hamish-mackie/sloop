mod support;

use std::fs;

use sloop::clock::{Clock, SystemClock};
use support::World;

#[test]
fn post_manual_stamps_and_registers_without_an_activation() {
    let world = World::configured();
    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        "version: 1\nagent:\n  default_target: fake\n  targets:\n    fake:\n      cmd: [fake, '{prompt}']\n",
    )
    .unwrap();
    world.start_daemon();
    let ticket = world.write_ticket(
        "cooldown.md",
        "---\nmodel: sonnet\neffort: medium\n---\n# Persist cooldowns\n",
    );
    let output = world.sloop(&[
        "post",
        ticket.to_str().expect("UTF-8 ticket path"),
        "--manual",
    ]);

    assert!(output.status.success());
    let response = World::json_stdout(&output);
    assert_eq!(response["ok"], true);
    assert_eq!(
        response["data"]["ticket"]["file"],
        ".agents/sloop/tickets/cooldown.md"
    );
    assert_eq!(response["data"]["ticket"]["project"], "default");
    assert_eq!(response["data"]["ticket"]["state"], "ready");
    assert_eq!(response["data"]["ticket"]["name"], "cooldown");
    assert_eq!(
        response["data"]["ticket"]["blocked_by"],
        serde_json::json!([])
    );
    assert_eq!(response["data"]["ticket"]["worktree"], "sloop/TICK-1");
    assert_eq!(response["data"]["ticket"]["target"], "fake");
    assert_eq!(response["data"]["ticket"]["model"], "sonnet");
    assert_eq!(response["data"]["ticket"]["effort"], "medium");
    assert_eq!(response["data"]["ticket"]["id"], "TICK-1");
    assert!(response["data"]["activation"].is_null());

    let contents = fs::read_to_string(world.root().join(ticket)).expect("read stamped ticket");
    assert!(
        contents.contains("id:"),
        "ticket was not stamped: {contents}"
    );
    assert!(contents.contains("project: default"));
    assert!(contents.contains("worktree: sloop/TICK-1"));
    assert!(contents.contains("# Persist cooldowns"));
}

#[test]
fn configured_content_directories_drive_project_indexing_and_post_validation() {
    let world = World::configured();
    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        concat!(
            "version: 1\n",
            "project_dir: planning/projects\n",
            "ticket_dir: planning/tickets\n",
        ),
    )
    .unwrap();
    fs::create_dir_all(world.root().join("planning/projects")).unwrap();
    fs::create_dir_all(world.root().join("planning/tickets")).unwrap();
    fs::write(
        world.root().join("planning/projects/team.md"),
        "---\nid: team\ntitle: Team\n---\nCustom project directory.\n",
    )
    .unwrap();
    fs::write(
        world.root().join("planning/tickets/custom.md"),
        "---\nname: Custom layout\nblocked_by: []\n---\nCustom ticket directory.\n",
    )
    .unwrap();
    world.start_daemon();

    let accepted = world.sloop(&[
        "post",
        "planning/tickets/custom.md",
        "--project",
        "team",
        "--manual",
    ]);
    assert!(
        accepted.status.success(),
        "{}",
        String::from_utf8_lossy(&accepted.stderr)
    );
    assert_eq!(
        World::json_stdout(&accepted)["data"]["ticket"]["file"],
        "planning/tickets/custom.md"
    );
    assert_eq!(
        World::json_stdout(&accepted)["data"]["ticket"]["project"],
        "team"
    );

    let rejected = world.sloop(&["post", ".agents/sloop/tickets/custom.md", "--manual"]);
    assert!(!rejected.status.success());
    assert!(
        World::json_stdout_or_stderr(&rejected)["error"]["message"]
            .as_str()
            .unwrap()
            .contains("planning/tickets")
    );
}

fn raw_ticket(world: &World, name: &str, content: &str) -> std::path::PathBuf {
    let path = std::path::PathBuf::from(".agents/sloop/tickets").join(name);
    fs::write(world.root().join(&path), content).unwrap();
    path
}

fn post_error(world: &World, path: &std::path::Path) -> serde_json::Value {
    let output = world.sloop(&["post", path.to_str().unwrap(), "--manual"]);
    assert!(!output.status.success());
    World::json_stdout_or_stderr(&output)
}

#[test]
fn post_rejects_each_incomplete_judgment_field() {
    let world = World::configured();
    world.start_daemon();
    let cases = [
        (
            "missing-name.md",
            "---\nblocked_by: []\n---\nbody\n",
            "name",
        ),
        (
            "missing-blockers.md",
            "---\nname: Missing blockers\n---\nbody\n",
            "blocked_by",
        ),
        (
            "empty-body.md",
            "---\nname: Empty body\nblocked_by: []\n---\n  \n",
            "body",
        ),
    ];

    for (file, content, field) in cases {
        let error = post_error(&world, &raw_ticket(&world, file, content));
        assert_eq!(error["error"]["code"], "invalid_arguments");
        let message = error["error"]["message"].as_str().unwrap();
        assert!(message.contains(field), "{message}");
        assert!(message.contains("add "), "{message}");
    }
}

#[test]
fn post_rejects_unknown_blocked_by_reference() {
    let world = World::configured();
    world.start_daemon();
    let ticket = raw_ticket(
        &world,
        "unknown-blocker.md",
        "---\nname: Unknown blocker\nblocked_by: [TICK-99]\n---\nbody\n",
    );

    let error = post_error(&world, &ticket);
    assert_eq!(error["error"]["code"], "not_found");
    let message = error["error"]["message"].as_str().unwrap();
    assert!(message.contains("blocked_by"), "{message}");
    assert!(message.contains("TICK-99"), "{message}");
}

#[test]
fn repost_rejects_a_dependency_cycle_and_keeps_the_previous_edges() {
    let world = World::configured();
    world.start_daemon();
    let first = raw_ticket(
        &world,
        "first.md",
        "---\nid: T1\nname: First\nblocked_by: []\n---\nfirst body\n",
    );
    let second = raw_ticket(
        &world,
        "second.md",
        "---\nid: T2\nname: Second\nblocked_by: [T1]\n---\nsecond body\n",
    );
    assert!(
        world
            .sloop(&["post", first.to_str().unwrap(), "--manual"])
            .status
            .success()
    );
    assert!(
        world
            .sloop(&["post", second.to_str().unwrap(), "--manual"])
            .status
            .success()
    );
    fs::write(
        world.root().join(&first),
        "---\nid: T1\nname: First\nblocked_by: [T2]\n---\nfirst body\n",
    )
    .unwrap();

    let error = post_error(&world, &first);
    assert_eq!(error["error"]["code"], "conflict");
    let message = error["error"]["message"].as_str().unwrap();
    assert!(message.contains("blocked_by"), "{message}");
    assert!(message.contains("T1 -> T2 -> T1"), "{message}");
}

#[test]
fn post_accepts_empty_blockers_and_preserves_an_explicit_worktree() {
    let world = World::configured();
    world.start_daemon();
    let ticket = raw_ticket(
        &world,
        "explicit.md",
        "---\nname: Explicit branch\nblocked_by: []\nworktree: feature/explicit\n---\nbody\n",
    );

    let output = world.sloop(&["post", ticket.to_str().unwrap(), "--manual"]);
    assert!(output.status.success());
    let response = World::json_stdout(&output);
    assert_eq!(
        response["data"]["ticket"]["blocked_by"],
        serde_json::json!([])
    );
    assert_eq!(response["data"]["ticket"]["worktree"], "feature/explicit");
    let content = fs::read_to_string(world.root().join(ticket)).unwrap();
    assert_eq!(content.matches("worktree:").count(), 1);
    assert!(content.contains("worktree: feature/explicit"));
}

#[test]
fn repost_refreshes_name_blockers_and_worktree_without_changing_identity() {
    let world = World::configured();
    world.start_daemon();
    let blocker = raw_ticket(
        &world,
        "blocker.md",
        "---\nid: T0\nname: Blocker\nblocked_by: []\n---\nblocker body\n",
    );
    let ticket = raw_ticket(
        &world,
        "subject.md",
        "---\nid: T1\nname: Old name\nblocked_by: []\nworktree: old/branch\n---\nsubject body\n",
    );
    assert!(
        world
            .sloop(&["post", blocker.to_str().unwrap(), "--manual"])
            .status
            .success()
    );
    let first = world.sloop(&["post", ticket.to_str().unwrap(), "--manual"]);
    assert!(first.status.success());
    fs::write(
        world.root().join(&ticket),
        "---\nid: T1\nproject: default\nname: New name\nblocked_by: [T0]\nworktree: new/branch\n---\nupdated subject body\n",
    )
    .unwrap();

    let second = world.sloop(&["post", ticket.to_str().unwrap(), "--manual"]);
    assert!(second.status.success());
    let response = World::json_stdout(&second);
    assert_eq!(response["data"]["ticket"]["id"], "T1");
    assert_eq!(response["data"]["ticket"]["name"], "New name");
    assert_eq!(
        response["data"]["ticket"]["blocked_by"],
        serde_json::json!(["T0"])
    );
    assert_eq!(response["data"]["ticket"]["worktree"], "new/branch");
    assert!(response["data"]["activation"].is_null());
    let connection = rusqlite::Connection::open(world.db_path()).unwrap();
    let body: String = connection
        .query_row("SELECT body FROM tickets WHERE id = 'T1'", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(body.trim(), "updated subject body");
}

#[test]
fn unknown_target_is_rejected_without_registering_or_activating_the_ticket() {
    let world = World::configured();
    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        "version: 1\nagent:\n  default_target: fake\n  targets:\n    fake:\n      cmd: [fake, '{prompt}']\n",
    )
    .unwrap();
    let ticket = world.write_ticket("unknown.md", "---\ntarget: absent\n---\n# Unknown target\n");
    world.start_daemon();

    let output = world.sloop(&["post", ticket.to_str().unwrap(), "--auto"]);
    assert!(!output.status.success());
    let error = World::json_stdout_or_stderr(&output);
    assert_eq!(error["error"]["code"], "invalid_arguments");
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("agent target `absent` is not configured")
    );

    let status = World::json_stdout(&world.sloop(&["status"]));
    assert_eq!(status["data"]["tickets"]["ready"], 0);
    assert!(
        status["data"]["queued_activations"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        !fs::read_to_string(world.root().join(ticket))
            .unwrap()
            .contains("id:")
    );
}

#[test]
fn post_defaults_to_auto_and_creates_one_queued_activation() {
    let world = World::configured();
    world.start_daemon();
    let ticket = world.write_ticket("cooldown.md", "# Persist cooldowns\n");
    let output = world.sloop(&["post", ticket.to_str().expect("UTF-8 ticket path")]);

    assert!(output.status.success());
    let response = World::json_stdout(&output);
    assert_eq!(response["data"]["activation"]["state"], "queued");
    assert_eq!(response["data"]["activation"]["kind"], "auto");
}

#[test]
fn post_at_queues_a_timed_activation_and_reposting_reschedules_it() {
    let world = World::configured();
    world.start_daemon();
    let ticket = world.write_ticket("cooldown.md", "# Persist cooldowns\n");
    let path = ticket.to_str().expect("UTF-8 ticket path");

    let output = world.sloop(&["post", path, "--at", "03:00"]);
    assert!(
        output.status.success(),
        "post --at failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let first = World::json_stdout(&output);
    assert_eq!(first["data"]["ticket"]["state"], "ready");
    assert_eq!(first["data"]["activation"]["kind"], "at");
    assert_eq!(first["data"]["activation"]["state"], "queued");
    let first_eligible = first["data"]["activation"]["eligible_at_ms"]
        .as_i64()
        .expect("timed activation carries its eligibility instant");
    assert!(first_eligible > world.now_ms());
    assert_eq!(
        SystemClock.local_minute(first_eligible),
        3 * 60,
        "eligibility lands on the next local 03:00"
    );

    let repost = world.sloop(&["post", path, "--at", "04:00"]);
    assert!(repost.status.success());
    let second = World::json_stdout(&repost);
    assert_eq!(
        second["data"]["activation"]["id"], first["data"]["activation"]["id"],
        "reposting reuses the queued activation"
    );
    assert_eq!(
        SystemClock.local_minute(
            second["data"]["activation"]["eligible_at_ms"]
                .as_i64()
                .expect("rescheduled activation carries its eligibility instant")
        ),
        4 * 60,
        "reposting moves the activation to the new local time"
    );
}

#[test]
fn post_hold_registers_a_held_ticket_without_an_activation() {
    let world = World::configured();
    world.start_daemon();
    let ticket = world.write_ticket("later.md", "# Do this later\n");
    let output = world.sloop(&[
        "post",
        ticket.to_str().expect("UTF-8 ticket path"),
        "--hold",
    ]);

    assert!(output.status.success());
    let response = World::json_stdout(&output);
    assert_eq!(response["data"]["ticket"]["state"], "held");
    assert!(response["data"]["activation"].is_null());
}

#[test]
fn real_daemon_allocates_monotonic_ticket_ids_with_a_repository_prefix() {
    let world = World::configured();
    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        "version: 1\nids:\n  ticket_prefix: WORK\n",
    )
    .expect("write configured ticket prefix");
    let first = world.write_ticket("first.md", "# First\n");
    let second = world.write_ticket("second.md", "# Second\n");
    world.start_daemon();

    let first_output = world.sloop(&[
        "post",
        first.to_str().expect("UTF-8 ticket path"),
        "--manual",
    ]);
    let second_output = world.sloop(&[
        "post",
        second.to_str().expect("UTF-8 ticket path"),
        "--manual",
    ]);

    assert_eq!(
        World::json_stdout(&first_output)["data"]["ticket"]["id"],
        "WORK-1"
    );
    assert_eq!(
        World::json_stdout(&second_output)["data"]["ticket"]["id"],
        "WORK-2"
    );
    assert!(
        fs::read_to_string(world.root().join(first))
            .expect("read first ticket")
            .contains("id: WORK-1")
    );
    assert!(
        fs::read_to_string(world.root().join(second))
            .expect("read second ticket")
            .contains("id: WORK-2")
    );
}

#[test]
fn post_without_flow_stamps_the_built_in_default_flow_name() {
    let world = World::configured();
    world.start_daemon();
    let ticket = world.write_ticket("cooldown.md", "# Persist cooldowns\n");

    let output = world.sloop(&["post", ticket.to_str().unwrap(), "--manual"]);

    assert!(output.status.success());
    let response = World::json_stdout(&output);
    assert_eq!(response["data"]["ticket"]["flow"], "default");
    let contents = fs::read_to_string(world.root().join(&ticket)).unwrap();
    assert!(contents.contains("flow: default"), "{contents}");
}

#[test]
fn explicit_flow_flag_is_honored_and_stamped() {
    let world = World::configured();
    fs::create_dir_all(world.root().join(".agents/sloop/flows")).unwrap();
    fs::write(
        world.root().join(".agents/sloop/flows/hotfix.yaml"),
        "- { name: build, kind: build }\n",
    )
    .unwrap();
    world.start_daemon();
    let ticket = world.write_ticket("urgent.md", "# Ship the hotfix\n");

    let output = world.sloop(&[
        "post",
        ticket.to_str().unwrap(),
        "--flow",
        "hotfix",
        "--manual",
    ]);

    assert!(output.status.success());
    let response = World::json_stdout(&output);
    assert_eq!(response["data"]["ticket"]["flow"], "hotfix");
    let contents = fs::read_to_string(world.root().join(&ticket)).unwrap();
    assert!(contents.contains("flow: hotfix"), "{contents}");
}

#[test]
fn a_flag_matching_the_stamped_flow_reposts_without_conflict() {
    let world = World::configured();
    world.start_daemon();
    let ticket = world.write_ticket("cooldown.md", "# Persist cooldowns\n");
    assert!(
        world
            .sloop(&["post", ticket.to_str().unwrap(), "--manual"])
            .status
            .success()
    );

    let output = world.sloop(&[
        "post",
        ticket.to_str().unwrap(),
        "--flow",
        "default",
        "--manual",
    ]);

    assert!(output.status.success());
    assert_eq!(
        World::json_stdout(&output)["data"]["ticket"]["flow"],
        "default"
    );
}

#[test]
fn a_flag_conflicting_with_the_stamped_flow_is_rejected() {
    let world = World::configured();
    fs::create_dir_all(world.root().join(".agents/sloop/flows")).unwrap();
    fs::write(
        world.root().join(".agents/sloop/flows/hotfix.yaml"),
        "- { name: build, kind: build }\n",
    )
    .unwrap();
    world.start_daemon();
    let ticket = world.write_ticket("cooldown.md", "# Persist cooldowns\n");
    assert!(
        world
            .sloop(&["post", ticket.to_str().unwrap(), "--manual"])
            .status
            .success()
    );

    let output = world.sloop(&[
        "post",
        ticket.to_str().unwrap(),
        "--flow",
        "hotfix",
        "--manual",
    ]);
    assert!(!output.status.success());
    let error = World::json_stdout_or_stderr(&output);
    assert_eq!(error["error"]["code"], "conflict");
    let message = error["error"]["message"].as_str().unwrap();
    assert!(message.contains("default"), "{message}");
    assert!(message.contains("hotfix"), "{message}");
}

#[test]
fn unknown_flow_is_rejected_and_names_known_flows() {
    let world = World::configured();
    fs::create_dir_all(world.root().join(".agents/sloop/flows")).unwrap();
    fs::write(
        world.root().join(".agents/sloop/flows/hotfix.yaml"),
        "- { name: build, kind: build }\n",
    )
    .unwrap();
    world.start_daemon();
    let ticket = world.write_ticket("urgent.md", "# Ship the hotfix\n");

    let output = world.sloop(&[
        "post",
        ticket.to_str().unwrap(),
        "--flow",
        "bogus",
        "--manual",
    ]);

    assert!(!output.status.success());
    let error = World::json_stdout_or_stderr(&output);
    assert_eq!(error["error"]["code"], "not_found");
    let message = error["error"]["message"].as_str().unwrap();
    assert!(message.contains("bogus"), "{message}");
    assert!(message.contains("default"), "{message}");
    assert!(message.contains("hotfix"), "{message}");
    assert!(
        !fs::read_to_string(world.root().join(&ticket))
            .unwrap()
            .contains("id:")
    );
}
