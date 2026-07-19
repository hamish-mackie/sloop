mod support;

use std::fs;
use std::process::Command;

use serde_json::Value;
use support::{FakeAgent, World, process_alive, wait_until};

fn write_project(world: &World, id: &str, title: &str) {
    fs::write(
        world.root().join(format!(".agents/sloop/projects/{id}.md")),
        format!("---\nid: {id}\ntitle: {title}\n---\n{title} project.\n"),
    )
    .expect("write project");
}

fn write_ticket(world: &World, file: &str, frontmatter: &str, body: &str) {
    fs::write(
        world.root().join(format!(".agents/sloop/tickets/{file}")),
        format!("---\n{frontmatter}---\n{body}\n"),
    )
    .expect("write ticket");
}

fn post_manual(world: &World, file: &str) {
    let output = world.sloop(&["post", &format!(".agents/sloop/tickets/{file}"), "--manual"]);
    assert!(
        output.status.success(),
        "post failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn commit_ticket_files(world: &World, message: &str) {
    let status = Command::new("git")
        .args(["add", ".agents/sloop/projects", ".agents/sloop/tickets"])
        .current_dir(world.root())
        .status()
        .expect("stage indexed files");
    assert!(status.success(), "git add failed with {status}");
    let status = Command::new("git")
        .args([
            "-c",
            "user.name=sloop-test",
            "-c",
            "user.email=sloop-test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            message,
        ])
        .current_dir(world.root())
        .status()
        .expect("commit indexed files");
    assert!(status.success(), "git commit failed with {status}");
}

fn stop_daemon(world: &World, pid: u32) {
    let output = world.sloop(&["stop"]);
    assert!(
        output.status.success(),
        "stop failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    wait_until("the daemon stops", || !process_alive(pid));
}

fn database_count(world: &World, table: &str) -> i64 {
    let connection = rusqlite::Connection::open(world.db_path()).expect("open state database");
    connection
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .expect("count database rows")
}

fn create_merged_branch(world: &World, branch: &str, file: &str) {
    let default_branch = Command::new("git")
        .args(["symbolic-ref", "--short", "HEAD"])
        .current_dir(world.root())
        .output()
        .expect("read default branch");
    assert!(default_branch.status.success());
    let default_branch = String::from_utf8(default_branch.stdout)
        .expect("default branch is UTF-8")
        .trim()
        .to_owned();
    for args in [
        vec!["checkout", "--quiet", "-b", branch],
        vec!["add", file],
        vec![
            "-c",
            "user.name=sloop-test",
            "-c",
            "user.email=sloop-test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "state evidence",
        ],
        vec!["checkout", "--quiet", &default_branch],
        vec!["merge", "--quiet", "--ff-only", branch],
    ] {
        if args.first() == Some(&"add") {
            fs::write(world.root().join(file), "state evidence\n").expect("write branch work");
        }
        let status = Command::new("git")
            .args(args)
            .current_dir(world.root())
            .status()
            .expect("update merged branch");
        assert!(status.success(), "git command failed with {status}");
    }
}

fn create_unmerged_worktree(world: &World, branch: &str, directory: &str) {
    let path = format!(".worktrees/{directory}");
    let status = Command::new("git")
        .args(["worktree", "add", "--quiet", "-b", branch, &path])
        .current_dir(world.root())
        .status()
        .expect("create orphan worktree");
    assert!(status.success(), "git worktree add failed with {status}");
    fs::write(world.root().join(&path).join("orphan-work.txt"), "work\n")
        .expect("write orphan work");
    for args in [
        ["add", "orphan-work.txt"].as_slice(),
        [
            "-c",
            "user.name=sloop-test",
            "-c",
            "user.email=sloop-test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "orphan work",
        ]
        .as_slice(),
    ] {
        let status = Command::new("git")
            .args(args)
            .current_dir(world.root().join(&path))
            .status()
            .expect("commit orphan work");
        assert!(status.success(), "git command failed with {status}");
    }
}

#[test]
fn reindex_does_not_treat_an_untouched_branch_as_work_after_a_rewrite() {
    let world = World::configured();
    write_ticket(
        &world,
        "rewrite.md",
        "id: T1\nproject: default\nname: Rewrite\nblocked_by: []\nworktree: sloop/T1\n",
        "# Keep an untouched branch ready",
    );
    world.commit_all("initial ticket");

    let branch = Command::new("git")
        .args(["branch", "sloop/T1"])
        .current_dir(world.root())
        .status()
        .expect("create ticket branch");
    assert!(branch.success());
    fs::write(world.root().join("rewritten.txt"), "rewritten\n").expect("write rewrite marker");
    let add = Command::new("git")
        .args(["add", "rewritten.txt"])
        .current_dir(world.root())
        .status()
        .expect("stage rewrite marker");
    assert!(add.success());
    let amend = Command::new("git")
        .args([
            "-c",
            "user.name=sloop-test",
            "-c",
            "user.email=sloop-test@example.invalid",
            "commit",
            "--quiet",
            "--amend",
            "--no-edit",
        ])
        .current_dir(world.root())
        .status()
        .expect("rewrite default branch");
    assert!(amend.success());

    let output = world.sloop(&["reindex"]);
    assert!(output.status.success());
    assert_eq!(
        World::json_stdout(&output)["data"]["tickets_state_changed"],
        0
    );
    let shown = world.sloop(&["show", "T1"]);
    assert!(shown.status.success());
    assert_eq!(
        World::json_stdout(&shown)["data"]["value"]["state"],
        "ready"
    );
}

#[test]
fn reindex_rebuilds_files_and_git_but_not_deleted_runtime_history() {
    let world = World::configured();
    world.configure_fake_agent(
        FakeAgent::new()
            .note("runtime-only note")
            .commit("completed ticket")
            .exit(0),
    );
    write_project(&world, "alpha", "Alpha");
    write_ticket(
        &world,
        "finished.md",
        "id: T1\nproject: alpha\nname: Finished\nblocked_by: []\nworktree: topic/finished\n",
        "# Finish the work",
    );
    write_ticket(
        &world,
        "follow-up.md",
        "id: T2\nproject: alpha\nname: Follow up\nblocked_by: [T1]\nworktree: topic/follow-up\n",
        "# Follow up after T1",
    );
    world.commit_all("initial indexed files");
    let pid = world.start_daemon()["data"]["pid"]
        .as_u64()
        .expect("daemon pid") as u32;
    post_manual(&world, "finished.md");
    post_manual(&world, "follow-up.md");
    assert!(world.sloop(&["run", "T1"]).status.success());
    wait_until("T1 is merged", || {
        World::json_stdout(&world.sloop(&["status"]))["data"]["tickets"]["merged"] == 1
    });
    commit_ticket_files(&world, "commit stamped ticket files");

    let before_list = World::json_stdout(&world.sloop(&["list"]))["data"].clone();
    let before_show = World::json_stdout(&world.sloop(&["show", "alpha"]))["data"].clone();
    assert_eq!(
        before_show["value"]["tickets"][0]["notes"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        before_show["value"]["tickets"][0]["commits"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let status = Command::new("git")
        .args(["worktree", "remove", ".worktrees/R1"])
        .current_dir(world.root())
        .status()
        .expect("remove completed run worktree");
    assert!(status.success(), "git worktree remove failed with {status}");

    stop_daemon(&world, pid);
    for path in [
        world.db_path(),
        world.db_path().with_extension("db-wal"),
        world.db_path().with_extension("db-shm"),
    ] {
        let _ = fs::remove_file(path);
    }
    world.start_daemon();

    let first = world.sloop(&["reindex"]);
    assert!(
        first.status.success(),
        "reindex failed: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first_data = World::json_stdout(&first)["data"].clone();
    assert_eq!(first_data["projects_indexed"], 2);
    assert_eq!(first_data["tickets_indexed"], 2);
    assert_eq!(first_data["tickets_state_changed"], 0);
    assert_eq!(first_data["state_changes"], serde_json::json!([]));
    assert_eq!(first_data["rows_dropped"], 0);
    assert_eq!(
        World::json_stdout(&world.sloop(&["list"]))["data"],
        before_list
    );

    let after_show = World::json_stdout(&world.sloop(&["show", "alpha"]))["data"].clone();
    assert_eq!(after_show["value"]["id"], before_show["value"]["id"]);
    assert_eq!(after_show["value"]["title"], before_show["value"]["title"]);
    for (before, after) in before_show["value"]["tickets"]
        .as_array()
        .unwrap()
        .iter()
        .zip(after_show["value"]["tickets"].as_array().unwrap())
    {
        assert_eq!(after["id"], before["id"]);
        assert_eq!(after["name"], before["name"]);
        assert_eq!(after["state"], before["state"]);
        assert_eq!(after["notes"], serde_json::json!([]));
        assert_eq!(after["commits"], serde_json::json!([]));
    }
    let finished = World::json_stdout(&world.sloop(&["show", "T1"]))["data"]["value"].clone();
    assert_eq!(finished["project"], "alpha");
    assert_eq!(finished["state"], "merged");
    assert_eq!(finished["blocked_by"], serde_json::json!([]));
    assert_eq!(finished["worktree"], "topic/finished");
    let follow_up = World::json_stdout(&world.sloop(&["show", "T2"]))["data"]["value"].clone();
    assert_eq!(follow_up["project"], "alpha");
    assert_eq!(follow_up["state"], "ready");
    assert_eq!(follow_up["blocked_by"], serde_json::json!(["T1"]));
    assert_eq!(follow_up["worktree"], "topic/follow-up");
    assert_eq!(database_count(&world, "runs"), 0);
    assert_eq!(database_count(&world, "notes"), 0);

    let second = world.sloop(&["reindex"]);
    assert!(second.status.success());
    assert_eq!(World::json_stdout(&second)["data"], first_data);
    assert_eq!(
        World::json_stdout(&world.sloop(&["list"]))["data"],
        before_list
    );

    let human = world.sloop_plain(&["reindex"]);
    assert!(human.status.success());
    assert!(String::from_utf8_lossy(&human.stdout).contains("reindexed 2 projects and 2 tickets"));

    assert!(world.sloop(&["run", "T2"]).status.success());
    wait_until("a fresh run succeeds after database recovery", || {
        World::json_stdout(&world.sloop(&["status"]))["data"]["tickets"]["merged"] == 2
    });
    let connection = rusqlite::Connection::open(world.db_path()).expect("open state database");
    let run_id: String = connection
        .query_row("SELECT id FROM runs", [], |row| row.get(0))
        .expect("read recovered run ID");
    assert_eq!(run_id, "R2");
}

#[test]
fn reindex_drops_history_for_tickets_removed_from_files() {
    let world = World::configured();
    world.configure_fake_agent(
        FakeAgent::new()
            .note("discard me")
            .commit("stale ticket work")
            .exit(0),
    );
    write_ticket(
        &world,
        "stale.md",
        "id: T1\nproject: default\nname: Stale\nblocked_by: []\n",
        "# Remove this ticket",
    );
    write_ticket(
        &world,
        "kept.md",
        "id: T2\nproject: default\nname: Kept\nblocked_by: []\n",
        "# Keep this ticket and its history",
    );
    write_ticket(
        &world,
        "state.md",
        "id: T3\nproject: default\nname: State\nblocked_by: []\n",
        "# Derive this ticket state from Git",
    );
    write_ticket(
        &world,
        "bare.md",
        "id: T4\nproject: default\nname: Bare\nblocked_by: []\n",
        "# A bare branch is not completed work",
    );
    write_ticket(
        &world,
        "held.md",
        "id: T5\nproject: default\nname: Held\nblocked_by: []\n",
        "# Preserve this runtime hold",
    );
    write_ticket(
        &world,
        "orphan.md",
        "id: T6\nproject: default\nname: Orphan\nblocked_by: []\n",
        "# Recover this orphaned worktree",
    );
    world.commit_all("initial tickets");
    world.start_daemon();
    post_manual(&world, "stale.md");
    post_manual(&world, "kept.md");
    post_manual(&world, "state.md");
    post_manual(&world, "bare.md");
    let held = world.sloop(&["post", ".agents/sloop/tickets/held.md", "--hold"]);
    assert!(held.status.success());
    post_manual(&world, "orphan.md");
    assert!(world.sloop(&["run", "T2"]).status.success());
    wait_until("the kept ticket run finishes", || {
        World::json_stdout(&world.sloop(&["status"]))["data"]["tickets"]["merged"] == 1
    });
    assert!(world.sloop(&["run", "T1"]).status.success());
    wait_until("the stale ticket run finishes", || {
        World::json_stdout(&world.sloop(&["status"]))["data"]["tickets"]["merged"] == 2
    });
    assert_eq!(database_count(&world, "runs"), 2);
    assert_eq!(database_count(&world, "notes"), 2);
    fs::remove_file(world.root().join(".agents/sloop/tickets/stale.md"))
        .expect("remove stale ticket file");
    create_merged_branch(&world, "sloop/T3", "state-evidence.txt");
    create_unmerged_worktree(&world, "sloop/T6-a1-R99", "R99");
    let status = Command::new("git")
        .args(["branch", "sloop/T4"])
        .current_dir(world.root())
        .status()
        .expect("create a bare ticket branch");
    assert!(status.success(), "git branch failed with {status}");

    let output = world.sloop(&["reindex"]);
    assert!(
        output.status.success(),
        "reindex failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let data = World::json_stdout(&output)["data"].clone();
    assert_eq!(data["tickets_indexed"], 5);
    assert_eq!(data["tickets_state_changed"], 2);
    assert_eq!(
        data["state_changes"],
        serde_json::json!([
            {"ticket": "T3", "previous_state": "ready", "state": "merged"},
            {"ticket": "T6", "previous_state": "ready", "state": "needs_review"}
        ])
    );
    assert!(data["rows_dropped"].as_u64().unwrap() >= 5, "{data}");
    assert_eq!(database_count(&world, "runs"), 1);
    assert_eq!(database_count(&world, "notes"), 1);
    let shown = world.sloop(&["show", "T1"]);
    assert!(!shown.status.success());
    assert_eq!(
        World::json_stdout_or_stderr(&shown)["error"]["code"],
        "not_found"
    );
    let project = World::json_stdout(&world.sloop(&["show", "default"]))["data"].clone();
    let kept = project["value"]["tickets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|ticket| ticket["id"] == "T2")
        .expect("kept ticket remains indexed");
    assert_eq!(kept["notes"].as_array().unwrap().len(), 1);
    assert_eq!(kept["commits"].as_array().unwrap().len(), 1);
    let bare = World::json_stdout(&world.sloop(&["show", "T4"]))["data"]["value"].clone();
    assert_eq!(bare["state"], "ready");
    let held = World::json_stdout(&world.sloop(&["show", "T5"]))["data"]["value"].clone();
    assert_eq!(held["state"], "held");
    let orphan = World::json_stdout(&world.sloop(&["show", "T6"]))["data"]["value"].clone();
    assert_eq!(orphan["state"], "needs_review");

    assert!(world.sloop(&["run", "T4"]).status.success());
    wait_until("the post-reindex run finishes", || {
        World::json_stdout(&world.sloop(&["status"]))["data"]["tickets"]["merged"] == 3
    });
    let connection = rusqlite::Connection::open(world.db_path()).expect("open state database");
    let run_ids = connection
        .prepare("SELECT id FROM runs ORDER BY id")
        .expect("prepare run ID query")
        .query_map([], |row| row.get::<_, String>(0))
        .expect("query run IDs")
        .collect::<Result<Vec<_>, _>>()
        .expect("read run IDs");
    assert_eq!(run_ids, ["R1", "R100"]);
    assert_eq!(database_count(&world, "notes"), 2);
}

#[test]
fn reindex_is_rejected_while_an_agent_is_active() {
    let world = World::configured();
    world.configure_fake_agent(
        FakeAgent::new()
            .block_until_released("active")
            .commit("released work")
            .exit(0),
    );
    write_ticket(
        &world,
        "active.md",
        "id: T1\nproject: default\nname: Active\nblocked_by: []\n",
        "# Stay active",
    );
    world.commit_all("active ticket");
    world.start_daemon();
    post_manual(&world, "active.md");
    assert!(world.sloop(&["run", "T1"]).status.success());
    wait_until("the fake agent is active", || {
        world.fake_agent_reached("active")
    });

    let output = world.sloop(&["reindex"]);
    assert!(!output.status.success());
    let response: Value = World::json_stdout_or_stderr(&output);
    assert_eq!(response["error"]["code"], "conflict");
    assert!(
        response["error"]["message"]
            .as_str()
            .unwrap()
            .contains("R1")
    );

    world.release("active");
    wait_until("the released agent finishes", || {
        World::json_stdout(&world.sloop(&["status"]))["data"]["tickets"]["merged"] == 1
    });
}

#[test]
fn reindex_preserves_project_scoped_run_history_when_a_ticket_moves() {
    let world = World::configured();
    world.configure_fake_agent(
        FakeAgent::new()
            .note("keep project history")
            .commit("project-scoped work")
            .exit(0),
    );
    write_project(&world, "old", "Old");
    write_project(&world, "new", "New");
    write_ticket(
        &world,
        "moving.md",
        "id: T1\nproject: old\nname: Moving\nblocked_by: []\n",
        "# Move between projects",
    );
    world.commit_all("project move setup");
    world.start_daemon();
    post_manual(&world, "moving.md");
    assert!(world.sloop(&["run", "--project", "old"]).status.success());
    wait_until("the project-scoped run finishes", || {
        World::json_stdout(&world.sloop(&["status"]))["data"]["tickets"]["merged"] == 1
    });

    let ticket_path = world.root().join(".agents/sloop/tickets/moving.md");
    let ticket = fs::read_to_string(&ticket_path)
        .expect("read moving ticket")
        .replace("project: old", "project: new");
    fs::write(&ticket_path, ticket).expect("move ticket project");
    fs::remove_file(world.root().join(".agents/sloop/projects/old.md"))
        .expect("remove old project");
    commit_ticket_files(&world, "move ticket to new project");

    let output = world.sloop(&["reindex"]);
    assert!(
        output.status.success(),
        "reindex failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(database_count(&world, "runs"), 1);
    assert_eq!(database_count(&world, "notes"), 1);
    let ticket = World::json_stdout(&world.sloop(&["show", "T1"]))["data"]["value"].clone();
    assert_eq!(ticket["project"], "new");
    let project = World::json_stdout(&world.sloop(&["show", "new"]))["data"].clone();
    assert_eq!(
        project["value"]["tickets"][0]["notes"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        project["value"]["tickets"][0]["commits"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let old = world.sloop(&["show", "old"]);
    assert!(!old.status.success());
    assert_eq!(
        World::json_stdout_or_stderr(&old)["error"]["code"],
        "not_found"
    );
}

#[test]
fn reindex_holds_a_stale_blocked_by_and_names_the_file_to_edit() {
    let world = World::configured();
    write_ticket(
        &world,
        "orphan.md",
        "id: T1\nproject: default\nname: Orphan\nblocked_by: [T404]\n",
        "# Depends on a ticket that does not exist",
    );
    world.commit_all("stale dependency");

    let output = world.sloop(&["reindex"]);
    assert!(output.status.success());
    let response = World::json_stdout(&world.sloop(&["list"]));
    let ticket = &response["data"]["tickets"][0];
    assert_eq!(ticket["state"], "held");
    let message = ticket["reason"].as_str().expect("held reason");
    assert!(
        message.contains("references unknown ticket `T404`"),
        "diagnostic changed: {message}"
    );
    assert!(
        message.contains("orphan.md"),
        "remedy does not name the file to edit: {message}"
    );
}

#[test]
fn reindex_holds_an_unknown_flow_without_blocking_valid_siblings_and_releases_when_fixed() {
    let world = World::configured();
    write_ticket(
        &world,
        "broken.md",
        "id: T1\nproject: default\nname: Broken\nblocked_by: []\nflow: missing\n",
        "# Uses a missing flow",
    );
    write_ticket(
        &world,
        "valid.md",
        "id: T2\nproject: default\nname: Valid\nblocked_by: []\nflow: default\n",
        "# Uses the default flow",
    );
    world.commit_all("tickets with mixed flow validity");

    let output = world.sloop(&["reindex"]);
    assert!(
        output.status.success(),
        "reindex failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let response = World::json_stdout(&world.sloop(&["list"]));
    let tickets = response["data"]["tickets"].as_array().unwrap();
    let broken = tickets.iter().find(|ticket| ticket["id"] == "T1").unwrap();
    let valid = tickets.iter().find(|ticket| ticket["id"] == "T2").unwrap();
    assert_eq!(broken["state"], "held");
    assert!(
        broken["reason"]
            .as_str()
            .unwrap()
            .contains("flow `missing` is not defined")
    );
    assert_eq!(valid["state"], "ready");
    let human = world.sloop_plain(&["list"]);
    assert!(String::from_utf8_lossy(&human.stdout).contains("flow `missing` is not defined"));

    write_ticket(
        &world,
        "broken.md",
        "id: T1\nproject: default\nname: Broken\nblocked_by: []\nflow: default\n",
        "# Uses the default flow now",
    );
    commit_ticket_files(&world, "fix missing flow");
    let output = world.sloop(&["reindex"]);
    assert!(output.status.success());
    let response = World::json_stdout(&world.sloop(&["list"]));
    let broken = response["data"]["tickets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|ticket| ticket["id"] == "T1")
        .unwrap();
    assert_eq!(broken["state"], "ready");
    assert!(
        !broken["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("flow `missing`"))
    );
}

#[test]
fn reindex_holds_an_unknown_target_without_blocking_valid_siblings() {
    let world = World::configured();
    world.configure_fake_agent(FakeAgent::new().exit(0));
    write_ticket(
        &world,
        "broken-target.md",
        "id: T1\nproject: default\nname: Broken target\nblocked_by: []\ntarget: missing\n",
        "# Uses a missing target",
    );
    write_ticket(
        &world,
        "valid-target.md",
        "id: T2\nproject: default\nname: Valid target\nblocked_by: []\ntarget: fake\n",
        "# Uses the configured target",
    );
    world.commit_all("tickets with mixed target validity");

    let output = world.sloop(&["reindex"]);
    assert!(
        output.status.success(),
        "reindex failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let response = World::json_stdout(&world.sloop(&["list"]));
    let tickets = response["data"]["tickets"].as_array().unwrap();
    let broken = tickets.iter().find(|ticket| ticket["id"] == "T1").unwrap();
    let valid = tickets.iter().find(|ticket| ticket["id"] == "T2").unwrap();
    assert_eq!(broken["state"], "held");
    assert!(
        broken["reason"]
            .as_str()
            .unwrap()
            .contains("agent target `missing` is not configured")
    );
    assert_eq!(valid["state"], "ready");
}

#[test]
fn exec_source_pulls_tickets_and_receives_the_final_outcome() {
    let world = World::configured();
    world.configure_fake_agent(FakeAgent::new().exit(1));
    let report_path = world.root().join("source-report.json");
    let script = world.root().join("ticket-source.sh");
    fs::write(
        &script,
        format!(
            "#!/bin/sh\nset -eu\nrequest=$(cat)\ncase \"$request\" in\n  *'\"verb\":\"pull\"'*) printf '%s\\n' '[{{\"id\":\"EXT-1\",\"name\":\"External ticket\",\"project\":\"default\",\"blocked_by\":[],\"target\":\"fake\",\"flow\":\"default\",\"body\":\"Do external work\"}}]' ;;\n  *'\"verb\":\"report\"'*) printf '%s' \"$request\" > '{}' ;;\n  *) exit 2 ;;\nesac\n",
            report_path.display()
        ),
    )
    .expect("write ticket source");
    let config_path = world.root().join(".agents/sloop/config.yaml");
    let mut config = fs::read_to_string(&config_path).expect("read config");
    config.push_str("sources:\n  tickets:\n    exec: [\"sh\", \"./ticket-source.sh\"]\n");
    fs::write(config_path, config).expect("configure ticket source");
    world.commit_all("configure exec ticket source");

    let output = world.sloop(&["reindex"]);
    assert!(
        output.status.success(),
        "reindex failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let list = World::json_stdout(&world.sloop(&["list"]));
    assert_eq!(list["data"]["tickets"][0]["id"], "EXT-1");
    assert_eq!(list["data"]["tickets"][0]["state"], "ready");

    let run = world.sloop(&["run", "EXT-1"]);
    assert!(
        run.status.success(),
        "run failed: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    wait_until("exec source report", || report_path.is_file());
    let report: Value =
        serde_json::from_str(&fs::read_to_string(&report_path).expect("read source report"))
            .expect("parse source report");
    assert_eq!(report["verb"], "report");
    assert_eq!(report["ticket"], "EXT-1");
    assert_eq!(report["outcome"], "failed");
}
