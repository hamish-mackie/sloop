mod support;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use support::{World, wait_until};

#[test]
fn brief_sends_an_authenticated_worker_request() {
    let world = World::configured();
    let reply = json!({
        "id": "req-1",
        "ok": true,
        "data": {
            "run": "R1",
            "ticket": {"id": "T1", "body": "Persist cooldowns", "acceptance": []},
            "worktree": "/repo/.worktrees/R1",
            "branch": "sloop/R1-T1",
            "definition_of_done": ["Changes committed", "Tests pass"]
        }
    });
    let (output, request) = world.worker_exchange(&["brief"], reply.clone());

    assert!(output.status.success());
    assert_eq!(World::json_stdout(&output), reply);
    assert_eq!(request["v"], 1);
    assert_eq!(request["verb"], "brief");
    assert_eq!(request["args"], json!({}));
    assert_eq!(request["token"], "test-worker-token");
}

#[test]
fn show_sends_the_requested_reference() {
    let world = World::configured();
    let reply = json!({
        "id": "req-1",
        "ok": true,
        "data": {"ref": "T1", "kind": "ticket", "value": {"id": "T1"}}
    });
    let (output, request) = world.worker_exchange(&["show", "T1"], reply.clone());

    assert!(output.status.success());
    assert_eq!(World::json_stdout(&output), reply);
    assert_eq!(request["verb"], "show");
    assert_eq!(request["args"], json!({"ref": "T1"}));
    assert_eq!(request["token"], "test-worker-token");
}

#[test]
fn note_preserves_the_complete_note_text() {
    let world = World::configured();
    let reply = json!({
        "id": "req-1",
        "ok": true,
        "data": {"note": {"id": "N1", "run": "R1", "text": "work in progress"}}
    });
    let (output, request) =
        world.worker_exchange(&["note", "work", "in", "progress"], reply.clone());

    assert!(output.status.success());
    assert_eq!(World::json_stdout(&output), reply);
    assert_eq!(request["verb"], "note");
    assert_eq!(request["args"], json!({"text": "work in progress"}));
    assert_eq!(request["token"], "test-worker-token");
}

#[test]
fn worker_verbs_reject_missing_worker_context() {
    let world = World::configured();
    let output = world.sloop(&["brief"]);

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let response: serde_json::Value =
        serde_json::from_slice(&output.stderr).expect("stderr is JSON");
    assert_eq!(response["ok"], false);
    assert_eq!(response["error"]["code"], "unauthorized");
}

/// Writes a fake agent that exercises the worker verbs from inside its run,
/// recording each reply in the worktree. `blocking` agents wait for `release`
/// in the repository root so a test can inspect the live worker socket.
fn configure_worker_agent(world: &World, blocking: bool) {
    fs::create_dir_all(world.root().join(".agents/sloop/flows")).unwrap();
    fs::write(
        world.root().join(".agents/sloop/flows/default.yaml"),
        "stages:\n  - { name: build, kind: build }\n  - { name: merge, kind: merge }\n",
    )
    .unwrap();
    let script = world.root().join("worker-agent.sh");
    let release = world.root().join("release");
    let wait_loop = if blocking {
        format!(
            "tries=0\nwhile [ ! -e \"{}\" ] && [ \"$tries\" -lt 200 ]; do sleep 0.05; tries=$((tries + 1)); done\n",
            release.display()
        )
    } else {
        String::new()
    };
    fs::write(
        &script,
        format!(
            "#!/bin/sh\n\
             SLOOP=\"{sloop}\"\n\
             {wait_loop}\
             \"$SLOOP\" --json brief > brief.json 2> brief.err\n\
             \"$SLOOP\" --json show \"$SLOOP_TICKET_ID\" > show.json 2> show.err\n\
             \"$SLOOP\" --json show T999 > foreign-show.out 2> foreign-show.json\n\
             echo $? > foreign-show.exit\n\
             \"$SLOOP\" --json note work in progress > note.json 2> note.err\n\
             exit 0\n",
            sloop = env!("CARGO_BIN_EXE_sloop"),
        ),
    )
    .expect("write worker agent script");

    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        format!(
            "version: 1\nscheduler:\n  max_parallel_tasks: 1\nagent:\n  default_target: fake\n  targets:\n    fake:\n      cmd: [\"sh\", \"{}\", \"{{prompt}}\"]\n",
            script.display()
        ),
    )
    .expect("write agent config");
}

fn post_manual(world: &World, name: &str, body: &str) -> String {
    let ticket = world.write_ticket(name, body);
    let output = world.sloop(&[
        "post",
        ticket.to_str().expect("UTF-8 ticket path"),
        "--manual",
    ]);
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

fn post_manual_in_project(world: &World, name: &str, body: &str, project: &str) -> String {
    let ticket = world.write_ticket(name, body);
    let output = world.sloop(&[
        "post",
        ticket.to_str().expect("UTF-8 ticket path"),
        "--project",
        project,
        "--manual",
    ]);
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

fn file_snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, directory: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(directory).expect("read snapshot directory") {
            let path = entry.expect("read snapshot entry").path();
            if path.is_dir() {
                visit(root, &path, files);
            } else {
                files.insert(
                    path.strip_prefix(root)
                        .expect("snapshot path below root")
                        .into(),
                    fs::read(&path).expect("read snapshot file"),
                );
            }
        }
    }

    let mut files = BTreeMap::new();
    visit(root, root, &mut files);
    files
}

fn run_settled(world: &World) -> bool {
    let output = world.sloop(&["status"]);
    assert!(output.status.success());
    World::json_stdout(&output)["data"]["gate"]["active_agents"] == 0
}

fn worktree_json(world: &World, run: &str, name: &str) -> Value {
    let path = world.root().join(".worktrees").join(run).join(name);
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(text.trim()).unwrap_or_else(|error| panic!("{name} is JSON: {error}"))
}

#[test]
fn a_running_agent_reads_its_brief_and_records_a_note() {
    let world = World::configured();
    configure_worker_agent(&world, false);
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_manual(
        &world,
        "cooldown.md",
        "# Persist cooldowns\n\nSurvive restarts.\n",
    );

    assert!(world.sloop(&["run", &ticket]).status.success());
    wait_until("the run settles", || run_settled(&world));

    let brief = worktree_json(&world, "R1", "brief.json");
    assert_eq!(brief["ok"], true, "brief failed: {brief}");
    assert_eq!(brief["data"]["run"], "R1");
    assert_eq!(brief["data"]["ticket"]["id"], ticket.as_str());
    assert_eq!(brief["data"]["ticket"]["name"], "cooldown");
    assert_eq!(brief["data"]["ticket"]["blocked_by"], serde_json::json!([]));
    assert_eq!(brief["data"]["ticket"]["worktree"], "sloop/TICK-1");
    assert_eq!(brief["data"]["ticket"]["target"], "fake");
    let body = brief["data"]["ticket"]["body"].as_str().expect("body");
    assert!(body.contains("Persist cooldowns"), "brief body: {body}");
    assert!(
        brief["data"]["worktree"]
            .as_str()
            .expect("worktree")
            .ends_with("R1")
    );
    assert!(
        brief["data"]["branch"]
            .as_str()
            .expect("branch")
            .starts_with("sloop/")
    );
    assert!(
        !brief["data"]["definition_of_done"]
            .as_array()
            .expect("definition_of_done")
            .is_empty()
    );

    let show = worktree_json(&world, "R1", "show.json");
    assert_eq!(show["ok"], true, "show failed: {show}");
    assert_eq!(show["data"]["ref"], ticket.as_str());
    assert_eq!(show["data"]["kind"], "ticket");
    assert_eq!(show["data"]["value"]["name"], "cooldown");
    assert_eq!(show["data"]["value"]["blocked_by"], serde_json::json!([]));
    assert_eq!(show["data"]["value"]["worktree"], "sloop/TICK-1");
    assert_eq!(show["data"]["value"]["target"], "fake");

    // `show` is scoped to the run's own ticket; everything else is
    // unauthorized, whether or not it exists.
    let foreign = worktree_json(&world, "R1", "foreign-show.json");
    assert_eq!(foreign["ok"], false);
    assert_eq!(foreign["error"]["code"], "unauthorized");

    let note = worktree_json(&world, "R1", "note.json");
    assert_eq!(note["ok"], true, "note failed: {note}");
    assert_eq!(note["data"]["note"]["run"], "R1");
    assert_eq!(note["data"]["note"]["text"], "work in progress");

    // The note is durable evidence, not a courtesy reply.
    let store = sloop::store::Store::open(&world.db_path(), 0).expect("open runtime store");
    let notes = store.notes_for_run("R1").expect("read notes");
    assert_eq!(notes, vec!["work in progress".to_owned()]);
}

#[test]
fn a_worker_brief_uses_the_ticket_body_captured_at_claim() {
    let world = World::configured();
    configure_worker_agent(&world, true);
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_manual(
        &world,
        "admission.md",
        "# Admission body\n\nOriginal instructions.\n",
    );

    assert!(world.sloop(&["run", &ticket]).status.success());
    wait_until("the claimed run starts", || {
        world.worker_socket("R1").exists()
    });
    fs::write(
        world.root().join(".agents/sloop/tickets/admission.md"),
        "# Changed after claim\n",
    )
    .expect("edit source ticket after claim");
    fs::write(world.root().join("release"), "go\n").expect("release the agent");
    wait_until("the run settles", || run_settled(&world));

    let brief = worktree_json(&world, "R1", "brief.json");
    let body = brief["data"]["ticket"]["body"].as_str().expect("body");
    assert!(body.contains("Original instructions"), "brief body: {body}");
    assert!(!body.contains("Changed after claim"), "brief body: {body}");
}

#[test]
fn project_show_groups_notes_and_git_commits_without_writing_source_files() {
    let world = World::configured();
    fs::create_dir_all(world.root().join(".agents/sloop/flows")).unwrap();
    fs::write(
        world.root().join(".agents/sloop/flows/default.yaml"),
        "stages:\n  - { name: build, kind: build }\n  - { name: merge, kind: merge }\n",
    )
    .unwrap();
    let script = world.root().join("activity-agent.sh");
    fs::write(
        &script,
        format!(
            "#!/bin/sh\n\
             set -eu\n\
             SLOOP={}\n\
             \"$SLOOP\" --json show \"$SLOOP_TICKET_ID\" > ticket-show.json\n\
             \"$SLOOP\" --json note \"note from $SLOOP_TICKET_ID\" >/dev/null\n\
             git -c user.name=sloop-test-agent -c user.email=sloop-test-agent@example.invalid commit --quiet --allow-empty -m \"commit from $SLOOP_TICKET_ID\"\n",
            serde_json::to_string(env!("CARGO_BIN_EXE_sloop")).expect("quote sloop path"),
        ),
    )
    .expect("write activity agent");
    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        format!(
            "version: 1\nscheduler:\n  max_parallel_tasks: 1\nagent:\n  default_target: fake\n  targets:\n    fake:\n      cmd: [\"sh\", {}, \"{{prompt}}\"]\n",
            serde_json::to_string(&script.to_string_lossy()).expect("quote agent path"),
        ),
    )
    .expect("write activity agent config");
    fs::write(
        world.root().join(".agents/sloop/projects/activity.md"),
        "---\nid: activity\ntitle: Activity\n---\nHuman-authored project description.\n",
    )
    .expect("write activity project");
    world.commit_all("initial");
    world.start_daemon();

    let first = post_manual_in_project(&world, "first.md", "# First\n", "activity");
    let second = post_manual_in_project(&world, "second.md", "# Second\n", "activity");
    let source_root = world.root().join(".agents/sloop");
    let before_show = file_snapshot(&source_root);

    assert!(world.sloop(&["run", &first]).status.success());
    wait_until("the first activity run settles", || run_settled(&world));
    assert!(world.sloop(&["run", &second]).status.success());
    wait_until("the second activity run settles", || run_settled(&world));

    let output = world.sloop(&["show", "activity"]);
    assert!(
        output.status.success(),
        "project show failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let show = World::json_stdout(&output);
    assert_eq!(show["data"]["ref"], "activity");
    assert_eq!(show["data"]["kind"], "project");
    let tickets = show["data"]["value"]["tickets"]
        .as_array()
        .expect("project tickets");
    for (ticket_id, run_id) in [(&first, "R1"), (&second, "R2")] {
        let ticket = tickets
            .iter()
            .find(|ticket| ticket["id"] == ticket_id.as_str())
            .expect("ticket activity group");
        assert_eq!(ticket["notes"].as_array().expect("notes").len(), 1);
        assert_eq!(ticket["notes"][0]["run"], run_id);
        assert_eq!(ticket["notes"][0]["text"], format!("note from {ticket_id}"));
        assert_eq!(ticket["commits"].as_array().expect("commits").len(), 1);
        assert_eq!(ticket["commits"][0]["run"], run_id);
        assert_eq!(
            ticket["commits"][0]["message"],
            format!("commit from {ticket_id}")
        );
        assert!(
            ticket["commits"][0]["hash"]
                .as_str()
                .is_some_and(|hash| !hash.is_empty())
        );
    }

    let ticket_show = worktree_json(&world, "R1", "ticket-show.json");
    assert_eq!(ticket_show["data"]["ref"], first);
    assert_eq!(ticket_show["data"]["kind"], "ticket");
    assert_eq!(ticket_show["data"]["value"]["name"], "first");
    assert_eq!(ticket_show["data"]["value"]["blocked_by"], json!([]));
    assert_eq!(ticket_show["data"]["value"]["worktree"], "sloop/TICK-1");
    assert_eq!(ticket_show["data"]["value"]["target"], "fake");

    let human = world.sloop_plain(&["show", "activity"]);
    assert!(human.status.success());
    let human = String::from_utf8(human.stdout).expect("human output is UTF-8");
    assert!(
        human.contains(&format!("{first}  first  (merged)")),
        "{human}"
    );
    assert!(human.contains(&format!("note from {second}")), "{human}");
    assert!(human.contains(&format!("commit from {first}")), "{human}");

    assert_eq!(file_snapshot(&source_root), before_show);
}

#[test]
fn the_worker_socket_rejects_wrong_tokens_and_operator_verbs() {
    let world = World::configured();
    configure_worker_agent(&world, true);
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_manual(&world, "gate.md", "# Gate\n");
    assert!(world.sloop(&["run", &ticket]).status.success());

    let socket: PathBuf = world.worker_socket("R1");
    wait_until("the worker socket appears", || socket.exists());

    let wrong_token = World::socket_exchange(
        &socket,
        r#"{"v":1,"id":"req-1","verb":"brief","args":{},"token":"wrong"}"#,
    );
    assert_eq!(wrong_token["ok"], false);
    assert_eq!(wrong_token["error"]["code"], "unauthorized");

    let missing_token = World::socket_exchange(
        &socket,
        r#"{"v":1,"id":"req-2","verb":"brief","args":{},"token":null}"#,
    );
    assert_eq!(missing_token["ok"], false);
    assert_eq!(missing_token["error"]["code"], "unauthorized");

    let operator_verb = World::socket_exchange(
        &socket,
        r#"{"v":1,"id":"req-3","verb":"status","args":{},"token":"wrong"}"#,
    );
    assert_eq!(operator_verb["ok"], false);
    assert_eq!(operator_verb["error"]["code"], "unauthorized");

    fs::write(world.root().join("release"), "go\n").expect("release the agent");
    wait_until("the run settles", || run_settled(&world));
    // The token dies with the run: the per-run socket is torn down.
    wait_until("the worker socket is removed", || !socket.exists());
}
