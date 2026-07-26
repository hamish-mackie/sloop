mod support;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use support::{World, wait_until, wait_until_slow};

#[test]
fn brief_sends_an_authenticated_worker_request() {
    let world = World::configured();
    let reply = json!({
        "id": "req-1",
        "ok": true,
        "data": {
            "run": "R1",
            "ticket": {"id": "T1", "body": "Persist cooldowns"},
            "worktree": "/repo/.worktrees/R1",
            "branch": "sloop/R1-T1",
            "stage": {"name": "build", "attempt": 1, "result_check": "commits"},
            "definition_of_done": ["Commit your work to the run branch"]
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
fn verdict_sends_the_selected_verdict_and_reason() {
    let world = World::configured();
    let reply = json!({
        "id": "req-1",
        "ok": true,
        "data": {"verdict": {"run": "R1", "stage": "review", "verdict": "fail", "reason": "changes requested"}}
    });
    let (output, request) = world.worker_exchange(
        &["verdict", "fail", "--reason", "changes requested"],
        reply.clone(),
    );

    assert!(output.status.success());
    assert_eq!(World::json_stdout(&output), reply);
    assert_eq!(request["verb"], "verdict");
    assert_eq!(
        request["args"],
        json!({"verdict": "fail", "reason": "changes requested"})
    );
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
        "stages:\n  - { name: build, action: agent }\n  - { name: merge, action: { builtin: merge } }\n",
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
             git -c user.name=sloop-test-agent -c user.email=sloop-test-agent@example.invalid commit --quiet --allow-empty -m worker\n\
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

fn worktree_json(world: &World, position: usize, name: &str) -> Value {
    let path = world.run_worktree(position).join(name);
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

    let brief = worktree_json(&world, 1, "brief.json");
    assert_eq!(brief["ok"], true, "brief failed: {brief}");
    assert_eq!(brief["data"]["run"], world.run_id(1));
    assert_eq!(brief["data"]["ticket"]["id"], ticket.as_str());
    assert_eq!(brief["data"]["ticket"]["name"], "cooldown");
    assert_eq!(brief["data"]["ticket"]["blocked_by"], serde_json::json!([]));
    assert_eq!(brief["data"]["ticket"]["worktree"], "sloop/cooldown");
    assert_eq!(brief["data"]["ticket"]["target"], "fake");
    let body = brief["data"]["ticket"]["body"].as_str().expect("body");
    assert!(body.contains("Persist cooldowns"), "brief body: {body}");
    assert!(
        brief["data"]["worktree"]
            .as_str()
            .expect("worktree")
            .ends_with(&world.run_id(1)[..8])
    );
    assert!(
        brief["data"]["branch"]
            .as_str()
            .expect("branch")
            .starts_with("sloop/")
    );
    // The brief is keyed on the stage the worker is executing, not on the run.
    assert_eq!(brief["data"]["stage"]["name"], "build");
    assert_eq!(brief["data"]["stage"]["attempt"], 1);
    assert_eq!(brief["data"]["stage"]["result_check"], "commits");
    // A builder's obligation is unchanged in substance: an `action: agent`
    // stage is checked for commits, so commits are what it is asked for.
    assert_eq!(
        brief["data"]["definition_of_done"],
        json!(["Commit your work to the run branch"])
    );
    // `acceptance` was hardcoded empty and read by nothing; it is gone.
    assert!(
        brief["data"]["ticket"]["acceptance"].is_null(),
        "brief kept the dead acceptance field: {brief}"
    );

    let show = worktree_json(&world, 1, "show.json");
    assert_eq!(show["ok"], true, "show failed: {show}");
    assert_eq!(show["data"]["ref"], ticket.as_str());
    assert_eq!(show["data"]["kind"], "ticket");
    assert_eq!(show["data"]["value"]["name"], "cooldown");
    assert_eq!(show["data"]["value"]["blocked_by"], serde_json::json!([]));
    assert_eq!(show["data"]["value"]["worktree"], "sloop/cooldown");
    assert_eq!(show["data"]["value"]["target"], "fake");
    // The worker's `show` is unchanged: it never gained the operator's body.
    assert!(
        show["data"]["value"]["body"].is_null(),
        "worker show must not carry a body: {show}"
    );

    // `show` is scoped to the run's own ticket; everything else is
    // unauthorized, whether or not it exists.
    let foreign = worktree_json(&world, 1, "foreign-show.json");
    assert_eq!(foreign["ok"], false);
    assert_eq!(foreign["error"]["code"], "unauthorized");

    let note = worktree_json(&world, 1, "note.json");
    assert_eq!(note["ok"], true, "note failed: {note}");
    assert_eq!(note["data"]["note"]["run"], world.run_id(1));
    assert_eq!(note["data"]["note"]["text"], "work in progress");

    // The note is durable evidence, not a courtesy reply.
    let db = sloop::db::Db::open(&world.db_path(), 0).expect("open runtime database");
    let store = sloop::run_store::RunStore::from_db(db);
    let notes = store.notes_for_run(&world.run_id(1)).expect("read notes");
    assert_eq!(notes, vec!["work in progress".to_owned()]);
}

#[test]
fn reported_stage_records_the_first_verdict_and_rejects_the_second() {
    let world = World::configured();
    configure_worker_agent(&world, false);
    let reviewer = world.root().join("reviewer.sh");
    fs::write(
        &reviewer,
        format!(
            "#!/bin/sh\n\
             SLOOP={}\n\
             \"$SLOOP\" --json verdict fail --reason 'changes requested' --confidence high > verdict.json 2> verdict.err\n\
             \"$SLOOP\" --json verdict pass > duplicate.out 2> duplicate.json\n\
             echo $? > duplicate.exit\n\
             exit 0\n",
            serde_json::to_string(env!("CARGO_BIN_EXE_sloop")).expect("quote sloop path"),
        ),
    )
    .expect("write reviewer");
    fs::write(
        world.root().join(".agents/sloop/flows/default.yaml"),
        format!(
            "stages:\n  - {{ name: build, action: agent }}\n  - name: review\n    action: {{ exec: [\"sh\", {}] }}\n    result_check: reported\n  - {{ name: merge, action: {{ builtin: merge }} }}\n",
            serde_json::to_string(&reviewer.to_string_lossy()).expect("quote reviewer path"),
        ),
    )
    .expect("write reported flow");
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_manual(&world, "reported.md", "# Reported review\n");

    assert!(world.sloop(&["run", &ticket]).status.success());
    wait_until("the reported failure settles", || run_settled(&world));

    let first = worktree_json(&world, 1, "verdict.json");
    assert_eq!(first["ok"], true, "first verdict failed: {first}");
    assert_eq!(first["data"]["verdict"]["stage"], "review");
    assert_eq!(first["data"]["verdict"]["verdict"], "fail");
    assert_eq!(first["data"]["verdict"]["reason"], "changes requested");

    let duplicate = worktree_json(&world, 1, "duplicate.json");
    assert_eq!(duplicate["ok"], false);
    assert_eq!(duplicate["error"]["code"], "conflict");
    assert_eq!(
        fs::read_to_string(world.run_worktree(1).join("duplicate.exit"))
            .expect("read duplicate exit")
            .trim(),
        "1"
    );
    let persisted = world
        .run_evidence(&world.run_id(1), "stage_verdict")
        .expect("reported verdict evidence");
    assert_eq!(persisted["verdict"], "fail");
    assert_eq!(persisted["reason"], "changes requested");

    // `--confidence` means the same thing on a solo reported stage as it does
    // on a panel seat: recorded as evidence, never weighted. It has to reach
    // the store and come back out of `show`, or the flag is a lie the help
    // text tells.
    assert_eq!(first["data"]["verdict"]["confidence"], "high");
    assert_eq!(persisted["confidence"], "high");
    let review = world.show_snapshot(&world.run_alias(1))["stages"]
        .as_array()
        .expect("stages")
        .iter()
        .find(|stage| stage["stage"] == "review")
        .expect("a review row")
        .clone();
    assert_eq!(review["verdict_source"], "reported");
    assert_eq!(review["confidence"], "high");
}

/// Two stages of one run, two obligations. The builder is checked for commits
/// and is told to commit; the reviewer's stage turns on its report, and its
/// brief must not send it off to write code — which is exactly what a brief
/// keyed on the run did, in contradiction of the prompt the same daemon
/// handed it.
#[test]
fn a_reported_stages_brief_names_its_own_stage_and_asks_for_no_commit() {
    let world = World::configured();
    configure_worker_agent(&world, false);
    let reviewer = world.root().join("reviewer.sh");
    fs::write(
        &reviewer,
        format!(
            "#!/bin/sh\n\
             SLOOP={}\n\
             \"$SLOOP\" --json brief > review-brief.json 2> review-brief.err\n\
             \"$SLOOP\" --json verdict pass --reason 'looks right' >/dev/null 2>&1\n\
             exit 0\n",
            serde_json::to_string(env!("CARGO_BIN_EXE_sloop")).expect("quote sloop path"),
        ),
    )
    .expect("write reviewer");
    fs::write(
        world.root().join(".agents/sloop/flows/default.yaml"),
        format!(
            "stages:\n  - {{ name: build, action: agent }}\n  - name: review\n    action: {{ exec: [\"sh\", {}] }}\n    result_check: reported\n  - {{ name: merge, action: {{ builtin: merge }} }}\n",
            serde_json::to_string(&reviewer.to_string_lossy()).expect("quote reviewer path"),
        ),
    )
    .expect("write reported flow");
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_manual(&world, "reported-brief.md", "# Reported review\n");

    assert!(world.sloop(&["run", &ticket]).status.success());
    wait_until("the reviewed run settles", || run_settled(&world));

    let brief = worktree_json(&world, 1, "review-brief.json");
    assert_eq!(brief["ok"], true, "reviewer brief failed: {brief}");
    assert_eq!(brief["data"]["stage"]["name"], "review");
    assert_eq!(brief["data"]["stage"]["attempt"], 1);
    assert_eq!(brief["data"]["stage"]["result_check"], "reported");
    let obligations = brief["data"]["definition_of_done"]
        .as_array()
        .expect("definition_of_done");
    assert_eq!(obligations.len(), 1, "{obligations:?}");
    let obligation = obligations[0]
        .as_str()
        .expect("obligation text")
        .to_lowercase();
    assert!(
        obligation.contains("reported verdict"),
        "reviewer obligation: {obligation}"
    );
    assert!(
        !obligation.contains("commit"),
        "a reported stage's worker was told to commit: {obligation}"
    );

    // The same run's builder still reads the obligation it always did. The two
    // briefs differ because the stages differ, which is the whole point.
    let build = worktree_json(&world, 1, "brief.json");
    assert_eq!(build["data"]["stage"]["name"], "build");
    assert_eq!(build["data"]["stage"]["result_check"], "commits");
    assert_eq!(
        build["data"]["definition_of_done"],
        json!(["Commit your work to the run branch"])
    );
}

/// A `return_to` edge re-enters the stage, and the second execution is a
/// separate assignment owed its own report. A worker that could not see the
/// attempt could not tell a retry from a first run.
#[test]
fn a_re_entered_stages_brief_reports_the_second_attempt() {
    let world = World::configured();
    configure_worker_agent(&world, false);
    let counter = world.root().join("rounds");
    fs::write(&counter, b"").expect("create round counter");
    let reviewer = world.root().join("reviewer.sh");
    fs::write(
        &reviewer,
        format!(
            "#!/bin/sh\n\
             SLOOP={sloop}\n\
             COUNTER={counter}\n\
             printf x >> \"$COUNTER\"\n\
             round=$(wc -c < \"$COUNTER\" | tr -d ' ')\n\
             \"$SLOOP\" --json brief > \"review-brief-$round.json\" 2>&1\n\
             if [ \"$round\" -le 1 ]; then\n  \
               \"$SLOOP\" --json verdict fail --reason 'round one' >/dev/null 2>&1\n\
             else\n  \
               \"$SLOOP\" --json verdict pass --reason 'round two' >/dev/null 2>&1\n\
             fi\n\
             exit 0\n",
            sloop = serde_json::to_string(env!("CARGO_BIN_EXE_sloop")).expect("quote sloop path"),
            counter =
                serde_json::to_string(&counter.to_string_lossy()).expect("quote counter path"),
        ),
    )
    .expect("write reviewer");
    fs::write(
        world.root().join(".agents/sloop/flows/default.yaml"),
        format!(
            "stages:\n  - {{ name: build, action: agent }}\n  - name: review\n    action: {{ exec: [\"sh\", {}] }}\n    result_check: reported\n    fail_action: {{ return_to: build, attempts: 1 }}\n  - {{ name: merge, action: {{ builtin: merge }} }}\n",
            serde_json::to_string(&reviewer.to_string_lossy()).expect("quote reviewer path"),
        ),
    )
    .expect("write re-entering flow");
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_manual(&world, "re-entered.md", "# Re-entered review\n");

    assert!(world.sloop(&["run", &ticket]).status.success());
    wait_until_slow("the re-reviewed run settles", || run_settled(&world));

    for (round, attempt) in [(1, 1), (2, 2)] {
        let brief = worktree_json(&world, 1, &format!("review-brief-{round}.json"));
        assert_eq!(brief["ok"], true, "round {round} brief failed: {brief}");
        assert_eq!(brief["data"]["stage"]["name"], "review");
        assert_eq!(
            brief["data"]["stage"]["attempt"], attempt,
            "round {round}: {brief}"
        );
        assert_eq!(brief["data"]["stage"]["result_check"], "reported");
    }
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
    rusqlite::Connection::open(world.db_path())
        .unwrap()
        .execute("UPDATE tickets SET body = NULL WHERE id = ?1", [&ticket])
        .unwrap();

    assert!(world.sloop(&["run", &ticket]).status.success());
    wait_until("the claimed run starts", || {
        world.worker_socket(&world.run_id(1)).exists()
    });
    fs::write(
        world.root().join(".agents/sloop/flows/default.yaml"),
        "- { name: build, action: unknown }\n",
    )
    .expect("invalidate the flow after admission");
    fs::write(
        world.root().join(".agents/sloop/tickets/admission.md"),
        "# Changed after claim\n",
    )
    .expect("edit source ticket after claim");
    fs::write(world.root().join("release"), "go\n").expect("release the agent");
    wait_until("the run settles", || run_settled(&world));

    let brief = worktree_json(&world, 1, "brief.json");
    let body = brief["data"]["ticket"]["body"].as_str().expect("body");
    assert!(body.contains("Original instructions"), "brief body: {body}");
    assert!(!body.contains("Changed after claim"), "brief body: {body}");
    assert_eq!(worktree_json(&world, 1, "show.json")["ok"], true);
    assert_eq!(worktree_json(&world, 1, "note.json")["ok"], true);
    assert_eq!(
        World::json_stdout(&world.sloop(&["status"]))["data"]["tickets"]["merged"],
        1
    );
}

#[test]
fn project_show_groups_notes_and_git_commits_without_writing_source_files() {
    let world = World::configured();
    fs::create_dir_all(world.root().join(".agents/sloop/flows")).unwrap();
    fs::write(
        world.root().join(".agents/sloop/flows/default.yaml"),
        "stages:\n  - { name: build, action: agent }\n  - { name: merge, action: { builtin: merge } }\n",
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
    for (ticket_id, run_id, alias) in [
        (&first, world.run_id(1), world.run_alias(1)),
        (&second, world.run_id(2), world.run_alias(2)),
    ] {
        let ticket = tickets
            .iter()
            .find(|ticket| ticket["id"] == ticket_id.as_str())
            .expect("ticket activity group");
        assert_eq!(ticket["notes"].as_array().expect("notes").len(), 1);
        assert_eq!(ticket["notes"][0]["run"], alias);
        assert_eq!(ticket["notes"][0]["run_id"], run_id);
        assert_eq!(ticket["notes"][0]["text"], format!("note from {ticket_id}"));
        assert_eq!(ticket["commits"].as_array().expect("commits").len(), 1);
        assert_eq!(ticket["commits"][0]["run"], alias);
        assert_eq!(ticket["commits"][0]["run_id"], run_id);
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

    let ticket_show = worktree_json(&world, 1, "ticket-show.json");
    assert_eq!(ticket_show["data"]["ref"], first);
    assert_eq!(ticket_show["data"]["kind"], "ticket");
    assert_eq!(ticket_show["data"]["value"]["name"], "first");
    assert_eq!(ticket_show["data"]["value"]["blocked_by"], json!([]));
    assert_eq!(ticket_show["data"]["value"]["worktree"], "sloop/first");
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
fn operator_show_reads_a_ticket_by_id_and_name_with_its_body() {
    let world = World::configured();
    world.start_daemon();
    let ticket = post_manual(
        &world,
        "cooldown.md",
        "# Persist cooldowns\n\nSurvive restarts.\n",
    );

    // By id, as a `--json` envelope: the frontmatter summary plus the body
    // read from the committed ticket file.
    let output = world.sloop(&["show", &ticket]);
    assert!(
        output.status.success(),
        "operator show by id failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let show = World::json_stdout(&output);
    assert_eq!(show["ok"], true);
    assert_eq!(show["data"]["ref"], ticket.as_str());
    assert_eq!(show["data"]["kind"], "ticket");
    let value = &show["data"]["value"];
    assert_eq!(value["id"], ticket.as_str());
    assert_eq!(value["name"], "cooldown");
    assert_eq!(value["state"], "ready");
    assert_eq!(value["project"], "default");
    assert_eq!(value["worktree"], "sloop/cooldown");
    assert_eq!(value["blocked_by"], json!([]));
    let body = value["body"].as_str().expect("ticket body");
    assert!(
        body.contains("Persist cooldowns") && body.contains("Survive restarts"),
        "ticket body: {body}"
    );

    // The same ticket resolves by its human name, echoing the reference.
    let by_name = World::json_stdout(&world.sloop(&["show", "cooldown"]));
    assert_eq!(by_name["data"]["ref"], "cooldown");
    assert_eq!(by_name["data"]["kind"], "ticket");
    assert_eq!(by_name["data"]["value"]["id"], ticket.as_str());

    // Human output stays scannable: a summary line, a blank line, then body.
    let human = world.sloop_plain(&["show", &ticket]);
    assert!(human.status.success());
    let human = String::from_utf8(human.stdout).expect("human output is UTF-8");
    assert!(
        human.contains(&format!("{ticket}  cooldown  (ready)")),
        "{human}"
    );
    assert!(human.contains("# Persist cooldowns"), "{human}");
}

#[test]
fn operator_show_reports_a_run_by_id() {
    let world = World::configured();
    configure_worker_agent(&world, false);
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_manual(&world, "cooldown.md", "# Persist cooldowns\n\nBody.\n");

    assert!(world.sloop(&["run", &ticket]).status.success());
    wait_until("the run settles", || run_settled(&world));

    let run_id = world.run_id(1);
    let output = world.sloop(&["show", &run_id]);
    assert!(
        output.status.success(),
        "operator show of a run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let show = World::json_stdout(&output);
    assert_eq!(show["ok"], true);
    assert_eq!(show["data"]["ref"], run_id);
    assert_eq!(show["data"]["kind"], "run");
    let value = &show["data"]["value"];
    assert_eq!(value["id"], run_id);
    assert_eq!(value["ticket"], ticket.as_str());
    assert_eq!(value["ticket_name"], "cooldown");
    assert_eq!(value["state"], "merged");
    assert_eq!(value["terminal"], true);
    assert_eq!(value["exit_code"], 0);
    assert!(
        value["branch"]
            .as_str()
            .expect("branch")
            .starts_with("sloop/"),
        "branch: {value}"
    );
    assert!(
        value["worktree"]
            .as_str()
            .expect("worktree")
            .ends_with(&run_id[..8]),
        "worktree: {value}"
    );

    // Human output names the run and its settled evidence.
    let human = world.sloop_plain(&["show", &run_id]);
    let human = String::from_utf8(human.stdout).expect("human output is UTF-8");
    assert!(
        human.contains(&format!("{}  (merged)", world.run_alias(1))),
        "{human}"
    );
    assert!(
        human.contains(&format!("ticket: {ticket}  cooldown")),
        "{human}"
    );
}

#[test]
fn the_worker_socket_rejects_wrong_tokens_and_operator_verbs() {
    let world = World::configured();
    configure_worker_agent(&world, true);
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_manual(&world, "gate.md", "# Gate\n");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until("the worker socket appears", || {
        world.worker_socket(&world.run_id(1)).exists()
    });
    let socket: PathBuf = world.worker_socket(&world.run_id(1));

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
