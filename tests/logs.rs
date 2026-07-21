mod support;

use std::fs;
use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};

use support::{World, wait_until};

fn configure_agent_script(world: &World, script_body: &str) {
    configure_flow(
        world,
        "  - { name: build, kind: build }\n  - { name: merge, kind: merge }\n",
        script_body,
    );
}

fn configure_flow(world: &World, stages: &str, script_body: &str) {
    fs::create_dir_all(world.root().join(".agents/sloop/flows")).unwrap();
    fs::write(
        world.root().join(".agents/sloop/flows/default.yaml"),
        format!("stages:\n{stages}"),
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

fn post_and_run(world: &World, name: &str) -> String {
    let ticket = world.write_ticket(name, "# Work\n");
    let output = world.sloop(&["post", ticket.to_str().unwrap(), "--manual"]);
    assert!(output.status.success());
    let id = World::json_stdout(&output)["data"]["ticket"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(world.sloop(&["run", &id]).status.success());
    id
}

#[test]
fn logs_returns_ordered_captured_output_from_both_streams() {
    let world = World::configured();
    configure_agent_script(
        &world,
        "echo starting work\necho oh no >&2\necho done\nexit 0\n",
    );
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "captured.md");

    wait_until("the run exits", || {
        let output = world.sloop(&["status"]);
        World::json_stdout(&output)["data"]["gate"]["active_agents"] == 0
    });

    let output = world.sloop(&["logs", &world.run_id(1)]);
    assert!(
        output.status.success(),
        "logs failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let data = World::json_stdout(&output)["data"].clone();
    assert_eq!(data["id"], world.run_id(1));
    assert_eq!(data["alias"], world.run_alias(1));
    assert_eq!(data["complete"], true);

    let entries = data["entries"].as_array().unwrap();
    assert!(!entries.is_empty(), "captured entries expected");

    let mut sequences = Vec::new();
    let mut stdout_text = String::new();
    let mut stderr_text = String::new();
    for entry in entries {
        sequences.push(entry["sequence"].as_u64().unwrap());
        assert!(
            entry["timestamp"]
                .as_str()
                .is_some_and(|value| value.ends_with('Z')),
            "agent log entry must have a UTC timestamp: {entry}"
        );
        assert_eq!(entry["source"], "agent");
        assert_eq!(entry["encoding"], "utf8");
        match entry["stream"].as_str().unwrap() {
            "stdout" => stdout_text.push_str(entry["text"].as_str().unwrap()),
            "stderr" => stderr_text.push_str(entry["text"].as_str().unwrap()),
            other => panic!("unexpected stream {other}"),
        }
    }
    assert!(sequences.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(data["next_cursor"], *sequences.last().unwrap());
    assert_eq!(stdout_text, "starting work\ndone\n");
    assert_eq!(stderr_text, "oh no\n");
}

#[test]
fn logs_for_an_unknown_run_is_not_found() {
    let world = World::configured();
    world.commit_all("initial");
    world.start_daemon();

    let output = world.sloop(&["logs", "R99"]);
    assert!(!output.status.success());
    let error: serde_json::Value = serde_json::from_slice(&output.stderr).expect("stderr is JSON");
    assert_eq!(error["error"]["code"], "not_found");
}

#[test]
fn logs_for_a_missing_run_names_the_run_id_shape() {
    let world = World::configured();

    let output = world.sloop(&["logs", "my-ticket"]);

    assert!(!output.status.success());
    let response = World::json_stdout_or_stderr(&output);
    assert_eq!(response["error"]["code"], "not_found");
    let message = response["error"]["message"]
        .as_str()
        .expect("error message");
    assert!(
        message.contains("`TICK-20-r1`") && message.contains("sloop show"),
        "remedy does not name the run id shape: {message}"
    );
}

/// The stage names an operator reads in a flow are the names `--stage`
/// accepts, for the agent stage as much as for exec stages.
const MULTI_STAGE_FLOW: &str = "  - { name: build, kind: agent, verdict: exit }\n  - { name: check, kind: exec, cmd: [\"sh\", \"-c\", \"echo exec stage speaking\"] }\n  - { name: merge, kind: merge }\n";

fn settled_multi_stage_run(world: &World, ticket: &str) -> String {
    configure_flow(
        world,
        MULTI_STAGE_FLOW,
        "echo agent stage speaking\nexit 0\n",
    );
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(world, ticket);
    let run = world.run_id(1);
    wait_until("the run settles", || {
        matches!(world.run_state(&run).as_str(), "merged" | "failed")
    });
    run
}

/// Concatenates the `text` of every returned entry, so a test can assert on
/// what an operator would read without depending on chunk boundaries.
fn entry_text(data: &serde_json::Value) -> String {
    data["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .filter_map(|entry| entry["text"].as_str())
        .collect()
}

#[test]
fn stage_selects_the_output_of_one_flow_stage() {
    let world = World::configured();
    let run = settled_multi_stage_run(&world, "staged.md");

    let output = world.sloop(&["logs", &run, "--stage", "check"]);
    assert!(output.status.success());
    let data = World::json_stdout(&output)["data"].clone();
    let text = entry_text(&data);
    assert!(
        text.contains("exec stage speaking") && !text.contains("agent stage speaking"),
        "--stage check must show only the exec stage: {text}"
    );
    for entry in data["entries"].as_array().unwrap() {
        assert_eq!(entry["stage"], "check");
    }

    // The agent stage is addressable by its flow name, not by an internal
    // `source: agent` distinction the flow never mentions.
    let output = world.sloop(&["logs", &run, "--stage", "build"]);
    assert!(output.status.success());
    let data = World::json_stdout(&output)["data"].clone();
    let text = entry_text(&data);
    assert!(
        text.contains("agent stage speaking") && !text.contains("exec stage speaking"),
        "--stage build must show only the agent stage: {text}"
    );
    for entry in data["entries"].as_array().unwrap() {
        assert_eq!(entry["source"], "agent");
    }
}

#[test]
fn an_unknown_stage_names_the_stages_the_flow_defines() {
    let world = World::configured();
    let run = settled_multi_stage_run(&world, "unknown-stage.md");

    let output = world.sloop(&["logs", &run, "--stage", "tset"]);

    assert!(
        !output.status.success(),
        "an unknown stage must not succeed"
    );
    let response = World::json_stdout_or_stderr(&output);
    assert_eq!(response["error"]["code"], "invalid_arguments");
    let message = response["error"]["message"].as_str().expect("message");
    assert!(
        message.contains("`build`") && message.contains("`check`") && message.contains("`merge`"),
        "the error must name the flow's stages: {message}"
    );
}

#[test]
fn tail_returns_exactly_the_last_matching_entries() {
    let world = World::configured();
    let run = settled_multi_stage_run(&world, "tailed.md");

    // Entries are pipe-read boundaries, not lines, so a process cannot be
    // asked for an exact number of them. Appending to the settled run's log
    // fixes the boundaries the way the daemon itself would write them.
    let log = world
        .state_dir()
        .join("runs")
        .join(&run)
        .join("output.ndjson");
    let existing = fs::read_to_string(&log).expect("read run log");
    let captured = existing
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter_map(|record| record["sequence"].as_u64())
        .max()
        .expect("the run captured output");
    let mut appended = String::new();
    for index in 1..=8 {
        let sequence = captured + index;
        appended.push_str(&format!(
            "{{\"sequence\":{sequence},\"timestamp\":\"2026-07-20T00:00:0{index}Z\",\"source\":\"aftercare\",\"stage\":\"check\",\"stream\":\"stdout\",\"encoding\":\"utf8\",\"text\":\"tail line {index}\\n\"}}\n"
        ));
    }
    fs::write(&log, format!("{existing}{appended}")).expect("extend run log");

    let output = world.sloop(&["logs", &run, "--stage", "check", "--tail", "5"]);

    assert!(output.status.success());
    let data = World::json_stdout(&output)["data"].clone();
    let entries = data["entries"].as_array().expect("entries");
    assert_eq!(entries.len(), 5, "--tail 5 must return five entries");
    let text = entry_text(&data);
    assert!(
        (4..=8).all(|index| text.contains(&format!("tail line {index}"))),
        "--tail 5 must keep the newest entries: {text}"
    );
    assert!(
        !text.contains("tail line 3") && !text.contains("exec stage speaking"),
        "--tail 5 must drop everything older: {text}"
    );
}

#[test]
fn follow_streams_entries_appended_after_it_started_then_exits() {
    let world = World::configured();
    let released = world.root().join("released");
    configure_flow(
        &world,
        MULTI_STAGE_FLOW,
        &format!(
            "echo before the release\nwhile [ ! -f {} ]; do sleep 0.02; done\necho after the release\nexit 0\n",
            released.display()
        ),
    );
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "followed.md");
    let run = world.run_id(1);
    wait_until("the agent produces its first output", || {
        let output = world.sloop(&["logs", &run]);
        World::json_stdout(&output)["data"]["entries"]
            .as_array()
            .is_some_and(|entries| !entries.is_empty())
    });

    let mut follower = world.spawn_sloop(&["logs", &run, "--stage", "build", "--follow"]);
    let pages = Arc::new(Mutex::new(Vec::new()));
    let collector = {
        let pages = Arc::clone(&pages);
        let stdout = follower.stdout.take().expect("follower stdout is piped");
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Ok(envelope) = serde_json::from_str::<serde_json::Value>(&line) {
                    pages.lock().unwrap().push(entry_text(&envelope["data"]));
                }
            }
        })
    };

    // The agent stays blocked until the follower has printed a first page, so
    // everything after it is provably output that arrived while following —
    // not a single page the follower happened to read late.
    wait_until("the follower prints its first page", || {
        !pages.lock().unwrap().is_empty()
    });
    let before_release = pages.lock().unwrap().len();
    fs::write(&released, "go").expect("release the agent");

    wait_until("the follower exits with the run", || {
        follower.try_wait().expect("poll follower").is_some()
    });
    collector.join().expect("collect follower output");
    let pages = pages.lock().unwrap().clone();
    let streamed: String = pages[before_release..].concat();
    assert!(
        pages[..before_release]
            .concat()
            .contains("before the release"),
        "the first page must carry what existed when follow started: {pages:?}"
    );
    assert!(
        streamed.contains("after the release"),
        "follow must stream output appended after it started: {pages:?}"
    );
    assert!(
        !pages.concat().contains("exec stage speaking"),
        "--stage build must filter the stream too: {pages:?}"
    );
}

#[test]
fn follow_on_a_settled_run_prints_what_exists_and_exits() {
    let world = World::configured();
    let run = settled_multi_stage_run(&world, "already-done.md");

    // Blocking on a run that can produce no more output would hang forever;
    // the harness deadline is what makes that a failure rather than a wedge.
    let output = world.sloop(&["logs", &run, "--follow"]);

    assert!(output.status.success());
    let data = World::json_stdout(&output)["data"].clone();
    assert_eq!(data["terminal"], true);
    assert_eq!(data["complete"], true);
    assert!(entry_text(&data).contains("agent stage speaking"));
}
