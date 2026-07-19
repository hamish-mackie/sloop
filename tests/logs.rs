mod support;

use std::fs;

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

    let output = world.sloop(&["logs", "R1"]);
    assert!(
        output.status.success(),
        "logs failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let data = World::json_stdout(&output)["data"].clone();
    assert_eq!(data["run"], "R1");
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
        message.contains("`R14`") && message.contains("sloop list"),
        "remedy does not name the run id shape: {message}"
    );
}
