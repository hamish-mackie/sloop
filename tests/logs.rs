mod support;

use std::fs;
use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};

use support::{World, wait_until};

fn configure_agent_script(world: &World, script_body: &str) {
    configure_flow(
        world,
        "  - { name: build, action: agent }\n  - { name: merge, action: { builtin: merge } }\n",
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
const MULTI_STAGE_FLOW: &str = "  - { name: build, action: agent, result_check: { exec: ['true'] } }\n  - { name: check, action: { exec: [\"sh\", \"-c\", \"echo exec stage speaking\"] } }\n  - { name: merge, action: { builtin: merge } }\n";

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

/// Appends `count` single-entry records to a settled run's log, numbered from
/// 1 and tagged with `stage`.
///
/// Entries are pipe-read boundaries, not lines, so a process cannot be asked
/// for an exact number of them. Writing the records the way the daemon writes
/// them fixes the boundaries a windowing test needs to count.
fn extend_log(world: &World, run: &str, stage: &str, count: usize) {
    let log = world
        .state_dir()
        .join("runs")
        .join(run)
        .join("output.ndjson");
    let existing = fs::read_to_string(&log).expect("read run log");
    let captured = existing
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter_map(|record| record["sequence"].as_u64())
        .max()
        .expect("the run captured output");
    let mut appended = String::new();
    for index in 1..=count {
        let sequence = captured + index as u64;
        appended.push_str(&format!(
            "{{\"sequence\":{sequence},\"timestamp\":\"2026-07-20T00:00:00Z\",\"source\":\"stage\",\"stage\":\"{stage}\",\"stream\":\"stdout\",\"encoding\":\"utf8\",\"text\":\"tail line {index}\\n\"}}\n"
        ));
    }
    fs::write(&log, format!("{existing}{appended}")).expect("extend run log");
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

/// A backward edge runs one stage twice, and the two executions land in one
/// log under one name. Without an attempt in the selector `--stage build` is
/// two interleaved runs of the same command, which is exactly the page nobody
/// wants when they are chasing one failure.
#[test]
fn stage_selects_one_execution_of_a_re_run_stage() {
    let world = World::configured();
    // The agent says which pass it is on, counting through a file outside the
    // worktree so the count survives the span re-running.
    let counter = world.root().join("passes.count");
    fs::write(&counter, b"").unwrap();
    configure_flow(
        &world,
        "  - name: build\n    action: agent\n    result_check: { builtin: commits }\n  - name: gate\n    action: { exec: [\"sh\", \"-c\", \"test -f second\"] }\n    result_check: none\n    fail_action: { return_to: build, attempts: 1 }\n  - { name: merge, action: { builtin: merge } }\n",
        &format!(
            "printf x >> {counter}\npass=$(wc -c < {counter} | tr -d ' ')\necho \"agent speaking on pass $pass\"\nif [ \"$pass\" -gt 1 ]; then touch second; fi\ngit -c user.name=a -c user.email=a@example.invalid commit --quiet --allow-empty -m work\nexit 0\n",
            counter = counter.display(),
        ),
    );
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "looped.md");
    let run = world.run_id(1);
    wait_until("the looped run settles", || {
        matches!(world.run_state(&run).as_str(), "merged" | "failed")
    });

    // Both passes are under the one name...
    let both =
        entry_text(&World::json_stdout(&world.sloop(&["logs", &run, "--stage", "build"]))["data"]);
    assert!(both.contains("pass 1") && both.contains("pass 2"), "{both}");

    // ...and each is reachable alone, by the label `sloop show` prints.
    let first = entry_text(
        &World::json_stdout(&world.sloop(&["logs", &run, "--stage", "build#1"]))["data"],
    );
    assert!(
        first.contains("pass 1") && !first.contains("pass 2"),
        "{first}"
    );
    let second = entry_text(
        &World::json_stdout(&world.sloop(&["logs", &run, "--stage", "build#2"]))["data"],
    );
    assert!(
        second.contains("pass 2") && !second.contains("pass 1"),
        "{second}"
    );

    // An attempt the stage never reached is an empty page, not an error: the
    // stage exists, and how many times it ran is what the caller is asking.
    let unreached =
        World::json_stdout(&world.sloop(&["logs", &run, "--stage", "build#3"]))["data"].clone();
    assert_eq!(unreached["entries"].as_array().expect("entries").len(), 0);

    // A malformed attempt is refused rather than folded back into the name,
    // which would answer a typo with an empty page.
    let malformed = world.sloop(&["logs", &run, "--stage", "build#two"]);
    assert!(!malformed.status.success());
    let response = World::json_stdout_or_stderr(&malformed);
    assert_eq!(response["error"]["code"], "invalid_arguments");
    let message = response["error"]["message"].as_str().expect("message");
    assert!(message.contains("<stage>#<attempt>"), "{message}");
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

    extend_log(&world, &run, "check", 8);

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

/// The bug this replaces: a bare read showed the *oldest* page and said
/// nothing, so a busy run looked identical to a hung one.
#[test]
fn a_bare_read_tails_and_states_what_the_window_hid() {
    let world = World::configured();
    let run = settled_multi_stage_run(&world, "windowed.md");
    extend_log(&world, &run, "check", 100);

    let output = world.sloop_plain(&["logs", &run]);

    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).expect("utf8 output");
    assert!(
        text.contains("tail line 100") && !text.contains("tail line 1\n"),
        "a bare read must anchor at the newest entries: {text}"
    );
    assert!(
        text.contains("showing the last 64 of"),
        "a windowed page must say what it hid: {text}"
    );
}

#[test]
fn a_bare_read_that_shows_everything_adds_no_notice() {
    let world = World::configured();
    let run = settled_multi_stage_run(&world, "short.md");

    let output = world.sloop_plain(&["logs", &run]);

    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).expect("utf8 output");
    assert!(text.contains("agent stage speaking"), "{text}");
    assert!(
        !text.contains("showing the last"),
        "a complete page must not claim a window: {text}"
    );
}

/// The symptom that caused the false stall diagnosis: repeated bare reads of
/// a growing log returned byte-identical output.
#[test]
fn a_second_bare_read_shows_entries_the_first_could_not() {
    let world = World::configured();
    let run = settled_multi_stage_run(&world, "growing.md");
    extend_log(&world, &run, "check", 70);
    let first = world.sloop_plain(&["logs", &run]);
    let first = String::from_utf8(first.stdout).expect("utf8 output");

    extend_log(&world, &run, "check", 80);
    let second = world.sloop_plain(&["logs", &run]);
    let second = String::from_utf8(second.stdout).expect("utf8 output");

    assert_ne!(
        first, second,
        "a bare read of a grown log must not repeat itself"
    );
    assert!(
        second.contains("tail line 80"),
        "the second read must reach the newest entry: {second}"
    );
}

#[test]
fn a_windowed_stage_page_states_the_window_and_keeps_the_filter() {
    let world = World::configured();
    let run = settled_multi_stage_run(&world, "windowed-stage.md");
    extend_log(&world, &run, "check", 100);

    let output = world.sloop_plain(&["logs", &run, "--stage", "check"]);

    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).expect("utf8 output");
    // 100 appended, plus the exec stage's own line: the total is what the
    // filter matched, not what the run captured.
    assert!(
        text.contains("showing the last 64 of 101 entries"),
        "{text}"
    );
    assert!(
        !text.contains("agent stage speaking"),
        "the stage filter must still apply: {text}"
    );
}

/// The window is reported, not just rendered: a machine consumer sees how
/// much the tail dropped, and `complete`/`next_cursor` keep their meaning.
#[test]
fn json_mode_reports_what_the_window_dropped() {
    let world = World::configured();
    let run = settled_multi_stage_run(&world, "windowed-json.md");
    extend_log(&world, &run, "check", 100);

    let output = world.sloop(&["logs", &run, "--stage", "check", "--tail", "10"]);

    let data = World::json_stdout(&output)["data"].clone();
    assert_eq!(data["entries"].as_array().expect("entries").len(), 10);
    // 100 appended plus the exec stage's own line, less the 10 kept.
    assert_eq!(data["elided"], 91);
    assert_eq!(
        data["complete"], true,
        "a tail reads to the end however much it drops"
    );
}
