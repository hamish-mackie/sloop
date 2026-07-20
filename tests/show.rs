//! Integration coverage for the run and stage history `sloop show` renders.
//!
//! Real daemon, real git, scripted fake agents and multi-stage flows. The
//! point of these tests is the one an operator hit in a dogfooding smoke test:
//! a run whose agent exited 0 but whose later stage failed must *say* that,
//! from the stored stage evidence, rather than leaving the reader to invent an
//! explanation for a bare `exit: 0`.

mod support;

use std::fs;

use serde_json::Value;
use support::{World, wait_until_slow};

/// build -> test -> merge, where `test` is an `exec` stage that always passes.
const FLOW_PASSING_TEST: &str = "stages:
  - { name: build, kind: agent, verdict: exit }
  - { name: test, kind: exec, cmd: [\"true\"] }
  - { name: merge, kind: merge }
";

/// The same flow with a `test` stage that always fails.
const FLOW_FAILING_TEST: &str = "stages:
  - { name: build, kind: agent, verdict: exit }
  - { name: test, kind: exec, cmd: [\"false\"] }
  - { name: merge, kind: merge }
";

/// A single-stage flow, for cases that never need a run at all.
const FLOW_AGENT_ONLY: &str = "stages:\n  - { name: build, kind: agent, verdict: exit }\n";

fn configure(world: &World, stages: &str, script_body: &str) {
    let flow_directory = world.root().join(".agents/sloop/flows");
    fs::create_dir_all(&flow_directory).expect("create flow directory");
    fs::write(flow_directory.join("default.yaml"), stages).expect("write flow");
    let script = world.root().join("fake-agent.sh");
    fs::write(&script, format!("#!/bin/sh\nset -eu\n{script_body}")).expect("write fake agent");
    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        format!(
            "version: 1\nscheduler:\n  max_parallel_tasks: 1\nagent:\n  default_target: fake\n  \
             targets:\n    fake:\n      cmd: [\"sh\", {}, \"{{prompt}}\"]\n",
            serde_json::to_string(&script.to_string_lossy()).expect("serialize script path"),
        ),
    )
    .expect("write config");
}

/// A committing agent: the run has real work behind it, so a later stage
/// failure lands in `needs_review` with a branch worth reading rather than
/// being discarded as an empty attempt.
fn committing_agent() -> String {
    "git -c user.name=agent -c user.email=agent@example.invalid commit --quiet --allow-empty \
     -m work\nexit 0\n"
        .to_owned()
}

fn post(world: &World, name: &str) -> String {
    let ticket = world.write_ticket(name, "# Show history\nwork\n");
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

fn show(world: &World, reference: &str) -> Value {
    let output = world.sloop(&["show", reference]);
    assert!(
        output.status.success(),
        "show {reference} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    World::json_stdout(&output)["data"]["value"].clone()
}

fn show_text(world: &World, reference: &str) -> String {
    let output = world.sloop_plain(&["show", reference]);
    assert!(
        output.status.success(),
        "show {reference} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// The stage entry named `stage`, from either a run's `stages` table or a
/// ticket run's compact strip.
fn stage<'a>(stages: &'a Value, name: &str) -> &'a Value {
    stages
        .as_array()
        .expect("stages array")
        .iter()
        .find(|entry| entry["stage"] == name)
        .unwrap_or_else(|| panic!("no stage `{name}` in {stages}"))
}

#[test]
fn a_merged_run_shows_every_stage_passed_on_the_ticket_and_the_run() {
    let world = World::configured();
    configure(&world, FLOW_PASSING_TEST, &committing_agent());
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "merged-history.md");
    assert!(world.sloop(&["run", &ticket]).status.success());
    wait_until_slow("the run merges", || {
        status(&world)["tickets"]["merged"] == 1
    });

    // The ticket lists its one run with the whole flow passed.
    let shown = show(&world, &ticket);
    let runs = shown["runs"].as_array().expect("runs array");
    assert_eq!(runs.len(), 1, "{runs:?}");
    assert_eq!(runs[0]["alias"], world.run_alias(1));
    assert_eq!(runs[0]["state"], "merged");
    assert!(runs[0]["started_at_ms"].is_i64(), "{}", runs[0]);
    assert!(runs[0]["finished_at_ms"].is_i64(), "{}", runs[0]);
    for name in ["build", "test", "merge"] {
        assert_eq!(stage(&runs[0]["stages"], name)["state"], "passed");
    }
    // A merged run is the one outcome that needs no explaining.
    assert_eq!(runs[0]["reason"], Value::Null);

    // The run itself carries the per-stage table, with exit codes and times.
    let run = show(&world, &world.run_alias(1));
    assert_eq!(run["state"], "merged");
    assert_eq!(run["reason"], Value::Null);
    assert!(run["claimed_at_ms"].is_i64(), "{run}");
    assert!(run["started_at_ms"].is_i64(), "{run}");
    assert!(run["finished_at_ms"].is_i64(), "{run}");
    let stages = &run["stages"];
    assert_eq!(stages.as_array().expect("stages").len(), 3);
    for name in ["build", "test", "merge"] {
        let row = stage(stages, name);
        assert_eq!(row["state"], "passed", "{row}");
        assert_eq!(row["exit_code"], 0, "{row}");
        assert_eq!(row["attempts"], 1, "{row}");
        assert!(row["started_at_ms"].is_i64(), "{row}");
        assert!(row["finished_at_ms"].is_i64(), "{row}");
    }

    let text = show_text(&world, &world.run_alias(1));
    assert!(text.contains("stages:"), "{text}");
    assert!(text.contains("build  passed"), "{text}");
    assert!(text.contains("agent exit: 0"), "{text}");
    assert!(text.contains("timeline: claimed "), "{text}");
}

#[test]
fn an_agent_that_succeeds_before_a_failing_stage_reports_that_stage_as_the_reason() {
    let world = World::configured();
    // The exact smoke-test shape: the agent exits 0 with a commit, then the
    // `test` stage fails. The old output showed `exit: 0` and an empty reason,
    // which read as "the run succeeded and something killed it".
    configure(&world, FLOW_FAILING_TEST, &committing_agent());
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "failing-stage.md");
    assert!(world.sloop(&["run", &ticket]).status.success());
    wait_until_slow("the run lands in needs_review", || {
        status(&world)["tickets"]["needs_review"] == 1
    });

    let run = show(&world, &world.run_alias(1));
    assert_eq!(run["state"], "needs_review");
    // The agent's own exit is still 0 and is now labeled as the agent's.
    assert_eq!(run["exit_code"], 0);
    assert_eq!(run["agent_exit_code"], 0);
    let reason = run["reason"].as_str().expect("a derived reason");
    assert!(reason.contains("`test`"), "{reason}");
    assert!(reason.contains("failed"), "{reason}");
    assert!(reason.contains("exit 1"), "{reason}");
    assert!(reason.contains("agent completed with commits"), "{reason}");

    // The stage rows carry the evidence the reason was derived from.
    assert_eq!(stage(&run["stages"], "build")["state"], "passed");
    let failed = stage(&run["stages"], "test");
    assert_eq!(failed["state"], "failed");
    assert_eq!(failed["exit_code"], 1);
    assert_eq!(failed["verdict_source"], "exit_code");
    // `merge` never ran, and a settled run must not pretend it is still going.
    assert_eq!(stage(&run["stages"], "merge")["state"], "pending");

    let text = show_text(&world, &world.run_alias(1));
    assert!(text.contains("agent exit: 0"), "{text}");
    assert!(
        !text.contains("\nexit: 0"),
        "bare `exit:` must be gone: {text}"
    );
    assert!(text.contains("test   failed"), "{text}");
    assert!(text.contains("exit 1"), "{text}");

    // And the ticket's runs section shows the failure in the strip.
    let shown = show(&world, &ticket);
    let runs = shown["runs"].as_array().expect("runs array");
    assert_eq!(runs[0]["state"], "needs_review");
    assert_eq!(stage(&runs[0]["stages"], "test")["state"], "failed");
    let ticket_text = show_text(&world, &ticket);
    assert!(ticket_text.contains("runs:"), "{ticket_text}");
    assert!(ticket_text.contains("test:FAIL"), "{ticket_text}");
}

#[test]
fn a_ticket_with_several_runs_lists_them_newest_first() {
    let world = World::configured();
    // No commit, so each attempt settles `failed` and can be retried.
    configure(&world, FLOW_FAILING_TEST, "exit 0\n");
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "two-runs.md");

    for attempt in 1..=2 {
        if attempt > 1 {
            assert!(world.sloop(&["retry", &ticket]).status.success());
        }
        assert!(world.sloop(&["run", &ticket]).status.success());
        wait_until_slow("the attempt fails", || {
            status(&world)["tickets"]["failed"] == 1
        });
    }

    let runs = show(&world, &ticket)["runs"].as_array().cloned().unwrap();
    assert_eq!(runs.len(), 2, "{runs:?}");
    assert_eq!(runs[0]["attempt"], 2);
    assert_eq!(runs[1]["attempt"], 1);
    assert_eq!(runs[0]["alias"], world.run_alias(2));
    assert_eq!(runs[1]["alias"], world.run_alias(1));
}

#[test]
fn a_ticket_that_has_never_run_reports_no_runs() {
    let world = World::configured();
    configure(&world, FLOW_AGENT_ONLY, "exit 0\n");
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "never-run.md");

    assert_eq!(show(&world, &ticket)["runs"], Value::Array(Vec::new()));
    assert!(show_text(&world, &ticket).contains("runs: none"));
}

/// `show <project>` is untouched by this feature; runs and stages belong to the
/// ticket and run views.
#[test]
fn project_show_is_unchanged() {
    let world = World::configured();
    configure(&world, FLOW_AGENT_ONLY, "exit 0\n");
    world.commit_all("initial");
    world.start_daemon();
    post(&world, "project-untouched.md");

    let project = show(&world, "default");
    assert!(project["tickets"].is_array(), "{project}");
    assert_eq!(project["runs"], Value::Null);
}
