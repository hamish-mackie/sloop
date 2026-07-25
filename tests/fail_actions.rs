//! Integration coverage for live fail actions: advisory `continue` failures
//! and bounded `return_to` loops, driven end to end by scripted fake agents.
//!
//! Every assertion here is about what the *evidence* says, because that is
//! what the walk replays: the prompt a re-entered agent was handed, the rows
//! `sloop show` renders, and the outcome the ticket landed on.

mod support;

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;
use support::{World, wait_until, wait_until_slow};

/// Writes the repository config with one fake agent target and a committed
/// flow file.
fn configure(world: &World, flow: &str, script: &Path) {
    let flow_dir = world.root().join(".agents/sloop/flows");
    fs::create_dir_all(&flow_dir).expect("create flow directory");
    fs::write(flow_dir.join("default.yaml"), flow).expect("write flow");
    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        format!(
            "version: 1\nscheduler:\n  max_parallel_tasks: 1\nagent:\n  default_target: fake\n  targets:\n    fake:\n      cmd: [\"sh\", {}, \"{{prompt}}\"]\n",
            serde_json::to_string(&script.to_string_lossy()).expect("serialize script path"),
        ),
    )
    .expect("write config");
}

fn write_script(world: &World, name: &str, body: &str) -> PathBuf {
    let path = world.root().join(name);
    fs::write(&path, format!("#!/bin/sh\nset -eu\n{body}")).expect("write fake agent script");
    path
}

fn post(world: &World, name: &str) -> String {
    let ticket = world.write_ticket(name, "# Fail action scenario\n");
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

fn commit(identity: &str) -> String {
    format!("git -c user.name={identity} -c user.email={identity}@example.invalid commit --quiet")
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

/// Every prompt the fake agent was launched with, one per spawn, separated by
/// a marker the test can split on.
fn prompts(log: &Path) -> Vec<String> {
    fs::read_to_string(log)
        .unwrap_or_default()
        .split("\u{1}PROMPT\u{1}\n")
        .skip(1)
        .map(str::to_owned)
        .collect()
}

/// A fake agent that records the prompt it was handed and commits once.
fn recording_agent(world: &World, prompt_log: &Path) -> PathBuf {
    fs::write(prompt_log, b"").expect("create prompt log");
    write_script(
        world,
        "fake-agent.sh",
        &format!(
            "printf '\\001PROMPT\\001\\n%s\\n' \"$1\" >> {log}\n{commit} --allow-empty -m build\nexit 0\n",
            log = shell_quote(&prompt_log.to_string_lossy()),
            commit = commit("agent"),
        ),
    )
}

/// A fake agent that records its prompt and commits nothing. A run whose
/// branch never moved has no work to preserve, so a halt settles it `failed`
/// rather than parking it for review.
fn barren_agent(world: &World, prompt_log: &Path) -> PathBuf {
    fs::write(prompt_log, b"").expect("create prompt log");
    write_script(
        world,
        "fake-agent.sh",
        &format!(
            "printf '\\001PROMPT\\001\\n%s\\n' \"$1\" >> {log}\nexit 0\n",
            log = shell_quote(&prompt_log.to_string_lossy()),
        ),
    )
}

/// A `test` command that fails the first `failures` times it runs and passes
/// afterwards, counting through a file outside the worktree so the count
/// survives the span re-running.
fn flaky_test_command(world: &World, name: &str, failures: usize) -> PathBuf {
    let counter = world.root().join(format!("{name}.count"));
    fs::write(&counter, b"").expect("create counter");
    write_script(
        world,
        name,
        &format!(
            // `tr -d ' '` because BSD `wc` pads its count to a fixed width and
            // GNU `wc` does not; the run number reaches the prompt verbatim.
            "printf x >> {counter}\nruns=$(wc -c < {counter} | tr -d ' ')\nif [ \"$runs\" -le {failures} ]; then\n  echo \"assertion failed: the widget is not blue\"\n  echo \"run $runs of the test command\"\n  exit 3\nfi\necho \"tests passed on run $runs\"\nexit 0\n",
            counter = shell_quote(&counter.to_string_lossy()),
        ),
    )
}

/// The stage rows `sloop show` renders for a run, as `(label, state)`.
fn shown_stages(world: &World, run: &str) -> Vec<(String, String)> {
    let value = world.show_snapshot(run);
    value["stages"]
        .as_array()
        .expect("stages array")
        .iter()
        .map(|stage| {
            let name = stage["stage"].as_str().unwrap_or("?").to_owned();
            let label = match stage["attempt"].as_u64() {
                Some(attempt) if attempt > 1 => format!("{name}#{attempt}"),
                _ => name,
            };
            (label, stage["state"].as_str().unwrap_or("?").to_owned())
        })
        .collect()
}

/// The whole point of a backward edge: the re-entered agent is told what
/// failed, fixes it, and the run merges. The context has to reach the prompt,
/// or the second attempt is just the first one repeated.
#[test]
fn a_return_to_loop_re_runs_the_span_with_the_failure_in_the_prompt() {
    let world = World::configured();
    let prompt_log = world.root().join("prompts.log");
    let script = recording_agent(&world, &prompt_log);
    let test_command = flaky_test_command(&world, "flaky-test.sh", 1);
    configure(
        &world,
        &format!(
            "- {{ name: build, action: agent, result_check: {{ exec: ['true'] }} }}\n- name: test\n  action: {{ exec: [\"sh\", {command}] }}\n  result_check: none\n  fail_action: {{ return_to: build, attempts: 2 }}\n- {{ name: merge, action: {{ builtin: merge }} }}\n",
            command = serde_json::to_string(&test_command.to_string_lossy()).unwrap(),
        ),
        &script,
    );
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "return-to-loop.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the looping run merges", || {
        status(&world)["tickets"]["merged"] == 1
    });

    // Two build spawns: the first entry and the re-entry the failing test
    // sent the walk back to.
    let prompts = prompts(&prompt_log);
    assert_eq!(prompts.len(), 2, "{prompts:?}");

    // The first attempt has nothing to answer for and is told nothing.
    assert!(
        !prompts[0].contains("previous attempt failed"),
        "{}",
        prompts[0]
    );

    // The re-entry names the stage, its reason, and quotes its output.
    let rerun = &prompts[1];
    assert!(rerun.contains("--- previous attempt failed ---"), "{rerun}");
    assert!(
        rerun.contains("Stage `test` (attempt 1) failed: exit 3"),
        "{rerun}"
    );
    assert!(
        rerun.contains("assertion failed: the widget is not blue"),
        "{rerun}"
    );
    assert!(rerun.contains("--- end previous attempt ---"), "{rerun}");
    // The same bytes a restarted daemon has to reproduce.
    assert!(rerun.trim_end().ends_with(FIRST_FAILURE_BLOCK), "{rerun}");
    // The block lands after the bootstrap and the ticket framing, not before.
    let bootstrap = rerun
        .find("sloop brief")
        .expect("the bootstrap prompt is present");
    assert!(
        bootstrap < rerun.find("previous attempt failed").expect("the block"),
        "{rerun}"
    );

    // Both passes are on the record, told apart by attempt.
    assert_eq!(
        shown_stages(&world, &world.run_alias(1)),
        [
            ("build".to_owned(), "passed".to_owned()),
            ("build#2".to_owned(), "passed".to_owned()),
            ("test".to_owned(), "failed".to_owned()),
            ("test#2".to_owned(), "passed".to_owned()),
            ("merge".to_owned(), "passed".to_owned()),
        ]
    );
}

/// A budget that runs out lands the ticket exactly where the same failure
/// would have without the edge, and both attempts stay visible.
#[test]
fn an_exhausted_return_budget_fails_with_every_attempt_visible() {
    let world = World::configured();
    let prompt_log = world.root().join("prompts.log");
    // The agent commits nothing, so this is a run whose failure the edge could
    // not repair and whose branch holds nothing worth reviewing — `failed`,
    // exactly as the same flow without the edge would have settled.
    let script = barren_agent(&world, &prompt_log);
    // Nothing ever fixes the test, so the single re-entry is spent and the
    // walk halts on the failure it could not get past.
    let test_command = flaky_test_command(&world, "always-failing.sh", 99);
    configure(
        &world,
        &format!(
            "- {{ name: build, action: agent, result_check: {{ exec: ['true'] }} }}\n- name: test\n  action: {{ exec: [\"sh\", {command}] }}\n  result_check: none\n  fail_action: {{ return_to: build, attempts: 1 }}\n- {{ name: merge, action: {{ builtin: merge }} }}\n",
            command = serde_json::to_string(&test_command.to_string_lossy()).unwrap(),
        ),
        &script,
    );
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "return-to-exhausted.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the exhausted run fails", || {
        status(&world)["tickets"]["failed"] == 1
    });
    assert_eq!(status(&world)["tickets"]["merged"], 0);

    // One jump was taken, so the agent ran twice and no more.
    assert_eq!(prompts(&prompt_log).len(), 2);

    // Merge was never reached, and both failing test executions are shown.
    assert_eq!(
        shown_stages(&world, &world.run_alias(1)),
        [
            ("build".to_owned(), "passed".to_owned()),
            ("build#2".to_owned(), "passed".to_owned()),
            ("test".to_owned(), "failed".to_owned()),
            ("test#2".to_owned(), "failed".to_owned()),
            ("merge".to_owned(), "pending".to_owned()),
        ]
    );
    // The reason names the failure the walk actually stopped on, not the
    // superseded first one.
    let reason = world.show_snapshot(&world.run_alias(1))["reason"]
        .as_str()
        .expect("a derived reason")
        .to_owned();
    assert!(reason.contains("stage `test` failed"), "{reason}");
    assert!(reason.contains("on attempt 2"), "{reason}");
    // A spent budget is a different ending from a plain halt, and the run says
    // which it was rather than leaving it to be inferred from the attempt
    // count. The wire token exists so a client need not parse the prose.
    assert!(reason.contains("return_to budget spent"), "{reason}");
    assert_eq!(
        world.show_snapshot(&world.run_alias(1))["halt"],
        "return_budget_exhausted"
    );
}

/// An advisory failure is recorded and rendered, and changes nothing about
/// where the run lands.
#[test]
fn an_advisory_failure_is_visible_but_does_not_block_the_merge() {
    let world = World::configured();
    let prompt_log = world.root().join("prompts.log");
    let script = recording_agent(&world, &prompt_log);
    configure(
        &world,
        "- { name: build, action: agent, result_check: { exec: ['true'] } }\n- name: lint\n  action: { exec: [\"false\"] }\n  result_check: none\n  fail_action: continue\n- { name: merge, action: { builtin: merge } }\n",
        &script,
    );
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "advisory-lint.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the run merges past its advisory lint", || {
        status(&world)["tickets"]["merged"] == 1
    });

    assert_eq!(
        shown_stages(&world, &world.run_alias(1)),
        [
            ("build".to_owned(), "passed".to_owned()),
            ("lint".to_owned(), "failed".to_owned()),
            ("merge".to_owned(), "passed".to_owned()),
        ]
    );
    // The agent ran once: an advisory failure loops nothing.
    assert_eq!(prompts(&prompt_log).len(), 1);

    // The row that failed is marked as the advisory it is, and the rows that
    // did not are not — a reader scanning the table has to be able to tell the
    // failure that would have ended the run from the one that could not.
    let stages = world.show_snapshot(&world.run_alias(1))["stages"].clone();
    let advisory = |name: &str| {
        stages
            .as_array()
            .expect("stages")
            .iter()
            .find(|stage| stage["stage"] == name)
            .unwrap_or_else(|| panic!("no stage `{name}`"))["advisory"]
            .clone()
    };
    assert_eq!(advisory("lint"), true);
    assert_eq!(advisory("build"), false);
    assert_eq!(advisory("merge"), false);

    // And the scan strip carries it in one token, so the distinction survives
    // into the view an operator reads before deciding to open the run.
    let text = String::from_utf8_lossy(&world.sloop_plain(&["show", &ticket]).stdout).into_owned();
    assert!(
        text.contains("build:ok  lint:warn  merge:ok"),
        "advisory strip missing: {text}"
    );
}

/// An advisory failure is never the reason a run ended: the walk stepped over
/// it by construction, so blaming the outcome on it would name the one stage
/// that provably did not cause it.
#[test]
fn the_derived_reason_names_the_halting_failure_not_the_advisory_one() {
    let world = World::configured();
    let prompt_log = world.root().join("prompts.log");
    let script = recording_agent(&world, &prompt_log);
    configure(
        &world,
        "- { name: build, action: agent, result_check: { exec: ['true'] } }\n- name: lint\n  action: { exec: [\"false\"] }\n  result_check: none\n  fail_action: continue\n- name: test\n  action: { exec: [\"false\"] }\n  result_check: none\n  fail_action: fail\n- { name: merge, action: { builtin: merge } }\n",
        &script,
    );
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "advisory-then-halt.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the run stops for review", || {
        status(&world)["tickets"]["needs_review"] == 1
    });

    assert_eq!(
        shown_stages(&world, &world.run_alias(1)),
        [
            ("build".to_owned(), "passed".to_owned()),
            ("lint".to_owned(), "failed".to_owned()),
            ("test".to_owned(), "failed".to_owned()),
            ("merge".to_owned(), "pending".to_owned()),
        ]
    );
    let run = world.show_snapshot(&world.run_alias(1));
    let reason = run["reason"].as_str().expect("a derived reason").to_owned();
    assert!(reason.contains("stage `test` failed"), "{reason}");
    assert!(!reason.contains("lint"), "{reason}");
    // A plain `fail_action: fail` halt: the sentence above already explains
    // itself, so the reason gains no epithet, but the token still says which
    // halt it was.
    assert_eq!(run["halt"], "fail_action");
}

/// The exact block a re-entry is shown after the first `flaky_test_command`
/// failure. Pinned as a literal, and asserted by both the straight-through
/// loop and the restarted one, because "the resumed daemon composes the same
/// prompt" is a claim about bytes: the block is rebuilt from the run's
/// persisted evidence by a process that never watched the failure happen.
const FIRST_FAILURE_BLOCK: &str = concat!(
    "--- previous attempt failed ---\n",
    "Stage `test` (attempt 1) failed: exit 3\n",
    "You are running again to address that failure.\n",
    "\n",
    "The last 100 lines of that stage's output:\n",
    "assertion failed: the widget is not blue\n",
    "run 1 of the test command\n",
    "--- end previous attempt ---",
);

/// A daemon that dies inside a loop must resume on the execution the walk was
/// standing on, and hand it the prompt the dead daemon would have. Nothing
/// about the jump survived in memory, so every byte of the block comes back
/// out of the evidence log.
#[test]
fn a_restart_mid_loop_resumes_the_same_attempt_with_the_same_prompt() {
    let world = World::configured();
    let prompt_log = world.root().join("prompts.log");
    let script = recording_agent(&world, &prompt_log);
    let test_command = flaky_test_command(&world, "flaky-test.sh", 1);
    configure(
        &world,
        &format!(
            "- {{ name: build, action: agent, result_check: {{ exec: ['true'] }} }}\n- name: test\n  action: {{ exec: [\"sh\", {command}] }}\n  result_check: none\n  fail_action: {{ return_to: build, attempts: 2 }}\n- {{ name: merge, action: {{ builtin: merge }} }}\n",
            command = serde_json::to_string(&test_command.to_string_lossy()).unwrap(),
        ),
        &script,
    );
    world.commit_all("initial");
    // Stop the walk in the gap between recording the failure and acting on
    // it: the jump is durable, nothing has re-launched, and the run is
    // between stages — which is where a daemon can be replaced.
    world.arm_test_hook("after-stage-test");
    let first = World::json_stdout(&world.sloop(&["daemon"]))["data"]["pid"]
        .as_u64()
        .unwrap() as u32;
    let ticket = post(&world, "restart-mid-loop.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow(
        "the walk records the failure that triggers the jump",
        || world.test_hook_reached("after-stage-test"),
    );
    world.kill_daemon(first);
    // The first daemon composed no re-run prompt: it died holding the jump.
    assert_eq!(prompts(&prompt_log).len(), 1);

    world.release_test_hook("after-stage-test");
    world.start_daemon();
    wait_until_slow("the resumed run merges", || {
        status(&world)["tickets"]["merged"] == 1
    });

    // The resumed daemon re-entered `build` — not `test`, and not `build`
    // from the top — and rebuilt the whole block from the log.
    let prompts = prompts(&prompt_log);
    assert_eq!(prompts.len(), 2, "{prompts:?}");
    assert!(
        prompts[1].trim_end().ends_with(FIRST_FAILURE_BLOCK),
        "{}",
        prompts[1]
    );

    // The interrupted execution was resumed, not recorded twice or skipped.
    assert_eq!(
        shown_stages(&world, &world.run_alias(1)),
        [
            ("build".to_owned(), "passed".to_owned()),
            ("build#2".to_owned(), "passed".to_owned()),
            ("test".to_owned(), "failed".to_owned()),
            ("test#2".to_owned(), "passed".to_owned()),
            ("merge".to_owned(), "passed".to_owned()),
        ]
    );
}

/// A `return_to` onto a `reported` stage gets a fresh report per execution:
/// the first attempt's verdict must not be reused for the second, and the
/// second worker's report must not be rejected as a duplicate.
#[test]
fn a_re_entered_reported_stage_is_judged_again() {
    let world = World::configured();
    // The review stage fails its first execution and passes its second,
    // reporting through the worker socket each time.
    let counter = world.root().join("review.count");
    fs::write(&counter, b"").unwrap();
    let review = write_script(
        &world,
        "review.sh",
        &format!(
            "printf x >> {counter}\nruns=$(wc -c < {counter} | tr -d ' ')\nif [ \"$runs\" -le 1 ]; then\n  {sloop} --json verdict fail --reason 'needs another pass' >/dev/null\nelse\n  {sloop} --json verdict pass --reason 'looks right now' >/dev/null\nfi\nexit 0\n",
            counter = shell_quote(&counter.to_string_lossy()),
            sloop = shell_quote(env!("CARGO_BIN_EXE_sloop")),
        ),
    );
    let prompt_log = world.root().join("prompts.log");
    let script = recording_agent(&world, &prompt_log);
    configure(
        &world,
        &format!(
            "- {{ name: build, action: agent, result_check: {{ exec: ['true'] }} }}\n- name: review\n  action: {{ exec: [\"sh\", {command}] }}\n  result_check: reported\n  fail_action: {{ return_to: build, attempts: 2 }}\n- {{ name: merge, action: {{ builtin: merge }} }}\n",
            command = serde_json::to_string(&review.to_string_lossy()).unwrap(),
        ),
        &script,
    );
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "reported-loop.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the re-reviewed run merges", || {
        status(&world)["tickets"]["merged"] == 1
    });

    assert_eq!(
        shown_stages(&world, &world.run_alias(1)),
        [
            ("build".to_owned(), "passed".to_owned()),
            ("build#2".to_owned(), "passed".to_owned()),
            ("review".to_owned(), "failed".to_owned()),
            ("review#2".to_owned(), "passed".to_owned()),
            ("merge".to_owned(), "passed".to_owned()),
        ]
    );
    // The reviewer's own words reached the re-entered agent, not an exit code.
    let rerun = &prompts(&prompt_log)[1];
    assert!(
        rerun.contains("Stage `review` (attempt 1) failed: needs another pass"),
        "{rerun}"
    );
}

/// An `exec` action's command line is fixed by the flow, so a re-entered exec
/// stage is handed no context — there is nowhere for it to go, and inventing
/// an argument would change what the operator asked to run.
#[test]
fn a_re_entered_exec_stage_is_handed_no_context() {
    let world = World::configured();
    let prompt_log = world.root().join("prompts.log");
    let script = recording_agent(&world, &prompt_log);
    // `probe` re-runs inside the span and records its whole argv and
    // environment; nothing in either may mention the failure.
    let argv_log = world.root().join("probe-argv.log");
    fs::write(&argv_log, b"").unwrap();
    let probe = write_script(
        &world,
        "probe.sh",
        &format!(
            "printf '%s\\n' \"$*\" >> {log}\nenv >> {log}\nexit 0\n",
            log = shell_quote(&argv_log.to_string_lossy()),
        ),
    );
    let test_command = flaky_test_command(&world, "flaky-test.sh", 1);
    configure(
        &world,
        &format!(
            "- {{ name: build, action: agent, result_check: {{ exec: ['true'] }} }}\n- name: probe\n  action: {{ exec: [\"sh\", {probe}, \"fixed-argument\"] }}\n  result_check: none\n- name: test\n  action: {{ exec: [\"sh\", {command}] }}\n  result_check: none\n  fail_action: {{ return_to: probe, attempts: 2 }}\n- {{ name: merge, action: {{ builtin: merge }} }}\n",
            probe = serde_json::to_string(&probe.to_string_lossy()).unwrap(),
            command = serde_json::to_string(&test_command.to_string_lossy()).unwrap(),
        ),
        &script,
    );
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "exec-reentry.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the run merges after re-running the probe", || {
        status(&world)["tickets"]["merged"] == 1
    });

    let recorded = fs::read_to_string(&argv_log).unwrap();
    // The probe ran twice with the same fixed argument and learned nothing
    // about the failure that sent the walk back to it.
    assert_eq!(recorded.matches("fixed-argument").count(), 2, "{recorded}");
    assert!(!recorded.contains("previous attempt failed"), "{recorded}");
    assert!(!recorded.contains("the widget is not blue"), "{recorded}");
    // `build` sits before the jump target, outside the span, so it ran once:
    // a backward edge re-runs the span it names and nothing else.
    assert_eq!(prompts(&prompt_log).len(), 1);
}

/// `on_fail` is deprecated, not removed: a flow using it still repairs, and
/// the daemon says so in its log when it admits the run.
#[test]
fn a_legacy_on_fail_flow_still_repairs_and_logs_the_deprecation() {
    let world = World::configured();
    let script = write_script(
        &world,
        "fake-agent.sh",
        &format!(
            "case \"$1\" in\n  *REPAIR*)\n    : > fixed.txt\n    git add fixed.txt\n    {commit} --allow-empty -m repair ;;\n  *)\n    {commit} --allow-empty -m build ;;\nesac\nexit 0\n",
            commit = commit("agent"),
        ),
    );
    configure(
        &world,
        "- { name: build, action: agent, result_check: { exec: ['true'] } }\n- name: test\n  action: { exec: [\"sh\", \"-c\", \"test -f fixed.txt\"] }\n  on_fail:\n    agent: \"REPAIR make the test pass\"\n    attempts: 2\n- { name: merge, action: { builtin: merge } }\n",
        &script,
    );
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "legacy-on-fail.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the repaired legacy run merges", || {
        status(&world)["tickets"]["merged"] == 1
    });

    let note = fs::read_to_string(world.daemon_log())
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|record| record["event"] == "flow_on_fail_deprecated")
        .expect("the deprecation note is in the daemon log");
    let fields = &note["fields"];
    assert_eq!(fields["stages"][0], "test");
    assert_eq!(fields["ticket_id"], ticket.as_str());
    assert!(
        fields["replacement"]
            .as_str()
            .is_some_and(|text| text.contains("return_to")),
        "{note}"
    );
}

/// A flow whose backward edges could spin is refused before anything runs,
/// so the bound is a property of the file rather than of the walk.
#[test]
fn a_flow_whose_return_budgets_exceed_the_cap_is_rejected_at_post_time() {
    let world = World::configured();
    let script = write_script(&world, "fake-agent.sh", "exit 0\n");
    let mut flow = String::from("- { name: build, action: agent }\n");
    for index in 1..9 {
        flow.push_str(&format!(
            "- {{ name: s{index}, action: {{ exec: ['true'] }} }}\n"
        ));
    }
    flow.push_str(
        "- { name: last, action: { exec: ['true'] }, fail_action: { return_to: build, attempts: 3 } }\n",
    );
    configure(&world, &flow, &script);
    world.commit_all("initial");

    let output = world.sloop(&["daemon"]);
    let message = World::json_stdout_or_stderr(&output).to_string();
    assert!(!output.status.success(), "{message}");
    assert!(message.contains("at most 32 stages"), "{message}");
    assert!(message.contains("imply 40"), "{message}");
}

/// The advisory and looping forms are usable through the shipped template's
/// own vocabulary, which is the spelling operators copy.
#[test]
fn the_new_grammar_spellings_are_accepted_by_a_running_daemon() {
    let world = World::configured();
    let prompt_log = world.root().join("prompts.log");
    let script = recording_agent(&world, &prompt_log);
    configure(
        &world,
        "- name: build\n  action: agent\n  result_check: { builtin: commits }\n  fail_action: fail\n- name: lint\n  action: { exec: ['false'] }\n  result_check: none\n  fail_action: continue\n- name: merge\n  action: { builtin: merge }\n  result_check: none\n",
        &script,
    );
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "new-grammar.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the new-grammar flow merges", || {
        status(&world)["tickets"]["merged"] == 1
    });
    wait_until("the run alias resolves", || world.run_alias(1) != "pending");
    assert_eq!(
        shown_stages(&world, &world.run_alias(1))
            .into_iter()
            .map(|(_, state)| state)
            .collect::<Vec<_>>(),
        ["passed", "failed", "passed"]
    );
}
