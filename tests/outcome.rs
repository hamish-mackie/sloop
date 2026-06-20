mod support;

use std::fs;
use std::path::PathBuf;
use std::process::Command;

use support::{World, process_alive, wait_until};

/// Writes a scripted fake agent and a repository config pointing at it, with
/// an optional aftercare test command. The agent script is committed before
/// the daemon starts so worktrees branch from a clean default branch.
fn configure(world: &World, agent_body: &str, test_cmd: Option<&str>) {
    let script = world.root().join("fake-agent.sh");
    fs::write(&script, format!("#!/bin/sh\n{agent_body}")).expect("write fake agent script");

    let aftercare = match test_cmd {
        Some(cmd) => format!("aftercare:\n  test_cmd: [\"sh\", \"-c\", \"{cmd}\"]\n"),
        None => String::new(),
    };
    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        format!(
            "version: 1\nscheduler:\n  max_parallel_tasks: 1\nagent:\n  default_target: fake\n  targets:\n    fake:\n      cmd: [\"sh\", \"{}\", \"{{prompt}}\"]\n{aftercare}",
            script.display()
        ),
    )
    .expect("write agent config");
}

/// An agent that commits one file, as a well-behaved worker would.
const COMMITTING_AGENT: &str = concat!(
    "echo done > work.txt\n",
    "git add work.txt\n",
    "git -c user.name=agent -c user.email=agent@example.invalid commit --quiet -m 'agent work'\n",
    "exit 0\n"
);

fn post_and_run(world: &World, name: &str) -> String {
    let ticket = world.write_ticket(name, "# Work\n");
    let output = world.sloop(&["post", ticket.to_str().unwrap(), "--manual"]);
    assert!(output.status.success());
    let id = World::json_stdout(&output)["data"]["ticket"]["id"]
        .as_str()
        .expect("ticket id")
        .to_owned();
    assert!(world.sloop(&["run", &id]).status.success());
    id
}

/// Failure envelopes are written to stderr; success envelopes to stdout.
fn json_stderr(output: &std::process::Output) -> serde_json::Value {
    serde_json::from_slice(&output.stderr).expect("stderr is JSON")
}

fn tickets(world: &World) -> serde_json::Value {
    let output = world.sloop(&["status"]);
    assert!(output.status.success());
    World::json_stdout(&output)["data"]["tickets"].clone()
}

fn default_branch_has(world: &World, file: &str) -> bool {
    // The default branch is what the root checkout points at; a completed
    // fast-forward merge makes the agent's file visible there.
    world.root().join(file).is_file()
}

fn read_process_ids(path: PathBuf) -> Vec<u32> {
    fs::read_to_string(path)
        .expect("read aftercare process IDs")
        .split_whitespace()
        .map(|pid| pid.parse().expect("process ID is an integer"))
        .collect()
}

fn wait_for_processes_to_exit(process_ids: Vec<u32>) {
    for pid in process_ids {
        wait_until(&format!("aftercare process {pid} exits"), || {
            !process_alive(pid)
        });
    }
}

#[test]
fn committed_work_with_passing_tests_is_merged() {
    let world = World::configured();
    configure(&world, COMMITTING_AGENT, Some("echo tests passed"));
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "merge-me.md");

    wait_until("the ticket reaches merged", || {
        tickets(&world)["merged"] == 1
    });
    assert!(default_branch_has(&world, "work.txt"));

    // The aftercare test stage's output is captured evidence.
    let output = world.sloop(&["logs", "R1"]);
    assert!(output.status.success());
    let entries = World::json_stdout(&output)["data"]["entries"].clone();
    assert!(
        entries
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["source"] == "aftercare" && entry["stage"] == "test"),
        "no aftercare records in {entries}"
    );
}

#[test]
fn restart_reruns_an_interrupted_aftercare_stage_and_then_merges() {
    let world = World::configured();
    let started = world.root().join("test-started");
    let release = world.root().join("test-release");
    let finished = world.root().join("test-finished");
    let invocations = world.root().join("test-invocations");
    let test_cmd = format!(
        "printf x >> {invocations}; touch {started}; while [ ! -e {release} ]; do sleep 0.01; done; touch {finished}",
        invocations = invocations.display(),
        started = started.display(),
        release = release.display(),
        finished = finished.display(),
    );
    configure(&world, COMMITTING_AGENT, Some(&test_cmd));
    world.commit_all("initial");
    let daemon_pid = world.start_daemon()["data"]["pid"]
        .as_u64()
        .expect("daemon pid") as u32;
    post_and_run(&world, "recover-aftercare.md");
    wait_until("the first test invocation starts", || started.is_file());

    world.kill_daemon(daemon_pid);
    world.start_daemon();
    wait_until("recovery replaces the interrupted test", || {
        fs::read_to_string(&invocations).is_ok_and(|contents| contents == "xx")
    });
    fs::write(&release, "").expect("release interrupted test");

    wait_until("recovered aftercare merges the work", || {
        tickets(&world)["merged"] == 1
    });
    assert!(finished.is_file());
    assert_eq!(
        fs::read_to_string(invocations).expect("read invocation record"),
        "xx"
    );
    assert!(default_branch_has(&world, "work.txt"));
}

#[test]
fn cancel_stops_an_active_aftercare_process_and_releases_the_ticket() {
    let world = World::configured();
    let process_ids = world.root().join("cancel-test-processes");
    let test_cmd = format!(
        "sleep 1000 & printf '%s %s' $$ $! > {process_ids}; wait",
        process_ids = process_ids.display(),
    );
    configure(&world, COMMITTING_AGENT, Some(&test_cmd));
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "cancel-aftercare.md");
    wait_until("the aftercare process checkpoint is durable", || {
        process_ids.is_file() && world.run_evidence("R1", "test_process").is_some()
    });
    let process_ids = read_process_ids(process_ids);

    let cancelled = world.sloop(&["cancel", "R1"]);
    assert!(cancelled.status.success());
    wait_until("cancelled aftercare settles", || {
        let counts = tickets(&world);
        counts["ready"] == 1 && counts["claimed"] == 0
    });
    let waited = world.sloop(&["wait", "R1", "--timeout", "5"]);
    assert!(!waited.status.success());
    assert_eq!(
        World::json_stdout_or_stderr(&waited)["data"]["state"],
        "cancelled"
    );
    wait_for_processes_to_exit(process_ids);
}

#[test]
fn cancellation_before_the_test_process_checkpoint_stops_aftercare() {
    const HOOK: &str = "before-test-process-checkpoint";

    let world = World::configured();
    world.arm_test_hook(HOOK);
    let process_ids = world.root().join("racing-test-processes");
    let test_cmd = format!(
        "sleep 1000 & printf '%s %s' $$ $! > {process_ids}; wait",
        process_ids = process_ids.display(),
    );
    configure(&world, COMMITTING_AGENT, Some(&test_cmd));
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "cancel-before-checkpoint.md");
    wait_until("the test process reaches its checkpoint gate", || {
        world.test_hook_reached(HOOK) && process_ids.is_file()
    });
    assert!(world.run_evidence("R1", "test_process").is_none());

    let cancelled = world.sloop(&["cancel", "R1"]);
    assert!(cancelled.status.success());
    assert!(world.run_evidence("R1", "cancel_requested").is_some());
    assert!(world.run_evidence("R1", "test_process").is_none());
    let process_ids = read_process_ids(process_ids);
    world.release_test_hook(HOOK);

    wait_until("cancelled aftercare settles", || {
        let counts = tickets(&world);
        counts["ready"] == 1 && counts["claimed"] == 0
    });
    let waited = world.sloop(&["wait", "R1", "--timeout", "5"]);
    assert!(!waited.status.success());
    assert_eq!(
        World::json_stdout_or_stderr(&waited)["data"]["state"],
        "cancelled"
    );
    assert!(world.run_evidence("R1", "test_process").is_some());
    wait_for_processes_to_exit(process_ids);
}

#[test]
fn a_moved_default_branch_without_conflicts_still_merges() {
    let world = World::configured();
    // The agent commits its work, then moves the default branch with an
    // unrelated commit before exiting — deterministically simulating an
    // operator landing something mid-run. `$0` is the script's absolute
    // path in the repository root.
    configure(
        &world,
        concat!(
            "echo done > work.txt\n",
            "git add work.txt\n",
            "git -c user.name=agent -c user.email=agent@example.invalid commit --quiet -m 'agent work'\n",
            "root=\"$(dirname \"$0\")\"\n",
            "echo moved > \"$root/moved.txt\"\n",
            "git -C \"$root\" add moved.txt\n",
            "git -C \"$root\" -c user.name=op -c user.email=op@example.invalid commit --quiet -m 'main moved'\n",
            "exit 0\n",
        ),
        None,
    );
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "diverge-me.md");

    wait_until("the ticket reaches merged", || {
        tickets(&world)["merged"] == 1
    });
    // Both histories are present: the merge produced a merge commit rather
    // than parking the run for a human.
    assert!(default_branch_has(&world, "work.txt"));
    assert!(default_branch_has(&world, "moved.txt"));
}

#[test]
fn a_conflicting_merge_parks_the_ticket_and_leaves_the_checkout_clean() {
    let world = World::configured();
    // Same interleaving, but both sides edit the same file: the merge must
    // conflict, abort, and leave the work for a human.
    configure(
        &world,
        concat!(
            "echo agent-version > work.txt\n",
            "git add work.txt\n",
            "git -c user.name=agent -c user.email=agent@example.invalid commit --quiet -m 'agent work'\n",
            "root=\"$(dirname \"$0\")\"\n",
            "echo main-version > \"$root/work.txt\"\n",
            "git -C \"$root\" add work.txt\n",
            "git -C \"$root\" -c user.name=op -c user.email=op@example.invalid commit --quiet -m 'main moved'\n",
            "exit 0\n",
        ),
        None,
    );
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "conflict-me.md");

    wait_until("the ticket reaches needs_review", || {
        tickets(&world)["needs_review"] == 1
    });
    // The aborted merge must leave no conflict markers or MERGE_HEAD behind.
    assert!(!world.root().join(".git/MERGE_HEAD").exists());
    // Untracked runtime state (tickets, worktrees) is expected; tracked
    // files must show no leftover conflict content.
    let status = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=no"])
        .current_dir(world.root())
        .output()
        .unwrap();
    assert_eq!(
        String::from_utf8_lossy(&status.stdout).trim(),
        "",
        "checkout must be clean after an aborted merge"
    );
    assert_eq!(
        fs::read_to_string(world.root().join("work.txt"))
            .unwrap()
            .trim(),
        "main-version",
        "the default branch keeps its own version"
    );
}

#[test]
fn exit_zero_without_commits_fails_the_ticket() {
    let world = World::configured();
    configure(&world, "exit 0\n", Some("echo tested > tested.txt"));
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "empty.md");

    wait_until("the ticket reaches failed", || {
        tickets(&world)["failed"] == 1
    });
    // Tests qualify committed work for merging; a run without commits never
    // reaches the test stage.
    assert!(!world.root().join(".worktrees/R1/tested.txt").exists());
}

#[test]
fn committed_work_with_failing_tests_is_left_for_review() {
    let world = World::configured();
    configure(&world, COMMITTING_AGENT, Some("exit 1"));
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "review-me.md");

    wait_until("the ticket reaches needs_review", || {
        tickets(&world)["needs_review"] == 1
    });
    assert!(
        !default_branch_has(&world, "work.txt"),
        "failing tests must never merge"
    );

    // The branch survives so a human can salvage the commits.
    let branches = Command::new("git")
        .args(["branch", "--list", "sloop/*"])
        .current_dir(world.root())
        .output()
        .unwrap();
    assert!(!String::from_utf8_lossy(&branches.stdout).trim().is_empty());
}

fn pid_alive(pid: &str) -> bool {
    Command::new("kill")
        .args(["-0", pid])
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

#[test]
fn cancel_kills_the_whole_process_group_and_frees_the_ticket() {
    let world = World::configured();
    // The agent backgrounds a grandchild, records its PID, then blocks.
    configure(
        &world,
        concat!(
            "echo started\n",
            "sh -c 'sleep 30' &\n",
            "echo $! > grandchild.pid\n",
            "tries=0\n",
            "while [ \"$tries\" -lt 400 ]; do sleep 0.05; tries=$((tries + 1)); done\n",
        ),
        None,
    );
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "cancel-me.md");

    let pid_file: PathBuf = world.root().join(".worktrees/R1/grandchild.pid");
    wait_until("the agent starts and records its grandchild", || {
        pid_file.is_file()
    });
    let grandchild = fs::read_to_string(&pid_file).unwrap().trim().to_owned();
    assert!(pid_alive(&grandchild));

    let output = world.sloop(&["cancel", "R1"]);
    assert!(
        output.status.success(),
        "cancel failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let data = World::json_stdout(&output)["data"].clone();
    assert_eq!(data["run"], "R1");
    assert_eq!(data["state"], "cancelling");
    assert_eq!(data["preserved"], true);

    wait_until("the grandchild dies with the group", || {
        !pid_alive(&grandchild)
    });
    wait_until("the ticket returns to ready", || {
        tickets(&world)["ready"] == 1
    });

    // Worktree, branch, and captured logs survive cancellation as evidence.
    assert!(world.root().join(".worktrees/R1").is_dir());
    let output = world.sloop(&["logs", "R1"]);
    assert!(output.status.success());
    assert!(
        !World::json_stdout(&output)["data"]["entries"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    // Cancelling twice while the exit is already resolved cannot double-free.
    let repeat = world.sloop(&["cancel", "R1"]);
    let response = json_stderr(&repeat);
    assert_eq!(response["ok"], false);
    assert_eq!(response["error"]["code"], "conflict");
}

#[test]
fn a_finished_run_cannot_be_cancelled() {
    let world = World::configured();
    configure(&world, "exit 0\n", None);
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "done.md");

    wait_until("the run finishes", || tickets(&world)["failed"] == 1);

    let output = world.sloop(&["cancel", "R1"]);
    let response = json_stderr(&output);
    assert_eq!(response["ok"], false);
    assert_eq!(response["error"]["code"], "conflict");

    let missing = world.sloop(&["cancel", "R99"]);
    let response = json_stderr(&missing);
    assert_eq!(response["error"]["code"], "not_found");
}
