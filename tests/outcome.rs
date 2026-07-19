mod support;

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;

use support::{World, process_alive, wait_until};

/// Writes a scripted fake agent and a repository config pointing at it, with
/// an optional aftercare test command. The agent script is committed before
/// the daemon starts so worktrees branch from a clean default branch.
fn configure(world: &World, agent_body: &str, test_cmd: Option<&str>) {
    let script = world.root().join("fake-agent.sh");
    fs::write(&script, format!("#!/bin/sh\n{agent_body}")).expect("write fake agent script");
    fs::create_dir_all(world.root().join(".agents/sloop/flows")).expect("create flow directory");
    fs::write(
        world.root().join(".agents/sloop/flows/default.yaml"),
        "stages:\n  - { name: build, kind: build }\n  - { name: merge, kind: merge }\n",
    )
    .expect("write default test flow");

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
    let id = post_manual(world, name);
    assert!(world.sloop(&["run", &id]).status.success());
    id
}

fn post_manual(world: &World, name: &str) -> String {
    let ticket = world.write_ticket(name, "# Work\n");
    let output = world.sloop(&["post", ticket.to_str().unwrap(), "--manual"]);
    assert!(output.status.success());
    World::json_stdout(&output)["data"]["ticket"]["id"]
        .as_str()
        .expect("ticket id")
        .to_owned()
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

fn write_flow(world: &World, contents: &str) {
    fs::write(
        world.root().join(".agents/sloop/flows/default.yaml"),
        contents,
    )
    .expect("write test flow");
}

fn aftercare_stages(world: &World, run_id: &str) -> Vec<(i64, String, String, String)> {
    let connection = rusqlite::Connection::open(world.db_path()).expect("open state database");
    let mut statement = connection
        .prepare(
            "SELECT stage_index, stage, state, evidence_json
             FROM aftercare_stages WHERE run_id = ?1 ORDER BY stage_index",
        )
        .expect("prepare aftercare stage query");
    statement
        .query_map([run_id], |row| {
            let evidence: String = row.get(3)?;
            let evidence: serde_json::Value = serde_json::from_str(&evidence).unwrap();
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                evidence["output"].as_str().unwrap_or_default().to_owned(),
            ))
        })
        .expect("query aftercare stages")
        .collect::<Result<Vec<_>, _>>()
        .expect("read aftercare stages")
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

fn spawn_unrelated_process_group() -> Child {
    Command::new("sh")
        .args(["-c", "exec sleep 1000"])
        .process_group(0)
        .spawn()
        .expect("spawn unrelated process group")
}

struct InterruptedMergeFixture {
    started: PathBuf,
    invocations: PathBuf,
}

fn configure_blocking_merge(world: &World) -> InterruptedMergeFixture {
    configure(
        world,
        concat!(
            "echo agent > work.txt\n",
            "git add work.txt\n",
            "git -c user.name=agent -c user.email=agent@example.invalid commit --quiet -m agent\n",
            "root=\"$(dirname \"$0\")\"\n",
            "echo main > \"$root/main.txt\"\n",
            "git -C \"$root\" add main.txt\n",
            "git -C \"$root\" -c user.name=operator -c user.email=operator@example.invalid commit --quiet -m main\n",
        ),
        None,
    );
    let hook = world.root().join(".git/hooks/pre-merge-commit");
    let fixture = InterruptedMergeFixture {
        started: world.root().join("merge-hook-started"),
        invocations: world.root().join("merge-hook-invocations"),
    };
    let release = world.root().join("merge-hook-release");
    fs::write(
        &hook,
        format!(
            "#!/bin/sh\nprintf x >> {invocations}\ntouch {started}\nwhile [ ! -e {release} ]; do sleep 0.01; done\n",
            invocations = fixture.invocations.display(),
            started = fixture.started.display(),
            release = release.display(),
        ),
    )
    .unwrap();
    fs::set_permissions(&hook, fs::Permissions::from_mode(0o755)).unwrap();
    world.commit_all("initial");
    fixture
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
fn built_in_default_without_test_command_merges_committed_work() {
    let world = World::configured();
    let invocations = world.root().join("agent-invocations");
    let agent = format!(
        "printf x >> {}\n{}",
        invocations.display(),
        COMMITTING_AGENT
    );
    configure(&world, &agent, None);
    fs::remove_file(world.root().join(".agents/sloop/flows/default.yaml")).unwrap();
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "built-in-default.md");

    wait_until("the built-in default merges", || {
        tickets(&world)["merged"] == 1
    });
    assert_eq!(fs::read_to_string(invocations).unwrap(), "x");
    assert!(default_branch_has(&world, "work.txt"));
}

#[test]
fn flow_executes_in_order_and_records_one_row_per_stage() {
    let world = World::configured();
    configure(&world, COMMITTING_AGENT, None);
    let check = world.root().join("check-stage.sh");
    fs::write(
        &check,
        "#!/bin/sh\nset -eu\ntest -z \"${SLOOP_SOCKET:-}\"\ntest -z \"${SLOOP_TOKEN:-}\"\necho checked\n",
    )
    .unwrap();
    write_flow(
        &world,
        &format!(
            "stages:\n  - {{ name: build, kind: build }}\n  - name: check\n    kind: exec\n    cmd: [\"sh\", {}]\n  - {{ name: merge, kind: merge }}\n",
            serde_json::to_string(&check.to_string_lossy()).unwrap(),
        ),
    );
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "flow-pass.md");

    wait_until("the flow merges", || tickets(&world)["merged"] == 1);
    assert!(default_branch_has(&world, "work.txt"));
    let stages = aftercare_stages(&world, "R1");
    assert_eq!(
        stages
            .iter()
            .map(|(index, name, state, _)| (*index, name.as_str(), state.as_str()))
            .collect::<Vec<_>>(),
        [
            (0, "build", "passed"),
            (1, "check", "passed"),
            (2, "merge", "passed")
        ]
    );
    assert!(
        stages
            .iter()
            .all(|(_, _, _, output)| output == "runs/R1/output.ndjson")
    );
    let logs = world.sloop(&["logs", "R1"]);
    let entries = World::json_stdout(&logs)["data"]["entries"]
        .as_array()
        .unwrap()
        .clone();
    assert!(
        entries
            .iter()
            .any(|entry| entry["source"] == "aftercare" && entry["stage"] == "check")
    );
}

#[test]
fn failed_exec_halts_before_merge_and_preserves_commits_for_review() {
    let world = World::configured();
    configure(&world, COMMITTING_AGENT, None);
    write_flow(
        &world,
        "stages:\n  - { name: build, kind: build }\n  - { name: reject, kind: exec, cmd: ['false'] }\n  - { name: merge, kind: merge }\n",
    );
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "flow-fail.md");

    wait_until("the flow needs review", || {
        tickets(&world)["needs_review"] == 1
    });
    assert!(!default_branch_has(&world, "work.txt"));
    assert_eq!(
        aftercare_stages(&world, "R1")
            .into_iter()
            .map(|(_, name, state, _)| (name, state))
            .collect::<Vec<_>>(),
        [
            ("build".into(), "passed".into()),
            ("reject".into(), "failed".into())
        ]
    );
}

#[test]
fn unknown_bound_flow_never_falls_back_to_default() {
    let world = World::configured();
    configure(&world, COMMITTING_AGENT, None);
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_manual(&world, "missing-flow.md");
    let connection = rusqlite::Connection::open(world.db_path()).unwrap();
    connection
        .execute(
            "UPDATE tickets SET flow = 'missing' WHERE id = ?1",
            [&ticket],
        )
        .unwrap();

    assert!(world.sloop(&["run", &ticket]).status.success());
    wait_until("the invalid flow is rejected before claim", || {
        fs::read_to_string(world.daemon_log())
            .is_ok_and(|log| log.contains("bound_flow_resolution_failed"))
    });
    assert!(!world.root().join(".worktrees/R1").exists());
    let runs: i64 = connection
        .query_row("SELECT COUNT(*) FROM runs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(runs, 0);
    assert_eq!(tickets(&world)["ready"], 1);
}

#[test]
fn stage_evidence_write_failure_does_not_strand_active_accounting() {
    let world = World::configured();
    configure(&world, COMMITTING_AGENT, None);
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_manual(&world, "stage-write-failure.md");
    let connection = rusqlite::Connection::open(world.db_path()).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_aftercare_stage
             BEFORE INSERT ON aftercare_stages
             BEGIN SELECT RAISE(FAIL, 'stage write denied'); END;",
        )
        .unwrap();

    assert!(world.sloop(&["run", &ticket]).status.success());
    wait_until("the evidence failure settles for review", || {
        tickets(&world)["needs_review"] == 1
    });
    let status = world.sloop(&["status"]);
    assert_eq!(
        World::json_stdout(&status)["data"]["gate"]["active_agents"],
        0
    );
}

#[test]
fn merge_success_survives_merge_stage_evidence_failure() {
    let world = World::configured();
    configure(&world, COMMITTING_AGENT, None);
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_manual(&world, "merge-evidence-failure.md");
    let connection = rusqlite::Connection::open(world.db_path()).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_merge_stage
             BEFORE INSERT ON aftercare_stages
             WHEN NEW.stage = 'merge'
             BEGIN SELECT RAISE(FAIL, 'merge stage write denied'); END;",
        )
        .unwrap();

    assert!(world.sloop(&["run", &ticket]).status.success());
    wait_until(
        "the successful merge settles despite evidence failure",
        || tickets(&world)["merged"] == 1,
    );
    assert!(default_branch_has(&world, "work.txt"));
    assert_eq!(
        aftercare_stages(&world, "R1")
            .into_iter()
            .map(|(_, stage, _, _)| stage)
            .collect::<Vec<_>>(),
        ["build"]
    );
}

#[test]
fn incomplete_commit_observation_keeps_failed_aftercare_for_review() {
    let world = World::configured();
    configure(
        &world,
        "root=\"$(dirname \"$0\")\"\ngit reflog expire --expire=now --all\nexit 0\n",
        None,
    );
    write_flow(
        &world,
        "stages:\n  - { name: build, kind: build }\n  - { name: reject, kind: exec, cmd: ['false'] }\n  - { name: merge, kind: merge }\n",
    );
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "unknown-commits.md");

    wait_until("unknown commit evidence is retained for review", || {
        tickets(&world)["needs_review"] == 1
    });
    assert_eq!(
        world.run_evidence("R1", "commits_observed").unwrap()["complete"],
        false
    );
}

#[test]
fn exec_stage_order_comes_from_the_flow_file() {
    let world = World::configured();
    configure(&world, COMMITTING_AGENT, None);
    let order = world.root().join("stage-order");
    write_flow(
        &world,
        &format!(
            "stages:\n  - {{ name: build, kind: build }}\n  - {{ name: first, kind: exec, cmd: [\"sh\", \"-c\", \"printf first, >> {}\"] }}\n  - {{ name: second, kind: exec, cmd: [\"sh\", \"-c\", \"printf second >> {}\"] }}\n  - {{ name: merge, kind: merge }}\n",
            order.display(),
            order.display(),
        ),
    );
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "ordered.md");

    wait_until("the ordered flow merges", || tickets(&world)["merged"] == 1);
    assert_eq!(fs::read_to_string(order).unwrap(), "first,second");
}

#[test]
fn restart_between_exec_stages_skips_the_completed_stage() {
    const HOOK: &str = "after-aftercare-stage-first";

    let world = World::configured();
    configure(&world, COMMITTING_AGENT, None);
    world.arm_test_hook(HOOK);
    let invocations = world.root().join("flow-invocations");
    fs::write(
        world.root().join(".agents/sloop/flows/resume.yaml"),
        format!(
            "stages:\n  - {{ name: build, kind: build }}\n  - {{ name: first, kind: exec, cmd: [\"sh\", \"-c\", \"printf 1 >> {}\"] }}\n  - {{ name: second, kind: exec, cmd: [\"sh\", \"-c\", \"printf 2 >> {}\"] }}\n  - {{ name: merge, kind: merge }}\n",
            invocations.display(),
            invocations.display(),
        ),
    )
    .expect("write named recovery flow");
    world.commit_all("initial");
    let daemon_pid = world.start_daemon()["data"]["pid"].as_u64().unwrap() as u32;
    let ticket_path = world.write_ticket("resume-flow.md", "---\nflow: resume\n---\n# Work\n");
    let output = world.sloop(&["post", ticket_path.to_str().unwrap(), "--manual"]);
    assert!(output.status.success());
    let ticket = World::json_stdout(&output)["data"]["ticket"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(world.sloop(&["run", &ticket]).status.success());
    wait_until("the first stage is durably complete", || {
        world.test_hook_reached(HOOK)
            && fs::read_to_string(&invocations).is_ok_and(|value| value == "1")
    });

    world.kill_daemon(daemon_pid);
    fs::remove_file(world.root().join(".agents/sloop/flows/resume.yaml"))
        .expect("remove admitted flow before restart");
    world.start_daemon();
    wait_until("recovery completes the remaining flow", || {
        tickets(&world)["merged"] == 1
    });
    assert_eq!(fs::read_to_string(invocations).unwrap(), "12");
    assert_eq!(aftercare_stages(&world, "R1").len(), 4);
}

#[test]
fn cancel_kills_a_custom_exec_process_group_and_preserves_the_worktree() {
    let world = World::configured();
    configure(&world, COMMITTING_AGENT, None);
    let process_ids = world.root().join("custom-stage-processes");
    write_flow(
        &world,
        &format!(
            "stages:\n  - {{ name: build, kind: build }}\n  - {{ name: wait, kind: exec, cmd: [\"sh\", \"-c\", \"sleep 1000 & printf '%s %s' $$ $! > {}; wait\"] }}\n  - {{ name: merge, kind: merge }}\n",
            process_ids.display(),
        ),
    );
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "cancel-flow.md");
    wait_until("the custom stage is checkpointed", || {
        process_ids.is_file() && world.run_evidence("R1", "aftercare_process").is_some()
    });
    let process_ids = read_process_ids(process_ids);

    assert!(world.sloop(&["cancel", "R1"]).status.success());
    wait_until("the cancelled flow releases its ticket", || {
        let counts = tickets(&world);
        counts["ready"] == 1 && counts["claimed"] == 0
    });
    wait_for_processes_to_exit(process_ids);
    assert!(world.root().join(".worktrees/R1").is_dir());
}

#[test]
fn cancel_never_signals_a_recycled_aftercare_process_group() {
    let world = World::configured();
    configure(&world, COMMITTING_AGENT, None);
    let process_ids = world.root().join("recycled-stage-processes");
    write_flow(
        &world,
        &format!(
            "stages:\n  - {{ name: build, kind: build }}\n  - {{ name: wait, kind: exec, cmd: [\"sh\", \"-c\", \"sleep 1000 & printf '%s %s' $$ $! > {}; wait\"] }}\n  - {{ name: merge, kind: merge }}\n",
            process_ids.display(),
        ),
    );
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "recycled-aftercare-pgid.md");
    wait_until("the custom stage is checkpointed", || {
        process_ids.is_file() && world.run_evidence("R1", "aftercare_process").is_some()
    });
    let original_processes = read_process_ids(process_ids);
    let mut unrelated = spawn_unrelated_process_group();
    let unrelated_pid = unrelated.id();
    let connection = rusqlite::Connection::open(world.db_path()).unwrap();
    connection
        .execute(
            "UPDATE run_evidence SET data_json = ?1
             WHERE run_id = 'R1' AND kind = 'aftercare_process'",
            [serde_json::json!({
                "stage": "wait",
                "pid": unrelated_pid,
                "pid_start_time": 0,
                "process_group_id": unrelated_pid,
            })
            .to_string()],
        )
        .unwrap();

    assert!(world.sloop(&["cancel", "R1"]).status.success());
    assert!(
        unrelated.try_wait().unwrap().is_none(),
        "a recycled process group must not be signalled"
    );

    world.kill_process_group(original_processes[0]);
    wait_until("the cancelled flow releases its ticket", || {
        let counts = tickets(&world);
        counts["ready"] == 1 && counts["claimed"] == 0
    });
    unsafe {
        libc::kill(-(unrelated_pid as libc::pid_t), libc::SIGKILL);
    }
    unrelated.wait().unwrap();
}

#[test]
fn exec_stage_exit_kills_pipe_holding_stragglers() {
    let world = World::configured();
    configure(&world, COMMITTING_AGENT, None);
    let process_id = world.root().join("exec-straggler");
    write_flow(
        &world,
        &format!(
            "stages:\n  - {{ name: build, kind: build }}\n  - {{ name: check, kind: exec, cmd: [\"sh\", \"-c\", \"sleep 600 & echo $! > {}; exit 0\"] }}\n  - {{ name: merge, kind: merge }}\n",
            process_id.display(),
        ),
    );
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "exec-straggler.md");

    wait_until("the flow settles despite inherited pipes", || {
        tickets(&world)["merged"] == 1
    });
    wait_for_processes_to_exit(read_process_ids(process_id));
}

#[test]
fn recovery_does_not_signal_a_group_after_the_recorded_leader_exits() {
    const HOOK: &str = "after-aftercare-process-checkpoint-wait";

    let world = World::configured();
    configure(&world, COMMITTING_AGENT, None);
    world.arm_test_hook(HOOK);
    let process_ids = world.root().join("orphaned-exec-processes");
    write_flow(
        &world,
        &format!(
            "stages:\n  - {{ name: build, kind: build }}\n  - {{ name: wait, kind: exec, cmd: [\"sh\", \"-c\", \"sleep 1000 & printf '%s %s\\n' $$ $! >> {}; exit 0\"] }}\n  - {{ name: merge, kind: merge }}\n",
            process_ids.display(),
        ),
    );
    world.commit_all("initial");
    let daemon_pid = world.start_daemon()["data"]["pid"].as_u64().unwrap() as u32;
    post_and_run(&world, "orphaned-exec-child.md");
    wait_until("the exec checkpoint is durable", || {
        world.test_hook_reached(HOOK)
            && world.run_evidence("R1", "aftercare_process").is_some()
            && process_ids.is_file()
    });
    let first_processes = read_process_ids(process_ids.clone());

    world.kill_daemon(daemon_pid);
    wait_until("the exec leader exits while its child survives", || {
        !process_alive(first_processes[0]) && process_alive(first_processes[1])
    });
    world.release_test_hook(HOOK);
    world.start_daemon();
    wait_until(
        "recovery completes without signalling the stale group",
        || tickets(&world)["merged"] == 1,
    );
    assert!(
        process_alive(first_processes[1]),
        "the unverifiable leaderless group must not be signalled"
    );
    assert!(
        fs::read_to_string(world.daemon_log())
            .unwrap()
            .contains("stale_aftercare_group_not_signalled")
    );
    unsafe {
        libc::kill(first_processes[1] as libc::pid_t, libc::SIGKILL);
    }
    wait_until("the stale descendant is cleaned up by the test", || {
        !process_alive(first_processes[1])
    });
}

#[test]
fn recovery_preserves_partial_merge_state_and_fails_for_review() {
    let world = World::configured();
    let fixture = configure_blocking_merge(&world);
    let daemon_pid = world.start_daemon()["data"]["pid"].as_u64().unwrap() as u32;
    post_and_run(&world, "interrupted-merge.md");
    wait_until("the first merge reaches its hook", || {
        fixture.started.is_file() && world.run_evidence("R1", "aftercare_process").is_some()
    });

    world.kill_daemon(daemon_pid);
    let status_before = Command::new("git")
        .args(["status", "--porcelain=v1", "-z"])
        .current_dir(world.root())
        .output()
        .unwrap()
        .stdout;
    let index_before = Command::new("git")
        .args(["ls-files", "--stage", "-z"])
        .current_dir(world.root())
        .output()
        .unwrap()
        .stdout;
    let work_before = fs::read(world.root().join("work.txt")).ok();
    world.start_daemon();
    wait_until("the partial merge fails safely for review", || {
        tickets(&world)["needs_review"] == 1
    });
    let status_after = Command::new("git")
        .args(["status", "--porcelain=v1", "-z"])
        .current_dir(world.root())
        .output()
        .unwrap()
        .stdout;
    let index_after = Command::new("git")
        .args(["ls-files", "--stage", "-z"])
        .current_dir(world.root())
        .output()
        .unwrap()
        .stdout;
    assert_eq!(status_after, status_before);
    assert_eq!(index_after, index_before);
    assert_eq!(fs::read(world.root().join("work.txt")).ok(), work_before);
    assert_eq!(fs::read_to_string(fixture.invocations).unwrap(), "x");
}

#[test]
fn recovery_settles_a_completed_uncheckpointed_merge_idempotently() {
    const HOOK: &str = "after-successful-merge-process-exit";

    let world = World::configured();
    world.arm_test_hook(HOOK);
    configure(&world, COMMITTING_AGENT, None);
    world.commit_all("initial");
    let daemon_pid = world.start_daemon()["data"]["pid"].as_u64().unwrap() as u32;
    post_and_run(&world, "completed-before-checkpoint.md");
    wait_until("the merge completes before its stage checkpoint", || {
        world.test_hook_reached(HOOK)
            && default_branch_has(&world, "work.txt")
            && world.run_evidence("R1", "aftercare_process").is_some()
    });

    world.kill_daemon(daemon_pid);
    world.release_test_hook(HOOK);
    world.start_daemon();
    wait_until("recovery settles the completed merge", || {
        tickets(&world)["merged"] == 1
    });
    assert!(default_branch_has(&world, "work.txt"));
    assert_eq!(
        aftercare_stages(&world, "R1")
            .into_iter()
            .map(|(_, stage, state, _)| (stage, state))
            .collect::<Vec<_>>(),
        [
            ("build".into(), "passed".into()),
            ("merge".into(), "passed".into())
        ]
    );
}

#[test]
fn recovery_does_not_reapply_a_completed_merge_after_target_reset() {
    const HOOK: &str = "after-successful-merge-process-exit";

    let world = World::configured();
    world.arm_test_hook(HOOK);
    configure(&world, COMMITTING_AGENT, None);
    world.commit_all("initial");
    let initial_head = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(world.root())
        .output()
        .unwrap();
    let initial_head = String::from_utf8(initial_head.stdout).unwrap();
    let daemon_pid = world.start_daemon()["data"]["pid"].as_u64().unwrap() as u32;
    post_and_run(&world, "reset-after-merge.md");
    wait_until("the merge completes before target reset", || {
        world.test_hook_reached(HOOK)
            && world
                .run_evidence("R1", "aftercare_process")
                .is_some_and(|evidence| evidence["merge"]["completed_target"].is_string())
    });

    world.kill_daemon(daemon_pid);
    let reset = Command::new("git")
        .args(["reset", "--hard", initial_head.trim()])
        .current_dir(world.root())
        .status()
        .unwrap();
    assert!(reset.success());
    world.release_test_hook(HOOK);
    world.start_daemon();
    wait_until("the reset merge is left for review", || {
        tickets(&world)["needs_review"] == 1
    });
    let head = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(world.root())
        .output()
        .unwrap();
    assert_eq!(head.stdout, initial_head.as_bytes());
    assert!(!default_branch_has(&world, "work.txt"));
}

#[test]
fn recovery_does_not_reapply_a_completed_merge_after_target_moves() {
    const HOOK: &str = "after-successful-merge-process-exit";

    let world = World::configured();
    world.arm_test_hook(HOOK);
    configure(&world, COMMITTING_AGENT, None);
    world.commit_all("initial");
    let initial_head = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(world.root())
        .output()
        .unwrap();
    let initial_head = String::from_utf8(initial_head.stdout).unwrap();
    let daemon_pid = world.start_daemon()["data"]["pid"].as_u64().unwrap() as u32;
    post_and_run(&world, "move-after-merge.md");
    wait_until("the merge completes before target move", || {
        world.test_hook_reached(HOOK)
            && world
                .run_evidence("R1", "aftercare_process")
                .is_some_and(|evidence| evidence["merge"]["completed_target"].is_string())
    });

    world.kill_daemon(daemon_pid);
    assert!(
        Command::new("git")
            .args(["reset", "--hard", initial_head.trim()])
            .current_dir(world.root())
            .status()
            .unwrap()
            .success()
    );
    fs::write(world.root().join("operator.txt"), "operator\n").unwrap();
    assert!(
        Command::new("git")
            .args(["add", "operator.txt"])
            .current_dir(world.root())
            .status()
            .unwrap()
            .success()
    );
    assert!(
        Command::new("git")
            .args([
                "-c",
                "user.name=operator",
                "-c",
                "user.email=operator@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "operator moved target",
            ])
            .current_dir(world.root())
            .status()
            .unwrap()
            .success()
    );
    let moved_head = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(world.root())
        .output()
        .unwrap()
        .stdout;
    world.release_test_hook(HOOK);
    world.start_daemon();
    wait_until("the moved target is left for review", || {
        tickets(&world)["needs_review"] == 1
    });
    assert_eq!(
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(world.root())
            .output()
            .unwrap()
            .stdout,
        moved_head
    );
    assert_eq!(
        fs::read_to_string(world.root().join("operator.txt")).unwrap(),
        "operator\n"
    );
    assert!(!default_branch_has(&world, "work.txt"));
}

#[test]
fn recovery_does_not_merge_an_advanced_run_branch() {
    const HOOK: &str = "after-aftercare-process-checkpoint-merge";

    let world = World::configured();
    world.arm_test_hook(HOOK);
    configure(&world, COMMITTING_AGENT, None);
    world.commit_all("initial");
    let initial_head = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(world.root())
        .output()
        .unwrap()
        .stdout;
    let daemon_pid = world.start_daemon()["data"]["pid"].as_u64().unwrap() as u32;
    post_and_run(&world, "advanced-run-branch.md");
    wait_until("the blocked merge has its baseline checkpoint", || {
        world.test_hook_reached(HOOK)
            && world
                .run_evidence("R1", "aftercare_process")
                .is_some_and(|evidence| evidence["merge"]["branch_tip"].is_string())
    });

    world.kill_daemon(daemon_pid);
    let worktree = world.root().join(".worktrees/R1");
    fs::write(worktree.join("advanced.txt"), "advanced\n").unwrap();
    assert!(
        Command::new("git")
            .args(["add", "advanced.txt"])
            .current_dir(&worktree)
            .status()
            .unwrap()
            .success()
    );
    assert!(
        Command::new("git")
            .args([
                "-c",
                "user.name=operator",
                "-c",
                "user.email=operator@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "advanced run branch",
            ])
            .current_dir(&worktree)
            .status()
            .unwrap()
            .success()
    );
    world.release_test_hook(HOOK);
    world.start_daemon();
    wait_until("the advanced run branch is left for review", || {
        tickets(&world)["needs_review"] == 1
    });
    assert_eq!(
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(world.root())
            .output()
            .unwrap()
            .stdout,
        initial_head
    );
    assert!(!default_branch_has(&world, "work.txt"));
    assert!(!default_branch_has(&world, "advanced.txt"));
}

#[test]
fn recovery_preserves_conflicted_and_unrelated_operator_edits_after_merge_exit() {
    const HOOK: &str = "after-failed-merge-process-exit";

    let world = World::configured();
    world.arm_test_hook(HOOK);
    configure(
        &world,
        concat!(
            "echo agent-version > work.txt\n",
            "git add work.txt\n",
            "git -c user.name=agent -c user.email=agent@example.invalid commit --quiet -m agent\n",
            "root=\"$(dirname \"$0\")\"\n",
            "echo main-version > \"$root/work.txt\"\n",
            "git -C \"$root\" add work.txt\n",
            "git -C \"$root\" -c user.name=operator -c user.email=operator@example.invalid commit --quiet -m main\n",
        ),
        None,
    );
    world.commit_all("initial");
    let daemon_pid = world.start_daemon()["data"]["pid"].as_u64().unwrap() as u32;
    post_and_run(&world, "exited-conflict.md");
    wait_until("the failed merge exits with owned conflict state", || {
        world.test_hook_reached(HOOK)
            && world.run_evidence("R1", "aftercare_process").is_some()
            && world.root().join(".git/MERGE_HEAD").is_file()
    });
    let merge_pid = world.run_evidence("R1", "aftercare_process").unwrap()["pid"]
        .as_u64()
        .unwrap() as u32;
    assert!(!process_alive(merge_pid));

    let conflict_edit = b"operator conflict edit\n";
    let unrelated_edit = b"operator unrelated edit\n";
    fs::write(world.root().join("work.txt"), conflict_edit).unwrap();
    fs::write(world.root().join("operator.txt"), unrelated_edit).unwrap();
    let added = Command::new("git")
        .args(["add", "operator.txt"])
        .current_dir(world.root())
        .status()
        .unwrap();
    assert!(added.success());
    let status_before = Command::new("git")
        .args(["status", "--porcelain=v1", "-z"])
        .current_dir(world.root())
        .output()
        .unwrap()
        .stdout;
    let index_before = Command::new("git")
        .args(["ls-files", "--stage", "-z"])
        .current_dir(world.root())
        .output()
        .unwrap()
        .stdout;
    let merge_head_before = fs::read(world.root().join(".git/MERGE_HEAD")).unwrap();
    let merge_message_before = fs::read(world.root().join(".git/MERGE_MSG")).unwrap();

    world.kill_daemon(daemon_pid);
    world.release_test_hook(HOOK);
    world.start_daemon();
    wait_until("recovery leaves the edited conflict for review", || {
        tickets(&world)["needs_review"] == 1
    });
    assert_eq!(
        fs::read(world.root().join("work.txt")).unwrap(),
        conflict_edit
    );
    assert_eq!(
        fs::read(world.root().join("operator.txt")).unwrap(),
        unrelated_edit
    );
    assert_eq!(
        fs::read(world.root().join(".git/MERGE_HEAD")).unwrap(),
        merge_head_before
    );
    assert_eq!(
        fs::read(world.root().join(".git/MERGE_MSG")).unwrap(),
        merge_message_before
    );
    assert_eq!(
        Command::new("git")
            .args(["status", "--porcelain=v1", "-z"])
            .current_dir(world.root())
            .output()
            .unwrap()
            .stdout,
        status_before
    );
    assert_eq!(
        Command::new("git")
            .args(["ls-files", "--stage", "-z"])
            .current_dir(world.root())
            .output()
            .unwrap()
            .stdout,
        index_before
    );
}

#[test]
fn recovery_preserves_operator_changes_made_during_an_interrupted_merge() {
    let world = World::configured();
    let fixture = configure_blocking_merge(&world);
    let daemon_pid = world.start_daemon()["data"]["pid"].as_u64().unwrap() as u32;
    post_and_run(&world, "operator-changed-merge.md");
    wait_until("the merge reaches its hook", || {
        fixture.started.is_file() && world.run_evidence("R1", "aftercare_process").is_some()
    });
    let merge_pid = world.run_evidence("R1", "aftercare_process").unwrap()["pid"]
        .as_u64()
        .unwrap() as u32;

    world.kill_daemon(daemon_pid);
    fs::write(world.root().join("operator.txt"), "operator\n").unwrap();
    let added = Command::new("git")
        .args(["add", "operator.txt"])
        .current_dir(world.root())
        .status()
        .unwrap();
    assert!(added.success());
    let staged_before = Command::new("git")
        .args(["diff", "--cached", "--name-only"])
        .current_dir(world.root())
        .output()
        .unwrap()
        .stdout;
    world.start_daemon();
    wait_until("recovery leaves the modified merge for review", || {
        !process_alive(merge_pid) && tickets(&world)["needs_review"] == 1
    });

    let staged = Command::new("git")
        .args(["diff", "--cached", "--name-only"])
        .current_dir(world.root())
        .output()
        .unwrap();
    assert!(
        String::from_utf8_lossy(&staged.stdout)
            .lines()
            .any(|path| path == "operator.txt"),
        "operator.txt must remain staged: {}",
        String::from_utf8_lossy(&staged.stdout)
    );
    assert_eq!(staged.stdout, staged_before);
    assert_eq!(fs::read_to_string(fixture.invocations).unwrap(), "x");
}

#[test]
fn recovery_preserves_unrelated_merge_state() {
    let world = World::configured();
    let fixture = configure_blocking_merge(&world);
    let daemon_pid = world.start_daemon()["data"]["pid"].as_u64().unwrap() as u32;
    post_and_run(&world, "unrelated-merge.md");
    wait_until("the merge reaches its hook", || {
        fixture.started.is_file() && world.run_evidence("R1", "aftercare_process").is_some()
    });
    let merge_pid = world.run_evidence("R1", "aftercare_process").unwrap()["pid"]
        .as_u64()
        .unwrap() as u32;
    world.kill_daemon(daemon_pid);
    let unrelated_head = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(world.root())
        .output()
        .unwrap();
    let unrelated_head = String::from_utf8(unrelated_head.stdout).unwrap();
    fs::write(world.root().join(".git/MERGE_HEAD"), &unrelated_head).unwrap();
    let status_before = Command::new("git")
        .args(["status", "--porcelain=v1", "-z"])
        .current_dir(world.root())
        .output()
        .unwrap()
        .stdout;
    let index_before = Command::new("git")
        .args(["ls-files", "--stage", "-z"])
        .current_dir(world.root())
        .output()
        .unwrap()
        .stdout;

    world.start_daemon();
    wait_until("recovery refuses the unrelated merge", || {
        !process_alive(merge_pid) && tickets(&world)["needs_review"] == 1
    });
    assert_eq!(
        fs::read_to_string(world.root().join(".git/MERGE_HEAD")).unwrap(),
        unrelated_head
    );
    assert_eq!(
        Command::new("git")
            .args(["status", "--porcelain=v1", "-z"])
            .current_dir(world.root())
            .output()
            .unwrap()
            .stdout,
        status_before
    );
    assert_eq!(
        Command::new("git")
            .args(["ls-files", "--stage", "-z"])
            .current_dir(world.root())
            .output()
            .unwrap()
            .stdout,
        index_before
    );
    assert_eq!(fs::read_to_string(fixture.invocations).unwrap(), "x");
}

#[test]
fn recovery_never_removes_an_unowned_index_lock() {
    let world = World::configured();
    let fixture = configure_blocking_merge(&world);
    let daemon_pid = world.start_daemon()["data"]["pid"].as_u64().unwrap() as u32;
    post_and_run(&world, "unowned-index-lock.md");
    wait_until("the merge reaches its hook", || {
        fixture.started.is_file() && world.run_evidence("R1", "aftercare_process").is_some()
    });
    let merge_pid = world.run_evidence("R1", "aftercare_process").unwrap()["pid"]
        .as_u64()
        .unwrap() as u32;
    world.kill_daemon(daemon_pid);
    let index_lock = world.root().join(".git/index.lock");
    fs::write(&index_lock, "operator lock\n").unwrap();
    let status_before = Command::new("git")
        .args(["status", "--porcelain=v1", "-z"])
        .current_dir(world.root())
        .output()
        .unwrap()
        .stdout;
    let index_before = fs::read(world.root().join(".git/index")).unwrap();

    world.start_daemon();
    wait_until("recovery refuses the unowned index lock", || {
        !process_alive(merge_pid) && tickets(&world)["needs_review"] == 1
    });
    assert_eq!(fs::read_to_string(index_lock).unwrap(), "operator lock\n");
    assert_eq!(
        fs::read(world.root().join(".git/index")).unwrap(),
        index_before
    );
    assert_eq!(
        Command::new("git")
            .args(["status", "--porcelain=v1", "-z"])
            .current_dir(world.root())
            .output()
            .unwrap()
            .stdout,
        status_before
    );
    assert_eq!(fs::read_to_string(fixture.invocations).unwrap(), "x");
}

#[test]
fn cancellation_at_arbitrary_exec_startup_kills_the_stage_group() {
    const HOOK: &str = "before-aftercare-process-checkpoint-wait";

    let world = World::configured();
    configure(&world, COMMITTING_AGENT, None);
    world.arm_test_hook(HOOK);
    let process_ids = world.root().join("racing-exec-processes");
    write_flow(
        &world,
        &format!(
            "stages:\n  - {{ name: build, kind: build }}\n  - {{ name: wait, kind: exec, cmd: [\"sh\", \"-c\", \"sleep 1000 & printf '%s %s' $$ $! > {}; wait\"] }}\n  - {{ name: merge, kind: merge }}\n",
            process_ids.display(),
        ),
    );
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "cancel-racing-exec.md");
    wait_until("the arbitrary exec reaches its startup gate", || {
        world.test_hook_reached(HOOK) && process_ids.is_file()
    });
    assert!(world.run_evidence("R1", "aftercare_process").is_none());

    assert!(world.sloop(&["cancel", "R1"]).status.success());
    let process_ids = read_process_ids(process_ids);
    world.release_test_hook(HOOK);
    wait_until("the startup-racing exec is cancelled", || {
        let counts = tickets(&world);
        counts["ready"] == 1 && counts["claimed"] == 0
    });
    wait_for_processes_to_exit(process_ids);
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
        process_ids.is_file() && world.run_evidence("R1", "aftercare_process").is_some()
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
    assert!(world.run_evidence("R1", "aftercare_process").is_none());

    let cancelled = world.sloop(&["cancel", "R1"]);
    assert!(cancelled.status.success());
    assert!(world.run_evidence("R1", "cancel_requested").is_some());
    assert!(world.run_evidence("R1", "aftercare_process").is_none());
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
    assert!(world.run_evidence("R1", "aftercare_process").is_none());
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
fn a_conflicting_merge_parks_the_ticket_and_preserves_the_conflict() {
    let world = World::configured();
    // Same interleaving, but both sides edit the same file: the merge state is
    // evidence for the human and must not be automatically aborted.
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
    assert!(world.root().join(".git/MERGE_HEAD").is_file());
    let status = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=no"])
        .current_dir(world.root())
        .output()
        .unwrap();
    assert!(
        String::from_utf8_lossy(&status.stdout).contains("AA work.txt"),
        "conflict must remain visible: {}",
        String::from_utf8_lossy(&status.stdout)
    );
    let conflict = fs::read_to_string(world.root().join("work.txt")).unwrap();
    assert!(conflict.contains("agent-version"));
    assert!(conflict.contains("main-version"));
}

#[test]
fn exit_zero_without_commits_runs_tests_and_completes_the_ticket() {
    let world = World::configured();
    configure(&world, "exit 0\n", Some("echo tested > tested.txt"));
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "empty.md");

    wait_until("the ticket reaches merged", || {
        tickets(&world)["merged"] == 1
    });
    assert!(world.root().join(".worktrees/R1/tested.txt").exists());
}

#[test]
fn rewriting_the_default_branch_does_not_invent_run_commits() {
    let world = World::configured();
    configure(
        &world,
        concat!(
            "root=\"$(dirname \"$0\")\"\n",
            "echo rewritten > \"$root/rewritten.txt\"\n",
            "git -C \"$root\" add rewritten.txt\n",
            "git -C \"$root\" -c user.name=op -c user.email=op@example.invalid commit --quiet --amend --no-edit\n",
            "exit 0\n",
        ),
        None,
    );
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_and_run(&world, "rewrite.md");

    wait_until("the no-op ticket reaches merged", || {
        tickets(&world)["merged"] == 1
    });
    assert!(default_branch_has(&world, "rewritten.txt"));

    let shown = world.sloop(&["show", "default"]);
    assert!(shown.status.success());
    let response = World::json_stdout(&shown);
    let activity = response["data"]["value"]["tickets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["id"] == ticket)
        .expect("ticket activity");
    assert_eq!(activity["commits"], serde_json::json!([]));
}

#[test]
fn a_nonzero_exit_fails_even_when_the_agent_committed() {
    let world = World::configured();
    configure(
        &world,
        &COMMITTING_AGENT.replace("exit 0", "exit 1"),
        Some("echo should-not-run > tested.txt"),
    );
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "failed.md");

    wait_until("the ticket reaches failed", || {
        tickets(&world)["failed"] == 1
    });
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

    wait_until("the run finishes", || tickets(&world)["merged"] == 1);

    let output = world.sloop(&["cancel", "R1"]);
    let response = json_stderr(&output);
    assert_eq!(response["ok"], false);
    assert_eq!(response["error"]["code"], "conflict");

    let missing = world.sloop(&["cancel", "R99"]);
    let response = json_stderr(&missing);
    assert_eq!(response["error"]["code"], "not_found");
}

#[test]
fn straggler_group_members_are_killed_and_the_run_still_settles() {
    let world = World::configured();
    let straggler_pid = world.root().join("straggler-pid");
    let agent = format!(
        "sleep 600 &\necho $! > {pid_file}\n{committing}",
        pid_file = straggler_pid.display(),
        committing = COMMITTING_AGENT,
    );
    configure(&world, &agent, Some("echo tests passed"));
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "straggler.md");

    // Without the straggler kill, the background sleep keeps the agent's
    // stdout pipe open and the run never leaves `running`.
    wait_until("the ticket reaches merged", || {
        tickets(&world)["merged"] == 1
    });
    wait_for_processes_to_exit(read_process_ids(straggler_pid));
    wait_until("no agents remain active", || {
        let output = world.sloop(&["status"]);
        World::json_stdout(&output)["data"]["gate"]["active_agents"] == 0
    });
}

#[test]
fn authentication_and_configuration_rejections_fail_without_aftercare() {
    let cases = [
        (
            "auth.md",
            "printf '%s\n' 'status 401 Missing bearer or basic authentication in header' >&2\nexit 1\n",
            "authentication_required",
            "codex.authentication.missing-header",
        ),
        (
            "config.md",
            "printf '%s\n' 'status 400 model is not supported when using Codex with a ChatGPT account' >&2\nexit 1\n",
            "invalid_configuration",
            "codex.configuration.unsupported-chatgpt-model",
        ),
    ];

    for (ticket_name, body, class, rule_id) in cases {
        let world = World::configured();
        let aftercare_marker = world.root().join("aftercare-ran");
        configure(
            &world,
            body,
            Some(&format!("touch {}", aftercare_marker.display())),
        );
        world.commit_all("initial");
        world.start_daemon();
        let ticket = post_and_run(&world, ticket_name);

        wait_until("the rejected run fails", || tickets(&world)["failed"] == 1);
        assert!(!aftercare_marker.exists());
        let evidence = world
            .run_evidence("R1", "vendor_error_classified")
            .expect("vendor classification evidence");
        assert_eq!(evidence["class"], class);
        assert_eq!(evidence["vendor"], "codex");
        assert_eq!(evidence["rule_id"], rule_id);
        let encoded = evidence.to_string();
        assert!(
            !encoded.contains("Missing bearer"),
            "unsafe evidence: {encoded}"
        );
        assert!(
            !encoded.contains("ChatGPT account"),
            "unsafe evidence: {encoded}"
        );

        let shown = world.sloop(&["show", &ticket]);
        assert!(shown.status.success());
        assert_eq!(
            World::json_stdout(&shown)["data"]["value"]["classification"]["class"],
            class
        );
        assert!(world.sloop(&["retry", &ticket]).status.success());
        let shown = world.sloop(&["show", &ticket]);
        assert!(World::json_stdout(&shown)["data"]["value"]["classification"].is_null());
    }
}

#[test]
fn retryable_and_unknown_rejections_release_under_a_target_cooldown() {
    let cases = [
        (
            "rate.md",
            "printf \"You've hit your limit\\n\" >&2\nexit 1\n",
            "rate_limited",
        ),
        (
            "unknown.md",
            "printf '%s\n' 'Unexpected server error. Check server logs for details.' >&2\nexit 1\n",
            "unknown_rejection",
        ),
    ];

    for (ticket_name, body, class) in cases {
        let world = World::configured();
        configure(&world, body, None);
        world.commit_all("initial");
        world.start_daemon();
        post_and_run(&world, ticket_name);

        wait_until("the rejected ticket is released", || {
            let counts = tickets(&world);
            counts["ready"] == 1 && counts["claimed"] == 0
        });
        let waited = world.sloop(&["wait", "R1", "--timeout", "5"]);
        assert!(!waited.status.success());
        let response = World::json_stdout_or_stderr(&waited);
        assert_eq!(response["data"]["state"], "rate_limited");
        assert_eq!(response["data"]["classification"]["class"], class);

        let status = world.sloop(&["status"]);
        assert_eq!(
            World::json_stdout(&status)["data"]["gate"]["cooldowns"][0]["target"],
            "fake"
        );

        let listed = world.sloop(&["list"]);
        let reason = World::json_stdout(&listed)["data"]["tickets"][0]["reason"]
            .as_str()
            .unwrap()
            .to_owned();
        assert!(reason.contains("cooling down"), "{reason}");
    }
}

#[test]
fn cooldown_and_automatic_retry_survive_a_daemon_restart() {
    let world = World::configured();
    let first_run = world.root().join("first-run-finished");
    let body = format!(
        "if [ ! -e {marker} ]; then touch {marker}; printf \"You've hit your limit\\n\" >&2; exit 1; fi\n\
         echo recovered > work.txt\n\
         git add work.txt\n\
         git -c user.name=agent -c user.email=agent@example.invalid commit --quiet -m recovered\n",
        marker = first_run.display(),
    );
    configure(&world, &body, None);
    world.commit_all("initial");
    let daemon = world.start_daemon();
    let ticket = post_and_run(&world, "restart-cooldown.md");
    wait_until("the first run enters cooldown", || {
        tickets(&world)["ready"] == 1
            && world
                .run_evidence("R1", "vendor_error_classified")
                .is_some()
    });

    let pid = daemon["data"]["pid"].as_u64().unwrap() as u32;
    world.kill_daemon(pid);
    world.start_daemon();
    let connection = rusqlite::Connection::open(world.db_path()).unwrap();
    let run_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM runs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(run_count, 1, "the cooldown must gate restart dispatch");
    drop(connection);

    let listed = world.sloop(&["list"]);
    let rows = World::json_stdout(&listed);
    let row = rows["data"]["tickets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == ticket)
        .unwrap();
    assert!(row["reason"].as_str().unwrap().contains("cooling down"));

    world.tick(Duration::from_secs(301));
    wait_until("the released activation retries after cooldown", || {
        tickets(&world)["merged"] == 1
    });
    assert!(world.root().join("work.txt").is_file());
}

#[test]
fn a_recognized_rejection_with_commits_never_tests_or_merges_the_work() {
    let world = World::configured();
    let aftercare_marker = world.root().join("rejected-aftercare");
    configure(
        &world,
        concat!(
            "echo preserved > rejected-work.txt\n",
            "git add rejected-work.txt\n",
            "git -c user.name=agent -c user.email=agent@example.invalid commit --quiet -m preserved\n",
            "printf '%s\n' 'Unexpected server error. Check server logs for details.' >&2\n",
            "exit 0\n",
        ),
        Some(&format!("touch {}", aftercare_marker.display())),
    );
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "preserve-rejected.md");

    wait_until("the rejected ticket is released under a cooldown", || {
        tickets(&world)["ready"] == 1
    });
    assert!(!aftercare_marker.exists());
    assert!(!world.root().join("rejected-work.txt").exists());
    assert_eq!(
        world.run_evidence("R1", "vendor_error_classified").unwrap()["class"],
        "unknown_rejection"
    );
    assert_eq!(
        aftercare_stages(&world, "R1")
            .into_iter()
            .map(|(_, stage, state, _)| (stage, state))
            .collect::<Vec<_>>(),
        [("build".into(), "failed".into())]
    );
    let waited = world.sloop_plain(&["wait", "R1", "--timeout", "5"]);
    let text = String::from_utf8_lossy(&waited.stderr);
    assert!(text.contains("OpenCode rejected the request"), "{text}");
}

#[test]
fn a_target_cooldown_does_not_block_other_agent_targets() {
    let world = World::configured();
    fs::create_dir_all(world.root().join(".agents/sloop/flows")).unwrap();
    write_flow(
        &world,
        "stages:\n  - { name: build, kind: build }\n  - { name: merge, kind: merge }\n",
    );
    let limited = world.root().join("limited.sh");
    let healthy = world.root().join("healthy.sh");
    fs::write(
        &limited,
        "#!/bin/sh\nprintf \"You've hit your limit\\n\" >&2\nexit 1\n",
    )
    .unwrap();
    fs::write(
        &healthy,
        concat!(
            "#!/bin/sh\n",
            "echo healthy > healthy.txt\n",
            "git add healthy.txt\n",
            "git -c user.name=agent -c user.email=agent@example.invalid commit --quiet -m healthy\n",
        ),
    )
    .unwrap();
    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        format!(
            "version: 1\nscheduler:\n  max_parallel_tasks: 1\nagent:\n  default_target: limited\n  targets:\n    limited:\n      cmd: [\"sh\", \"{}\", \"{{prompt}}\"]\n    healthy:\n      cmd: [\"sh\", \"{}\", \"{{prompt}}\"]\n",
            limited.display(),
            healthy.display(),
        ),
    )
    .unwrap();
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "limited.md");
    wait_until("the limited target cools down", || {
        tickets(&world)["ready"] == 1
    });

    let healthy_ticket = world.write_ticket(
        "healthy.md",
        "---\ntarget: healthy\n---\n# Healthy target\n",
    );
    let posted = world.sloop(&["post", healthy_ticket.to_str().unwrap(), "--manual"]);
    assert!(posted.status.success());
    let id = World::json_stdout(&posted)["data"]["ticket"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(world.sloop(&["run", &id]).status.success());

    wait_until("the healthy target runs during the other cooldown", || {
        tickets(&world)["merged"] == 1
    });
    assert!(world.root().join("healthy.txt").is_file());
}

#[test]
fn cancellation_after_rejection_checkpoint_wins_without_a_cooldown() {
    const HOOK: &str = "after-agent-exit-checkpoint";

    let world = World::configured();
    world.arm_test_hook(HOOK);
    configure(
        &world,
        "printf \"You've hit your limit\\n\" >&2\nexit 1\n",
        None,
    );
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "cancel-rejection.md");
    wait_until("the rejection checkpoint is durable", || {
        world.test_hook_reached(HOOK)
            && world
                .run_evidence("R1", "vendor_error_classified")
                .is_some()
    });

    assert!(world.sloop(&["cancel", "R1"]).status.success());
    world.release_test_hook(HOOK);
    wait_until("cancellation settles", || tickets(&world)["ready"] == 1);
    let waited = world.sloop(&["wait", "R1", "--timeout", "5"]);
    assert_eq!(
        World::json_stdout_or_stderr(&waited)["data"]["state"],
        "cancelled"
    );
    let connection = rusqlite::Connection::open(world.db_path()).unwrap();
    let cooldowns: i64 = connection
        .query_row("SELECT COUNT(*) FROM cooldowns", [], |row| row.get(0))
        .unwrap();
    assert_eq!(cooldowns, 0);
}

#[test]
fn recovery_preserves_the_checkpointed_cooldown_deadline() {
    const HOOK: &str = "after-agent-exit-checkpoint";

    let world = World::configured();
    world.arm_test_hook(HOOK);
    configure(
        &world,
        "printf \"You've hit your limit\\n\" >&2\nexit 1\n",
        None,
    );
    world.commit_all("initial");
    let daemon = world.start_daemon();
    post_and_run(&world, "recover-deadline.md");
    wait_until("the rejection deadline is checkpointed", || {
        world.test_hook_reached(HOOK)
            && world
                .run_evidence("R1", "vendor_error_classified")
                .is_some()
    });
    let deadline =
        world.run_evidence("R1", "vendor_error_classified").unwrap()["cooldown_until_ms"]
            .as_i64()
            .unwrap();

    world.kill_daemon(daemon["data"]["pid"].as_u64().unwrap() as u32);
    world.tick(Duration::from_secs(240));
    world.start_daemon();
    wait_until("recovery settles the rejected run", || {
        tickets(&world)["ready"] == 1
    });

    let connection = rusqlite::Connection::open(world.db_path()).unwrap();
    let persisted: i64 = connection
        .query_row("SELECT until_ms FROM cooldowns", [], |row| row.get(0))
        .unwrap();
    assert_eq!(persisted, deadline);
    let run_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM runs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        run_count, 1,
        "recovery must not dispatch before the deadline"
    );
}
