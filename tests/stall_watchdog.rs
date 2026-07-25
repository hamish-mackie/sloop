mod support;

use std::fs;
use std::time::Duration;

use support::{FakeAgent, World, process_alive, wait_until, wait_until_slow};

fn post_and_run(world: &World, name: &str) -> String {
    let ticket = world.write_ticket(name, "# Stall watchdog\n");
    let posted = world.sloop(&["post", ticket.to_str().unwrap(), "--manual"]);
    assert!(posted.status.success());
    let ticket_id = World::json_stdout(&posted)["data"]["ticket"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(world.sloop(&["run", &ticket_id]).status.success());
    ticket_id
}

fn configure_process_tree_agent(world: &World) {
    let flow_directory = world.root().join(".agents/sloop/flows");
    fs::create_dir_all(&flow_directory).unwrap();
    fs::write(
        flow_directory.join("default.yaml"),
        "stages:\n  - { name: build, action: agent, result_check: { exec: ['true'] } }\n  - { name: merge, action: { builtin: merge } }\n",
    )
    .unwrap();
    let script = world.root().join("fake-agent.sh");
    fs::write(
        &script,
        "#!/bin/sh\nset -eu\nsleep 1000 &\necho $! > grandchild.pid\necho initial\nwhile true; do sleep 1; done\n",
    )
    .unwrap();
    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        format!(
            "version: 1\nscheduler:\n  stall_report_after: 10s\n  stall_after: 30s\nagent:\n  default_target: fake\n  targets:\n    fake:\n      cmd: [\"sh\", {}, \"{{prompt}}\"]\n",
            serde_json::to_string(&script.to_string_lossy()).unwrap(),
        ),
    )
    .unwrap();
}

#[test]
fn silent_agent_is_failed_with_stall_evidence_and_its_process_group_is_killed() {
    let world = World::configured();
    configure_process_tree_agent(&world);
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "silent-agent.md");

    let pid_file = world.run_worktree(1).join("grandchild.pid");
    wait_until("the agent records its grandchild", || pid_file.is_file());
    let run_id = world.run_id(1);
    let grandchild = fs::read_to_string(&pid_file)
        .unwrap()
        .trim()
        .parse::<u32>()
        .unwrap();
    assert!(process_alive(grandchild));

    world.tick(Duration::from_secs(30));
    wait_until_slow("the watchdog settles the run", || {
        world.run_state(&run_id) == "failed"
    });
    wait_until("the grandchild exits with the process group", || {
        !process_alive(grandchild)
    });

    let evidence = world
        .run_evidence(&run_id, "output_stall")
        .expect("stall evidence");
    assert_eq!(evidence["stage"], "build");
    assert_eq!(evidence["threshold_ms"], 30_000);
    assert!(evidence["last_output_at_ms"].is_i64());
    let shown = world.sloop_plain(&["show", &world.run_alias(1)]);
    assert!(shown.status.success());
    assert!(String::from_utf8_lossy(&shown.stdout).contains("failed (stalled: no output for 30s)"));
}

#[test]
fn resumed_output_moves_the_kill_deadline() {
    let world = World::configured();
    world.configure_fake_agent_with_stall_thresholds(
        FakeAgent::new()
            .output("initial\n")
            .block_until_released("first-silence")
            .output("resumed\n")
            .block_until_released("second-silence")
            .exit(0),
        "10s",
        "30s",
    );
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "resumed-output.md");
    wait_until("the first silence starts", || {
        world.fake_agent_reached("first-silence")
    });
    let run_id = world.run_id(1);

    world.tick(Duration::from_secs(20));
    world.release("first-silence");
    wait_until("the resumed output is written", || {
        world.fake_agent_reached("second-silence")
    });
    world.tick(Duration::from_secs(20));
    assert_eq!(world.run_state(&run_id), "running");
    assert!(world.run_evidence(&run_id, "output_stall").is_none());

    world.release("second-silence");
    wait_until_slow("the run merges", || world.run_state(&run_id) == "merged");
}

#[test]
fn restart_rederives_an_overdue_stall_and_fails_the_run() {
    let world = World::configured();
    world.configure_fake_agent_with_stall_thresholds(
        FakeAgent::new()
            .output("before restart\n")
            .block_until_released("restart-silence"),
        "10s",
        "30s",
    );
    world.commit_all("initial");
    let daemon = world.start_daemon();
    post_and_run(&world, "restart-stall.md");
    wait_until("the agent blocks", || {
        world.fake_agent_reached("restart-silence")
    });
    let run_id = world.run_id(1);

    world.tick(Duration::from_secs(20));
    world.kill_daemon(daemon["data"]["pid"].as_u64().unwrap() as u32);
    world.tick(Duration::from_secs(10));
    world.start_daemon();

    wait_until_slow("restart settles the stalled run", || {
        world.run_state(&run_id) == "failed"
    });
    assert!(world.run_evidence(&run_id, "output_stall").is_some());
}

#[test]
fn explicit_cancellation_remains_cancelled() {
    let world = World::configured();
    world.configure_fake_agent_with_stall_thresholds(
        FakeAgent::new().block_until_released("cancelled-agent"),
        "10s",
        "30s",
    );
    world.commit_all("initial");
    world.start_daemon();
    post_and_run(&world, "cancelled-agent.md");
    wait_until("the agent blocks", || {
        world.fake_agent_reached("cancelled-agent")
    });
    let run_id = world.run_id(1);
    assert!(world.sloop(&["cancel", &run_id]).status.success());

    wait_until_slow("the cancelled run settles", || {
        world.run_state(&run_id) == "cancelled"
    });
    assert!(world.run_evidence(&run_id, "cancel_requested").is_some());
    assert!(world.run_evidence(&run_id, "output_stall").is_none());
}
