mod support;

use std::fs;
use std::os::unix::fs::PermissionsExt;

use serde_json::Value;
use support::{FakeAgent, World, process_alive, wait_until, wait_until_slow};

fn status(world: &World) -> Value {
    let output = world.sloop(&["status"]);
    assert!(
        output.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    World::json_stdout(&output)["data"].clone()
}

fn stop_daemon(world: &World, pid: u32) {
    let output = world.sloop(&["stop"]);
    assert!(
        output.status.success(),
        "stop failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    wait_until("the daemon stops", || !process_alive(pid));
}

fn log_event_count(world: &World, event: &str) -> usize {
    fs::read_to_string(world.daemon_log())
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|record| record["event"] == event)
        .count()
}

fn activity_event_count(world: &World, event: &str) -> i64 {
    rusqlite::Connection::open(world.db_path())
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM events WHERE kind = ?1",
            [event],
            |row| row.get(0),
        )
        .unwrap()
}

#[test]
fn daemon_start_is_idempotent_and_uses_one_process() {
    let world = World::configured();

    let first = world.start_daemon();
    let second = world.start_daemon();

    assert_eq!(first["data"]["started"], true);
    assert_eq!(second["data"]["started"], false);
    assert_eq!(first["data"]["pid"], second["data"]["pid"]);
    assert_eq!(first["data"]["version"], env!("CARGO_PKG_VERSION"));
}

#[test]
fn foreground_start_cannot_bypass_the_single_daemon_lock() {
    let world = World::configured();
    let daemon = world.start_daemon();

    let competing = world.sloop(&["daemon", "--foreground"]);
    assert!(competing.status.success());
    assert!(competing.stdout.is_empty());
    assert!(competing.stderr.is_empty());

    let status = world.sloop(&["status"]);
    assert!(status.status.success());
    assert_eq!(
        World::json_stdout(&status)["data"]["daemon"]["pid"],
        daemon["data"]["pid"]
    );
}

#[test]
fn alternate_runtime_roots_cannot_bypass_the_state_database_lock() {
    let world = World::configured();
    let daemon = world.start_daemon();
    let alternate_runtime = tempfile::tempdir().expect("create alternate runtime");

    let competing = world.sloop_with_runtime(&["status"], alternate_runtime.path());

    assert!(competing.status.success());
    assert_eq!(
        World::json_stdout(&competing)["data"]["daemon"]["pid"],
        daemon["data"]["pid"]
    );
    let foreground =
        world.sloop_with_runtime(&["daemon", "--foreground"], alternate_runtime.path());
    assert!(foreground.status.success());
    let status = world.sloop(&["status"]);
    assert!(status.status.success());
    assert_eq!(
        World::json_stdout(&status)["data"]["daemon"]["pid"],
        daemon["data"]["pid"]
    );
}

#[test]
fn status_uses_the_real_socket_and_dispatcher() {
    let world = World::configured();
    let daemon = world.start_daemon();
    let nested = world.root().join("src/nested");
    fs::create_dir_all(&nested).expect("create nested directory");

    let output = world.sloop_in(&nested, &["status"]);

    assert!(output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .starts_with("note: 'sloop status' is now 'sloop show'")
    );
    let response = World::json_stdout(&output);
    assert!(response["id"].as_str().unwrap().starts_with("req-"));
    assert_eq!(response["data"]["daemon"]["pid"], daemon["data"]["pid"]);
    assert_eq!(response["data"]["daemon"]["paused"], false);
    assert_eq!(response["data"]["gate"]["active_agents"], 0);
    assert_eq!(response["data"]["gate"]["max_agents"], 1);
    assert_eq!(response["data"]["runs"], serde_json::json!([]));
}

#[test]
fn operational_verbs_survive_an_invalid_flow_and_stop_the_daemon() {
    let world = World::configured();
    world.configure_fake_agent(
        FakeAgent::new()
            .block_until_released("invalid-flow")
            .commit("completed work")
            .exit(0),
    );
    let ticket = world.write_ticket("active.md", "# Active work\n");
    world.commit_all("initial");
    let pid = world.start_daemon()["data"]["pid"].as_u64().unwrap() as u32;
    assert!(
        world
            .sloop(&["post", ticket.to_str().unwrap(), "--auto"])
            .status
            .success()
    );
    wait_until("the agent reaches its blocking point", || {
        world.fake_agent_reached("invalid-flow")
    });
    let run = world.run_id(1);
    fs::write(
        world.root().join(".agents/sloop/flows/default.yaml"),
        "- { name: build, action: unknown }\n",
    )
    .expect("invalidate the flow file");

    for args in [
        vec!["status"],
        vec!["list"],
        vec!["logs", run.as_str()],
        vec!["pause"],
        vec!["resume"],
    ] {
        let output = world.sloop(&args);
        assert!(
            output.status.success(),
            "{args:?} failed after invalidating the flow: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    world.release("invalid-flow");
    wait_until("the admitted flow completes from its snapshot", || {
        status(&world)["tickets"]["merged"] == 1
    });
    let stopped = world.sloop(&["stop"]);
    assert!(
        stopped.status.success(),
        "stop failed: {}",
        String::from_utf8_lossy(&stopped.stderr)
    );
    wait_until("the daemon stops", || !process_alive(pid));

    let restart = world.sloop(&["daemon"]);
    assert!(!restart.status.success());
    let error = World::json_stdout_or_stderr(&restart);
    assert_eq!(error["error"]["code"], "invalid_arguments");
    let message = error["error"]["message"].as_str().unwrap();
    assert!(message.contains("default.yaml"), "{message}");
    assert!(message.contains("unknown action `unknown`"), "{message}");
}

#[test]
fn idle_restart_self_execs_and_reacquires_the_socket_and_lock() {
    let world = World::configured();
    world.commit_all("initial");
    let pid = world.start_daemon()["data"]["pid"].as_u64().unwrap() as u32;

    let output = world.sloop(&["daemon", "restart"]);

    assert!(output.status.success());
    let response = World::json_stdout(&output);
    assert_eq!(response["data"]["active_runs"], 0);
    assert_eq!(response["data"]["pid"], pid);
    wait_until("the replacement daemon starts", || {
        log_event_count(&world, "daemon_started") >= 2 && world.operator_socket().exists()
    });
    let restarted = status(&world);
    assert_eq!(restarted["daemon"]["pid"], pid);
    assert_eq!(restarted["daemon"]["draining"], false);
    assert!(process_alive(pid));
    assert_eq!(log_event_count(&world, "restart_drain_started"), 1);
    assert_eq!(log_event_count(&world, "restart_drain_complete"), 1);
    assert_eq!(log_event_count(&world, "restart_exec"), 1);
    assert_eq!(activity_event_count(&world, "daemon_restart_requested"), 1);
}

#[test]
fn restart_drains_active_stages_before_resuming_the_queue() {
    let world = World::configured();
    world.configure_fake_agent(
        FakeAgent::new()
            .block_until_released("drain")
            .commit("completed work")
            .exit(0),
    );
    let first = world.write_ticket("first.md", "# First\n");
    let second = world.write_ticket("second.md", "# Second\n");
    world.commit_all("initial");
    let pid = world.start_daemon()["data"]["pid"].as_u64().unwrap() as u32;
    for ticket in [&first, &second] {
        let output = world.sloop(&["post", ticket.to_str().unwrap(), "--auto"]);
        assert!(output.status.success());
    }
    wait_until("the first run blocks", || world.fake_agent_reached("drain"));

    let output = world.sloop(&["daemon", "restart"]);
    assert!(output.status.success());
    assert_eq!(World::json_stdout(&output)["data"]["active_runs"], 1);
    let draining = status(&world);
    assert_eq!(draining["daemon"]["draining"], true);
    assert_eq!(draining["gate"]["active_agents"], 1);
    assert_eq!(draining["queued_triggers"].as_array().unwrap().len(), 1);
    assert!(!world.run_worktree(2).exists());
    let human = world.sloop_plain(&["status"]);
    assert!(String::from_utf8_lossy(&human.stdout).contains("draining - 1/1 agents active"));

    world.release("drain");
    wait_until_slow("the daemon restarts and drains the queue", || {
        let snapshot = status(&world);
        log_event_count(&world, "daemon_started") >= 2
            && snapshot["daemon"]["pid"] == pid
            && snapshot["daemon"]["draining"] == false
            && snapshot["tickets"]["merged"] == 2
    });
    assert!(world.run_worktree(2).exists());
}

#[test]
fn resume_cancels_a_pending_restart_without_replacing_the_process() {
    let world = World::configured();
    world.configure_fake_agent(
        FakeAgent::new()
            .block_until_released("cancel-drain")
            .commit("completed work")
            .exit(0),
    );
    let first = world.write_ticket("first.md", "# First\n");
    let second = world.write_ticket("second.md", "# Second\n");
    world.commit_all("initial");
    let pid = world.start_daemon()["data"]["pid"].as_u64().unwrap() as u32;
    for ticket in [&first, &second] {
        assert!(
            world
                .sloop(&["post", ticket.to_str().unwrap(), "--auto"])
                .status
                .success()
        );
    }
    wait_until("the first run blocks", || {
        world.fake_agent_reached("cancel-drain")
    });
    assert!(world.sloop(&["daemon", "restart"]).status.success());

    let resumed = world.sloop(&["resume"]);
    assert!(resumed.status.success());
    assert_eq!(
        World::json_stdout(&resumed)["data"]["restart_cancelled"],
        true
    );
    assert_eq!(status(&world)["daemon"]["pid"], pid);
    assert_eq!(status(&world)["daemon"]["draining"], false);
    world.release("cancel-drain");
    wait_until_slow("the current daemon drains the queue", || {
        status(&world)["tickets"]["merged"] == 2
    });
    assert_eq!(log_event_count(&world, "daemon_started"), 1);
    assert_eq!(log_event_count(&world, "restart_cancelled"), 1);
    assert_eq!(log_event_count(&world, "restart_exec"), 0);
}

#[test]
fn a_fresh_daemon_clears_restart_draining_after_a_crash() {
    let world = World::configured();
    world.configure_fake_agent(
        FakeAgent::new()
            .block_until_released("crash-drain")
            .commit("completed work")
            .exit(0),
    );
    let ticket = world.write_ticket("active.md", "# Active\n");
    world.commit_all("initial");
    let pid = world.start_daemon()["data"]["pid"].as_u64().unwrap() as u32;
    assert!(
        world
            .sloop(&["post", ticket.to_str().unwrap(), "--auto"])
            .status
            .success()
    );
    wait_until("the run blocks", || world.fake_agent_reached("crash-drain"));
    assert!(world.sloop(&["daemon", "restart"]).status.success());
    assert_eq!(status(&world)["daemon"]["draining"], true);

    world.kill_daemon(pid);
    let recovered = status(&world);
    assert_ne!(recovered["daemon"]["pid"], pid);
    assert_eq!(recovered["daemon"]["draining"], false);
    world.release("crash-drain");
}

#[test]
fn exec_failure_clears_draining_and_keeps_the_scheduler_running() {
    let world = World::configured();
    world.configure_fake_agent(
        FakeAgent::new()
            .block_until_released("missing-binary")
            .commit("completed work")
            .exit(0),
    );
    let first = world.write_ticket("first.md", "# First\n");
    let second = world.write_ticket("second.md", "# Second\n");
    world.commit_all("initial");
    let installed = world.root().join("installed-sloop");
    fs::copy(env!("CARGO_BIN_EXE_sloop"), &installed).expect("copy installed binary");
    let pid = world.start_daemon_with_binary(&installed)["data"]["pid"]
        .as_u64()
        .unwrap() as u32;
    for ticket in [&first, &second] {
        assert!(
            world
                .sloop(&["post", ticket.to_str().unwrap(), "--auto"])
                .status
                .success()
        );
    }
    wait_until("the first run blocks", || {
        world.fake_agent_reached("missing-binary")
    });
    assert!(world.sloop(&["daemon", "restart"]).status.success());
    fs::remove_file(installed).expect("remove installed executable");
    world.release("missing-binary");

    wait_until_slow("the old daemon recovers from exec failure", || {
        let snapshot = status(&world);
        log_event_count(&world, "restart_exec_failed") == 1
            && snapshot["daemon"]["pid"] == pid
            && snapshot["daemon"]["draining"] == false
            && snapshot["tickets"]["merged"] == 2
    });
    assert!(process_alive(pid));
}

#[cfg(target_os = "linux")]
#[test]
fn restart_executes_the_binary_now_at_the_original_path() {
    use std::os::unix::fs::MetadataExt;

    let world = World::configured();
    world.configure_fake_agent(
        FakeAgent::new()
            .block_until_released("upgrade")
            .commit("completed work")
            .exit(0),
    );
    let ticket = world.write_ticket("active.md", "# Active\n");
    world.commit_all("initial");
    let installed = world.root().join("installed-sloop");
    fs::copy(env!("CARGO_BIN_EXE_sloop"), &installed).expect("copy installed binary");
    let pid = world.start_daemon_with_binary(&installed)["data"]["pid"]
        .as_u64()
        .unwrap() as u32;
    assert!(
        world
            .sloop(&["post", ticket.to_str().unwrap(), "--auto"])
            .status
            .success()
    );
    wait_until("the run blocks", || world.fake_agent_reached("upgrade"));
    assert!(world.sloop(&["daemon", "restart"]).status.success());

    let staged = world.root().join("replacement-sloop");
    fs::copy(env!("CARGO_BIN_EXE_sloop"), &staged).expect("stage replacement binary");
    let replacement_inode = fs::metadata(&staged).unwrap().ino();
    fs::rename(&staged, &installed).expect("replace installed binary atomically");
    world.release("upgrade");

    wait_until_slow("the replacement executable image starts", || {
        fs::metadata(format!("/proc/{pid}/exe"))
            .map(|metadata| metadata.ino() == replacement_inode)
            .unwrap_or(false)
            && status(&world)["daemon"]["draining"] == false
    });
    assert_eq!(status(&world)["daemon"]["pid"], pid);
}

#[test]
fn operator_socket_is_private() {
    let world = World::configured();
    world.start_daemon();

    let mode = fs::metadata(world.operator_socket())
        .expect("operator socket metadata")
        .permissions()
        .mode()
        & 0o777;

    assert_eq!(mode, 0o600);
}

#[test]
fn machine_local_files_live_outside_the_repository() {
    let world = World::configured();
    let daemon = world.start_daemon();

    assert_eq!(
        daemon["data"]["socket"],
        world.operator_socket().to_string_lossy().as_ref()
    );
    assert_eq!(
        daemon["data"]["state_dir"],
        world.state_dir().to_string_lossy().as_ref()
    );
    assert_eq!(
        daemon["data"]["log"],
        world.daemon_log().to_string_lossy().as_ref()
    );
    assert!(world.db_path().is_file());
    assert!(world.lock_path().is_file());
    assert!(world.daemon_log().is_file());
    for directory in [world.state_dir(), world.runtime_dir()] {
        let mode = fs::metadata(directory).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
    }
    assert!(!world.root().join(".sloop").exists());
}

#[test]
fn the_spawned_daemon_leads_its_own_session() {
    let world = World::configured();
    let daemon = world.start_daemon();
    let pid = daemon["data"]["pid"].as_u64().expect("daemon pid") as libc::pid_t;

    // Any client command spawns the daemon and then exits, so the daemon has to
    // outlive whichever short-lived process happened to start it. Inheriting
    // that client's session or process group makes a signal aimed at the client
    // -- a terminal hangup, a supervisor reaping a finished command's group --
    // kill the daemon too, which reads as an unexplained crash.
    let session = unsafe { libc::getsid(pid) };
    assert_ne!(session, -1, "the daemon's session id is readable");
    assert_eq!(session, pid, "the daemon leads its own session");
    assert_eq!(
        unsafe { libc::getpgid(pid) },
        pid,
        "the daemon leads its own process group"
    );
    assert_ne!(
        session,
        unsafe { libc::getsid(0) },
        "the daemon left the session of the process that spawned it"
    );
}

#[test]
fn malformed_requests_return_errors_without_stopping_the_daemon() {
    let world = World::configured();
    world.start_daemon();

    let malformed = world.operator_exchange("{");
    assert_eq!(malformed["ok"], false);
    assert_eq!(malformed["error"]["code"], "invalid_request");

    let output = world.sloop(&["status"]);
    assert!(output.status.success());
    assert_eq!(World::json_stdout(&output)["ok"], true);
}

#[test]
fn unknown_verbs_and_versions_are_structured_socket_errors() {
    let world = World::configured();
    world.start_daemon();

    let unknown =
        world.operator_exchange(r#"{"v":1,"id":"req-x","verb":"merge","args":{},"token":null}"#);
    assert_eq!(unknown["error"]["code"], "unknown_verb");

    let version =
        world.operator_exchange(r#"{"v":99,"id":"req-y","verb":"status","args":{},"token":null}"#);
    assert_eq!(version["error"]["code"], "unsupported_version");
}

#[test]
fn daemon_log_is_ndjson_and_separate_from_cli_output() {
    let world = World::configured();
    world.start_daemon();
    let output = world.sloop(&["status"]);
    assert!(output.status.success());

    let log = fs::read_to_string(world.daemon_log()).expect("read daemon log");
    let records: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("daemon log line is JSON"))
        .collect();
    assert!(
        records
            .iter()
            .any(|record| record["event"] == "daemon_started")
    );
    assert!(
        records
            .iter()
            .any(|record| record["event"] == "request_handled")
    );
}

#[test]
fn malformed_config_fails_before_daemon_start() {
    let world = World::configured();
    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        "version: 9\n",
    )
    .expect("write invalid config");

    let output = world.sloop(&["daemon"]);

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let response: Value = serde_json::from_slice(&output.stderr).expect("error is JSON");
    assert_eq!(response["error"]["code"], "invalid_arguments");
    assert!(!world.operator_socket().exists());
}

#[test]
fn invalid_id_prefix_fails_before_daemon_start_with_the_configuration_key() {
    let world = World::configured();
    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        "version: 1\nids:\n  project_prefix: 'not valid'\n",
    )
    .expect("write invalid config");

    let output = world.sloop(&["daemon"]);

    assert!(!output.status.success());
    let response: Value = serde_json::from_slice(&output.stderr).expect("error is JSON");
    assert_eq!(response["error"]["code"], "invalid_arguments");
    assert!(
        response["error"]["message"]
            .as_str()
            .expect("error message")
            .contains("ids.project_prefix")
    );
    assert!(!world.operator_socket().exists());
}

#[test]
fn startup_stamps_idless_projects_in_sorted_order_without_rewriting_explicit_projects() {
    let world = World::configured();
    let projects = world.root().join(".agents/sloop/projects");
    let default_path = projects.join("default.md");
    let default_content = fs::read_to_string(&default_path).expect("read default project");
    fs::write(
        projects.join("zeta.md"),
        "---\ntitle: Zeta\n---\nZeta body\n",
    )
    .expect("write zeta project");
    fs::write(projects.join("alpha.md"), "# Alpha body\n").expect("write alpha project");

    world.start_daemon();

    let alpha = fs::read_to_string(projects.join("alpha.md")).expect("read alpha project");
    let zeta = fs::read_to_string(projects.join("zeta.md")).expect("read zeta project");
    assert!(alpha.starts_with("---\nid: PROJ-1\n---\n"), "{alpha}");
    assert!(zeta.contains("id: PROJ-2\n"), "{zeta}");
    assert!(!alpha.contains("project:"));
    assert!(!zeta.contains("project:"));
    assert_eq!(
        fs::read_to_string(default_path).expect("reread default project"),
        default_content
    );

    let ticket = world.write_ticket("alpha-work.md", "# Alpha work\n");
    let output = world.sloop(&[
        "post",
        ticket.to_str().expect("UTF-8 ticket path"),
        "--project",
        "PROJ-1",
        "--manual",
    ]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        World::json_stdout(&output)["data"]["ticket"]["project"],
        "PROJ-1"
    );
}

#[test]
fn startup_uses_the_repository_project_prefix() {
    let world = World::configured();
    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        "version: 1\nids:\n  project_prefix: TEAM\n",
    )
    .expect("write configured project prefix");
    let project = world.root().join(".agents/sloop/projects/team.md");
    fs::write(&project, "---\ntitle: Team\n---\n").expect("write team project");

    world.start_daemon();

    assert!(
        fs::read_to_string(project)
            .expect("read stamped project")
            .contains("id: TEAM-1")
    );
}

#[test]
fn user_scheduler_defaults_apply_when_the_repository_omits_them() {
    let world = World::configured();
    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        "version: 1\n",
    )
    .expect("write repository config");
    let user_config = world.root().join("home/.config/sloop");
    fs::create_dir_all(&user_config).expect("create user config directory");
    fs::write(
        user_config.join("config.yaml"),
        "version: 1\ndefaults:\n  scheduler:\n    max_parallel_tasks: 4\n",
    )
    .expect("write user config");

    world.start_daemon();
    let status = world.sloop(&["status"]);

    assert!(status.status.success());
    assert_eq!(World::json_stdout(&status)["data"]["gate"]["max_agents"], 4);
}

#[test]
fn repository_scheduler_values_override_user_defaults() {
    let world = World::configured();
    let user_config = world.root().join("home/.config/sloop");
    fs::create_dir_all(&user_config).expect("create user config directory");
    fs::write(
        user_config.join("config.yaml"),
        "version: 1\ndefaults:\n  scheduler:\n    max_parallel_tasks: 4\n",
    )
    .expect("write user config");

    world.start_daemon();
    let status = world.sloop(&["status"]);

    assert!(status.status.success());
    assert_eq!(World::json_stdout(&status)["data"]["gate"]["max_agents"], 1);
}

#[test]
fn user_configuration_cannot_supply_repository_id_prefixes() {
    let world = World::configured();
    let user_config = world.root().join("home/.config/sloop");
    fs::create_dir_all(&user_config).expect("create user config directory");
    fs::write(
        user_config.join("config.yaml"),
        "version: 1\nids:\n  ticket_prefix: USER\n  project_prefix: PERSONAL\n",
    )
    .expect("write user config");
    let project = world.root().join(".agents/sloop/projects/other.md");
    fs::write(&project, "# Other\n").expect("write project");
    let ticket = world.write_ticket("other.md", "# Other work\n");

    world.start_daemon();
    let output = world.sloop(&[
        "post",
        ticket.to_str().expect("UTF-8 ticket path"),
        "--manual",
    ]);

    assert_eq!(
        World::json_stdout(&output)["data"]["ticket"]["id"],
        "TICK-1"
    );
    assert!(
        fs::read_to_string(project)
            .expect("read stamped project")
            .contains("id: PROJ-1")
    );
}

#[test]
fn user_configuration_cannot_supply_repository_directories() {
    let world = World::configured();
    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        "version: 1\n",
    )
    .expect("write repository config");
    let user_config = world.root().join("home/.config/sloop");
    fs::create_dir_all(&user_config).expect("create user config directory");
    fs::write(
        user_config.join("config.yaml"),
        concat!(
            "version: 1\n",
            "worktree_dir: /tmp/user-worktrees\n",
            "project_dir: /tmp/user-projects\n",
            "ticket_dir: /tmp/user-tickets\n",
        ),
    )
    .expect("write user config");

    let output = world.sloop(&["daemon"]);

    assert!(
        output.status.success(),
        "repository defaults should ignore user directory settings: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn status_renders_a_human_summary_without_the_json_flag() {
    let world = World::configured();
    world.start_daemon();

    let output = world.sloop_plain(&["status"]);

    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    assert!(
        serde_json::from_str::<Value>(&text).is_err(),
        "default status output must not be JSON: {text}"
    );
    assert!(text.contains("ready"), "ticket counts are shown: {text}");
    assert!(text.contains("agents"), "the gate is shown: {text}");
}

#[test]
fn show_treats_unknown_text_as_a_ticket_pattern() {
    let world = World::configured();

    // Once every exact reference form misses, the final resolution rung is a
    // ticket pattern. A valid pattern that matches nothing is still a list.
    let output = world.sloop(&["show", "nonexistent"]);

    assert!(output.status.success());
    let response = World::json_stdout(&output);
    assert_eq!(response["data"]["kind"], "matches");
    assert_eq!(response["data"]["tickets"], serde_json::json!([]));
}

#[test]
fn worker_verbs_on_the_operator_socket_point_at_an_alternative() {
    let world = World::configured();
    world.start_daemon();

    let response =
        world.operator_exchange(r#"{"v":1,"id":"req-1","verb":"brief","args":{},"token":null}"#);

    assert_eq!(response["error"]["code"], "unauthorized");
    let message = response["error"]["message"]
        .as_str()
        .expect("error message");
    assert!(
        message.contains("sloop show"),
        "remedy does not name an operator alternative: {message}"
    );
}

/// A run claimed by a `0.3.x` binary carries a flow snapshot in a grammar this
/// one no longer reads. The snapshot is machine-local, so the daemon cannot
/// repair it, but the failure belongs to that one run: it is parked for review
/// by id and startup carries on rather than the recovery pass — and with it
/// every other run's recovery — dying on one unreadable row.
#[test]
fn an_unreadable_flow_snapshot_parks_its_run_without_stopping_recovery() {
    let world = World::configured();
    world.configure_fake_agent(FakeAgent::new().block_until_released("legacy-snapshot"));
    let ticket = world.write_ticket("legacy.md", "# Legacy\nwork\n");
    world.commit_all("seed");
    let daemon_pid = world.start_daemon()["data"]["pid"].as_u64().unwrap() as u32;
    let posted = world.sloop(&["post", ticket.to_str().unwrap(), "--auto"]);
    assert!(posted.status.success());
    wait_until("the agent reaches its blocking point", || {
        world.fake_agent_reached("legacy-snapshot")
    });
    let run_id = world.run_id(1);
    let agent_pid = world.run_process_id(&run_id);

    world.kill_daemon(daemon_pid);
    world.release("legacy-snapshot");
    wait_until("the orphaned agent exits", || !process_alive(agent_pid));

    // Forge exactly what a pre-split binary left behind: a driving run whose
    // snapshot spells its stages in the removed `kind`/`verdict` grammar.
    let connection = rusqlite::Connection::open(world.db_path()).unwrap();
    connection
        .execute(
            "UPDATE runs
             SET state = 'driving',
                 flow_json = '{\"name\":\"default\",\"stages\":[{\"name\":\"build\",\"kind\":\"Build\"}]}'
             WHERE id = ?1",
            [&run_id],
        )
        .unwrap();
    drop(connection);

    world.start_daemon();
    wait_until("the unresumable run is parked for review", || {
        let snapshot = status(&world);
        snapshot["gate"]["active_agents"] == 0 && snapshot["tickets"]["needs_review"] == 1
    });
    assert_eq!(world.show_snapshot(&run_id)["state"], "needs_review");

    let named_the_run = fs::read_to_string(world.daemon_log())
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .any(|record| {
            record["event"] == "driver_resume_failed" && record["fields"]["run_id"] == run_id
        });
    assert!(named_the_run, "the parked run is not named in the log");

    // The pass survived the bad row: the scheduler is still serving demand.
    let next = world.write_ticket("next.md", "# Next\nwork\n");
    world.commit_all("second ticket");
    world.configure_fake_agent(FakeAgent::new().commit("completed work").exit(0));
    let posted = world.sloop(&["post", next.to_str().unwrap(), "--auto"]);
    assert!(posted.status.success());
    wait_until_slow("the daemon still dispatches after the parked run", || {
        status(&world)["tickets"]["merged"] == 1
    });
}

/// A trigger pinned to a merged ticket can never fire: dispatch requires the
/// ticket to be `ready`, and `merged` is terminal. Settling now retires such a
/// trigger in the same transaction as the merge, but rows stranded before that
/// rule existed outlive every future settlement, so startup sweeps them.
#[test]
fn startup_completes_triggers_pinned_to_an_already_merged_ticket() {
    let world = World::configured();
    let ticket = world.write_ticket("stranded.md", "# Stranded\n");
    world.commit_all("initial");
    let pid = world.start_daemon()["data"]["pid"].as_u64().unwrap() as u32;
    let posted = world.sloop(&["post", ticket.to_str().unwrap(), "--manual"]);
    assert!(
        posted.status.success(),
        "post failed: {}",
        String::from_utf8_lossy(&posted.stderr)
    );
    let ticket_id = World::json_stdout(&posted)["data"]["ticket"]["id"]
        .as_str()
        .expect("posted ticket id")
        .to_owned();
    stop_daemon(&world, pid);

    // Forge exactly what an older daemon left behind: a queued trigger pinned
    // to a ticket that has since merged.
    let connection = rusqlite::Connection::open(world.db_path()).unwrap();
    connection
        .execute(
            "UPDATE tickets SET state = 'merged' WHERE id = ?1",
            [&ticket_id],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO triggers
                 (id, kind, state, ticket_id, created_at_ms, updated_at_ms)
             VALUES ('TR68', 'immediate', 'queued', ?1, 1, 1)",
            [&ticket_id],
        )
        .unwrap();
    drop(connection);

    let pid = world.start_daemon()["data"]["pid"].as_u64().unwrap() as u32;
    assert_eq!(
        status(&world)["queued_triggers"],
        serde_json::json!([]),
        "the dead trigger is still counted as pending demand"
    );
    assert_eq!(
        log_event_count(&world, "trigger_completed_on_merged_ticket"),
        1,
        "the sweep mutated rows without saying so"
    );

    // Idempotent: the next start finds nothing left to complete.
    stop_daemon(&world, pid);
    world.start_daemon();
    assert_eq!(
        log_event_count(&world, "trigger_completed_on_merged_ticket"),
        1
    );
}
