mod support;

use std::fs;
use std::os::unix::fs::PermissionsExt;

use serde_json::Value;
use support::World;

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
    assert!(output.stderr.is_empty());
    let response = World::json_stdout(&output);
    assert!(response["id"].as_str().unwrap().starts_with("req-"));
    assert_eq!(response["data"]["daemon"]["pid"], daemon["data"]["pid"]);
    assert_eq!(response["data"]["daemon"]["paused"], false);
    assert_eq!(response["data"]["gate"]["active_agents"], 0);
    assert_eq!(response["data"]["gate"]["max_agents"], 1);
    assert_eq!(response["data"]["runs"], serde_json::json!([]));
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
fn show_names_the_reference_kinds_it_accepts() {
    let world = World::configured();

    // R99 is a run-shaped id, but `show` only resolves tickets and projects.
    let output = world.sloop(&["show", "R99"]);

    assert!(!output.status.success());
    let response = World::json_stdout_or_stderr(&output);
    assert_eq!(response["error"]["code"], "not_found");
    let message = response["error"]["message"]
        .as_str()
        .expect("error message");
    assert!(
        message.contains("ticket or project") && message.contains("sloop list"),
        "remedy does not name the accepted references: {message}"
    );
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
        message.contains("sloop list"),
        "remedy does not name an operator alternative: {message}"
    );
}
