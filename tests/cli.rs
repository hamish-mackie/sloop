mod support;

use std::fs;

use serde_json::Value;
use support::World;

#[test]
fn world_is_an_isolated_git_repository() {
    let world = World::new();

    assert!(world.root().join(".git").is_dir());
}

#[test]
fn init_does_not_modify_gitignore() {
    let world = World::new();
    fs::write(world.root().join(".gitignore"), "target/\n").unwrap();

    let output = world.sloop(&["init"]);
    assert!(
        output.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let gitignore = fs::read_to_string(world.root().join(".gitignore")).unwrap();
    assert_eq!(gitignore, "target/\n");
}

#[test]
fn init_scaffolds_the_default_flow_and_review_prompt() {
    let world = World::new();

    let output = world.sloop(&["init"]);
    assert!(output.status.success());
    let flow = fs::read_to_string(world.root().join(".agents/sloop/flows/default.yaml")).unwrap();
    assert!(flow.contains("kind: build"));
    assert!(flow.contains("kind: exec"));
    assert!(flow.contains("kind: merge"));
    assert!(flow.contains(".agents/sloop/prompts/review.md"));
    assert!(
        world
            .root()
            .join(".agents/sloop/prompts/review.md")
            .is_file()
    );
}

#[test]
fn invalid_flow_prevents_daemon_startup_with_a_named_error() {
    let world = World::configured();
    fs::create_dir_all(world.root().join(".agents/sloop/flows")).unwrap();
    fs::write(
        world.root().join(".agents/sloop/flows/broken.yaml"),
        "- { name: build, kind: build }\n- { name: surprise, kind: unknown }\n",
    )
    .unwrap();

    let output = world.sloop(&["daemon"]);
    assert!(!output.status.success());
    let response = World::json_stdout_or_stderr(&output);
    let message = response["error"]["message"].as_str().unwrap();
    assert!(message.contains("broken.yaml"), "{message}");
    assert!(message.contains("unknown kind `unknown`"), "{message}");
}

#[test]
fn documented_verbs_are_exposed_by_the_real_binary() {
    let world = World::new();
    let output = world.sloop(&["--help", "--all"]);

    assert!(output.status.success());
    let response: Value = serde_json::from_slice(&output.stdout).expect("help is JSON");
    assert_eq!(response["ok"], true);
    assert_eq!(response["data"]["kind"], "help");
    let help = response["data"]["text"].as_str().expect("help text");
    for verb in [
        "init", "daemon", "post", "run", "retry", "hold", "ready", "list", "status", "pause",
        "resume", "cancel", "logs", "reindex", "brief", "show", "note",
    ] {
        assert!(help.contains(verb), "help did not contain {verb:?}");
    }
}

#[test]
fn expanded_help_explains_every_ticket_state() {
    let world = World::new();
    let output = world.sloop_plain(&["--help", "--all"]);

    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).expect("help is UTF-8");
    assert!(
        help.contains("Ticket states:"),
        "ticket state glossary missing"
    );
    for state in [
        "ready",
        "held",
        "blocked",
        "claimed",
        "merged",
        "failed",
        "needs_review",
    ] {
        assert!(
            help.contains(&format!("  {state}")),
            "help did not explain {state:?}"
        );
    }
    assert!(
        help.contains("Terminal: aftercare could not merge the run; inspect manually."),
        "needs_review meaning missing"
    );
}

#[test]
fn default_help_only_shows_common_commands() {
    let world = World::new();
    let output = world.sloop_plain(&["--help"]);

    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).expect("help is UTF-8");
    for verb in ["init", "daemon", "post", "list", "status", "brief"] {
        assert!(
            help.contains(&format!("  {verb}")),
            "help did not contain {verb:?}"
        );
    }
    for verb in ["run", "retry", "pause", "cancel", "logs", "reindex", "note"] {
        assert!(
            !help.contains(&format!("  {verb}")),
            "compact help unexpectedly contained {verb:?}"
        );
    }
    assert!(
        help.contains("sloop --help --all"),
        "expanded-help hint missing"
    );
    assert!(
        !help.contains("Ticket states:"),
        "compact help unexpectedly contained the ticket state glossary"
    );
}

#[test]
fn output_is_human_readable_without_the_json_flag() {
    let world = World::configured();
    let output = world.sloop_plain(&["pause"]);

    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    assert!(
        serde_json::from_str::<Value>(&text).is_err(),
        "default output must not be JSON: {text}"
    );
    assert_eq!(text, "scheduler paused\n");
}

#[test]
fn errors_are_human_readable_without_the_json_flag() {
    let world = World::new();
    let output = world.sloop_plain(&["post", "ticket.md"]);

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let text = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(
        serde_json::from_str::<Value>(&text).is_err(),
        "default error output must not be JSON: {text}"
    );
    assert!(!text.trim().is_empty());
}

#[test]
fn help_is_plain_text_without_the_json_flag() {
    let world = World::new();
    let output = world.sloop_plain(&["--help"]);

    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    assert!(
        serde_json::from_str::<Value>(&text).is_err(),
        "plain help must not be JSON: {text}"
    );
    assert!(text.contains("Usage"), "clap help text expected: {text}");
}

#[test]
fn the_json_flag_is_accepted_before_or_after_the_verb() {
    let world = World::configured();
    for args in [
        ["--json", "pause"].as_slice(),
        ["pause", "--json"].as_slice(),
    ] {
        let output = world.sloop_plain(args);
        assert!(output.status.success());
        let response: Value =
            serde_json::from_slice(&output.stdout).expect("--json output is an envelope");
        assert_eq!(response["ok"], true, "for {args:?}");
        assert_eq!(response["data"]["paused"], true, "for {args:?}");
    }
}

#[test]
fn pause_reaches_the_daemon_dispatch() {
    let world = World::configured();
    let output = world.sloop(&["pause"]);

    assert!(output.status.success());
    let response: Value = serde_json::from_slice(&output.stdout).expect("daemon output is JSON");
    assert_eq!(response["ok"], true);
    assert_eq!(response["data"]["paused"], true);
}

#[test]
fn invalid_arguments_fail_before_dispatch() {
    let world = World::new();
    let output = world.sloop(&["run", "--at", "03:00", "--every", "30m"]);

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let response: Value = serde_json::from_slice(&output.stderr).expect("error output is JSON");
    assert_eq!(response["ok"], false);
    assert_eq!(response["error"]["code"], "invalid_arguments");
}

#[test]
fn post_rejects_multiple_activation_modes() {
    let world = World::new();
    let output = world.sloop(&["post", "ticket.md", "--auto", "--manual"]);

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let response: Value = serde_json::from_slice(&output.stderr).expect("error output is JSON");
    assert_eq!(response["ok"], false);
    assert_eq!(response["error"]["code"], "invalid_arguments");
}

#[test]
fn version_output_is_json() {
    let world = World::new();
    let output = world.sloop(&["--version"]);

    assert!(output.status.success());
    let response: Value = serde_json::from_slice(&output.stdout).expect("version is JSON");
    assert_eq!(response["ok"], true);
    assert_eq!(response["data"]["kind"], "version");
    assert!(response["data"]["version"].is_string());
}
