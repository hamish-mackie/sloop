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
    assert!(flow.contains("action: agent"));
    assert!(flow.contains("exec:"));
    assert!(flow.contains("builtin: merge"));
    assert!(flow.contains(".agents/sloop/prompts/review.md"));
    assert!(
        world
            .root()
            .join(".agents/sloop/prompts/review.md")
            .is_file()
    );
}

/// The train ships beside the default flow, not instead of it, and a fresh
/// repository gets a file the loader accepts — a scaffolded flow that does not
/// parse would take the whole repository's configuration down with it.
#[test]
fn init_materializes_the_train_flow_beside_the_default_one() {
    let world = World::new();

    let output = world.sloop(&["init"]);
    assert!(
        output.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let train = fs::read_to_string(world.root().join(".agents/sloop/flows/train.yaml")).unwrap();
    assert!(train.contains("builtin: sync"), "{train}");
    assert!(train.contains("ff_only: true"), "{train}");
    assert!(train.contains("return_to: sync"), "{train}");
    assert!(
        world
            .root()
            .join(".agents/sloop/flows/default.yaml")
            .is_file()
    );

    // The daemon validates every flow in the repository at startup, so it
    // coming up is the loader accepting what `init` just wrote.
    world.commit_all("scaffold");
    world.start_daemon();
    let flows = World::json_stdout(&world.sloop(&["status"]))["data"].clone();
    assert!(flows.is_object(), "{flows}");
}

#[test]
fn invalid_flow_prevents_daemon_startup_with_a_named_error() {
    let world = World::configured();
    fs::create_dir_all(world.root().join(".agents/sloop/flows")).unwrap();
    fs::write(
        world.root().join(".agents/sloop/flows/broken.yaml"),
        "- { name: build, action: agent }\n- { name: surprise, action: unknown }\n",
    )
    .unwrap();

    let output = world.sloop(&["daemon"]);
    assert!(!output.status.success());
    let response = World::json_stdout_or_stderr(&output);
    let message = response["error"]["message"].as_str().unwrap();
    assert!(message.contains("broken.yaml"), "{message}");
    assert!(message.contains("unknown action `unknown`"), "{message}");
}

/// The templates are the only grammar documentation an installed binary can
/// reach, so the binary must accept exactly what it prints.
///
/// The unit tests in `src/templates.rs` already run each template through its
/// own loader. This goes one rung further out: the *printed bytes* of `sloop
/// template flow` and `sloop template ticket`, dropped into a fresh repository
/// and posted. `post` snapshots the ticket's flow, so a template the parser
/// accepts but the post path rejects — a panel seat naming no configured agent,
/// a stage colliding with a spliced `flow.test_cmd` — fails here rather than in
/// a user's terminal.
#[test]
fn the_printed_templates_post_cleanly_in_a_fresh_repository() {
    let world = World::new();
    assert!(world.sloop(&["init"]).status.success());

    let printed = |kind: &str| {
        let output = world.sloop_plain(&["template", kind]);
        assert!(
            output.status.success(),
            "template {kind} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("template is UTF-8")
    };

    // The flow is installed under the name the ticket will bind to, so the
    // post has to read and snapshot this exact text.
    fs::write(
        world.root().join(".agents/sloop/flows/from-template.yaml"),
        printed("flow"),
    )
    .unwrap();
    let ticket = world.root().join(".agents/sloop/tickets/from-template.md");
    fs::write(&ticket, printed("ticket")).unwrap();

    let output = world.sloop(&[
        "post",
        ticket.to_str().unwrap(),
        "--flow",
        "from-template",
        "--manual",
    ]);
    assert!(
        output.status.success(),
        "posting the printed templates failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let posted = World::json_stdout(&output)["data"]["ticket"].clone();
    assert_eq!(posted["name"], "Add request logging");
    assert_eq!(posted["state"], "ready");
    assert_eq!(posted["flow"], "from-template");

    // The success above only means something if this path can fail: `post`
    // reads and snapshots the named flow, so a flow it cannot parse stops the
    // post rather than being discovered when the run is dispatched.
    fs::write(
        world.root().join(".agents/sloop/flows/broken.yaml"),
        "- { name: build, action: agent }\n- { name: oops, action: nonsense }\n",
    )
    .unwrap();
    let rejected = world.sloop(&[
        "post",
        ticket.to_str().unwrap(),
        "--flow",
        "broken",
        "--manual",
    ]);
    assert!(!rejected.status.success());
    let message = World::json_stdout_or_stderr(&rejected)["error"]["message"]
        .as_str()
        .expect("error message")
        .to_owned();
    assert!(message.contains("broken.yaml"), "{message}");
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
        "resume", "cancel", "logs", "reindex", "brief", "show", "note", "verdict",
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
        help.contains("Terminal: the run could not be merged; inspect manually."),
        "needs_review meaning missing"
    );
}

#[test]
fn default_help_only_shows_common_commands() {
    let world = World::new();
    let output = world.sloop_plain(&["--help"]);

    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).expect("help is UTF-8");
    for verb in ["init", "daemon", "post", "show", "logs"] {
        assert!(
            help.contains(&format!("  {verb}")),
            "help did not contain {verb:?}"
        );
    }
    for verb in [
        "run", "retry", "pause", "cancel", "list", "status", "watch", "wait", "reindex", "brief",
        "note",
    ] {
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
fn show_help_teaches_the_read_model() {
    let world = World::new();
    let output = world.sloop_plain(&["show", "--help"]);

    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).expect("help is UTF-8");
    for phrase in [
        "resolved in this order",
        "exact match always wins",
        "run-id prefix",
        "case-insensitively",
        "regex metacharacters",
        "EXIT CODES",
        "sloop show TICK-12 --follow",
        "sloop show TICK-12 -f -q",
    ] {
        assert!(help.contains(phrase), "show help missed {phrase:?}: {help}");
    }
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
fn post_rejects_multiple_trigger_modes() {
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
