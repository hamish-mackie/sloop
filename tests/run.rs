mod support;

use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use serde_json::json;
use sloop::clock::{Clock, SystemClock};
use support::{FakeAgent, World, process_alive, wait_until, wait_until_slow};

/// Writes a scripted fake agent and points the repository config at it. The
/// script records its worktree and env, optionally blocks until `release`
/// exists in the repository root, and always gives up after ~10 seconds so a
/// failing test cannot leak a spinning process.
fn configure_fake_agent(world: &World, max_parallel_tasks: usize, blocking: bool) {
    configure_fake_agent_with_hours(world, max_parallel_tasks, blocking, None);
}

fn configure_failing_fake_agent(world: &World, max_parallel_tasks: usize, blocking: bool) {
    configure_fake_agent(world, max_parallel_tasks, blocking);
    let script = world.root().join("fake-agent.sh");
    let body = fs::read_to_string(&script).expect("read fake agent script");
    fs::write(script, body.replace("exit 0\n", "exit 1\n"))
        .expect("write failing fake agent script");
}

fn configure_fake_agent_with_hours(
    world: &World,
    max_parallel_tasks: usize,
    blocking: bool,
    running_hours: Option<(u16, u16)>,
) {
    fs::create_dir_all(world.root().join(".agents/sloop/flows")).unwrap();
    fs::write(
        world.root().join(".agents/sloop/flows/default.yaml"),
        "stages:\n  - { name: build, kind: agent, verdict: exit }\n  - { name: merge, kind: merge }\n",
    )
    .unwrap();
    let script = world.root().join("fake-agent.sh");
    let release = world.root().join("release");
    let wait_loop = if blocking {
        format!(
            "tries=0\nwhile [ ! -e \"{}\" ] && [ \"$tries\" -lt 200 ]; do sleep 0.05; tries=$((tries + 1)); done\n",
            release.display()
        )
    } else {
        String::new()
    };
    fs::write(
        &script,
        format!("#!/bin/sh\necho \"$SLOOP_TICKET_ID\" > agent-ran.txt\n{wait_loop}exit 0\n"),
    )
    .expect("write fake agent script");

    let hours = running_hours.map_or_else(String::new, |(start, end)| {
        format!(
            "  running_hours:\n    start: '{:02}:{:02}'\n    end: '{:02}:{:02}'\n",
            start / 60,
            start % 60,
            end / 60,
            end % 60,
        )
    });
    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        format!(
            "version: 1\nscheduler:\n  max_parallel_tasks: {max_parallel_tasks}\n{hours}agent:\n  default_target: fake\n  targets:\n    fake:\n      cmd: [\"sh\", \"{}\", \"{{prompt}}\"]\n",
            script.display()
        ),
    )
    .expect("write agent config");
}

fn post_manual(world: &World, name: &str, body: &str) -> String {
    let ticket = world.write_ticket(name, body);
    let output = world.sloop(&[
        "post",
        ticket.to_str().expect("UTF-8 ticket path"),
        "--manual",
    ]);
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

fn post_manual_blocked(world: &World, name: &str, blockers: &[&str]) -> String {
    let ticket = Path::new(".agents/sloop/tickets").join(name);
    fs::write(
        world.root().join(&ticket),
        format!(
            "---\nname: blocked dependent\nblocked_by: [{}]\n---\n# Blocked dependent\n",
            blockers.join(", ")
        ),
    )
    .unwrap();
    let output = world.sloop(&["post", ticket.to_str().unwrap(), "--manual"]);
    assert!(
        output.status.success(),
        "post failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    World::json_stdout(&output)["data"]["ticket"]["id"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn worktree_marker(world: &World, position: usize) -> std::path::PathBuf {
    world.run_worktree(position).join("agent-ran.txt")
}

/// The short form of a run's internal id: what names its worktree directory
/// and the tail of its run branch.
fn short_run_id(world: &World, position: usize) -> String {
    let id = world.run_id(position);
    id.get(..8).unwrap_or(&id).to_owned()
}

fn status(world: &World) -> serde_json::Value {
    let output = world.sloop(&["status"]);
    assert!(output.status.success());
    World::json_stdout(&output)["data"].clone()
}

fn is_git_worktree(path: &Path) -> bool {
    // A linked worktree carries a `.git` file pointing back at the parent.
    path.join(".git").is_file()
}

#[test]
fn run_executes_the_fake_agent_in_an_isolated_worktree() {
    let world = World::configured();
    configure_fake_agent(&world, 1, false);
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_manual(&world, "cooldown.md", "# Persist cooldowns\n");

    let output = world.sloop(&["run", &ticket]);
    assert!(
        output.status.success(),
        "run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let response = World::json_stdout(&output);
    assert_eq!(response["data"]["activation"]["state"], "queued");
    assert_eq!(response["data"]["activation"]["ticket"], ticket.as_str());

    wait_until("the fake agent runs in its worktree", || {
        worktree_marker(&world, 1).is_file()
    });
    let marker = worktree_marker(&world, 1);
    assert_eq!(fs::read_to_string(&marker).unwrap().trim(), ticket);
    assert!(is_git_worktree(marker.parent().unwrap()));

    wait_until("the run exit is recorded", || {
        let data = status(&world);
        data["gate"]["active_agents"] == 0 && data["runs"].as_array().unwrap().is_empty()
    });
    // A successful no-op settles normally and never leaves the ticket claimed.
    let tickets = status(&world)["tickets"].clone();
    assert_eq!(tickets["claimed"], 0);
    assert_eq!(tickets["merged"], 1);
}

#[test]
fn agent_receives_composed_prompt_and_sloop_binary_environment() {
    let world = World::configured();
    let script = world.root().join("record-launch.sh");
    fs::write(
        &script,
        "#!/bin/sh\nset -eu\nprintf '%s' \"$1\" > prompt.txt\nprintf '%s' \"$SLOOP_BIN\" > sloop-bin.txt\ncommand -v sloop > sloop-resolved.txt\n",
    )
    .unwrap();
    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        format!(
            "version: 1\nagent:\n  default_target: fake\n  targets:\n    fake:\n      cmd: [\"sh\", {}, \"{{prompt}}\"]\n",
            serde_json::to_string(&script.to_string_lossy()).unwrap(),
        ),
    )
    .unwrap();
    world.commit_all("initial");
    world.start_daemon();

    fs::write(
        world.root().join(".agents/sloop/instructions.md"),
        "Follow the repository's launch-time instructions.\n",
    )
    .unwrap();
    world.commit_all("add worker instructions");
    let ticket = post_manual(&world, "prompt.md", "# Record launch context\n");

    assert!(world.sloop(&["run", &ticket]).status.success());
    wait_until("the fake agent records its launch context", || {
        world.run_worktree(1).join("sloop-resolved.txt").is_file()
    });
    let worktree = world.run_worktree(1);

    let prompt = fs::read_to_string(worktree.join("prompt.txt")).unwrap();
    let bootstrap = include_str!("../src/worker-instructions.md").trim_ascii();
    assert_eq!(
        prompt,
        format!("{bootstrap}\n\nFollow the repository's launch-time instructions.\n")
    );
    let sloop_bin = fs::read_to_string(worktree.join("sloop-bin.txt")).unwrap();
    let resolved = fs::read_to_string(worktree.join("sloop-resolved.txt")).unwrap();
    assert!(Path::new(&sloop_bin).is_absolute(), "{sloop_bin}");
    assert_eq!(resolved.trim(), sloop_bin);
}

#[test]
fn daemon_rejects_a_target_without_the_prompt_placeholder() {
    let world = World::configured();
    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        "version: 1\nagent:\n  default_target: promptless\n  targets:\n    promptless:\n      cmd: [agent]\n",
    )
    .unwrap();

    let output = world.sloop(&["daemon"]);
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("agent.targets.promptless.cmd"), "{error}");
    assert!(error.contains("`{prompt}` exactly once"), "{error}");
}

#[test]
fn run_honors_a_custom_worktree_directory_end_to_end() {
    let world = World::configured();
    configure_fake_agent(&world, 1, false);
    let config_path = world.root().join(".agents/sloop/config.yaml");
    let config = fs::read_to_string(&config_path).unwrap();
    fs::write(
        &config_path,
        format!("worktree_dir: local/agent-worktrees\n{config}"),
    )
    .unwrap();
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_manual(&world, "custom-location.md", "# Custom location\n");

    assert!(world.sloop(&["run", &ticket]).status.success());

    let marker = |world: &World| {
        world
            .root()
            .join("local/agent-worktrees")
            .join(short_run_id(world, 1))
            .join("agent-ran.txt")
    };
    wait_until("the fake agent runs in the configured directory", || {
        marker(&world).is_file()
    });
    let marker = marker(&world);
    assert_eq!(fs::read_to_string(&marker).unwrap().trim(), ticket);
    assert!(is_git_worktree(marker.parent().unwrap()));
    assert!(!world.run_worktree(1).exists());
    assert!(!world.root().join(".sloop").exists());
}

#[test]
fn tickets_launch_the_command_for_their_snapshotted_target() {
    let world = World::configured();
    let first_script = world.root().join("first-agent.sh");
    let second_script = world.root().join("second-agent.sh");
    fs::write(
        &first_script,
        "#!/bin/sh\necho first > selected-target.txt\n",
    )
    .unwrap();
    fs::write(
        &second_script,
        "#!/bin/sh\necho second > selected-target.txt\n",
    )
    .unwrap();
    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        format!(
            "version: 1\nscheduler:\n  max_parallel_tasks: 1\nagent:\n  default_target: first\n  targets:\n    first:\n      cmd: [\"sh\", \"{}\", \"{{prompt}}\"]\n    second:\n      cmd: [\"sh\", \"{}\", \"{{prompt}}\"]\n",
            first_script.display(),
            second_script.display(),
        ),
    )
    .unwrap();
    world.commit_all("initial");
    world.start_daemon();

    let first = post_manual(&world, "first.md", "# Default target\n");
    let second = post_manual(
        &world,
        "second.md",
        "---\ntarget: second\n---\n# Explicit target\n",
    );
    assert!(world.sloop(&["run", &first]).status.success());
    wait_until("the default target command runs", || {
        world.run_worktree(1).join("selected-target.txt").is_file()
    });
    let first_marker = world.run_worktree(1).join("selected-target.txt");
    wait_until("the default target run settles", || {
        status(&world)["gate"]["active_agents"] == 0
    });

    assert!(world.sloop(&["run", &second]).status.success());
    wait_until("the explicit target command runs", || {
        world.run_worktree(2).join("selected-target.txt").is_file()
    });
    let second_marker = world.run_worktree(2).join("selected-target.txt");
    assert_eq!(fs::read_to_string(first_marker).unwrap().trim(), "first");
    assert_eq!(fs::read_to_string(second_marker).unwrap().trim(), "second");
}

#[test]
fn retry_requeues_a_failed_ticket_and_allows_another_run() {
    let world = World::configured();
    configure_failing_fake_agent(&world, 1, false);
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_manual(&world, "retry.md", "# Retry failed work\n");

    let conflict = world.sloop(&["retry", &ticket]);
    assert!(!conflict.status.success());
    assert_eq!(
        World::json_stdout_or_stderr(&conflict)["error"]["code"],
        "conflict"
    );

    assert!(world.sloop(&["run", &ticket]).status.success());
    wait_until("the first run fails", || {
        status(&world)["tickets"]["failed"] == 1
    });

    let retried = world.sloop(&["retry", &ticket]);
    assert!(
        retried.status.success(),
        "retry failed: {}",
        String::from_utf8_lossy(&retried.stderr)
    );
    let response = World::json_stdout(&retried);
    assert_eq!(response["data"]["ticket"], ticket);
    assert_eq!(response["data"]["previous_state"], "failed");
    assert_eq!(response["data"]["state"], "ready");

    assert!(world.sloop(&["run", &ticket]).status.success());
    wait_until("the retried ticket dispatches again", || {
        worktree_marker(&world, 2).is_file()
    });
}

#[test]
fn list_explains_paused_failed_held_and_claimed_tickets() {
    let world = World::configured();
    configure_failing_fake_agent(&world, 1, true);
    world.commit_all("initial");
    world.start_daemon();

    fs::write(world.root().join("release"), "go\n").unwrap();
    let failed = post_manual(&world, "failed.md", "# Failed\n");
    assert!(world.sloop(&["run", &failed]).status.success());
    wait_until("the first ticket fails", || {
        status(&world)["tickets"]["failed"] == 1
    });

    fs::remove_file(world.root().join("release")).unwrap();
    let claimed = post_manual(&world, "claimed.md", "# Claimed\n");
    assert!(world.sloop(&["run", &claimed]).status.success());
    wait_until("the claimed ticket starts running", || {
        status(&world)["gate"]["active_agents"] == 1
    });
    assert!(world.sloop(&["pause"]).status.success());
    let paused = post_manual(&world, "paused.md", "# Paused\n");
    let held_file = world.write_ticket("held.md", "# Held\n");
    let held_output = world.sloop(&["post", held_file.to_str().unwrap(), "--hold"]);
    assert!(held_output.status.success());
    let held = World::json_stdout(&held_output)["data"]["ticket"]["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let output = world.sloop(&["list"]);
    assert!(
        output.status.success(),
        "list failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let response = World::json_stdout(&output);
    let tickets = response["data"]["tickets"].as_array().unwrap();
    let row = |id: &str| tickets.iter().find(|ticket| ticket["id"] == id).unwrap();

    assert_eq!(
        row(&paused)["reason"],
        "scheduler is paused; resume with `sloop resume`"
    );
    assert_eq!(
        row(&failed)["reason"],
        "failed after 1 attempt(s); requeue with `sloop retry`"
    );
    assert_eq!(
        row(&held)["reason"],
        "held by operator; release with `sloop ready`"
    );
    assert_eq!(row(&paused)["name"], "paused");
    assert_eq!(row(&failed)["name"], "failed");
    assert_eq!(row(&held)["name"], "held");
    assert_eq!(row(&claimed)["name"], "claimed");
    assert_eq!(row(&claimed)["run"], world.run_alias(2));
    assert_eq!(
        row(&claimed)["reason"],
        format!("claimed by run {}", world.run_alias(2))
    );

    let human = world.sloop_plain(&["list"]);
    assert!(human.status.success());
    let human = String::from_utf8(human.stdout).unwrap();
    let human = human
        .lines()
        .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(human.contains(&format!(
        "{paused} ready (default) paused — scheduler is paused"
    )));
    assert!(human.contains(&format!(
        "{failed} failed (default) failed — failed after 1 attempt(s)"
    )));
    assert!(human.contains(&format!("{held} held (default) held — held by operator")));
    assert!(human.contains(&format!("{claimed} claimed (default) claimed")));
}

#[test]
fn list_orders_tickets_newest_first_and_honours_a_row_limit() {
    let world = World::configured();
    configure_fake_agent(&world, 1, false);
    world.commit_all("initial");
    world.start_daemon();

    // Registered oldest to newest, so `list` must return them reversed.
    let first = post_manual(&world, "first.md", "# First\n");
    let second = post_manual(&world, "second.md", "# Second\n");
    let third = post_manual(&world, "third.md", "# Third\n");

    let listed_ids = |args: &[&str]| -> Vec<String> {
        let output = world.sloop(args);
        assert!(
            output.status.success(),
            "{args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        World::json_stdout(&output)["data"]["tickets"]
            .as_array()
            .expect("tickets array")
            .iter()
            .map(|ticket| ticket["id"].as_str().expect("ticket id").to_owned())
            .collect()
    };

    assert_eq!(
        listed_ids(&["list"]),
        [third.clone(), second.clone(), first.clone()]
    );
    // Every spelling of the limit keeps the same newest-first prefix.
    for limited in [
        listed_ids(&["list", "-2"]),
        listed_ids(&["list", "--limit", "2"]),
        listed_ids(&["list", "-n", "2"]),
    ] {
        assert_eq!(limited, [third.clone(), second.clone()]);
    }
    assert_eq!(listed_ids(&["list", "-99"]).len(), 3);

    // Human output carries the same order and count as `--json`.
    let human = world.sloop_plain(&["list", "-2"]);
    assert!(human.status.success());
    let human = String::from_utf8(human.stdout).unwrap();
    let lines: Vec<&str> = human.lines().collect();
    assert_eq!(lines.len(), 2, "{human}");
    assert!(lines[0].starts_with(&third), "{human}");
    assert!(lines[1].starts_with(&second), "{human}");

    // A zero or non-numeric limit is a usage error, not an empty list.
    for arguments in [
        ["list", "-0"].as_slice(),
        ["list", "-abc"].as_slice(),
        ["list", "--limit", "abc"].as_slice(),
    ] {
        let output = world.sloop_plain(arguments);
        assert!(!output.status.success(), "{arguments:?} should have failed");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("For more information, try '--help'"),
            "{arguments:?} did not fail with a clap usage error: {stderr}"
        );
    }
}

#[test]
fn blocked_dependencies_are_reported_and_release_after_every_blocker_merges() {
    let world = World::configured();
    configure_fake_agent(&world, 1, false);
    world.commit_all("initial");
    world.start_daemon();

    let first = post_manual(&world, "first-blocker.md", "# First blocker\n");
    let second = post_manual(&world, "second-blocker.md", "# Second blocker\n");
    let dependent = post_manual_blocked(&world, "dependent.md", &[first.as_str(), second.as_str()]);
    assert!(world.sloop(&["hold", &first]).status.success());
    assert!(world.sloop(&["hold", &second]).status.success());

    // An unscoped activation has no dispatchable ticket while both blockers
    // are held, and a named activation cannot bypass the dependency gate.
    assert!(world.sloop(&["run"]).status.success());
    assert!(world.sloop(&["run", &dependent]).status.success());
    let snapshot = status(&world);
    assert_eq!(snapshot["gate"]["active_agents"], 0);
    assert_eq!(snapshot["tickets"]["held"], 2);
    assert_eq!(snapshot["tickets"]["ready"], 0);
    assert_eq!(snapshot["tickets"]["blocked"], 1);
    assert_eq!(snapshot["queued_activations"].as_array().unwrap().len(), 2);

    let listed = World::json_stdout(&world.sloop(&["list"]));
    let row = listed["data"]["tickets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|ticket| ticket["id"] == dependent)
        .unwrap();
    assert_eq!(row["state"], "blocked");
    assert_eq!(
        row["reason"],
        format!("blocked by unmerged {first}, {second}")
    );

    // The old unscoped activation takes the first released blocker.
    assert!(world.sloop(&["ready", &first]).status.success());
    wait_until("the first blocker merges", || {
        status(&world)["tickets"]["merged"] == 1
    });
    let listed = World::json_stdout(&world.sloop(&["list"]));
    let row = listed["data"]["tickets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|ticket| ticket["id"] == dependent)
        .unwrap();
    assert_eq!(row["reason"], format!("blocked by unmerged {second}"));

    assert!(world.sloop(&["ready", &second]).status.success());
    assert!(world.sloop(&["run", &second]).status.success());
    wait_until("the dependent runs after its last blocker merges", || {
        worktree_marker(&world, 3).is_file()
    });
    assert_eq!(
        fs::read_to_string(worktree_marker(&world, 3))
            .unwrap()
            .trim(),
        dependent
    );
    wait_until("all dependency-chain tickets merge", || {
        status(&world)["tickets"]["merged"] == 3
    });
}

#[test]
fn a_failed_blocker_keeps_its_dependent_blocked() {
    let world = World::configured();
    configure_failing_fake_agent(&world, 1, false);
    world.commit_all("initial");
    world.start_daemon();

    let blocker = post_manual(&world, "failing-blocker.md", "# Failing blocker\n");
    let dependent = post_manual_blocked(&world, "failed-dependent.md", &[blocker.as_str()]);
    assert!(world.sloop(&["run", &blocker]).status.success());
    wait_until("the blocker fails", || {
        status(&world)["tickets"]["failed"] == 1
    });

    assert!(world.sloop(&["run", &dependent]).status.success());
    let snapshot = status(&world);
    assert_eq!(snapshot["gate"]["active_agents"], 0);
    assert_eq!(snapshot["tickets"]["failed"], 1);
    assert_eq!(snapshot["tickets"]["blocked"], 1);
    assert_eq!(snapshot["queued_activations"].as_array().unwrap().len(), 1);
    assert!(!worktree_marker(&world, 2).exists());
}

#[test]
fn parallelism_never_exceeds_the_configured_capacity() {
    let world = World::configured();
    configure_fake_agent(&world, 1, true);
    world.commit_all("initial");
    world.start_daemon();
    let first = post_manual(&world, "first.md", "# First\n");
    let second = post_manual(&world, "second.md", "# Second\n");

    assert!(world.sloop(&["run", &first]).status.success());
    assert!(world.sloop(&["run", &second]).status.success());

    wait_until("the first agent starts", || {
        worktree_marker(&world, 1).is_file()
    });
    let data = status(&world);
    assert_eq!(data["gate"]["active_agents"], 1);
    assert_eq!(data["runs"].as_array().unwrap().len(), 1);
    assert_eq!(data["queued_activations"].as_array().unwrap().len(), 1);
    assert!(
        !world.run_worktree(2).exists(),
        "the second run spawned past the capacity gate"
    );

    fs::write(world.root().join("release"), "go\n").unwrap();
    wait_until("the second agent runs after capacity frees", || {
        worktree_marker(&world, 2).is_file()
    });
    wait_until("both runs finish", || {
        status(&world)["gate"]["active_agents"] == 0
    });
}

#[test]
fn pause_gates_the_queue_survives_restart_and_resume_drains_it() {
    let world = World::configured();
    configure_fake_agent(&world, 1, true);
    let first = world.write_ticket("first.md", "# First\n");
    let second = world.write_ticket("second.md", "# Second\n");
    world.commit_all("initial");
    world.start_daemon();

    for ticket in [&first, &second] {
        let posted = world.sloop(&["post", ticket.to_str().unwrap(), "--auto"]);
        assert!(
            posted.status.success(),
            "post failed: {}",
            String::from_utf8_lossy(&posted.stderr)
        );
    }
    wait_until("the first agent starts", || {
        worktree_marker(&world, 1).is_file()
    });

    let paused = world.sloop(&["pause"]);
    assert!(paused.status.success());
    assert_eq!(World::json_stdout(&paused)["data"]["paused"], true);

    fs::write(world.root().join("release"), "go\n").unwrap();
    wait_until("the first agent finishes while paused", || {
        status(&world)["gate"]["active_agents"] == 0
    });
    for _ in 0..3 {
        let data = status(&world);
        assert_eq!(data["daemon"]["paused"], true);
        assert_eq!(data["gate"]["active_agents"], 0);
        assert_eq!(data["queued_activations"].as_array().unwrap().len(), 1);
        assert!(
            !worktree_marker(&world, 2).exists(),
            "the second ticket started while paused"
        );
    }

    let pid = status(&world)["daemon"]["pid"].as_u64().unwrap() as u32;
    assert!(world.sloop(&["stop"]).status.success());
    wait_until("the paused daemon stops", || !process_alive(pid));

    let restarted = status(&world);
    assert_eq!(restarted["daemon"]["paused"], true);
    assert_eq!(restarted["gate"]["active_agents"], 0);
    assert!(!worktree_marker(&world, 2).exists());

    let resumed = world.sloop(&["resume"]);
    assert!(resumed.status.success());
    assert_eq!(World::json_stdout(&resumed)["data"]["paused"], false);
    wait_until("the second agent starts after resume", || {
        worktree_marker(&world, 2).is_file()
    });
}

#[test]
fn a_project_scoped_run_selects_only_that_projects_tickets() {
    let world = World::configured();
    configure_fake_agent(&world, 2, false);
    fs::write(
        world.root().join(".agents/sloop/projects/backend.md"),
        "---\nid: backend\ntitle: Backend\n---\n",
    )
    .unwrap();
    world.commit_all("initial");
    world.start_daemon();

    let other = post_manual(&world, "elsewhere.md", "# Default project work\n");
    let ticket = world.write_ticket("scoped.md", "# Backend work\n");
    let output = world.sloop(&[
        "post",
        ticket.to_str().unwrap(),
        "--project",
        "backend",
        "--manual",
    ]);
    assert!(output.status.success());
    let scoped = World::json_stdout(&output)["data"]["ticket"]["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let output = world.sloop(&["run", "--project", "backend"]);
    assert!(output.status.success());
    assert_eq!(
        World::json_stdout(&output)["data"]["activation"]["project"],
        "backend"
    );

    let marker = worktree_marker(&world, 1);
    wait_until("the scoped agent runs", || marker.is_file());
    assert_eq!(fs::read_to_string(&marker).unwrap().trim(), scoped);

    wait_until("the scoped run finishes", || {
        status(&world)["gate"]["active_agents"] == 0
    });
    let data = status(&world);
    assert_eq!(data["tickets"]["ready"], 1, "{other} must stay untouched");
    assert!(!world.run_worktree(2).exists());
}

#[test]
fn a_held_ticket_rejects_named_runs_until_an_operator_releases_it() {
    let world = World::configured();
    configure_fake_agent(&world, 1, false);
    world.commit_all("initial");
    world.start_daemon();
    let file = world.write_ticket("later.md", "# Later\n");
    let posted = world.sloop(&["post", file.to_str().unwrap(), "--hold"]);
    assert!(posted.status.success());
    let ticket = World::json_stdout(&posted)["data"]["ticket"]["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let rejected = world.sloop(&["run", &ticket]);
    assert!(!rejected.status.success());
    let error: serde_json::Value =
        serde_json::from_slice(&rejected.stderr).expect("stderr is JSON");
    assert_eq!(error["error"]["code"], "conflict");
    assert_eq!(status(&world)["tickets"]["held"], 1);

    let released = world.sloop(&["ready", &ticket]);
    assert!(released.status.success());
    let released = World::json_stdout(&released);
    assert_eq!(released["data"]["previous_state"], "held", "{released}");
    let repeated = world.sloop(&["ready", &ticket]);
    assert!(repeated.status.success());
    assert_eq!(World::json_stdout(&repeated)["data"]["overridden"], false);
    assert!(!world.run_worktree(1).exists());

    assert!(world.sloop(&["run", &ticket]).status.success());
    wait_until("released ticket runs", || {
        worktree_marker(&world, 1).is_file()
    });
    wait_until("the ticket reaches its derived outcome", || {
        status(&world)["tickets"]["merged"] == 1
    });
    let rejected = world.sloop(&["hold", &ticket]);
    assert!(!rejected.status.success());
    let error: serde_json::Value =
        serde_json::from_slice(&rejected.stderr).expect("stderr is JSON");
    assert_eq!(error["error"]["code"], "conflict");
    assert_eq!(status(&world)["tickets"]["merged"], 1);
}

#[test]
fn running_hours_hold_queued_work_until_the_opening_boundary() {
    let world = World::configured();
    let current = SystemClock.local_minute(world.now_ms());
    let start = (current + 2) % (24 * 60);
    let end = (start + 2) % (24 * 60);
    configure_fake_agent_with_hours(&world, 1, false, Some((start, end)));
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_manual(&world, "scheduled.md", "# Scheduled\n");

    assert!(world.sloop(&["run", &ticket]).status.success());
    let data = status(&world);
    assert_eq!(data["gate"]["running_hours"]["open"], false);
    assert!(data["next_wake"].is_string());
    assert!(!worktree_marker(&world, 1).exists());

    world.tick(Duration::from_secs(2 * 60));
    wait_until("the opening boundary wakes the dispatcher", || {
        worktree_marker(&world, 1).is_file()
    });
}

#[test]
fn overnight_dispatches_once_inside_the_window() {
    let world = World::configured();
    let current = SystemClock.local_minute(world.now_ms());
    let start = (current + 2) % (24 * 60);
    let end = (start + 4) % (24 * 60);
    configure_fake_agent_with_hours(&world, 2, true, Some((start, end)));
    world.commit_all("initial");
    world.start_daemon();
    let first = post_manual(&world, "overnight-first.md", "# Overnight first\n");
    let second = post_manual(&world, "overnight-second.md", "# Overnight second\n");

    let output = world.sloop(&["run", "--overnight", "--only", &format!("{first},{second}")]);
    assert!(output.status.success());
    assert_eq!(
        World::json_stdout(&output)["data"]["activation"]["kind"],
        "overnight"
    );
    assert!(!worktree_marker(&world, 1).exists());

    world.tick(Duration::from_secs(2 * 60));
    wait_until("overnight work starts after the window opens", || {
        worktree_marker(&world, 1).is_file()
    });
    assert_eq!(
        fs::read_to_string(worktree_marker(&world, 1))
            .unwrap()
            .trim(),
        first
    );
    assert!(!worktree_marker(&world, 2).exists());

    fs::write(world.root().join("release"), "go\n").unwrap();
    wait_until("the overnight run settles", || {
        status(&world)["gate"]["active_agents"] == 0
    });
    world.tick(Duration::from_secs(60));
    assert!(!worktree_marker(&world, 2).exists());
    assert_eq!(status(&world)["tickets"]["ready"], 1);
}

#[test]
fn overnight_without_running_hours_dispatches_immediately() {
    let world = World::configured();
    configure_fake_agent(&world, 1, false);
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_manual(&world, "overnight-now.md", "# Overnight now\n");

    let output = world.sloop(&["run", &ticket, "--overnight"]);
    assert!(output.status.success());
    wait_until("overnight work starts without configured hours", || {
        worktree_marker(&world, 1).is_file()
    });
}

#[test]
fn every_waits_for_the_window_rearms_and_dispatches_again() {
    let world = World::configured();
    let current = SystemClock.local_minute(world.now_ms());
    let start = (current + 5) % (24 * 60);
    let end = (start + 10) % (24 * 60);
    configure_fake_agent_with_hours(&world, 2, true, Some((start, end)));
    world.commit_all("initial");
    world.start_daemon();
    let first = post_manual(&world, "every-first.md", "# Every first\n");
    let second = post_manual(&world, "every-second.md", "# Every second\n");

    let output = world.sloop(&[
        "run",
        "--every",
        "2m",
        "--only",
        &format!("{first},{second}"),
    ]);
    assert!(output.status.success());
    let response = World::json_stdout(&output);
    assert_eq!(response["data"]["activation"]["kind"], "every");
    assert_eq!(response["data"]["activation"]["interval_ms"], 120_000);

    world.tick(Duration::from_secs(2 * 60));
    assert!(!worktree_marker(&world, 1).exists());
    world.tick(Duration::from_secs(3 * 60));
    wait_until("the overdue recurring run starts at opening", || {
        worktree_marker(&world, 1).is_file()
    });
    assert_eq!(
        fs::read_to_string(worktree_marker(&world, 1))
            .unwrap()
            .trim(),
        first
    );
    assert!(!worktree_marker(&world, 2).exists());

    fs::write(world.root().join("release"), "go\n").unwrap();
    wait_until("the first recurring run settles", || {
        status(&world)["gate"]["active_agents"] == 0
    });
    assert!(!worktree_marker(&world, 2).exists());

    // The original two-minute cadence makes the next slot one minute after
    // this delayed dispatch, rather than immediately or two minutes from now.
    world.tick(Duration::from_secs(60));
    wait_until("the rearmed recurring activation dispatches again", || {
        worktree_marker(&world, 2).is_file()
    });
    assert_eq!(
        fs::read_to_string(worktree_marker(&world, 2))
            .unwrap()
            .trim(),
        second
    );
}

fn local_time_after(world: &World, minutes: u16) -> String {
    let target = (SystemClock.local_minute(world.now_ms()) + minutes) % (24 * 60);
    format!("{:02}:{:02}", target / 60, target % 60)
}

#[test]
fn at_dispatches_only_once_its_scheduled_time_passes() {
    let world = World::configured();
    configure_fake_agent(&world, 1, false);
    world.commit_all("initial");
    world.start_daemon();
    let ticket = world.write_ticket("timed.md", "# Timed\n");
    let at = local_time_after(&world, 2);

    let output = world.sloop(&[
        "post",
        ticket.to_str().expect("UTF-8 ticket path"),
        "--at",
        &at,
    ]);
    assert!(
        output.status.success(),
        "post --at failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        World::json_stdout(&output)["data"]["activation"]["kind"],
        "at"
    );
    assert!(
        status(&world)["next_wake"].is_string(),
        "the dispatcher schedules a deadline instead of polling"
    );

    world.tick(Duration::from_secs(60));
    assert!(!worktree_marker(&world, 1).exists());

    world.tick(Duration::from_secs(2 * 60));
    wait_until("the timed activation dispatches once due", || {
        worktree_marker(&world, 1).is_file()
    });
}

#[test]
fn at_outside_running_hours_waits_for_the_window() {
    let world = World::configured();
    let current = SystemClock.local_minute(world.now_ms());
    let start = (current + 5) % (24 * 60);
    let end = (start + 5) % (24 * 60);
    configure_fake_agent_with_hours(&world, 1, false, Some((start, end)));
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_manual(&world, "timed.md", "# Timed\n");
    let at = local_time_after(&world, 2);

    let output = world.sloop(&["run", &ticket, "--at", &at]);
    assert!(
        output.status.success(),
        "run --at failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    world.tick(Duration::from_secs(3 * 60));
    assert_eq!(status(&world)["gate"]["running_hours"]["open"], false);
    assert!(!worktree_marker(&world, 1).exists());

    world.tick(Duration::from_secs(3 * 60));
    wait_until("the due timed run starts at the opening boundary", || {
        worktree_marker(&world, 1).is_file()
    });
}

#[test]
fn closing_time_does_not_cancel_active_work_or_start_the_next_run() {
    let world = World::configured();
    let current = SystemClock.local_minute(world.now_ms());
    let end = (current + 1) % (24 * 60);
    configure_fake_agent_with_hours(&world, 1, true, Some((current, end)));
    world.commit_all("initial");
    world.start_daemon();
    let first = post_manual(&world, "first.md", "# First\n");
    let second = post_manual(&world, "second.md", "# Second\n");

    assert!(world.sloop(&["run", &first]).status.success());
    assert!(world.sloop(&["run", &second]).status.success());
    wait_until("the first run starts", || {
        worktree_marker(&world, 1).is_file()
    });
    let rejected = world.sloop(&["hold", &first]);
    assert!(!rejected.status.success());
    let error: serde_json::Value =
        serde_json::from_slice(&rejected.stderr).expect("stderr is JSON");
    assert_eq!(error["error"]["code"], "conflict");

    world.tick(Duration::from_secs(60));
    fs::write(world.root().join("release"), "go\n").unwrap();
    wait_until("the active run finishes after closing", || {
        status(&world)["gate"]["active_agents"] == 0
    });
    let data = status(&world);
    assert_eq!(data["gate"]["running_hours"]["open"], false);
    assert_eq!(data["queued_activations"].as_array().unwrap().len(), 1);
    assert!(!worktree_marker(&world, 2).exists());
}

#[test]
fn hold_then_ready_round_trips_an_auto_activation_without_dispatching_while_held() {
    let world = World::configured();
    let current = SystemClock.local_minute(world.now_ms());
    let start = (current + 2) % (24 * 60);
    let end = (start + 2) % (24 * 60);
    configure_fake_agent_with_hours(&world, 1, false, Some((start, end)));
    world.commit_all("initial");
    world.start_daemon();
    let file = world.write_ticket("suspended.md", "# Suspended\n");
    let posted = world.sloop(&["post", file.to_str().unwrap(), "--auto"]);
    assert!(posted.status.success());
    let ticket = World::json_stdout(&posted)["data"]["ticket"]["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let held = world.sloop(&["hold", &ticket]);
    assert!(held.status.success());
    assert_eq!(World::json_stdout(&held)["data"]["previous_state"], "ready");
    assert_eq!(World::json_stdout(&held)["data"]["state"], "held");

    world.tick(Duration::from_secs(2 * 60));
    wait_until("running-hours gate opens", || {
        status(&world)["gate"]["running_hours"]["open"] == true
    });
    assert!(!worktree_marker(&world, 1).exists());
    assert_eq!(
        status(&world)["queued_activations"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let ready = world.sloop(&["ready", &ticket]);
    assert!(ready.status.success());
    assert_eq!(World::json_stdout(&ready)["data"]["previous_state"], "held");
    assert_eq!(World::json_stdout(&ready)["data"]["state"], "ready");
    wait_until("released activation runs", || {
        worktree_marker(&world, 1).is_file()
    });
}

#[test]
fn the_daemon_records_its_identity_in_the_lockfile() {
    let world = World::configured();
    world.commit_all("seed");
    let response = world.start_daemon();
    let pid = response["data"]["pid"].as_u64().expect("daemon pid") as u32;

    let lock_path = world.lock_path();
    let socket = world.operator_socket();
    wait_until("the lockfile records the daemon identity", || {
        sloop::daemon::read_lock_identity(&lock_path).is_some_and(|identity| {
            identity.pid == pid && identity.socket.as_deref() == Some(socket.as_path())
        })
    });
}

#[test]
fn restart_readopts_a_matching_live_process_until_it_exits() {
    let world = World::configured();
    world.configure_fake_agent(
        FakeAgent::new()
            .block_until_released("recovery")
            .note("recovered worker socket"),
    );
    let ticket = world.write_ticket("recovery.md", "# Recovery\nwork\n");
    world.commit_all("seed");
    let daemon_pid = world.start_daemon()["data"]["pid"]
        .as_u64()
        .expect("daemon pid") as u32;
    let posted = world.sloop(&["post", ticket.to_str().unwrap(), "--auto"]);
    assert!(posted.status.success());
    wait_until("the agent reaches its blocking point", || {
        world.fake_agent_reached("recovery")
    });
    let agent_pid = world.run_process_id(&world.run_id(1));

    world.kill_daemon(daemon_pid);
    assert!(
        process_alive(agent_pid),
        "the agent survives its supervisor"
    );

    let alternate_runtime = tempfile::tempdir().expect("create alternate runtime");
    let restarted = world.sloop_with_runtime(&["daemon"], alternate_runtime.path());
    assert!(restarted.status.success());
    wait_until("the restarted daemon adopts the run", || {
        let snapshot = status(&world);
        snapshot["gate"]["active_agents"] == 1 && snapshot["runs"][0]["id"] == world.run_id(1)
    });

    world.release("recovery");
    wait_until("the recovered run is settled", || {
        let snapshot = status(&world);
        snapshot["gate"]["active_agents"] == 0
            && snapshot["runs"].as_array().is_some_and(Vec::is_empty)
            && snapshot["tickets"]["ready"] == 1
    });
    assert_eq!(world.run_note_count(&world.run_id(1)), 1);
    let run_id = world.run_id(1);
    assert_eq!(world.show_snapshot(&run_id)["state"], "orphaned");
}

#[test]
fn restart_does_not_orphan_a_live_process_with_unverifiable_identity() {
    let world = World::configured();
    world.configure_fake_agent(FakeAgent::new().block_until_released("unverifiable-recovery"));
    let ticket = world.write_ticket("unverifiable-recovery.md", "# Recovery\nwork\n");
    world.commit_all("seed");
    let daemon_pid = world.start_daemon()["data"]["pid"]
        .as_u64()
        .expect("daemon pid") as u32;
    let posted = world.sloop(&["post", ticket.to_str().unwrap(), "--auto"]);
    assert!(posted.status.success());
    wait_until("the agent reaches its blocking point", || {
        world.fake_agent_reached("unverifiable-recovery")
    });
    let agent_pid = world.run_process_id(&world.run_id(1));
    rusqlite::Connection::open(world.db_path())
        .expect("open state database")
        .execute(
            "UPDATE runs SET pid_start_time = NULL WHERE id = ?1",
            [world.run_id(1)],
        )
        .expect("make process identity unverifiable");

    world.kill_daemon(daemon_pid);
    world.start_daemon();

    let deadline = Instant::now() + Duration::from_millis(2_200);
    while Instant::now() < deadline {
        assert!(process_alive(agent_pid));
        let snapshot = status(&world);
        assert_eq!(snapshot["gate"]["active_agents"], 1);
        assert_eq!(snapshot["runs"][0]["state"], "running");
        std::thread::sleep(Duration::from_millis(50));
    }

    world.release("unverifiable-recovery");
    wait_until("the recovered run is settled", || {
        let snapshot = status(&world);
        snapshot["gate"]["active_agents"] == 0
            && snapshot["runs"].as_array().is_some_and(Vec::is_empty)
            && snapshot["tickets"]["ready"] == 1
    });
}

#[test]
fn restart_orphans_a_dead_process_without_using_commits_as_a_verdict() {
    let world = World::configured();
    world.configure_fake_agent(
        FakeAgent::new()
            .commit("recovered work")
            .block_until_released("committed"),
    );
    let ticket = world.write_ticket("committed.md", "# Committed\nwork\n");
    world.commit_all("seed");
    let daemon_pid = world.start_daemon()["data"]["pid"]
        .as_u64()
        .expect("daemon pid") as u32;
    let posted = world.sloop(&["post", ticket.to_str().unwrap(), "--auto"]);
    assert!(posted.status.success());
    wait_until("the agent commits before the crash", || {
        world.fake_agent_reached("committed")
    });
    let agent_pid = world.run_process_id(&world.run_id(1));

    world.kill_daemon(daemon_pid);
    world.kill_process_group(agent_pid);
    world.start_daemon();

    wait_until("the dead run is classified", || {
        let snapshot = status(&world);
        snapshot["gate"]["active_agents"] == 0 && snapshot["tickets"]["ready"] == 1
    });
    let run_id = world.run_id(1);
    assert_eq!(world.show_snapshot(&run_id)["state"], "orphaned");
}

#[test]
fn periodic_reconciliation_orphans_a_dead_agent_without_restarting_the_daemon() {
    let world = World::configured();
    world.configure_fake_agent(FakeAgent::new().block_until_released("periodic-recovery"));
    assert_periodic_dead_agent_is_orphaned(&world, "periodic-recovery.md", "periodic-recovery");
}

#[test]
fn periodic_reconciliation_does_not_use_commits_as_a_dead_run_verdict() {
    let world = World::configured();
    world.configure_fake_agent(
        FakeAgent::new()
            .commit("work before death")
            .block_until_released("periodic-commit-recovery"),
    );
    assert_periodic_dead_agent_is_orphaned(
        &world,
        "periodic-commit-recovery.md",
        "periodic-commit-recovery",
    );
}

#[test]
fn periodic_reconciliation_leaves_a_healthy_live_agent_untouched() {
    let world = World::configured();
    world.configure_fake_agent(FakeAgent::new().block_until_released("healthy-periodic"));
    let ticket = world.write_ticket("healthy-periodic.md", "# Keep live agent\nwork\n");
    world.commit_all("seed");
    world.start_daemon();
    let posted = world.sloop(&["post", ticket.to_str().unwrap(), "--auto"]);
    assert!(posted.status.success());
    wait_until("the healthy agent reaches its blocking point", || {
        world.fake_agent_reached("healthy-periodic")
    });
    let agent_pid = world.run_process_id(&world.run_id(1));

    // Observe continuously across two liveness intervals rather than sleeping
    // and checking only the final state.
    let deadline = Instant::now() + Duration::from_millis(4_200);
    while Instant::now() < deadline {
        assert!(process_alive(agent_pid));
        let snapshot = status(&world);
        assert_eq!(snapshot["gate"]["active_agents"], 1);
        assert_eq!(snapshot["runs"][0]["state"], "running");
        std::thread::sleep(Duration::from_millis(50));
    }

    world.release("healthy-periodic");
    wait_until("the healthy agent settles normally", || {
        status(&world)["tickets"]["merged"] == 1
    });
}

#[test]
fn periodic_reconciliation_treats_a_mismatched_start_time_as_pid_reuse() {
    let world = World::configured();
    world.configure_fake_agent(FakeAgent::new().block_until_released("pid-reuse"));
    let ticket = world.write_ticket("pid-reuse.md", "# Detect PID reuse\nwork\n");
    world.commit_all("seed");
    world.start_daemon();
    let posted = world.sloop(&["post", ticket.to_str().unwrap(), "--auto"]);
    assert!(posted.status.success());
    wait_until("the agent reaches its blocking point", || {
        world.fake_agent_reached("pid-reuse")
    });
    let agent_pid = world.run_process_id(&world.run_id(1));
    let connection = rusqlite::Connection::open(world.db_path()).expect("open state database");
    connection
        .execute(
            "UPDATE runs SET pid_start_time = pid_start_time + 1 WHERE id = ?1",
            [world.run_id(1)],
        )
        .expect("fabricate reused PID identity");

    wait_until_slow("the mismatched PID identity is recovered", || {
        let snapshot = status(&world);
        snapshot["gate"]["active_agents"] == 0
            && snapshot["tickets"]["ready"] == 1
            && snapshot["runs"].as_array().is_some_and(Vec::is_empty)
    });
    assert!(
        process_alive(agent_pid),
        "reconciliation must not signal a process whose identity mismatches"
    );
    world.kill_process_group(agent_pid);
}

#[test]
fn durable_capacity_is_repaired_before_another_agent_can_spawn() {
    let world = World::configured();
    world.configure_fake_agent(FakeAgent::new().block_until_released("unexpected-spawn"));
    let leased_path = world.write_ticket("leased.md", "# Durable lease\nwork\n");
    let candidate_path = world.write_ticket("candidate.md", "# Candidate\nwork\n");
    world.commit_all("seed");
    let daemon_pid = world.start_daemon()["data"]["pid"]
        .as_u64()
        .expect("daemon pid") as u32;
    let leased = world.sloop(&["post", leased_path.to_str().unwrap(), "--manual"]);
    let candidate = world.sloop(&["post", candidate_path.to_str().unwrap(), "--manual"]);
    assert!(leased.status.success());
    assert!(candidate.status.success());
    let leased_id = World::json_stdout(&leased)["data"]["ticket"]["id"]
        .as_str()
        .expect("leased ticket id")
        .to_owned();
    let candidate_id = World::json_stdout(&candidate)["data"]["ticket"]["id"]
        .as_str()
        .expect("candidate ticket id")
        .to_owned();

    assert!(world.sloop(&["pause"]).status.success());
    assert!(world.sloop(&["run", &candidate_id]).status.success());

    let mut connection = rusqlite::Connection::open(world.db_path()).expect("open state database");
    let transaction = connection.transaction().expect("begin capacity fixture");
    transaction
        .execute(
            "UPDATE tickets SET state = 'claimed', attempts = attempts + 1 WHERE id = ?1",
            [&leased_id],
        )
        .expect("claim durable ticket");
    transaction
        .execute(
            "INSERT INTO activations
                 (id, kind, state, ticket_id, created_at_ms, updated_at_ms)
             VALUES ('A-capacity', 'auto', 'completed', ?1, 1, 1)",
            [&leased_id],
        )
        .expect("insert durable activation");
    transaction
        .execute(
            "INSERT INTO runs
                 (id, activation_id, ticket_id, state, attempt, pid, created_at_ms, updated_at_ms)
             VALUES ('R-capacity', 'A-capacity', ?1, 'running', 1, ?2, 1, 1)",
            rusqlite::params![leased_id, i64::from(daemon_pid)],
        )
        .expect("insert durable run");
    transaction
        .execute(
            "INSERT INTO leases
                 (ticket_id, run_id, owner_id, acquired_at_ms, renewed_at_ms, expires_at_ms)
             VALUES (?1, 'R-capacity', 'lost-dispatcher-state', 1, 1, 9999999999999)",
            [&leased_id],
        )
        .expect("insert durable lease");
    transaction.commit().expect("commit capacity fixture");

    world.arm_test_hook("before-spawn-capacity-reconciliation");
    let resume = world.spawn_sloop(&["resume"]);
    wait_until("the spawn path checks durable capacity", || {
        world.test_hook_reached("before-spawn-capacity-reconciliation")
    });
    let candidate_runs: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM runs WHERE ticket_id = ?1",
            [&candidate_id],
            |row| row.get(0),
        )
        .expect("count candidate runs");
    assert_eq!(candidate_runs, 0);

    world.release_test_hook("before-spawn-capacity-reconciliation");
    let resumed = resume.wait_with_output().expect("wait for resume");
    assert!(resumed.status.success());
    wait_until("durable capacity becomes authoritative", || {
        let snapshot = status(&world);
        snapshot["gate"]["active_agents"] == 1
            && snapshot["runs"]
                .as_array()
                .is_some_and(|runs| runs.len() == 1 && runs[0]["id"] == "R-capacity")
    });
    assert!(!world.fake_agent_reached("unexpected-spawn"));
}

#[test]
fn periodic_reconciliation_does_not_duplicate_supervisor_aftercare() {
    let world = World::configured();
    world.configure_fake_agent(FakeAgent::new());
    let ticket = world.write_ticket("single-aftercare.md", "# Single aftercare\nwork\n");
    world.commit_all("seed");
    world.arm_test_hook("after-agent-exit-checkpoint");
    world.start_daemon();
    let posted = world.sloop(&["post", ticket.to_str().unwrap(), "--auto"]);
    assert!(posted.status.success());
    wait_until("the supervisor owns the exit handoff", || {
        world.test_hook_reached("after-agent-exit-checkpoint")
    });

    world.arm_test_hook("after-run-liveness-reconciliation");
    wait_until_slow("reconciliation observes supervisor-owned aftercare", || {
        world.test_hook_reached("after-run-liveness-reconciliation")
    });
    world.release_test_hook("after-run-liveness-reconciliation");
    world.release_test_hook("after-agent-exit-checkpoint");
    wait_until("the run settles once", || {
        status(&world)["tickets"]["merged"] == 1
    });

    let connection = rusqlite::Connection::open(world.db_path()).expect("open state database");
    let stage_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM aftercare_stages WHERE run_id = ?1",
            [world.run_id(1)],
            |row| row.get(0),
        )
        .expect("count aftercare stages");
    assert_eq!(stage_count, 2);
    let exit_evidence_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM run_evidence
             WHERE run_id = ?1 AND kind = 'exit_classified'",
            [world.run_id(1)],
            |row| row.get(0),
        )
        .expect("count exit evidence");
    assert_eq!(exit_evidence_count, 1);
}

/// A committing agent plus a flow whose exec stage always fails: the run halts
/// at `needs_review` with the agent's commit preserved on its run branch, ready
/// for an operator to merge by hand.
fn configure_review_agent(world: &World) {
    fs::create_dir_all(world.root().join(".agents/sloop/flows")).unwrap();
    fs::write(
        world.root().join(".agents/sloop/flows/default.yaml"),
        "stages:\n  - { name: build, kind: agent, verdict: exit }\n  - { name: reject, kind: exec, cmd: ['false'] }\n  - { name: merge, kind: merge }\n",
    )
    .unwrap();
    let script = world.root().join("fake-agent.sh");
    fs::write(
        &script,
        "#!/bin/sh\necho \"$SLOOP_TICKET_ID\" > agent-ran.txt\necho work > work.txt\ngit add work.txt\ngit -c user.name=agent -c user.email=agent@example.invalid commit --quiet -m 'agent work'\nexit 0\n",
    )
    .expect("write committing agent script");
    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        format!(
            "version: 1\nscheduler:\n  max_parallel_tasks: 1\nagent:\n  default_target: fake\n  targets:\n    fake:\n      cmd: [\"sh\", \"{}\", \"{{prompt}}\"]\n",
            script.display()
        ),
    )
    .expect("write agent config");
}

fn git_root(world: &World, args: &[&str]) -> std::process::Output {
    Command::new("git")
        .args(args)
        .current_dir(world.root())
        .output()
        .expect("run git in root")
}

/// Advances the default branch with an unrelated commit, so a later hand-merge
/// of a diverged run branch produces a merge commit (a strict ancestor) rather
/// than a fast-forward — the shape the operator gets once other runs have
/// already moved the default branch.
fn advance_default_branch(world: &World, marker: &str) {
    fs::write(world.root().join(marker), b"advance\n").unwrap();
    assert!(git_root(world, &["add", marker]).status.success());
    assert!(
        git_root(
            world,
            &[
                "-c",
                "user.name=operator",
                "-c",
                "user.email=operator@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "advance default branch",
            ],
        )
        .status
        .success()
    );
}

fn external_merge_count(world: &World) -> i64 {
    let connection = rusqlite::Connection::open(world.db_path()).expect("open state database");
    connection
        .query_row(
            "SELECT COUNT(*) FROM run_evidence WHERE kind = 'external_merge_observed'",
            [],
            |row| row.get(0),
        )
        .expect("count external merge evidence")
}

#[test]
fn a_hand_merged_review_branch_settles_to_merged_and_releases_its_dependent() {
    let world = World::configured();
    configure_review_agent(&world);
    world.commit_all("initial");
    world.start_daemon();

    let blocker = post_manual(&world, "review-blocker.md", "# Review blocker\n");
    let dependent = post_manual_blocked(&world, "review-dependent.md", &[blocker.as_str()]);
    assert!(world.sloop(&["run", &blocker]).status.success());
    wait_until("the blocker halts at needs_review", || {
        status(&world)["tickets"]["needs_review"] == 1
    });
    let snapshot = status(&world);
    assert_eq!(snapshot["tickets"]["blocked"], 1);
    assert_eq!(snapshot["tickets"]["ready"], 0);

    // An operator reviews and merges the preserved run branch by hand.
    advance_default_branch(&world, "unrelated.txt");
    let branch = format!("sloop/{blocker}-a1-{}", short_run_id(&world, 1));
    let merged = git_root(
        &world,
        &[
            "-c",
            "user.name=operator",
            "-c",
            "user.email=operator@example.invalid",
            "merge",
            "--no-ff",
            "-m",
            "operator merges the run branch",
            &branch,
        ],
    );
    assert!(
        merged.status.success(),
        "hand merge failed: {}",
        String::from_utf8_lossy(&merged.stderr)
    );

    // The running daemon notices within one reconciliation interval; polling
    // `status` drives those passes without a reindex or a restart.
    wait_until_slow("the daemon reconciles the external merge", || {
        status(&world)["tickets"]["merged"] == 1
    });
    let snapshot = status(&world);
    assert_eq!(snapshot["tickets"]["needs_review"], 0);
    assert_eq!(
        snapshot["tickets"]["blocked"], 0,
        "the dependent releases once its blocker merges"
    );
    assert_eq!(snapshot["tickets"]["ready"], 1);

    let listed = World::json_stdout(&world.sloop(&["list"]));
    let row = listed["data"]["tickets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|ticket| ticket["id"] == dependent)
        .unwrap();
    assert_ne!(row["state"], "blocked");
    assert_eq!(external_merge_count(&world), 1);
}

#[test]
fn a_squash_merged_review_branch_stays_in_needs_review() {
    let world = World::configured();
    configure_review_agent(&world);
    world.commit_all("initial");
    world.start_daemon();

    let blocker = post_manual(&world, "squash-blocker.md", "# Squash blocker\n");
    assert!(world.sloop(&["run", &blocker]).status.success());
    wait_until("the blocker halts at needs_review", || {
        status(&world)["tickets"]["needs_review"] == 1
    });

    // A squash-merge rewrites the commits, so the run branch tip is not an
    // ancestor of the default branch and ancestry cannot prove integration.
    let branch = format!("sloop/{blocker}-a1-{}", short_run_id(&world, 1));
    assert!(
        git_root(&world, &["merge", "--squash", &branch])
            .status
            .success()
    );
    assert!(
        git_root(
            &world,
            &[
                "-c",
                "user.name=operator",
                "-c",
                "user.email=operator@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "squash the run branch",
            ],
        )
        .status
        .success()
    );

    // Drive several reconciliation passes; the ticket must stay for review.
    for _ in 0..4 {
        let snapshot = status(&world);
        assert_eq!(snapshot["tickets"]["needs_review"], 1);
        assert_eq!(snapshot["tickets"]["merged"], 0);
    }
    assert_eq!(external_merge_count(&world), 0);
}

#[test]
fn a_deleted_review_branch_leaves_the_ticket_and_daemon_untouched() {
    let world = World::configured();
    configure_review_agent(&world);
    world.commit_all("initial");
    world.start_daemon();

    let blocker = post_manual(&world, "deleted-blocker.md", "# Deleted blocker\n");
    assert!(world.sloop(&["run", &blocker]).status.success());
    wait_until("the blocker halts at needs_review", || {
        status(&world)["tickets"]["needs_review"] == 1
    });

    // The operator discards the run branch entirely. Its worktree must be
    // released first, since Git refuses to delete a checked-out branch.
    let worktree = world.run_worktree(1);
    assert!(
        git_root(
            &world,
            &["worktree", "remove", "--force", &worktree.to_string_lossy()],
        )
        .status
        .success()
    );
    let branch = format!("sloop/{blocker}-a1-{}", short_run_id(&world, 1));
    assert!(
        git_root(&world, &["branch", "-D", &branch])
            .status
            .success()
    );

    // An unresolvable ref is not evidence: the ticket stays and the daemon
    // keeps answering.
    for _ in 0..4 {
        let snapshot = status(&world);
        assert_eq!(snapshot["tickets"]["needs_review"], 1);
        assert_eq!(snapshot["tickets"]["merged"], 0);
    }
    assert_eq!(external_merge_count(&world), 0);
}

#[test]
fn external_merge_reconciliation_survives_a_restart_without_duplicating_evidence() {
    let world = World::configured();
    configure_review_agent(&world);
    world.commit_all("initial");
    world.start_daemon();

    let blocker = post_manual(&world, "restart-blocker.md", "# Restart blocker\n");
    assert!(world.sloop(&["run", &blocker]).status.success());
    wait_until("the blocker halts at needs_review", || {
        status(&world)["tickets"]["needs_review"] == 1
    });

    advance_default_branch(&world, "unrelated.txt");
    let branch = format!("sloop/{blocker}-a1-{}", short_run_id(&world, 1));
    assert!(
        git_root(
            &world,
            &[
                "-c",
                "user.name=operator",
                "-c",
                "user.email=operator@example.invalid",
                "merge",
                "--no-ff",
                "-m",
                "operator merges the run branch",
                &branch,
            ],
        )
        .status
        .success()
    );
    wait_until_slow("the daemon reconciles the external merge", || {
        status(&world)["tickets"]["merged"] == 1
    });
    assert_eq!(external_merge_count(&world), 1);

    // A restart re-derives the same fact from Git, but the settled ticket is no
    // longer in review, so the evidence row is neither duplicated nor lost.
    assert!(world.sloop(&["stop"]).status.success());
    world.start_daemon();
    for _ in 0..4 {
        let snapshot = status(&world);
        assert_eq!(snapshot["tickets"]["merged"], 1);
        assert_eq!(snapshot["tickets"]["needs_review"], 0);
    }
    assert_eq!(external_merge_count(&world), 1);
}

fn assert_periodic_dead_agent_is_orphaned(world: &World, ticket_name: &str, marker: &str) {
    let ticket = world.write_ticket(ticket_name, "# Recover dead agent\nwork\n");
    world.commit_all("seed");
    world.arm_test_hook("before-agent-exit-checkpoint");
    world.start_daemon();
    let posted = world.sloop(&["post", ticket.to_str().unwrap(), "--auto"]);
    assert!(posted.status.success());
    wait_until("the agent reaches its blocking point", || {
        world.fake_agent_reached(marker)
    });

    let agent_pid = world.run_process_id(&world.run_id(1));
    world.kill_process_group(agent_pid);
    wait_until("the supervisor reaches the exit handoff", || {
        world.test_hook_reached("before-agent-exit-checkpoint")
    });
    wait_until_slow("periodic recovery settles the dead run", || {
        let snapshot = status(world);
        snapshot["gate"]["active_agents"] == 0
            && snapshot["tickets"]["ready"] == 1
            && snapshot["runs"].as_array().is_some_and(Vec::is_empty)
    });

    world.release_test_hook("before-agent-exit-checkpoint");
    let run_id = world.run_id(1);
    assert_eq!(world.show_snapshot(&run_id)["state"], "orphaned");
}

#[test]
fn stop_shuts_down_an_idle_daemon_and_never_autostarts_one() {
    let world = World::configured();
    world.commit_all("seed");

    // No daemon: stop succeeds without starting one.
    let output = world.sloop(&["stop"]);
    assert!(output.status.success());
    let response = World::json_stdout(&output);
    assert_eq!(response["data"]["running"], false);
    assert!(!world.operator_socket().exists());

    // Live daemon: stop makes the process exit and the socket disappear.
    let pid = world.start_daemon()["data"]["pid"].as_u64().expect("pid") as u32;
    let output = world.sloop(&["stop"]);
    assert!(output.status.success());
    assert_eq!(World::json_stdout(&output)["data"]["stopping"], true);
    wait_until("the daemon process exits", || !process_alive(pid));
}

#[test]
fn stop_refuses_while_a_run_is_active_unless_forced() {
    let world = World::configured();
    configure_fake_agent(&world, 1, true); // blocking agent
    let ticket = world.write_ticket("t1.md", "# T1\nwork\n");
    world.commit_all("seed");
    let pid = world.start_daemon()["data"]["pid"].as_u64().expect("pid") as u32;
    let output = world.sloop(&["post", ticket.to_str().unwrap(), "--auto"]);
    assert!(output.status.success());
    wait_until("the agent starts", || {
        status(&world)["gate"]["active_agents"] == 1
    });

    let output = world.sloop(&["stop"]);
    assert!(!output.status.success());
    let error = World::json_stdout_or_stderr(&output);
    assert_eq!(error["error"]["code"], "conflict");
    assert!(
        process_alive(pid),
        "a refused stop must not kill the daemon"
    );

    let output = world.sloop(&["stop", "--force"]);
    assert!(output.status.success());
    wait_until("the daemon exits after force-stop", || !process_alive(pid));
}

#[test]
fn the_daemon_exits_when_its_project_root_disappears() {
    let world = World::configured();
    world.commit_all("seed");
    let pid = world.start_daemon()["data"]["pid"].as_u64().expect("pid") as u32;

    // The liveness probe keys on `.git` vanishing; `TempDir` teardown
    // removes the rest later.
    fs::remove_dir_all(world.root().join(".git")).expect("delete repository");

    wait_until_slow("the orphaned daemon exits", || !process_alive(pid));
}

#[test]
fn wait_blocks_until_a_run_finishes_and_reports_the_outcome() {
    let world = World::configured();
    configure_failing_fake_agent(&world, 1, false);
    let ticket = world.write_ticket("t1.md", "# T1\nwork\n");
    world.commit_all("seed");
    world.start_daemon();
    let output = world.sloop(&["post", ticket.to_str().unwrap(), "--auto"]);
    assert!(output.status.success());
    wait_until("a run exists", || {
        !status(&world)["runs"].as_array().unwrap().is_empty()
            || status(&world)["tickets"]["failed"].as_u64() == Some(1)
            || status(&world)["tickets"]["needs_review"].as_u64() == Some(1)
    });

    // The fake agent exits nonzero, so the derived outcome is `failed`; the
    // deprecated wait alias is the quiet show form and returns only its code.
    let run_id = world.run_id(1);
    let output = world.sloop(&["wait", &run_id, "--timeout", "30"]);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .starts_with("note: 'sloop wait' is now 'sloop show --follow --quiet'")
    );
}

#[test]
fn wait_rejects_unknown_runs() {
    let world = World::configured();
    world.commit_all("seed");
    world.start_daemon();
    let output = world.sloop(&["wait", "R99", "--timeout", "5"]);
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("'sloop wait' is now 'sloop show --follow --quiet'"));
    assert!(error.contains(r#""code":"not_found""#), "{error}");
}

#[test]
fn dropping_a_world_reaps_daemons_it_never_explicitly_started() {
    let world = World::configured();
    world.commit_all("seed");
    // `status` autostarts a daemon that `daemon_pids` never records.
    let output = world.sloop(&["status"]);
    assert!(output.status.success());
    let lock = world.lock_path();
    let pid = {
        let mut pid = None;
        wait_until("the autostarted daemon writes its identity", || {
            pid = sloop::daemon::read_lock_identity(&lock).map(|identity| identity.pid);
            pid.is_some()
        });
        pid.expect("lockfile pid")
    };
    assert!(process_alive(pid));

    drop(world);
    wait_until("the reaped daemon exits", || !process_alive(pid));
}

#[test]
fn events_feed_reports_the_run_lifecycle_in_order() {
    let world = World::configured();
    configure_fake_agent(&world, 1, false);
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_manual(&world, "watched.md", "# Watched work\n");
    let output = world.sloop(&["run", &ticket]);
    assert!(output.status.success());

    wait_until("the run settles", || {
        status(&world)["tickets"]["merged"] == 1
    });

    let response = world.operator_exchange(
        r#"{"v":1,"id":"req-events","verb":"events","args":{"after":0},"token":null}"#,
    );
    assert_eq!(response["ok"], true, "events failed: {response}");
    let events = response["data"]["events"].as_array().expect("events array");
    let lifecycle: Vec<&str> = events
        .iter()
        .filter(|event| event["run"] == world.run_id(1))
        .map(|event| event["kind"].as_str().unwrap())
        .collect();
    assert_eq!(lifecycle, ["run_claimed", "run_started", "run_finished"]);
    for event in events
        .iter()
        .filter(|event| event["run"] == world.run_id(1))
    {
        assert_eq!(event["ticket"], ticket.as_str());
    }
    let finished = events
        .iter()
        .find(|event| event["kind"] == "run_finished")
        .expect("finished event");
    assert_eq!(finished["data"]["outcome"], "merged");
    assert_eq!(finished["data"]["ticket_state"], "merged");
    assert_eq!(
        response["data"]["next_cursor"],
        events.last().unwrap()["sequence"]
    );

    // `tail` starts near the newest event; a cursor at the end returns an
    // empty page, which is what a live watcher polls on.
    let tail = world.operator_exchange(
        r#"{"v":1,"id":"req-tail","verb":"events","args":{"tail":1},"token":null}"#,
    );
    let tail_events = tail["data"]["events"].as_array().expect("tail events");
    assert_eq!(tail_events.len(), 1);
    assert_eq!(tail_events[0]["kind"], "run_finished");

    let cursor = response["data"]["next_cursor"].as_i64().unwrap();
    let empty = world.operator_exchange(&format!(
        r#"{{"v":1,"id":"req-empty","verb":"events","args":{{"after":{cursor}}},"token":null}}"#,
    ));
    assert_eq!(empty["data"]["events"].as_array().unwrap().len(), 0);
    assert_eq!(empty["data"]["next_cursor"], cursor);
}

/// A live `sloop watch` process, with its NDJSON stdout drained on a
/// background thread so a test can inspect what has streamed so far without
/// blocking on a process that never exits on its own.
struct Watcher {
    child: std::process::Child,
    events: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
}

impl Watcher {
    fn spawn(world: &World, args: &[&str]) -> Self {
        let mut child = world.spawn_sloop(args);
        let stdout = child.stdout.take().expect("watcher stdout");
        let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = std::sync::Arc::clone(&events);
        std::thread::spawn(move || {
            use std::io::BufRead;
            for line in std::io::BufReader::new(stdout)
                .lines()
                .map_while(Result::ok)
            {
                let event: serde_json::Value =
                    serde_json::from_str(&line).expect("watch emits one JSON object per line");
                sink.lock().expect("watcher sink").push(event);
            }
        });
        Self { child, events }
    }

    fn events(&self) -> Vec<serde_json::Value> {
        self.events.lock().expect("watcher sink").clone()
    }

    /// The tickets this watcher has seen a run settle for, in arrival order.
    fn finished_tickets(&self) -> Vec<String> {
        self.events()
            .iter()
            .filter(|event| event["kind"] == "run_finished")
            .filter_map(|event| event["ticket"].as_str().map(str::to_owned))
            .collect()
    }
}

impl Drop for Watcher {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A watcher scoped to a ticket streams that ticket's runs and nothing else,
/// while a bare watcher on the same feed sees every ticket.
///
/// The two runs are settled in a known order so the absence assertion is
/// airtight rather than a race: cursors only move forward, so once the scoped
/// watcher has emitted an event from the *later* run it has already scanned
/// past every event of the earlier one and chosen not to emit them.
#[test]
fn watch_scopes_its_stream_to_the_given_reference() {
    let world = World::configured();
    configure_fake_agent(&world, 1, false);
    world.commit_all("initial");
    world.start_daemon();
    let other = post_manual(&world, "other.md", "# Other work\n");
    let watched = post_manual(&world, "watched.md", "# Watched work\n");

    let scoped = Watcher::spawn(&world, &["watch", &watched]);
    let every = Watcher::spawn(&world, &["watch"]);

    assert!(world.sloop(&["run", &other]).status.success());
    wait_until("the unscoped run settles", || {
        status(&world)["tickets"]["merged"] == 1
    });
    assert!(world.sloop(&["run", &watched]).status.success());
    wait_until("the scoped run settles", || {
        status(&world)["tickets"]["merged"] == 2
    });

    wait_until("the scoped watcher streams its ticket's outcome", || {
        scoped.finished_tickets() == [watched.clone()]
    });
    wait_until("the bare watcher streams both outcomes", || {
        every.finished_tickets() == [other.clone(), watched.clone()]
    });

    let scoped_events = scoped.events();
    assert!(
        scoped_events
            .iter()
            .all(|event| event["ticket"] == watched.as_str()),
        "scoped watch leaked another ticket's events: {scoped_events:?}"
    );
    // Scoping narrows the feed; it does not reduce it to outcomes.
    let kinds: Vec<&str> = scoped_events
        .iter()
        .filter_map(|event| event["kind"].as_str())
        .collect();
    assert!(
        kinds.contains(&"run_claimed") && kinds.contains(&"run_started"),
        "expected the full lifecycle within the scope, got {kinds:?}"
    );
}

/// A non-reference is a ticket pattern, including when it currently matches
/// nothing. Pattern output keeps the list shape rather than becoming detail.
#[test]
fn show_unknown_text_as_an_empty_ticket_pattern() {
    let world = World::configured();
    configure_fake_agent(&world, 1, false);
    world.commit_all("initial");
    world.start_daemon();

    let output = world.sloop(&["show", "NOPE-404"]);
    assert!(output.status.success());
    let envelope = World::json_stdout(&output);
    assert_eq!(envelope["data"]["kind"], "matches");
    assert_eq!(envelope["data"]["tickets"], json!([]));
}

/// Run and project references scope the same feed the CLI streams, exercised
/// over the socket because that envelope, not the CLI, is the public API.
#[test]
fn events_scope_accepts_run_and_project_references() {
    let world = World::configured();
    configure_fake_agent(&world, 1, false);
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post_manual(&world, "scoped.md", "# Scoped work\n");
    assert!(world.sloop(&["run", &ticket]).status.success());
    wait_until("the scoped run settles", || {
        status(&world)["tickets"]["merged"] == 1
    });
    // A second run gives every scope something it must exclude, so a filter
    // that silently matched everything would not pass here.
    let unscoped = post_manual(&world, "unscoped.md", "# Unscoped work\n");
    assert!(world.sloop(&["run", &unscoped]).status.success());
    wait_until("the unscoped run settles", || {
        status(&world)["tickets"]["merged"] == 2
    });

    let run_id = world.run_id(1);
    let alias = world.run_alias(1);
    for scope in [run_id.as_str(), alias.as_str(), ticket.as_str()] {
        let response = world.operator_exchange(&format!(
            r#"{{"v":1,"id":"req-scope","verb":"events","args":{{"after":0,"scope":"{scope}"}},"token":null}}"#,
        ));
        assert_eq!(response["ok"], true, "scope `{scope}` failed: {response}");
        let events = response["data"]["events"].as_array().expect("events array");
        assert!(!events.is_empty(), "scope `{scope}` matched nothing");
        assert!(
            events.iter().all(|event| event["run"] == run_id.as_str()),
            "scope `{scope}` leaked events: {events:?}"
        );
        // The cursor tracks rows scanned, not rows emitted, so a watcher
        // polling with it never rescans the feed.
        assert_eq!(response["data"]["next_cursor"], response["data"]["latest"]);
    }

    // A project reaches both of its tickets, and nothing without a ticket:
    // the daemon-wide rows an operator sees on a bare `watch` stay out.
    let project = world.operator_exchange(
        r#"{"v":1,"id":"req-project","verb":"events","args":{"after":0,"scope":"default"},"token":null}"#,
    );
    assert_eq!(project["ok"], true, "project scope failed: {project}");
    let mut scoped_tickets: Vec<&str> = project["data"]["events"]
        .as_array()
        .expect("events array")
        .iter()
        .filter_map(|event| event["ticket"].as_str())
        .collect();
    scoped_tickets.sort_unstable();
    scoped_tickets.dedup();
    let mut expected = [ticket.as_str(), unscoped.as_str()];
    expected.sort_unstable();
    assert_eq!(scoped_tickets, expected);

    let unknown = world.operator_exchange(
        r#"{"v":1,"id":"req-unknown","verb":"events","args":{"after":0,"scope":"NOPE-404"},"token":null}"#,
    );
    assert_eq!(unknown["ok"], true);
    assert_eq!(unknown["data"]["events"], json!([]));
}
