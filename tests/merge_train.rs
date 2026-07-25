//! Integration coverage for the merge train: the `sync` builtin, the
//! `ff_only` merge, and the loop the two of them close.
//!
//! Every scenario here turns on the same fact — the default branch moves while
//! a run is in flight — and asks what the train does about it at each point it
//! can move: during the build, between the sync and the merge, and in a way
//! that cannot be integrated at all.

mod support;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;
use support::{World, wait_until_slow};

/// The train flow the scenarios share, with `verify` supplied per test. It is
/// the shipped `train` shape: build, integrate, judge the integrated tree,
/// then a fast-forward that can only succeed if nothing moved underneath it.
fn train_flow(verify: &str) -> String {
    format!(
        "stages:\n\
         \x20 - {{ name: build, action: agent, result_check: {{ builtin: commits }} }}\n\
         \x20 - name: sync\n\
         \x20   action: {{ builtin: sync }}\n\
         \x20   result_check: none\n\
         \x20   fail_action: {{ return_to: build, attempts: 1 }}\n\
         \x20 - name: verify\n\
         \x20   action: {{ exec: [\"sh\", \"-c\", {verify}] }}\n\
         \x20   result_check: none\n\
         \x20   fail_action: {{ return_to: build, attempts: 1 }}\n\
         \x20 - name: merge\n\
         \x20   action: {{ builtin: merge, ff_only: true }}\n\
         \x20   result_check: none\n\
         \x20   fail_action: {{ return_to: sync, attempts: 3 }}\n",
        verify = serde_json::to_string(verify).expect("serialize verify command"),
    )
}

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

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

/// A fake agent that records its prompt, writes `file` with `contents`, and
/// commits. `--allow-empty` so a re-entry that reproduces the same tree still
/// makes a commit and so still satisfies the build stage's commits check.
fn agent_writing(world: &World, prompt_log: &Path, file: &str, contents: &str) -> PathBuf {
    fs::write(prompt_log, b"").expect("create prompt log");
    let path = world.root().join("fake-agent.sh");
    fs::write(
        &path,
        format!(
            "#!/bin/sh\nset -eu\nprintf '\\001PROMPT\\001\\n%s\\n' \"$1\" >> {log}\nprintf '%s\\n' {contents} > {file}\ngit add -- {file}\ngit -c user.name=agent -c user.email=agent@example.invalid commit --quiet --allow-empty -m 'agent work'\nexit 0\n",
            log = shell_quote(&prompt_log.to_string_lossy()),
            contents = shell_quote(contents),
            file = shell_quote(file),
        ),
    )
    .expect("write fake agent script");
    path
}

/// Every prompt the fake agent was launched with, one per spawn.
fn prompts(log: &Path) -> Vec<String> {
    fs::read_to_string(log)
        .unwrap_or_default()
        .split("\u{1}PROMPT\u{1}\n")
        .skip(1)
        .map(str::to_owned)
        .collect()
}

fn git(directory: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(directory)
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

/// Advances the default branch by one commit, touching only the named path.
/// Deliberately not `git add -A`: by now the repository holds run worktrees,
/// and sweeping them into a commit would be a different test entirely.
fn advance_default_branch(world: &World, file: &str, contents: &str) {
    fs::write(world.root().join(file), contents).expect("write default-branch file");
    git(world.root(), &["add", "--", file]);
    git(
        world.root(),
        &[
            "-c",
            "user.name=sloop-test",
            "-c",
            "user.email=sloop-test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            &format!("default branch: {file}"),
        ],
    );
}

fn post(world: &World, name: &str) -> String {
    let ticket = world.write_ticket(name, "# Merge train scenario\n");
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

/// The run branch of the `position`-th run.
fn run_branch(world: &World, position: usize) -> String {
    world.show_snapshot(&world.run_alias(position))["branch"]
        .as_str()
        .expect("the run records its branch")
        .to_owned()
}

/// Whether the run worktree is sitting on an unfinished merge.
fn merge_in_progress(worktree: &Path) -> bool {
    let path = Command::new("git")
        .args(["rev-parse", "--git-path", "MERGE_HEAD"])
        .current_dir(worktree)
        .output()
        .expect("resolve MERGE_HEAD path");
    let path = PathBuf::from(String::from_utf8_lossy(&path.stdout).trim().to_owned());
    let path = if path.is_absolute() {
        path
    } else {
        worktree.join(path)
    };
    path.exists()
}

/// The whole point of the train. The default branch moves while the agent is
/// still working, `sync` integrates it, `verify` therefore judges the merged
/// tree rather than the branch in isolation, and the fast-forward lands
/// exactly that tree on the default branch.
#[test]
fn a_train_syncs_a_moved_default_branch_verifies_the_merge_and_fast_forwards_it() {
    let world = World::configured();
    let prompt_log = world.root().join("prompts.log");
    let script = agent_writing(&world, &prompt_log, "agent.txt", "from the run branch");
    // Verification that can only pass on the *merged* tree: the file it looks
    // for was committed to the default branch after this run branched, so it
    // exists in the worktree only if the sync brought it in.
    configure(
        &world,
        &train_flow("test -f landed-after-build.txt"),
        &script,
    );
    world.commit_all("initial");
    world.arm_test_hook("after-stage-build");
    world.start_daemon();
    let ticket = post(&world, "train-sync.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the build stage finishes", || {
        world.test_hook_reached("after-stage-build")
    });
    advance_default_branch(&world, "landed-after-build.txt", "landed while building\n");
    world.release_test_hook("after-stage-build");

    wait_until_slow("the train merges", || {
        status(&world)["tickets"]["merged"] == 1
    });

    // One pass through, no loop: the sync had something to do and did it.
    assert_eq!(
        shown_stages(&world, &world.run_alias(1)),
        [
            ("build".to_owned(), "passed".to_owned()),
            ("sync".to_owned(), "passed".to_owned()),
            ("verify".to_owned(), "passed".to_owned()),
            ("merge".to_owned(), "passed".to_owned()),
        ]
    );
    assert_eq!(prompts(&prompt_log).len(), 1);

    // A fast-forward and nothing else: the default branch is now the exact
    // commit the verify stage ran against, not a merge of it with something.
    let branch = run_branch(&world, 1);
    let head = git(world.root(), &["rev-parse", "HEAD"]);
    assert_eq!(head, git(world.root(), &["rev-parse", &branch]));
    // And both sides' work is in it.
    assert!(world.root().join("agent.txt").is_file());
    assert!(world.root().join("landed-after-build.txt").is_file());
}

/// A sync that cannot be completed sends the walk back to the agent with the
/// conflict in its prompt — and hands it a worktree it can actually work in.
#[test]
fn a_sync_conflict_returns_to_build_with_the_conflict_in_the_prompt() {
    let world = World::configured();
    let prompt_log = world.root().join("prompts.log");
    // Both sides add the same path with different contents: an add/add
    // conflict that no merge strategy can resolve on its own.
    let script = agent_writing(&world, &prompt_log, "contested.txt", "from the run branch");
    configure(&world, &train_flow("true"), &script);
    world.commit_all("initial");
    world.arm_test_hook("after-stage-build");
    world.start_daemon();
    let ticket = post(&world, "train-conflict.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the build stage finishes", || {
        world.test_hook_reached("after-stage-build")
    });
    advance_default_branch(&world, "contested.txt", "from the default branch\n");
    world.release_test_hook("after-stage-build");

    // The agent reproduces the same conflicting commit, so the single return
    // is spent and the walk halts on the sync it could not get past. The
    // branch holds real work, so it is parked for review rather than failed.
    wait_until_slow("the conflicted train parks for review", || {
        status(&world)["tickets"]["needs_review"] == 1
    });
    assert_eq!(status(&world)["tickets"]["merged"], 0);

    // The re-entered agent was told which stage failed and what git said, or
    // it would have nothing to work from but the ticket it already tried.
    let prompts = prompts(&prompt_log);
    assert_eq!(prompts.len(), 2, "{prompts:?}");
    let rerun = &prompts[1];
    assert!(rerun.contains("--- previous attempt failed ---"), "{rerun}");
    assert!(rerun.contains("Stage `sync` (attempt 1) failed"), "{rerun}");
    assert!(rerun.contains("contested.txt"), "{rerun}");
    assert!(rerun.contains("CONFLICT"), "{rerun}");

    // The merge stage was never requested, and both sync executions are on
    // the record.
    assert_eq!(
        shown_stages(&world, &world.run_alias(1)),
        [
            ("build".to_owned(), "passed".to_owned()),
            ("build#2".to_owned(), "passed".to_owned()),
            ("sync".to_owned(), "failed".to_owned()),
            ("sync#2".to_owned(), "failed".to_owned()),
            ("verify".to_owned(), "pending".to_owned()),
            ("merge".to_owned(), "pending".to_owned()),
        ]
    );
}

/// A `return_to` target must get a tree it can work in. A sync that failed and
/// left `MERGE_HEAD` behind would wedge the re-entered agent's first commit,
/// so the abort is part of the builtin's contract rather than a tidy-up.
#[test]
fn a_conflicted_sync_leaves_no_merge_in_progress_in_the_worktree() {
    let world = World::configured();
    let prompt_log = world.root().join("prompts.log");
    let script = agent_writing(&world, &prompt_log, "contested.txt", "from the run branch");
    configure(&world, &train_flow("true"), &script);
    world.commit_all("initial");
    world.arm_test_hook("after-stage-build");
    world.start_daemon();
    let ticket = post(&world, "train-conflict-state.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the build stage finishes", || {
        world.test_hook_reached("after-stage-build")
    });
    advance_default_branch(&world, "contested.txt", "from the default branch\n");
    world.release_test_hook("after-stage-build");

    wait_until_slow("the conflicted train parks for review", || {
        status(&world)["tickets"]["needs_review"] == 1
    });

    // The agent committed a second time after the first conflict, which it
    // could not have done from a conflicted index.
    assert_eq!(prompts(&prompt_log).len(), 2);

    let worktree = world.run_worktree(1);
    assert!(
        !merge_in_progress(&worktree),
        "the worktree is still mid-merge: {}",
        git(&worktree, &["status", "--porcelain"])
    );
    // Nothing unmerged and nothing staged: the tree is back on the branch tip.
    assert_eq!(git(&worktree, &["ls-files", "--unmerged"]), "");
    assert_eq!(git(&worktree, &["diff", "--cached", "--name-only"]), "");
}

/// The window the fast-forward exists to close. Everything was verified, then
/// the default branch moved before the merge could land — so the fast-forward
/// is impossible, the merge fails without touching the default branch, and the
/// train goes round again rather than merging a tree nothing tested.
#[test]
fn a_default_branch_that_moves_before_the_merge_trips_ff_only_and_loops_the_train() {
    let world = World::configured();
    let prompt_log = world.root().join("prompts.log");
    let script = agent_writing(&world, &prompt_log, "agent.txt", "from the run branch");
    configure(&world, &train_flow("true"), &script);
    world.commit_all("initial");
    world.arm_test_hook("after-stage-verify");
    world.start_daemon();
    let ticket = post(&world, "train-ff-only.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the verify stage finishes", || {
        world.test_hook_reached("after-stage-verify")
    });
    let before = git(world.root(), &["rev-parse", "HEAD"]);
    advance_default_branch(
        &world,
        "landed-after-verify.txt",
        "landed while verifying\n",
    );
    let moved = git(world.root(), &["rev-parse", "HEAD"]);
    assert_ne!(before, moved);
    world.release_test_hook("after-stage-verify");

    wait_until_slow("the looping train merges", || {
        status(&world)["tickets"]["merged"] == 1
    });

    // The first merge failed and took the walk back to `sync`, not to `build`:
    // nothing about the work was wrong, only what it was sitting on. The agent
    // therefore never ran again.
    assert_eq!(prompts(&prompt_log).len(), 1);
    // `sloop show` groups executions under their stage, so the second lap
    // reads as a second attempt of each stage rather than a second flow.
    assert_eq!(
        shown_stages(&world, &world.run_alias(1)),
        [
            ("build".to_owned(), "passed".to_owned()),
            ("sync".to_owned(), "passed".to_owned()),
            ("sync#2".to_owned(), "passed".to_owned()),
            ("verify".to_owned(), "passed".to_owned()),
            ("verify#2".to_owned(), "passed".to_owned()),
            ("merge".to_owned(), "failed".to_owned()),
            ("merge#2".to_owned(), "passed".to_owned()),
        ]
    );

    // The failed fast-forward left the default branch exactly where it was;
    // only the second one moved it, and again by fast-forward alone.
    let branch = run_branch(&world, 1);
    let head = git(world.root(), &["rev-parse", "HEAD"]);
    assert_eq!(head, git(world.root(), &["rev-parse", &branch]));
    // The commit that arrived mid-flight is still an ancestor: the loop
    // integrated it rather than merging over it.
    assert!(
        Command::new("git")
            .args(["merge-base", "--is-ancestor", &moved, "HEAD"])
            .current_dir(world.root())
            .status()
            .expect("run git merge-base")
            .success(),
        "the mid-flight commit was not preserved"
    );
    assert!(world.root().join("agent.txt").is_file());
    assert!(world.root().join("landed-after-verify.txt").is_file());
}
