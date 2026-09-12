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
    world.arm_test_hook("after-stage-sync");
    let daemon = world.start_daemon();
    let ticket = post(&world, "train-conflict.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the build stage finishes", || {
        world.test_hook_reached("after-stage-build")
    });
    advance_default_branch(&world, "contested.txt", "from the default branch\n");
    let target = git(world.root(), &["rev-parse", "HEAD"]);
    world.release_test_hook("after-stage-build");
    wait_until_slow("the failed sync is recorded", || {
        world.test_hook_reached("after-stage-sync")
    });
    world.kill_daemon(daemon["data"]["pid"].as_u64().unwrap() as u32);
    advance_default_branch(&world, "later.txt", "a later target\n");
    world.release_test_hook("after-stage-sync");
    world.start_daemon();

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
    assert!(
        rerun.contains(&format!("git merge --no-edit {target}")),
        "{rerun}"
    );

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
    let shown = world.show_snapshot(&world.run_alias(1));
    let refused = shown["stages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|stage| stage["stage"] == "merge" && stage["attempt"] == 1)
        .unwrap();
    assert_eq!(refused["integration_failure"]["kind"], "ff_only_refused");
    assert!(refused["reason"].as_str().unwrap().contains("ff-only"));
    let output = world.sloop(&["logs", &world.run_alias(1), "--stage", "merge"]);
    let log = String::from_utf8_lossy(&output.stdout);
    assert!(log.contains("Not possible to fast-forward"), "{log}");
}

/// Exercise the materialized default, replacing only the model and repository
/// check commands. Both agents finish against the same base before either lands.
#[test]
fn the_default_flow_heals_two_concurrent_conflicting_runs() {
    let world = World::configured();
    let script = world.root().join("fake-agent.sh");
    let log_prefix = world.root().join("prompt-");
    fs::write(
        &script,
        format!(
            r#"#!/bin/sh
set -eu
printf '\001PROMPT\001\n%s\n' "$1" >> {log}"$SLOOP_TICKET_ID"
export GIT_AUTHOR_NAME=agent GIT_COMMITTER_NAME=agent
export GIT_AUTHOR_EMAIL=agent@example.invalid GIT_COMMITTER_EMAIL=agent@example.invalid
case "$1" in
  *"Integration repair:"*)
    target=$(printf '%s\n' "$1" | sed -n 's/.*`git merge --no-edit \([0-9a-f]*\)`.*/\1/p')
    test -n "$target"
    git merge --no-edit "$target" || test -n "$(git ls-files --unmerged)"
    {{ git show HEAD:contested.txt; git show "$target:contested.txt"; }} | sort -u > contested.txt
    ;;
  *) printf '%s\n' "$SLOOP_TICKET_ID" > contested.txt ;;
esac
git add contested.txt
git -c user.name=agent -c user.email=agent@example.invalid commit --quiet -m 'integrated work'
"#,
            log = shell_quote(&log_prefix.to_string_lossy())
        ),
    )
    .unwrap();
    configure(&world, &train_flow("true"), &script);
    fs::remove_file(world.root().join(".agents/sloop/flows/default.yaml")).unwrap();
    let config = world.root().join(".agents/sloop/config.yaml");
    fs::write(
        &config,
        fs::read_to_string(&config)
            .unwrap()
            .replace("max_parallel_tasks: 1", "max_parallel_tasks: 2"),
    )
    .unwrap();
    assert!(world.sloop(&["init"]).status.success());
    let flow_path = world.root().join(".agents/sloop/flows/default.yaml");
    let mut flow: serde_yaml::Value =
        serde_yaml::from_str(&fs::read_to_string(&flow_path).unwrap()).unwrap();
    flow["stages"][1]["action"]["exec"] = serde_yaml::to_value([
        env!("CARGO_BIN_EXE_sloop"),
        "verdict",
        "pass",
        "--reason",
        "reviewed",
    ])
    .unwrap();
    flow["stages"][3]["action"]["exec"] =
        serde_yaml::to_value(["git", "diff", "--exit-code", "HEAD", "--"]).unwrap();
    fs::write(flow_path, serde_yaml::to_string(&flow).unwrap()).unwrap();
    fs::write(world.root().join("contested.txt"), "base\n").unwrap();
    world.commit_all("initial");
    world.arm_test_hook("after-stage-build");
    world.start_daemon();
    let first = post(&world, "first.md");
    let second = post(&world, "second.md");
    assert!(world.sloop(&["run", &first]).status.success());
    assert!(world.sloop(&["run", &second]).status.success());
    wait_until_slow("both builds finish before integration", || {
        [1, 2].iter().all(|position| {
            let shown = world.show_snapshot(&world.run_alias(*position));
            shown["stages"].as_array().is_some_and(|stages| {
                stages
                    .iter()
                    .any(|stage| stage["stage"] == "build" && stage["state"] == "passed")
            })
        })
    });
    world.release_test_hook("after-stage-build");
    wait_until_slow("both conflicting runs heal and land", || {
        let state = status(&world);
        if state["tickets"]["needs_review"].as_u64().unwrap_or(0) > 0
            || state["tickets"]["failed"].as_u64().unwrap_or(0) > 0
        {
            panic!(
                "unexpected failure: {state}\n{}\n{}\n{}\n{}",
                world.show_snapshot(&world.run_alias(1)),
                world.show_snapshot(&world.run_alias(2)),
                String::from_utf8_lossy(&world.sloop(&["logs", &world.run_alias(1)]).stdout),
                String::from_utf8_lossy(&world.sloop(&["logs", &world.run_alias(2)]).stdout)
            );
        }
        state["tickets"]["merged"] == 2
    });
    let contents = fs::read_to_string(world.root().join("contested.txt")).unwrap();
    assert!(contents.lines().any(|line| line == first), "{contents}");
    assert!(contents.lines().any(|line| line == second), "{contents}");
    let launches: Vec<_> = [&first, &second]
        .iter()
        .flat_map(|ticket| prompts(&world.root().join(format!("prompt-{ticket}"))))
        .collect();
    assert_eq!(
        launches.len(),
        3,
        "only the conflicting run needs an extra agent"
    );
    assert_eq!(
        launches
            .iter()
            .filter(|prompt| prompt.contains("Integration repair:"))
            .count(),
        1
    );
    assert!(!merge_in_progress(world.root()));
    for position in [1, 2] {
        let shown = world.show_snapshot(&world.run_alias(position));
        let stages = shown["stages"].as_array().unwrap();
        let builds = stages
            .iter()
            .filter(|stage| stage["stage"] == "build")
            .count();
        let reviews = stages
            .iter()
            .filter(|stage| stage["stage"] == "review")
            .count();
        assert_eq!(builds, reviews, "repairs are reviewed again: {shown}");
    }
}

#[test]
fn checkout_refusals_explain_themselves_and_do_not_retry_the_train() {
    for (kind, reason) in [
        ("staged_changes", "staged changes"),
        ("operation_in_progress", "merge in progress"),
        ("checkout_locked", "index locked"),
        ("local_changes", "local changes would be overwritten"),
    ] {
        let world = World::configured();
        let prompt_log = world.root().join("prompts.log");
        let script = agent_writing(&world, &prompt_log, "agent.txt", "agent work");
        configure(&world, &train_flow("true"), &script);
        world.commit_all("initial");
        world.arm_test_hook("after-stage-verify");
        world.arm_test_hook("after-stage-merge");
        let daemon = world.start_daemon();
        let ticket = post(&world, "checkout-refused.md");
        assert!(world.sloop(&["run", &ticket]).status.success());
        wait_until_slow("verification finishes", || {
            world.test_hook_reached("after-stage-verify")
        });
        let before = git(world.root(), &["rev-parse", "HEAD"]);
        match kind {
            "staged_changes" => {
                fs::write(world.root().join("operator.txt"), "operator work\n").unwrap();
                git(world.root(), &["add", "operator.txt"]);
            }
            "operation_in_progress" => {
                fs::write(world.root().join(".git/MERGE_HEAD"), format!("{before}\n")).unwrap();
            }
            "checkout_locked" => {
                fs::write(world.root().join(".git/index.lock"), "").unwrap();
            }
            "local_changes" => {
                fs::write(world.root().join("agent.txt"), "operator work\n").unwrap();
            }
            _ => unreachable!(),
        }
        world.release_test_hook("after-stage-verify");
        wait_until_slow("the refusal is persisted", || {
            world.test_hook_reached("after-stage-merge")
        });
        // Recovery must replay the typed failure rather than retry the old
        // blanket return_to rule after forgetting in-memory diagnostics.
        world.kill_daemon(daemon["data"]["pid"].as_u64().unwrap() as u32);
        world.release_test_hook("after-stage-merge");
        world.start_daemon();
        wait_until_slow("the checkout refusal halts", || {
            status(&world)["tickets"]["needs_review"] == 1
        });
        let shown = world.show_snapshot(&world.run_alias(1));
        let merges: Vec<_> = shown["stages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|stage| stage["stage"] == "merge")
            .collect();
        assert_eq!(merges.len(), 1, "{shown}");
        assert_eq!(merges[0]["integration_failure"]["kind"], kind, "{shown}");
        assert!(
            merges[0]["reason"].as_str().unwrap().contains(reason),
            "{shown}"
        );
        assert_eq!(git(world.root(), &["rev-parse", "HEAD"]), before);
        assert_eq!(prompts(&prompt_log).len(), 1);
        let output = world.sloop(&["logs", &world.run_alias(1), "--stage", "merge"]);
        let log = String::from_utf8_lossy(&output.stdout);
        assert!(log.contains(reason), "{log}");
    }
}

#[test]
fn configured_checks_run_after_sync_and_return_to_the_agent_for_repair() {
    let world = World::configured();
    let script = world.root().join("fake-agent.sh");
    fs::write(
        &script,
        r#"#!/bin/sh
set -eu
case "$1" in
  *'Stage `test` (attempt 1) failed'*)
    test -f landed.txt
    printf 'fixed\n' > repaired.txt
    git add repaired.txt
    ;;
  *) printf 'implementation\n' > work.txt; git add work.txt ;;
esac
git -c user.name=agent -c user.email=agent@example.invalid commit --quiet -m 'agent work'
"#,
    )
    .unwrap();
    configure(&world, &train_flow("true"), &script);
    fs::remove_file(world.root().join(".agents/sloop/flows/default.yaml")).unwrap();
    let config = world.root().join(".agents/sloop/config.yaml");
    let mut contents = fs::read_to_string(&config).unwrap();
    contents
        .push_str("flow:\n  test_cmd: [sh, -c, 'test -f landed.txt && test -f repaired.txt']\n");
    fs::write(&config, contents).unwrap();
    world.commit_all("initial");
    world.arm_test_hook("after-stage-build");
    world.arm_test_hook("after-stage-test");
    let daemon = world.start_daemon();
    let ticket = post(&world, "verify-repair.md");
    assert!(world.sloop(&["run", &ticket]).status.success());
    wait_until_slow("build completes", || {
        world.test_hook_reached("after-stage-build")
    });
    advance_default_branch(&world, "landed.txt", "another run's work\n");
    world.release_test_hook("after-stage-build");
    wait_until_slow("the failed check is recorded", || {
        world.test_hook_reached("after-stage-test")
    });
    world.kill_daemon(daemon["data"]["pid"].as_u64().unwrap() as u32);
    let updated = fs::read_to_string(&config).unwrap().replace(
        "test_cmd: [sh, -c, 'test -f landed.txt && test -f repaired.txt']",
        "test_cmd: ['false']",
    );
    fs::write(config, updated).unwrap();
    world.release_test_hook("after-stage-test");
    world.start_daemon();
    wait_until_slow("verification is repaired and the run lands", || {
        status(&world)["tickets"]["merged"] == 1
    });
    assert!(world.root().join("repaired.txt").is_file());
    assert!(world.show_snapshot(&world.run_alias(1))["halt"].is_null());
    let stages = shown_stages(&world, &world.run_alias(1));
    assert!(
        stages.contains(&("test".into(), "failed".into())),
        "{stages:?}"
    );
    assert!(
        stages.contains(&("test#2".into(), "passed".into())),
        "{stages:?}"
    );
    assert!(
        stages.contains(&("build#2".into(), "passed".into())),
        "{stages:?}"
    );
}
