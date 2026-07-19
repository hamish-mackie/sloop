//! Integration coverage for `on_fail` repair agents on `exec` and `merge`
//! stages. Real daemon, real git, scripted fake agents that branch on their
//! prompt: the build spawn and the repair spawn run the same script but do
//! different work depending on whether the prompt carries the `REPAIR` marker.

mod support;

use std::fs;
use std::path::Path;
use std::process::Command;

use serde_json::Value;
use support::{World, wait_until, wait_until_slow};

/// Writes the repository config with the given agent targets block and a
/// committed flow file. `targets` is spliced under `agent.targets`.
fn configure(world: &World, flow: &str, targets: &str) {
    let flow_dir = world.root().join(".agents/sloop/flows");
    fs::create_dir_all(&flow_dir).expect("create flow directory");
    fs::write(flow_dir.join("default.yaml"), flow).expect("write flow");
    fs::write(
        world.root().join(".agents/sloop/config.yaml"),
        format!(
            "version: 1\nscheduler:\n  max_parallel_tasks: 1\nagent:\n  default_target: fake\n  targets:\n{targets}"
        ),
    )
    .expect("write config");
}

/// Writes an executable fake-agent script and returns its absolute path.
fn write_script(world: &World, name: &str, body: &str) -> std::path::PathBuf {
    let path = world.root().join(name);
    fs::write(&path, format!("#!/bin/sh\nset -eu\n{body}")).expect("write fake agent script");
    path
}

/// One agent target invoking `script` with just the prompt.
fn target(name: &str, script: &Path) -> String {
    format!(
        "    {name}:\n      cmd: [\"sh\", {}, \"{{prompt}}\"]\n",
        serde_json::to_string(&script.to_string_lossy()).expect("serialize script path"),
    )
}

fn post(world: &World, name: &str) -> String {
    let ticket = world.write_ticket(name, "# Repair scenario\n");
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

fn repair_attempts(world: &World, run: &str) -> Vec<Value> {
    let connection = rusqlite::Connection::open(world.db_path()).expect("open state database");
    let mut statement = connection
        .prepare("SELECT data_json FROM run_evidence WHERE run_id = ?1 AND kind = 'repair_attempt' ORDER BY dedupe_key")
        .expect("prepare repair query");
    statement
        .query_map([run], |row| row.get::<_, String>(0))
        .expect("query repair attempts")
        .map(|data| serde_json::from_str(&data.unwrap()).unwrap())
        .collect()
}

fn commit(identity: &str) -> String {
    format!("git -c user.name={identity} -c user.email={identity}@example.invalid commit --quiet")
}

/// The default branch the daemon merges into, resolved after the initial commit.
fn default_branch(world: &World) -> String {
    let output = Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .current_dir(world.root())
        .output()
        .expect("resolve default branch");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

#[test]
fn exec_repair_fixes_the_tree_and_the_run_merges() {
    let world = World::configured();
    // Build makes a commit so the eventual merge is real; the repair agent
    // creates the file the `test` stage checks and commits it.
    let script = write_script(
        &world,
        "fake-agent.sh",
        &format!(
            "case \"$1\" in\n  *REPAIR*)\n    : > fixed.txt\n    git add fixed.txt\n    {commit} --allow-empty -m repair ;;\n  *)\n    {commit} --allow-empty -m build ;;\nesac\nexit 0\n",
            commit = commit("agent"),
        ),
    );
    configure(
        &world,
        "- { name: build, kind: agent, verdict: exit }\n- name: test\n  kind: exec\n  cmd: [\"sh\", \"-c\", \"test -f fixed.txt\"]\n  on_fail:\n    agent: \"REPAIR make the test pass\"\n    attempts: 2\n- { name: merge, kind: merge }\n",
        &target("fake", &script),
    );
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "exec-repair.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the repaired run merges", || {
        status(&world)["tickets"]["merged"] == 1
    });

    // Exactly one repair attempt, and the fix reached the default branch.
    let attempts = repair_attempts(&world, "R1");
    assert_eq!(attempts.len(), 1, "{attempts:?}");
    assert_eq!(attempts[0]["stage"], "test");
    assert_eq!(attempts[0]["attempt"], 1);
    assert_eq!(attempts[0]["retry_verdict"], "pass");
    assert!(world.root().join("fixed.txt").is_file());
}

#[test]
fn exec_repair_that_does_not_fix_exhausts_attempts_and_fails() {
    let world = World::configured();
    // Neither build nor repair commits, so the failing exec stage settles the
    // run `failed`, identical to a run without `on_fail`. The repair log lives
    // outside the worktree so every spawn shares it.
    let log = world.root().join("repair-spawns.log");
    fs::write(&log, b"").unwrap();
    let script = write_script(
        &world,
        "fake-agent.sh",
        &format!(
            "case \"$1\" in\n  *REPAIR*) printf x >> {log} ;;\nesac\nexit 0\n",
            log = shell_quote(&log.to_string_lossy()),
        ),
    );
    configure(
        &world,
        "- { name: build, kind: agent, verdict: exit }\n- name: test\n  kind: exec\n  cmd: [\"false\"]\n  on_fail:\n    agent: \"REPAIR (cannot help)\"\n    attempts: 2\n- { name: merge, kind: merge }\n",
        &target("fake", &script),
    );
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "exec-exhaust.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the unrepaired run fails", || {
        status(&world)["tickets"]["failed"] == 1
    });

    // The full attempt budget was spent before giving up.
    let attempts = repair_attempts(&world, "R1");
    assert_eq!(attempts.len(), 2, "{attempts:?}");
    assert_eq!(fs::read_to_string(&log).unwrap().len(), 2);
    assert_eq!(status(&world)["tickets"]["merged"], 0);
}

#[test]
fn repair_honors_target_model_and_effort_overrides() {
    let world = World::configured();
    // The build target has no placeholders; the repair target substitutes the
    // overridden model and effort and records its whole argv.
    let build = write_script(
        &world,
        "build-agent.sh",
        &format!(
            "{commit} --allow-empty -m build\nexit 0\n",
            commit = commit("agent")
        ),
    );
    let argv_log = world.root().join("repair-argv.log");
    fs::write(&argv_log, b"").unwrap();
    let repair = write_script(
        &world,
        "repair-agent.sh",
        &format!(
            "printf '%s\\n' \"$*\" >> {log}\n: > fixed.txt\ngit add fixed.txt\n{commit} --allow-empty -m repair\nexit 0\n",
            log = shell_quote(&argv_log.to_string_lossy()),
            commit = commit("agent"),
        ),
    );
    let targets = format!(
        "{}    special:\n      cmd: [\"sh\", {}, \"--model\", \"{{model}}\", \"--effort\", \"{{effort}}\", \"{{prompt}}\"]\n",
        target("fake", &build),
        serde_json::to_string(&repair.to_string_lossy()).unwrap(),
    );
    configure(
        &world,
        "- { name: build, kind: agent, verdict: exit }\n- name: test\n  kind: exec\n  cmd: [\"sh\", \"-c\", \"test -f fixed.txt\"]\n  on_fail:\n    agent: \"REPAIR with overrides\"\n    attempts: 1\n    target: special\n    model: haiku\n    effort: low\n- { name: merge, kind: merge }\n",
        &targets,
    );
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "exec-overrides.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until_slow("the overridden repair merges", || {
        status(&world)["tickets"]["merged"] == 1
    });

    let argv = fs::read_to_string(&argv_log).unwrap();
    assert!(argv.contains("--model haiku"), "{argv}");
    assert!(argv.contains("--effort low"), "{argv}");
    assert!(argv.contains("REPAIR with overrides"), "{argv}");
    assert_eq!(repair_attempts(&world, "R1")[0]["target"], "special");
}

#[test]
fn a_closed_gate_skips_repair_and_settles_as_if_absent() {
    let world = World::configured();
    // A cooldown on the target closes the spawn gate. Build blocks so the
    // cooldown can be inserted after it has already spawned, then neither the
    // build nor a (never-spawned) repair commits, so the run settles `failed`.
    let release = world.root().join("release");
    let ready = world.root().join("build-ready");
    let spawn_log = world.root().join("gate-spawns.log");
    fs::write(&spawn_log, b"").unwrap();
    let script = write_script(
        &world,
        "fake-agent.sh",
        &format!(
            "case \"$1\" in\n  *REPAIR*)\n    printf x >> {log} ;;\n  *)\n    : > {ready}\n    tries=0\n    while [ ! -e {release} ] && [ \"$tries\" -lt 400 ]; do sleep 0.05; tries=$((tries + 1)); done ;;\nesac\nexit 0\n",
            log = shell_quote(&spawn_log.to_string_lossy()),
            ready = shell_quote(&ready.to_string_lossy()),
            release = shell_quote(&release.to_string_lossy()),
        ),
    );
    configure(
        &world,
        "- { name: build, kind: agent, verdict: exit }\n- name: test\n  kind: exec\n  cmd: [\"false\"]\n  on_fail:\n    agent: \"REPAIR should be gated out\"\n    attempts: 2\n- { name: merge, kind: merge }\n",
        &target("fake", &script),
    );
    world.commit_all("initial");
    world.start_daemon();
    let ticket = post(&world, "gate-closed.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until("the build agent is running", || ready.is_file());
    insert_cooldown(&world, "fake");
    fs::write(&release, b"").unwrap();

    wait_until_slow("the gated run fails without repair", || {
        status(&world)["tickets"]["failed"] == 1
    });
    assert!(repair_attempts(&world, "R1").is_empty());
    assert_eq!(fs::read_to_string(&spawn_log).unwrap(), "");
}

#[test]
fn merge_repair_integrates_the_default_branch_and_merges() {
    let world = World::configured();
    fs::write(world.root().join("shared.txt"), "base\n").unwrap();
    world.commit_all("initial");
    let default = default_branch(&world);

    let release = world.root().join("release");
    let ready = world.root().join("build-ready");
    let pwd_log = world.root().join("repair-pwd.log");
    let worktree = world.root().join(".worktrees/R1");
    // Build changes the shared file and blocks so the default branch can
    // advance with a conflicting change before the merge is attempted. The
    // repair agent — running only in the worktree — merges the default branch
    // in, resolves, and records its working directory to prove it never
    // operated on the default checkout.
    let script = write_script(
        &world,
        "fake-agent.sh",
        &format!(
            "GIT=\"git -c user.name=agent -c user.email=agent@example.invalid\"\ncase \"$1\" in\n  *REPAIR*)\n    pwd >> {pwd_log}\n    $GIT merge --no-edit {default} || true\n    printf 'resolved\\n' > shared.txt\n    $GIT add shared.txt\n    $GIT commit -m resolve || true ;;\n  *)\n    printf 'run\\n' > shared.txt\n    git add shared.txt\n    {commit} -m build\n    : > {ready}\n    tries=0\n    while [ ! -e {release} ] && [ \"$tries\" -lt 400 ]; do sleep 0.05; tries=$((tries + 1)); done ;;\nesac\nexit 0\n",
            pwd_log = shell_quote(&pwd_log.to_string_lossy()),
            ready = shell_quote(&ready.to_string_lossy()),
            release = shell_quote(&release.to_string_lossy()),
            default = default,
            commit = commit("agent"),
        ),
    );
    configure(
        &world,
        &format!(
            "- {{ name: build, kind: agent, verdict: exit }}\n- name: merge\n  kind: merge\n  on_fail:\n    agent: \"REPAIR integrate {default}\"\n    attempts: 2\n"
        ),
        &target("fake", &script),
    );
    world.start_daemon();
    let ticket = post(&world, "merge-repair.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until("the build agent is running", || ready.is_file());
    // The default branch advances with a conflicting change while the run sits.
    fs::write(world.root().join("shared.txt"), "main\n").unwrap();
    git_root(&world, &["add", "shared.txt"]);
    git_root(
        &world,
        &[
            "-c",
            "user.name=op",
            "-c",
            "user.email=op@example.invalid",
            "commit",
            "-m",
            "advance",
        ],
    );
    fs::write(&release, b"").unwrap();

    wait_until_slow("the merge-repaired run merges", || {
        status(&world)["tickets"]["merged"] == 1
    });

    let attempts = repair_attempts(&world, "R1");
    assert_eq!(attempts.len(), 1, "{attempts:?}");
    assert_eq!(attempts[0]["stage"], "merge");
    // The repair worked only in the run worktree, never the default checkout.
    let pwd = fs::read_to_string(&pwd_log).unwrap();
    assert!(
        pwd.trim().ends_with(worktree.to_string_lossy().as_ref()),
        "{pwd}"
    );
    // The resolution reached the default branch.
    assert_eq!(
        fs::read_to_string(world.root().join("shared.txt")).unwrap(),
        "resolved\n"
    );
}

#[test]
fn merge_repair_that_leaves_conflicts_parks_needs_review() {
    let world = World::configured();
    fs::write(world.root().join("shared.txt"), "base\n").unwrap();
    world.commit_all("initial");
    let default = default_branch(&world);

    let release = world.root().join("release");
    let ready = world.root().join("build-ready");
    // The repair agent does nothing useful, so the retried merge conflicts
    // again and the exhausted run parks `needs_review` with the branch kept.
    let script = write_script(
        &world,
        "fake-agent.sh",
        &format!(
            "case \"$1\" in\n  *REPAIR*) : ;;\n  *)\n    printf 'run\\n' > shared.txt\n    git add shared.txt\n    {commit} -m build\n    : > {ready}\n    tries=0\n    while [ ! -e {release} ] && [ \"$tries\" -lt 400 ]; do sleep 0.05; tries=$((tries + 1)); done ;;\nesac\nexit 0\n",
            ready = shell_quote(&ready.to_string_lossy()),
            release = shell_quote(&release.to_string_lossy()),
            commit = commit("agent"),
        ),
    );
    configure(
        &world,
        &format!(
            "- {{ name: build, kind: agent, verdict: exit }}\n- name: merge\n  kind: merge\n  on_fail:\n    agent: \"REPAIR integrate {default}\"\n    attempts: 1\n"
        ),
        &target("fake", &script),
    );
    world.start_daemon();
    let ticket = post(&world, "merge-exhaust.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until("the build agent is running", || ready.is_file());
    fs::write(world.root().join("shared.txt"), "main\n").unwrap();
    git_root(&world, &["add", "shared.txt"]);
    git_root(
        &world,
        &[
            "-c",
            "user.name=op",
            "-c",
            "user.email=op@example.invalid",
            "commit",
            "-m",
            "advance",
        ],
    );
    fs::write(&release, b"").unwrap();

    wait_until_slow("the unrepaired merge parks for review", || {
        status(&world)["tickets"]["needs_review"] == 1
    });
    assert_eq!(repair_attempts(&world, "R1").len(), 1);
    // The run branch is preserved for a human to reconcile.
    assert!(
        git_root(
            &world,
            &["rev-parse", "--verify", &format!("sloop/{ticket}-a1-R1")]
        )
        .status
        .success()
    );
}

#[test]
fn a_restart_mid_repair_resumes_without_double_spawning() {
    let world = World::configured();
    // The repair agent records each spawn, does not fix the tree, and blocks.
    // The daemon is killed mid-repair, then restarted: recovery must not spawn
    // a second repair (the attempt is already counted), so the single-attempt
    // budget is exhausted and the run fails.
    let release = world.root().join("release");
    let spawn_log = world.root().join("repair-spawns.log");
    let repairing = world.root().join("repairing");
    fs::write(&spawn_log, b"").unwrap();
    let script = write_script(
        &world,
        "fake-agent.sh",
        &format!(
            "case \"$1\" in\n  *REPAIR*)\n    printf x >> {log}\n    : > {repairing}\n    tries=0\n    while [ ! -e {release} ] && [ \"$tries\" -lt 400 ]; do sleep 0.05; tries=$((tries + 1)); done ;;\nesac\nexit 0\n",
            log = shell_quote(&spawn_log.to_string_lossy()),
            repairing = shell_quote(&repairing.to_string_lossy()),
            release = shell_quote(&release.to_string_lossy()),
        ),
    );
    configure(
        &world,
        "- { name: build, kind: agent, verdict: exit }\n- name: test\n  kind: exec\n  cmd: [\"false\"]\n  on_fail:\n    agent: \"REPAIR (blocks)\"\n    attempts: 1\n- { name: merge, kind: merge }\n",
        &target("fake", &script),
    );
    world.commit_all("initial");
    let first = World::json_stdout(&world.sloop(&["daemon"]))["data"]["pid"]
        .as_u64()
        .unwrap() as u32;
    let ticket = post(&world, "restart-repair.md");
    assert!(world.sloop(&["run", &ticket]).status.success());

    wait_until("the repair agent is running", || repairing.is_file());
    world.kill_daemon(first);
    // Release the orphaned repair agent so it cannot linger past recovery.
    fs::write(&release, b"").unwrap();
    world.start_daemon();

    wait_until_slow("the resumed run exhausts and fails", || {
        status(&world)["tickets"]["failed"] == 1
    });
    // Exactly one repair spawn survived the restart: the attempt was neither
    // lost nor repeated.
    assert_eq!(fs::read_to_string(&spawn_log).unwrap(), "x");
    assert_eq!(repair_attempts(&world, "R1").len(), 1);
}

fn git_root(world: &World, args: &[&str]) -> std::process::Output {
    Command::new("git")
        .args(args)
        .current_dir(world.root())
        .output()
        .expect("run git in root")
}

fn insert_cooldown(world: &World, target: &str) {
    let connection = rusqlite::Connection::open(world.db_path()).expect("open state database");
    let until = world.now_ms() + 10_000_000;
    connection
        .execute(
            "INSERT OR REPLACE INTO cooldowns (key, until_ms, reason, updated_at_ms) VALUES (?1, ?2, 'test', ?3)",
            rusqlite::params![format!("agent_target:{target}"), until, world.now_ms()],
        )
        .expect("insert cooldown");
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}
