use std::io::Write;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};

use serde_json::json;

use crate::clock::Clock;
use crate::flow::{Flow, Verdict};
use crate::logging::{LogLevel, OperationalLog};
use crate::outcome::MergeOutcome;
use crate::run_log::{OutputSource, OutputStream, RunLogWriter};
use crate::store::{ExitClaim, Store};
use crate::vendor_error::VendorErrorMatch;

use super::runner::{process_start_time, spawn_output_reader};
use super::{
    MERGE_LOCK, MergeProcessCheckpoint, drive_flow, git_stdout, kill_straggler_process_group,
    merge_checkout_ready, process_identity_matches, record_merge_process_checkpoint,
    wait_for_test_hook,
};

/// One executed flow stage as observed by the supervisor.
pub(super) struct StageResult {
    pub(super) verdict: Verdict,
    pub(super) exit_code: Option<i32>,
    pub(super) started_at_ms: i64,
    pub(super) finished_at_ms: i64,
}

/// Gathers post-exit evidence in the supervisor thread, keeping slow Git and
/// flow execution out of the dispatcher.
#[allow(clippy::too_many_arguments)]
pub(super) fn gather_exit_evidence(
    run_id: &str,
    root: &Path,
    worktree: &Path,
    branch: &str,
    flow: &Flow,
    test_cmd: Option<&[String]>,
    clock: &dyn Clock,
    output_log: &RunLogWriter,
    exit_code: Option<i32>,
    capture_complete: bool,
    vendor_error: Option<&VendorErrorMatch>,
    cooldown_until_ms: Option<i64>,
    mut checkpoint_store: Option<&mut Store>,
    operational_log: &OperationalLog,
) -> Option<(Vec<String>, bool, bool, Option<MergeOutcome>)> {
    let commit_observation = try_commits_on_branch(root, branch);
    let commit_observation_complete = commit_observation.is_ok();
    let commits = commit_observation.unwrap_or_default();
    wait_for_test_hook("before-agent-exit-checkpoint");
    let checkpointed = if let Some(store) = checkpoint_store.as_deref_mut() {
        match store.record_agent_exit(
            run_id,
            exit_code,
            capture_complete,
            &json!({"complete": commit_observation_complete, "oids": commits}).to_string(),
            vendor_error,
            cooldown_until_ms,
            clock.now_ms(),
        ) {
            Ok(ExitClaim::Claimed) => true,
            Ok(ExitClaim::AlreadyClaimed { state }) => {
                operational_log.emit_with_fields(
                    LogLevel::Info,
                    "sloop::supervisor",
                    "exit_checkpoint_already_claimed",
                    json!({"run_id": run_id, "state": state}),
                );
                return None;
            }
            Err(error) => {
                operational_log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::supervisor",
                    "agent_exit_checkpoint_failed",
                    json!({"run_id": run_id, "error": error.to_string()}),
                );
                false
            }
        }
    } else {
        false
    };
    if checkpointed {
        wait_for_test_hook("after-agent-exit-checkpoint");
    }
    // Tests and merge can have side effects. Without the pre-aftercare
    // checkpoint, preserve the run branch for review rather than performing
    // an action that recovery could no longer prove or resume.
    if !checkpointed
        || checkpoint_store
            .as_deref()
            .is_some_and(|store| aftercare_cancelled(store, run_id, operational_log))
    {
        return Some((commits, commit_observation_complete, false, None));
    }
    let store = checkpoint_store.as_deref()?;
    match drive_flow(
        root,
        worktree,
        branch,
        flow,
        test_cmd,
        exit_code,
        vendor_error.is_some(),
        &commits,
        commit_observation_complete,
        output_log,
        clock,
        store,
        run_id,
        operational_log,
    ) {
        Ok(result) => Some((
            commits,
            commit_observation_complete,
            result.aftercare_failed,
            result.merge,
        )),
        Err(error) => {
            operational_log.emit_with_fields(
                LogLevel::Error,
                "sloop::supervisor",
                "aftercare_failed",
                json!({"run_id": run_id, "error": error}),
            );
            Some((commits, commit_observation_complete, true, None))
        }
    }
}

pub(super) fn aftercare_cancelled(store: &Store, run_id: &str, log: &OperationalLog) -> bool {
    match store.cancellation_requested(run_id) {
        Ok(cancelled) => cancelled,
        Err(error) => {
            log.emit_with_fields(
                LogLevel::Error,
                "sloop::supervisor",
                "cancellation_read_failed",
                json!({"run_id": run_id, "error": error.to_string()}),
            );
            true
        }
    }
}

/// Commits made since the run branch was created. The branch's own reflog is
/// the stable baseline, so rewriting the default branch cannot change this
/// activity metadata.
pub(super) fn try_commits_on_branch(root: &Path, branch: &str) -> Result<Vec<String>, String> {
    let start = git_stdout(root, &["reflog", "show", "--format=%H", branch])?
        .lines()
        .last()
        .map(str::to_owned)
        .ok_or_else(|| format!("branch `{branch}` has no reflog"))?;
    git_stdout(
        root,
        &["rev-list", "--reverse", &format!("{start}..{branch}")],
    )
    .map(|output| output.lines().map(str::to_owned).collect())
}

/// Runs one exec stage in the run's worktree, capturing its output as
/// `aftercare` evidence in the same ordered run log.
#[allow(clippy::too_many_arguments)]
pub(super) fn run_exec_stage(
    worktree: &Path,
    stage: &str,
    cmd: &[String],
    output_log: &RunLogWriter,
    clock: &dyn Clock,
    store: &Store,
    run_id: &str,
    operational_log: &OperationalLog,
) -> StageResult {
    let started_at_ms = clock.now_ms();
    let failed = |finished_at_ms| StageResult {
        verdict: Verdict::Fail,
        exit_code: None,
        started_at_ms,
        finished_at_ms,
    };
    if aftercare_cancelled(store, run_id, operational_log) {
        return failed(clock.now_ms());
    }

    let mut command = Command::new(&cmd[0]);
    command
        .args(&cmd[1..])
        .current_dir(worktree)
        .env_remove("SLOOP_SOCKET")
        .env_remove("SLOOP_TOKEN")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let Ok(mut child) = command.spawn() else {
        return failed(clock.now_ms());
    };
    let pid = child.id();
    let Some(pid_start_time) = process_start_time(pid) else {
        unsafe {
            libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
        }
        let _ = child.wait();
        return failed(clock.now_ms());
    };
    let readers = vec![
        spawn_output_reader(
            child.stdout.take().expect("stdout was piped"),
            output_log.clone(),
            OutputSource::Aftercare,
            Some(stage.to_owned()),
            OutputStream::Stdout,
        ),
        spawn_output_reader(
            child.stderr.take().expect("stderr was piped"),
            output_log.clone(),
            OutputSource::Aftercare,
            Some(stage.to_owned()),
            OutputStream::Stderr,
        ),
    ];
    if stage == "test" {
        wait_for_test_hook("before-test-process-checkpoint");
    }
    wait_for_test_hook(&format!("before-aftercare-process-checkpoint-{stage}"));
    if let Err(error) = store.record_aftercare_evidence(
        run_id,
        "aftercare_process",
        &json!({
            "stage": stage,
            "pid": pid,
            "pid_start_time": pid_start_time,
            "process_group_id": pid,
        })
        .to_string(),
        clock.now_ms(),
    ) {
        operational_log.emit_with_fields(
            LogLevel::Error,
            "sloop::supervisor",
            "aftercare_process_checkpoint_failed",
            json!({"run_id": run_id, "stage": stage, "error": error.to_string()}),
        );
        if process_identity_matches(pid, Some(pid_start_time)) {
            unsafe {
                libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
            }
        }
        let _ = child.wait();
        for reader in readers {
            let _ = reader.join();
        }
        return failed(clock.now_ms());
    }
    wait_for_test_hook(&format!("after-aftercare-process-checkpoint-{stage}"));
    if aftercare_cancelled(store, run_id, operational_log) {
        if process_identity_matches(pid, Some(pid_start_time)) {
            unsafe {
                libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
            }
        }
        let _ = child.wait();
        for reader in readers {
            let _ = reader.join();
        }
        return failed(clock.now_ms());
    }

    let status = child.wait();
    if kill_straggler_process_group(pid) {
        operational_log.emit_with_fields(
            LogLevel::Info,
            "sloop::supervisor",
            "aftercare_stragglers_killed",
            json!({"run_id": run_id, "stage": stage, "process_group_id": pid}),
        );
    }
    let mut capture_complete = true;
    for reader in readers {
        capture_complete &= reader.join().unwrap_or(false);
    }
    if !capture_complete {
        return failed(clock.now_ms());
    }
    let Ok(status) = status else {
        return failed(clock.now_ms());
    };
    StageResult {
        verdict: if status.success() {
            Verdict::Pass
        } else {
            Verdict::Fail
        },
        exit_code: status.code(),
        started_at_ms,
        finished_at_ms: clock.now_ms(),
    }
}

/// Attempts the policy merge into the default branch: fast-forward when
/// possible, otherwise a merge commit. Failed merges leave the exact checkout
/// state for human review; Sloop never guesses which post-merge edits it owns.
#[allow(clippy::too_many_arguments)]
pub(super) fn attempt_merge(
    root: &Path,
    branch: &str,
    branch_unchanged: bool,
    stage: &str,
    checkpoint_store: &Store,
    run_id: &str,
    clock: &dyn Clock,
    operational_log: &OperationalLog,
) -> MergeOutcome {
    if branch_unchanged {
        return MergeOutcome::Merged;
    }
    let Ok(_guard) = MERGE_LOCK.lock() else {
        return MergeOutcome::Diverged;
    };
    let Ok(true) = merge_checkout_ready(root) else {
        return MergeOutcome::Diverged;
    };
    let Ok(target_head) = git_stdout(root, &["rev-parse", "HEAD"]) else {
        return MergeOutcome::Diverged;
    };
    let Ok(branch_tip) = git_stdout(root, &["rev-parse", branch]) else {
        return MergeOutcome::Diverged;
    };
    let message = format!("Merge run branch '{branch}'");
    // The merge commit is sloop's own action, not the operator's or the
    // agent's, so it carries sloop's identity; a fast-forward creates no
    // commit and ignores these.
    let mut command = Command::new("sh");
    command
        .args([
            "-c",
            "IFS= read -r _ || exit 125; exec git \"$@\"",
            "sloop-merge",
        ])
        .args([
            "-c",
            "user.name=sloop",
            "-c",
            "user.email=sloop@sloop.invalid",
            "merge",
            "--quiet",
            "-m",
            &message,
            &branch_tip,
        ])
        .current_dir(root)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0);
    let Ok(mut child) = command.spawn() else {
        return MergeOutcome::Diverged;
    };
    let pid = child.id();
    let mut gate = child.stdin.take().expect("merge gate stdin was piped");
    let Some(pid_start_time) = process_start_time(pid) else {
        unsafe {
            libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
        }
        let _ = child.wait();
        return MergeOutcome::Diverged;
    };
    let checkpoint = MergeProcessCheckpoint {
        target_head,
        branch_tip,
        completed_target: None,
    };
    if let Err(error) = record_merge_process_checkpoint(
        checkpoint_store,
        run_id,
        stage,
        pid,
        pid_start_time,
        &checkpoint,
        clock.now_ms(),
    ) {
        operational_log.emit_with_fields(
            LogLevel::Error,
            "sloop::supervisor",
            "aftercare_process_checkpoint_failed",
            json!({"run_id": run_id, "stage": stage, "error": error.to_string()}),
        );
        unsafe {
            libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
        }
        let _ = child.wait();
        return MergeOutcome::Diverged;
    }
    wait_for_test_hook(&format!("after-aftercare-process-checkpoint-{stage}"));
    if aftercare_cancelled(checkpoint_store, run_id, operational_log) {
        unsafe {
            libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
        }
        let _ = child.wait();
        return MergeOutcome::Diverged;
    }
    if gate.write_all(b"run\n").is_err() {
        unsafe {
            libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
        }
        let _ = child.wait();
        return MergeOutcome::Diverged;
    }
    drop(gate);
    match child.wait() {
        Ok(status) if status.success() => {
            if let Ok(completed_target) = git_stdout(root, &["rev-parse", "HEAD"]) {
                let completed = MergeProcessCheckpoint {
                    completed_target: Some(completed_target),
                    ..checkpoint
                };
                if let Err(error) = record_merge_process_checkpoint(
                    checkpoint_store,
                    run_id,
                    stage,
                    pid,
                    pid_start_time,
                    &completed,
                    clock.now_ms(),
                ) {
                    operational_log.emit_with_fields(
                        LogLevel::Error,
                        "sloop::supervisor",
                        "merge_completion_checkpoint_failed",
                        json!({"run_id": run_id, "stage": stage, "error": error.to_string()}),
                    );
                }
            }
            wait_for_test_hook("after-successful-merge-process-exit");
            MergeOutcome::Merged
        }
        _ => {
            wait_for_test_hook("after-failed-merge-process-exit");
            MergeOutcome::Diverged
        }
    }
}
