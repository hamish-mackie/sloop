//! Soak test: churn many full run lifecycles through a real daemon and watch
//! the resource curves. A healthy daemon shows flat file-descriptor, memory,
//! and child-process curves and bounded worktree/database growth; degradation
//! shows up as a trend across batches, not a single failure.
//!
//! Ignored by default because it runs for minutes. Run it with:
//!
//! ```sh
//! cargo test --release --test soak -- --ignored --nocapture
//! ```
//!
//! Scale with `SOAK_BATCHES`, `SOAK_BATCH_SIZE`, and `SOAK_PARALLEL`.

mod support;

use std::fs;
use std::path::Path;
use std::time::Instant;

use support::{FakeAgent, World};

struct Sample {
    batch: usize,
    merged: i64,
    post_secs: f64,
    drain_secs: f64,
    runs_per_sec: f64,
    rss_kb: i64,
    fd_count: usize,
    children: usize,
    zombies: usize,
    db_kb: u64,
    wal_kb: u64,
    worktrees: usize,
    run_dirs: usize,
    state_kb: u64,
    git_kb: u64,
}

#[test]
#[ignore = "long-running soak; run explicitly with --ignored --nocapture"]
fn lifecycle_churn_keeps_resource_curves_flat() {
    let batches: usize = env_or("SOAK_BATCHES", 10);
    let batch_size: usize = env_or("SOAK_BATCH_SIZE", 30);
    let parallel: usize = env_or("SOAK_PARALLEL", 8);

    let world = World::configured();
    world.configure_fake_agent_with_parallelism(
        FakeAgent::new().commit("soak work").exit(0),
        parallel,
    );
    world.commit_all("initial");
    let daemon = world.start_daemon();
    let daemon_pid = daemon["data"]["pid"].as_u64().expect("daemon pid") as u32;

    println!(
        "soak: {batches} batches x {batch_size} runs, parallelism {parallel}, daemon pid {daemon_pid}"
    );
    println!(
        "{:>5} {:>7} {:>8} {:>9} {:>7} {:>9} {:>5} {:>6} {:>7} {:>8} {:>8} {:>9} {:>8} {:>9} {:>9}",
        "batch",
        "merged",
        "post_s",
        "drain_s",
        "run/s",
        "rss_mb",
        "fds",
        "child",
        "zombie",
        "db_kb",
        "wal_kb",
        "worktree",
        "run_dir",
        "state_mb",
        "git_mb",
    );

    let mut samples: Vec<Sample> = Vec::new();
    let mut posted_total = 0usize;

    for batch in 1..=batches {
        let post_started = Instant::now();
        for index in 0..batch_size {
            let ticket = world.write_ticket(
                &format!("soak-{batch:03}-{index:03}.md"),
                "Churn one full run lifecycle.\n",
            );
            let output = world.sloop(&["post", ticket.to_str().expect("utf-8 ticket path")]);
            if !output.status.success() {
                let log = fs::read_to_string(world.daemon_log()).unwrap_or_default();
                let tail: Vec<&str> = log.lines().rev().take(40).collect();
                panic!(
                    "post failed in batch {batch}: {}\ndaemon log tail:\n{}",
                    String::from_utf8_lossy(&output.stderr),
                    tail.into_iter().rev().collect::<Vec<_>>().join("\n"),
                );
            }
        }
        posted_total += batch_size;
        let post_secs = post_started.elapsed().as_secs_f64();

        let drain_started = Instant::now();
        let target = posted_total as i64;
        wait_for(
            &format!("batch {batch} to drain to {target} merged tickets"),
            600,
            || {
                let status = status(&world);
                status["tickets"]["merged"].as_i64() == Some(target)
                    && status["gate"]["active_agents"].as_i64() == Some(0)
            },
        );
        let drain_secs = drain_started.elapsed().as_secs_f64();

        let sample = Sample {
            batch,
            merged: target,
            post_secs,
            drain_secs,
            runs_per_sec: batch_size as f64 / drain_secs,
            rss_kb: proc_status_kb(daemon_pid, "VmRSS:"),
            fd_count: fs::read_dir(format!("/proc/{daemon_pid}/fd"))
                .map(|entries| entries.count())
                .unwrap_or(0),
            children: child_pids(daemon_pid).len(),
            zombies: zombie_children(daemon_pid),
            db_kb: file_kb(&world.db_path()),
            wal_kb: file_kb(&world.db_path().with_extension("db-wal")),
            worktrees: dir_entry_count(&world.root().join(".worktrees")),
            run_dirs: dir_entry_count(&world.state_dir().join("runs")),
            state_kb: dir_size_kb(&world.state_dir()),
            git_kb: dir_size_kb(&world.root().join(".git")),
        };
        println!(
            "{:>5} {:>7} {:>8.2} {:>9.2} {:>7.2} {:>9.1} {:>5} {:>6} {:>7} {:>8} {:>8} {:>9} {:>8} {:>9.1} {:>9.1}",
            sample.batch,
            sample.merged,
            sample.post_secs,
            sample.drain_secs,
            sample.runs_per_sec,
            sample.rss_kb as f64 / 1024.0,
            sample.fd_count,
            sample.children,
            sample.zombies,
            sample.db_kb,
            sample.wal_kb,
            sample.worktrees,
            sample.run_dirs,
            sample.state_kb as f64 / 1024.0,
            sample.git_kb as f64 / 1024.0,
        );
        samples.push(sample);

        // Age settled worktrees past the 7-day retention default so periodic
        // reconciliation must reclaim them; every run is settled at this
        // point, so no live lease can expire under the jump.
        world.tick(std::time::Duration::from_secs(8 * 24 * 60 * 60));
    }

    let first = samples.first().expect("at least one batch");
    let last = samples.last().expect("at least one batch");

    assert_eq!(last.zombies, 0, "zombie children remain after the soak");
    assert_eq!(
        last.children, 0,
        "child processes remain after all runs settled"
    );
    assert!(
        last.fd_count <= first.fd_count + 16,
        "file descriptors grew from {} to {} across the soak",
        first.fd_count,
        last.fd_count
    );
    let status = status(&world);
    assert_eq!(
        status["tickets"]["merged"].as_i64(),
        Some(posted_total as i64),
        "daemon lost track of merged tickets"
    );

    println!(
        "soak complete: {posted_total} runs; rss {:.1} -> {:.1} MB; fds {} -> {}; throughput {:.2} -> {:.2} run/s",
        first.rss_kb as f64 / 1024.0,
        last.rss_kb as f64 / 1024.0,
        first.fd_count,
        last.fd_count,
        first.runs_per_sec,
        last.runs_per_sec,
    );
}

/// Isolates the post path from run churn: with the daemon paused nothing
/// dispatches, so per-post latency growth here indicts the post handler
/// itself (id reservation, dependency-cycle scan) rather than interleaved
/// scheduling work.
#[test]
#[ignore = "long-running scaling probe; run explicitly with --ignored --nocapture"]
fn post_latency_with_a_paused_daemon_stays_flat() {
    let tickets: usize = env_or("SOAK_POSTS", 1000);
    let window = 100;

    let world = World::configured();
    world.configure_fake_agent_with_parallelism(FakeAgent::new().exit(0), 1);
    world.commit_all("initial");
    world.start_daemon();
    assert!(world.sloop(&["pause"]).status.success());

    println!("{:>8} {:>10}", "tickets", "ms/post");
    let mut window_started = Instant::now();
    for index in 1..=tickets {
        let ticket = world.write_ticket(
            &format!("probe-{index:05}.md"),
            "Measure post latency at scale.\n",
        );
        let output = world.sloop(&["post", ticket.to_str().expect("utf-8 ticket path")]);
        assert!(
            output.status.success(),
            "post {index} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        if index % window == 0 {
            let elapsed = window_started.elapsed().as_secs_f64();
            println!("{:>8} {:>10.2}", index, elapsed * 1000.0 / window as f64);
            window_started = Instant::now();
        }
    }
}

fn status(world: &World) -> serde_json::Value {
    let output = world.sloop(&["status"]);
    assert!(
        output.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    World::json_stdout(&output)["data"].clone()
}

fn env_or(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// Like `support::wait_until`, with a caller-chosen deadline for whole-batch
/// drains that legitimately take minutes.
fn wait_for(what: &str, deadline_secs: u64, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + std::time::Duration::from_secs(deadline_secs);
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    panic!("timed out waiting for {what}");
}

/// A field from `/proc/<pid>/status`, in kilobytes.
fn proc_status_kb(pid: u32, field: &str) -> i64 {
    let Ok(contents) = fs::read_to_string(format!("/proc/{pid}/status")) else {
        return -1;
    };
    contents
        .lines()
        .find(|line| line.starts_with(field))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse().ok())
        .unwrap_or(-1)
}

/// Direct children of a process, from `/proc/<pid>/task/*/children`.
fn child_pids(pid: u32) -> Vec<u32> {
    let mut pids = Vec::new();
    if let Ok(tasks) = fs::read_dir(format!("/proc/{pid}/task")) {
        for task in tasks.flatten() {
            if let Ok(children) = fs::read_to_string(task.path().join("children")) {
                pids.extend(
                    children
                        .split_whitespace()
                        .filter_map(|child| child.parse::<u32>().ok()),
                );
            }
        }
    }
    pids
}

fn zombie_children(pid: u32) -> usize {
    child_pids(pid)
        .into_iter()
        .filter(|child| {
            fs::read_to_string(format!("/proc/{child}/stat"))
                .ok()
                .and_then(|stat| {
                    stat.rsplit_once(") ")
                        .map(|(_, rest)| rest.starts_with('Z'))
                })
                .unwrap_or(false)
        })
        .count()
}

fn file_kb(path: &Path) -> u64 {
    fs::metadata(path)
        .map(|meta| meta.len() / 1024)
        .unwrap_or(0)
}

fn dir_entry_count(path: &Path) -> usize {
    fs::read_dir(path)
        .map(|entries| entries.count())
        .unwrap_or(0)
}

fn dir_size_kb(path: &Path) -> u64 {
    fn walk(path: &Path) -> u64 {
        let Ok(entries) = fs::read_dir(path) else {
            return 0;
        };
        entries
            .flatten()
            .map(|entry| match entry.metadata() {
                Ok(meta) if meta.is_dir() => walk(&entry.path()),
                Ok(meta) => meta.len(),
                Err(_) => 0,
            })
            .sum()
    }
    walk(path) / 1024
}
