#![allow(dead_code)]

use std::cell::RefCell;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value;
use tempfile::TempDir;

pub struct FakeAgent {
    moves: Vec<FakeAgentMove>,
}

enum FakeAgentMove {
    BlockUntilReleased(String),
    Commit(String),
    Exit(i32),
    Note(String),
    Sleep(Duration),
}

impl FakeAgent {
    pub fn new() -> Self {
        Self { moves: Vec::new() }
    }

    pub fn block_until_released(mut self, marker: &str) -> Self {
        self.moves
            .push(FakeAgentMove::BlockUntilReleased(marker.to_owned()));
        self
    }

    pub fn commit(mut self, message: &str) -> Self {
        self.moves.push(FakeAgentMove::Commit(message.to_owned()));
        self
    }

    pub fn exit(mut self, code: i32) -> Self {
        self.moves.push(FakeAgentMove::Exit(code));
        self
    }

    pub fn note(mut self, text: &str) -> Self {
        self.moves.push(FakeAgentMove::Note(text.to_owned()));
        self
    }

    pub fn sleep(mut self, duration: Duration) -> Self {
        self.moves.push(FakeAgentMove::Sleep(duration));
        self
    }
}

pub struct World {
    root: TempDir,
    clock: TempDir,
    state: TempDir,
    runtime: TempDir,
    daemon_pids: RefCell<Vec<u32>>,
}

impl World {
    pub fn new() -> Self {
        let root = tempfile::tempdir().expect("create test directory");
        let status = Command::new("git")
            .args(["init", "--quiet"])
            .arg(root.path())
            .status()
            .expect("run git init");
        assert!(status.success(), "git init failed with {status}");
        let clock = tempfile::tempdir().expect("create test clock directory");
        let state = tempfile::tempdir().expect("create test state directory");
        let runtime = tempfile::tempdir().expect("create test runtime directory");
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time is after the epoch")
            .as_millis() as i64;
        fs::write(clock.path().join("now_ms"), now_ms.to_string()).expect("initialize test clock");

        Self {
            root,
            clock,
            state,
            runtime,
            daemon_pids: RefCell::new(Vec::new()),
        }
    }

    pub fn configured() -> Self {
        let world = Self::new();
        let config_dir = world.root().join(".agents/sloop");
        fs::create_dir_all(&config_dir).expect("create Sloop config directory");
        fs::write(
            config_dir.join("config.yaml"),
            "version: 1\nscheduler:\n  max_parallel_tasks: 1\n",
        )
        .expect("write Sloop config");
        fs::create_dir(config_dir.join("projects")).expect("create project directory");
        fs::write(
            config_dir.join("projects/default.md"),
            "---\nid: default\ntitle: Default\n---\nTickets not assigned to another project.\n",
        )
        .expect("write default project");
        fs::create_dir(config_dir.join("tickets")).expect("create ticket directory");
        world
    }

    pub fn configure_fake_agent(&self, agent: FakeAgent) {
        self.configure_fake_agent_with_parallelism(agent, 1);
    }

    pub fn configure_fake_agent_with_parallelism(
        &self,
        agent: FakeAgent,
        max_parallel_tasks: usize,
    ) {
        let script = self.root().join("fake-agent.sh");
        let mut body = String::from("#!/bin/sh\nset -eu\n");

        for agent_move in agent.moves {
            match agent_move {
                FakeAgentMove::BlockUntilReleased(marker) => {
                    let reached = self.fake_agent_marker_path(&marker, "reached");
                    let release = self.fake_agent_marker_path(&marker, "release");
                    for path in [&reached, &release] {
                        fs::create_dir_all(path.parent().expect("fake-agent marker parent"))
                            .expect("create fake-agent marker directory");
                        let _ = fs::remove_file(path);
                    }
                    body.push_str(&format!(
                        ": > {reached}\nwhile [ ! -e {release} ]; do sleep 0.01; done\n",
                        reached = shell_quote(&reached.to_string_lossy()),
                        release = shell_quote(&release.to_string_lossy()),
                    ));
                }
                FakeAgentMove::Commit(message) => body.push_str(&format!(
                    "git -c user.name=sloop-test-agent -c user.email=sloop-test-agent@example.invalid commit --quiet --allow-empty -m {}\n",
                    shell_quote(&message),
                )),
                FakeAgentMove::Exit(code) => body.push_str(&format!("exit {code}\n")),
                FakeAgentMove::Note(text) => body.push_str(&format!(
                    "{} --json note {} >/dev/null\n",
                    shell_quote(env!("CARGO_BIN_EXE_sloop")),
                    shell_quote(&text),
                )),
                FakeAgentMove::Sleep(duration) => {
                    body.push_str(&format!("sleep {}\n", duration.as_secs_f64()));
                }
            }
        }
        fs::write(&script, body).expect("write fake-agent script");

        fs::write(
            self.root().join(".agents/sloop/config.yaml"),
            format!(
                "version: 1\nscheduler:\n  max_parallel_tasks: {max_parallel_tasks}\nagent:\n  default_target: fake\n  targets:\n    fake:\n      cmd: [\"sh\", {}, \"{{prompt}}\"]\n",
                serde_json::to_string(&script.to_string_lossy()).expect("serialize fake-agent path"),
            ),
        )
        .expect("write fake-agent config");
    }

    pub fn fake_agent_reached(&self, marker: &str) -> bool {
        self.fake_agent_marker_path(marker, "reached").is_file()
    }

    pub fn release(&self, marker: &str) {
        let path = self.fake_agent_marker_path(marker, "release");
        fs::create_dir_all(path.parent().expect("fake-agent release parent"))
            .expect("create fake-agent release directory");
        fs::write(path, b"").expect("release fake agent");
    }

    fn fake_agent_marker_path(&self, marker: &str, state: &str) -> PathBuf {
        self.state_dir()
            .join("fake-agent")
            .join(format!("{marker}.{state}"))
    }

    pub fn root(&self) -> &Path {
        self.root.path()
    }

    pub fn state_dir(&self) -> PathBuf {
        self.state
            .path()
            .join("sloop/repositories")
            .join(self.repository_key())
    }

    pub fn runtime_dir(&self) -> PathBuf {
        let key = self.repository_key();
        let hash = key.rsplit_once('-').expect("repository key has hash").1;
        self.runtime.path().join("sloop").join(hash)
    }

    fn repository_key(&self) -> String {
        let root = self
            .root()
            .canonicalize()
            .expect("canonicalize test repository");
        sloop::paths::repository_key(&root)
    }

    pub fn operator_socket(&self) -> PathBuf {
        self.runtime_dir().join("operator.sock")
    }

    pub fn lock_path(&self) -> PathBuf {
        self.state_dir().join("daemon.lock")
    }

    pub fn daemon_log(&self) -> PathBuf {
        self.state_dir().join("logs/daemon.ndjson")
    }

    pub fn db_path(&self) -> PathBuf {
        self.state_dir().join("sloop.db")
    }

    pub fn worker_socket(&self, run: &str) -> PathBuf {
        self.runtime_dir()
            .join("workers")
            .join(format!("{run}.sock"))
    }

    pub fn now_ms(&self) -> i64 {
        fs::read_to_string(self.clock.path().join("now_ms"))
            .expect("read test clock")
            .trim()
            .parse()
            .expect("test clock is an integer")
    }

    pub fn tick(&self, duration: Duration) {
        let now_ms = self.now_ms();
        fs::write(
            self.clock.path().join("now_ms"),
            (now_ms + duration.as_millis() as i64).to_string(),
        )
        .expect("advance test clock");
    }

    /// Runs sloop with `--json`, as agents and scripts should: envelopes on
    /// stdout/stderr. Tests parse them with `json_stdout`.
    pub fn sloop(&self, args: &[&str]) -> Output {
        self.sloop_command(&Self::with_json(args))
            .output()
            .expect("run sloop")
    }

    pub fn sloop_in(&self, directory: &Path, args: &[&str]) -> Output {
        self.sloop_command_in(directory, &Self::with_json(args))
            .output()
            .expect("run sloop")
    }

    pub fn sloop_with_runtime(&self, args: &[&str], runtime: &Path) -> Output {
        self.sloop_command(&Self::with_json(args))
            .env("XDG_RUNTIME_DIR", runtime)
            .output()
            .expect("run sloop with alternate runtime")
    }

    /// Runs sloop without `--json`: the human-readable default output.
    pub fn sloop_plain(&self, args: &[&str]) -> Output {
        self.sloop_command(args).output().expect("run sloop")
    }

    fn with_json<'a>(args: &[&'a str]) -> Vec<&'a str> {
        // Prepended so verbs with trailing arguments (`note`) cannot
        // swallow the flag.
        let mut with_flag = Vec::with_capacity(args.len() + 1);
        with_flag.push("--json");
        with_flag.extend_from_slice(args);
        with_flag
    }

    pub fn start_daemon(&self) -> Value {
        let output = self.sloop(&["daemon"]);
        assert!(
            output.status.success(),
            "daemon failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let response = Self::json_stdout(&output);
        let pid = response["data"]["pid"].as_u64().expect("daemon pid") as u32;
        let mut daemon_pids = self.daemon_pids.borrow_mut();
        if !daemon_pids.contains(&pid) {
            daemon_pids.push(pid);
        }
        drop(daemon_pids);
        response
    }

    pub fn kill_daemon(&self, pid: u32) {
        let status = Command::new("kill")
            .args(["-9", &pid.to_string()])
            .status()
            .expect("kill daemon");
        assert!(status.success(), "kill failed with {status}");
        wait_until("the crashed daemon exits", || !process_alive(pid));
    }

    pub fn kill_process_group(&self, leader: u32) {
        let status = Command::new("kill")
            .args(["-9", "--", &format!("-{leader}")])
            .status()
            .expect("kill process group");
        assert!(status.success(), "kill process group failed with {status}");
        wait_until("the process group leader exits", || !process_alive(leader));
    }

    pub fn run_process_id(&self, run_id: &str) -> u32 {
        let connection = rusqlite::Connection::open(self.db_path()).expect("open state database");
        let pid: i64 = connection
            .query_row("SELECT pid FROM runs WHERE id = ?1", [run_id], |row| {
                row.get(0)
            })
            .expect("read run pid");
        u32::try_from(pid).expect("run pid fits u32")
    }

    pub fn run_note_count(&self, run_id: &str) -> i64 {
        let connection = rusqlite::Connection::open(self.db_path()).expect("open state database");
        connection
            .query_row(
                "SELECT COUNT(*) FROM notes WHERE run_id = ?1",
                [run_id],
                |row| row.get(0),
            )
            .expect("count run notes")
    }

    pub fn run_evidence(&self, run_id: &str, kind: &str) -> Option<Value> {
        let connection = rusqlite::Connection::open(self.db_path()).expect("open state database");
        let mut statement = connection
            .prepare("SELECT data_json FROM run_evidence WHERE run_id = ?1 AND kind = ?2")
            .expect("prepare evidence query");
        let mut rows = statement.query([run_id, kind]).expect("query run evidence");
        rows.next().expect("read run evidence").map(|row| {
            let data: String = row.get(0).expect("read evidence JSON");
            serde_json::from_str(&data).expect("evidence is JSON")
        })
    }

    pub fn arm_test_hook(&self, name: &str) {
        let directory = self.state.path().join("test-hooks");
        fs::create_dir_all(&directory).expect("create test hook directory");
        for state in ["reached", "release"] {
            let _ = fs::remove_file(directory.join(format!("{name}.{state}")));
        }
        fs::write(directory.join(format!("{name}.armed")), b"").expect("arm test hook");
    }

    pub fn test_hook_reached(&self, name: &str) -> bool {
        self.state
            .path()
            .join("test-hooks")
            .join(format!("{name}.reached"))
            .is_file()
    }

    pub fn release_test_hook(&self, name: &str) {
        fs::write(
            self.state
                .path()
                .join("test-hooks")
                .join(format!("{name}.release")),
            b"",
        )
        .expect("release test hook");
    }

    pub fn operator_exchange(&self, request: &str) -> Value {
        Self::socket_exchange(&self.operator_socket(), request)
    }

    /// Sends one raw envelope line over a Unix socket and returns the reply.
    /// Lets tests speak to per-run worker sockets directly.
    pub fn socket_exchange(socket: &Path, request: &str) -> Value {
        let mut stream = UnixStream::connect(socket).expect("connect to socket");
        stream.write_all(request.as_bytes()).expect("write request");
        stream.write_all(b"\n").expect("finish request");

        let mut response = String::new();
        BufReader::new(stream)
            .read_line(&mut response)
            .expect("read response");
        serde_json::from_str(response.trim_end()).expect("response is JSON")
    }

    pub fn write_ticket(&self, name: &str, body: &str) -> PathBuf {
        let relative = PathBuf::from(".agents/sloop/tickets").join(name);
        let title = name.strip_suffix(".md").unwrap_or(name).replace('-', " ");
        let content = if body.starts_with("---\n") {
            body.replacen("---\n", &format!("---\nname: {title}\nblocked_by: []\n"), 1)
        } else {
            format!("---\nname: {title}\nblocked_by: []\n---\n{body}")
        };
        fs::write(self.root().join(&relative), content).expect("write ticket");
        relative
    }

    /// Commits everything in the world's repository so worktrees have a HEAD
    /// to branch from.
    pub fn commit_all(&self, message: &str) {
        let status = Command::new("git")
            .args([
                "-c",
                "user.name=sloop-test",
                "-c",
                "user.email=sloop-test@example.invalid",
                "add",
                "-A",
            ])
            .current_dir(self.root())
            .status()
            .expect("run git add");
        assert!(status.success(), "git add failed with {status}");
        let status = Command::new("git")
            .args([
                "-c",
                "user.name=sloop-test",
                "-c",
                "user.email=sloop-test@example.invalid",
                "commit",
                "--quiet",
                "-m",
                message,
            ])
            .current_dir(self.root())
            .status()
            .expect("run git commit");
        assert!(status.success(), "git commit failed with {status}");
    }

    pub fn json_stdout(output: &Output) -> Value {
        serde_json::from_slice(&output.stdout).expect("stdout is JSON")
    }

    /// Parses whichever stream carried the envelope; errors land on stderr.
    pub fn json_stdout_or_stderr(output: &Output) -> Value {
        if output.stdout.is_empty() {
            serde_json::from_slice(&output.stderr).expect("stderr is JSON")
        } else {
            serde_json::from_slice(&output.stdout).expect("stdout is JSON")
        }
    }

    pub fn worker_exchange(&self, args: &[&str], response: Value) -> (Output, Value) {
        let socket = self.root().join("worker.sock");
        let listener = UnixListener::bind(&socket).expect("bind worker socket");
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept worker request");
            let mut request = String::new();
            BufReader::new(&stream)
                .read_line(&mut request)
                .expect("read worker request");

            let mut stream = stream;
            if serde_json::to_writer(&mut stream, &response).is_ok() {
                let _ = stream.write_all(b"\n");
            }

            serde_json::from_str(request.trim()).expect("worker request is JSON")
        });

        let output = self
            .sloop_command(args)
            .env("SLOOP_SOCKET", &socket)
            .env("SLOOP_TOKEN", "test-worker-token")
            .output()
            .expect("run worker command");

        // Unblock the fixture when an unimplemented client never opened the socket.
        if let Ok(mut stream) = UnixStream::connect(&socket) {
            let _ = stream.write_all(b"{}\n");
        }

        let request = server.join().expect("worker server thread");
        (output, request)
    }

    fn sloop_command(&self, args: &[&str]) -> Command {
        self.sloop_command_in(self.root(), args)
    }

    fn sloop_command_in(&self, directory: &Path, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_sloop"));
        command
            .args(args)
            .current_dir(directory)
            .env("HOME", self.root().join("home"))
            .env("XDG_STATE_HOME", self.state.path())
            .env("XDG_RUNTIME_DIR", self.runtime.path())
            .env_remove("SLOOP_SOCKET")
            .env_remove("SLOOP_TOKEN")
            .env("SLOOP_TEST_CLOCK_PATH", self.clock.path().join("now_ms"))
            .env("SLOOP_TEST_HOOK_DIR", self.state.path().join("test-hooks"));
        command
    }
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

/// Whether a PID currently exists (signal 0 probe).
pub fn process_alive(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

/// Polls an observable condition until it holds or a deadline passes. Tests
/// must wait on state, never sleep and hope.
pub fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if condition() {
            return;
        }
        thread::sleep(std::time::Duration::from_millis(25));
    }
    panic!("timed out waiting until {what}");
}

/// Like `wait_until`, with a 20-second deadline for probes on multi-second
/// timers such as the daemon liveness tick.
pub fn wait_until_slow(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    while std::time::Instant::now() < deadline {
        if condition() {
            return;
        }
        thread::sleep(std::time::Duration::from_millis(100));
    }
    panic!("timed out waiting until {what}");
}

impl Drop for World {
    fn drop(&mut self) {
        // Layer 1: a clean stop through the public verb; never autostarts.
        let _ = self.sloop_command(&["--json", "stop", "--force"]).output();

        // Layer 2: identity-checked kill of whatever owns the lockfile,
        // catching daemons autostarted by arbitrary verbs during the test.
        let lock = self.lock_path();
        if let Some(identity) = sloop::daemon::read_lock_identity(&lock) {
            let cmdline = fs::read(format!("/proc/{}/cmdline", identity.pid)).unwrap_or_default();
            if String::from_utf8_lossy(&cmdline).contains("sloop") {
                let _ = Command::new("kill")
                    .args(["-9", &identity.pid.to_string()])
                    .status();
            }
        }

        // Layer 3: pids tests registered explicitly (kept as a backstop for
        // daemons whose lockfile was already deleted).
        for pid in self.daemon_pids.get_mut().drain(..) {
            let _ = Command::new("kill")
                .arg(pid.to_string())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
        }
    }
}
