use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as BASE64_TOKEN;
use tokio::net::UnixListener;

use super::{
    AgentProcessCheckpoint, ExecProcessCheckpoint, ExecutionEvidence, ExecutionFailure,
    ProcessIdentity, RunnerError, StageExecution, StageHooks, StageOrder, WorkerCredentials,
};
use crate::clock::Clock;
use crate::run_log::{OutputSource, OutputStream, RunLogWriter};

const WORKER_BOOTSTRAP_PROMPT: &str = include_str!("../worker-instructions.md").trim_ascii();

/// A launched agent whose worker socket can be registered before supervision
/// moves to a blocking task.
pub struct LaunchedAgent {
    child: Child,
    readers: Vec<std::thread::JoinHandle<bool>>,
    process: ProcessIdentity,
    started_at_ms: i64,
    worker_listener: Option<UnixListener>,
    worker: WorkerCredentials,
}

pub struct AgentCompletion {
    pub evidence: ExecutionEvidence,
    pub wait_error: Option<String>,
}

impl LaunchedAgent {
    pub fn process(&self) -> ProcessIdentity {
        self.process
    }

    pub fn worker(&self) -> &WorkerCredentials {
        &self.worker
    }

    pub fn take_worker_listener(&mut self) -> UnixListener {
        self.worker_listener
            .take()
            .expect("worker listener is taken only once")
    }

    pub fn wait(mut self, clock: &dyn Clock) -> AgentCompletion {
        let (exit_code, wait_error) = match self.child.wait() {
            Ok(status) => (status.code(), None),
            Err(error) => (None, Some(error.to_string())),
        };
        let stragglers_killed = kill_straggler_process_group(self.process.process_group_id);
        let mut output_capture_complete = true;
        for reader in self.readers {
            output_capture_complete &= reader.join().unwrap_or(false);
        }
        AgentCompletion {
            evidence: ExecutionEvidence {
                exit_code,
                started_at_ms: self.started_at_ms,
                finished_at_ms: clock.now_ms(),
                process: Some(self.process),
                output_capture_complete,
                stragglers_killed,
            },
            wait_error,
        }
    }
}

pub fn launch_agent<H: StageHooks>(
    order: StageOrder,
    hooks: &H,
    clock: &dyn Clock,
) -> Result<LaunchedAgent, RunnerError<H::Error>> {
    let StageExecution::Agent(launch) = &order.execution else {
        return Err(RunnerError::Execution(
            "agent launch requires an agent stage order".into(),
        ));
    };
    let Some(program) = launch.argv.first() else {
        return Err(RunnerError::Execution("agent argv is empty".into()));
    };

    fs::create_dir_all(
        order
            .worktree
            .parent()
            .ok_or_else(|| RunnerError::Execution("worktree has no parent".into()))?,
    )
    .map_err(|error| RunnerError::Execution(error.to_string()))?;
    let git = Command::new("git")
        .args(["worktree", "add", "--quiet", "-b", &order.branch])
        .arg(&order.worktree)
        .current_dir(&launch.repository)
        .output()
        .map_err(|error| RunnerError::Execution(error.to_string()))?;
    if !git.status.success() {
        return Err(RunnerError::Execution(format!(
            "git worktree add failed: {}",
            String::from_utf8_lossy(&git.stderr).trim()
        )));
    }

    let output_log = RunLogWriter::open(&order.output_path)
        .map_err(|error| RunnerError::Execution(error.to_string()))?;
    let worker_token = generate_worker_token().map_err(RunnerError::Execution)?;
    let socket_path = &launch.worker_socket_path;
    fs::create_dir_all(socket_path.parent().expect("worker sockets have a parent"))
        .map_err(|error| RunnerError::Execution(error.to_string()))?;
    let _ = fs::remove_file(socket_path);
    let worker_listener = UnixListener::bind(socket_path)
        .map_err(|error| RunnerError::Execution(error.to_string()))?;
    fs::set_permissions(socket_path, fs::Permissions::from_mode(0o600))
        .map_err(|error| RunnerError::Execution(error.to_string()))?;

    let mut command = Command::new(program);
    command
        .args(&launch.argv[1..])
        .current_dir(&order.worktree)
        .envs(launch.environment.iter().cloned())
        .env("SLOOP_SOCKET", socket_path)
        .env("SLOOP_TOKEN", &worker_token)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let mut child = command
        .spawn()
        .map_err(|error| RunnerError::Execution(error.to_string()))?;
    let readers = vec![
        spawn_output_reader(
            child.stdout.take().expect("stdout was piped"),
            output_log.clone(),
            OutputSource::Agent,
            None,
            OutputStream::Stdout,
        ),
        spawn_output_reader(
            child.stderr.take().expect("stderr was piped"),
            output_log,
            OutputSource::Agent,
            None,
            OutputStream::Stderr,
        ),
    ];

    let pid = child.id();
    let process = ProcessIdentity {
        pid,
        start_time: process_start_time(pid),
        process_group_id: pid,
    };
    let started_at_ms = clock.now_ms();
    let worker = WorkerCredentials {
        socket: socket_path.clone(),
        token: worker_token,
    };
    let checkpoint = AgentProcessCheckpoint {
        run_id: order.run_id,
        branch: order.branch,
        worktree: order.worktree,
        process,
        worker: worker.clone(),
        started_at_ms,
    };
    if let Err(error) = hooks.record_agent_process(&checkpoint) {
        kill_process_group(process);
        let _ = child.wait();
        for reader in readers {
            let _ = reader.join();
        }
        return Err(RunnerError::Hook(error));
    }

    Ok(LaunchedAgent {
        child,
        readers,
        process,
        started_at_ms,
        worker_listener: Some(worker_listener),
        worker,
    })
}

pub fn run_exec_stage<H: StageHooks>(
    order: &StageOrder,
    hooks: &H,
    clock: &dyn Clock,
) -> Result<ExecutionEvidence, ExecutionFailure<H::Error>> {
    let started_at_ms = clock.now_ms();
    let failed = |process, error| ExecutionFailure {
        evidence: ExecutionEvidence {
            exit_code: None,
            started_at_ms,
            finished_at_ms: clock.now_ms(),
            process,
            output_capture_complete: false,
            stragglers_killed: false,
        },
        error,
    };
    let StageExecution::Exec(launch) = &order.execution else {
        return Err(failed(
            None,
            RunnerError::Execution("exec launch requires an exec stage order".into()),
        ));
    };
    let Some(program) = launch.argv.first() else {
        return Err(failed(
            None,
            RunnerError::Execution("exec argv is empty".into()),
        ));
    };
    if hooks.cancellation_requested(&order.run_id) {
        return Err(failed(
            None,
            RunnerError::Execution("stage cancelled before launch".into()),
        ));
    }
    let output_log = RunLogWriter::open(&order.output_path).map_err(|error| {
        failed(
            None,
            RunnerError::Execution(format!("cannot open run output: {error}")),
        )
    })?;

    let mut command = if launch.worker.is_some() {
        let mut command = Command::new("sh");
        command
            .args([
                "-c",
                "IFS= read -r _ || exit 125; exec \"$@\"",
                "sloop-stage",
            ])
            .args(&launch.argv);
        command
    } else {
        let mut command = Command::new(program);
        command.args(&launch.argv[1..]);
        command
    };
    command
        .current_dir(&order.worktree)
        .envs(launch.environment.iter().cloned())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    if let Some(worker) = &launch.worker {
        command
            .env("SLOOP_SOCKET", &worker.socket)
            .env("SLOOP_TOKEN", &worker.token)
            .stdin(Stdio::piped());
    } else {
        command
            .env_remove("SLOOP_SOCKET")
            .env_remove("SLOOP_TOKEN")
            .stdin(Stdio::null());
    }
    let mut child = command
        .spawn()
        .map_err(|error| failed(None, RunnerError::Execution(error.to_string())))?;
    let mut gate = launch
        .worker
        .as_ref()
        .map(|_| child.stdin.take().expect("reported stage stdin was piped"));
    let pid = child.id();
    let process = ProcessIdentity {
        pid,
        start_time: process_start_time(pid),
        process_group_id: pid,
    };
    let Some(start_time) = process.start_time else {
        kill_process_group(process);
        let _ = child.wait();
        return Err(failed(
            Some(process),
            RunnerError::Execution("cannot identify exec process".into()),
        ));
    };
    let readers = vec![
        spawn_output_reader(
            child.stdout.take().expect("stdout was piped"),
            output_log.clone(),
            OutputSource::Aftercare,
            Some(order.stage.clone()),
            OutputStream::Stdout,
        ),
        spawn_output_reader(
            child.stderr.take().expect("stderr was piped"),
            output_log,
            OutputSource::Aftercare,
            Some(order.stage.clone()),
            OutputStream::Stderr,
        ),
    ];
    if order.stage == "test" {
        wait_for_test_hook("before-test-process-checkpoint");
    }
    wait_for_test_hook(&format!(
        "before-aftercare-process-checkpoint-{}",
        order.stage
    ));
    let checkpoint = ExecProcessCheckpoint {
        run_id: order.run_id.clone(),
        stage: order.stage.clone(),
        process: ProcessIdentity {
            start_time: Some(start_time),
            ..process
        },
        started_at_ms: clock.now_ms(),
    };
    if let Err(error) = hooks.record_exec_process(&checkpoint) {
        kill_process_group_if_matches(checkpoint.process);
        let _ = child.wait();
        join_readers(readers);
        return Err(failed(Some(process), RunnerError::Hook(error)));
    }
    wait_for_test_hook(&format!(
        "after-aftercare-process-checkpoint-{}",
        order.stage
    ));
    if hooks.cancellation_requested(&order.run_id) {
        kill_process_group_if_matches(checkpoint.process);
        let _ = child.wait();
        join_readers(readers);
        return Err(failed(
            Some(process),
            RunnerError::Execution("stage cancelled after launch".into()),
        ));
    }
    if let Some(gate) = gate.as_mut()
        && gate.write_all(b"run\n").is_err()
    {
        kill_process_group_if_matches(checkpoint.process);
        let _ = child.wait();
        join_readers(readers);
        return Err(failed(
            Some(process),
            RunnerError::Execution("cannot release exec process".into()),
        ));
    }
    drop(gate);

    let status = child.wait();
    let stragglers_killed = kill_straggler_process_group(pid);
    let output_capture_complete = join_readers(readers);
    if !output_capture_complete {
        return Err(ExecutionFailure {
            evidence: ExecutionEvidence {
                exit_code: None,
                started_at_ms,
                finished_at_ms: clock.now_ms(),
                process: Some(process),
                output_capture_complete: false,
                stragglers_killed,
            },
            error: RunnerError::Execution("exec output capture was incomplete".into()),
        });
    }
    let status = match status {
        Ok(status) => status,
        Err(error) => {
            return Err(ExecutionFailure {
                evidence: ExecutionEvidence {
                    exit_code: None,
                    started_at_ms,
                    finished_at_ms: clock.now_ms(),
                    process: Some(process),
                    output_capture_complete: true,
                    stragglers_killed,
                },
                error: RunnerError::Execution(error.to_string()),
            });
        }
    };
    Ok(ExecutionEvidence {
        exit_code: status.code(),
        started_at_ms,
        finished_at_ms: clock.now_ms(),
        process: Some(process),
        output_capture_complete: true,
        stragglers_killed,
    })
}

fn spawn_output_reader(
    pipe: impl Read + Send + 'static,
    log: RunLogWriter,
    source: OutputSource,
    stage: Option<String>,
    stream: OutputStream,
) -> std::thread::JoinHandle<bool> {
    std::thread::spawn(move || {
        let mut pipe = pipe;
        let mut buffer = [0u8; 8192];
        loop {
            match pipe.read(&mut buffer) {
                Ok(0) => return true,
                Ok(read) => {
                    if log
                        .append(source, stage.as_deref(), stream, &buffer[..read])
                        .is_err()
                    {
                        return false;
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => return false,
            }
        }
    })
}

fn join_readers(readers: Vec<std::thread::JoinHandle<bool>>) -> bool {
    readers.into_iter().fold(true, |complete, reader| {
        complete & reader.join().unwrap_or(false)
    })
}

pub fn compose_worker_prompt(root: &Path) -> Result<String, String> {
    let path = root.join(".agents/sloop/instructions.md");
    match fs::read_to_string(&path) {
        Ok(instructions) => Ok(format!("{WORKER_BOOTSTRAP_PROMPT}\n\n{instructions}")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Ok(WORKER_BOOTSTRAP_PROMPT.to_owned())
        }
        Err(error) => Err(format!("cannot read {}: {error}", path.display())),
    }
}

fn generate_worker_token() -> Result<String, String> {
    let mut bytes = [0u8; 32];
    let mut urandom = fs::File::open("/dev/urandom").map_err(|error| error.to_string())?;
    urandom
        .read_exact(&mut bytes)
        .map_err(|error| error.to_string())?;
    Ok(BASE64_TOKEN.encode(bytes))
}

pub fn run_output_path(state_dir: &Path, run_id: &str) -> PathBuf {
    state_dir.join("runs").join(run_id).join("output.ndjson")
}

pub fn worker_socket_path(runtime_dir: &Path, run_id: &str) -> PathBuf {
    runtime_dir.join("workers").join(format!("{run_id}.sock"))
}

pub fn process_start_time(pid: u32) -> Option<i64> {
    #[cfg(target_os = "linux")]
    {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let after_command = &stat[stat.rfind(')')? + 1..];
        after_command
            .split_whitespace()
            .nth(19)
            .and_then(|field| field.parse().ok())
    }

    #[cfg(not(target_os = "linux"))]
    {
        let output = Command::new("ps")
            .args(["-o", "lstart=", "-p", &pid.to_string()])
            .output()
            .ok()?;
        if !output.status.success() || output.stdout.iter().all(u8::is_ascii_whitespace) {
            return None;
        }
        let mut hash = 0xcbf29ce484222325_u64;
        for byte in output.stdout {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        Some(hash as i64)
    }
}

pub fn process_identity_matches(pid: u32, expected_start_time: Option<i64>) -> bool {
    matches!(
        (expected_start_time, process_start_time(pid)),
        (Some(expected), Some(actual)) if expected == actual
    )
}

pub fn kill_straggler_process_group(group: u32) -> bool {
    let group = -(group as libc::pid_t);
    let stragglers_present = unsafe { libc::kill(group, 0) } == 0;
    if stragglers_present {
        unsafe {
            libc::kill(group, libc::SIGKILL);
        }
    }
    stragglers_present
}

fn kill_process_group(process: ProcessIdentity) {
    unsafe {
        libc::kill(-(process.process_group_id as libc::pid_t), libc::SIGKILL);
    }
}

fn kill_process_group_if_matches(process: ProcessIdentity) {
    if process_identity_matches(process.pid, process.start_time) {
        kill_process_group(process);
    }
}

#[cfg(debug_assertions)]
pub fn wait_for_test_hook(name: &str) {
    use std::time::Duration;

    let Some(directory) = std::env::var_os("SLOOP_TEST_HOOK_DIR").map(PathBuf::from) else {
        return;
    };
    let armed = directory.join(format!("{name}.armed"));
    if !armed.is_file() {
        return;
    }
    let reached = directory.join(format!("{name}.reached"));
    let release = directory.join(format!("{name}.release"));
    if fs::write(&reached, b"").is_err() {
        return;
    }
    while !release.is_file() {
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(not(debug_assertions))]
pub fn wait_for_test_hook(_name: &str) {}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::fs;

    use tempfile::tempdir;

    use super::{WORKER_BOOTSTRAP_PROMPT, compose_worker_prompt, run_exec_stage};
    use crate::clock::SystemClock;
    use crate::config::{AgentTarget, expand_agent_cmd};
    use crate::runner::{
        AgentProcessCheckpoint, ExecLaunch, ExecProcessCheckpoint, StageExecution, StageHooks,
        StageOrder,
    };

    struct NoopHooks;

    impl StageHooks for NoopHooks {
        type Error = Infallible;

        fn cancellation_requested(&self, _run_id: &str) -> bool {
            false
        }

        fn record_agent_process(
            &self,
            _checkpoint: &AgentProcessCheckpoint,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        fn record_exec_process(
            &self,
            _checkpoint: &ExecProcessCheckpoint,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    fn target(cmd: &[&str], model: Option<&str>, effort: Option<&str>) -> AgentTarget {
        AgentTarget {
            cmd: cmd.iter().map(|argument| (*argument).to_owned()).collect(),
            model: model.map(str::to_owned),
            effort: effort.map(str::to_owned),
        }
    }

    #[test]
    fn agent_command_expands_ticket_model_and_effort() {
        let template = target(
            &[
                "agent",
                "--model={model}",
                "--effort",
                "{effort}",
                "prompt={prompt}",
            ],
            None,
            None,
        );

        assert_eq!(
            expand_agent_cmd(&template, Some("sonnet"), Some("medium"), "assignment").unwrap(),
            [
                "agent",
                "--model=sonnet",
                "--effort",
                "medium",
                "prompt=assignment"
            ]
        );
    }

    #[test]
    fn agent_command_rejects_a_missing_ticket_field() {
        let template = target(&["agent", "{model}"], None, Some("medium"));

        assert_eq!(
            expand_agent_cmd(&template, None, Some("medium"), "assignment"),
            Err("does not specify `model`".to_owned())
        );
    }

    #[test]
    fn agent_command_falls_back_to_target_defaults() {
        let template = target(
            &["agent", "{model}", "{effort}", "{prompt}"],
            Some("opus"),
            Some("high"),
        );

        assert_eq!(
            expand_agent_cmd(&template, None, None, "assignment").unwrap(),
            ["agent", "opus", "high", "assignment"]
        );
    }

    #[test]
    fn agent_command_prefers_ticket_values_over_target_defaults() {
        let template = target(
            &["agent", "{model}", "{effort}", "{prompt}"],
            Some("opus"),
            Some("high"),
        );

        assert_eq!(
            expand_agent_cmd(&template, Some("haiku"), Some("low"), "assignment").unwrap(),
            ["agent", "haiku", "low", "assignment"]
        );
    }

    #[test]
    fn scripted_exec_returns_factual_evidence_without_a_daemon_or_store() {
        let directory = tempdir().unwrap();
        let order = StageOrder {
            run_id: "R1".into(),
            stage: "check".into(),
            execution: StageExecution::Exec(ExecLaunch {
                argv: vec!["sh".into(), "-c".into(), "printf runner-output".into()],
                worker: None,
                environment: Vec::new(),
            }),
            worktree: directory.path().into(),
            branch: "sloop/T1-a1-R1".into(),
            output_path: directory.path().join("output.ndjson"),
        };

        let evidence = run_exec_stage(&order, &NoopHooks, &SystemClock).unwrap();

        assert_eq!(evidence.exit_code, Some(0));
        assert!(evidence.output_capture_complete);
        assert!(evidence.process.is_some());
    }

    #[test]
    fn worker_prompt_uses_the_builtin_when_instructions_are_absent() {
        let root = tempdir().unwrap();

        assert_eq!(
            compose_worker_prompt(root.path()).unwrap(),
            WORKER_BOOTSTRAP_PROMPT
        );
    }

    #[test]
    fn worker_prompt_appends_repository_instructions() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/instructions.md"),
            "Use repository conventions.\n",
        )
        .unwrap();

        assert_eq!(
            compose_worker_prompt(root.path()).unwrap(),
            format!("{WORKER_BOOTSTRAP_PROMPT}\n\nUse repository conventions.\n")
        );
    }
}
