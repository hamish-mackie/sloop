use std::fs;
use std::io::{self, Read};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as BASE64_TOKEN;
use tokio::net::UnixListener;

use super::{DispatcherState, WORKER_BOOTSTRAP_PROMPT};
use crate::config::expand_agent_cmd;
use crate::run_log::{OutputSource, OutputStream, RunLogWriter};

/// A supervised agent process plus the reader threads draining its pipes;
/// the readers finish when the pipes close, and joining them guarantees
/// capture is flushed before exit evidence is reported. Worktree, branch,
/// and log writer stay with the supervisor for evidence gathering after
/// the exit.
pub(super) struct LaunchedRun {
    pub(super) child: Child,
    pub(super) readers: Vec<std::thread::JoinHandle<bool>>,
    pub(super) worktree: PathBuf,
    pub(super) branch: String,
    pub(super) output_log: RunLogWriter,
    /// The bound per-run worker socket, handed to an accept-loop task once
    /// the launch is registered.
    pub(super) worker_listener: UnixListener,
    pub(super) worker_token: String,
    pub(super) worker_socket_path: PathBuf,
}

/// Creates the run's branch and isolated worktree, starts the configured
/// agent as its own process group, and records durable process identity.
pub(super) fn launch_agent(
    state: &DispatcherState,
    run_id: &str,
    ticket_id: &str,
    attempt: i64,
) -> Result<LaunchedRun, String> {
    let agent = state
        .agent
        .as_ref()
        .ok_or_else(|| "no agent targets configured".to_owned())?;
    let ticket = state
        .store
        .ticket(ticket_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("ticket `{ticket_id}` no longer exists"))?;
    let target = ticket
        .target
        .as_deref()
        .ok_or_else(|| format!("ticket `{ticket_id}` does not specify an agent target"))?;
    let template = agent
        .targets
        .get(target)
        .ok_or_else(|| format!("ticket `{ticket_id}` names unknown agent target `{target}`"))?;
    let prompt = compose_worker_prompt(&state.root)?;
    let cmd = expand_agent_cmd(
        template,
        ticket.model.as_deref(),
        ticket.effort.as_deref(),
        &prompt,
    )
    .map_err(|error| format!("ticket `{ticket_id}` {error}"))?;
    let executable = std::env::current_exe()
        .map_err(|error| format!("cannot locate sloop executable: {error}"))?;
    let executable_dir = executable
        .parent()
        .ok_or_else(|| "sloop executable has no parent directory".to_owned())?;
    let mut path_entries = vec![executable_dir.to_path_buf()];
    if let Some(path) = std::env::var_os("PATH") {
        path_entries.extend(std::env::split_paths(&path));
    }
    let path = std::env::join_paths(path_entries)
        .map_err(|error| format!("cannot construct agent PATH: {error}"))?;
    // `retry` resets attempts, so the run ID keeps preserved failed branches
    // from colliding with a later run's first attempt.
    let branch = format!("sloop/{ticket_id}-a{attempt}-{run_id}");
    fs::create_dir_all(&state.worktree_dir).map_err(|error| error.to_string())?;
    let worktree = state.worktree_dir.join(run_id);

    let git = Command::new("git")
        .args(["worktree", "add", "--quiet", "-b", &branch])
        .arg(&worktree)
        .current_dir(&state.root)
        .output()
        .map_err(|error| error.to_string())?;
    if !git.status.success() {
        return Err(format!(
            "git worktree add failed: {}",
            String::from_utf8_lossy(&git.stderr).trim()
        ));
    }

    let output_log = RunLogWriter::open(&run_output_path(&state.state_dir, run_id))
        .map_err(|error| error.to_string())?;

    let worker_token = generate_worker_token()?;
    let socket_path = worker_socket_path(&state.runtime_dir, run_id);
    fs::create_dir_all(socket_path.parent().expect("worker sockets have a parent"))
        .map_err(|error| error.to_string())?;
    let _ = fs::remove_file(&socket_path);
    let worker_listener = UnixListener::bind(&socket_path).map_err(|error| error.to_string())?;
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))
        .map_err(|error| error.to_string())?;

    let mut command = Command::new(&cmd[0]);
    command
        .args(&cmd[1..])
        .current_dir(&worktree)
        .env("SLOOP_RUN_ID", run_id)
        .env("SLOOP_TICKET_ID", ticket_id)
        .env("SLOOP_BIN", &executable)
        .env("PATH", path)
        .env("SLOOP_SOCKET", &socket_path)
        .env("SLOOP_TOKEN", &worker_token)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let mut child = command.spawn().map_err(|error| error.to_string())?;
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
            output_log.clone(),
            OutputSource::Agent,
            None,
            OutputStream::Stderr,
        ),
    ];

    let pid = child.id();
    if let Err(error) = state.store.mark_run_running(
        run_id,
        &branch,
        &worktree.to_string_lossy(),
        pid,
        process_start_time(pid),
        pid, // process_group(0) makes the child its own group leader
        &worker_token,
        &socket_path.to_string_lossy(),
        state.clock.now_ms(),
    ) {
        unsafe {
            libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
        }
        let _ = child.wait();
        for reader in readers {
            let _ = reader.join();
        }
        return Err(error.to_string());
    }
    Ok(LaunchedRun {
        child,
        readers,
        worktree,
        branch,
        output_log,
        worker_listener,
        worker_token,
        worker_socket_path: socket_path,
    })
}

/// Continuously drains one child pipe into the run log so a verbose agent
/// can never fill the pipe and deadlock. Returns whether every chunk was
/// durably captured.
pub(super) fn spawn_output_reader(
    pipe: impl Read + Send + 'static,
    log: RunLogWriter,
    source: OutputSource,
    stage: Option<&'static str>,
    stream: OutputStream,
) -> std::thread::JoinHandle<bool> {
    std::thread::spawn(move || {
        let mut pipe = pipe;
        let mut buffer = [0u8; 8192];
        loop {
            match pipe.read(&mut buffer) {
                Ok(0) => return true,
                Ok(read) => {
                    if log.append(source, stage, stream, &buffer[..read]).is_err() {
                        return false;
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => return false,
            }
        }
    })
}

pub(super) fn compose_worker_prompt(root: &Path) -> Result<String, String> {
    let path = root.join(".agents/sloop/instructions.md");
    match fs::read_to_string(&path) {
        Ok(instructions) => Ok(format!("{WORKER_BOOTSTRAP_PROMPT}\n\n{instructions}")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Ok(WORKER_BOOTSTRAP_PROMPT.to_owned())
        }
        Err(error) => Err(format!("cannot read {}: {error}", path.display())),
    }
}

/// 32 random bytes from the kernel, encoded for an environment variable.
/// Guessing it stops accidents, which is the threat model; same-uid
/// isolation needs a real sandbox.
pub(super) fn generate_worker_token() -> Result<String, String> {
    let mut bytes = [0u8; 32];
    let mut urandom = fs::File::open("/dev/urandom").map_err(|error| error.to_string())?;
    urandom
        .read_exact(&mut bytes)
        .map_err(|error| error.to_string())?;
    Ok(BASE64_TOKEN.encode(bytes))
}

pub(super) fn run_output_path(state_dir: &Path, run_id: &str) -> PathBuf {
    state_dir.join("runs").join(run_id).join("output.ndjson")
}

pub(super) fn worker_socket_path(runtime_dir: &Path, run_id: &str) -> PathBuf {
    runtime_dir.join("workers").join(format!("{run_id}.sock"))
}

/// Reads a stable start-time token for a PID, the second half of the durable
/// process identity. Linux exposes clock ticks directly; other Unix systems
/// use a stable hash of `ps`'s process start timestamp.
pub(super) fn process_start_time(pid: u32) -> Option<i64> {
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
