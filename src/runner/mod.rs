//! Execution boundary for one flow stage at a time.
//!
//! The daemon decides what runs and in what order; the runner only executes
//! one stage order and returns factual evidence. Runners never merge, access
//! durable storage directly, or derive verdicts and run outcomes.

use std::ffi::OsString;
use std::path::PathBuf;

pub mod local;

#[derive(Debug, Clone)]
pub struct StageOrder {
    pub run_id: String,
    pub stage: String,
    pub execution: StageExecution,
    pub worktree: PathBuf,
    pub branch: String,
    pub output_path: PathBuf,
}

#[derive(Debug, Clone)]
pub enum StageExecution {
    Agent(AgentLaunch),
    Exec(ExecLaunch),
}

#[derive(Debug, Clone)]
pub struct AgentLaunch {
    pub argv: Vec<String>,
    pub environment: Vec<(OsString, OsString)>,
    pub repository: PathBuf,
    pub worker_socket_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct ExecLaunch {
    pub argv: Vec<String>,
    pub worker: Option<WorkerCredentials>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerCredentials {
    pub socket: PathBuf,
    pub token: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub start_time: Option<i64>,
    pub process_group_id: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionEvidence {
    pub exit_code: Option<i32>,
    pub started_at_ms: i64,
    pub finished_at_ms: i64,
    pub process: Option<ProcessIdentity>,
    pub output_capture_complete: bool,
    pub stragglers_killed: bool,
}

#[derive(Debug, Clone)]
pub struct AgentProcessCheckpoint {
    pub run_id: String,
    pub branch: String,
    pub worktree: PathBuf,
    pub process: ProcessIdentity,
    pub worker: WorkerCredentials,
    pub started_at_ms: i64,
}

#[derive(Debug, Clone)]
pub struct ExecProcessCheckpoint {
    pub run_id: String,
    pub stage: String,
    pub process: ProcessIdentity,
    pub started_at_ms: i64,
}

/// The only daemon-owned services available while a stage is executing.
pub trait StageHooks {
    type Error;

    fn cancellation_requested(&self, run_id: &str) -> bool;

    fn record_agent_process(&self, checkpoint: &AgentProcessCheckpoint) -> Result<(), Self::Error>;

    fn record_exec_process(&self, checkpoint: &ExecProcessCheckpoint) -> Result<(), Self::Error>;
}

#[derive(Debug)]
pub enum RunnerError<E> {
    Execution(String),
    Hook(E),
}

impl<E: std::fmt::Display> std::fmt::Display for RunnerError<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Execution(message) => formatter.write_str(message),
            Self::Hook(error) => error.fmt(formatter),
        }
    }
}

#[derive(Debug)]
pub struct ExecutionFailure<E> {
    pub evidence: ExecutionEvidence,
    pub error: RunnerError<E>,
}
