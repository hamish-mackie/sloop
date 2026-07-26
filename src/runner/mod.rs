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
    /// Which execution of `stage` this order is. A backward edge re-runs a
    /// stage, so everything the execution produces — captured output above
    /// all — is tagged with the attempt as well as the name.
    pub attempt: u32,
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
    /// The credentials this stage's agent presents on the worker socket. The
    /// caller mints them so the daemon can serve them before the process
    /// exists; the runner only hands them to the child.
    pub worker: WorkerCredentials,
}

#[derive(Debug, Clone)]
pub struct ExecLaunch {
    pub argv: Vec<String>,
    pub worker: Option<WorkerCredentials>,
    /// Extra environment applied to the child. Empty for ordinary exec
    /// stages; a spawned agent carries the agent environment here.
    pub environment: Vec<(OsString, OsString)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerCredentials {
    pub socket: PathBuf,
    pub token: String,
    /// What the token authorises. Minted with the credential and never read
    /// off a request, so a worker cannot widen its own authority by argument.
    pub scope: WorkerScope,
}

/// The authority a worker token carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerScope {
    /// The stage's own worker, named by the credential the same way a panel
    /// seat is. The stage cannot be read back off the checkpointed process
    /// instead: the driver clears the `stage_process` row when a stage ends and
    /// writes the next one only after that stage's child is already spawned, so
    /// between those two points there is no row to read. A worker that reported
    /// inside the window was refused outright, and the check it was answering
    /// never saw the report it was waiting for.
    Stage { stage: String, attempt: u32 },
    /// One seat on a panel. A panel runs several workers against one stage
    /// execution, so "the executing stage" no longer picks out a report row:
    /// the seat is named by the credential instead, and a reviewer holding it
    /// can report for nothing else — not another seat, not another attempt,
    /// not another run.
    PanelReviewer {
        stage: String,
        stage_index: usize,
        attempt: u32,
        reviewer_index: usize,
    },
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
    pub stage: String,
    pub attempt: u32,
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
    pub attempt: u32,
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
