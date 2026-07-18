mod runner;

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use fs2::FileExt;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader as AsyncBufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot};

use crate::clock::{Clock, FileClock, SystemClock, format_timestamp, next_local_minute_ms};
use crate::config::{AgentConfig, Config, ConfigError, Project, RunningHours, parse_local_time};
use crate::flow::{
    Flow, Stage, StageEvidence, StageKind, Step, Verdict, VerdictSource, next_step, resolve_verdict,
};
use crate::frontmatter::{Frontmatter, FrontmatterError};
use crate::ids::{IdError, next_id};
use crate::logging::{LogLevel, OperationalLog};
use crate::outcome::{ExitClass, MergeOutcome, RunEvidence, classify_exit, derive_outcome};
use crate::protocol::{
    Capability, ErrorBody, ErrorCode, Request, RequestEnvelope, RequestId, ResponseEnvelope,
};
use crate::run_log::{OutputSource, OutputStream, RunLogWriter};
use crate::store::{
    ActivationKind, ClaimRequest, CooldownUpdate, EvidenceRecord, ExitClaim, NewActivation,
    QueuedActivation, RecoverableRun, StageRecord, Store, StoreError, TicketState,
};

use crate::vendor_error::{CatalogError, VendorErrorClassifier, VendorErrorMatch};
use runner::{
    LaunchedRun, launch_agent, process_start_time, run_output_path, spawn_output_reader,
    worker_socket_path,
};

const MAX_ENVELOPE_BYTES: u64 = 1024 * 1024;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(5);
const DISPATCH_CHANNEL_CAPACITY: usize = 64;
const DEFAULT_LEASE_MS: i64 = 10 * 60 * 1000;
const VENDOR_COOLDOWN_MS: i64 = 5 * 60 * 1000;
pub(crate) const WORKER_BOOTSTRAP_PROMPT: &str =
    include_str!("../worker-instructions.md").trim_ascii();
/// One `logs` page; chunks are ≤8KiB, so a page stays well inside the
/// envelope limit once cursor arguments arrive.
const LOGS_PAGE_LIMIT: usize = 64;

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
static MERGE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub struct ClientResponse {
    pub response: ResponseEnvelope,
    pub started: bool,
}

pub fn request(request: Request) -> Result<ClientResponse, DaemonError> {
    let cwd = std::env::current_dir().map_err(DaemonError::CurrentDirectory)?;
    let project = Project::discover(&cwd)?;
    Config::load(&project)?;

    if let Ok(response) = send_existing(&project, request.clone()) {
        return Ok(ClientResponse {
            response,
            started: false,
        });
    }

    spawn_daemon(&project)?;
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        match send_existing(&project, request.clone()) {
            Ok(response) => {
                return Ok(ClientResponse {
                    response,
                    started: true,
                });
            }
            Err(error) if Instant::now() >= deadline => return Err(error),
            Err(_) => std::thread::sleep(Duration::from_millis(20)),
        }
    }
}

/// Sends a request only if a daemon is already listening; `Ok(None)` means
/// no daemon. Never spawns one.
pub fn request_running(request: Request) -> Result<Option<ResponseEnvelope>, DaemonError> {
    let cwd = std::env::current_dir().map_err(DaemonError::CurrentDirectory)?;
    let project = Project::discover(&cwd)?;
    Config::load(&project)?;
    match send_existing(&project, request) {
        Ok(response) => Ok(Some(response)),
        Err(DaemonError::Connect(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

pub fn serve_current_project() -> Result<(), DaemonError> {
    let cwd = std::env::current_dir().map_err(DaemonError::CurrentDirectory)?;
    let project = Project::discover(&cwd)?;
    let config = Config::load(&project)?;
    let classifier = Arc::new(VendorErrorClassifier::built_in().map_err(DaemonError::Catalog)?);
    fs::create_dir_all(&project.state_dir).map_err(|source| DaemonError::Io {
        path: project.state_dir.clone(),
        source,
    })?;
    fs::set_permissions(&project.state_dir, fs::Permissions::from_mode(0o700)).map_err(
        |source| DaemonError::Io {
            path: project.state_dir.clone(),
            source,
        },
    )?;
    let runtime_root = project
        .runtime_dir
        .parent()
        .expect("repository runtime directories have a parent");
    fs::create_dir_all(runtime_root).map_err(|source| DaemonError::Io {
        path: runtime_root.to_path_buf(),
        source,
    })?;
    fs::set_permissions(runtime_root, fs::Permissions::from_mode(0o700)).map_err(|source| {
        DaemonError::Io {
            path: runtime_root.to_path_buf(),
            source,
        }
    })?;
    fs::create_dir(&project.runtime_dir)
        .or_else(|source| {
            if source.kind() == io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(source)
            }
        })
        .map_err(|source| DaemonError::Io {
            path: project.runtime_dir.clone(),
            source,
        })?;
    fs::set_permissions(&project.runtime_dir, fs::Permissions::from_mode(0o700)).map_err(
        |source| DaemonError::Io {
            path: project.runtime_dir.clone(),
            source,
        },
    )?;

    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&project.lock_path)
        .map_err(|source| DaemonError::Io {
            path: project.lock_path.clone(),
            source,
        })?;
    lock.try_lock_exclusive().map_err(|source| {
        if source.kind() == io::ErrorKind::WouldBlock {
            DaemonError::AlreadyRunning
        } else {
            DaemonError::Io {
                path: project.lock_path.clone(),
                source,
            }
        }
    })?;
    // Hold the pre-v7 runtime lock as well during the lock-location
    // transition, preventing an already-running older daemon in this runtime
    // root from sharing the database with the new process.
    let legacy_lock_path = project.runtime_dir.join("daemon.lock");
    let legacy_lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&legacy_lock_path)
        .map_err(|source| DaemonError::Io {
            path: legacy_lock_path.clone(),
            source,
        })?;
    legacy_lock.try_lock_exclusive().map_err(|source| {
        if source.kind() == io::ErrorKind::WouldBlock {
            DaemonError::AlreadyRunning
        } else {
            DaemonError::Io {
                path: legacy_lock_path,
                source,
            }
        }
    })?;
    // Identity is advisory; the flock is the guard, so write errors are
    // ignored rather than fatal.
    let identity = json!({
        "pid": std::process::id(),
        "started_at_ms": process_start_time(std::process::id()),
        "socket": project.operator_socket,
    });
    let _ = lock.set_len(0);
    let _ = {
        use std::io::Write as _;
        writeln!(&lock, "{identity}")
    };

    let clock: Arc<dyn Clock> = match std::env::var_os("SLOOP_TEST_CLOCK_PATH") {
        Some(path) => Arc::new(FileClock::new(path.into())),
        None => Arc::new(SystemClock),
    };
    let store = Store::open(&project.db_path, clock.now_ms()).map_err(DaemonError::Store)?;
    if let Some(agent) = &config.agent {
        store
            .backfill_ticket_targets(&agent.default_target, clock.now_ms())
            .map_err(DaemonError::Store)?;
    }
    let _ = index_projects(
        &project.root,
        &config.project_dir,
        &store,
        clock.now_ms(),
        &config.project_prefix,
    )?;
    reconcile_tickets(
        &project.root,
        &store,
        clock.now_ms(),
        config.delete_missing_after_ms,
    )?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(DaemonError::Runtime)?;
    runtime.block_on(serve(
        project,
        config,
        store,
        lock,
        legacy_lock,
        clock,
        classifier,
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OrphanDisposition {
    MarkMissing,
    Delete,
    Keep,
}

/// Policy for a DB ticket whose file has disappeared: stamp first, delete
/// once the stamp outlives the grace window, and never delete a row
/// something else still references — such rows just stay stamped.
fn orphan_disposition(
    missing_since_ms: Option<i64>,
    is_referenced: bool,
    now_ms: i64,
    delete_missing_after_ms: i64,
) -> OrphanDisposition {
    match missing_since_ms {
        None => OrphanDisposition::MarkMissing,
        Some(since) if now_ms - since >= delete_missing_after_ms && !is_referenced => {
            OrphanDisposition::Delete
        }
        Some(_) => OrphanDisposition::Keep,
    }
}

/// Reconciles local ticket rows against their committed files. Runs at
/// startup; `reindex` will share it.
fn reconcile_tickets(
    root: &Path,
    store: &Store,
    now_ms: i64,
    delete_missing_after_ms: i64,
) -> Result<(), DaemonError> {
    for ticket in store.local_ticket_files().map_err(DaemonError::Store)? {
        if root.join(&ticket.file_path).is_file() {
            if ticket.missing_at_ms.is_some() {
                store
                    .clear_ticket_missing(&ticket.id, now_ms)
                    .map_err(DaemonError::Store)?;
            }
            continue;
        }
        let is_referenced = store
            .ticket_is_referenced(&ticket.id)
            .map_err(DaemonError::Store)?;
        match orphan_disposition(
            ticket.missing_at_ms,
            is_referenced,
            now_ms,
            delete_missing_after_ms,
        ) {
            OrphanDisposition::MarkMissing => {
                store
                    .mark_ticket_missing(&ticket.id, now_ms)
                    .map_err(DaemonError::Store)?;
            }
            OrphanDisposition::Delete => {
                store
                    .delete_ticket(&ticket.id)
                    .map_err(DaemonError::Store)?;
            }
            OrphanDisposition::Keep => {}
        }
    }
    Ok(())
}

/// Indexes committed project Markdown files into the store so ticket membership
/// can be validated. Runs at startup; `reindex` will share it.
fn index_projects(
    root: &Path,
    project_dir: &Path,
    store: &Store,
    now_ms: i64,
    project_prefix: &str,
) -> Result<Vec<String>, DaemonError> {
    let directory = root.join(project_dir);
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(DaemonError::Io {
                path: directory,
                source,
            });
        }
    };

    let mut paths = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|source| DaemonError::Io {
                path: directory.clone(),
                source,
            })?
            .path();
        if path.extension().and_then(|extension| extension.to_str()) == Some("md") {
            paths.push(path);
        }
    }
    paths.sort();

    struct ProjectFile {
        path: PathBuf,
        content: String,
        stem: String,
        frontmatter: Frontmatter,
    }

    let mut projects = Vec::new();
    for path in paths {
        let content = fs::read_to_string(&path).map_err(|source| DaemonError::Io {
            path: path.clone(),
            source,
        })?;
        let stem = path
            .file_stem()
            .map(|stem| stem.to_string_lossy().into_owned())
            .unwrap_or_default();
        // A malformed project file must not keep the daemon from starting;
        // it is simply not indexed until fixed.
        let Ok(frontmatter) = crate::frontmatter::parse(&content) else {
            continue;
        };
        projects.push(ProjectFile {
            path,
            content,
            stem,
            frontmatter,
        });
    }

    // Explicit IDs in every file establish the high-water mark before sorted
    // idless files are assigned, regardless of where those explicit files sort.
    let mut ids: Vec<String> = projects
        .iter()
        .filter_map(|project| project.frontmatter.id.clone())
        .collect();
    let mut indexed = Vec::with_capacity(projects.len());
    for project in projects {
        let id = match project.frontmatter.id {
            Some(id) => id,
            None => {
                let id = next_id(project_prefix, ids.iter().map(String::as_str))?;
                let updated =
                    crate::frontmatter::stamp_id(&project.content, &id).map_err(|error| {
                        DaemonError::Frontmatter {
                            path: project.path.clone(),
                            error,
                        }
                    })?;
                fs::write(
                    &project.path,
                    updated.expect("idless project always needs an ID stamp"),
                )
                .map_err(|source| DaemonError::Io {
                    path: project.path.clone(),
                    source,
                })?;
                ids.push(id.clone());
                id
            }
        };
        let title = project.frontmatter.title.unwrap_or(project.stem);
        let relative = project
            .path
            .strip_prefix(root)
            .unwrap_or(&project.path)
            .to_string_lossy()
            .into_owned();
        store
            .upsert_local_project(&id, &relative, &title, now_ms)
            .map_err(DaemonError::Store)?;
        indexed.push(id);
    }
    Ok(indexed)
}

async fn serve(
    project: Project,
    config: Config,
    store: Store,
    _lock: fs::File,
    _legacy_lock: fs::File,
    clock: Arc<dyn Clock>,
    classifier: Arc<VendorErrorClassifier>,
) -> Result<(), DaemonError> {
    if project.operator_socket.exists() {
        fs::remove_file(&project.operator_socket).map_err(|source| DaemonError::Io {
            path: project.operator_socket.clone(),
            source,
        })?;
    }

    let listener =
        UnixListener::bind(&project.operator_socket).map_err(|source| DaemonError::Io {
            path: project.operator_socket.clone(),
            source,
        })?;
    fs::set_permissions(&project.operator_socket, fs::Permissions::from_mode(0o600)).map_err(
        |source| DaemonError::Io {
            path: project.operator_socket.clone(),
            source,
        },
    )?;

    let log = OperationalLog::open(&project.daemon_log).map_err(|source| DaemonError::Io {
        path: project.daemon_log.clone(),
        source,
    })?;
    log.emit(LogLevel::Info, "sloop::daemon", "daemon_started");

    let paused = store.paused().map_err(DaemonError::Store)?;
    let (dispatcher_tx, dispatcher_rx) = mpsc::channel(DISPATCH_CHANNEL_CAPACITY);
    let (events_tx, events_rx) = mpsc::channel(DISPATCH_CHANNEL_CAPACITY);
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let mut state = DispatcherState {
        pid: std::process::id(),
        paused,
        max_agents: config.max_parallel_tasks,
        ticket_prefix: config.ticket_prefix.clone(),
        project_prefix: config.project_prefix.clone(),
        running_hours: config.running_hours.clone(),
        agent: config.agent.clone(),
        flows: config.flows.clone(),
        default_flow: config.default_flow.clone(),
        aftercare_test_cmd: config.aftercare_test_cmd.clone(),
        root: project.root.clone(),
        project_dir: config.project_dir.clone(),
        ticket_dir: config.ticket_dir.clone(),
        worktree_dir: project.root.join(&config.worktree_dir),
        state_dir: project.state_dir.clone(),
        runtime_dir: project.runtime_dir.clone(),
        socket: project.operator_socket.clone(),
        daemon_log: project.daemon_log.clone(),
        store,
        storage_full: Cell::new(false),
        active: HashSet::new(),
        cancelling: HashSet::new(),
        worker_tokens: HashMap::new(),
        worker_listeners: HashMap::new(),
        worker_socket_paths: HashMap::new(),
        pending_exits: HashMap::new(),
        requests_tx: dispatcher_tx.clone(),
        log: log.clone(),
        clock,
        classifier,
        shutdown: shutdown_tx.clone(),
        shutdown_flag: shutdown_flag.clone(),
    };
    recover_inflight_runs(&mut state, &events_tx, &log)?;
    tokio::spawn(run_dispatcher(
        state,
        dispatcher_rx,
        events_rx,
        events_tx,
        log.clone(),
    ));

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(|source| DaemonError::Io {
                    path: project.operator_socket.clone(),
                    source,
                })?;
                let dispatcher_tx = dispatcher_tx.clone();
                let log = log.clone();
                let shutdown = shutdown_tx.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(stream, dispatcher_tx, shutdown).await {
                        log.emit_with_fields(
                            LogLevel::Error,
                            "sloop::socket",
                            "connection_failed",
                            json!({"error": error.to_string()}),
                        );
                    }
                });
            }
            _ = shutdown_rx.recv() => {
                shutdown_flag.store(true, Ordering::Release);
                log.emit(LogLevel::Info, "sloop::daemon", "daemon_stopped");
                let _ = fs::remove_file(&project.operator_socket);
                return Ok(());
            }
        }
    }
}

async fn handle_connection(
    stream: UnixStream,
    dispatcher: mpsc::Sender<DispatcherMessage>,
    shutdown: mpsc::Sender<()>,
) -> io::Result<()> {
    let reader = AsyncBufReader::new(stream);
    let mut limited = reader.take(MAX_ENVELOPE_BYTES + 1);
    let mut bytes = Vec::new();
    let read = limited.read_until(b'\n', &mut bytes).await?;
    if read == 0 {
        return Ok(());
    }

    let mut stream = limited.into_inner().into_inner();
    let envelope = if bytes.len() as u64 > MAX_ENVELOPE_BYTES {
        Err(protocol_error("request envelope is too large"))
    } else {
        std::str::from_utf8(&bytes)
            .map_err(|_| protocol_error("request envelope must be UTF-8"))
            .and_then(|line| RequestEnvelope::decode(line.trim_end()).map_err(|error| error.body))
    };

    let is_stop = matches!(
        &envelope,
        Ok(envelope) if matches!(envelope.request, Request::Stop(_))
    );
    let response = match envelope {
        Ok(envelope) if envelope.token.is_some() => ResponseEnvelope::failure(
            Some(envelope.id),
            unauthorized("operator socket does not accept worker tokens"),
        ),
        Ok(envelope)
            if !matches!(
                envelope.request.capability(),
                Capability::Operator | Capability::Both
            ) =>
        {
            ResponseEnvelope::failure(
                Some(envelope.id),
                unauthorized("worker verbs are not available on the operator socket"),
            )
        }
        Ok(envelope) => dispatch_envelope(envelope, RequestOrigin::Operator, &dispatcher).await,
        Err(error) => ResponseEnvelope::failure(None, error),
    };

    // The reply must be flushed before the daemon exits, so the connection
    // handler owns the shutdown signal for an accepted stop.
    let stopping = is_stop && response.ok;
    let encoded = serde_json::to_vec(&response).map_err(io::Error::other)?;
    stream.write_all(&encoded).await?;
    stream.write_all(b"\n").await?;
    stream.shutdown().await?;
    if stopping {
        let _ = shutdown.send(()).await;
    }
    Ok(())
}

/// Reads one request line from a per-run worker socket, enforces the verb
/// split at the boundary, and funnels the request through the dispatcher
/// with the presented token for validation against the run's issued one.
async fn handle_worker_connection(
    stream: UnixStream,
    run_id: String,
    dispatcher: mpsc::Sender<DispatcherMessage>,
) -> io::Result<()> {
    let reader = AsyncBufReader::new(stream);
    let mut limited = reader.take(MAX_ENVELOPE_BYTES + 1);
    let mut bytes = Vec::new();
    let read = limited.read_until(b'\n', &mut bytes).await?;
    if read == 0 {
        return Ok(());
    }

    let mut stream = limited.into_inner().into_inner();
    let envelope = if bytes.len() as u64 > MAX_ENVELOPE_BYTES {
        Err(protocol_error("request envelope is too large"))
    } else {
        std::str::from_utf8(&bytes)
            .map_err(|_| protocol_error("request envelope must be UTF-8"))
            .and_then(|line| RequestEnvelope::decode(line.trim_end()).map_err(|error| error.body))
    };

    let response = match envelope {
        Ok(envelope)
            if !matches!(
                envelope.request.capability(),
                Capability::Worker | Capability::Both
            ) =>
        {
            ResponseEnvelope::failure(
                Some(envelope.id),
                unauthorized("operator verbs are not available on a worker socket"),
            )
        }
        Ok(envelope) => {
            let token = envelope.token.clone();
            dispatch_envelope(
                envelope,
                RequestOrigin::Worker { run_id, token },
                &dispatcher,
            )
            .await
        }
        Err(error) => ResponseEnvelope::failure(None, error),
    };

    let encoded = serde_json::to_vec(&response).map_err(io::Error::other)?;
    stream.write_all(&encoded).await?;
    stream.write_all(b"\n").await?;
    stream.shutdown().await
}

async fn dispatch_envelope(
    envelope: RequestEnvelope,
    origin: RequestOrigin,
    dispatcher: &mpsc::Sender<DispatcherMessage>,
) -> ResponseEnvelope {
    let (reply_tx, reply_rx) = oneshot::channel();
    let id = envelope.id;
    if dispatcher
        .send(DispatcherMessage {
            id: id.clone(),
            request: envelope.request,
            origin,
            reply: reply_tx,
        })
        .await
        .is_err()
    {
        ResponseEnvelope::failure(Some(id), internal("dispatcher is unavailable"))
    } else {
        reply_rx.await.unwrap_or_else(|_| {
            ResponseEnvelope::failure(Some(id), internal("dispatcher dropped response"))
        })
    }
}

struct DispatcherMessage {
    id: RequestId,
    request: Request,
    origin: RequestOrigin,
    reply: oneshot::Sender<ResponseEnvelope>,
}

/// Which socket a request arrived on. Worker requests carry the run whose
/// socket accepted the connection plus the token the caller presented; the
/// dispatcher owns the comparison against the run's issued token.
enum RequestOrigin {
    Operator,
    Worker {
        run_id: String,
        token: Option<String>,
    },
}

struct DispatcherState {
    pid: u32,
    paused: bool,
    max_agents: usize,
    ticket_prefix: String,
    project_prefix: String,
    running_hours: Option<RunningHours>,
    agent: Option<AgentConfig>,
    flows: BTreeMap<String, Flow>,
    default_flow: String,
    aftercare_test_cmd: Option<Vec<String>>,
    root: PathBuf,
    project_dir: PathBuf,
    ticket_dir: PathBuf,
    worktree_dir: PathBuf,
    state_dir: PathBuf,
    runtime_dir: PathBuf,
    socket: PathBuf,
    daemon_log: PathBuf,
    store: Store,
    /// `SQLITE_FULL` is a dispatcher gate. The daemon retains active and
    /// pending run evidence in memory until a committed probe succeeds.
    storage_full: Cell<bool>,
    /// Run IDs with a live supervised process; its size is the capacity gate.
    active: HashSet<String>,
    /// Run IDs whose cancellation was requested but whose exit has not been
    /// resolved yet; mirrors the durable `cancel_requested` evidence.
    cancelling: HashSet<String>,
    /// Tokens issued to live runs; a worker request must present its run's
    /// token exactly. Entries die with the run.
    worker_tokens: HashMap<String, String>,
    /// Accept-loop tasks for live per-run worker sockets, aborted at settle.
    worker_listeners: HashMap<String, tokio::task::JoinHandle<()>>,
    worker_socket_paths: HashMap<String, PathBuf>,
    /// Exit evidence remains here until its atomic store transaction commits.
    /// The dispatcher retries these records on every reconciliation pass.
    pending_exits: HashMap<String, RunEvent>,
    /// The dispatcher's own request channel, cloned into each worker
    /// accept loop so every request funnels through the single owner.
    requests_tx: mpsc::Sender<DispatcherMessage>,
    log: OperationalLog,
    clock: Arc<dyn Clock>,
    classifier: Arc<VendorErrorClassifier>,
    /// Signals the accept loop to end the process; used by daemon-side
    /// exits such as the project-root liveness check.
    shutdown: mpsc::Sender<()>,
    shutdown_flag: Arc<AtomicBool>,
}

/// One executed flow stage as observed by the supervisor.
struct StageResult {
    verdict: Verdict,
    exit_code: Option<i32>,
    started_at_ms: i64,
    finished_at_ms: i64,
}

/// Internal dispatcher events reported by effect tasks, never by clients.
enum RunEvent {
    Exited {
        run_id: String,
        target: String,
        exit_code: Option<i32>,
        /// False when a pipe reader failed to durably record every chunk;
        /// the loss becomes explicit run evidence instead of silence.
        capture_complete: bool,
        /// Commits made after the run branch was created. This is activity
        /// metadata only; it does not determine the run's outcome.
        commits: Vec<String>,
        commit_observation_complete: bool,
        aftercare_failed: bool,
        merge: Option<MergeOutcome>,
        vendor_error: Option<VendorErrorMatch>,
        cooldown_until_ms: Option<i64>,
        recovery: Option<RecoveryClassification>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryClassification {
    Aftercare,
    Orphaned,
}

async fn run_dispatcher(
    mut state: DispatcherState,
    mut requests: mpsc::Receiver<DispatcherMessage>,
    mut events: mpsc::Receiver<RunEvent>,
    events_tx: mpsc::Sender<RunEvent>,
    log: OperationalLog,
) {
    reconcile(&mut state, &events_tx, &log);
    loop {
        let deadline = next_dispatch_deadline(&state);
        let clock = state.clock.clone();
        tokio::select! {
            message = requests.recv() => {
                let Some(message) = message else { break };
                let response = match message.origin {
                    RequestOrigin::Operator => dispatch(&mut state, message.id, message.request),
                    RequestOrigin::Worker { run_id, token } => dispatch_worker(
                        &mut state,
                        message.id,
                        message.request,
                        &run_id,
                        token.as_deref(),
                    ),
                };
                let _ = message.reply.send(response);
                log.emit(LogLevel::Info, "sloop::dispatcher", "request_handled");
            }
            event = events.recv() => {
                let Some(event) = event else { break };
                settle_run_exit(&mut state, event, &log);
            }
            () = wait_for_deadline(clock, deadline) => {
                log.emit(LogLevel::Info, "sloop::dispatcher", "timer_fired");
            }
            // Wall-clock is deliberate: this is a liveness probe, not
            // decision logic, so the manual test clock must not gate it.
            () = tokio::time::sleep(std::time::Duration::from_secs(2)) => {
                if !state.root.join(".git").exists() {
                    log.emit(LogLevel::Error, "sloop::dispatcher", "project_root_missing");
                    let _ = state.shutdown.send(()).await;
                    break;
                }
            }
        }
        reconcile(&mut state, &events_tx, &log);
    }
}

async fn wait_for_deadline(clock: Arc<dyn Clock>, deadline: Option<i64>) {
    match deadline {
        Some(deadline) => clock.sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Resolves one finished run: derives the outcome from the gathered evidence
/// and commits the whole settlement in one store transaction. Cancellation
/// intent recorded before the exit wins over every other reading, keeping a
/// racing `cancel` and natural exit idempotent.
fn settle_run_exit(state: &mut DispatcherState, event: RunEvent, log: &OperationalLog) {
    let run_id = match &event {
        RunEvent::Exited { run_id, .. } => run_id.clone(),
    };
    state.pending_exits.insert(run_id, event);
    if !state.storage_full.get() {
        settle_pending_exits(state, log);
    }
}

fn settle_pending_exits(state: &mut DispatcherState, log: &OperationalLog) {
    let run_ids: Vec<String> = state.pending_exits.keys().cloned().collect();
    for run_id in run_ids {
        let Some(event) = state.pending_exits.remove(&run_id) else {
            continue;
        };
        match try_settle_run_exit(state, &event) {
            Ok(outcome) => {
                state.cancelling.remove(&run_id);
                state.active.remove(&run_id);
                close_worker_socket(state, &run_id);
                log.emit_with_fields(
                    LogLevel::Info,
                    "sloop::dispatcher",
                    "run_exited",
                    json!({"run_id": run_id, "outcome": outcome.as_str()}),
                );
            }
            Err(error) => {
                let disk_full = error.is_disk_full();
                mark_storage_full(state, &error);
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::dispatcher",
                    "run_exit_persist_failed",
                    json!({"run_id": run_id, "error": error.to_string()}),
                );
                state.pending_exits.insert(run_id, event);
                if disk_full {
                    break;
                }
            }
        }
    }
}

fn try_settle_run_exit(
    state: &mut DispatcherState,
    event: &RunEvent,
) -> Result<crate::outcome::Outcome, StoreError> {
    let RunEvent::Exited {
        run_id,
        target,
        exit_code,
        capture_complete,
        commits,
        commit_observation_complete,
        aftercare_failed,
        merge,
        vendor_error,
        cooldown_until_ms,
        recovery,
    } = event;

    let cancelled =
        state.cancelling.contains(run_id) || state.store.cancellation_requested(run_id)?;
    let evidence = RunEvidence {
        cancelled,
        exit: classify_exit(*exit_code),
        vendor_error: vendor_error.as_ref().map(|error| error.class),
        commit_count: commit_observation_complete.then_some(commits.len()),
        aftercare_failed: *aftercare_failed,
        merge: *merge,
    };
    let outcome = if *recovery == Some(RecoveryClassification::Orphaned)
        && !cancelled
        && vendor_error.is_none()
    {
        crate::outcome::Outcome::Orphaned
    } else {
        derive_outcome(&evidence)
    };

    let mut records = vec![
        EvidenceRecord {
            kind: "exit_classified",
            data_json: json!({"exit_code": exit_code}).to_string(),
        },
        EvidenceRecord {
            kind: "commits_observed",
            data_json: json!({"complete": commit_observation_complete, "oids": commits})
                .to_string(),
        },
    ];
    if let Some(classification) = recovery {
        records.push(EvidenceRecord {
            kind: "recovery_classified",
            data_json: json!({
                "classification": match classification {
                    RecoveryClassification::Aftercare => "aftercare",
                    RecoveryClassification::Orphaned => "orphaned",
                }
            })
            .to_string(),
        });
    }
    if let Some(merge) = *merge {
        records.push(EvidenceRecord {
            kind: "merge_result",
            data_json: json!({"merged": merge == MergeOutcome::Merged}).to_string(),
        });
    }
    if !capture_complete {
        records.push(EvidenceRecord {
            kind: "capture_incomplete",
            data_json: json!({}).to_string(),
        });
    }
    if let Some(vendor_error) = vendor_error {
        records.push(EvidenceRecord {
            kind: "vendor_error_classified",
            data_json: vendor_error.evidence_json(*cooldown_until_ms),
        });
    }
    let ticket_id = state
        .store
        .run(run_id)?
        .ok_or_else(|| StoreError::RunNotFound {
            run_id: run_id.clone(),
        })?
        .ticket_id;
    let cooldown = vendor_error
        .as_ref()
        .filter(|error| error.class.requires_cooldown() && !cancelled)
        .and_then(|error| cooldown_until_ms.map(|until_ms| (error, until_ms)))
        .map(|(error, until_ms)| CooldownUpdate {
            target,
            until_ms,
            reason: &error.diagnostic,
        });
    state.store.finish_run(
        run_id,
        &ticket_id,
        *exit_code,
        outcome,
        &records,
        cooldown.as_ref(),
        state.clock.now_ms(),
    )?;
    Ok(outcome)
}

/// Tears down a run's worker boundary: the token stops validating, the
/// accept loop ends, and the socket file disappears. Idempotent, so crash
/// recovery and racing settlements can call it freely.
fn close_worker_socket(state: &mut DispatcherState, run_id: &str) {
    state.worker_tokens.remove(run_id);
    if let Some(listener) = state.worker_listeners.remove(run_id) {
        listener.abort();
    }
    let socket_path = state
        .worker_socket_paths
        .remove(run_id)
        .unwrap_or_else(|| worker_socket_path(&state.runtime_dir, run_id));
    let _ = fs::remove_file(socket_path);
}

/// Classifies every durable lease before normal dispatch. Matching processes
/// consume capacity and are monitored by identity; dead or reused PIDs are
/// settled from the work preserved in their branches.
fn recover_inflight_runs(
    state: &mut DispatcherState,
    events: &mpsc::Sender<RunEvent>,
    log: &OperationalLog,
) -> Result<(), DaemonError> {
    let runs = state.store.recoverable_runs().map_err(DaemonError::Store)?;
    for run in runs {
        // Every durable lease consumes capacity until adoption or settlement
        // succeeds; a transient database error must never permit double-spawn.
        state.active.insert(run.id.clone());
        if recoverable_process_matches(&run) {
            let cancellation_requested = state
                .store
                .cancellation_requested(&run.id)
                .map_err(DaemonError::Store)?;
            if cancellation_requested {
                state.cancelling.insert(run.id.clone());
                if recoverable_process_matches(&run)
                    && let Some(group) = run.process_group_id
                {
                    unsafe {
                        libc::kill(-(group as libc::pid_t), libc::SIGKILL);
                    }
                }
            }
            if let Err(error) = restore_worker_socket(state, &run) {
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::recovery",
                    "worker_socket_restore_failed",
                    json!({"run_id": run.id, "error": error}),
                );
            }
            monitor_recovered_run(state, events.clone(), run.clone());
            log.emit_with_fields(
                LogLevel::Info,
                "sloop::recovery",
                "run_readopted",
                json!({"run_id": run.id, "ticket_id": run.ticket_id}),
            );
        } else {
            if run.state == "aftercare" {
                spawn_aftercare_recovery(state, events.clone(), run, log.clone())?;
            } else {
                spawn_dead_run_recovery(state, events.clone(), run, log.clone());
            }
        }
    }
    Ok(())
}

fn restore_worker_socket(state: &mut DispatcherState, run: &RecoverableRun) -> Result<(), String> {
    let token = run
        .worker_token
        .as_ref()
        .ok_or_else(|| "the persisted run has no worker token".to_owned())?;
    let socket_path = run
        .worker_socket_path
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(|| worker_socket_path(&state.runtime_dir, &run.id));
    fs::create_dir_all(socket_path.parent().expect("worker sockets have a parent"))
        .map_err(|error| error.to_string())?;
    let _ = fs::remove_file(&socket_path);
    let listener = UnixListener::bind(&socket_path).map_err(|error| error.to_string())?;
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))
        .map_err(|error| error.to_string())?;
    state.worker_tokens.insert(run.id.clone(), token.clone());
    state
        .worker_socket_paths
        .insert(run.id.clone(), socket_path.clone());
    let accept_loop = tokio::spawn(serve_worker_socket(
        listener,
        run.id.clone(),
        state.requests_tx.clone(),
        state.log.clone(),
    ));
    state.worker_listeners.insert(run.id.clone(), accept_loop);
    Ok(())
}

fn monitor_recovered_run(
    state: &DispatcherState,
    events: mpsc::Sender<RunEvent>,
    run: RecoverableRun,
) {
    let root = state.root.clone();
    let state_dir = state.state_dir.clone();
    let classifier = state.classifier.clone();
    let clock = state.clock.clone();
    let log = state.log.clone();
    let shutdown = state.shutdown_flag.clone();
    tokio::task::spawn_blocking(move || {
        while recoverable_process_matches(&run) {
            if shutdown.load(Ordering::Acquire) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        while !shutdown.load(Ordering::Acquire) {
            match recovered_exit_event(&root, &state_dir, &classifier, clock.now_ms(), &run) {
                Ok(event) => {
                    let _ = events.blocking_send(event);
                    break;
                }
                Err(error) => {
                    log.emit_with_fields(
                        LogLevel::Error,
                        "sloop::recovery",
                        "run_observation_failed",
                        json!({"run_id": run.id, "error": error}),
                    );
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        }
    });
}

fn spawn_dead_run_recovery(
    state: &DispatcherState,
    events: mpsc::Sender<RunEvent>,
    run: RecoverableRun,
    log: OperationalLog,
) {
    let root = state.root.clone();
    let state_dir = state.state_dir.clone();
    let classifier = state.classifier.clone();
    let clock = state.clock.clone();
    let shutdown = state.shutdown_flag.clone();
    tokio::task::spawn_blocking(move || {
        while !shutdown.load(Ordering::Acquire) {
            match recovered_exit_event(&root, &state_dir, &classifier, clock.now_ms(), &run) {
                Ok(event) => {
                    let _ = events.blocking_send(event);
                    break;
                }
                Err(error) => {
                    log.emit_with_fields(
                        LogLevel::Error,
                        "sloop::recovery",
                        "run_observation_failed",
                        json!({"run_id": run.id, "error": error}),
                    );
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        }
    });
}

fn recovered_exit_event(
    root: &Path,
    state_dir: &Path,
    classifier: &VendorErrorClassifier,
    now_ms: i64,
    run: &RecoverableRun,
) -> Result<RunEvent, String> {
    let commits = run
        .branch
        .as_deref()
        .map(|branch| try_commits_on_branch(root, branch))
        .transpose()?
        .unwrap_or_default();
    let exit_code = run.exit_code.and_then(|code| i32::try_from(code).ok());
    let vendor_error = classify_run_output(classifier, state_dir, &run.id, exit_code)?;
    let cooldown_until_ms = vendor_error
        .as_ref()
        .filter(|error| error.class.requires_cooldown())
        .map(|_| now_ms + VENDOR_COOLDOWN_MS);
    Ok(RunEvent::Exited {
        run_id: run.id.clone(),
        target: run.target.clone(),
        exit_code,
        capture_complete: false,
        commits,
        commit_observation_complete: true,
        aftercare_failed: false,
        merge: None,
        vendor_error,
        cooldown_until_ms,
        recovery: Some(RecoveryClassification::Orphaned),
    })
}

fn spawn_aftercare_recovery(
    state: &DispatcherState,
    events: mpsc::Sender<RunEvent>,
    run: RecoverableRun,
    log: OperationalLog,
) -> Result<(), DaemonError> {
    let root = state.root.clone();
    let state_dir = state.state_dir.clone();
    let test_cmd = state.aftercare_test_cmd.clone();
    let flow = bound_flow(&state.store, &state.flows, &run.ticket_id)
        .map_err(DaemonError::InvalidResponse)?;
    let clock = state.clock.clone();
    let db_path = state.state_dir.join("sloop.db");
    let shutdown = state.shutdown_flag.clone();
    tokio::task::spawn_blocking(move || {
        while !shutdown.load(Ordering::Acquire) {
            let result = Store::open(&db_path, clock.now_ms())
                .map_err(|error| error.to_string())
                .and_then(|store| {
                    resume_aftercare(
                        &root,
                        &state_dir,
                        &flow,
                        test_cmd.as_deref(),
                        clock.as_ref(),
                        &store,
                        &run,
                        &log,
                    )
                });
            match result {
                Ok(event) => {
                    let _ = events.blocking_send(event);
                    break;
                }
                Err(error) => {
                    log.emit_with_fields(
                        LogLevel::Error,
                        "sloop::recovery",
                        "aftercare_resume_failed",
                        json!({"run_id": run.id, "error": error}),
                    );
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        }
    });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn resume_aftercare(
    root: &Path,
    state_dir: &Path,
    flow: &Flow,
    test_cmd: Option<&[String]>,
    clock: &dyn Clock,
    store: &Store,
    run: &RecoverableRun,
    log: &OperationalLog,
) -> Result<RunEvent, String> {
    let rows = store
        .run_evidence(&run.id)
        .map_err(|error| error.to_string())?;
    let value = |kind: &str| {
        rows.iter()
            .find(|(candidate, _)| candidate == kind)
            .and_then(|(_, data)| serde_json::from_str::<serde_json::Value>(data).ok())
    };
    let commit_observation = value("commits_observed")
        .and_then(|data| {
            data["oids"].as_array().map(|oids| {
                let complete = data["complete"].as_bool().unwrap_or(true);
                let commits = oids
                    .iter()
                    .filter_map(|oid| oid.as_str().map(str::to_owned))
                    .collect::<Vec<_>>();
                (commits, complete)
            })
        })
        .ok_or_else(|| "the aftercare checkpoint has no valid commit evidence".to_owned())?;
    let (commits, commit_observation_complete) = commit_observation;
    let exit_code = run.exit_code.and_then(|code| i32::try_from(code).ok());
    let vendor_error = value("vendor_error_classified")
        .and_then(|data| serde_json::from_value::<VendorErrorMatch>(data).ok());
    let cooldown_until_ms =
        value("vendor_error_classified").and_then(|data| data["cooldown_until_ms"].as_i64());
    let output_log = RunLogWriter::open(&run_output_path(state_dir, &run.id))
        .map_err(|error| format!("cannot open run output: {error}"))?;
    if aftercare_cancelled(store, &run.id, log) {
        return Ok(RunEvent::Exited {
            run_id: run.id.clone(),
            target: run.target.clone(),
            exit_code,
            capture_complete: !rows.iter().any(|(kind, _)| kind == "capture_incomplete"),
            commits,
            commit_observation_complete,
            aftercare_failed: false,
            merge: None,
            vendor_error,
            cooldown_until_ms,
            recovery: Some(RecoveryClassification::Aftercare),
        });
    }
    let worktree = run
        .worktree_path
        .as_deref()
        .ok_or_else(|| "the aftercare checkpoint has no worktree".to_owned())?;
    let branch = run
        .branch
        .as_deref()
        .ok_or_else(|| "the aftercare checkpoint has no branch".to_owned())?;
    let result = drive_flow(
        root,
        Path::new(worktree),
        branch,
        flow,
        test_cmd,
        exit_code,
        vendor_error.is_some(),
        &commits,
        commit_observation_complete,
        &output_log,
        clock,
        store,
        &run.id,
        log,
    )?;

    Ok(RunEvent::Exited {
        run_id: run.id.clone(),
        target: run.target.clone(),
        exit_code,
        capture_complete: !rows.iter().any(|(kind, _)| kind == "capture_incomplete"),
        commits,
        commit_observation_complete,
        aftercare_failed: result.aftercare_failed,
        merge: result.merge,
        vendor_error,
        cooldown_until_ms,
        recovery: Some(RecoveryClassification::Aftercare),
    })
}

struct FlowRunResult {
    aftercare_failed: bool,
    merge: Option<MergeOutcome>,
}

#[allow(clippy::too_many_arguments)]
fn drive_flow(
    root: &Path,
    worktree: &Path,
    branch: &str,
    bound_flow: &Flow,
    test_cmd: Option<&[String]>,
    exit_code: Option<i32>,
    vendor_rejected: bool,
    commits: &[String],
    commit_observation_complete: bool,
    output_log: &RunLogWriter,
    clock: &dyn Clock,
    store: &Store,
    run_id: &str,
    log: &OperationalLog,
) -> Result<FlowRunResult, String> {
    let flow = flow_with_implicit_test(bound_flow, test_cmd)?;
    let rows = store
        .aftercare_stages(run_id)
        .map_err(|error| error.to_string())?;
    let mut evidence = rows
        .iter()
        .map(|row| StageEvidence {
            stage: row.stage.clone(),
            verdict: if row.state == "passed" {
                Verdict::Pass
            } else {
                Verdict::Fail
            },
            source: VerdictSource::ExitCode,
            reason: None,
        })
        .collect::<Vec<_>>();
    let mut merge = flow.stages.iter().find_map(|stage| {
        if stage.kind != StageKind::Merge {
            return None;
        }
        evidence
            .iter()
            .find(|row| row.stage == stage.name)
            .map(|row| {
                if row.verdict == Verdict::Pass {
                    MergeOutcome::Merged
                } else {
                    MergeOutcome::Diverged
                }
            })
    });
    let interrupted = store
        .run_evidence(run_id)
        .map_err(|error| error.to_string())?;

    loop {
        if aftercare_cancelled(store, run_id, log) {
            return Ok(FlowRunResult {
                aftercare_failed: false,
                merge,
            });
        }
        let stage = match next_step(&flow, &evidence) {
            Step::Run(stage) => stage,
            Step::Halted { failed_stage } => {
                let build = flow
                    .stages
                    .first()
                    .is_some_and(|stage| stage.name == failed_stage);
                return Ok(FlowRunResult {
                    aftercare_failed: !build,
                    merge,
                });
            }
            Step::Complete => {
                return Ok(FlowRunResult {
                    aftercare_failed: false,
                    merge,
                });
            }
        };
        let stage_index = flow
            .stages
            .iter()
            .position(|candidate| candidate.name == stage.name)
            .expect("next_step returned a stage from this flow");
        let interrupted_process = stop_interrupted_process(&interrupted, &stage.name)?;
        if let Some((identity, PersistedProcessStop::LeaderMissing)) = &interrupted_process {
            log.emit_with_fields(
                LogLevel::Info,
                "sloop::recovery",
                "stale_aftercare_group_not_signalled",
                json!({
                    "run_id": run_id,
                    "stage": stage.name,
                    "process_group_id": identity.group,
                }),
            );
        }
        let merge_recovery = if let Some((identity, _)) = &interrupted_process
            && stage.kind == StageKind::Merge
        {
            match inspect_interrupted_merge(root, branch, identity) {
                Ok(recovery) => Some(recovery),
                Err(error) => {
                    log.emit_with_fields(
                        LogLevel::Error,
                        "sloop::recovery",
                        "merge_recovery_inspection_failed",
                        json!({"run_id": run_id, "error": error}),
                    );
                    Some(MergeRecovery::UnsafePartial)
                }
            }
        } else {
            None
        };
        if interrupted_process.is_some() {
            store
                .clear_aftercare_process(run_id)
                .map_err(|error| error.to_string())?;
        }
        let result = match &stage.kind {
            StageKind::Build => {
                let now = clock.now_ms();
                StageResult {
                    verdict: if !vendor_rejected && classify_exit(exit_code) == ExitClass::Success {
                        Verdict::Pass
                    } else {
                        Verdict::Fail
                    },
                    exit_code,
                    started_at_ms: now,
                    finished_at_ms: now,
                }
            }
            StageKind::Exec { cmd } => run_exec_stage(
                worktree,
                &stage.name,
                cmd,
                output_log,
                clock,
                store,
                run_id,
                log,
            ),
            StageKind::Merge if merge_recovery == Some(MergeRecovery::AlreadyCompleted) => {
                let now = clock.now_ms();
                merge = Some(MergeOutcome::Merged);
                StageResult {
                    verdict: Verdict::Pass,
                    exit_code: Some(0),
                    started_at_ms: now,
                    finished_at_ms: now,
                }
            }
            StageKind::Merge if merge_recovery == Some(MergeRecovery::UnsafePartial) => {
                let now = clock.now_ms();
                merge = Some(MergeOutcome::Diverged);
                StageResult {
                    verdict: Verdict::Fail,
                    exit_code: Some(1),
                    started_at_ms: now,
                    finished_at_ms: now,
                }
            }
            StageKind::Merge => {
                let started_at_ms = clock.now_ms();
                let outcome = attempt_merge(
                    root,
                    branch,
                    commit_observation_complete && commits.is_empty(),
                    &stage.name,
                    store,
                    run_id,
                    clock,
                    log,
                );
                merge = Some(outcome);
                StageResult {
                    verdict: if outcome == MergeOutcome::Merged {
                        Verdict::Pass
                    } else {
                        Verdict::Fail
                    },
                    exit_code: Some(if outcome == MergeOutcome::Merged {
                        0
                    } else {
                        1
                    }),
                    started_at_ms,
                    finished_at_ms: clock.now_ms(),
                }
            }
        };
        let (verdict, source, reason) = resolve_verdict(&stage.kind, result.verdict, None);
        if let Err(error) = store.record_aftercare_stage(
            run_id,
            &StageRecord {
                stage_index,
                stage: stage.name.clone(),
                state: if verdict == Verdict::Pass {
                    "passed".into()
                } else {
                    "failed".into()
                },
                started_at_ms: result.started_at_ms,
                finished_at_ms: result.finished_at_ms,
                exit_code: result.exit_code,
                output_ref: format!("runs/{run_id}/output.ndjson"),
            },
        ) {
            log.emit_with_fields(
                LogLevel::Error,
                "sloop::supervisor",
                "aftercare_stage_persist_failed",
                json!({"run_id": run_id, "stage": stage.name, "error": error.to_string()}),
            );
            return Ok(FlowRunResult {
                aftercare_failed: true,
                merge,
            });
        }
        if let Err(error) = store.clear_aftercare_process(run_id) {
            log.emit_with_fields(
                LogLevel::Error,
                "sloop::supervisor",
                "aftercare_process_clear_failed",
                json!({"run_id": run_id, "stage": stage.name, "error": error.to_string()}),
            );
            return Ok(FlowRunResult {
                aftercare_failed: true,
                merge,
            });
        }
        evidence.push(StageEvidence {
            stage: stage.name.clone(),
            verdict,
            source,
            reason,
        });
        wait_for_test_hook(&format!("after-aftercare-stage-{}", stage.name));
    }
}

fn flow_with_implicit_test(flow: &Flow, test_cmd: Option<&[String]>) -> Result<Flow, String> {
    let mut flow = flow.clone();
    if let Some(cmd) = test_cmd {
        if flow.stages.iter().any(|stage| stage.name == "test") {
            return Err("aftercare.test_cmd conflicts with flow stage `test`".into());
        }
        flow.stages.insert(
            1,
            Stage {
                name: "test".into(),
                kind: StageKind::Exec { cmd: cmd.to_vec() },
            },
        );
    }
    Ok(flow)
}

#[derive(Debug, Clone)]
struct AftercareProcessIdentity {
    pid: u32,
    start_time: i64,
    group: i64,
    merge: Option<MergeProcessCheckpoint>,
}

#[derive(Debug, Clone)]
struct MergeProcessCheckpoint {
    target_head: String,
    branch_tip: String,
    completed_target: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistedProcessState {
    OriginalLeader,
    ReusedLeader,
    LeaderMissing,
    UnverifiableLeader,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistedProcessStop {
    StoppedOriginal,
    LeaderMissing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MergeRecovery {
    Retry,
    AlreadyCompleted,
    UnsafePartial,
}

fn stop_interrupted_process(
    rows: &[(String, String)],
    stage: &str,
) -> Result<Option<(AftercareProcessIdentity, PersistedProcessStop)>, String> {
    let Some(identity) = aftercare_process_identity(rows, Some(stage))? else {
        return Ok(None);
    };
    if identity.group <= 0 {
        return Err("the interrupted aftercare stage has an invalid process group".into());
    }
    let stopped = stop_persisted_process_group(&identity)?;
    Ok(Some((identity, stopped)))
}

#[cfg(target_os = "linux")]
fn process_group_alive(group: i64) -> bool {
    let Ok(processes) = fs::read_dir("/proc") else {
        return unsafe { libc::kill(-(group as libc::pid_t), 0) == 0 };
    };
    processes.filter_map(Result::ok).any(|process| {
        let Ok(pid) = process.file_name().to_string_lossy().parse::<u32>() else {
            return false;
        };
        let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) else {
            return false;
        };
        let Some(after_command) = stat.rfind(')').map(|index| &stat[index + 1..]) else {
            return false;
        };
        let mut fields = after_command.split_whitespace();
        let state = fields.next();
        let _parent = fields.next();
        let process_group = fields.next().and_then(|value| value.parse::<i64>().ok());
        state != Some("Z") && process_group == Some(group)
    })
}

#[cfg(not(target_os = "linux"))]
fn process_group_alive(group: i64) -> bool {
    unsafe { libc::kill(-(group as libc::pid_t), 0) == 0 }
}

fn inspect_interrupted_merge(
    root: &Path,
    branch: &str,
    identity: &AftercareProcessIdentity,
) -> Result<MergeRecovery, String> {
    let checkpoint = identity
        .merge
        .as_ref()
        .ok_or_else(|| "the interrupted merge has no baseline checkpoint".to_owned())?;
    if shared_checkout_has_git_operation(root)? || git_index_lock_path(root)?.exists() {
        return Ok(MergeRecovery::UnsafePartial);
    }
    if !git_stdout(root, &["ls-files", "--unmerged"])?.is_empty() {
        return Ok(MergeRecovery::UnsafePartial);
    }
    let branch_tip = git_stdout(root, &["rev-parse", branch])?;
    if branch_tip != checkpoint.branch_tip {
        return Ok(MergeRecovery::UnsafePartial);
    }
    let target_head = git_stdout(root, &["rev-parse", "HEAD"])?;
    if checkpoint.completed_target.is_some() {
        return if git_is_ancestor(root, &checkpoint.branch_tip, &target_head)? {
            Ok(MergeRecovery::AlreadyCompleted)
        } else {
            Ok(MergeRecovery::UnsafePartial)
        };
    }
    if target_head == checkpoint.target_head {
        return if git_index_matches_head(root)? {
            Ok(MergeRecovery::Retry)
        } else {
            Ok(MergeRecovery::UnsafePartial)
        };
    }
    if git_is_ancestor(root, &checkpoint.branch_tip, &target_head)? {
        return Ok(MergeRecovery::AlreadyCompleted);
    }
    Ok(MergeRecovery::UnsafePartial)
}

fn aftercare_process_identity(
    rows: &[(String, String)],
    stage: Option<&str>,
) -> Result<Option<AftercareProcessIdentity>, String> {
    let Some(data) = rows
        .iter()
        .find(|(candidate, _)| candidate == "aftercare_process")
        .and_then(|(_, data)| serde_json::from_str::<serde_json::Value>(data).ok())
    else {
        return Ok(None);
    };
    if stage.is_some_and(|stage| data["stage"].as_str() != Some(stage)) {
        return Ok(None);
    }
    let pid = data["pid"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .ok_or_else(|| "the interrupted aftercare stage has no valid pid".to_owned())?;
    let start_time = data["pid_start_time"]
        .as_i64()
        .ok_or_else(|| "the interrupted aftercare stage has no valid start time".to_owned())?;
    let group = data["process_group_id"]
        .as_i64()
        .ok_or_else(|| "the interrupted aftercare stage has no valid process group".to_owned())?;
    let merge = data
        .get("merge")
        .map(|merge| -> Result<_, String> {
            Ok(MergeProcessCheckpoint {
                target_head: merge["target_head"]
                    .as_str()
                    .ok_or_else(|| "the interrupted merge has no target HEAD".to_owned())?
                    .to_owned(),
                branch_tip: merge["branch_tip"]
                    .as_str()
                    .ok_or_else(|| "the interrupted merge has no branch tip".to_owned())?
                    .to_owned(),
                completed_target: merge["completed_target"].as_str().map(str::to_owned),
            })
        })
        .transpose()?;
    Ok(Some(AftercareProcessIdentity {
        pid,
        start_time,
        group,
        merge,
    }))
}

fn persisted_process_state(identity: &AftercareProcessIdentity) -> PersistedProcessState {
    let observed_start_time = process_start_time(identity.pid);
    classify_persisted_process(
        identity.start_time,
        observed_start_time,
        observed_start_time.is_some() || process_exists(identity.pid),
    )
}

fn classify_persisted_process(
    expected_start_time: i64,
    observed_start_time: Option<i64>,
    leader_exists: bool,
) -> PersistedProcessState {
    match observed_start_time {
        Some(actual) if actual == expected_start_time => PersistedProcessState::OriginalLeader,
        Some(_) => PersistedProcessState::ReusedLeader,
        None if leader_exists => PersistedProcessState::UnverifiableLeader,
        None => PersistedProcessState::LeaderMissing,
    }
}

fn stop_persisted_process_group(
    identity: &AftercareProcessIdentity,
) -> Result<PersistedProcessStop, String> {
    if identity.group <= 0
        || identity.group != i64::from(identity.pid)
        || libc::pid_t::try_from(identity.group).is_err()
    {
        return Err("the persisted aftercare process group is not its recorded leader".into());
    }
    match persisted_process_state(identity) {
        PersistedProcessState::ReusedLeader => {
            return Err("the aftercare process group ID was reused; refusing to signal it".into());
        }
        PersistedProcessState::UnverifiableLeader => {
            return Err("cannot verify the persisted aftercare process leader".into());
        }
        // The group may still exist, but without the recorded leader its
        // identity is unverifiable and signaling it is unsafe.
        PersistedProcessState::LeaderMissing => return Ok(PersistedProcessStop::LeaderMissing),
        PersistedProcessState::OriginalLeader => {}
    }
    unsafe {
        libc::kill(-(identity.group as libc::pid_t), libc::SIGKILL);
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while process_group_alive(identity.group) {
        if Instant::now() >= deadline {
            return Err("the interrupted aftercare process group did not exit".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Ok(PersistedProcessStop::StoppedOriginal)
}

fn process_exists(pid: u32) -> bool {
    let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
    result == 0 || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

fn git_is_ancestor(root: &Path, ancestor: &str, descendant: &str) -> Result<bool, String> {
    let status = Command::new("git")
        .args(["merge-base", "--is-ancestor", ancestor, descendant])
        .current_dir(root)
        .status()
        .map_err(|error| error.to_string())?;
    match status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => Err(format!(
            "git merge-base --is-ancestor {ancestor} {descendant} failed: {status}"
        )),
    }
}

fn git_index_matches_head(root: &Path) -> Result<bool, String> {
    let status = Command::new("git")
        .args(["diff", "--cached", "--quiet", "--no-ext-diff", "HEAD", "--"])
        .current_dir(root)
        .status()
        .map_err(|error| error.to_string())?;
    match status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => Err(format!("git diff --cached --quiet failed: {status}")),
    }
}

fn git_index_lock_path(root: &Path) -> Result<PathBuf, String> {
    git_path(root, "index.lock")
}

fn git_path(root: &Path, name: &str) -> Result<PathBuf, String> {
    let path = git_stdout(root, &["rev-parse", "--git-path", name])?;
    let path = PathBuf::from(path);
    Ok(if path.is_absolute() {
        path
    } else {
        root.join(path)
    })
}

fn shared_checkout_has_git_operation(root: &Path) -> Result<bool, String> {
    for state in [
        "MERGE_HEAD",
        "AUTO_MERGE",
        "MERGE_MODE",
        "CHERRY_PICK_HEAD",
        "REVERT_HEAD",
        "REBASE_HEAD",
        "rebase-merge",
        "rebase-apply",
        "sequencer",
    ] {
        if git_path(root, state)?.exists() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn recoverable_process_matches(run: &RecoverableRun) -> bool {
    if run.state != "running" {
        return false;
    }
    let Some(pid) = run.pid.and_then(|pid| u32::try_from(pid).ok()) else {
        return false;
    };
    process_identity_matches(pid, run.pid_start_time)
}

fn process_identity_matches(pid: u32, expected_start_time: Option<i64>) -> bool {
    matches!(
        (expected_start_time, process_start_time(pid)),
        (Some(expected), Some(actual)) if expected == actual
    )
}

/// SIGKILLs whatever is still alive in an exited run's process group and
/// returns whether live members were found; ESRCH is the clean common case.
fn kill_straggler_process_group(group: u32) -> bool {
    let group = -(group as libc::pid_t);
    let stragglers_present = unsafe { libc::kill(group, 0) } == 0;
    if stragglers_present {
        unsafe {
            libc::kill(group, libc::SIGKILL);
        }
    }
    stragglers_present
}

fn mark_storage_full(state: &DispatcherState, error: &StoreError) {
    if error.is_disk_full() && !state.storage_full.replace(true) {
        state.log.emit_with_fields(
            LogLevel::Error,
            "sloop::dispatcher",
            "storage_full",
            json!({"error": error.to_string()}),
        );
    }
}

fn recover_storage(state: &DispatcherState, now_ms: i64) -> bool {
    if !state.storage_full.get() {
        return true;
    }
    match state.store.probe_writable(now_ms) {
        Ok(()) => {
            state.storage_full.set(false);
            state
                .log
                .emit(LogLevel::Info, "sloop::dispatcher", "storage_recovered");
            true
        }
        Err(error) => {
            mark_storage_full(state, &error);
            false
        }
    }
}

/// The single spawn decision point: every queued activation passes the same
/// pause and capacity gates, selects deterministically, claims conditionally,
/// and only then touches Git and processes.
fn reconcile(state: &mut DispatcherState, events: &mpsc::Sender<RunEvent>, log: &OperationalLog) {
    let now_ms = state.clock.now_ms();
    if !recover_storage(state, now_ms) {
        return;
    }
    settle_pending_exits(state, log);
    if state.storage_full.get()
        || state.paused
        || state.agent.is_none()
        || !running_hours_open(state, now_ms)
    {
        return;
    }
    let activations = match state.store.dispatchable_activations(now_ms) {
        Ok(activations) => activations,
        Err(error) => {
            log.emit_with_fields(
                LogLevel::Error,
                "sloop::dispatcher",
                "activation_scan_failed",
                json!({"error": error.to_string()}),
            );
            return;
        }
    };

    for activation in activations {
        if state.active.len() >= state.max_agents {
            break;
        }
        let Some(ticket_id) = eligible_ticket(&state.store, &activation, now_ms) else {
            continue;
        };

        let now_ms = state.clock.now_ms();
        let run_ordinal = match state.store.next_run_ordinal() {
            Ok(ordinal) => ordinal,
            Err(error) => {
                mark_storage_full(state, &error);
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::dispatcher",
                    "run_id_reservation_failed",
                    json!({"error": error.to_string()}),
                );
                if error.is_disk_full() {
                    break;
                }
                continue;
            }
        };
        let run_id = format!("R{run_ordinal}");
        let owner = format!("daemon-{}", state.pid);
        let claim = ClaimRequest {
            ticket_id: &ticket_id,
            run_id: &run_id,
            activation_id: &activation.id,
            owner_id: &owner,
            lease_ms: DEFAULT_LEASE_MS,
            next_activation_eligible_at_ms: if activation.kind == "every" {
                match (activation.eligible_at_ms, activation.interval_ms) {
                    (Some(eligible_at_ms), Some(interval_ms)) => {
                        rearm_every_at(eligible_at_ms, interval_ms, now_ms)
                    }
                    _ => None,
                }
            } else {
                None
            },
        };
        if activation.kind == "every" && claim.next_activation_eligible_at_ms.is_none() {
            log.emit_with_fields(
                LogLevel::Error,
                "sloop::dispatcher",
                "invalid_recurring_activation",
                json!({"activation_id": activation.id}),
            );
            continue;
        }
        let claimed = match state.store.claim_ticket(&claim, now_ms) {
            Ok(claimed) => claimed,
            // Not ready right now; the activation stays queued for later.
            Err(StoreError::TicketNotReady { .. }) => continue,
            Err(error) => {
                mark_storage_full(state, &error);
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::dispatcher",
                    "claim_failed",
                    json!({
                        "activation_id": activation.id,
                        "ticket_id": ticket_id,
                        "run_id": run_id,
                        "error": error.to_string(),
                    }),
                );
                if error.is_disk_full() {
                    break;
                }
                continue;
            }
        };
        let flow = match bound_flow(&state.store, &state.flows, &ticket_id) {
            Ok(flow) => flow,
            Err(error) => {
                if let Err(abort_error) = state.store.abort_claim(&run_id, &ticket_id, now_ms) {
                    mark_storage_full(state, &abort_error);
                    log.emit_with_fields(
                        LogLevel::Error,
                        "sloop::dispatcher",
                        "claim_abort_failed",
                        json!({"run_id": run_id, "ticket_id": ticket_id, "error": abort_error.to_string()}),
                    );
                }
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::dispatcher",
                    "bound_flow_resolution_failed",
                    json!({"run_id": run_id, "ticket_id": ticket_id, "error": error}),
                );
                continue;
            }
        };
        match launch_agent(state, &run_id, &ticket_id, claimed.attempt) {
            Ok(launched) => {
                state.active.insert(run_id.clone());
                let events = events.clone();
                let exited_run = run_id.clone();
                let root = state.root.clone();
                let test_cmd = state.aftercare_test_cmd.clone();
                let clock = state.clock.clone();
                let classifier = state.classifier.clone();
                let supervisor_log = log.clone();
                let state_dir = state.state_dir.clone();
                let db_path = state.state_dir.join("sloop.db");
                let LaunchedRun {
                    mut child,
                    readers,
                    worktree,
                    branch,
                    output_log,
                    target,
                    worker_listener,
                    worker_token,
                    worker_socket_path,
                } = launched;
                state.worker_tokens.insert(run_id.clone(), worker_token);
                state
                    .worker_socket_paths
                    .insert(run_id.clone(), worker_socket_path);
                let accept_loop = tokio::spawn(serve_worker_socket(
                    worker_listener,
                    run_id.clone(),
                    state.requests_tx.clone(),
                    state.log.clone(),
                ));
                state.worker_listeners.insert(run_id.clone(), accept_loop);
                let pid = child.id();
                tokio::task::spawn_blocking(move || {
                    let exit_code = match child.wait() {
                        Ok(status) => status.code(),
                        Err(error) => {
                            supervisor_log.emit_with_fields(
                                LogLevel::Error,
                                "sloop::supervisor",
                                "agent_wait_failed",
                                json!({"run_id": exited_run, "error": error.to_string()}),
                            );
                            None
                        }
                    };
                    // Stragglers inherit the pipes and would keep the
                    // readers below from ever reaching EOF.
                    if kill_straggler_process_group(pid) {
                        supervisor_log.emit_with_fields(
                            LogLevel::Info,
                            "sloop::supervisor",
                            "stragglers_killed",
                            json!({"run_id": exited_run, "process_group_id": pid}),
                        );
                    }
                    // Capture must be complete on disk before the exit is
                    // reported; the readers end when the pipes close.
                    let mut capture_complete = true;
                    for reader in readers {
                        capture_complete &= reader.join().unwrap_or(false);
                    }
                    let vendor_error = match classify_run_output(
                        &classifier,
                        &state_dir,
                        &exited_run,
                        exit_code,
                    ) {
                        Ok(classification) => classification,
                        Err(error) => {
                            capture_complete = false;
                            supervisor_log.emit_with_fields(
                                LogLevel::Error,
                                "sloop::supervisor",
                                "vendor_error_classification_failed",
                                json!({"run_id": exited_run, "error": error}),
                            );
                            None
                        }
                    };
                    let cooldown_until_ms = vendor_error
                        .as_ref()
                        .filter(|error| error.class.requires_cooldown())
                        .map(|_| clock.now_ms() + VENDOR_COOLDOWN_MS);
                    let mut checkpoint_store = match Store::open(&db_path, clock.now_ms()) {
                        Ok(store) => Some(store),
                        Err(error) => {
                            supervisor_log.emit_with_fields(
                                LogLevel::Error,
                                "sloop::supervisor",
                                "aftercare_checkpoint_open_failed",
                                json!({"run_id": exited_run, "error": error.to_string()}),
                            );
                            None
                        }
                    };
                    let Some((commits, commit_observation_complete, aftercare_failed, merge)) =
                        gather_exit_evidence(
                            &exited_run,
                            &root,
                            &worktree,
                            &branch,
                            &flow,
                            test_cmd.as_deref(),
                            clock.as_ref(),
                            &output_log,
                            exit_code,
                            capture_complete,
                            vendor_error.as_ref(),
                            cooldown_until_ms,
                            checkpoint_store.as_mut(),
                            &supervisor_log,
                        )
                    else {
                        return;
                    };
                    let _ = events.blocking_send(RunEvent::Exited {
                        run_id: exited_run,
                        target,
                        exit_code,
                        capture_complete,
                        commits,
                        commit_observation_complete,
                        aftercare_failed,
                        merge,
                        vendor_error,
                        cooldown_until_ms,
                        recovery: None,
                    });
                });
                log.emit_with_fields(
                    LogLevel::Info,
                    "sloop::dispatcher",
                    "run_started",
                    json!({"run_id": run_id, "ticket_id": ticket_id, "pid": pid}),
                );
            }
            Err(error) => {
                if let Some(store_error) = error.store_error() {
                    mark_storage_full(state, store_error);
                }
                if let Err(abort_error) =
                    state
                        .store
                        .abort_claim(&run_id, &ticket_id, state.clock.now_ms())
                {
                    mark_storage_full(state, &abort_error);
                    log.emit_with_fields(
                        LogLevel::Error,
                        "sloop::dispatcher",
                        "claim_abort_failed",
                        json!({
                            "run_id": run_id,
                            "ticket_id": ticket_id,
                            "error": abort_error.to_string(),
                        }),
                    );
                }
                // A launch can fail after the worker socket was bound.
                close_worker_socket(state, &run_id);
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::dispatcher",
                    "run_launch_failed",
                    json!({
                        "run_id": run_id,
                        "ticket_id": ticket_id,
                        "error": error.to_string(),
                    }),
                );
            }
        }
    }
}

fn running_hours_open(state: &DispatcherState, now_ms: i64) -> bool {
    state
        .running_hours
        .as_ref()
        .is_none_or(|hours| hours.is_open(state.clock.local_minute(now_ms)))
}

fn next_dispatch_deadline(state: &DispatcherState) -> Option<i64> {
    let now_ms = state.clock.now_ms();
    let cooldown_deadline = state.store.next_active_cooldown(now_ms).ok().flatten();
    let next_eligible = state
        .store
        .next_activation_eligible_at_ms(now_ms)
        .ok()
        .flatten();
    let hours_deadline = 'hours: {
        let Some(hours) = state.running_hours.as_ref() else {
            break 'hours next_eligible;
        };
        if hours.is_open(state.clock.local_minute(now_ms)) {
            break 'hours next_eligible;
        }
        let opening = hours.next_opening_ms(state.clock.as_ref(), now_ms);
        let has_due_demand = state
            .store
            .dispatchable_activations(now_ms)
            .is_ok_and(|activations| !activations.is_empty());
        if has_due_demand || next_eligible.is_some_and(|deadline| deadline <= opening) {
            Some(opening)
        } else {
            next_eligible
        }
    };
    [hours_deadline, cooldown_deadline]
        .into_iter()
        .flatten()
        .min()
}

/// Advances a recurring cadence to its first future slot. Missed slots are
/// skipped deterministically so reopening a dispatch window cannot cause a
/// burst of catch-up runs.
fn rearm_every_at(eligible_at_ms: i64, interval_ms: i64, now_ms: i64) -> Option<i64> {
    if interval_ms <= 0 || eligible_at_ms > now_ms {
        return None;
    }
    let missed = now_ms.checked_sub(eligible_at_ms)?.div_euclid(interval_ms);
    let steps = missed.checked_add(1)?;
    eligible_at_ms.checked_add(interval_ms.checked_mul(steps)?)
}

fn eligible_ticket(store: &Store, activation: &QueuedActivation, now_ms: i64) -> Option<String> {
    match &activation.ticket_id {
        Some(ticket) if store.ticket_is_dispatchable(ticket).unwrap_or(false) => {
            let record = store.ticket(ticket).ok().flatten()?;
            let target = record.target.as_deref()?;
            store
                .active_cooldown_for_target(target, now_ms)
                .ok()
                .flatten()
                .is_none()
                .then(|| ticket.clone())
        }
        Some(_) => None,
        None => store.select_ready_ticket(activation, now_ms).ok().flatten(),
    }
}

fn bound_flow(
    store: &Store,
    flows: &BTreeMap<String, Flow>,
    ticket_id: &str,
) -> Result<Flow, String> {
    let ticket = store
        .ticket(ticket_id)
        .map_err(|error| format!("cannot read ticket `{ticket_id}`: {error}"))?
        .ok_or_else(|| format!("ticket `{ticket_id}` no longer exists"))?;
    let flow_name = ticket
        .flow
        .ok_or_else(|| format!("ticket `{ticket_id}` has no bound flow"))?;
    flows
        .get(&flow_name)
        .cloned()
        .ok_or_else(|| format!("ticket `{ticket_id}` names unknown bound flow `{flow_name}`"))
}

/// Gathers post-exit evidence in the supervisor thread, keeping slow Git and
/// flow execution out of the dispatcher.
#[allow(clippy::too_many_arguments)]
fn gather_exit_evidence(
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

fn aftercare_cancelled(store: &Store, run_id: &str, log: &OperationalLog) -> bool {
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
fn try_commits_on_branch(root: &Path, branch: &str) -> Result<Vec<String>, String> {
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

fn git_stdout(root: &Path, args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(root)
        .output()
        .map_err(|error| error.to_string())?;
    match output {
        output if output.status.success() => {
            Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
        }
        output => Err(format!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        )),
    }
}

/// Runs one exec stage in the run's worktree, capturing its output as
/// `aftercare` evidence in the same ordered run log.
#[allow(clippy::too_many_arguments)]
fn run_exec_stage(
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
fn attempt_merge(
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

#[allow(clippy::too_many_arguments)]
fn record_merge_process_checkpoint(
    store: &Store,
    run_id: &str,
    stage: &str,
    pid: u32,
    pid_start_time: i64,
    checkpoint: &MergeProcessCheckpoint,
    now_ms: i64,
) -> Result<(), StoreError> {
    store.record_aftercare_evidence(
        run_id,
        "aftercare_process",
        &json!({
            "stage": stage,
            "pid": pid,
            "pid_start_time": pid_start_time,
            "process_group_id": pid,
            "merge": {
                "target_head": checkpoint.target_head,
                "branch_tip": checkpoint.branch_tip,
                "completed_target": checkpoint.completed_target,
            },
        })
        .to_string(),
        now_ms,
    )
}

fn merge_checkout_ready(root: &Path) -> Result<bool, String> {
    Ok(!shared_checkout_has_git_operation(root)?
        && !git_index_lock_path(root)?.exists()
        && git_index_matches_head(root)?)
}

fn classify_run_output(
    classifier: &VendorErrorClassifier,
    state_dir: &Path,
    run_id: &str,
    exit_code: Option<i32>,
) -> Result<Option<VendorErrorMatch>, String> {
    let mut scanner = classifier.scanner(exit_code);
    crate::run_log::visit_agent_output(&run_output_path(state_dir, run_id), |stream, bytes| {
        match stream {
            OutputStream::Stdout => scanner.feed_stdout(bytes),
            OutputStream::Stderr => scanner.feed_stderr(bytes),
        }
    })
    .map_err(|error| format!("cannot read captured agent output: {error}"))?;
    Ok(scanner.finish())
}

/// Identity of the daemon owning a project's lockfile: PID plus process start
/// time, mirroring the identity rule used for supervised agents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockIdentity {
    pub pid: u32,
    pub started_at_ms: Option<i64>,
    pub socket: Option<PathBuf>,
}

pub fn read_lock_identity(path: &Path) -> Option<LockIdentity> {
    let content = fs::read_to_string(path).ok()?;
    let value: serde_json::Value = serde_json::from_str(content.trim()).ok()?;
    Some(LockIdentity {
        pid: u32::try_from(value["pid"].as_u64()?).ok()?,
        started_at_ms: value["started_at_ms"].as_i64(),
        socket: value["socket"].as_str().map(PathBuf::from),
    })
}

#[cfg(debug_assertions)]
fn wait_for_test_hook(name: &str) {
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
fn wait_for_test_hook(_name: &str) {}

/// Accepts connections on one run's worker socket until the settle path
/// aborts this task. Each connection is one request, mirroring the
/// operator socket.
async fn serve_worker_socket(
    listener: UnixListener,
    run_id: String,
    dispatcher: mpsc::Sender<DispatcherMessage>,
    log: OperationalLog,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let run_id = run_id.clone();
        let dispatcher = dispatcher.clone();
        let log = log.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_worker_connection(stream, run_id.clone(), dispatcher).await {
                log.emit_with_fields(
                    LogLevel::Error,
                    "sloop::socket",
                    "worker_connection_failed",
                    json!({"run_id": run_id, "error": error.to_string()}),
                );
            }
        });
    }
}

/// Serves a worker verb after proving the caller holds the run's token.
/// Everything an agent can reach flows through here: `brief` and `show` are
/// scoped reads, `note` is the only write and moves nothing.
fn dispatch_worker(
    state: &mut DispatcherState,
    id: RequestId,
    request: Request,
    run_id: &str,
    token: Option<&str>,
) -> ResponseEnvelope {
    let valid = token.is_some_and(|presented| {
        state
            .worker_tokens
            .get(run_id)
            .is_some_and(|issued| issued == presented)
    });
    if !valid {
        return ResponseEnvelope::failure(
            Some(id),
            unauthorized("the presented token is not valid for this run"),
        );
    }

    let data = match request {
        Request::Brief(_) => handle_brief(state, run_id),
        Request::Show(args) => handle_show(state, run_id, &args.reference),
        Request::Note(args) => handle_note(state, run_id, &args.text),
        // The connection handler already rejected operator verbs.
        _ => Err(unauthorized(
            "operator verbs are not available on a worker socket",
        )),
    };
    match data {
        Ok(data) => ResponseEnvelope::success(Some(id), data),
        Err(error) => ResponseEnvelope::failure(Some(id), error),
    }
}

/// Everything the agent needs to work, re-readable after a compaction: the
/// ticket body from its committed file, the isolated workspace, and the
/// evidence-based definition of done.
fn handle_brief(state: &DispatcherState, run_id: &str) -> Result<serde_json::Value, ErrorBody> {
    let run = lookup(state, |store| store.run(run_id))?
        .ok_or_else(|| internal("the run for this token no longer exists"))?;
    let ticket = lookup(state, |store| store.ticket(&run.ticket_id))?
        .ok_or_else(|| internal("the ticket for this run no longer exists"))?;
    let body = ticket
        .file_path
        .as_ref()
        .and_then(|file_path| fs::read_to_string(state.root.join(file_path)).ok())
        .unwrap_or_default();

    let mut definition_of_done = vec!["Commit your work to the run branch".to_owned()];
    if state.aftercare_test_cmd.is_some() {
        definition_of_done.push("The configured test command passes".to_owned());
    }

    Ok(json!({
        "run": run_id,
        "ticket": {
            "id": ticket.id,
            "name": ticket.name,
            "blocked_by": ticket.blocked_by,
            "worktree": ticket.worktree,
            "body": body,
            "acceptance": [],
            "target": ticket.target,
            "model": ticket.model,
            "effort": ticket.effort,
        },
        "worktree": run.worktree_path,
        "branch": run.branch,
        "definition_of_done": definition_of_done,
    }))
}

/// Read-only lookup, scoped to the run's own ticket. Whether a foreign
/// reference exists is not the worker's to learn: everything else is
/// uniformly unauthorized.
fn handle_show(
    state: &DispatcherState,
    run_id: &str,
    reference: &str,
) -> Result<serde_json::Value, ErrorBody> {
    let run = lookup(state, |store| store.run(run_id))?
        .ok_or_else(|| internal("the run for this token no longer exists"))?;
    if reference != run.ticket_id {
        return Err(unauthorized("workers may only show their own run's ticket"));
    }
    let ticket = lookup(state, |store| store.ticket(&run.ticket_id))?
        .ok_or_else(|| internal("the ticket for this run no longer exists"))?;
    let vendor_error = current_ticket_vendor_error(state, &ticket)?;
    Ok(ticket_show(reference, &ticket, vendor_error.as_ref()))
}

fn ticket_show(
    reference: &str,
    ticket: &crate::store::TicketRecord,
    vendor_error: Option<&VendorErrorMatch>,
) -> serde_json::Value {
    json!({
        "ref": reference,
        "kind": "ticket",
        "value": {
            "id": ticket.id,
            "project": ticket.project_id,
            "state": ticket.state,
            "file": ticket.file_path,
            "name": ticket.name,
            "blocked_by": ticket.blocked_by,
            "worktree": ticket.worktree,
            "target": ticket.target,
            "model": ticket.model,
            "effort": ticket.effort,
            "reason": vendor_error.map(|error| error.diagnostic.as_str()),
            "classification": vendor_error,
        },
    })
}

fn current_ticket_vendor_error(
    state: &DispatcherState,
    ticket: &crate::store::TicketRecord,
) -> Result<Option<VendorErrorMatch>, ErrorBody> {
    let vendor_error = lookup(state, |store| {
        store.latest_vendor_error_for_ticket(&ticket.id)
    })?;
    if ticket.state != "ready" {
        return Ok(vendor_error);
    }
    let cooldown_active = match ticket.target.as_deref() {
        Some(target) => lookup(state, |store| {
            store.active_cooldown_for_target(target, state.clock.now_ms())
        })?
        .is_some(),
        None => false,
    };
    Ok(vendor_error.filter(|error| error.class.requires_cooldown() && cooldown_active))
}

fn handle_operator_show(
    state: &DispatcherState,
    reference: &str,
) -> Result<serde_json::Value, ErrorBody> {
    if let Some(ticket) = lookup(state, |store| store.ticket(reference))? {
        let vendor_error = current_ticket_vendor_error(state, &ticket)?;
        return Ok(ticket_show(reference, &ticket, vendor_error.as_ref()));
    }
    let project = lookup(state, |store| store.project(reference))?
        .ok_or_else(|| not_found(&format!("reference `{reference}` is not indexed")))?;
    let tickets = lookup(state, |store| store.tickets_for_project(reference))?;
    let mut vendor_errors = HashMap::new();
    for ticket in &tickets {
        if let Some(error) = current_ticket_vendor_error(state, ticket)? {
            vendor_errors.insert(ticket.id.clone(), error);
        }
    }

    let mut notes: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
    for note in lookup(state, |store| store.notes_for_project(reference))? {
        notes.entry(note.ticket_id).or_default().push(json!({
            "id": note.id,
            "run": note.run_id,
            "text": note.text,
            "recorded_at_ms": note.recorded_at_ms,
        }));
    }

    let mut commits: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
    for evidence in lookup(state, |store| store.commit_evidence_for_project(reference))? {
        let data: serde_json::Value = serde_json::from_str(&evidence.data_json)
            .map_err(|error| internal(&format!("cannot decode commit evidence: {error}")))?;
        for oid in data["oids"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or_default()
            .iter()
            .filter_map(serde_json::Value::as_str)
        {
            let (short_hash, message) = git_commit(&state.root, oid)?;
            commits
                .entry(evidence.ticket_id.clone())
                .or_default()
                .push(json!({
                    "run": evidence.run_id.clone(),
                    "hash": short_hash,
                    "message": message,
                }));
        }
    }

    let activity = tickets
        .into_iter()
        .map(|ticket| {
            let ticket_notes = notes.remove(&ticket.id).unwrap_or_default();
            let ticket_commits = commits.remove(&ticket.id).unwrap_or_default();
            let vendor_error = vendor_errors.remove(&ticket.id);
            json!({
                "id": ticket.id,
                "name": ticket.name,
                "state": ticket.state,
                "notes": ticket_notes,
                "commits": ticket_commits,
                "reason": vendor_error.as_ref().map(|error| error.diagnostic.as_str()),
                "classification": vendor_error,
            })
        })
        .collect::<Vec<_>>();

    Ok(json!({
        "ref": reference,
        "kind": "project",
        "value": {
            "id": project.id,
            "title": project.title,
            "file": project.file_path,
            "tickets": activity,
        },
    }))
}

fn git_commit(root: &Path, oid: &str) -> Result<(String, String), ErrorBody> {
    let output = Command::new("git")
        .args(["show", "--no-patch", "--format=%h%x00%s", oid, "--"])
        .current_dir(root)
        .output()
        .map_err(|error| internal(&format!("cannot read commit `{oid}`: {error}")))?;
    if !output.status.success() {
        return Err(internal(&format!(
            "cannot read commit `{oid}`: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let rendered = String::from_utf8_lossy(&output.stdout);
    let (hash, message) = rendered
        .trim_end()
        .split_once('\0')
        .ok_or_else(|| internal(&format!("Git returned malformed data for commit `{oid}`")))?;
    Ok((hash.to_owned(), message.to_owned()))
}

/// The agent's only write: an advisory note recorded against its run. It
/// transitions nothing.
fn handle_note(
    state: &DispatcherState,
    run_id: &str,
    text: &str,
) -> Result<serde_json::Value, ErrorBody> {
    let ordinal = lookup(state, |store| store.next_note_ordinal())?;
    let note_id = format!("N{ordinal}");
    state
        .store
        .insert_note(&note_id, run_id, text, state.clock.now_ms())
        .map_err(|error| {
            mark_storage_full(state, &error);
            internal(&format!("cannot record note: {error}"))
        })?;
    Ok(json!({"note": {"id": note_id, "run": run_id, "text": text}}))
}

fn dispatch(state: &mut DispatcherState, id: RequestId, request: Request) -> ResponseEnvelope {
    let data = match request {
        Request::Show(args) => match handle_operator_show(state, &args.reference) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Run(args) => match handle_run(state, &args) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Daemon(_) => json!({
            "pid": state.pid,
            "socket": state.socket.to_string_lossy(),
            "state_dir": state.state_dir.to_string_lossy(),
            "log": state.daemon_log.to_string_lossy(),
            "version": env!("CARGO_PKG_VERSION"),
            "started": false
        }),
        Request::Post(args) => {
            let now_ms = state.clock.now_ms();
            let at_eligible_ms = match &args.activation {
                crate::protocol::PostActivation::At { time } => {
                    let Some(minute) = parse_local_time(time) else {
                        return ResponseEnvelope::failure(
                            Some(id),
                            invalid_arguments(&format!(
                                "time `{time}` must use a valid HH:MM value"
                            )),
                        );
                    };
                    let Some(eligible_at_ms) =
                        next_local_minute_ms(state.clock.as_ref(), now_ms, minute)
                    else {
                        return ResponseEnvelope::failure(
                            Some(id),
                            invalid_arguments("the requested local time is out of range"),
                        );
                    };
                    Some(eligible_at_ms)
                }
                _ => None,
            };
            match crate::post::handle(
                &state.root,
                &state.ticket_dir,
                &state.store,
                &args,
                now_ms,
                at_eligible_ms,
                &state.ticket_prefix,
                state.agent.as_ref(),
                &state.flows,
                &state.default_flow,
            ) {
                Ok(data) => data,
                Err(error) => {
                    if let crate::post::PostError::Store(store_error) = &error {
                        mark_storage_full(state, store_error);
                    }
                    return ResponseEnvelope::failure(Some(id), post_error_body(&error));
                }
            }
        }
        Request::List(_) => match handle_list(state) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Status(_) => {
            let tickets = match state.store.ticket_counts() {
                Ok(counts) => counts,
                Err(error) => {
                    return ResponseEnvelope::failure(
                        Some(id),
                        internal(&format!("cannot read ticket counts: {error}")),
                    );
                }
            };
            let runs: Vec<_> = match state.store.active_runs() {
                Ok(runs) => runs
                    .into_iter()
                    .map(|run| {
                        json!({
                            "id": run.id,
                            "project": run.project_id,
                            "ticket": run.ticket_id,
                            "state": run.state,
                        })
                    })
                    .collect(),
                Err(error) => {
                    return ResponseEnvelope::failure(
                        Some(id),
                        internal(&format!("cannot read active runs: {error}")),
                    );
                }
            };
            let queued: Vec<_> = match state.store.queued_activations() {
                Ok(activations) => activations
                    .into_iter()
                    .map(|activation| {
                        json!({
                            "id": activation.id,
                            "ticket": activation.ticket_id,
                            "project": activation.project_id,
                            "state": "queued",
                        })
                    })
                    .collect(),
                Err(error) => {
                    return ResponseEnvelope::failure(
                        Some(id),
                        internal(&format!("cannot read queued activations: {error}")),
                    );
                }
            };
            let now_ms = state.clock.now_ms();
            let mut gate = json!({
                "active_agents": state.active.len(),
                "max_agents": state.max_agents,
                "storage": {
                    "writable": !state.storage_full.get(),
                    "reason": state.storage_full.get().then_some("database_full"),
                },
            });
            if let Some(hours) = &state.running_hours {
                gate["running_hours"] = json!({
                    "start": hours.start,
                    "end": hours.end,
                    "open": hours.is_open(state.clock.local_minute(now_ms)),
                });
            }
            let cooldowns = match state.store.active_cooldowns(now_ms) {
                Ok(cooldowns) => cooldowns
                    .into_iter()
                    .map(|cooldown| {
                        json!({
                            "target": cooldown.target,
                            "until_ms": cooldown.until_ms,
                            "reason": cooldown.reason,
                        })
                    })
                    .collect::<Vec<_>>(),
                Err(error) => {
                    return ResponseEnvelope::failure(
                        Some(id),
                        internal(&format!("cannot read cooldowns: {error}")),
                    );
                }
            };
            gate["cooldowns"] = json!(cooldowns);
            let mut snapshot = json!({
                "daemon": {"pid": state.pid, "paused": state.paused},
                "gate": gate,
                "runs": runs,
                "queued_activations": queued,
                "tickets": {
                    "ready": tickets.ready,
                    "held": tickets.held,
                    "blocked": tickets.blocked,
                    "claimed": tickets.claimed,
                    "merged": tickets.merged,
                    "failed": tickets.failed,
                    "needs_review": tickets.needs_review
                }
            });
            if let Some(deadline) = next_dispatch_deadline(state)
                && let Some(formatted) = format_timestamp(deadline)
            {
                snapshot["next_wake"] = json!(formatted);
            }
            snapshot
        }
        Request::Pause(_) => {
            if let Err(error) = state.store.set_paused(true, state.clock.now_ms()) {
                mark_storage_full(state, &error);
                return ResponseEnvelope::failure(
                    Some(id),
                    internal(&format!("cannot pause scheduler: {error}")),
                );
            }
            state.paused = true;
            json!({"paused": true})
        }
        Request::Resume(_) => {
            if let Err(error) = state.store.set_paused(false, state.clock.now_ms()) {
                mark_storage_full(state, &error);
                return ResponseEnvelope::failure(
                    Some(id),
                    internal(&format!("cannot resume scheduler: {error}")),
                );
            }
            state.paused = false;
            json!({"paused": false})
        }
        Request::Hold(args) => match handle_hold(state, &args) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Ready(args) => match handle_ready(state, &args) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Retry(args) => match handle_retry(state, &args) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Logs(args) => match handle_logs(state, &args) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Cancel(args) => match handle_cancel(state, &args) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Reindex(_) => match handle_reindex(state) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Stop(args) => match handle_stop(state, &args) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        Request::Wait(args) => match handle_wait(state, &args) {
            Ok(data) => data,
            Err(error) => return ResponseEnvelope::failure(Some(id), error),
        },
        request => {
            return ResponseEnvelope::failure(
                Some(id),
                ErrorBody {
                    code: ErrorCode::InvalidRequest,
                    message: format!("verb `{}` is not implemented by the daemon", request.verb()),
                    details: json!({"verb": request.verb()}),
                },
            );
        }
    };
    ResponseEnvelope::success(Some(id), data)
}

fn handle_list(state: &DispatcherState) -> Result<serde_json::Value, ErrorBody> {
    let now_ms = state.clock.now_ms();
    let gates = crate::eligibility::Gates {
        paused: state.paused,
        storage_writable: !state.storage_full.get(),
        agent_configured: state.agent.is_some(),
        hours_open: running_hours_open(state, now_ms),
        at_capacity: state.active.len() >= state.max_agents,
        has_queued_activation: !lookup(state, Store::queued_activations)?.is_empty(),
    };
    let mut rows = Vec::new();
    for ticket in lookup(state, Store::tickets)? {
        let active_run = lookup(state, |store| store.active_run_for_ticket(&ticket.id))?;
        let blockers = lookup(state, |store| store.unmerged_blockers(&ticket.id))?;
        let mut vendor_error = lookup(state, |store| {
            store.latest_vendor_error_for_ticket(&ticket.id)
        })?;
        let cooldown = match ticket.target.as_deref() {
            Some(target) => lookup(state, |store| {
                store.active_cooldown_for_target(target, now_ms)
            })?,
            None => None,
        };
        if ticket.state == "ready"
            && !vendor_error
                .as_ref()
                .is_some_and(|error| error.class.requires_cooldown() && cooldown.is_some())
        {
            vendor_error = None;
        }
        let ineligibility = crate::eligibility::ticket_ineligibility(
            &ticket.state,
            ticket.attempts,
            active_run.as_deref(),
            &blockers,
            &gates,
        );
        let display_state =
            crate::eligibility::display_state(&ticket.state, ineligibility.as_ref());
        let mut reason = ineligibility.map(|reason| reason.describe());
        if ticket.state == "failed"
            && let Some(error) = &vendor_error
        {
            reason = Some(format!(
                "{}; failed after {} attempt(s); requeue with `sloop retry`",
                error.diagnostic, ticket.attempts
            ));
        } else if ticket.state == "ready"
            && let (Some(target), Some(cooldown)) = (ticket.target.as_deref(), cooldown.as_ref())
        {
            reason = Some(format!(
                "target `{target}` is cooling down until {}: {}",
                format_timestamp(cooldown.until_ms)
                    .unwrap_or_else(|| cooldown.until_ms.to_string()),
                cooldown.reason
            ));
        }
        rows.push(json!({
            "id": ticket.id,
            "name": ticket.name,
            "project": ticket.project_id,
            "state": display_state,
            "run": active_run,
            "reason": reason,
            "classification": vendor_error,
        }));
    }
    Ok(json!({"tickets": rows}))
}

fn spawn_daemon(project: &Project) -> Result<(), DaemonError> {
    let executable = std::env::current_exe().map_err(DaemonError::CurrentExecutable)?;
    Command::new(executable)
        .args(["daemon", "--foreground"])
        .current_dir(&project.root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map(|_| ())
        .map_err(DaemonError::Spawn)
}

fn send_existing(project: &Project, request: Request) -> Result<ResponseEnvelope, DaemonError> {
    match send(&project.operator_socket, request.clone()) {
        Ok(response) => Ok(response),
        Err(current_error) => {
            let Some(identity) = read_lock_identity(&project.lock_path) else {
                return Err(current_error);
            };
            if !process_identity_matches(identity.pid, identity.started_at_ms) {
                return Err(current_error);
            }
            let Some(socket) = identity.socket else {
                return Err(current_error);
            };
            if socket == project.operator_socket {
                return Err(current_error);
            }
            send(&socket, request)
        }
    }
}

fn send(socket: &Path, request: Request) -> Result<ResponseEnvelope, DaemonError> {
    let mut stream = StdUnixStream::connect(socket).map_err(DaemonError::Connect)?;
    stream
        .set_read_timeout(Some(CLIENT_TIMEOUT))
        .map_err(DaemonError::Connect)?;
    stream
        .set_write_timeout(Some(CLIENT_TIMEOUT))
        .map_err(DaemonError::Connect)?;

    let sequence = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    let envelope = RequestEnvelope::new(
        RequestId::new(format!("req-{}-{sequence}", std::process::id())),
        request,
        None,
    );
    serde_json::to_writer(&mut stream, &envelope).map_err(DaemonError::Encode)?;
    stream.write_all(b"\n").map_err(DaemonError::Write)?;

    let mut line = String::new();
    let mut reader = BufReader::new(stream).take(MAX_ENVELOPE_BYTES + 1);
    reader.read_line(&mut line).map_err(DaemonError::Read)?;
    if line.len() as u64 > MAX_ENVELOPE_BYTES {
        return Err(DaemonError::InvalidResponse(
            "response envelope is too large".into(),
        ));
    }
    serde_json::from_str(line.trim_end()).map_err(DaemonError::Decode)
}

/// Validates a `run` request and persists one queued activation. Acceptance
/// never implies a spawn; reconciliation decides that separately.
fn handle_run(
    state: &mut DispatcherState,
    args: &crate::protocol::RunArgs,
) -> Result<serde_json::Value, ErrorBody> {
    use crate::protocol::RunActivation;

    if args.ticket.is_some() && args.project.is_some() {
        return Err(invalid_arguments(
            "a run may target a ticket or a project, not both",
        ));
    }
    if let Some(ticket_id) = &args.ticket {
        let Some(ticket) = lookup(state, |store| store.ticket(ticket_id))? else {
            return Err(not_found(&format!(
                "ticket `{ticket_id}` is not registered"
            )));
        };
        if ticket.state == TicketState::Held.as_str() {
            return Err(conflict(&format!(
                "ticket `{ticket_id}` is held; release it with `sloop ready {ticket_id}`"
            )));
        }
    }
    if let Some(project) = &args.project
        && !lookup(state, |store| store.project_exists(project))?
    {
        return Err(not_found(&format!("project `{project}` is not indexed")));
    }
    for only in &args.only {
        let Some(ticket) = lookup(state, |store| store.ticket(only))? else {
            return Err(not_found(&format!("ticket `{only}` is not registered")));
        };
        if let Some(project) = &args.project
            && &ticket.project_id != project
        {
            return Err(invalid_arguments(&format!(
                "ticket `{only}` belongs to project `{}`, not `{project}`",
                ticket.project_id
            )));
        }
    }

    let now_ms = state.clock.now_ms();
    let (kind, echo_kind, eligible_at_ms, interval_ms) = match &args.activation {
        RunActivation::Now => (ActivationKind::Immediate, "now", None, None),
        RunActivation::At { local_time } => {
            let minute = parse_local_time(local_time).ok_or_else(|| {
                invalid_arguments(&format!("time `{local_time}` must use a valid HH:MM value"))
            })?;
            let eligible_at_ms = next_local_minute_ms(state.clock.as_ref(), now_ms, minute)
                .ok_or_else(|| invalid_arguments("the requested local time is out of range"))?;
            (ActivationKind::At, "at", Some(eligible_at_ms), None)
        }
        RunActivation::Every { interval_ms } => {
            let interval_ms = i64::try_from(*interval_ms)
                .ok()
                .filter(|interval_ms| *interval_ms > 0)
                .ok_or_else(|| invalid_arguments("--every requires a positive interval"))?;
            let eligible_at_ms = now_ms
                .checked_add(interval_ms)
                .ok_or_else(|| invalid_arguments("--every interval is too large"))?;
            (
                ActivationKind::Every,
                "every",
                Some(eligible_at_ms),
                Some(interval_ms),
            )
        }
        RunActivation::Overnight => {
            let eligible_at_ms = state.running_hours.as_ref().map_or(now_ms, |hours| {
                if hours.is_open(state.clock.local_minute(now_ms)) {
                    now_ms
                } else {
                    hours.next_opening_ms(state.clock.as_ref(), now_ms)
                }
            });
            (
                ActivationKind::Overnight,
                "overnight",
                Some(eligible_at_ms),
                None,
            )
        }
    };
    let activation_id = format!(
        "A{}",
        lookup(state, |store| store.next_activation_ordinal())?
    );
    lookup(state, |store| {
        store.insert_activation(
            &NewActivation {
                id: &activation_id,
                kind,
                ticket_id: args.ticket.as_deref(),
                project_id: args.project.as_deref(),
                eligible_at_ms,
                interval_ms,
            },
            now_ms,
        )
    })?;
    for only in &args.only {
        lookup(state, |store| {
            store.insert_activation_filter(&activation_id, only)
        })?;
    }

    let mut activation = json!({
        "id": activation_id,
        "kind": echo_kind,
        "state": "queued",
    });
    if let Some(ticket) = &args.ticket {
        activation["ticket"] = json!(ticket);
    }
    if let Some(project) = &args.project {
        activation["project"] = json!(project);
    }
    if let Some(eligible_at_ms) = eligible_at_ms {
        activation["eligible_at_ms"] = json!(eligible_at_ms);
    }
    match &args.activation {
        RunActivation::At { local_time } => activation["local_time"] = json!(local_time),
        RunActivation::Every { .. } => activation["interval_ms"] = json!(interval_ms),
        RunActivation::Now | RunActivation::Overnight => {}
    }
    Ok(json!({"activation": activation}))
}

fn handle_hold(
    state: &mut DispatcherState,
    args: &crate::protocol::TicketReferenceArgs,
) -> Result<serde_json::Value, ErrorBody> {
    let requested = TicketState::Held;
    let previous = state
        .store
        .set_ticket_hold(&args.ticket, requested, state.clock.now_ms())
        .map_err(|error| match error {
            StoreError::TicketNotFound { .. } => not_found(&error.to_string()),
            StoreError::TicketStateConflict { .. } => conflict(&error.to_string()),
            _ => {
                mark_storage_full(state, &error);
                internal(&error.to_string())
            }
        })?;
    Ok(json!({
        "ticket": args.ticket,
        "previous_state": previous,
        "state": requested.as_str(),
        "overridden": previous != requested.as_str(),
    }))
}

fn handle_ready(
    state: &mut DispatcherState,
    args: &crate::protocol::TicketReferenceArgs,
) -> Result<serde_json::Value, ErrorBody> {
    let requested = TicketState::Ready;
    let previous = state
        .store
        .set_ticket_hold(&args.ticket, requested, state.clock.now_ms())
        .map_err(|error| match error {
            StoreError::TicketNotFound { .. } => not_found(&error.to_string()),
            StoreError::TicketStateConflict { .. } => conflict(&error.to_string()),
            _ => {
                mark_storage_full(state, &error);
                internal(&error.to_string())
            }
        })?;
    Ok(json!({
        "ticket": args.ticket,
        "previous_state": previous,
        "state": requested.as_str(),
        "overridden": previous != requested.as_str(),
    }))
}

fn handle_retry(
    state: &mut DispatcherState,
    args: &crate::protocol::TicketReferenceArgs,
) -> Result<serde_json::Value, ErrorBody> {
    let previous = state
        .store
        .retry_ticket(&args.ticket, state.clock.now_ms())
        .map_err(|error| match error {
            StoreError::TicketNotFound { .. } => not_found(&error.to_string()),
            StoreError::TicketStateConflict { .. } => conflict(&error.to_string()),
            _ => {
                mark_storage_full(state, &error);
                internal(&error.to_string())
            }
        })?;
    Ok(json!({
        "ticket": args.ticket,
        "previous_state": previous,
        "state": TicketState::Ready.as_str(),
    }))
}

/// One non-blocking snapshot of a run's state; the client loops. Launch and
/// recovery closures are terminal alongside ordinary derived outcomes.
fn handle_wait(
    state: &DispatcherState,
    args: &crate::protocol::RunReferenceArgs,
) -> Result<serde_json::Value, ErrorBody> {
    let Some(run) = lookup(state, |store| store.run(&args.run))? else {
        return Err(not_found(&format!("run `{}` does not exist", args.run)));
    };
    let terminal = matches!(
        run.state.as_str(),
        "merged"
            | "failed"
            | "needs_review"
            | "cancelled"
            | "rate_limited"
            | "orphaned"
            | "aborted"
    );
    let vendor_error = lookup(state, |store| store.vendor_error_for_run(&run.id))?;
    Ok(json!({
        "run": run.id,
        "state": run.state,
        "terminal": terminal,
        "exit_code": run.exit_code,
        "reason": vendor_error.as_ref().map(|error| error.diagnostic.as_str()),
        "classification": vendor_error,
    }))
}

/// Returns one finite page of captured run output. Records are stored
/// escaped inside the response; raw agent bytes never reach Sloop's stdout.
fn handle_logs(
    state: &DispatcherState,
    args: &crate::protocol::RunReferenceArgs,
) -> Result<serde_json::Value, ErrorBody> {
    if lookup(state, |store| store.run(&args.run))?.is_none() {
        return Err(not_found(&format!("run `{}` does not exist", args.run)));
    }
    let page = crate::run_log::read_page(
        &run_output_path(&state.state_dir, &args.run),
        0,
        LOGS_PAGE_LIMIT,
    )
    .map_err(|error| internal(&format!("cannot read run log: {error}")))?;
    let entries = page
        .entries
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| internal(&format!("cannot encode run log: {error}")))?;
    Ok(json!({
        "run": args.run,
        "entries": entries,
        "next_cursor": page.next_cursor,
        "complete": page.complete,
    }))
}

/// Records cancellation intent durably, then kills the run's whole process
/// group. Termination is confirmed by the exit event, which reads the intent
/// and settles the outcome as `Cancelled`; the worktree, branch, and captured
/// logs are preserved as evidence.
fn handle_cancel(
    state: &mut DispatcherState,
    args: &crate::protocol::RunReferenceArgs,
) -> Result<serde_json::Value, ErrorBody> {
    let Some(run) = lookup(state, |store| store.run(&args.run))? else {
        return Err(not_found(&format!("run `{}` does not exist", args.run)));
    };
    if !matches!(run.state.as_str(), "running" | "aftercare") || run.exited_at_ms.is_some() {
        return Err(conflict(&format!(
            "run `{}` is `{}` and cannot be cancelled",
            run.id, run.state
        )));
    }

    // Intent must be durable before any signal: if the daemon dies between
    // the kill and the exit event, recovery still reads the cancellation.
    lookup(state, |store| {
        store.record_cancel_requested(&run.id, state.clock.now_ms())
    })?;
    state.cancelling.insert(run.id.clone());

    if run.state == "aftercare" {
        let rows = lookup(state, |store| store.run_evidence(&run.id))?;
        if let Some(identity) =
            aftercare_process_identity(&rows, None).map_err(|error| internal(&error))?
        {
            if identity.group <= 0 {
                return Err(internal(
                    "the active aftercare stage has an invalid process group",
                ));
            }
            match stop_persisted_process_group(&identity) {
                Ok(PersistedProcessStop::LeaderMissing) => state.log.emit_with_fields(
                    LogLevel::Info,
                    "sloop::supervisor",
                    "stale_aftercare_group_not_signalled",
                    json!({"run_id": run.id, "process_group_id": identity.group}),
                ),
                Ok(PersistedProcessStop::StoppedOriginal) => {}
                Err(error) => {
                    state.log.emit_with_fields(
                        LogLevel::Error,
                        "sloop::supervisor",
                        "aftercare_cancel_signal_refused",
                        json!({"run_id": run.id, "error": error}),
                    );
                }
            }
        }
    } else {
        let process_matches = run
            .pid
            .and_then(|pid| u32::try_from(pid).ok())
            .is_some_and(|pid| process_identity_matches(pid, run.pid_start_time));
        if process_matches && let Some(group) = run.process_group_id {
            // A negative PID signals the whole group, so grandchildren die too.
            // ESRCH means the group already exited; the race resolves through
            // the recorded intent.
            unsafe {
                libc::kill(-(group as libc::pid_t), libc::SIGKILL);
            }
        }
    }

    Ok(json!({
        "run": run.id,
        "state": "cancelling",
        "worktree": run.worktree_path,
        "preserved": true,
    }))
}

/// Validates a stop request and, when forced, cancels every active run
/// through the same durable-intent path as `cancel`. The connection handler
/// owns the actual exit so the reply always reaches the caller first.
fn handle_stop(
    state: &mut DispatcherState,
    args: &crate::protocol::StopArgs,
) -> Result<serde_json::Value, ErrorBody> {
    let mut active: Vec<String> = state.active.iter().cloned().collect();
    active.sort();
    if !active.is_empty() && !args.force {
        return Err(conflict(&format!(
            "{} active run(s): {}; stop --force cancels them",
            active.len(),
            active.join(", "),
        )));
    }
    let mut cancelled = Vec::new();
    for run_id in active {
        if handle_cancel(
            state,
            &crate::protocol::RunReferenceArgs {
                run: run_id.clone(),
            },
        )
        .is_ok()
        {
            cancelled.push(run_id);
        }
    }
    Ok(json!({
        "stopping": true,
        "pid": state.pid,
        "cancelled_runs": cancelled,
    }))
}

fn handle_reindex(state: &mut DispatcherState) -> Result<serde_json::Value, ErrorBody> {
    let mut active: Vec<String> = state.active.iter().cloned().collect();
    active.sort();
    if !active.is_empty() {
        return Err(conflict(&format!(
            "{} active run(s): {}; reindex requires an idle daemon",
            active.len(),
            active.join(", "),
        )));
    }
    let now_ms = state.clock.now_ms();
    let project_ids = index_projects(
        &state.root,
        &state.project_dir,
        &state.store,
        now_ms,
        &state.project_prefix,
    )
    .map_err(|error| internal(&format!("cannot reindex projects: {error}")))?;
    crate::reindex::run(
        &state.root,
        &state.ticket_dir,
        &state.worktree_dir,
        &state.state_dir,
        &state.store,
        now_ms,
        &state.ticket_prefix,
        &project_ids,
        state.agent.as_ref(),
        &state.flows,
        &state.default_flow,
    )
    .map_err(|error| internal(&format!("cannot reindex tickets: {error}")))
}

fn lookup<T>(
    state: &DispatcherState,
    query: impl FnOnce(&Store) -> Result<T, StoreError>,
) -> Result<T, ErrorBody> {
    query(&state.store).map_err(|error| {
        mark_storage_full(state, &error);
        internal(&error.to_string())
    })
}

fn invalid_arguments(message: &str) -> ErrorBody {
    ErrorBody {
        code: ErrorCode::InvalidArguments,
        message: message.into(),
        details: json!({}),
    }
}

fn not_found(message: &str) -> ErrorBody {
    ErrorBody {
        code: ErrorCode::NotFound,
        message: message.into(),
        details: json!({}),
    }
}

fn conflict(message: &str) -> ErrorBody {
    ErrorBody {
        code: ErrorCode::Conflict,
        message: message.into(),
        details: json!({}),
    }
}

fn post_error_body(error: &crate::post::PostError) -> ErrorBody {
    use crate::post::PostError;
    let code = match error {
        PostError::TicketFileNotFound(_)
        | PostError::UnknownProject(_)
        | PostError::UnknownFlow { .. }
        | PostError::UnknownBlockedBy { .. } => ErrorCode::NotFound,
        PostError::OutsideRepository(_)
        | PostError::OutsideTicketDirectory { .. }
        | PostError::InvalidTicket { .. }
        | PostError::MissingName { .. }
        | PostError::MissingBlockedBy { .. }
        | PostError::InvalidBlockedBy { .. }
        | PostError::EmptyBody { .. }
        | PostError::UnknownTarget(_)
        | PostError::MissingTargetValue { .. } => ErrorCode::InvalidArguments,
        PostError::ProjectConflict { .. }
        | PostError::FlowConflict { .. }
        | PostError::TicketIdTaken { .. }
        | PostError::DependencyCycle(_) => ErrorCode::Conflict,
        PostError::Io { .. } | PostError::Store(_) | PostError::IdAllocation(_) => {
            ErrorCode::Internal
        }
    };
    ErrorBody {
        code,
        message: error.to_string(),
        details: json!({}),
    }
}

fn protocol_error(message: &str) -> ErrorBody {
    ErrorBody {
        code: ErrorCode::InvalidRequest,
        message: message.into(),
        details: json!({}),
    }
}

fn unauthorized(message: &str) -> ErrorBody {
    ErrorBody {
        code: ErrorCode::Unauthorized,
        message: message.into(),
        details: json!({}),
    }
}

fn internal(message: &str) -> ErrorBody {
    ErrorBody {
        code: ErrorCode::Internal,
        message: message.into(),
        details: json!({}),
    }
}

#[derive(Debug)]
pub enum DaemonError {
    Config(ConfigError),
    Catalog(CatalogError),
    Store(StoreError),
    CurrentDirectory(io::Error),
    CurrentExecutable(io::Error),
    Io {
        path: PathBuf,
        source: io::Error,
    },
    AlreadyRunning,
    Runtime(io::Error),
    Spawn(io::Error),
    Connect(io::Error),
    Write(io::Error),
    Read(io::Error),
    Encode(serde_json::Error),
    Decode(serde_json::Error),
    InvalidResponse(String),
    Frontmatter {
        path: PathBuf,
        error: FrontmatterError,
    },
    IdAllocation(IdError),
}

impl DaemonError {
    pub fn error_body(&self) -> ErrorBody {
        let code = match self {
            Self::Config(_) => ErrorCode::InvalidArguments,
            _ => ErrorCode::DaemonUnavailable,
        };
        ErrorBody {
            code,
            message: self.to_string(),
            details: json!({}),
        }
    }
}

impl From<ConfigError> for DaemonError {
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<IdError> for DaemonError {
    fn from(error: IdError) -> Self {
        Self::IdAllocation(error)
    }
}

impl std::fmt::Display for DaemonError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(error) => error.fmt(formatter),
            Self::Catalog(error) => error.fmt(formatter),
            Self::Store(error) => error.fmt(formatter),
            Self::CurrentDirectory(error) => {
                write!(formatter, "cannot read current directory: {error}")
            }
            Self::CurrentExecutable(error) => {
                write!(formatter, "cannot locate sloop executable: {error}")
            }
            Self::Io { path, source } => write!(formatter, "{}: {source}", path.display()),
            Self::AlreadyRunning => formatter.write_str("another sloop daemon holds the lock"),
            Self::Runtime(error) => write!(formatter, "cannot start async runtime: {error}"),
            Self::Spawn(error) => write!(formatter, "cannot spawn daemon: {error}"),
            Self::Connect(error) => write!(formatter, "cannot connect to daemon: {error}"),
            Self::Write(error) => write!(formatter, "cannot write daemon request: {error}"),
            Self::Read(error) => write!(formatter, "cannot read daemon response: {error}"),
            Self::Encode(error) => write!(formatter, "cannot encode daemon request: {error}"),
            Self::Decode(error) => write!(formatter, "cannot decode daemon response: {error}"),
            Self::InvalidResponse(message) => formatter.write_str(message),
            Self::Frontmatter { path, error } => write!(formatter, "{}: {error}", path.display()),
            Self::IdAllocation(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for DaemonError {}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::runner::compose_worker_prompt;
    use super::{
        PersistedProcessState, WORKER_BOOTSTRAP_PROMPT, classify_persisted_process, index_projects,
        process_start_time, rearm_every_at, recoverable_process_matches,
    };
    use crate::config::expand_agent_cmd;
    use crate::store::{RecoverableRun, Store};

    fn recoverable_current_process(start_time: Option<i64>) -> RecoverableRun {
        RecoverableRun {
            id: "R1".into(),
            ticket_id: "T1".into(),
            target: "fake".into(),
            state: "running".into(),
            branch: None,
            worktree_path: None,
            pid: Some(i64::from(std::process::id())),
            pid_start_time: start_time,
            process_group_id: None,
            worker_token: None,
            worker_socket_path: None,
            exit_code: None,
            lease_expires_at_ms: 1,
        }
    }

    #[test]
    fn recovery_requires_both_pid_and_start_time_to_match() {
        let Some(start_time) = process_start_time(std::process::id()) else {
            return;
        };
        assert!(recoverable_process_matches(&recoverable_current_process(
            Some(start_time)
        )));
        assert!(!recoverable_process_matches(&recoverable_current_process(
            Some(start_time + 1)
        )));
        assert!(!recoverable_process_matches(&recoverable_current_process(
            None
        )));
    }

    #[test]
    fn persisted_process_identity_requires_the_recorded_leader_to_signal() {
        assert_eq!(
            classify_persisted_process(10, Some(10), true),
            PersistedProcessState::OriginalLeader
        );
        assert_eq!(
            classify_persisted_process(10, Some(11), true),
            PersistedProcessState::ReusedLeader
        );
        assert_eq!(
            classify_persisted_process(10, None, false),
            PersistedProcessState::LeaderMissing
        );
        assert_eq!(
            classify_persisted_process(10, None, true),
            PersistedProcessState::UnverifiableLeader
        );
    }

    #[test]
    fn recurring_rearm_preserves_cadence_and_skips_missed_slots() {
        assert_eq!(rearm_every_at(1_000, 500, 1_000), Some(1_500));
        assert_eq!(rearm_every_at(1_000, 500, 2_200), Some(2_500));
        assert_eq!(rearm_every_at(1_000, 0, 1_000), None);
        assert_eq!(rearm_every_at(2_000, 500, 1_000), None);
    }

    #[test]
    fn agent_command_expands_ticket_model_and_effort() {
        let template = vec![
            "agent".to_owned(),
            "--model={model}".to_owned(),
            "--effort".to_owned(),
            "{effort}".to_owned(),
            "prompt={prompt}".to_owned(),
        ];

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
        let template = vec!["agent".to_owned(), "{model}".to_owned()];

        assert_eq!(
            expand_agent_cmd(&template, None, Some("medium"), "assignment"),
            Err("does not specify `model`".to_owned())
        );
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

    #[test]
    fn orphan_disposition_stamps_waits_then_deletes_unreferenced_rows() {
        use super::OrphanDisposition::{Delete, Keep, MarkMissing};
        use super::orphan_disposition;

        assert_eq!(orphan_disposition(None, false, 1_000, 100), MarkMissing);
        assert_eq!(orphan_disposition(Some(950), false, 1_000, 100), Keep);
        assert_eq!(orphan_disposition(Some(900), false, 1_000, 100), Delete);
        assert_eq!(orphan_disposition(Some(900), true, 1_000, 100), Keep);
    }

    #[test]
    fn reconcile_stamps_deletes_and_restores_tickets() {
        use crate::store::TicketState;

        let root = tempdir().unwrap();
        let tickets = root.path().join(".agents/sloop/tickets");
        fs::create_dir_all(&tickets).unwrap();
        fs::write(tickets.join("present.md"), "# Present\n").unwrap();
        let store = Store::open(&root.path().join("sloop.db"), 1_000).unwrap();
        store
            .insert_local_project(
                "default",
                ".agents/sloop/projects/default.md",
                "Default",
                1_000,
            )
            .unwrap();
        let insert = |id: &str, file: &str, blocked_by: &[String]| {
            store
                .insert_local_ticket(
                    id,
                    "default",
                    &format!(".agents/sloop/tickets/{file}"),
                    id,
                    blocked_by,
                    &format!("sloop/{id}"),
                    None,
                    None,
                    None,
                    "default",
                    TicketState::Ready,
                    1_000,
                )
                .unwrap();
        };
        insert("T1", "present.md", &[]);
        insert("T2", "gone.md", &[]);
        insert("T3", "blocked-gone.md", &[]);
        insert("T4", "dependent.md", &["T3".into()]);
        fs::write(tickets.join("dependent.md"), "# Dependent\n").unwrap();

        let window = 100;
        let stamps = |store: &Store| -> Vec<(String, Option<i64>)> {
            store
                .local_ticket_files()
                .unwrap()
                .into_iter()
                .map(|ticket| (ticket.id, ticket.missing_at_ms))
                .collect()
        };

        // First pass stamps the two tickets whose files are gone.
        super::reconcile_tickets(root.path(), &store, 2_000, window).unwrap();
        assert_eq!(
            stamps(&store),
            vec![
                ("T1".into(), None),
                ("T2".into(), Some(2_000)),
                ("T3".into(), Some(2_000)),
                ("T4".into(), None),
            ]
        );

        // Within the window nothing is deleted and stamps keep their origin.
        super::reconcile_tickets(root.path(), &store, 2_050, window).unwrap();
        assert_eq!(stamps(&store)[1], ("T2".into(), Some(2_000)));

        // Past the window the unreferenced orphan is deleted; T3 survives
        // because T4 still names it as a blocker.
        super::reconcile_tickets(root.path(), &store, 2_100, window).unwrap();
        assert_eq!(
            stamps(&store),
            vec![
                ("T1".into(), None),
                ("T3".into(), Some(2_000)),
                ("T4".into(), None),
            ]
        );

        // The file coming back clears the stamp even after the window.
        fs::write(tickets.join("blocked-gone.md"), "# Returned\n").unwrap();
        super::reconcile_tickets(root.path(), &store, 3_000, window).unwrap();
        assert_eq!(stamps(&store)[1], ("T3".into(), None));
    }

    #[test]
    fn project_allocation_uses_sorted_paths_after_explicit_high_water_marks() {
        let root = tempdir().unwrap();
        let projects = root.path().join(".agents/sloop/projects");
        fs::create_dir_all(&projects).unwrap();
        fs::write(projects.join("zeta.md"), "# Zeta\n").unwrap();
        fs::write(projects.join("alpha.md"), "# Alpha\n").unwrap();
        fs::write(
            projects.join("middle.md"),
            "---\nid: PROJ-7\ntitle: Explicit\n---\n",
        )
        .unwrap();
        let store = Store::open(&root.path().join("sloop.db"), 1_000).unwrap();

        index_projects(
            root.path(),
            std::path::Path::new(".agents/sloop/projects"),
            &store,
            1_000,
            "PROJ",
        )
        .unwrap();

        assert!(
            fs::read_to_string(projects.join("alpha.md"))
                .unwrap()
                .contains("id: PROJ-8")
        );
        assert!(
            fs::read_to_string(projects.join("zeta.md"))
                .unwrap()
                .contains("id: PROJ-9")
        );
    }
}
