use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream as StdUnixStream;
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

use crate::clock::{Clock, FileClock, SystemClock};
use crate::config::{Config, ConfigError, Repository};
use crate::frontmatter::FrontmatterError;
use crate::ids::IdError;
use crate::logging::{LogLevel, OperationalLog};
use crate::protocol::{
    Capability, ErrorBody, ErrorCode, Request, RequestEnvelope, RequestId, ResponseEnvelope,
};
use crate::store::{Store, StoreError};
use crate::vendor_error::{CatalogError, VendorErrorClassifier};

use super::dispatcher::{
    DispatcherMessage, DispatcherState, RequestOrigin, internal, protocol_error, run_dispatcher,
    unauthorized,
};
use super::recovery::{process_identity_matches, recover_inflight_runs};
use super::runner::process_start_time;
use super::scheduler::{index_projects, reconcile_tickets};

const MAX_ENVELOPE_BYTES: u64 = 1024 * 1024;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(5);
const DISPATCH_CHANNEL_CAPACITY: usize = 64;

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

pub struct ClientResponse {
    pub response: ResponseEnvelope,
    pub started: bool,
}

pub fn request(request: Request) -> Result<ClientResponse, DaemonError> {
    let cwd = std::env::current_dir().map_err(DaemonError::CurrentDirectory)?;
    let repository = Repository::discover(&cwd)?;
    Config::load(&repository)?;

    if let Ok(response) = send_existing(&repository, request.clone()) {
        return Ok(ClientResponse {
            response,
            started: false,
        });
    }

    spawn_daemon(&repository)?;
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        match send_existing(&repository, request.clone()) {
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
    let repository = Repository::discover(&cwd)?;
    Config::load(&repository)?;
    match send_existing(&repository, request) {
        Ok(response) => Ok(Some(response)),
        Err(DaemonError::Connect(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

pub fn serve_current_repository() -> Result<(), DaemonError> {
    let cwd = std::env::current_dir().map_err(DaemonError::CurrentDirectory)?;
    let repository = Repository::discover(&cwd)?;
    let config = Config::load(&repository)?;
    let classifier = Arc::new(VendorErrorClassifier::built_in().map_err(DaemonError::Catalog)?);
    fs::create_dir_all(&repository.state_dir).map_err(|source| DaemonError::Io {
        path: repository.state_dir.clone(),
        source,
    })?;
    fs::set_permissions(&repository.state_dir, fs::Permissions::from_mode(0o700)).map_err(
        |source| DaemonError::Io {
            path: repository.state_dir.clone(),
            source,
        },
    )?;
    let runtime_root = repository
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
    fs::create_dir(&repository.runtime_dir)
        .or_else(|source| {
            if source.kind() == io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(source)
            }
        })
        .map_err(|source| DaemonError::Io {
            path: repository.runtime_dir.clone(),
            source,
        })?;
    fs::set_permissions(&repository.runtime_dir, fs::Permissions::from_mode(0o700)).map_err(
        |source| DaemonError::Io {
            path: repository.runtime_dir.clone(),
            source,
        },
    )?;

    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&repository.lock_path)
        .map_err(|source| DaemonError::Io {
            path: repository.lock_path.clone(),
            source,
        })?;
    lock.try_lock_exclusive().map_err(|source| {
        if source.kind() == io::ErrorKind::WouldBlock {
            DaemonError::AlreadyRunning
        } else {
            DaemonError::Io {
                path: repository.lock_path.clone(),
                source,
            }
        }
    })?;
    // Hold the pre-v7 runtime lock as well during the lock-location
    // transition, preventing an already-running older daemon in this runtime
    // root from sharing the database with the new process.
    let legacy_lock_path = repository.runtime_dir.join("daemon.lock");
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
        "socket": repository.operator_socket,
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
    let store = Store::open(&repository.db_path, clock.now_ms()).map_err(DaemonError::Store)?;
    if let Some(agent) = &config.agent {
        store
            .backfill_ticket_targets(&agent.default_target, clock.now_ms())
            .map_err(DaemonError::Store)?;
    }
    let _ = index_projects(
        &repository.root,
        &config.project_dir,
        &store,
        clock.now_ms(),
        &config.project_prefix,
    )?;
    reconcile_tickets(
        &repository.root,
        &store,
        clock.now_ms(),
        config.delete_missing_after_ms,
    )?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(DaemonError::Runtime)?;
    runtime.block_on(serve(
        repository,
        config,
        store,
        lock,
        legacy_lock,
        clock,
        classifier,
    ))
}

async fn serve(
    repository: Repository,
    config: Config,
    store: Store,
    _lock: fs::File,
    _legacy_lock: fs::File,
    clock: Arc<dyn Clock>,
    classifier: Arc<VendorErrorClassifier>,
) -> Result<(), DaemonError> {
    if repository.operator_socket.exists() {
        fs::remove_file(&repository.operator_socket).map_err(|source| DaemonError::Io {
            path: repository.operator_socket.clone(),
            source,
        })?;
    }

    let listener =
        UnixListener::bind(&repository.operator_socket).map_err(|source| DaemonError::Io {
            path: repository.operator_socket.clone(),
            source,
        })?;
    fs::set_permissions(
        &repository.operator_socket,
        fs::Permissions::from_mode(0o600),
    )
    .map_err(|source| DaemonError::Io {
        path: repository.operator_socket.clone(),
        source,
    })?;

    let log = OperationalLog::open(&repository.daemon_log).map_err(|source| DaemonError::Io {
        path: repository.daemon_log.clone(),
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
        root: repository.root.clone(),
        project_dir: config.project_dir.clone(),
        ticket_dir: config.ticket_dir.clone(),
        worktree_dir: repository.root.join(&config.worktree_dir),
        state_dir: repository.state_dir.clone(),
        runtime_dir: repository.runtime_dir.clone(),
        socket: repository.operator_socket.clone(),
        daemon_log: repository.daemon_log.clone(),
        store,
        storage_full: Cell::new(false),
        reconciliation_blocked: false,
        active: HashSet::new(),
        supervised: HashSet::new(),
        suspected_dead: HashSet::new(),
        recovering: HashSet::new(),
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
                    path: repository.operator_socket.clone(),
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
                let _ = fs::remove_file(&repository.operator_socket);
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
                unauthorized(
                    "worker verbs are not available on the operator socket; \
                     run `sloop list` or `sloop show <ticket>` to inspect tickets from here",
                ),
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

/// Accepts connections on one run's worker socket until the settle path
/// aborts this task. Each connection is one request, mirroring the
/// operator socket.
pub(super) async fn serve_worker_socket(
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

fn spawn_daemon(repository: &Repository) -> Result<(), DaemonError> {
    let executable = std::env::current_exe().map_err(DaemonError::CurrentExecutable)?;
    Command::new(executable)
        .args(["daemon", "--foreground"])
        .current_dir(&repository.root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map(|_| ())
        .map_err(DaemonError::Spawn)
}

fn send_existing(
    repository: &Repository,
    request: Request,
) -> Result<ResponseEnvelope, DaemonError> {
    match send(&repository.operator_socket, request.clone()) {
        Ok(response) => Ok(response),
        Err(current_error) => {
            let Some(identity) = read_lock_identity(&repository.lock_path) else {
                return Err(current_error);
            };
            if !process_identity_matches(identity.pid, identity.started_at_ms) {
                return Err(current_error);
            }
            let Some(socket) = identity.socket else {
                return Err(current_error);
            };
            if socket == repository.operator_socket {
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

/// Identity of the daemon owning a repository's lockfile: PID plus process start
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
