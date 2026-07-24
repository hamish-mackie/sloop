use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use rusqlite::{Transaction, TransactionBehavior};
use serde_json::{Value, json};

use crate::config::{AgentConfig, expand_agent_cmd};
use crate::db::StoreError;
use crate::domain::ticket::TicketState;
use crate::domain::work::{ExecutionHints, SourceVersion, TicketRef, WorkTicket, WorkTicketState};
use crate::flow::Flow;
use crate::frontmatter::{self, FrontmatterError};
use crate::ids::{IdError, next_id};
use crate::protocol::{PostActivation, PostArgs};
use crate::run_store;
use crate::work_state::local::{
    self, ActivationKind, LocalSqlite, LocalTicketWrite, NewActivation,
};
use crate::work_state::{SourceError, WorkStateAuthor};

#[derive(Clone, Copy)]
struct ActivationRequest {
    kind: ActivationKind,
    eligible_at_ms: Option<i64>,
}

static POST_TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

struct StagedWrite {
    path: PathBuf,
    target: PathBuf,
    persisted: bool,
}

impl StagedWrite {
    fn new(target: PathBuf, content: &str) -> io::Result<Self> {
        let parent = target.parent().unwrap_or_else(|| Path::new("."));
        let name = target
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("ticket");
        let (path, mut file) = loop {
            let ordinal = POST_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = parent.join(format!(
                ".{name}.sloop-post-{}-{ordinal}.tmp",
                std::process::id()
            ));
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(file) => break (path, file),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        };
        let staged = Self {
            path,
            target,
            persisted: false,
        };
        if let Ok(metadata) = fs::metadata(&staged.target) {
            fs::set_permissions(&staged.path, metadata.permissions())?;
        }
        file.write_all(content.as_bytes())?;
        file.sync_all()?;
        Ok(staged)
    }

    fn persist(mut self) -> io::Result<()> {
        fs::rename(&self.path, &self.target)?;
        self.persisted = true;
        Ok(())
    }
}

impl Drop for StagedWrite {
    fn drop(&mut self) {
        if !self.persisted {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Local markdown authoring over SQLite.
///
/// A [`SourceVersion`] is the lowercase hexadecimal FNV-1a hash of the
/// complete markdown file content. Updates compare that version with the file
/// immediately before committing, so a concurrent edit is rejected instead
/// of silently overwritten.
struct MarkdownWorkStateAuthor<'a> {
    root: &'a Path,
    file_path: &'a str,
    worktree: &'a str,
    work_state: &'a LocalSqlite,
    original_content: &'a str,
    final_content: &'a str,
    original_version: SourceVersion,
    activation: Option<ActivationRequest>,
    now_ms: i64,
    activation_result: Mutex<Value>,
}

impl MarkdownWorkStateAuthor<'_> {
    fn absolute_path(&self) -> PathBuf {
        self.root.join(self.file_path)
    }

    fn ensure_source_version(&self, expected: &SourceVersion) -> Result<(), SourceError> {
        let path = self.absolute_path();
        let content = fs::read_to_string(&path).map_err(|error| SourceError::Corrupt {
            message: format!("cannot read {}: {error}", path.display()),
        })?;
        let actual = source_version(&content);
        if &actual != expected {
            return Err(SourceError::Rejected {
                message: format!(
                    "source version conflict for `{}`: expected {}, found {}",
                    self.file_path, expected.0, actual.0
                ),
            });
        }
        Ok(())
    }

    fn commit(
        &self,
        ticket: &WorkTicket,
        update: bool,
        expected: &SourceVersion,
    ) -> Result<(), SourceError> {
        let staged = (self.final_content != self.original_content)
            .then(|| StagedWrite::new(self.absolute_path(), self.final_content))
            .transpose()
            .map_err(|error| SourceError::Corrupt {
                message: format!("cannot stage {}: {error}", self.file_path),
            })?;
        self.ensure_source_version(expected)?;
        let db = self.work_state.db();
        let mut connection = db.lock();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(StoreError::from)
            .map_err(source_store_error)?;
        let write = LocalTicketWrite {
            id: &ticket.id,
            project_id: &ticket.project_id,
            file_path: self.file_path,
            name: &ticket.name,
            blocked_by: &ticket.blocked_by,
            worktree: self.worktree,
            target: ticket.hints.target.as_deref(),
            model: ticket.hints.model.as_deref(),
            effort: ticket.hints.effort.as_deref(),
            flow: ticket.hints.flow.as_deref().unwrap_or_default(),
            state: ticket.state.to_ticket_state(),
            body: &ticket.body,
            content_hash: &ticket.version.0,
            now_ms: self.now_ms,
        };
        if update {
            local::tx::update_authored_ticket(&transaction, &write).map_err(source_store_error)?;
        } else {
            local::tx::insert_authored_ticket(&transaction, &write).map_err(source_store_error)?;
        }
        let activation =
            queue_activation_transaction(&transaction, &ticket.id, self.activation, self.now_ms)
                .map_err(source_store_error)?;
        self.ensure_source_version(expected)?;
        transaction
            .commit()
            .map_err(StoreError::from)
            .map_err(source_store_error)?;
        drop(connection);

        if let Some(staged) = staged {
            staged.persist().map_err(|error| SourceError::Corrupt {
                message: format!("cannot replace {}: {error}", self.file_path),
            })?;
        }
        *self
            .activation_result
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = activation;
        Ok(())
    }

    fn activation_result(&self) -> Value {
        self.activation_result
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

#[async_trait]
impl WorkStateAuthor for MarkdownWorkStateAuthor<'_> {
    async fn post(&self, ticket: &WorkTicket) -> Result<TicketRef, SourceError> {
        self.commit(ticket, false, &self.original_version)?;
        Ok(TicketRef {
            id: ticket.id.clone(),
            source: "local".into(),
            source_ref: Some(self.file_path.into()),
        })
    }

    async fn update(
        &self,
        ticket: &TicketRef,
        content: &WorkTicket,
        expected: &SourceVersion,
    ) -> Result<SourceVersion, SourceError> {
        if ticket.id != content.id || ticket.source_ref.as_deref() != Some(self.file_path) {
            return Err(SourceError::Rejected {
                message: format!("ticket reference conflict for `{}`", content.id),
            });
        }
        self.commit(content, true, expected)?;
        Ok(content.version.clone())
    }
}

/// Registers a ticket file: validates and stamps frontmatter, indexes the
/// ticket, and for `auto` and `at` creates one queued activation. Reposting
/// a stamped file is idempotent; reposting with a different `--at` time
/// reschedules the queued activation. The dispatcher is the only caller and
/// computes `at_eligible_ms` from its injected clock, so plain reads before
/// writes here cannot race another writer.
#[allow(clippy::too_many_arguments)]
pub async fn handle(
    root: &Path,
    ticket_dir: &Path,
    work_state: &LocalSqlite,
    args: &PostArgs,
    now_ms: i64,
    at_eligible_ms: Option<i64>,
    ticket_prefix: &str,
    agent: Option<&AgentConfig>,
    flows: &BTreeMap<String, Flow>,
    default_flow: &str,
) -> Result<Value, PostError> {
    let initial_state = match args.activation {
        PostActivation::Hold => TicketState::Held,
        _ => TicketState::Ready,
    };
    let relative = repository_relative(root, ticket_dir, &args.file)?;
    let relative_str = relative.to_string_lossy().into_owned();
    let absolute = root.join(&relative);
    let content = fs::read_to_string(&absolute).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            PostError::TicketFileNotFound(relative_str.clone())
        } else {
            PostError::Io {
                path: relative_str.clone(),
                source,
            }
        }
    })?;
    let stamped = parse_ticket_frontmatter(&content, &relative_str)?;

    let project = match (stamped.project.as_deref(), args.project.as_deref()) {
        (Some(stamped), Some(requested)) if stamped != requested => {
            return Err(PostError::ProjectConflict {
                path: relative_str,
                stamped: stamped.into(),
                requested: requested.into(),
            });
        }
        (Some(stamped), _) => stamped.to_owned(),
        (None, Some(requested)) => requested.to_owned(),
        (None, None) => "default".to_owned(),
    };
    if !work_state.project_exists(&project)? {
        return Err(PostError::UnknownProject(project));
    }

    let flow_name = match (stamped.flow.as_deref(), args.flow.as_deref()) {
        (Some(stamped), Some(requested)) if stamped != requested => {
            return Err(PostError::FlowConflict {
                path: relative_str,
                stamped: stamped.into(),
                requested: requested.into(),
            });
        }
        (Some(stamped), _) => stamped.to_owned(),
        (None, Some(requested)) => requested.to_owned(),
        (None, None) => default_flow.to_owned(),
    };
    if !flows.contains_key(&flow_name) {
        let mut known: Vec<&str> = flows.keys().map(String::as_str).collect();
        known.sort_unstable();
        return Err(PostError::UnknownFlow {
            flow: flow_name,
            known: known.into_iter().map(str::to_owned).collect(),
        });
    }

    let target = match stamped.target.as_deref() {
        Some(target) if agent.is_some_and(|agent| agent.targets.contains_key(target)) => {
            Some(target.to_owned())
        }
        Some(target) => return Err(PostError::UnknownTarget(target.to_owned())),
        None => agent.map(|agent| agent.default_target.clone()),
    };
    if let (Some(agent), Some(target)) = (agent, target.as_deref()) {
        let command = agent
            .targets
            .get(target)
            .expect("configured default target was validated");
        expand_agent_cmd(
            command,
            stamped.model.as_deref(),
            stamped.effort.as_deref(),
            "",
        )
        .map_err(|message| PostError::MissingTargetValue {
            target: target.to_owned(),
            message,
        })?;
    }

    let (ticket_id, existing) = match stamped.id.as_deref() {
        Some(id) => {
            if let Some(existing) = work_state.ticket(id)? {
                if existing.file_path.as_deref() != Some(relative_str.as_str()) {
                    return Err(PostError::TicketIdTaken {
                        id: id.to_owned(),
                        file: existing.file_path.unwrap_or_default(),
                    });
                }
                if existing.project_id != project {
                    return Err(PostError::ProjectConflict {
                        path: relative_str,
                        stamped: project,
                        requested: existing.project_id,
                    });
                }
                (id.to_owned(), Some(existing))
            } else {
                (id.to_owned(), None)
            }
        }
        None => match work_state.ticket_by_file(&relative_str)? {
            Some(existing) => {
                if existing.project_id != project {
                    return Err(PostError::ProjectConflict {
                        path: relative_str,
                        stamped: project,
                        requested: existing.project_id,
                    });
                }
                (existing.id.clone(), Some(existing))
            }
            None => (allocate_ticket_id(work_state, ticket_prefix)?, None),
        },
    };
    for blocker in &stamped.blocked_by {
        if blocker != &ticket_id && work_state.ticket(blocker)?.is_none() {
            return Err(PostError::UnknownBlockedBy {
                ticket: ticket_id.clone(),
                blocker: blocker.clone(),
            });
        }
    }
    let mut dependencies = work_state.ticket_dependencies()?;
    dependencies.insert(ticket_id.clone(), stamped.blocked_by.clone());
    if let Some(chain) = crate::domain::graph::find_cycle(&dependencies) {
        return Err(PostError::DependencyCycle(chain));
    }

    let worktree = match stamped.worktree.clone() {
        Some(worktree) => worktree,
        None => {
            let stem = Path::new(&relative_str)
                .file_stem()
                .and_then(|stem| stem.to_str());
            crate::ids::default_worktree(stem, &ticket_id).map_err(|reason| {
                PostError::InvalidWorktreeStem {
                    path: relative_str.clone(),
                    reason,
                }
            })?
        }
    };
    let final_content = frontmatter::stamp(&content, &ticket_id, &project, &worktree, &flow_name)
        .map_err(|error| PostError::InvalidTicket {
            path: relative_str.clone(),
            error,
        })?
        .unwrap_or_else(|| content.clone());
    let activation_request = match &args.activation {
        PostActivation::Manual | PostActivation::Hold => None,
        PostActivation::Auto => Some(ActivationRequest {
            kind: ActivationKind::Auto,
            eligible_at_ms: None,
        }),
        PostActivation::At { .. } => Some(ActivationRequest {
            kind: ActivationKind::At,
            eligible_at_ms: Some(
                at_eligible_ms.expect("the dispatcher computes eligibility for at activations"),
            ),
        }),
    };
    let work_ticket = WorkTicket {
        id: ticket_id.clone(),
        project_id: project.clone(),
        name: stamped.name.clone(),
        body: frontmatter::body(&content)
            .expect("validated frontmatter has a body")
            .to_owned(),
        state: WorkTicketState::from_ticket_state(
            initial_state,
            false,
            String::new(),
            crate::domain::work::OwnerId(String::new()),
        ),
        blocked_by: stamped.blocked_by.clone(),
        attempts: existing.as_ref().map_or(0, |ticket| ticket.attempts as u32),
        hints: ExecutionHints {
            worktree: Some(worktree.clone()),
            activation_id: None,
            target,
            model: stamped.model.clone(),
            effort: stamped.effort.clone(),
            flow: Some(flow_name.clone()),
        },
        version: source_version(&final_content),
    };
    let author = MarkdownWorkStateAuthor {
        root,
        file_path: &relative_str,
        worktree: &worktree,
        work_state,
        original_content: &content,
        final_content: &final_content,
        original_version: source_version(&content),
        activation: activation_request,
        now_ms,
        activation_result: Mutex::new(Value::Null),
    };
    let ticket_ref = TicketRef {
        id: ticket_id,
        source: "local".into(),
        source_ref: Some(relative_str.clone()),
    };
    if existing.is_some() {
        author
            .update(&ticket_ref, &work_ticket, &author.original_version)
            .await?;
    } else {
        author.post(&work_ticket).await?;
    }
    let activation = author.activation_result();
    let ticket = work_state
        .ticket(&work_ticket.id)?
        .expect("registered ticket still exists");

    Ok(json!({
        "ticket": {
            "id": ticket.id,
            "project": project,
            "file": relative_str,
            "state": ticket.state,
            "name": ticket.name,
            "blocked_by": ticket.blocked_by,
            "worktree": ticket.worktree,
            "target": ticket.target,
            "model": ticket.model,
            "effort": ticket.effort,
            "flow": ticket.flow,
        },
        "activation": activation,
    }))
}

/// Validates a ticket file, reporting *every* independent problem at once so
/// authoring a ticket does not turn into one round-trip per mistake.
///
/// The split between short-circuiting and accumulating is deliberate. A file
/// whose frontmatter cannot be read at all — no block, unterminated, YAML
/// that does not parse, a block that is not a mapping — fails fast: no field
/// can be read out of it, so every other check would either be unanswerable
/// or degenerate into "everything is missing". Once a mapping is in hand,
/// each field and the body are independent, and the caller deserves the full
/// list. Checks that need the store (unknown blockers, dependency cycles,
/// project/flow/target resolution) stay in `handle`: they are registration
/// problems rather than problems with the file, and they carry their own
/// error codes.
pub(crate) fn parse_ticket_frontmatter(
    content: &str,
    path: &str,
) -> Result<frontmatter::Frontmatter, PostError> {
    let (stamped, field_errors) =
        frontmatter::parse_collecting(content).map_err(|error| PostError::InvalidTicket {
            path: path.to_owned(),
            error,
        })?;

    // A field that failed to parse is already reported by its own problem;
    // adding "missing" on top of "wrong type" would only muddy the list.
    let name_is_reported = field_errors
        .iter()
        .any(|error| matches!(error, FrontmatterError::InvalidFieldType { key } if key == "name"));
    let blocked_by_is_reported = field_errors
        .iter()
        .any(|error| matches!(error, FrontmatterError::InvalidBlockedBy));

    let mut problems = Vec::new();
    if !name_is_reported && stamped.name.trim().is_empty() {
        problems.push(TicketProblem::MissingName);
    }
    if !blocked_by_is_reported && !stamped.has_blocked_by() {
        problems.push(TicketProblem::MissingBlockedBy);
    }
    if frontmatter::body(content)
        .expect("frontmatter was already parsed")
        .trim()
        .is_empty()
    {
        problems.push(TicketProblem::EmptyBody);
    }
    problems.extend(field_errors.into_iter().map(TicketProblem::from));

    if problems.is_empty() {
        Ok(stamped)
    } else {
        Err(PostError::InvalidTicketFields {
            path: path.to_owned(),
            problems,
        })
    }
}

/// Reuses an existing queued activation of the same kind so reposting cannot
/// enqueue duplicate work. Ticket registration, counter reservation, and this
/// queue operation share one transaction.
fn queue_activation_transaction(
    transaction: &Transaction<'_>,
    ticket_id: &str,
    request: Option<ActivationRequest>,
    now_ms: i64,
) -> Result<Value, StoreError> {
    let Some(request) = request else {
        return Ok(Value::Null);
    };
    let id = match local::tx::queued_ticket_activation(transaction, ticket_id, request.kind)? {
        Some(id) => {
            if let Some(eligible_at_ms) = request.eligible_at_ms {
                local::tx::reschedule_activation(transaction, &id, eligible_at_ms, now_ms)?;
            }
            id
        }
        None => {
            let ordinal = run_store::tx::reserve_ordinal(transaction, "activation", "activations")?;
            let id = format!("A{ordinal}");
            local::tx::insert_activation(
                transaction,
                &NewActivation {
                    id: &id,
                    kind: request.kind,
                    ticket_id: Some(ticket_id),
                    project_id: None,
                    eligible_at_ms: request.eligible_at_ms,
                    interval_ms: None,
                },
                now_ms,
            )?;
            id
        }
    };
    let mut activation = json!({
        "id": id,
        "kind": request.kind.as_str(),
        "state": "queued",
        "ticket": ticket_id,
    });
    if let Some(eligible_at_ms) = request.eligible_at_ms {
        activation["eligible_at_ms"] = json!(eligible_at_ms);
    }
    Ok(activation)
}

fn source_store_error(error: StoreError) -> SourceError {
    if error.is_disk_full() {
        SourceError::Unavailable { retry_after: None }
    } else {
        SourceError::Corrupt {
            message: error.to_string(),
        }
    }
}

fn source_version(content: &str) -> SourceVersion {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in content.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    SourceVersion(format!("{hash:016x}"))
}

fn allocate_ticket_id(work_state: &LocalSqlite, prefix: &str) -> Result<String, PostError> {
    let ids = work_state.ticket_ids()?;
    next_id(prefix, ids.iter().map(String::as_str)).map_err(PostError::IdAllocation)
}

/// Resolves the request path against the repository root and requires the
/// result to stay inside the committed Sloop ticket directory.
fn repository_relative(root: &Path, ticket_dir: &Path, file: &str) -> Result<PathBuf, PostError> {
    let path = Path::new(file);
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };

    let mut normalized = PathBuf::new();
    for component in joined.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    return Err(PostError::OutsideRepository(file.to_owned()));
                }
            }
            component => normalized.push(component),
        }
    }
    let relative = normalized
        .strip_prefix(root)
        .map(Path::to_path_buf)
        .map_err(|_| PostError::OutsideRepository(file.to_owned()))?;
    if !relative.starts_with(ticket_dir) {
        return Err(PostError::OutsideTicketDirectory {
            path: file.to_owned(),
            directory: ticket_dir.to_path_buf(),
        });
    }
    Ok(relative)
}

/// A single problem with a ticket file, phrased without the file path so
/// several can be listed under one path heading.
#[derive(Debug)]
pub enum TicketProblem {
    Frontmatter(FrontmatterError),
    MissingName,
    MissingBlockedBy,
    InvalidBlockedBy,
    EmptyBody,
}

impl From<FrontmatterError> for TicketProblem {
    fn from(error: FrontmatterError) -> Self {
        match error {
            FrontmatterError::InvalidBlockedBy => Self::InvalidBlockedBy,
            error => Self::Frontmatter(error),
        }
    }
}

impl fmt::Display for TicketProblem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Frontmatter(error) => error.fmt(formatter),
            Self::MissingName => {
                formatter.write_str("missing or empty `name`; add `name: Your ticket title`")
            }
            Self::MissingBlockedBy => formatter.write_str(
                "missing `blocked_by`; add `blocked_by: []` if there are no dependencies",
            ),
            Self::InvalidBlockedBy => formatter.write_str(
                "invalid `blocked_by`; use `blocked_by: []` or a YAML list of ticket IDs",
            ),
            Self::EmptyBody => {
                formatter.write_str("empty `body`; add a ticket description after the frontmatter")
            }
        }
    }
}

#[derive(Debug)]
pub enum PostError {
    TicketFileNotFound(String),
    OutsideRepository(String),
    OutsideTicketDirectory {
        path: String,
        directory: PathBuf,
    },
    InvalidTicket {
        path: String,
        error: FrontmatterError,
    },
    /// One or more independent problems with the ticket file itself,
    /// reported together. Never empty.
    InvalidTicketFields {
        path: String,
        problems: Vec<TicketProblem>,
    },
    InvalidWorktreeStem {
        path: String,
        reason: String,
    },
    UnknownBlockedBy {
        ticket: String,
        blocker: String,
    },
    DependencyCycle(Vec<String>),
    UnknownProject(String),
    UnknownTarget(String),
    MissingTargetValue {
        target: String,
        message: String,
    },
    ProjectConflict {
        path: String,
        stamped: String,
        requested: String,
    },
    FlowConflict {
        path: String,
        stamped: String,
        requested: String,
    },
    UnknownFlow {
        flow: String,
        known: Vec<String>,
    },
    TicketIdTaken {
        id: String,
        file: String,
    },
    Io {
        path: String,
        source: io::Error,
    },
    Source(SourceError),
    Store(StoreError),
    IdAllocation(IdError),
}

impl From<SourceError> for PostError {
    fn from(error: SourceError) -> Self {
        Self::Source(error)
    }
}

impl From<StoreError> for PostError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl fmt::Display for PostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TicketFileNotFound(path) => write!(formatter, "ticket file `{path}` not found"),
            Self::OutsideRepository(path) => {
                write!(formatter, "`{path}` is outside the repository")
            }
            Self::OutsideTicketDirectory { path, directory } => write!(
                formatter,
                "`{path}` is outside the {} directory",
                directory.display()
            ),
            Self::InvalidTicket { path, error } => write!(formatter, "{path}: {error}"),
            // A lone problem keeps the original one-line `path: problem`
            // shape; only a genuine list needs the heading and bullets.
            Self::InvalidTicketFields { path, problems } => match problems.as_slice() {
                [problem] => write!(formatter, "{path}: {problem}"),
                problems => {
                    write!(formatter, "{path}:")?;
                    for problem in problems {
                        write!(formatter, "\n  - {problem}")?;
                    }
                    Ok(())
                }
            },
            Self::InvalidWorktreeStem { path, reason } => {
                write!(formatter, "{path}: {reason}")
            }
            Self::UnknownBlockedBy { ticket, blocker } => write!(
                formatter,
                "ticket `{ticket}` field `blocked_by` references unknown ticket `{blocker}`"
            ),
            Self::DependencyCycle(chain) => write!(
                formatter,
                "field `blocked_by` creates a dependency cycle: {}",
                chain.join(" -> ")
            ),
            Self::UnknownProject(project) => {
                write!(formatter, "project `{project}` is not indexed")
            }
            Self::UnknownTarget(target) => {
                write!(formatter, "agent target `{target}` is not configured")
            }
            Self::MissingTargetValue { target, message } => {
                write!(formatter, "ticket using agent target `{target}` {message}")
            }
            Self::ProjectConflict {
                path,
                stamped,
                requested,
            } => write!(
                formatter,
                "{path}: ticket belongs to project `{stamped}`, not `{requested}`"
            ),
            Self::FlowConflict {
                path,
                stamped,
                requested,
            } => write!(
                formatter,
                "{path}: ticket is bound to flow `{stamped}`, not `{requested}`"
            ),
            Self::UnknownFlow { flow, known } => write!(
                formatter,
                "flow `{flow}` is not defined; known flows: {}",
                known.join(", ")
            ),
            Self::TicketIdTaken { id, file } => write!(
                formatter,
                "ticket ID `{id}` is already registered by `{file}`"
            ),
            Self::Io { path, source } => write!(formatter, "{path}: {source}"),
            Self::Source(error) => error.fmt(formatter),
            Self::Store(error) => error.fmt(formatter),
            Self::IdAllocation(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for PostError {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tempfile::tempdir;

    use super::{
        MarkdownWorkStateAuthor, PostError, handle as handle_with_directory, source_version,
    };
    use crate::config::{AgentConfig, AgentTarget};
    use crate::db::Db;
    use crate::domain::work::{ExecutionHints, TicketRef, WorkTicket, WorkTicketState};
    use crate::flow::{Flow, Stage, StageKind, VerdictPolicy};
    use crate::protocol::{PostActivation, PostArgs};
    use crate::work_state::local::LocalSqlite;
    use crate::work_state::{SourceError, WorkStateAuthor};

    fn world() -> (tempfile::TempDir, LocalSqlite) {
        let root = tempdir().unwrap();
        std::fs::create_dir_all(root.path().join(".agents/sloop/tickets")).unwrap();
        let store = LocalSqlite::from_db(Db::open(&root.path().join("sloop.db"), 1_000).unwrap());
        store
            .upsert_local_project(
                "default",
                ".agents/sloop/projects/default.md",
                "Default",
                1_000,
            )
            .unwrap();
        (root, store)
    }

    #[allow(clippy::too_many_arguments)]
    fn handle(
        root: &std::path::Path,
        store: &LocalSqlite,
        args: &PostArgs,
        now_ms: i64,
        ticket_prefix: &str,
        agent: Option<&AgentConfig>,
        flows: &BTreeMap<String, Flow>,
        default_flow: &str,
    ) -> Result<serde_json::Value, PostError> {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(handle_with_directory(
                root,
                std::path::Path::new(".agents/sloop/tickets"),
                store,
                args,
                now_ms,
                None,
                ticket_prefix,
                agent,
                flows,
                default_flow,
            ))
    }

    fn handle_at(
        root: &std::path::Path,
        store: &LocalSqlite,
        args: &PostArgs,
        now_ms: i64,
        at_eligible_ms: i64,
    ) -> Result<serde_json::Value, PostError> {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(handle_with_directory(
                root,
                std::path::Path::new(".agents/sloop/tickets"),
                store,
                args,
                now_ms,
                Some(at_eligible_ms),
                "TICK",
                None,
                &flows(),
                "default",
            ))
    }

    fn post(file: &str, activation: PostActivation) -> PostArgs {
        PostArgs {
            file: file.into(),
            project: None,
            flow: None,
            activation,
        }
    }

    fn flows() -> BTreeMap<String, Flow> {
        BTreeMap::from([
            (
                "default".to_owned(),
                Flow {
                    name: "default".into(),
                    stages: vec![Stage {
                        name: "build".into(),
                        kind: StageKind::Agent,
                        verdict: VerdictPolicy::Commits,
                        on_fail: None,
                    }],
                },
            ),
            (
                "release".to_owned(),
                Flow {
                    name: "release".into(),
                    stages: vec![Stage {
                        name: "build".into(),
                        kind: StageKind::Agent,
                        verdict: VerdictPolicy::Commits,
                        on_fail: None,
                    }],
                },
            ),
        ])
    }

    fn ticket(frontmatter: &str, body: &str) -> String {
        format!("---\nname: Test ticket\nblocked_by: []\n{frontmatter}---\n{body}")
    }

    fn agent() -> AgentConfig {
        AgentConfig {
            default_target: "claude".into(),
            targets: BTreeMap::from([
                (
                    "claude".into(),
                    AgentTarget {
                        cmd: vec!["claude".into(), "{prompt}".into()],
                        model: None,
                        effort: None,
                    },
                ),
                (
                    "codex".into(),
                    AgentTarget {
                        cmd: vec![
                            "codex".into(),
                            "{model}".into(),
                            "{effort}".into(),
                            "{prompt}".into(),
                        ],
                        model: None,
                        effort: None,
                    },
                ),
            ]),
        }
    }

    #[test]
    fn posting_twice_reuses_the_registration_and_activation() {
        let (root, store) = world();
        std::fs::write(
            root.path().join(".agents/sloop/tickets/cooldown.md"),
            ticket("", "# Cooldowns\n"),
        )
        .unwrap();
        let args = post(".agents/sloop/tickets/cooldown.md", PostActivation::Auto);

        let first = handle(
            root.path(),
            &store,
            &args,
            2_000,
            "TICK",
            None,
            &flows(),
            "default",
        )
        .unwrap();
        let second = handle(
            root.path(),
            &store,
            &args,
            3_000,
            "TICK",
            None,
            &flows(),
            "default",
        )
        .unwrap();
        assert_eq!(first["ticket"]["id"], second["ticket"]["id"]);
        assert_eq!(first["activation"]["id"], second["activation"]["id"]);
        let db = store.db();
        let connection = db.lock();
        let tickets: i64 = connection
            .query_row("SELECT COUNT(*) FROM tickets", [], |row| row.get(0))
            .unwrap();
        let activations: i64 = connection
            .query_row("SELECT COUNT(*) FROM activations", [], |row| row.get(0))
            .unwrap();
        assert_eq!(tickets, 1);
        assert_eq!(activations, 1);
    }

    #[test]
    fn stale_source_version_rejects_update_without_clobbering_the_file() {
        let (root, store) = world();
        let relative = ".agents/sloop/tickets/cas.md";
        let path = root.path().join(relative);
        std::fs::write(&path, ticket("", "# Original\n")).unwrap();
        handle(
            root.path(),
            &store,
            &post(relative, PostActivation::Manual),
            2_000,
            "TICK",
            None,
            &flows(),
            "default",
        )
        .unwrap();
        let original = std::fs::read_to_string(&path).unwrap();
        let replacement = original.replace("name: Test ticket", "name: Replacement");
        let expected = source_version(&original);
        let external_edit = original.replace("# Original", "# External edit");
        std::fs::write(&path, &external_edit).unwrap();
        let author = MarkdownWorkStateAuthor {
            root: root.path(),
            file_path: relative,
            worktree: "cas",
            work_state: &store,
            original_content: &original,
            final_content: &replacement,
            original_version: expected.clone(),
            activation: None,
            now_ms: 3_000,
            activation_result: std::sync::Mutex::new(serde_json::Value::Null),
        };
        let content = WorkTicket {
            id: "TICK-1".into(),
            project_id: "default".into(),
            name: "Replacement".into(),
            body: "# Replacement\n".into(),
            state: WorkTicketState::Ready,
            blocked_by: Vec::new(),
            attempts: 0,
            hints: ExecutionHints {
                worktree: Some("sloop/TICK-1".into()),
                activation_id: None,
                target: None,
                model: None,
                effort: None,
                flow: Some("default".into()),
            },
            version: source_version(&replacement),
        };
        let ticket_ref = TicketRef {
            id: content.id.clone(),
            source: "local".into(),
            source_ref: Some(relative.into()),
        };

        let error = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(author.update(&ticket_ref, &content, &expected))
            .unwrap_err();

        assert!(matches!(
            error,
            SourceError::Rejected { message } if message.contains("source version conflict")
        ));
        assert_eq!(std::fs::read_to_string(path).unwrap(), external_edit);
        assert_eq!(store.ticket("TICK-1").unwrap().unwrap().name, "Test ticket");
    }

    #[test]
    fn activation_insert_failure_leaves_idless_file_and_database_unchanged() {
        let (root, store) = world();
        let relative = ".agents/sloop/tickets/fail.md";
        let path = root.path().join(relative);
        let original = ticket("", "# Failure\n");
        std::fs::write(&path, &original).unwrap();
        store
            .db()
            .lock()
            .execute_batch(
                "CREATE TRIGGER reject_activation BEFORE INSERT ON activations
                 BEGIN SELECT RAISE(ABORT, 'forced activation failure'); END;",
            )
            .unwrap();

        let error = handle(
            root.path(),
            &store,
            &post(relative, PostActivation::Auto),
            2_000,
            "TICK",
            None,
            &flows(),
            "default",
        )
        .unwrap_err();

        assert!(error.to_string().contains("forced activation failure"));
        assert_eq!(std::fs::read_to_string(path).unwrap(), original);
        assert!(store.ticket_ids().unwrap().is_empty());
        assert!(store.queued_activations().unwrap().is_empty());
        let next_ordinal: i64 = store
            .db()
            .lock()
            .query_row(
                "SELECT next_ordinal FROM id_counters WHERE kind = 'activation'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(next_ordinal, 1);
    }

    #[test]
    fn posting_at_queues_a_timed_activation_and_reposting_reschedules_it() {
        let (root, store) = world();
        std::fs::write(
            root.path().join(".agents/sloop/tickets/timed.md"),
            ticket("", "# Timed\n"),
        )
        .unwrap();
        let args = post(
            ".agents/sloop/tickets/timed.md",
            PostActivation::At {
                time: "03:00".into(),
            },
        );

        let first = handle_at(root.path(), &store, &args, 2_000, 10_000).unwrap();
        assert_eq!(first["ticket"]["state"], "ready");
        assert_eq!(first["activation"]["kind"], "at");
        assert_eq!(first["activation"]["eligible_at_ms"], 10_000);

        let second = handle_at(root.path(), &store, &args, 3_000, 20_000).unwrap();
        assert_eq!(second["activation"]["id"], first["activation"]["id"]);
        assert_eq!(second["activation"]["eligible_at_ms"], 20_000);

        let queued = store.queued_activations().unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].eligible_at_ms, Some(20_000));
    }

    #[test]
    fn posting_snapshots_the_default_target_and_reposting_refreshes_execution_values() {
        let (root, store) = world();
        let path = root.path().join(".agents/sloop/tickets/work.md");
        std::fs::write(&path, ticket("model: sonnet\neffort: medium\n", "# Work\n")).unwrap();
        let args = post(".agents/sloop/tickets/work.md", PostActivation::Manual);
        let agent = agent();

        let first = handle(
            root.path(),
            &store,
            &args,
            2_000,
            "TICK",
            Some(&agent),
            &flows(),
            "default",
        )
        .unwrap();
        assert_eq!(first["ticket"]["target"], "claude");

        std::fs::write(
            &path,
            ticket(
                "id: TICK-1\nproject: default\ntarget: codex\nmodel: o3\neffort: high\n",
                "# Work\n",
            ),
        )
        .unwrap();
        let second = handle(
            root.path(),
            &store,
            &args,
            3_000,
            "TICK",
            Some(&agent),
            &flows(),
            "default",
        )
        .unwrap();
        assert_eq!(second["ticket"]["id"], first["ticket"]["id"]);
        assert_eq!(second["ticket"]["target"], "codex");
        assert_eq!(second["ticket"]["model"], "o3");
        assert_eq!(second["ticket"]["effort"], "high");
    }

    #[test]
    fn unknown_targets_are_rejected_before_registration_or_activation() {
        let (root, store) = world();
        std::fs::write(
            root.path().join(".agents/sloop/tickets/work.md"),
            ticket("target: missing\n", "# Work\n"),
        )
        .unwrap();
        let args = post(".agents/sloop/tickets/work.md", PostActivation::Auto);

        assert!(matches!(
            handle(root.path(), &store, &args, 2_000, "TICK", Some(&agent()), &flows(), "default"),
            Err(PostError::UnknownTarget(target)) if target == "missing"
        ));
        assert!(store.ticket_ids().unwrap().is_empty());
        assert!(store.queued_activations().unwrap().is_empty());
    }

    #[test]
    fn selected_target_placeholders_require_ticket_values_before_registration() {
        let (root, store) = world();
        std::fs::write(
            root.path().join(".agents/sloop/tickets/work.md"),
            ticket("target: codex\neffort: high\n", "# Work\n"),
        )
        .unwrap();
        let args = post(".agents/sloop/tickets/work.md", PostActivation::Manual);

        let error = handle(
            root.path(),
            &store,
            &args,
            2_000,
            "TICK",
            Some(&agent()),
            &flows(),
            "default",
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("agent target `codex`"), "{error}");
        assert!(error.contains("does not specify `model`"), "{error}");
        assert!(store.ticket_ids().unwrap().is_empty());
    }

    #[test]
    fn a_stamped_project_mismatching_the_request_is_a_conflict() {
        let (root, store) = world();
        std::fs::write(
            root.path().join(".agents/sloop/tickets/t.md"),
            ticket("id: T1\nproject: default\n", "# Work\n"),
        )
        .unwrap();
        let args = PostArgs {
            file: ".agents/sloop/tickets/t.md".into(),
            project: Some("other".into()),
            flow: None,
            activation: PostActivation::Manual,
        };

        assert!(matches!(
            handle(
                root.path(),
                &store,
                &args,
                2_000,
                "TICK",
                None,
                &flows(),
                "default"
            ),
            Err(PostError::ProjectConflict { .. })
        ));
    }

    #[test]
    fn an_unknown_project_is_rejected() {
        let (root, store) = world();
        std::fs::write(
            root.path().join(".agents/sloop/tickets/t.md"),
            ticket("", "# T\n"),
        )
        .unwrap();
        let args = PostArgs {
            file: ".agents/sloop/tickets/t.md".into(),
            project: Some("missing".into()),
            flow: None,
            activation: PostActivation::Manual,
        };

        assert!(matches!(
            handle(root.path(), &store, &args, 2_000, "TICK", None, &flows(), "default"),
            Err(PostError::UnknownProject(project)) if project == "missing"
        ));
    }

    #[test]
    fn a_missing_flow_is_stamped_with_the_default() {
        let (root, store) = world();
        let path = root.path().join(".agents/sloop/tickets/t.md");
        std::fs::write(&path, ticket("", "# T\n")).unwrap();
        let args = post(".agents/sloop/tickets/t.md", PostActivation::Manual);

        let response = handle(
            root.path(),
            &store,
            &args,
            2_000,
            "TICK",
            None,
            &flows(),
            "default",
        )
        .unwrap();

        assert_eq!(response["ticket"]["flow"], "default");
        assert!(
            std::fs::read_to_string(&path)
                .unwrap()
                .contains("flow: default")
        );
    }

    #[test]
    fn an_explicit_flow_is_honored() {
        let (root, store) = world();
        std::fs::write(
            root.path().join(".agents/sloop/tickets/t.md"),
            ticket("flow: release\n", "# T\n"),
        )
        .unwrap();
        let args = post(".agents/sloop/tickets/t.md", PostActivation::Manual);

        let response = handle(
            root.path(),
            &store,
            &args,
            2_000,
            "TICK",
            None,
            &flows(),
            "default",
        )
        .unwrap();

        assert_eq!(response["ticket"]["flow"], "release");
    }

    #[test]
    fn a_stamped_flow_mismatching_the_request_is_a_conflict() {
        let (root, store) = world();
        std::fs::write(
            root.path().join(".agents/sloop/tickets/t.md"),
            ticket("flow: release\n", "# T\n"),
        )
        .unwrap();
        let args = PostArgs {
            file: ".agents/sloop/tickets/t.md".into(),
            project: None,
            flow: Some("default".into()),
            activation: PostActivation::Manual,
        };

        assert!(matches!(
            handle(
                root.path(),
                &store,
                &args,
                2_000,
                "TICK",
                None,
                &flows(),
                "default"
            ),
            Err(PostError::FlowConflict { .. })
        ));
    }

    #[test]
    fn an_unknown_flow_is_rejected_and_names_known_flows() {
        let (root, store) = world();
        std::fs::write(
            root.path().join(".agents/sloop/tickets/t.md"),
            ticket("flow: bogus\n", "# T\n"),
        )
        .unwrap();
        let args = post(".agents/sloop/tickets/t.md", PostActivation::Manual);

        let error = handle(
            root.path(),
            &store,
            &args,
            2_000,
            "TICK",
            None,
            &flows(),
            "default",
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("bogus"), "{error}");
        assert!(error.contains("default"), "{error}");
        assert!(error.contains("release"), "{error}");
        assert!(store.ticket_ids().unwrap().is_empty());
    }

    #[test]
    fn reindex_recovers_the_flow_binding_from_frontmatter_into_a_fresh_store() {
        let (root, store) = world();
        std::fs::write(
            root.path().join(".agents/sloop/tickets/t.md"),
            ticket("", "# T\n"),
        )
        .unwrap();
        let args = post(".agents/sloop/tickets/t.md", PostActivation::Manual);
        handle(
            root.path(),
            &store,
            &args,
            2_000,
            "TICK",
            None,
            &flows(),
            "default",
        )
        .unwrap();
        drop(store);

        // A fresh store with no rows of its own must recover the flow binding
        // purely from the committed frontmatter that the first post stamped.
        let fresh_store =
            LocalSqlite::from_db(Db::open(&root.path().join("fresh.db"), 3_000).unwrap());
        fresh_store
            .upsert_local_project(
                "default",
                ".agents/sloop/projects/default.md",
                "Default",
                3_000,
            )
            .unwrap();
        let response = handle(
            root.path(),
            &fresh_store,
            &args,
            3_000,
            "TICK",
            None,
            &flows(),
            "default",
        )
        .unwrap();

        assert_eq!(response["ticket"]["id"], "TICK-1");
        assert_eq!(response["ticket"]["flow"], "default");
    }

    #[test]
    fn idless_tickets_get_monotonic_generated_ids() {
        let (root, store) = world();
        std::fs::create_dir(root.path().join(".agents/sloop/tickets/nested")).unwrap();
        std::fs::write(
            root.path().join(".agents/sloop/tickets/fix.md"),
            ticket("", "# A\n"),
        )
        .unwrap();
        std::fs::write(
            root.path().join(".agents/sloop/tickets/nested/fix.md"),
            ticket("", "# B\n"),
        )
        .unwrap();

        let first = handle(
            root.path(),
            &store,
            &post(".agents/sloop/tickets/fix.md", PostActivation::Manual),
            2_000,
            "TICK",
            None,
            &flows(),
            "default",
        )
        .unwrap();
        let second = handle(
            root.path(),
            &store,
            &post(
                ".agents/sloop/tickets/nested/fix.md",
                PostActivation::Manual,
            ),
            2_100,
            "TICK",
            None,
            &flows(),
            "default",
        )
        .unwrap();
        assert_eq!(first["ticket"]["id"], "TICK-1");
        assert_eq!(second["ticket"]["id"], "TICK-2");
    }

    #[test]
    fn configured_prefix_and_explicit_high_water_mark_control_allocation() {
        let (root, store) = world();
        let explicit = root.path().join(".agents/sloop/tickets/explicit.md");
        let explicit_content = ticket(
            "id: WORK-9\nproject: default\nworktree: custom/work\nflow: default\n",
            "# Explicit\n",
        );
        std::fs::write(&explicit, &explicit_content).unwrap();
        handle(
            root.path(),
            &store,
            &post(".agents/sloop/tickets/explicit.md", PostActivation::Manual),
            2_000,
            "WORK",
            None,
            &flows(),
            "default",
        )
        .unwrap();
        assert_eq!(std::fs::read_to_string(explicit).unwrap(), explicit_content);

        std::fs::write(
            root.path().join(".agents/sloop/tickets/unrelated.md"),
            ticket("id: OTHER-100\nproject: default\n", "# Unrelated\n"),
        )
        .unwrap();
        handle(
            root.path(),
            &store,
            &post(".agents/sloop/tickets/unrelated.md", PostActivation::Manual),
            2_100,
            "WORK",
            None,
            &flows(),
            "default",
        )
        .unwrap();

        std::fs::write(
            root.path().join(".agents/sloop/tickets/generated.md"),
            ticket("", "# Generated\n"),
        )
        .unwrap();
        let generated = handle(
            root.path(),
            &store,
            &post(".agents/sloop/tickets/generated.md", PostActivation::Manual),
            2_200,
            "WORK",
            None,
            &flows(),
            "default",
        )
        .unwrap();
        assert_eq!(generated["ticket"]["id"], "WORK-10");
    }

    #[test]
    fn paths_escaping_the_repository_are_rejected() {
        let (root, store) = world();
        let args = post("../outside.md", PostActivation::Manual);

        assert!(matches!(
            handle(
                root.path(),
                &store,
                &args,
                2_000,
                "TICK",
                None,
                &flows(),
                "default"
            ),
            Err(PostError::OutsideRepository(_))
        ));
    }

    #[test]
    fn paths_outside_the_ticket_directory_are_rejected() {
        let (root, store) = world();
        std::fs::write(root.path().join("elsewhere.md"), "# Elsewhere\n").unwrap();

        assert!(matches!(
            handle(
                root.path(),
                &store,
                &post("elsewhere.md", PostActivation::Manual),
                2_000,
                "TICK",
                None,
                &flows(),
                "default",
            ),
            Err(PostError::OutsideTicketDirectory { .. })
        ));
    }
}
