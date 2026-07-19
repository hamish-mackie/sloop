use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use serde_json::{Value, json};

use crate::config::{AgentConfig, expand_agent_cmd};
use crate::domain::ticket::TicketState;
use crate::flow::Flow;
use crate::frontmatter::{self, FrontmatterError};
use crate::ids::{IdError, next_id};
use crate::protocol::{PostActivation, PostArgs};
use crate::store::{ActivationKind, NewActivation, Store, StoreError};

/// Registers a ticket file: validates and stamps frontmatter, indexes the
/// ticket, and for `auto` and `at` creates one queued activation. Reposting
/// a stamped file is idempotent; reposting with a different `--at` time
/// reschedules the queued activation. The dispatcher is the only caller and
/// computes `at_eligible_ms` from its injected clock, so plain reads before
/// writes here cannot race another writer.
#[allow(clippy::too_many_arguments)]
pub fn handle(
    root: &Path,
    ticket_dir: &Path,
    store: &Store,
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
    if !store.project_exists(&project)? {
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
            if let Some(existing) = store.ticket(id)? {
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
        None => (allocate_ticket_id(store, ticket_prefix)?, None),
    };
    for blocker in &stamped.blocked_by {
        if blocker != &ticket_id && store.ticket(blocker)?.is_none() {
            return Err(PostError::UnknownBlockedBy {
                ticket: ticket_id.clone(),
                blocker: blocker.clone(),
            });
        }
    }
    let mut dependencies = store.ticket_dependencies()?;
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
    if existing.is_some() {
        store.update_local_ticket(
            &ticket_id,
            &stamped.name,
            &stamped.blocked_by,
            &worktree,
            target.as_deref(),
            stamped.model.as_deref(),
            stamped.effort.as_deref(),
            &flow_name,
            now_ms,
        )?;
    } else {
        store.insert_local_ticket(
            &ticket_id,
            &project,
            &relative_str,
            &stamped.name,
            &stamped.blocked_by,
            &worktree,
            target.as_deref(),
            stamped.model.as_deref(),
            stamped.effort.as_deref(),
            &flow_name,
            initial_state,
            now_ms,
        )?;
    }
    store.update_ticket_body(
        &ticket_id,
        frontmatter::body(&content).expect("validated frontmatter has a body"),
        now_ms,
    )?;
    let ticket = store
        .ticket(&ticket_id)?
        .expect("registered ticket still exists");

    if let Some(updated) = frontmatter::stamp(&content, &ticket.id, &project, &worktree, &flow_name)
        .map_err(|error| PostError::InvalidTicket {
            path: relative_str.clone(),
            error,
        })?
    {
        fs::write(&absolute, updated).map_err(|source| PostError::Io {
            path: relative_str.clone(),
            source,
        })?;
    }

    let activation = match &args.activation {
        PostActivation::Manual | PostActivation::Hold => Value::Null,
        PostActivation::Auto => {
            queue_activation(store, &ticket.id, ActivationKind::Auto, None, now_ms)?
        }
        PostActivation::At { .. } => {
            let eligible_at_ms =
                at_eligible_ms.expect("the dispatcher computes eligibility for at activations");
            queue_activation(
                store,
                &ticket.id,
                ActivationKind::At,
                Some(eligible_at_ms),
                now_ms,
            )?
        }
    };

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

pub(crate) fn parse_ticket_frontmatter(
    content: &str,
    path: &str,
) -> Result<frontmatter::Frontmatter, PostError> {
    let stamped = frontmatter::parse(content).map_err(|error| match error {
        FrontmatterError::InvalidBlockedBy => PostError::InvalidBlockedBy {
            path: path.to_owned(),
        },
        error => PostError::InvalidTicket {
            path: path.to_owned(),
            error,
        },
    })?;
    if stamped.name.trim().is_empty() {
        return Err(PostError::MissingName {
            path: path.to_owned(),
        });
    }
    if !stamped.has_blocked_by() {
        return Err(PostError::MissingBlockedBy {
            path: path.to_owned(),
        });
    }
    if frontmatter::body(content)
        .expect("frontmatter was already parsed")
        .trim()
        .is_empty()
    {
        return Err(PostError::EmptyBody {
            path: path.to_owned(),
        });
    }
    Ok(stamped)
}

/// Reuses an existing queued activation of the same kind so reposting cannot
/// enqueue duplicate work. A timed repost moves the queued activation to the
/// newly requested instant instead of keeping the stale one.
fn queue_activation(
    store: &Store,
    ticket_id: &str,
    kind: ActivationKind,
    eligible_at_ms: Option<i64>,
    now_ms: i64,
) -> Result<Value, PostError> {
    let id = match store.queued_ticket_activation(ticket_id, kind)? {
        Some(id) => {
            if let Some(eligible_at_ms) = eligible_at_ms {
                store.reschedule_activation(&id, eligible_at_ms, now_ms)?;
            }
            id
        }
        None => {
            let id = format!("A{}", store.next_activation_ordinal()?);
            store.insert_activation(
                &NewActivation {
                    id: &id,
                    kind,
                    ticket_id: Some(ticket_id),
                    project_id: None,
                    eligible_at_ms,
                    interval_ms: None,
                },
                now_ms,
            )?;
            id
        }
    };
    let mut activation = json!({
        "id": id,
        "kind": kind.as_str(),
        "state": "queued",
        "ticket": ticket_id,
    });
    if let Some(eligible_at_ms) = eligible_at_ms {
        activation["eligible_at_ms"] = json!(eligible_at_ms);
    }
    Ok(activation)
}

fn allocate_ticket_id(store: &Store, prefix: &str) -> Result<String, PostError> {
    let ids = store.ticket_ids()?;
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
    MissingName {
        path: String,
    },
    InvalidWorktreeStem {
        path: String,
        reason: String,
    },
    MissingBlockedBy {
        path: String,
    },
    InvalidBlockedBy {
        path: String,
    },
    EmptyBody {
        path: String,
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
    Store(StoreError),
    IdAllocation(IdError),
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
            Self::MissingName { path } => write!(
                formatter,
                "{path}: missing or empty `name`; add `name: Your ticket title`"
            ),
            Self::InvalidWorktreeStem { path, reason } => {
                write!(formatter, "{path}: {reason}")
            }
            Self::MissingBlockedBy { path } => write!(
                formatter,
                "{path}: missing `blocked_by`; add `blocked_by: []` if there are no dependencies"
            ),
            Self::InvalidBlockedBy { path } => write!(
                formatter,
                "{path}: invalid `blocked_by`; use `blocked_by: []` or a YAML list of ticket IDs"
            ),
            Self::EmptyBody { path } => write!(
                formatter,
                "{path}: empty `body`; add a ticket description after the frontmatter"
            ),
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

    use super::{PostError, handle as handle_with_directory};
    use crate::config::AgentConfig;
    use crate::flow::{Flow, Stage, StageKind, VerdictPolicy};
    use crate::protocol::{PostActivation, PostArgs};
    use crate::store::Store;

    fn world() -> (tempfile::TempDir, Store) {
        let root = tempdir().unwrap();
        std::fs::create_dir_all(root.path().join(".agents/sloop/tickets")).unwrap();
        let store = Store::open(&root.path().join("sloop.db"), 1_000).unwrap();
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
        store: &Store,
        args: &PostArgs,
        now_ms: i64,
        ticket_prefix: &str,
        agent: Option<&AgentConfig>,
        flows: &BTreeMap<String, Flow>,
        default_flow: &str,
    ) -> Result<serde_json::Value, PostError> {
        handle_with_directory(
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
        )
    }

    fn handle_at(
        root: &std::path::Path,
        store: &Store,
        args: &PostArgs,
        now_ms: i64,
        at_eligible_ms: i64,
    ) -> Result<serde_json::Value, PostError> {
        handle_with_directory(
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
        )
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
                ("claude".into(), vec!["claude".into(), "{prompt}".into()]),
                (
                    "codex".into(),
                    vec![
                        "codex".into(),
                        "{model}".into(),
                        "{effort}".into(),
                        "{prompt}".into(),
                    ],
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
        let fresh_store = Store::open(&root.path().join("fresh.db"), 3_000).unwrap();
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
