use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::io;
use std::path::Path;

use serde_json::{Value, json};

use crate::config::{AgentConfig, expand_agent_cmd};
use crate::flow::Flow;
use crate::frontmatter;
use crate::ids::next_id;
use crate::sources::TicketSource;
use crate::store::{ReindexTicket, Store};

#[allow(clippy::too_many_arguments)]
pub fn run(
    root: &Path,
    ticket_source: &dyn TicketSource,
    worktree_dir: &Path,
    state_dir: &Path,
    store: &Store,
    now_ms: i64,
    ticket_prefix: &str,
    project_ids: &[String],
    agent: Option<&AgentConfig>,
    flows: &BTreeMap<String, Flow>,
    default_flow: &str,
) -> Result<Value, ReindexError> {
    let authored = ticket_source
        .pull()
        .map_err(|error| ReindexError(error.to_string()))?;
    let mut known_ids: Vec<String> = authored
        .iter()
        .filter_map(|ticket| ticket.frontmatter.id.clone())
        .collect();
    let mut unique_ids = BTreeSet::new();
    for id in &known_ids {
        if !unique_ids.insert(id.clone()) {
            return Err(ReindexError(format!(
                "duplicate ticket ID `{id}` in the configured ticket directory"
            )));
        }
    }
    let mut unique_refs = BTreeSet::new();
    for ticket in &authored {
        if !unique_refs.insert((&ticket.source, &ticket.source_ref)) {
            return Err(ReindexError(format!(
                "duplicate source reference `{}` from `{}`",
                ticket.source_ref, ticket.source
            )));
        }
    }
    let known_projects: BTreeSet<&str> = project_ids.iter().map(String::as_str).collect();
    let fallback_project = if known_projects.contains("default") {
        "default".to_owned()
    } else {
        project_ids
            .first()
            .cloned()
            .ok_or_else(|| ReindexError("cannot index tickets without an indexed project".into()))?
    };

    let mut tickets = Vec::with_capacity(authored.len());
    let mut assigned_ids = BTreeSet::new();
    for authored_ticket in authored {
        let prior_id = store
            .ticket_by_source_ref(&authored_ticket.source, &authored_ticket.source_ref)
            .map_err(|error| ReindexError(error.to_string()))?
            .map(|ticket| ticket.id);
        let id = match authored_ticket.frontmatter.id.clone().or(prior_id) {
            Some(id) => id,
            None => {
                let id = next_id(ticket_prefix, known_ids.iter().map(String::as_str))
                    .map_err(|error| ReindexError(error.to_string()))?;
                known_ids.push(id.clone());
                id
            }
        };
        if !assigned_ids.insert(id.clone()) {
            return Err(ReindexError(format!(
                "duplicate ticket ID `{id}` in the pulled ticket source"
            )));
        }
        if !known_ids.contains(&id) {
            known_ids.push(id.clone());
        }
        let authored_project = authored_ticket
            .frontmatter
            .project
            .clone()
            .unwrap_or_else(|| "default".to_owned());
        let mut held_reason = authored_ticket.validation_error.clone();
        if authored_ticket.frontmatter.name.trim().is_empty() {
            held_reason.get_or_insert_with(|| {
                format!(
                    "{}: frontmatter field `name` is required and must be non-empty",
                    authored_ticket.source_ref
                )
            });
        }
        if authored_ticket.body.trim().is_empty() {
            held_reason.get_or_insert_with(|| {
                format!(
                    "{}: ticket body must be non-empty",
                    authored_ticket.source_ref
                )
            });
        }
        let project = if known_projects.contains(authored_project.as_str()) {
            authored_project.clone()
        } else {
            held_reason.get_or_insert_with(|| {
                format!(
                    "{}: project `{authored_project}` is not indexed",
                    authored_ticket.source_ref
                )
            });
            fallback_project.clone()
        };
        let flow = authored_ticket
            .frontmatter
            .flow
            .clone()
            .unwrap_or_else(|| default_flow.to_owned());
        if !flows.contains_key(&flow) {
            held_reason.get_or_insert_with(|| {
                format!(
                    "{}: flow `{flow}` is not defined",
                    authored_ticket.source_ref
                )
            });
        }
        let target = match authored_ticket.frontmatter.target.as_deref() {
            Some(target) if agent.is_some_and(|agent| agent.targets.contains_key(target)) => {
                Some(target.to_owned())
            }
            Some(target) => {
                held_reason.get_or_insert_with(|| {
                    format!(
                        "{}: agent target `{target}` is not configured",
                        authored_ticket.source_ref
                    )
                });
                Some(target.to_owned())
            }
            None => agent.map(|agent| agent.default_target.clone()),
        };
        if let (Some(agent), Some(target)) = (agent, target.as_deref()) {
            if let Some(command) = agent.targets.get(target)
                && let Err(message) = expand_agent_cmd(
                    command,
                    authored_ticket.frontmatter.model.as_deref(),
                    authored_ticket.frontmatter.effort.as_deref(),
                    "",
                )
            {
                held_reason.get_or_insert_with(|| {
                    format!(
                        "{}: ticket using agent target `{target}` {message}",
                        authored_ticket.source_ref
                    )
                });
            }
        }
        let worktree = authored_ticket
            .frontmatter
            .worktree
            .clone()
            .unwrap_or_else(|| format!("sloop/{id}"));
        if held_reason.is_none()
            && let (Some(path), Some(content)) = (
                authored_ticket.file_path.as_ref(),
                authored_ticket.original_content.as_ref(),
            )
            && let Some(updated) = frontmatter::stamp(content, &id, &project, &worktree, &flow)
                .map_err(|error| ReindexError(format!("{}: {error}", authored_ticket.source_ref)))?
        {
            let absolute = root.join(path);
            fs::write(&absolute, updated).map_err(|source| ReindexError::io(&absolute, source))?;
        }

        tickets.push(ReindexTicket {
            id,
            project_id: project,
            source: authored_ticket.source,
            source_ref: authored_ticket.source_ref,
            file_path: authored_ticket
                .file_path
                .map(|path| path.to_string_lossy().into_owned()),
            name: authored_ticket.frontmatter.name,
            blocked_by: authored_ticket.frontmatter.blocked_by,
            worktree,
            target,
            model: authored_ticket.frontmatter.model,
            effort: authored_ticket.frontmatter.effort,
            flow,
            body: authored_ticket.body,
            held_reason,
            derived_state: None,
        });
    }

    let ticket_ids: BTreeSet<String> = tickets.iter().map(|ticket| ticket.id.clone()).collect();
    let mut dependencies = BTreeMap::new();
    for ticket in &mut tickets {
        let unknown_blocker = ticket
            .blocked_by
            .iter()
            .find(|blocker| !ticket_ids.contains(*blocker))
            .cloned();
        if let Some(blocker) = unknown_blocker {
            ticket.held_reason.get_or_insert_with(|| {
                format!(
                    "ticket `{}` field `blocked_by` references unknown ticket `{blocker}`; edit `{}` to drop or correct the reference",
                    ticket.id, ticket.source_ref
                )
            });
            ticket.blocked_by.clear();
        } else {
            dependencies.insert(ticket.id.clone(), ticket.blocked_by.clone());
        }
    }
    if let Some(chain) = crate::domain::graph::find_cycle(&dependencies) {
        return Err(ReindexError(format!(
            "field `blocked_by` creates a dependency cycle: {}",
            chain.join(" -> ")
        )));
    }

    crate::reindex_evidence::derive_states(root, worktree_dir, &mut tickets)?;
    let result = store
        .apply_reindex(project_ids, &tickets, now_ms)
        .map_err(|error| ReindexError(error.to_string()))?;
    crate::reindex_evidence::seed_run_counter(root, worktree_dir, state_dir, store)?;
    let state_changes: Vec<Value> = result
        .state_changes
        .into_iter()
        .map(|change| {
            json!({
                "ticket": change.ticket_id,
                "previous_state": change.previous_state,
                "state": change.state,
            })
        })
        .collect();
    Ok(json!({
        "projects_indexed": project_ids.len(),
        "tickets_indexed": tickets.len(),
        "tickets_state_changed": state_changes.len(),
        "state_changes": state_changes,
        "rows_dropped": result.rows_dropped,
    }))
}

#[derive(Debug)]
pub struct ReindexError(pub(crate) String);

impl ReindexError {
    pub(crate) fn io(path: &Path, source: io::Error) -> Self {
        Self(format!("{}: {source}", path.display()))
    }
}

impl fmt::Display for ReindexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ReindexError {}
