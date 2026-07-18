use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::{Value, json};

use crate::config::{AgentConfig, expand_agent_cmd};
use crate::domain::ticket::TicketState;
use crate::flow::Flow;
use crate::frontmatter::{self, Frontmatter};
use crate::ids::next_id;
use crate::post::parse_ticket_frontmatter;
use crate::store::{ReindexTicket, Store};

#[allow(clippy::too_many_arguments)]
pub fn run(
    root: &Path,
    ticket_dir: &Path,
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
    let mut files = ticket_files(root, ticket_dir)?;
    let mut known_ids: Vec<String> = files
        .iter()
        .filter_map(|file| file.frontmatter.id.clone())
        .collect();
    let mut unique_ids = BTreeSet::new();
    for id in &known_ids {
        if !unique_ids.insert(id.clone()) {
            return Err(ReindexError(format!(
                "duplicate ticket ID `{id}` in the configured ticket directory"
            )));
        }
    }
    let known_projects: BTreeSet<&str> = project_ids.iter().map(String::as_str).collect();

    let mut tickets = Vec::with_capacity(files.len());
    for file in &mut files {
        let id = match file.frontmatter.id.clone() {
            Some(id) => id,
            None => {
                let id = next_id(ticket_prefix, known_ids.iter().map(String::as_str))
                    .map_err(|error| ReindexError(error.to_string()))?;
                known_ids.push(id.clone());
                id
            }
        };
        let project = file
            .frontmatter
            .project
            .clone()
            .unwrap_or_else(|| "default".to_owned());
        if !known_projects.contains(project.as_str()) {
            return Err(ReindexError(format!(
                "{}: project `{project}` is not indexed",
                file.relative.display()
            )));
        }
        let flow = file
            .frontmatter
            .flow
            .clone()
            .unwrap_or_else(|| default_flow.to_owned());
        if !flows.contains_key(&flow) {
            return Err(ReindexError(format!(
                "{}: flow `{flow}` is not defined",
                file.relative.display()
            )));
        }
        let target = match file.frontmatter.target.as_deref() {
            Some(target) if agent.is_some_and(|agent| agent.targets.contains_key(target)) => {
                Some(target.to_owned())
            }
            Some(target) => {
                return Err(ReindexError(format!(
                    "{}: agent target `{target}` is not configured",
                    file.relative.display()
                )));
            }
            None => agent.map(|agent| agent.default_target.clone()),
        };
        if let (Some(agent), Some(target)) = (agent, target.as_deref()) {
            let command = &agent.targets[target];
            expand_agent_cmd(
                command,
                file.frontmatter.model.as_deref(),
                file.frontmatter.effort.as_deref(),
                "",
            )
            .map_err(|message| {
                ReindexError(format!(
                    "{}: ticket using agent target `{target}` {message}",
                    file.relative.display()
                ))
            })?;
        }
        let worktree = file
            .frontmatter
            .worktree
            .clone()
            .unwrap_or_else(|| format!("sloop/{id}"));
        if let Some(updated) = frontmatter::stamp(&file.content, &id, &project, &worktree, &flow)
            .map_err(|error| ReindexError(format!("{}: {error}", file.relative.display())))?
        {
            fs::write(&file.path, updated)
                .map_err(|source| ReindexError::io(&file.path, source))?;
        }

        tickets.push(ReindexTicket {
            id,
            project_id: project,
            file_path: file.relative.to_string_lossy().into_owned(),
            name: file.frontmatter.name.clone(),
            blocked_by: file.frontmatter.blocked_by.clone(),
            worktree,
            target,
            model: file.frontmatter.model.clone(),
            effort: file.frontmatter.effort.clone(),
            flow,
            derived_state: None,
        });
    }

    let ticket_ids: BTreeSet<&str> = tickets.iter().map(|ticket| ticket.id.as_str()).collect();
    let mut dependencies = BTreeMap::new();
    for ticket in &tickets {
        for blocker in &ticket.blocked_by {
            if !ticket_ids.contains(blocker.as_str()) {
                return Err(ReindexError(format!(
                    "ticket `{}` field `blocked_by` references unknown ticket `{blocker}`",
                    ticket.id
                )));
            }
        }
        dependencies.insert(ticket.id.clone(), ticket.blocked_by.clone());
    }
    if let Some(chain) = crate::domain::graph::find_cycle(&dependencies) {
        return Err(ReindexError(format!(
            "field `blocked_by` creates a dependency cycle: {}",
            chain.join(" -> ")
        )));
    }

    derive_states(root, worktree_dir, &mut tickets)?;
    let result = store
        .apply_reindex(project_ids, &tickets, now_ms)
        .map_err(|error| ReindexError(error.to_string()))?;
    seed_run_counter(root, worktree_dir, state_dir, store)?;
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

fn seed_run_counter(
    root: &Path,
    worktree_dir: &Path,
    state_dir: &Path,
    store: &Store,
) -> Result<(), ReindexError> {
    let mut greatest = 0;
    for directory in [worktree_dir.to_path_buf(), state_dir.join("runs")] {
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(source) => return Err(ReindexError::io(&directory, source)),
        };
        for entry in entries {
            let name = entry
                .map_err(|source| ReindexError::io(&directory, source))?
                .file_name();
            if let Some(ordinal) = run_ordinal(&name.to_string_lossy()) {
                greatest = greatest.max(ordinal);
            }
        }
    }
    for branch in git_branches(root)? {
        if let Some(ordinal) = branch.rsplit('-').next().and_then(run_ordinal) {
            greatest = greatest.max(ordinal);
        }
    }
    store
        .ensure_next_run_ordinal(greatest + 1)
        .map_err(|error| ReindexError(error.to_string()))
}

fn run_ordinal(value: &str) -> Option<i64> {
    value
        .strip_prefix('R')?
        .parse::<i64>()
        .ok()
        .filter(|ordinal| *ordinal > 0)
}

struct TicketFile {
    path: PathBuf,
    relative: PathBuf,
    content: String,
    frontmatter: Frontmatter,
}

fn ticket_files(root: &Path, ticket_dir: &Path) -> Result<Vec<TicketFile>, ReindexError> {
    let directory = root.join(ticket_dir);
    let mut paths = Vec::new();
    collect_markdown_files(&directory, &mut paths)?;
    paths.sort();
    paths
        .into_iter()
        .map(|path| {
            let relative = path.strip_prefix(root).unwrap_or(&path).to_path_buf();
            let content =
                fs::read_to_string(&path).map_err(|source| ReindexError::io(&path, source))?;
            let label = relative.to_string_lossy();
            let frontmatter = parse_ticket_frontmatter(&content, &label)
                .map_err(|error| ReindexError(error.to_string()))?;
            Ok(TicketFile {
                path,
                relative,
                content,
                frontmatter,
            })
        })
        .collect()
}

fn collect_markdown_files(directory: &Path, paths: &mut Vec<PathBuf>) -> Result<(), ReindexError> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(source) => return Err(ReindexError::io(directory, source)),
    };
    for entry in entries {
        let path = entry
            .map_err(|source| ReindexError::io(directory, source))?
            .path();
        if path.is_dir() {
            collect_markdown_files(&path, paths)?;
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("md") {
            paths.push(path);
        }
    }
    Ok(())
}

fn derive_states(
    root: &Path,
    worktree_dir: &Path,
    tickets: &mut [ReindexTicket],
) -> Result<(), ReindexError> {
    for branch in git_branches(root)? {
        if let Some(index) = ticket_for_branch(tickets, &branch)
            && let Some(state) = branch_state(root, &branch)?
        {
            merge_state(&mut tickets[index].derived_state, state);
        }
    }

    let entries = match fs::read_dir(worktree_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(source) => return Err(ReindexError::io(worktree_dir, source)),
    };
    for entry in entries {
        let path = entry
            .map_err(|source| ReindexError::io(worktree_dir, source))?
            .path();
        if !path.is_dir() {
            continue;
        }
        let output = Command::new("git")
            .args(["symbolic-ref", "--quiet", "--short", "HEAD"])
            .current_dir(&path)
            .output()
            .map_err(|source| ReindexError::io(&path, source))?;
        if !output.status.success() {
            continue;
        }
        let branch = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        let Some(index) = ticket_for_branch(tickets, &branch) else {
            continue;
        };
        if let Some(state) = branch_state(root, &branch)? {
            merge_state(&mut tickets[index].derived_state, state);
        }
    }
    Ok(())
}

fn git_branches(root: &Path) -> Result<Vec<String>, ReindexError> {
    let branches = Command::new("git")
        .args(["for-each-ref", "--format=%(refname:short)", "refs/heads"])
        .current_dir(root)
        .output()
        .map_err(|source| ReindexError::io(root, source))?;
    if !branches.status.success() {
        return Err(ReindexError(format!(
            "cannot enumerate Git branches: {}",
            String::from_utf8_lossy(&branches.stderr).trim()
        )));
    }
    Ok(String::from_utf8_lossy(&branches.stdout)
        .lines()
        .map(str::to_owned)
        .collect())
}

fn ticket_for_branch(tickets: &[ReindexTicket], branch: &str) -> Option<usize> {
    tickets.iter().position(|ticket| {
        branch == ticket.worktree || branch.starts_with(&format!("sloop/{}-a", ticket.id))
    })
}

fn branch_state(root: &Path, branch: &str) -> Result<Option<TicketState>, ReindexError> {
    let reference = format!("refs/heads/{branch}");
    let exists = Command::new("git")
        .args(["show-ref", "--verify", "--quiet", &reference])
        .current_dir(root)
        .status()
        .map_err(|source| ReindexError::io(root, source))?;
    if !exists.success() {
        return Ok(None);
    }
    let tip = git_output(root, &["rev-parse", branch], branch)?;
    let reflog = Command::new("git")
        .args(["reflog", "show", "--format=%H", branch])
        .current_dir(root)
        .output()
        .map_err(|source| ReindexError::io(root, source))?;
    let created_at = reflog
        .status
        .success()
        .then(|| {
            String::from_utf8_lossy(&reflog.stdout)
                .lines()
                .last()
                .map(str::to_owned)
        })
        .flatten();
    let merged = Command::new("git")
        .args(["merge-base", "--is-ancestor", branch, "HEAD"])
        .current_dir(root)
        .status()
        .map_err(|source| ReindexError::io(root, source))?;
    if !merged.success() && merged.code() != Some(1) {
        return Err(ReindexError(format!(
            "cannot compare Git branch `{branch}` with HEAD"
        )));
    }
    // The branch's creation tip is stable even if the default branch is later
    // rebased or squashed. Comparing against HEAD would turn untouched run
    // branches into apparent work after any such rewrite.
    let has_work = created_at.is_some_and(|created_at| created_at != tip);
    if !has_work {
        return Ok(None);
    }
    Ok(Some(if merged.success() {
        TicketState::Merged
    } else {
        TicketState::NeedsReview
    }))
}

fn git_output(root: &Path, args: &[&str], branch: &str) -> Result<String, ReindexError> {
    let output = Command::new("git")
        .args(args)
        .current_dir(root)
        .output()
        .map_err(|source| ReindexError::io(root, source))?;
    if !output.status.success() {
        return Err(ReindexError(format!(
            "cannot inspect Git branch `{branch}`: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn merge_state(current: &mut Option<TicketState>, observed: TicketState) {
    if observed == TicketState::NeedsReview || current.is_none() {
        *current = Some(observed);
    }
}

#[derive(Debug)]
pub struct ReindexError(String);

impl ReindexError {
    fn io(path: &Path, source: io::Error) -> Self {
        Self(format!("{}: {source}", path.display()))
    }
}

impl fmt::Display for ReindexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ReindexError {}
