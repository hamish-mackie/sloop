use std::fs;
use std::io;
use std::path::Path;
use std::process::Command;

use crate::domain::ticket::TicketState;
use crate::reindex::ReindexError;
use crate::store::ReindexTicket;

pub(crate) fn derive_states(
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
    let created_at = reflog.status.success().then(|| {
        String::from_utf8_lossy(&reflog.stdout)
            .lines()
            .last()
            .map(str::to_owned)
    });
    let created_at = created_at.flatten();
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
