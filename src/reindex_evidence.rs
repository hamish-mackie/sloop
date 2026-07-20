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
    if merged.success() {
        return Ok(Some(TicketState::Merged));
    }
    // The ancestor test only sees merges that keep the branch tip reachable from
    // HEAD. Sloop's merge stage, an operator squash and a rebase all land the
    // same changes as new commits, so a branch whose work is demonstrably on the
    // default branch still fails that test. Fall back to patch equivalence
    // before calling the branch unreviewed; only a genuinely unlanded commit
    // earns `NeedsReview`.
    Ok(Some(if patch_equivalent(root, branch)? {
        TicketState::Merged
    } else {
        TicketState::NeedsReview
    }))
}

/// Reports whether every commit unique to `branch` has a patch-equivalent
/// commit reachable from HEAD.
///
/// `git cherry HEAD <branch>` prefixes each commit unique to the branch with
/// `-` when an equivalent patch is already upstream and `+` when it is not, so
/// the absence of any `+` line means the branch's work has landed. This runs at
/// most once per matched branch, and only after the cheaper ancestor test has
/// failed. When `git cherry` cannot decide — an unrelated history has no merge
/// base, for instance — the branch keeps the conservative `NeedsReview` answer.
fn patch_equivalent(root: &Path, branch: &str) -> Result<bool, ReindexError> {
    let cherry = Command::new("git")
        .args(["cherry", "HEAD", branch])
        .current_dir(root)
        .output()
        .map_err(|source| ReindexError::io(root, source))?;
    if !cherry.status.success() {
        return Ok(false);
    }
    Ok(!String::from_utf8_lossy(&cherry.stdout)
        .lines()
        .any(|line| line.starts_with('+')))
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

/// Folds one branch's derived state into the ticket's, completing the decision
/// ladder that [`branch_state`] starts.
///
/// Per branch: an ancestor of HEAD is `Merged`; otherwise a branch whose unique
/// commits are all patch-equivalent upstream is `Merged`; otherwise
/// `NeedsReview`. Across a ticket's branches — its recorded worktree branch and
/// every `sloop/<id>-a*` attempt branch — `NeedsReview` wins over `Merged`,
/// because a branch that survives both landing tests carries work an operator
/// has not seen. Patch equivalence is what keeps that precedence honest:
/// without it a leftover attempt branch whose changes were squashed onto the
/// default branch would drag a settled ticket back out of `Merged`.
fn merge_state(current: &mut Option<TicketState>, observed: TicketState) {
    if observed == TicketState::NeedsReview || current.is_none() {
        *current = Some(observed);
    }
}
