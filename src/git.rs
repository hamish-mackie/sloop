//! The few Git questions Sloop asks about the repository itself, kept in one
//! place so every caller agrees on how a checkout is located and inspected.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// Runs `git` in `root` and returns its trimmed stdout, or a message naming
/// the failed invocation.
pub fn stdout(root: &Path, args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(root)
        .stdin(Stdio::null())
        .output()
        .map_err(|error| error.to_string())?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    } else {
        Err(format!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

/// Whether `root` is the top of a Git checkout: either the main worktree,
/// whose `.git` is a directory, or a linked worktree, whose `.git` is a file.
pub fn has_repository(root: &Path) -> bool {
    root.join(".git").exists()
}

/// The main checkout that a linked worktree at `directory` belongs to, or
/// `None` when `directory` is not a linked worktree or the answer cannot be
/// established without guessing.
pub fn main_worktree_root(directory: &Path) -> Option<PathBuf> {
    if !directory.join(".git").is_file() {
        return None;
    }
    let common_dir = directory
        .join(stdout(directory, &["rev-parse", "--git-common-dir"]).ok()?)
        .canonicalize()
        .ok()?;
    if common_dir.file_name() != Some(OsStr::new(".git")) {
        return None;
    }
    common_dir.parent().map(Path::to_path_buf)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::process::{Command, Stdio};

    use tempfile::tempdir;

    use super::{has_repository, main_worktree_root};

    pub(crate) fn git(root: &Path, args: &[&str]) {
        let status = Command::new("git")
            .args(args)
            .current_dir(root)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?}");
    }

    pub(crate) fn init_with_commit(root: &Path) {
        git(root, &["init", "-q", "-b", "main"]);
        git(root, &["config", "user.email", "test@example.com"]);
        git(root, &["config", "user.name", "test"]);
        fs::write(root.join("README"), "hello\n").unwrap();
        git(root, &["add", "."]);
        git(root, &["commit", "-q", "-m", "init"]);
    }

    #[test]
    fn a_plain_directory_has_no_repository_and_no_main_worktree() {
        let root = tempdir().unwrap();
        assert!(!has_repository(root.path()));
        assert_eq!(main_worktree_root(root.path()), None);
    }

    #[test]
    fn the_main_worktree_is_a_repository_but_not_a_linked_one() {
        let root = tempdir().unwrap();
        init_with_commit(root.path());
        assert!(has_repository(root.path()));
        assert_eq!(main_worktree_root(root.path()), None);
    }

    #[test]
    fn a_linked_worktree_resolves_to_its_main_checkout() {
        let root = tempdir().unwrap();
        init_with_commit(root.path());
        git(
            root.path(),
            &["worktree", "add", "-q", ".worktrees/run-1", "-b", "run-1"],
        );
        let worktree = root.path().join(".worktrees/run-1");
        assert!(has_repository(&worktree));
        assert_eq!(
            main_worktree_root(&worktree),
            Some(root.path().canonicalize().unwrap())
        );
    }
}
