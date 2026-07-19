use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

pub const DEFAULT_CONFIG: &str = include_str!("defaults/config.yaml");
pub const DEFAULT_PROJECT: &str = include_str!("defaults/projects/default.md");
pub const DEFAULT_FLOW: &str = include_str!("defaults/flows/default.yaml");
pub const DEFAULT_REVIEW_PROMPT: &str = include_str!("defaults/prompts/review.md");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitOutcome {
    pub repository_root: PathBuf,
    pub created: Vec<String>,
    pub existing: Vec<String>,
}

/// Scaffolds a Sloop repository in `root`: committed configuration, project,
/// ticket, flow, and prompt files. Idempotent; never starts the daemon and
/// never rewrites a file that already exists.
pub fn init(root: &Path) -> Result<InitOutcome, InitError> {
    let mut outcome = InitOutcome {
        repository_root: root.to_path_buf(),
        created: Vec::new(),
        existing: Vec::new(),
    };

    ensure_directory(root, ".agents/sloop", &mut outcome, false)?;
    ensure_file(
        root,
        ".agents/sloop/config.yaml",
        DEFAULT_CONFIG,
        &mut outcome,
    )?;
    ensure_directory(root, ".agents/sloop/projects", &mut outcome, true)?;
    ensure_file(
        root,
        ".agents/sloop/projects/default.md",
        DEFAULT_PROJECT,
        &mut outcome,
    )?;
    ensure_directory(root, ".agents/sloop/tickets", &mut outcome, true)?;
    ensure_directory(root, ".agents/sloop/flows", &mut outcome, true)?;
    ensure_file(
        root,
        ".agents/sloop/flows/default.yaml",
        DEFAULT_FLOW,
        &mut outcome,
    )?;
    ensure_directory(root, ".agents/sloop/prompts", &mut outcome, true)?;
    ensure_file(
        root,
        crate::flow::REVIEW_PROMPT_PATH,
        DEFAULT_REVIEW_PROMPT,
        &mut outcome,
    )?;

    Ok(outcome)
}

fn ensure_directory(
    root: &Path,
    relative: &str,
    outcome: &mut InitOutcome,
    report: bool,
) -> Result<(), InitError> {
    let path = root.join(relative);
    if path.is_dir() {
        if report {
            outcome.existing.push(relative.into());
        }
        return Ok(());
    }
    if path.exists() {
        return Err(InitError::Conflict {
            path: relative.into(),
            reason: "a non-directory file is in the way".into(),
        });
    }
    fs::create_dir_all(&path).map_err(|source| InitError::Io {
        path: relative.into(),
        source,
    })?;
    if report {
        outcome.created.push(relative.into());
    }
    Ok(())
}

fn ensure_file(
    root: &Path,
    relative: &str,
    contents: &str,
    outcome: &mut InitOutcome,
) -> Result<(), InitError> {
    let path = root.join(relative);
    if path.is_file() {
        outcome.existing.push(relative.into());
        return Ok(());
    }
    if path.exists() {
        return Err(InitError::Conflict {
            path: relative.into(),
            reason: "a directory is in the way".into(),
        });
    }
    fs::write(&path, contents).map_err(|source| InitError::Io {
        path: relative.into(),
        source,
    })?;
    outcome.created.push(relative.into());
    Ok(())
}

#[derive(Debug)]
pub enum InitError {
    Conflict { path: String, reason: String },
    Io { path: String, source: io::Error },
}

impl fmt::Display for InitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict { path, reason } => write!(formatter, "{path}: {reason}"),
            Self::Io { path, source } => write!(formatter, "{path}: {source}"),
        }
    }
}

impl std::error::Error for InitError {}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{DEFAULT_CONFIG, DEFAULT_FLOW, InitError, init};

    #[test]
    fn embedded_default_flow_parses() {
        let flow = crate::flow::parse("default", DEFAULT_FLOW).expect("default flow must parse");
        let names: Vec<&str> = flow
            .stages
            .iter()
            .map(|stage| stage.name.as_str())
            .collect();
        assert_eq!(names, ["build", "review", "merge"]);
    }

    #[test]
    fn init_scaffolds_every_committed_path() {
        let root = tempdir().unwrap();

        let outcome = init(root.path()).unwrap();
        assert_eq!(
            outcome.created,
            vec![
                ".agents/sloop/config.yaml",
                ".agents/sloop/projects",
                ".agents/sloop/projects/default.md",
                ".agents/sloop/tickets",
                ".agents/sloop/flows",
                ".agents/sloop/flows/default.yaml",
                ".agents/sloop/prompts",
                ".agents/sloop/prompts/review.md",
            ]
        );
        assert!(outcome.existing.is_empty());
        assert!(
            root.path()
                .join(".agents/sloop/projects/default.md")
                .is_file()
        );
        assert!(!root.path().join(".gitignore").exists());
        let config = std::fs::read_to_string(root.path().join(".agents/sloop/config.yaml"))
            .expect("read default config");
        assert_eq!(config, DEFAULT_CONFIG);
        assert!(config.contains("default_target: claude"));
        assert!(config.contains("model: opus"));
        assert!(config.contains("effort: high"));
        assert!(config.contains("worktree_retention: 7d"));
        assert!(config.contains("- claude"));
        assert!(config.contains("- opencode"));
        assert!(config.contains("- codex"));
        assert!(!config.contains("sloop brief"));
        let flow =
            std::fs::read_to_string(root.path().join(".agents/sloop/flows/default.yaml")).unwrap();
        assert!(flow.contains(".agents/sloop/prompts/review.md"));
    }

    #[test]
    fn init_preserves_an_existing_gitignore() {
        let root = tempdir().unwrap();
        std::fs::write(root.path().join(".gitignore"), "target/\n").unwrap();

        init(root.path()).unwrap();

        let gitignore = std::fs::read_to_string(root.path().join(".gitignore")).unwrap();
        assert_eq!(gitignore, "target/\n");
    }

    #[test]
    fn init_is_idempotent_and_preserves_existing_files() {
        let root = tempdir().unwrap();
        init(root.path()).unwrap();
        std::fs::write(
            root.path().join(".agents/sloop/projects/default.md"),
            "customized\n",
        )
        .unwrap();

        let outcome = init(root.path()).unwrap();
        assert!(outcome.created.is_empty());
        assert_eq!(
            std::fs::read_to_string(root.path().join(".agents/sloop/projects/default.md")).unwrap(),
            "customized\n"
        );
    }

    #[test]
    fn an_obstructing_directory_is_a_conflict() {
        let root = tempdir().unwrap();
        std::fs::create_dir_all(root.path().join(".agents/sloop/projects/default.md")).unwrap();

        assert!(matches!(init(root.path()), Err(InitError::Conflict { .. })));
    }
}
