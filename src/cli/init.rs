use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

pub const DEFAULT_CONFIG: &str = include_str!("../defaults/config.yaml");
pub const DEFAULT_PROJECT: &str = include_str!("../defaults/projects/default.md");
pub const DEFAULT_FLOW: &str = include_str!("../defaults/flows/default.yaml");
/// The merge-train flow, materialized beside `default` rather than in place of
/// it. Its `verify` stage is a `{test_cmd}` placeholder that
/// [`render_train_flow`] fills in, so the shipped copy is not a flow file until
/// it has been rendered.
pub const TRAIN_FLOW_TEMPLATE: &str = include_str!("../defaults/flows/train.yaml");
pub const DEFAULT_REVIEW_PROMPT: &str = include_str!("../defaults/prompts/review.md");

/// The verify command the train flow falls back to when the repository has
/// configured none. Named here rather than buried in the template so the
/// fallback is one thing to find and change.
const FALLBACK_TEST_CMD: [&str; 2] = ["cargo", "test"];

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
    // Read back rather than assumed: `init` is idempotent and is routinely run
    // in a repository that already has a config, so the train's verify stage
    // can name the test command that repository actually uses.
    ensure_file(
        root,
        ".agents/sloop/flows/train.yaml",
        &render_train_flow(configured_test_cmd(root).as_deref()),
        &mut outcome,
    )?;
    ensure_directory(root, ".agents/sloop/prompts", &mut outcome, true)?;
    ensure_file(
        root,
        crate::worker::REVIEW_PROMPT_PATH,
        DEFAULT_REVIEW_PROMPT,
        &mut outcome,
    )?;

    Ok(outcome)
}

/// Fills the train template's `verify` command in. A JSON array is a YAML flow
/// sequence, so serializing the argv is also how it is quoted.
pub fn render_train_flow(test_cmd: Option<&[String]>) -> String {
    let fallback: Vec<String> = FALLBACK_TEST_CMD
        .iter()
        .map(|part| (*part).into())
        .collect();
    let cmd = test_cmd.filter(|cmd| !cmd.is_empty()).unwrap_or(&fallback);
    let rendered = serde_json::to_string(cmd).expect("an argv of strings serializes");
    TRAIN_FLOW_TEMPLATE.replace("{test_cmd}", &rendered)
}

/// The repository's `flow.test_cmd`, read straight from the config file rather
/// than through `Config::load`. Loading validates the whole repository —
/// flows, agent targets, projects — and `init` runs precisely when those are
/// not all in place yet, so a config it cannot fully validate must still be
/// allowed to answer this one question.
fn configured_test_cmd(root: &Path) -> Option<Vec<String>> {
    #[derive(serde::Deserialize)]
    struct ConfigFile {
        flow: Option<FlowSection>,
    }

    #[derive(serde::Deserialize)]
    struct FlowSection {
        test_cmd: Option<Vec<String>>,
    }

    let contents = fs::read_to_string(root.join(".agents/sloop/config.yaml")).ok()?;
    serde_yaml::from_str::<ConfigFile>(&contents)
        .ok()?
        .flow?
        .test_cmd
        .filter(|cmd| !cmd.is_empty())
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

    use super::{
        DEFAULT_CONFIG, DEFAULT_FLOW, DEFAULT_REVIEW_PROMPT, InitError, init, render_train_flow,
    };

    /// The shipped train is the pattern the docs describe, so what it parses
    /// into is the claim: verification after the sync, a fast-forward-only
    /// merge, and a backward edge from that merge to the sync it depends on.
    #[test]
    fn the_embedded_train_flow_parses_into_the_merge_train() {
        let rendered = render_train_flow(None);
        let flow = crate::flow::parse("train", &rendered).expect("train flow must parse");

        let names: Vec<&str> = flow
            .stages
            .iter()
            .map(|stage| stage.name.as_str())
            .collect();
        assert_eq!(names, ["build", "sync", "verify", "merge"]);

        assert_eq!(
            flow.stages[1].action,
            crate::flow::Actor::Builtin(crate::flow::Builtin::Sync)
        );
        // Verification is after the sync, not before it: the point of the
        // flow is that the tested tree is the tree that lands.
        assert_eq!(
            flow.stages[2].action,
            crate::flow::Actor::Exec {
                cmd: vec!["cargo".into(), "test".into()],
            }
        );
        assert!(flow.stages[3].ff_only, "the merge stage must be ff_only");
        assert_eq!(
            flow.stages[3].fail_action,
            crate::flow::FailAction::ReturnTo {
                stage: "sync".into(),
                attempts: 3,
            }
        );
        // The irreversible stage carries no second opinion, on principle.
        assert_eq!(flow.stages[3].result_check, crate::flow::Check::None);
    }

    /// A repository that already says how it runs its tests should not be
    /// handed `cargo test` regardless.
    #[test]
    fn the_train_flow_takes_the_repositorys_configured_test_command() {
        let rendered = render_train_flow(Some(&["just".to_owned(), "test all".to_owned()]));
        let flow = crate::flow::parse("train", &rendered).expect("train flow must parse");

        assert_eq!(
            flow.stages[2].action,
            crate::flow::Actor::Exec {
                cmd: vec!["just".into(), "test all".into()],
            }
        );
    }

    #[test]
    fn init_materializes_the_train_flow_beside_the_default_one() {
        let root = tempdir().unwrap();
        std::fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        std::fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\nflow:\n  test_cmd: [make, check]\n",
        )
        .unwrap();

        init(root.path()).unwrap();

        let default =
            std::fs::read_to_string(root.path().join(".agents/sloop/flows/default.yaml")).unwrap();
        assert_eq!(default, DEFAULT_FLOW, "the default flow is not replaced");
        let train =
            std::fs::read_to_string(root.path().join(".agents/sloop/flows/train.yaml")).unwrap();
        let flow = crate::flow::parse("train", &train).expect("materialized train flow parses");
        assert_eq!(
            flow.stages[2].action,
            crate::flow::Actor::Exec {
                cmd: vec!["make".into(), "check".into()],
            }
        );
    }

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

    /// The shipped review stage must be a real gate. Under the exec default
    /// (`result_check: none`) it would pass whenever the reviewer command
    /// exits 0,
    /// which `claude --print` always does — approving every run silently.
    #[test]
    fn the_shipped_review_stage_is_a_reported_gate() {
        let flow = crate::flow::parse("default", DEFAULT_FLOW).expect("default flow must parse");
        let review = flow
            .stages
            .iter()
            .find(|stage| stage.name == "review")
            .expect("default flow has a review stage");

        assert_eq!(review.result_check, crate::flow::Check::Reported);
        let crate::flow::Actor::Exec { cmd } = &review.action else {
            panic!("review stage must be an exec stage: {:?}", review.action);
        };
        // Bare `claude --print` cannot run any command, so it could never call
        // `sloop verdict`; the tool allowance is load-bearing, not decorative.
        assert!(
            cmd.windows(2)
                .any(|pair| pair == ["--allowedTools", "Bash"]),
            "review command must let the reviewer run commands: {cmd:?}"
        );
        assert!(
            cmd.iter()
                .any(|arg| arg.contains(crate::worker::REVIEW_PROMPT_PATH)),
            "review command must point the reviewer at the shipped prompt: {cmd:?}"
        );
    }

    /// The prompt, not the flow, is what tells the reviewer how to report.
    #[test]
    fn the_shipped_review_prompt_demands_an_explicit_verdict_call() {
        assert!(DEFAULT_REVIEW_PROMPT.contains("sloop verdict pass --reason"));
        assert!(DEFAULT_REVIEW_PROMPT.contains("sloop verdict fail --reason"));
        assert!(DEFAULT_REVIEW_PROMPT.contains("exactly once"));
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
                ".agents/sloop/flows/train.yaml",
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
        assert!(config.contains("stall_report_after: 10m"));
        assert!(config.contains("stall_after: 2h"));
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
