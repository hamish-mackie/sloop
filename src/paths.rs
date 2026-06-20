use std::env;
use std::ffi::OsStr;
use std::fmt;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryPaths {
    pub state_dir: PathBuf,
    pub runtime_dir: PathBuf,
    pub operator_socket: PathBuf,
    pub lock_path: PathBuf,
    pub daemon_log: PathBuf,
    pub db_path: PathBuf,
}

pub fn resolve(root: &Path) -> Result<RepositoryPaths, PathError> {
    let home = env::var_os("HOME").map(PathBuf::from);
    let xdg_state = absolute_env("XDG_STATE_HOME");
    let xdg_runtime = absolute_env("XDG_RUNTIME_DIR");
    let bases = platform_bases(
        Platform::current(),
        home.as_deref(),
        &env::temp_dir(),
        xdg_state.as_deref(),
        xdg_runtime.as_deref(),
    )?;
    Ok(paths_from_bases(root, &bases))
}

pub fn repository_key(root: &Path) -> String {
    let name = root
        .file_name()
        .map(safe_name)
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "repository".into());
    format!("{name}-{:016x}", path_hash(root))
}

fn runtime_key(root: &Path) -> String {
    format!("{:016x}", path_hash(root))
}

fn path_hash(path: &Path) -> u64 {
    // FNV-1a is explicit and stable across Rust releases, unlike DefaultHasher.
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in path.as_os_str().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn safe_name(name: &OsStr) -> String {
    let mut safe = String::new();
    let mut last_was_dash = false;
    for character in name.to_string_lossy().chars().take(32) {
        let character = if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
            character
        } else {
            '-'
        };
        if character == '-' && last_was_dash {
            continue;
        }
        last_was_dash = character == '-';
        safe.push(character);
    }
    safe.trim_matches('-').to_owned()
}

fn absolute_env(name: &str) -> Option<PathBuf> {
    env::var_os(name)
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Platform {
    MacOs,
    OtherUnix,
}

impl Platform {
    fn current() -> Self {
        if cfg!(target_os = "macos") {
            Self::MacOs
        } else {
            Self::OtherUnix
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BasePaths {
    state_repositories: PathBuf,
    log_repositories: Option<PathBuf>,
    runtime_repositories: PathBuf,
}

fn platform_bases(
    platform: Platform,
    home: Option<&Path>,
    temp: &Path,
    xdg_state: Option<&Path>,
    xdg_runtime: Option<&Path>,
) -> Result<BasePaths, PathError> {
    let (state_repositories, log_repositories) = if let Some(state) = xdg_state {
        (state.join("sloop/repositories"), None)
    } else {
        let home = home.ok_or(PathError::MissingHome)?;
        match platform {
            Platform::MacOs => (
                home.join("Library/Application Support/sloop/repositories"),
                Some(home.join("Library/Logs/sloop")),
            ),
            Platform::OtherUnix => (home.join(".local/state/sloop/repositories"), None),
        }
    };

    let runtime_repositories = match xdg_runtime {
        Some(runtime) => runtime.join("sloop"),
        None if platform == Platform::MacOs => temp.join("sloop"),
        None => temp.join(format!("sloop-{}", unsafe { libc::geteuid() })),
    };

    Ok(BasePaths {
        state_repositories,
        log_repositories,
        runtime_repositories,
    })
}

fn paths_from_bases(root: &Path, bases: &BasePaths) -> RepositoryPaths {
    let key = repository_key(root);
    let state_dir = bases.state_repositories.join(&key);
    let runtime_dir = bases.runtime_repositories.join(runtime_key(root));
    let daemon_log = bases.log_repositories.as_ref().map_or_else(
        || state_dir.join("logs/daemon.ndjson"),
        |logs| logs.join(&key).join("daemon.ndjson"),
    );
    RepositoryPaths {
        operator_socket: runtime_dir.join("operator.sock"),
        // The lock protects the state database, so it must follow that
        // database even when two processes have different runtime roots.
        lock_path: state_dir.join("daemon.lock"),
        daemon_log,
        db_path: state_dir.join("sloop.db"),
        state_dir,
        runtime_dir,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathError {
    MissingHome,
}

impl fmt::Display for PathError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingHome => write!(
                formatter,
                "HOME is not set and XDG_STATE_HOME does not provide a state directory"
            ),
        }
    }
}

impl std::error::Error for PathError {}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{BasePaths, Platform, paths_from_bases, platform_bases, repository_key};

    #[test]
    fn repository_keys_are_readable_stable_and_path_specific() {
        let first = repository_key(Path::new("/work/one/my project"));
        let second = repository_key(Path::new("/work/two/my project"));

        assert!(first.starts_with("my-project-"), "{first}");
        assert_eq!(first, repository_key(Path::new("/work/one/my project")));
        assert_ne!(first, second);
    }

    #[test]
    fn xdg_layout_keeps_persistent_and_ephemeral_files_separate() {
        let bases = platform_bases(
            Platform::OtherUnix,
            Some(Path::new("/home/alice")),
            Path::new("/tmp"),
            Some(Path::new("/state")),
            Some(Path::new("/run/user/1000")),
        )
        .unwrap();
        let paths = paths_from_bases(Path::new("/work/repo"), &bases);

        assert!(paths.state_dir.starts_with("/state/sloop/repositories"));
        assert!(paths.daemon_log.starts_with(&paths.state_dir));
        assert!(paths.operator_socket.starts_with("/run/user/1000/sloop"));
        assert!(paths.lock_path.starts_with(&paths.state_dir));
        assert!(!paths.lock_path.starts_with(&paths.runtime_dir));
    }

    #[test]
    fn linux_layout_defaults_to_dot_local_state_and_a_uid_temp_directory() {
        let bases = platform_bases(
            Platform::OtherUnix,
            Some(Path::new("/home/alice")),
            Path::new("/tmp"),
            None,
            None,
        )
        .unwrap();
        let paths = paths_from_bases(Path::new("/work/repo"), &bases);

        assert!(
            paths
                .state_dir
                .starts_with("/home/alice/.local/state/sloop/repositories")
        );
        assert!(
            paths
                .operator_socket
                .starts_with(format!("/tmp/sloop-{}", unsafe { libc::geteuid() }))
        );
    }

    #[test]
    fn macos_layout_uses_library_and_the_user_temporary_directory() {
        let bases = platform_bases(
            Platform::MacOs,
            Some(Path::new("/Users/alice")),
            Path::new("/var/folders/user/T"),
            None,
            None,
        )
        .unwrap();
        let paths = paths_from_bases(Path::new("/work/repo"), &bases);

        assert!(
            paths
                .state_dir
                .starts_with("/Users/alice/Library/Application Support/sloop/repositories")
        );
        assert!(
            paths
                .daemon_log
                .starts_with("/Users/alice/Library/Logs/sloop")
        );
        assert!(
            paths
                .operator_socket
                .starts_with("/var/folders/user/T/sloop")
        );
    }

    #[test]
    fn path_construction_uses_short_runtime_keys() {
        let bases = BasePaths {
            state_repositories: "/state".into(),
            log_repositories: None,
            runtime_repositories: "/runtime".into(),
        };
        let paths = paths_from_bases(Path::new("/a/repository-with-a-very-long-name"), &bases);

        assert_eq!(
            paths
                .runtime_dir
                .file_name()
                .unwrap()
                .to_string_lossy()
                .len(),
            16
        );
    }
}
