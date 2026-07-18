use std::collections::BTreeMap;
use std::env;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::flow::{DEFAULT_FLOW_NAME, Flow};
use crate::ids::{DEFAULT_PROJECT_PREFIX, DEFAULT_TICKET_PREFIX, valid_prefix};

pub const CONFIG_VERSION: u32 = 1;
pub const DEFAULT_DELETE_MISSING_AFTER_MS: i64 = 30 * 24 * 60 * 60 * 1000;
pub const DEFAULT_WORKTREE_DIR: &str = ".worktrees";
pub const DEFAULT_PROJECT_DIR: &str = ".agents/sloop/projects";
pub const DEFAULT_TICKET_DIR: &str = ".agents/sloop/tickets";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Project {
    pub root: PathBuf,
    pub config_path: PathBuf,
    pub state_dir: PathBuf,
    pub runtime_dir: PathBuf,
    pub operator_socket: PathBuf,
    pub lock_path: PathBuf,
    pub daemon_log: PathBuf,
    pub db_path: PathBuf,
}

impl Project {
    pub fn discover(start: &Path) -> Result<Self, ConfigError> {
        let start = start.canonicalize().map_err(|source| ConfigError::Io {
            path: start.to_path_buf(),
            source,
        })?;

        for directory in start.ancestors() {
            let config_path = directory.join(".agents/sloop/config.yaml");
            if config_path.is_file() {
                let paths = crate::paths::resolve(directory).map_err(ConfigError::Paths)?;
                return Ok(Self {
                    root: directory.to_path_buf(),
                    config_path,
                    state_dir: paths.state_dir,
                    runtime_dir: paths.runtime_dir,
                    operator_socket: paths.operator_socket,
                    lock_path: paths.lock_path,
                    daemon_log: paths.daemon_log,
                    db_path: paths.db_path,
                });
            }
        }

        Err(ConfigError::ProjectNotFound(start))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub worktree_dir: PathBuf,
    pub project_dir: PathBuf,
    pub ticket_dir: PathBuf,
    pub max_parallel_tasks: usize,
    pub running_hours: Option<RunningHours>,
    /// Repository-scoped exec-shaped agent adapters. Absent means the
    /// repository has not configured an agent yet; queued work stays queued.
    pub agent: Option<AgentConfig>,
    /// Committed flow definitions plus the built-in `default` when it is not
    /// overridden by a repository file.
    pub flows: BTreeMap<String, Flow>,
    pub default_flow: String,
    /// The single test aftercare stage: an argv run in the worktree after a
    /// successful exit. Absent means the run branch merges without a test
    /// gate; an unchanged branch completes as a no-op.
    pub aftercare_test_cmd: Option<Vec<String>>,
    /// Repository-scoped prefixes for durable IDs stamped into committed
    /// files. These deliberately do not inherit user defaults.
    pub ticket_prefix: String,
    pub project_prefix: String,
    /// How long a ticket row stays stamped missing after its committed file
    /// disappears before reconciliation deletes it.
    pub delete_missing_after_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentConfig {
    pub default_target: String,
    pub targets: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunningHours {
    pub start: String,
    pub end: String,
    start_minute: u16,
    end_minute: u16,
}

impl RunningHours {
    pub fn is_open(&self, local_minute: u16) -> bool {
        if self.start_minute < self.end_minute {
            (self.start_minute..self.end_minute).contains(&local_minute)
        } else {
            local_minute >= self.start_minute || local_minute < self.end_minute
        }
    }

    pub fn next_opening_ms(&self, clock: &dyn crate::clock::Clock, now_ms: i64) -> i64 {
        let mut candidate = (now_ms.div_euclid(60_000) + 1) * 60_000;
        // Evaluate real instants rather than constructing a local wall time:
        // skipped and repeated DST minutes then follow the same `is_open`
        // policy as every spawn decision. Forty-nine hours covers even a
        // skipped local calendar day.
        for _ in 0..=(49 * 60) {
            if self.is_open(clock.local_minute(candidate)) {
                return candidate;
            }
            candidate += 60_000;
        }
        candidate
    }
}

impl Config {
    pub fn load(project: &Project) -> Result<Self, ConfigError> {
        let user_path = user_config_path().filter(|path| path.is_file());
        let user = user_path
            .as_ref()
            .map(|path| read_config(path))
            .transpose()?;
        let repository = read_config(&project.config_path)?;

        let worktree_dir = validate_worktree_dir(
            repository
                .worktree_dir
                .as_deref()
                .unwrap_or_else(|| Path::new(DEFAULT_WORKTREE_DIR)),
            &project.config_path,
        )?;
        let project_dir = validate_repository_dir(
            "project_dir",
            repository
                .project_dir
                .as_deref()
                .unwrap_or_else(|| Path::new(DEFAULT_PROJECT_DIR)),
            &project.config_path,
        )?;
        let ticket_dir = validate_repository_dir(
            "ticket_dir",
            repository
                .ticket_dir
                .as_deref()
                .unwrap_or_else(|| Path::new(DEFAULT_TICKET_DIR)),
            &project.config_path,
        )?;

        if user.as_ref().is_some_and(|config| {
            config.agent.is_some()
                || config
                    .defaults
                    .as_ref()
                    .is_some_and(|defaults| defaults.agent.is_some())
        }) {
            return Err(ConfigError::Invalid {
                path: user_path.expect("loaded user config has a path"),
                message: "agent targets are repository-scoped; configure `agent.default_target` and `agent.targets` in .agents/sloop/config.yaml".into(),
            });
        }

        let defaults = user
            .as_ref()
            .and_then(|config| config.defaults.as_ref())
            .and_then(|defaults| defaults.scheduler.as_ref());
        let repository_scheduler = repository.scheduler.as_ref();

        let max_parallel_tasks = repository_scheduler
            .and_then(|scheduler| scheduler.max_parallel_tasks)
            .or_else(|| defaults.and_then(|scheduler| scheduler.max_parallel_tasks))
            .unwrap_or(1);
        if max_parallel_tasks == 0 {
            return Err(ConfigError::Invalid {
                path: project.config_path.clone(),
                message: "scheduler.max_parallel_tasks must be greater than zero".into(),
            });
        }

        let running_hours = repository_scheduler
            .and_then(|scheduler| scheduler.running_hours.clone())
            .or_else(|| defaults.and_then(|scheduler| scheduler.running_hours.clone()))
            .map(|hours| validate_running_hours(hours, &project.config_path))
            .transpose()?;

        let agent = repository
            .agent
            .as_ref()
            .map(|agent| validate_agent(agent, &project.config_path))
            .transpose()?;

        let aftercare_test_cmd = repository
            .aftercare
            .as_ref()
            .or_else(|| {
                user.as_ref()
                    .and_then(|config| config.defaults.as_ref())
                    .and_then(|defaults| defaults.aftercare.as_ref())
            })
            .and_then(|aftercare| aftercare.test_cmd.clone());
        if let Some(cmd) = &aftercare_test_cmd
            && cmd.is_empty()
        {
            return Err(ConfigError::Invalid {
                path: project.config_path.clone(),
                message: "aftercare.test_cmd must name a command".into(),
            });
        }

        let ticket_prefix = repository
            .ids
            .as_ref()
            .and_then(|ids| ids.ticket_prefix.clone())
            .unwrap_or_else(|| DEFAULT_TICKET_PREFIX.into());
        validate_id_prefix("ids.ticket_prefix", &ticket_prefix, &project.config_path)?;
        let project_prefix = repository
            .ids
            .as_ref()
            .and_then(|ids| ids.project_prefix.clone())
            .unwrap_or_else(|| DEFAULT_PROJECT_PREFIX.into());
        validate_id_prefix("ids.project_prefix", &project_prefix, &project.config_path)?;

        let flows = load_flows(&project.root)?;
        if aftercare_test_cmd.is_some()
            && let Some(flow) = flows
                .values()
                .find(|flow| flow.stages.iter().any(|stage| stage.name == "test"))
        {
            return Err(ConfigError::Invalid {
                path: project.config_path.clone(),
                message: format!(
                    "aftercare.test_cmd conflicts with stage `test` in flow `{}`",
                    flow.name
                ),
            });
        }

        let delete_missing_after_ms = repository
            .delete_missing_after
            .as_deref()
            .map(|value| {
                parse_duration_ms(value).map_err(|message| ConfigError::Invalid {
                    path: project.config_path.clone(),
                    message: format!("delete_missing_after: {message}"),
                })
            })
            .transpose()?
            .unwrap_or(DEFAULT_DELETE_MISSING_AFTER_MS);

        Ok(Self {
            worktree_dir,
            project_dir,
            ticket_dir,
            max_parallel_tasks,
            running_hours,
            agent,
            flows,
            default_flow: DEFAULT_FLOW_NAME.into(),
            aftercare_test_cmd,
            ticket_prefix,
            project_prefix,
            delete_missing_after_ms,
        })
    }
}

fn load_flows(root: &Path) -> Result<BTreeMap<String, Flow>, ConfigError> {
    let mut flows = BTreeMap::from([(DEFAULT_FLOW_NAME.into(), crate::flow::built_in_default())]);
    let directory = root.join(".agents/sloop/flows");
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(flows),
        Err(source) => {
            return Err(ConfigError::Io {
                path: directory,
                source,
            });
        }
    };

    let mut paths = entries
        .map(|entry| {
            entry
                .map(|entry| entry.path())
                .map_err(|source| ConfigError::Io {
                    path: directory.clone(),
                    source,
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    paths.retain(|path| path.is_file() && path.extension().is_some_and(|ext| ext == "yaml"));
    paths.sort();

    for path in paths {
        let name = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or_else(|| ConfigError::Invalid {
                path: path.clone(),
                message: "flow filename must be valid UTF-8".into(),
            })?;
        let contents = fs::read_to_string(&path).map_err(|source| ConfigError::Io {
            path: path.clone(),
            source,
        })?;
        let flow = crate::flow::parse(name, &contents).map_err(|message| ConfigError::Invalid {
            path: path.clone(),
            message,
        })?;
        flows.insert(name.into(), flow);
    }
    Ok(flows)
}

fn validate_worktree_dir(value: &Path, path: &Path) -> Result<PathBuf, ConfigError> {
    validate_repository_dir("worktree_dir", value, path)
}

fn validate_repository_dir(key: &str, value: &Path, path: &Path) -> Result<PathBuf, ConfigError> {
    use std::path::Component;

    if value.is_absolute() {
        return Err(ConfigError::Invalid {
            path: path.to_path_buf(),
            message: format!("{key} must be repository-relative, not an absolute path"),
        });
    }

    let mut normalized = PathBuf::new();
    for component in value.components() {
        match component {
            Component::Normal(component) => normalized.push(component),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    return Err(ConfigError::Invalid {
                        path: path.to_path_buf(),
                        message: format!("{key} must not escape the repository root with `..`"),
                    });
                }
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(ConfigError::Invalid {
                    path: path.to_path_buf(),
                    message: format!("{key} must be repository-relative, not an absolute path"),
                });
            }
        }
    }
    if normalized.as_os_str().is_empty() {
        return Err(ConfigError::Invalid {
            path: path.to_path_buf(),
            message: format!("{key} must name a directory below the repository root"),
        });
    }
    Ok(normalized)
}

fn validate_agent(agent: &RawAgent, path: &Path) -> Result<AgentConfig, ConfigError> {
    if agent.cmd.is_some() {
        return Err(ConfigError::Invalid {
            path: path.to_path_buf(),
            message: "agent.cmd has been removed; use `agent.default_target` and `agent.targets`"
                .into(),
        });
    }
    let default_target = agent
        .default_target
        .as_ref()
        .ok_or_else(|| ConfigError::Invalid {
            path: path.to_path_buf(),
            message: "agent.default_target is required".into(),
        })?;
    let targets = agent.targets.as_ref().ok_or_else(|| ConfigError::Invalid {
        path: path.to_path_buf(),
        message: "agent.targets is required".into(),
    })?;
    if !targets.contains_key(default_target) {
        return Err(ConfigError::Invalid {
            path: path.to_path_buf(),
            message: format!(
                "agent.default_target `{default_target}` does not name an entry in agent.targets"
            ),
        });
    }

    let mut commands = BTreeMap::new();
    for (name, target) in targets {
        if target.cmd.is_empty() {
            return Err(ConfigError::Invalid {
                path: path.to_path_buf(),
                message: format!("agent.targets.{name}.cmd must name a command"),
            });
        }
        let prompt_count = target
            .cmd
            .iter()
            .map(|argument| argument.matches("{prompt}").count())
            .sum::<usize>();
        if prompt_count != 1 {
            return Err(ConfigError::Invalid {
                path: path.to_path_buf(),
                message: format!(
                    "agent.targets.{name}.cmd must contain `{{prompt}}` exactly once (found {prompt_count})"
                ),
            });
        }
        commands.insert(name.clone(), target.cmd.clone());
    }
    Ok(AgentConfig {
        default_target: default_target.clone(),
        targets: commands,
    })
}

pub(crate) fn expand_agent_cmd(
    template: &[String],
    model: Option<&str>,
    effort: Option<&str>,
    prompt: &str,
) -> Result<Vec<String>, String> {
    template
        .iter()
        .map(|argument| {
            let argument = match (argument.contains("{model}"), model) {
                (true, Some(model)) => argument.replace("{model}", model),
                (true, None) => return Err("does not specify `model`".to_owned()),
                (false, _) => argument.clone(),
            };
            let argument = match (argument.contains("{effort}"), effort) {
                (true, Some(effort)) => argument.replace("{effort}", effort),
                (true, None) => return Err("does not specify `effort`".to_owned()),
                (false, _) => argument,
            };
            Ok(argument.replace("{prompt}", prompt))
        })
        .collect()
}

fn validate_id_prefix(key: &str, prefix: &str, path: &Path) -> Result<(), ConfigError> {
    if valid_prefix(prefix) {
        return Ok(());
    }
    Err(ConfigError::Invalid {
        path: path.to_path_buf(),
        message: format!(
            "{key} must be non-empty and contain only ASCII letters, digits, `-`, or `_`, with a letter or digit at each end"
        ),
    })
}

fn user_config_path() -> Option<PathBuf> {
    env::var_os("HOME").map(|home| PathBuf::from(home).join(".config/sloop/config.yaml"))
}

fn read_config(path: &Path) -> Result<RawConfig, ConfigError> {
    let contents = fs::read_to_string(path).map_err(|source| ConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let config: RawConfig =
        serde_yaml::from_str(&contents).map_err(|source| ConfigError::Invalid {
            path: path.to_path_buf(),
            message: source.to_string(),
        })?;
    if config.version != CONFIG_VERSION {
        return Err(ConfigError::UnsupportedVersion {
            path: path.to_path_buf(),
            version: config.version,
        });
    }
    Ok(config)
}

fn validate_running_hours(
    hours: RawRunningHours,
    path: &Path,
) -> Result<RunningHours, ConfigError> {
    let start_minute = parse_local_time(&hours.start);
    let end_minute = parse_local_time(&hours.end);
    let (Some(start_minute), Some(end_minute)) = (start_minute, end_minute) else {
        return Err(ConfigError::Invalid {
            path: path.to_path_buf(),
            message: "scheduler.running_hours values must use a valid HH:MM time".into(),
        });
    };
    if start_minute == end_minute {
        return Err(ConfigError::Invalid {
            path: path.to_path_buf(),
            message: "scheduler.running_hours start and end must differ".into(),
        });
    }
    Ok(RunningHours {
        start: hours.start,
        end: hours.end,
        start_minute,
        end_minute,
    })
}

pub(crate) fn parse_local_time(value: &str) -> Option<u16> {
    let (hour, minute) = value.split_once(':')?;
    if hour.len() != 2 || minute.len() != 2 {
        return None;
    }
    if !hour.bytes().all(|byte| byte.is_ascii_digit())
        || !minute.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let hour = hour.parse::<u16>().ok()?;
    let minute = minute.parse::<u16>().ok()?;
    (hour < 24 && minute < 60).then_some(hour * 60 + minute)
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    version: u32,
    worktree_dir: Option<PathBuf>,
    project_dir: Option<PathBuf>,
    ticket_dir: Option<PathBuf>,
    defaults: Option<RawDefaults>,
    scheduler: Option<RawScheduler>,
    agent: Option<RawAgent>,
    aftercare: Option<RawAftercare>,
    ids: Option<RawIds>,
    delete_missing_after: Option<String>,
}

/// Parses durations like `45s`, `30m`, `12h`, `30d`, or `2w` into
/// milliseconds.
fn parse_duration_ms(value: &str) -> Result<i64, String> {
    let value = value.trim();
    let (digits, unit) = value.split_at(value.len().saturating_sub(1));
    let scale: i64 = match unit {
        "s" => 1000,
        "m" => 60 * 1000,
        "h" => 60 * 60 * 1000,
        "d" => 24 * 60 * 60 * 1000,
        "w" => 7 * 24 * 60 * 60 * 1000,
        _ => {
            return Err(format!(
                "`{value}` must look like 30d, 12h, 30m, 45s, or 2w"
            ));
        }
    };
    let count: i64 = digits
        .parse()
        .map_err(|_| format!("`{value}` must look like 30d, 12h, 30m, 45s, or 2w"))?;
    if count <= 0 {
        return Err(format!("`{value}` must be a positive duration"));
    }
    count
        .checked_mul(scale)
        .ok_or_else(|| format!("`{value}` is too large"))
}

#[derive(Debug, Deserialize)]
struct RawIds {
    ticket_prefix: Option<String>,
    project_prefix: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawDefaults {
    scheduler: Option<RawScheduler>,
    agent: Option<RawAgent>,
    aftercare: Option<RawAftercare>,
}

#[derive(Debug, Deserialize)]
struct RawAftercare {
    test_cmd: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct RawAgent {
    default_target: Option<String>,
    targets: Option<BTreeMap<String, RawAgentTarget>>,
    cmd: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct RawAgentTarget {
    cmd: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RawScheduler {
    max_parallel_tasks: Option<usize>,
    running_hours: Option<RawRunningHours>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawRunningHours {
    start: String,
    end: String,
}

#[derive(Debug)]
pub enum ConfigError {
    ProjectNotFound(PathBuf),
    Paths(crate::paths::PathError),
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    Invalid {
        path: PathBuf,
        message: String,
    },
    UnsupportedVersion {
        path: PathBuf,
        version: u32,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProjectNotFound(start) => write!(
                formatter,
                "no .agents/sloop/config.yaml found from {}",
                start.display()
            ),
            Self::Paths(error) => write!(formatter, "cannot resolve Sloop runtime paths: {error}"),
            Self::Io { path, source } => write!(formatter, "{}: {source}", path.display()),
            Self::Invalid { path, message } => write!(formatter, "{}: {message}", path.display()),
            Self::UnsupportedVersion { path, version } => write!(
                formatter,
                "{}: unsupported config version {version}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::future::Future;
    use std::path::PathBuf;
    use std::pin::Pin;

    use tempfile::tempdir;

    use super::{Config, ConfigError, Project, RunningHours};
    use crate::clock::Clock;

    struct SpringForwardClock;

    impl Clock for SpringForwardClock {
        fn now_ms(&self) -> i64 {
            30_000
        }

        fn local_minute(&self, timestamp_ms: i64) -> u16 {
            if timestamp_ms < 60_000 { 119 } else { 180 }
        }

        fn sleep_until(&self, _deadline_ms: i64) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(std::future::pending())
        }
    }

    #[test]
    fn discovers_the_nearest_project_from_a_nested_directory() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\n",
        )
        .unwrap();
        let nested = root.path().join("src/deep");
        fs::create_dir_all(&nested).unwrap();

        let project = Project::discover(&nested).unwrap();
        assert_eq!(project.root, root.path().canonicalize().unwrap());
    }

    #[test]
    fn repository_scheduler_values_are_loaded() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            concat!(
                "version: 1\n",
                "scheduler:\n",
                "  max_parallel_tasks: 3\n",
                "  running_hours:\n",
                "    start: '22:00'\n",
                "    end: '06:00'\n"
            ),
        )
        .unwrap();

        let project = Project::discover(root.path()).unwrap();
        let config = Config::load(&project).unwrap();
        assert_eq!(config.max_parallel_tasks, 3);
        assert_eq!(config.running_hours.unwrap().start, "22:00");
    }

    #[test]
    fn worktree_dir_defaults_to_dot_worktrees() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\n",
        )
        .unwrap();

        let project = Project::discover(root.path()).unwrap();
        assert_eq!(
            Config::load(&project).unwrap().worktree_dir,
            PathBuf::from(".worktrees")
        );
    }

    #[test]
    fn content_directories_default_to_the_sloop_layout() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\n",
        )
        .unwrap();

        let project = Project::discover(root.path()).unwrap();
        let config = Config::load(&project).unwrap();
        assert_eq!(config.project_dir, PathBuf::from(".agents/sloop/projects"));
        assert_eq!(config.ticket_dir, PathBuf::from(".agents/sloop/tickets"));
    }

    #[test]
    fn content_directories_load_repository_relative_paths() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\nproject_dir: planning/projects\nticket_dir: planning/tickets\n",
        )
        .unwrap();

        let project = Project::discover(root.path()).unwrap();
        let config = Config::load(&project).unwrap();
        assert_eq!(config.project_dir, PathBuf::from("planning/projects"));
        assert_eq!(config.ticket_dir, PathBuf::from("planning/tickets"));
    }

    #[test]
    fn content_directories_must_stay_below_the_repository_root() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\nproject_dir: /tmp/projects\nticket_dir: ../tickets\n",
        )
        .unwrap();

        let project = Project::discover(root.path()).unwrap();
        let error = Config::load(&project).unwrap_err().to_string();
        assert!(error.contains("project_dir"), "{error}");
        assert!(error.contains("repository-relative"), "{error}");

        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\nproject_dir: planning/projects\nticket_dir: ../tickets\n",
        )
        .unwrap();
        let error = Config::load(&project).unwrap_err().to_string();
        assert!(error.contains("ticket_dir"), "{error}");
        assert!(error.contains("repository root"), "{error}");
    }

    #[test]
    fn worktree_dir_loads_a_repository_relative_path() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\nworktree_dir: build/agent-worktrees\n",
        )
        .unwrap();

        let project = Project::discover(root.path()).unwrap();
        assert_eq!(
            Config::load(&project).unwrap().worktree_dir,
            PathBuf::from("build/agent-worktrees")
        );
    }

    #[test]
    fn worktree_dir_rejects_an_absolute_path() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\nworktree_dir: /tmp/sloop-worktrees\n",
        )
        .unwrap();

        let project = Project::discover(root.path()).unwrap();
        let error = Config::load(&project).unwrap_err().to_string();
        assert!(error.contains("worktree_dir"), "{error}");
        assert!(error.contains("repository-relative"), "{error}");
    }

    #[test]
    fn worktree_dir_rejects_parent_traversal_outside_the_repository() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\nworktree_dir: ../sloop-worktrees\n",
        )
        .unwrap();

        let project = Project::discover(root.path()).unwrap();
        let error = Config::load(&project).unwrap_err().to_string();
        assert!(error.contains("worktree_dir"), "{error}");
        assert!(error.contains("repository root"), "{error}");
    }

    #[test]
    fn repository_agent_loads_multiple_named_targets() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            concat!(
                "version: 1\n",
                "agent:\n",
                "  default_target: claude\n",
                "  targets:\n",
                "    claude:\n",
                "      cmd: [claude, --model, '{model}', '{prompt}']\n",
                "    codex:\n",
                "      cmd: [codex, exec, --model, '{model}', '{prompt}']\n",
            ),
        )
        .unwrap();

        let project = Project::discover(root.path()).unwrap();
        let agent = Config::load(&project).unwrap().agent.unwrap();
        assert_eq!(agent.default_target, "claude");
        assert_eq!(agent.targets.len(), 2);
        assert_eq!(agent.targets["codex"][0], "codex");
    }

    #[test]
    fn agent_default_target_must_name_a_configured_target() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\nagent:\n  default_target: missing\n  targets:\n    fake:\n      cmd: [fake]\n",
        )
        .unwrap();

        let project = Project::discover(root.path()).unwrap();
        let error = Config::load(&project).unwrap_err().to_string();
        assert!(error.contains("agent.default_target `missing`"), "{error}");
        assert!(error.contains("agent.targets"), "{error}");
    }

    #[test]
    fn every_agent_target_command_must_be_nonempty() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\nagent:\n  default_target: fake\n  targets:\n    fake:\n      cmd: []\n",
        )
        .unwrap();

        let project = Project::discover(root.path()).unwrap();
        let error = Config::load(&project).unwrap_err().to_string();
        assert!(error.contains("agent.targets.fake.cmd"), "{error}");
        assert!(error.contains("must name a command"), "{error}");
    }

    #[test]
    fn every_agent_target_requires_prompt_exactly_once() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        let config_path = root.path().join(".agents/sloop/config.yaml");
        fs::write(
            &config_path,
            "version: 1\nagent:\n  default_target: missing_prompt\n  targets:\n    missing_prompt:\n      cmd: [agent]\n",
        )
        .unwrap();

        let project = Project::discover(root.path()).unwrap();
        let error = Config::load(&project).unwrap_err().to_string();
        assert!(
            error.contains("agent.targets.missing_prompt.cmd"),
            "{error}"
        );
        assert!(error.contains("`{prompt}` exactly once"), "{error}");
        assert!(error.contains("found 0"), "{error}");

        fs::write(
            config_path,
            "version: 1\nagent:\n  default_target: duplicate_prompt\n  targets:\n    duplicate_prompt:\n      cmd: [agent, '{prompt}', 'again={prompt}']\n",
        )
        .unwrap();
        let error = Config::load(&project).unwrap_err().to_string();
        assert!(
            error.contains("agent.targets.duplicate_prompt.cmd"),
            "{error}"
        );
        assert!(error.contains("`{prompt}` exactly once"), "{error}");
        assert!(error.contains("found 2"), "{error}");
    }

    #[test]
    fn legacy_singular_agent_command_names_the_new_shape() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\nagent:\n  cmd: [fake]\n",
        )
        .unwrap();

        let project = Project::discover(root.path()).unwrap();
        let error = Config::load(&project).unwrap_err().to_string();
        assert!(error.contains("agent.cmd has been removed"), "{error}");
        assert!(error.contains("agent.default_target"), "{error}");
        assert!(error.contains("agent.targets"), "{error}");
    }

    #[test]
    fn id_prefixes_default_and_load_from_repository_configuration() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\n",
        )
        .unwrap();
        let project = Project::discover(root.path()).unwrap();
        let defaults = Config::load(&project).unwrap();
        assert_eq!(defaults.ticket_prefix, "TICK");
        assert_eq!(defaults.project_prefix, "PROJ");

        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\nids:\n  ticket_prefix: WORK\n  project_prefix: TEAM\n",
        )
        .unwrap();
        let configured = Config::load(&project).unwrap();
        assert_eq!(configured.ticket_prefix, "WORK");
        assert_eq!(configured.project_prefix, "TEAM");
    }

    #[test]
    fn invalid_id_prefixes_are_clear_configuration_errors() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\nids:\n  ticket_prefix: ''\n",
        )
        .unwrap();

        let project = Project::discover(root.path()).unwrap();
        let error = Config::load(&project).unwrap_err().to_string();
        assert!(error.contains("ids.ticket_prefix"), "{error}");
        assert!(error.contains("non-empty"), "{error}");
    }

    #[test]
    fn running_hours_are_start_inclusive_and_end_exclusive() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\nscheduler:\n  running_hours:\n    start: '09:00'\n    end: '17:00'\n",
        )
        .unwrap();

        let project = Project::discover(root.path()).unwrap();
        let hours = Config::load(&project).unwrap().running_hours.unwrap();
        assert!(!hours.is_open(8 * 60 + 59));
        assert!(hours.is_open(9 * 60));
        assert!(hours.is_open(16 * 60 + 59));
        assert!(!hours.is_open(17 * 60));
    }

    #[test]
    fn running_hours_may_cross_midnight() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\nscheduler:\n  running_hours:\n    start: '22:00'\n    end: '06:00'\n",
        )
        .unwrap();

        let project = Project::discover(root.path()).unwrap();
        let hours = Config::load(&project).unwrap().running_hours.unwrap();
        assert!(hours.is_open(23 * 60));
        assert!(hours.is_open(5 * 60 + 59));
        assert!(!hours.is_open(6 * 60));
        assert!(!hours.is_open(12 * 60));
    }

    #[test]
    fn equal_running_hour_boundaries_are_rejected() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\nscheduler:\n  running_hours:\n    start: '09:00'\n    end: '09:00'\n",
        )
        .unwrap();

        let project = Project::discover(root.path()).unwrap();
        assert!(matches!(
            Config::load(&project),
            Err(ConfigError::Invalid { .. })
        ));
    }

    #[test]
    fn next_opening_uses_the_first_open_instant_after_a_dst_skip() {
        let hours = RunningHours {
            start: "02:30".into(),
            end: "04:00".into(),
            start_minute: 150,
            end_minute: 240,
        };

        assert_eq!(
            hours.next_opening_ms(&SpringForwardClock, SpringForwardClock.now_ms()),
            60_000
        );
    }

    #[test]
    fn unsupported_versions_are_rejected() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 2\n",
        )
        .unwrap();

        let project = Project::discover(root.path()).unwrap();
        assert!(matches!(
            Config::load(&project),
            Err(ConfigError::UnsupportedVersion { version: 2, .. })
        ));
    }

    #[test]
    fn durations_parse_with_single_letter_units() {
        use super::parse_duration_ms;
        assert_eq!(parse_duration_ms("45s").unwrap(), 45_000);
        assert_eq!(parse_duration_ms("30m").unwrap(), 1_800_000);
        assert_eq!(parse_duration_ms("12h").unwrap(), 43_200_000);
        assert_eq!(parse_duration_ms("30d").unwrap(), 2_592_000_000);
        assert_eq!(parse_duration_ms("2w").unwrap(), 1_209_600_000);
        for invalid in ["", "30", "d", "0d", "-1d", "1.5h", "30 days"] {
            assert!(parse_duration_ms(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn delete_missing_after_is_configurable_and_defaults_to_a_month() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        let config_path = root.path().join(".agents/sloop/config.yaml");

        fs::write(&config_path, "version: 1\n").unwrap();
        let project = Project::discover(root.path()).unwrap();
        let config = Config::load(&project).unwrap();
        assert_eq!(
            config.delete_missing_after_ms,
            super::DEFAULT_DELETE_MISSING_AFTER_MS
        );

        fs::write(&config_path, "version: 1\ndelete_missing_after: 7d\n").unwrap();
        let config = Config::load(&project).unwrap();
        assert_eq!(config.delete_missing_after_ms, 7 * 24 * 60 * 60 * 1000);

        fs::write(&config_path, "version: 1\ndelete_missing_after: soon\n").unwrap();
        let error = Config::load(&project).unwrap_err();
        assert!(
            error.to_string().contains("delete_missing_after"),
            "{error}"
        );
    }

    #[test]
    fn built_in_default_flow_does_not_reuse_agent_placeholders() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\nagent:\n  default_target: reviewer\n  targets:\n    reviewer:\n      cmd: [review-agent, --model, '{model}', --effort, '{effort}', '{prompt}']\n",
        )
        .unwrap();

        let project = Project::discover(root.path()).unwrap();
        let config = Config::load(&project).unwrap();
        assert_eq!(config.default_flow, "default");
        assert_eq!(
            config.flows["default"]
                .stages
                .iter()
                .map(|stage| stage.name.as_str())
                .collect::<Vec<_>>(),
            ["build", "merge"]
        );
    }

    #[test]
    fn aftercare_test_command_is_not_duplicated_in_the_built_in_default_flow() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\naftercare:\n  test_cmd: [cargo, test]\n",
        )
        .unwrap();

        let project = Project::discover(root.path()).unwrap();
        let config = Config::load(&project).unwrap();
        let flow = &config.flows["default"];
        assert_eq!(
            flow.stages
                .iter()
                .map(|stage| stage.name.as_str())
                .collect::<Vec<_>>(),
            ["build", "merge"]
        );
    }

    #[test]
    fn committed_default_flow_overrides_the_built_in_flow() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop/flows")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\naftercare:\n  test_cmd: [cargo, test]\n",
        )
        .unwrap();
        fs::write(
            root.path().join(".agents/sloop/flows/default.yaml"),
            "- { name: build, kind: build }\n- { name: ship, kind: exec, cmd: [ship] }\n",
        )
        .unwrap();

        let project = Project::discover(root.path()).unwrap();
        let config = Config::load(&project).unwrap();
        assert_eq!(
            config.flows["default"]
                .stages
                .iter()
                .map(|stage| stage.name.as_str())
                .collect::<Vec<_>>(),
            ["build", "ship"]
        );
    }

    #[test]
    fn invalid_flow_error_names_the_file_and_problem() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop/flows")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/config.yaml"),
            "version: 1\n",
        )
        .unwrap();
        fs::write(
            root.path().join(".agents/sloop/flows/broken.yaml"),
            "- { name: build, kind: build }\n- { name: check, kind: exec, cmd: [] }\n",
        )
        .unwrap();

        let project = Project::discover(root.path()).unwrap();
        let error = Config::load(&project).unwrap_err().to_string();
        assert!(error.contains("broken.yaml"), "{error}");
        assert!(error.contains("non-empty `cmd`"), "{error}");
    }
}
