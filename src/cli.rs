use std::ffi::OsString;
use std::fmt;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;

use clap::error::{ContextKind, ContextValue, ErrorKind};
use clap::{
    ArgGroup, Args, ColorChoice, CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum,
};
use serde_json::json;

use crate::protocol::{
    EmptyArgs, ErrorBody, ErrorCode, EventsArgs, NoteArgs, PostActivation, PostArgs, Request,
    RequestEnvelope, RequestId, ResponseEnvelope, RunActivation, RunArgs, RunReferenceArgs,
    ShowArgs, StopArgs, TicketReferenceArgs, VerdictArgs, VerdictValue,
};
use crate::templates::TemplateKind;

const TICKET_STATES_HELP: &str = "Ticket states:
  ready         Eligible for dispatch once a run is queued and gates are open.
  held          Prevented from running by an operator; release with `sloop ready`.
  blocked       Waiting for every ticket in `blocked_by` to be merged.
  claimed       Owned by an active run, including aftercare or recovery.
  merged        Terminal: completed work was integrated into the default branch.
  failed        Terminal: the agent exited unsuccessfully; requeue with `sloop retry`.
  needs_review  Terminal: aftercare could not merge the run; inspect manually.";

#[derive(Debug, Parser)]
#[command(
    name = "sloop",
    version,
    about = "Schedule coding agents",
    color = ColorChoice::Never
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
    /// Emit JSON envelopes instead of human-readable output.
    #[arg(long, global = true)]
    pub json: bool,
}

/// How responses are written. Envelopes are always produced internally;
/// `Human` translates them at the final write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputMode {
    Json,
    Human,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Scaffold a Sloop project.
    Init,
    /// Print a commented canonical template for a file you author.
    ///
    /// Static compiled-in content: no daemon is contacted or started, and
    /// nothing is written. Redirect it where you want the file, for example
    /// `sloop template ticket > .agents/sloop/tickets/my-ticket.md`.
    Template {
        /// The file kind to print.
        #[arg(value_name = "KIND")]
        kind: TemplateKind,
    },
    /// Ensure the daemon is running.
    Daemon(DaemonCliArgs),
    /// Register a ticket file.
    Post(PostCliArgs),
    /// Enqueue a run.
    #[command(hide = true)]
    Run(RunCliArgs),
    /// Make a failed ticket ready to run again.
    #[command(hide = true)]
    Retry { ticket: String },
    /// Prevent a ready ticket from being dispatched.
    #[command(hide = true)]
    Hold { ticket: String },
    /// Release a held ticket for dispatch.
    #[command(hide = true)]
    Ready { ticket: String },
    /// List ticket names, states, and why they are not running.
    List,
    /// Show daemon state.
    Status,
    /// Stop spawning new agents.
    #[command(hide = true)]
    Pause,
    /// Resume spawning agents.
    #[command(hide = true)]
    Resume,
    /// Stop the daemon.
    #[command(hide = true)]
    Stop {
        /// Cancel active runs instead of refusing to stop.
        #[arg(long)]
        force: bool,
    },
    /// Cancel a run and preserve its worktree.
    #[command(hide = true)]
    Cancel {
        /// Run alias, ticket reference, or run-id prefix.
        run: String,
    },
    /// Show output from a run.
    #[command(hide = true)]
    Logs {
        /// Run alias, ticket reference, or run-id prefix.
        run: String,
    },
    /// Follow ticket and run activity as it happens.
    ///
    /// With no reference every event in the repository is streamed. With one,
    /// only events belonging to that scope are: a ticket covers the ticket and
    /// all of its runs, a project covers its tickets and their runs, and a run
    /// covers just that run. Repository-wide events, such as a daemon drain,
    /// belong to no scope and are streamed only by a bare `sloop watch`. An
    /// unknown reference fails immediately rather than streaming nothing.
    Watch {
        /// Ticket id or name, run alias or id prefix, or project id to scope to.
        r#ref: Option<String>,
        /// Number of recent events to show before following.
        #[arg(long, default_value_t = 20)]
        tail: u32,
    },
    /// Block until a run reaches a terminal state.
    #[command(hide = true)]
    Wait {
        /// Run alias, ticket reference, or run-id prefix.
        run: String,
        /// Give up after this many seconds.
        #[arg(long, default_value_t = 3600)]
        timeout: u64,
    },
    /// Rebuild local state from committed files and Git.
    #[command(hide = true)]
    Reindex,
    /// Show the current worker's assignment.
    Brief,
    /// Show a ticket, run, or project by reference.
    Show { r#ref: String },
    /// Append an advisory note to the current run.
    #[command(hide = true)]
    Note {
        #[arg(required = true, trailing_var_arg = true)]
        text: Vec<String>,
    },
    /// Report the current stage's verdict.
    #[command(hide = true)]
    Verdict {
        verdict: VerdictCliValue,
        #[arg(long)]
        reason: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum VerdictCliValue {
    Pass,
    Fail,
}

#[derive(Debug, Args)]
pub struct DaemonCliArgs {
    #[command(subcommand)]
    action: Option<DaemonAction>,
    #[arg(long, hide = true)]
    foreground: bool,
}

#[derive(Debug, Subcommand)]
enum DaemonAction {
    /// Drain active runs and restart with the binary currently installed.
    Restart,
}

#[derive(Debug, Args)]
#[command(
    after_help = "Ticket files need `name`, `blocked_by`, and a non-empty body. Run \
`sloop template ticket` for a commented example of every frontmatter field, or \
`sloop template flow` for the flow grammar that `--flow` selects.",
    group(
        ArgGroup::new("activation")
            .args(["auto", "at", "manual", "hold"])
            .multiple(false)
    )
)]
pub struct PostCliArgs {
    /// Markdown ticket to register.
    file: PathBuf,
    /// Project receiving the ticket; defaults to `default`.
    #[arg(long, value_name = "PROJECT")]
    project: Option<String>,
    /// Flow the ticket binds to; defaults to the repository's default flow.
    #[arg(long, value_name = "FLOW")]
    flow: Option<String>,
    /// Queue one run for the next available opportunity (default).
    #[arg(long)]
    auto: bool,
    /// Queue one run for the next occurrence of a local time.
    #[arg(long, value_name = "TIME")]
    at: Option<LocalTime>,
    /// Register the ticket without creating a run.
    #[arg(long)]
    manual: bool,
    /// Register the ticket as held without creating a run.
    #[arg(long)]
    hold: bool,
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("activation")
        .args(["at", "every", "overnight"])
        .multiple(false)
))]
pub struct RunCliArgs {
    /// Run a specific ticket instead of selecting ready work.
    ticket: Option<String>,
    /// Select ready work from one project.
    #[arg(long, value_name = "PROJECT", conflicts_with = "ticket")]
    project: Option<String>,
    /// Start at a local time, such as 03:00.
    #[arg(long, value_name = "TIME")]
    at: Option<LocalTime>,
    /// Recur at an interval, such as 30m.
    #[arg(long, value_name = "DURATION")]
    every: Option<DurationMs>,
    /// Run according to the configured overnight window.
    #[arg(long)]
    overnight: bool,
    /// Restrict selection to the comma-separated ticket IDs.
    #[arg(long, value_delimiter = ',', value_name = "TICKETS")]
    only: Option<Vec<String>>,
}

impl Cli {
    pub fn into_request(self) -> Result<Request, RequestConstructionError> {
        self.command.try_into()
    }
}

impl TryFrom<Command> for Request {
    type Error = RequestConstructionError;

    fn try_from(command: Command) -> Result<Self, Self::Error> {
        let empty = EmptyArgs::default;
        Ok(match command {
            Command::Init => Self::Init(empty()),
            // `template` is answered entirely from compiled-in content, so it
            // has no protocol verb and must never reach the daemon path.
            Command::Template { .. } => {
                return Err(RequestConstructionError(
                    "template is printed locally and has no daemon request".into(),
                ));
            }
            Command::Daemon(args) => match args.action {
                Some(DaemonAction::Restart) => Self::Restart(empty()),
                None => Self::Daemon(empty()),
            },
            Command::Post(args) => Self::Post(args.try_into()?),
            Command::Run(args) => Self::Run(args.into()),
            Command::Retry { ticket } => Self::Retry(TicketReferenceArgs { ticket }),
            Command::Hold { ticket } => Self::Hold(TicketReferenceArgs { ticket }),
            Command::Ready { ticket } => Self::Ready(TicketReferenceArgs { ticket }),
            Command::List => Self::List(empty()),
            Command::Status => Self::Status(empty()),
            Command::Pause => Self::Pause(empty()),
            Command::Resume => Self::Resume(empty()),
            Command::Stop { force } => Self::Stop(StopArgs { force }),
            Command::Cancel { run } => Self::Cancel(RunReferenceArgs { run }),
            Command::Logs { run } => Self::Logs(RunReferenceArgs { run }),
            Command::Watch { r#ref, tail } => Self::Events(EventsArgs {
                after: None,
                tail: Some(tail),
                limit: None,
                scope: r#ref,
            }),
            Command::Wait { run, .. } => Self::Wait(RunReferenceArgs { run }),
            Command::Reindex => Self::Reindex(empty()),
            Command::Brief => Self::Brief(empty()),
            Command::Show { r#ref } => Self::Show(ShowArgs { reference: r#ref }),
            Command::Note { text } => Self::Note(NoteArgs {
                text: text.join(" "),
            }),
            Command::Verdict { verdict, reason } => Self::Verdict(VerdictArgs {
                verdict: match verdict {
                    VerdictCliValue::Pass => VerdictValue::Pass,
                    VerdictCliValue::Fail => VerdictValue::Fail,
                },
                reason,
            }),
        })
    }
}

impl TryFrom<PostCliArgs> for PostArgs {
    type Error = RequestConstructionError;

    fn try_from(args: PostCliArgs) -> Result<Self, Self::Error> {
        let file = args
            .file
            .into_os_string()
            .into_string()
            .map_err(|_| RequestConstructionError("ticket path must be valid UTF-8".into()))?;
        let activation = if let Some(time) = args.at {
            PostActivation::At { time: time.0 }
        } else if args.manual {
            PostActivation::Manual
        } else if args.hold {
            PostActivation::Hold
        } else {
            PostActivation::Auto
        };

        Ok(Self {
            file,
            project: args.project,
            flow: args.flow,
            activation,
        })
    }
}

impl From<RunCliArgs> for RunArgs {
    fn from(args: RunCliArgs) -> Self {
        let activation = if let Some(time) = args.at {
            RunActivation::At { local_time: time.0 }
        } else if let Some(interval) = args.every {
            RunActivation::Every {
                interval_ms: interval.0,
            }
        } else if args.overnight {
            RunActivation::Overnight
        } else {
            RunActivation::Now
        };

        Self {
            ticket: args.ticket,
            project: args.project,
            activation,
            only: args.only.unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestConstructionError(String);

impl fmt::Display for RequestConstructionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for RequestConstructionError {}

#[derive(Debug, Clone)]
struct LocalTime(String);

impl FromStr for LocalTime {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (hour, minute) = value
            .split_once(':')
            .ok_or_else(|| "time must use HH:MM".to_owned())?;
        if hour.len() != 2 || minute.len() != 2 {
            return Err("time must use HH:MM".into());
        }
        let hour: u8 = hour.parse().map_err(|_| "hour must be numeric")?;
        let minute: u8 = minute.parse().map_err(|_| "minute must be numeric")?;
        if hour > 23 || minute > 59 {
            return Err("time must be between 00:00 and 23:59".into());
        }
        Ok(Self(value.to_owned()))
    }
}

#[derive(Debug, Clone, Copy)]
struct DurationMs(u64);

impl FromStr for DurationMs {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let digits = value.chars().take_while(char::is_ascii_digit).count();
        let (amount, unit) = value.split_at(digits);
        if amount.is_empty() || unit.is_empty() {
            return Err("duration must include a positive number and unit (ms, s, m, or h)".into());
        }
        let amount: u64 = amount
            .parse()
            .map_err(|_| "duration amount is too large".to_owned())?;
        if amount == 0 {
            return Err("duration must be greater than zero".into());
        }
        let multiplier = match unit {
            "ms" => 1,
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            _ => return Err("duration unit must be ms, s, m, or h".into()),
        };
        amount
            .checked_mul(multiplier)
            .map(Self)
            .ok_or_else(|| "duration is too large".into())
    }
}

pub fn run<I, T, O, E>(args: I, stdout: &mut O, stderr: &mut E) -> ExitCode
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
    O: Write,
    E: Write,
{
    let mut args: Vec<OsString> = args.into_iter().map(Into::into).collect();
    let expanded_help = args.iter().any(|arg| arg == "--all")
        && args
            .iter()
            .any(|arg| arg == "--help" || arg == "-h" || arg == "help");
    if expanded_help {
        args.retain(|arg| arg != "--all");
    }

    let mut command = Cli::command();
    if expanded_help {
        let subcommands: Vec<String> = command
            .get_subcommands()
            .map(|subcommand| subcommand.get_name().to_owned())
            .collect();
        for subcommand in subcommands {
            command = command.mut_subcommand(subcommand, |subcommand| subcommand.hide(false));
        }
        command = command.after_help(TICKET_STATES_HELP);
    } else {
        command = command.after_help("Run `sloop --help --all` to see every command.");
    }

    match command
        .try_get_matches_from(&args)
        .and_then(|matches| Cli::from_arg_matches(&matches))
    {
        Ok(cli) => {
            let mode = if cli.json {
                OutputMode::Json
            } else {
                OutputMode::Human
            };
            run_command(cli.command, mode, stdout, stderr)
        }
        // Parsing failed, so the flag is read from the raw arguments: an
        // agent asking for `--json --help` still gets an envelope.
        Err(error) => {
            let mode = if args.iter().any(|arg| arg == "--json") {
                OutputMode::Json
            } else {
                OutputMode::Human
            };
            match error.kind() {
                ErrorKind::DisplayHelp => write_plain_or(
                    mode,
                    stdout,
                    error.to_string().trim_end(),
                    &ResponseEnvelope::success(
                        None,
                        json!({"kind": "help", "text": error.to_string().trim_end()}),
                    ),
                ),
                ErrorKind::DisplayVersion => write_plain_or(
                    mode,
                    stdout,
                    concat!("sloop ", env!("CARGO_PKG_VERSION")),
                    &ResponseEnvelope::success(
                        None,
                        json!({"kind": "version", "version": env!("CARGO_PKG_VERSION")}),
                    ),
                ),
                _ => write_cli_error(
                    mode,
                    stderr,
                    augment_unknown_subcommand(&error, error.to_string().trim_end().to_owned()),
                ),
            }
        }
    }
}

/// Synonyms an agent is likely to type that clap's edit-distance matcher does
/// not catch, each mapped to the real verb it should have used. This is the one
/// place to add an alias; keep it small and keep every entry pointed at a verb
/// that exists. Suggestions are text only — nothing here executes.
fn subcommand_synonym(attempted: &str) -> Option<&'static str> {
    match attempted {
        "tickets" | "ls" | "queue" => Some("list"),
        "ps" => Some("status"),
        "start" => Some("run"),
        "kill" | "abort" => Some("cancel"),
        _ => None,
    }
}

/// Adds a remedy to clap's "unrecognized subcommand" error. clap already
/// appends a `tip:` line when the typo is a near-miss of a real verb, and that
/// text rides through the JSON envelope unchanged, so we leave those alone and
/// only fill the gap: when similarity matching finds nothing but our synonym
/// table does, point the caller at the verb they meant.
fn augment_unknown_subcommand(error: &clap::Error, rendered: String) -> String {
    if error.kind() != ErrorKind::InvalidSubcommand
        || error.get(ContextKind::SuggestedSubcommand).is_some()
    {
        return rendered;
    }
    let Some(attempted) = invalid_subcommand(error) else {
        return rendered;
    };
    let Some(verb) = subcommand_synonym(&attempted) else {
        return rendered;
    };
    let tip = format!(
        "\n\n  tip: `{attempted}` is not a verb; did you mean `{verb}`? run `sloop {verb}`"
    );
    match rendered.find("\n\nUsage:") {
        Some(index) => {
            let mut augmented = rendered;
            augmented.insert_str(index, &tip);
            augmented
        }
        None => rendered + &tip,
    }
}

fn invalid_subcommand(error: &clap::Error) -> Option<String> {
    match error.get(ContextKind::InvalidSubcommand) {
        Some(ContextValue::String(value)) => Some(value.clone()),
        _ => None,
    }
}

fn run_command(
    command: Command,
    mode: OutputMode,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> ExitCode {
    match command {
        Command::Init => run_init(mode, stdout, stderr),
        Command::Template { kind } => run_template(kind, mode, stdout),
        Command::Daemon(args) if args.foreground && args.action.is_none() => {
            match crate::daemon::serve_current_repository() {
                Ok(()) | Err(crate::daemon::DaemonError::AlreadyRunning) => ExitCode::SUCCESS,
                Err(_) => ExitCode::FAILURE,
            }
        }
        Command::Daemon(args) => {
            let (request, report_started) = match args.action {
                Some(DaemonAction::Restart) => (Request::Restart(EmptyArgs::default()), false),
                None => (Request::Daemon(EmptyArgs::default()), true),
            };
            run_daemon_request(request, report_started, mode, stdout, stderr)
        }
        Command::Stop { force } => run_stop_request(force, mode, stdout, stderr),
        Command::Wait { run, timeout } => run_wait(run, timeout, mode, stdout, stderr),
        Command::Watch { r#ref, tail } => run_watch(r#ref, tail, mode, stdout, stderr),
        command @ (Command::Post(_)
        | Command::Run(_)
        | Command::Retry { .. }
        | Command::Hold { .. }
        | Command::Ready { .. }
        | Command::List
        | Command::Status
        | Command::Pause
        | Command::Resume
        | Command::Cancel { .. }
        | Command::Logs { .. }
        | Command::Reindex) => match Request::try_from(command) {
            Ok(request) => run_daemon_request(request, false, mode, stdout, stderr),
            Err(error) => write_cli_error(mode, stderr, error.to_string()),
        },
        command @ Command::Show { .. } => match Request::try_from(command) {
            Ok(request)
                if std::env::var_os("SLOOP_SOCKET").is_some()
                    || std::env::var_os("SLOOP_TOKEN").is_some() =>
            {
                run_worker_request(request, mode, stdout, stderr)
            }
            Ok(request) => run_daemon_request(request, false, mode, stdout, stderr),
            Err(error) => write_cli_error(mode, stderr, error.to_string()),
        },
        command @ (Command::Brief | Command::Note { .. } | Command::Verdict { .. }) => {
            match Request::try_from(command) {
                Ok(request) => run_worker_request(request, mode, stdout, stderr),
                Err(error) => write_cli_error(mode, stderr, error.to_string()),
            }
        }
    }
}

/// Writes plain text in human mode or the given envelope in JSON mode; used
/// for help and version, which have no verb-shaped payload.
fn write_plain_or(
    mode: OutputMode,
    output: &mut impl Write,
    text: &str,
    envelope: &ResponseEnvelope,
) -> ExitCode {
    match mode {
        OutputMode::Json => write_response(mode, None, output, envelope, ExitCode::SUCCESS),
        OutputMode::Human => {
            if writeln!(output, "{text}").is_err() {
                return ExitCode::FAILURE;
            }
            ExitCode::SUCCESS
        }
    }
}

/// Prints a compiled-in template. Like `stop`, this verb never resurrects a
/// daemon — unlike `stop`, it never contacts one at all, because the answer
/// is static content baked into the binary. Plain mode writes the template
/// verbatim so it can be redirected straight into a file.
fn run_template(kind: TemplateKind, mode: OutputMode, stdout: &mut impl Write) -> ExitCode {
    let text = kind.text();
    match mode {
        OutputMode::Json => write_response(
            mode,
            Some("template"),
            stdout,
            &ResponseEnvelope::success(None, json!({"kind": kind.as_str(), "template": text})),
            ExitCode::SUCCESS,
        ),
        OutputMode::Human => {
            if stdout.write_all(text.as_bytes()).is_err() {
                return ExitCode::FAILURE;
            }
            ExitCode::SUCCESS
        }
    }
}

fn run_init(mode: OutputMode, stdout: &mut impl Write, stderr: &mut impl Write) -> ExitCode {
    let cwd = match std::env::current_dir() {
        Ok(cwd) => cwd,
        Err(error) => {
            return write_response(
                mode,
                Some("init"),
                stderr,
                &ResponseEnvelope::failure(
                    None,
                    ErrorBody {
                        code: ErrorCode::Internal,
                        message: format!("cannot read current directory: {error}"),
                        details: json!({}),
                    },
                ),
                ExitCode::FAILURE,
            );
        }
    };
    match crate::init::init(&cwd) {
        Ok(outcome) => write_response(
            mode,
            Some("init"),
            stdout,
            &ResponseEnvelope::success(
                None,
                json!({
                    "repository_root": outcome.repository_root.to_string_lossy(),
                    "created": outcome.created,
                    "existing": outcome.existing,
                }),
            ),
            ExitCode::SUCCESS,
        ),
        Err(error) => {
            let code = match error {
                crate::init::InitError::Conflict { .. } => ErrorCode::Conflict,
                crate::init::InitError::Io { .. } => ErrorCode::Internal,
            };
            write_response(
                mode,
                Some("init"),
                stderr,
                &ResponseEnvelope::failure(
                    None,
                    ErrorBody {
                        code,
                        message: error.to_string(),
                        details: json!({}),
                    },
                ),
                ExitCode::FAILURE,
            )
        }
    }
}

/// Polls the daemon until the run is terminal. The exit code is the outcome
/// (`0` only for `merged`), so scripts and CI can gate on a run directly.
/// Client-side wall-clock polling; the daemon stays stateless.
fn run_wait(
    run: String,
    timeout_secs: u64,
    mode: OutputMode,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> ExitCode {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    loop {
        let result = crate::daemon::request(Request::Wait(RunReferenceArgs { run: run.clone() }));
        match result {
            Ok(result) if result.response.ok => {
                let data = result.response.data.clone().unwrap_or_default();
                if data["terminal"] == serde_json::Value::Bool(true) {
                    return if data["state"] == "merged" {
                        write_response(
                            mode,
                            Some("wait"),
                            stdout,
                            &result.response,
                            ExitCode::SUCCESS,
                        )
                    } else {
                        write_response(
                            mode,
                            Some("wait"),
                            stderr,
                            &result.response,
                            ExitCode::FAILURE,
                        )
                    };
                }
            }
            Ok(result) => {
                return write_response(
                    mode,
                    Some("wait"),
                    stderr,
                    &result.response,
                    ExitCode::FAILURE,
                );
            }
            Err(error) => {
                return write_response(
                    mode,
                    Some("wait"),
                    stderr,
                    &ResponseEnvelope::failure(None, error.error_body()),
                    ExitCode::FAILURE,
                );
            }
        }
        if std::time::Instant::now() >= deadline {
            return write_cli_error(mode, stderr, format!("timed out waiting for run `{run}`"));
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
}

/// Follows the activity feed until interrupted. Same client-side polling
/// model as `wait`: each iteration asks the daemon for events past the
/// cursor from the previous page, so the daemon stays stateless and any
/// other client (a dashboard, a websocket bridge) can stream the same way.
/// In `--json` mode each event is written as one NDJSON line.
///
/// A `scope` reference rides along on every request and the daemon resolves
/// and applies it, so the filter stays part of the public protocol instead of
/// a CLI-only convenience. An unresolvable reference comes back as a
/// `not_found` failure on the very first request, before anything streams.
fn run_watch(
    scope: Option<String>,
    tail: u32,
    mode: OutputMode,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> ExitCode {
    let mut cursor: Option<i64> = None;
    loop {
        let args = match cursor {
            Some(after) => EventsArgs {
                after: Some(after),
                tail: None,
                limit: None,
                scope: scope.clone(),
            },
            None => EventsArgs {
                after: None,
                tail: Some(tail),
                limit: None,
                scope: scope.clone(),
            },
        };
        match crate::daemon::request(Request::Events(args)) {
            Ok(result) if result.response.ok => {
                let data = result.response.data.unwrap_or_default();
                let events = data["events"].as_array().cloned().unwrap_or_default();
                for event in &events {
                    let written = match mode {
                        OutputMode::Json => serde_json::to_writer(&mut *stdout, event)
                            .map_err(|_| ())
                            .and_then(|()| stdout.write_all(b"\n").map_err(|_| ())),
                        OutputMode::Human => {
                            writeln!(stdout, "{}", format_event(event)).map_err(|_| ())
                        }
                    };
                    if written.is_err() {
                        return ExitCode::FAILURE;
                    }
                }
                if stdout.flush().is_err() {
                    return ExitCode::FAILURE;
                }
                let next = data["next_cursor"].as_i64();
                let advanced = next.is_some() && next != cursor;
                if let Some(next) = next {
                    cursor = Some(next);
                }
                // A cursor short of the newest sequence means more rows are
                // already waiting; skip the sleep and drain them. The test is
                // on the cursor rather than on this page being non-empty
                // because a scoped page can filter out every row it scanned
                // and still leave matching rows further along. Requiring the
                // cursor to have moved keeps a daemon that returns no cursor
                // from spinning.
                if advanced && next != data["latest"].as_i64() {
                    continue;
                }
            }
            Ok(result) => {
                return write_response(
                    mode,
                    Some("events"),
                    stderr,
                    &result.response,
                    ExitCode::FAILURE,
                );
            }
            Err(error) => {
                return write_response(
                    mode,
                    Some("events"),
                    stderr,
                    &ResponseEnvelope::failure(None, error.error_body()),
                    ExitCode::FAILURE,
                );
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
}

/// Renders one activity event as a human `watch` line.
fn format_event(event: &serde_json::Value) -> String {
    let time = event["occurred_at_ms"]
        .as_i64()
        .and_then(crate::clock::format_timestamp)
        .unwrap_or_default();
    let run = event["run"].as_str().unwrap_or("?");
    let ticket = event["ticket"].as_str().unwrap_or("?");
    let data = &event["data"];
    match event["kind"].as_str().unwrap_or("?") {
        "run_claimed" => {
            let attempt = data["attempt"].as_i64().unwrap_or(1);
            format!("{time}  {ticket} claimed by {run} (attempt {attempt})")
        }
        "run_started" => format!("{time}  {run} started on {ticket}"),
        "run_finished" => {
            let outcome = data["outcome"].as_str().unwrap_or("?");
            let state = data["ticket_state"].as_str().unwrap_or("?");
            format!("{time}  {run} finished: {outcome} ({ticket} -> {state})")
        }
        "run_aborted" => format!("{time}  {run} aborted before launch ({ticket} back to ready)"),
        "run_worktree_cleaned" => format!("{time}  {run} worktree and branch removed"),
        "daemon_restart_requested" => {
            let active = data["active_runs"].as_u64().unwrap_or(0);
            let noun = if active == 1 { "run" } else { "runs" };
            format!("{time}  daemon draining for restart ({active} {noun} active)")
        }
        kind => format!("{time}  {kind} run={run} ticket={ticket}"),
    }
}

/// `stop` is the one operator verb that must never resurrect a daemon: an
/// unreachable socket already means the desired state.
fn run_stop_request(
    force: bool,
    mode: OutputMode,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> ExitCode {
    match crate::daemon::request_running(Request::Stop(StopArgs { force })) {
        Ok(Some(response)) if response.ok => {
            write_response(mode, Some("stop"), stdout, &response, ExitCode::SUCCESS)
        }
        Ok(Some(response)) => {
            write_response(mode, Some("stop"), stderr, &response, ExitCode::FAILURE)
        }
        Ok(None) => write_response(
            mode,
            Some("stop"),
            stdout,
            &ResponseEnvelope::success(None, json!({"stopping": false, "running": false})),
            ExitCode::SUCCESS,
        ),
        Err(error) => write_response(
            mode,
            Some("stop"),
            stderr,
            &ResponseEnvelope::failure(None, error.error_body()),
            ExitCode::FAILURE,
        ),
    }
}

fn run_daemon_request(
    request: Request,
    report_started: bool,
    mode: OutputMode,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> ExitCode {
    let verb = request.verb();
    match crate::daemon::request(request) {
        Ok(mut result) => {
            if report_started {
                let data = result
                    .response
                    .data
                    .as_mut()
                    .and_then(serde_json::Value::as_object_mut);
                if let Some(data) = data {
                    data.insert("started".into(), result.started.into());
                }
            }
            if result.response.ok {
                write_response(
                    mode,
                    Some(verb),
                    stdout,
                    &result.response,
                    ExitCode::SUCCESS,
                )
            } else {
                write_response(
                    mode,
                    Some(verb),
                    stderr,
                    &result.response,
                    ExitCode::FAILURE,
                )
            }
        }
        Err(error) => write_response(
            mode,
            Some(verb),
            stderr,
            &ResponseEnvelope::failure(None, error.error_body()),
            ExitCode::FAILURE,
        ),
    }
}

/// Sends a worker verb over the per-run socket injected by the agent adapter.
/// Worker verbs never resurrect a daemon: without a run's `SLOOP_SOCKET` and
/// `SLOOP_TOKEN` there is no state worth talking to, so they fail loudly. The
/// daemon's reply envelope is written verbatim; agents are the only callers
/// and the envelope is the API.
fn run_worker_request(
    request: Request,
    mode: OutputMode,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> ExitCode {
    let verb = request.verb();
    let socket = std::env::var_os("SLOOP_SOCKET");
    let token = std::env::var("SLOOP_TOKEN").ok();
    let (Some(socket), Some(token)) = (socket, token) else {
        return write_response(
            mode,
            Some(verb),
            stderr,
            &ResponseEnvelope::failure(
                None,
                ErrorBody {
                    code: ErrorCode::Unauthorized,
                    message: "worker verbs require SLOOP_SOCKET and SLOOP_TOKEN from a run".into(),
                    details: json!({}),
                },
            ),
            ExitCode::FAILURE,
        );
    };

    let envelope = RequestEnvelope::new(
        RequestId::new(format!("req-{}", std::process::id())),
        request,
        Some(token),
    );
    match worker_exchange(&socket, &envelope) {
        Ok(reply) => {
            let ok = serde_json::from_str::<ResponseEnvelope>(&reply)
                .map(|response| response.ok)
                .unwrap_or(false);
            let written = if ok {
                writeln!(stdout, "{}", reply.trim_end())
            } else {
                writeln!(stderr, "{}", reply.trim_end())
            };
            if written.is_err() {
                return ExitCode::FAILURE;
            }
            if ok {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(message) => write_response(
            mode,
            Some(verb),
            stderr,
            &ResponseEnvelope::failure(
                None,
                ErrorBody {
                    code: ErrorCode::DaemonUnavailable,
                    message,
                    details: json!({}),
                },
            ),
            ExitCode::FAILURE,
        ),
    }
}

fn worker_exchange(socket: &std::ffi::OsStr, envelope: &RequestEnvelope) -> Result<String, String> {
    let mut stream = UnixStream::connect(socket)
        .map_err(|error| format!("cannot connect to worker socket: {error}"))?;
    let encoded = envelope
        .encode()
        .map_err(|error| format!("cannot encode request: {error}"))?;
    stream
        .write_all(encoded.as_bytes())
        .and_then(|()| stream.write_all(b"\n"))
        .map_err(|error| format!("cannot send request: {error}"))?;

    let mut reply = String::new();
    BufReader::new(stream)
        .read_line(&mut reply)
        .map_err(|error| format!("cannot read response: {error}"))?;
    if reply.trim_end().is_empty() {
        return Err("the daemon closed the connection without replying".into());
    }
    Ok(reply)
}

fn write_cli_error(mode: OutputMode, output: &mut impl Write, message: String) -> ExitCode {
    write_response(
        mode,
        None,
        output,
        &ResponseEnvelope::failure(
            None,
            ErrorBody {
                code: ErrorCode::InvalidArguments,
                message,
                details: json!({}),
            },
        ),
        ExitCode::from(2),
    )
}

fn write_response(
    mode: OutputMode,
    verb: Option<&str>,
    output: &mut impl Write,
    response: &ResponseEnvelope,
    success: ExitCode,
) -> ExitCode {
    let written = match mode {
        OutputMode::Json => serde_json::to_writer(&mut *output, response)
            .map_err(|_| ())
            .and_then(|()| output.write_all(b"\n").map_err(|_| ())),
        OutputMode::Human => output
            .write_all(crate::render::render(verb, response).as_bytes())
            .map_err(|_| ()),
    };
    if written.is_err() {
        return ExitCode::FAILURE;
    }
    success
}

#[cfg(test)]
mod tests {
    use clap::{Parser, ValueEnum};
    use serde_json::{Value, json};

    use super::{Cli, subcommand_synonym};
    use crate::protocol::{Capability, Request};

    /// Drives the full CLI entry point and returns the error envelope written
    /// to stderr, exactly as an agent using `--json` would receive it.
    fn error_envelope(args: &[&str]) -> Value {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut argv = vec!["sloop", "--json"];
        argv.extend_from_slice(args);
        super::run(argv, &mut stdout, &mut stderr);
        serde_json::from_slice(&stderr).expect("stderr carries a JSON envelope")
    }

    #[test]
    fn unknown_subcommand_suggests_the_tickets_synonym() {
        let envelope = error_envelope(&["tickets"]);

        assert_eq!(envelope["error"]["code"], "invalid_arguments");
        let message = envelope["error"]["message"]
            .as_str()
            .expect("error message");
        assert!(
            message.contains("did you mean `list`") && message.contains("sloop list"),
            "synonym remedy missing from: {message}"
        );
    }

    #[test]
    fn unknown_subcommand_suggests_a_near_miss_spelling() {
        // clap's own similarity matcher supplies this tip; the envelope must
        // carry it through unchanged.
        let envelope = error_envelope(&["statuss"]);

        let message = envelope["error"]["message"]
            .as_str()
            .expect("error message");
        assert!(
            message.contains("status"),
            "near-miss suggestion missing from: {message}"
        );
    }

    #[test]
    fn synonym_table_only_points_at_real_verbs() {
        use clap::CommandFactory;

        let verbs: Vec<String> = Cli::command()
            .get_subcommands()
            .map(|subcommand| subcommand.get_name().to_owned())
            .collect();
        for attempted in ["tickets", "ls", "queue", "ps", "start", "kill", "abort"] {
            let verb = subcommand_synonym(attempted).expect("synonym maps to a verb");
            assert!(
                verbs.iter().any(|known| known == verb),
                "synonym `{attempted}` points at unknown verb `{verb}`"
            );
        }
        assert!(subcommand_synonym("definitely-not-a-verb").is_none());
    }

    #[test]
    fn parses_every_documented_verb() {
        let commands: &[&[&str]] = &[
            &["sloop", "init"],
            &["sloop", "template", "ticket"],
            &["sloop", "template", "flow"],
            &["sloop", "template", "project"],
            &["sloop", "template", "config"],
            &["sloop", "daemon"],
            &["sloop", "post", "ticket.md", "--auto"],
            &["sloop", "post", "ticket.md", "--at", "03:00"],
            &["sloop", "post", "ticket.md", "--manual"],
            &["sloop", "post", "ticket.md", "--hold"],
            &["sloop", "run"],
            &["sloop", "run", "T1", "--at", "03:00"],
            &["sloop", "run", "--every", "30m", "--only", "T1,T7"],
            &["sloop", "run", "--overnight"],
            &["sloop", "retry", "T1"],
            &["sloop", "hold", "T1"],
            &["sloop", "ready", "T1"],
            &["sloop", "list"],
            &["sloop", "status"],
            &["sloop", "pause"],
            &["sloop", "resume"],
            &["sloop", "cancel", "R1"],
            &["sloop", "logs", "R1"],
            &["sloop", "watch"],
            &["sloop", "watch", "--tail", "50"],
            &["sloop", "watch", "T1"],
            &["sloop", "watch", "T1-r2", "--tail", "50"],
            &["sloop", "reindex"],
            &["sloop", "brief"],
            &["sloop", "show", "T1"],
            &["sloop", "note", "work", "in", "progress"],
            &["sloop", "verdict", "fail", "--reason", "changes requested"],
        ];

        for command in commands {
            Cli::try_parse_from(*command).unwrap_or_else(|error| {
                panic!("failed to parse {command:?}: {error}");
            });
        }
    }

    #[test]
    fn every_documented_verb_constructs_a_typed_request() {
        let commands: &[&[&str]] = &[
            &["sloop", "init"],
            &["sloop", "daemon"],
            &["sloop", "post", "ticket.md", "--auto"],
            &["sloop", "run"],
            &["sloop", "retry", "T1"],
            &["sloop", "hold", "T1"],
            &["sloop", "ready", "T1"],
            &["sloop", "list"],
            &["sloop", "status"],
            &["sloop", "pause"],
            &["sloop", "resume"],
            &["sloop", "cancel", "R1"],
            &["sloop", "logs", "R1"],
            &["sloop", "watch"],
            &["sloop", "watch", "T1"],
            &["sloop", "reindex"],
            &["sloop", "brief"],
            &["sloop", "show", "T1"],
            &["sloop", "note", "working"],
            &["sloop", "verdict", "pass"],
        ];

        for command in commands {
            Cli::try_parse_from(*command)
                .unwrap()
                .into_request()
                .unwrap_or_else(|error| panic!("failed to construct {command:?}: {error}"));
        }
    }

    #[test]
    fn run_options_become_protocol_arguments() {
        let request = Cli::try_parse_from(["sloop", "run", "--every", "30m", "--only", "T1,T7"])
            .unwrap()
            .into_request()
            .unwrap();

        assert_eq!(
            serde_json::to_value(request).unwrap(),
            json!({
                "verb": "run",
                "args": {
                    "activation": {"kind": "every", "interval_ms": 1_800_000},
                    "only": ["T1", "T7"]
                }
            })
        );
    }

    #[test]
    fn hold_becomes_a_distinct_post_activation() {
        let request = Cli::try_parse_from(["sloop", "post", "ticket.md", "--hold"])
            .unwrap()
            .into_request()
            .unwrap();

        assert_eq!(
            serde_json::to_value(request).unwrap(),
            json!({
                "verb": "post",
                "args": {
                    "file": "ticket.md",
                    "activation": {"kind": "hold"}
                }
            })
        );
    }

    #[test]
    fn worker_request_text_and_capability_are_preserved() {
        let request = Cli::try_parse_from(["sloop", "note", "work", "in", "progress"])
            .unwrap()
            .into_request()
            .unwrap();

        assert_eq!(request.capability(), Capability::Worker);
        assert_eq!(
            serde_json::to_value(request).unwrap(),
            json!({"verb": "note", "args": {"text": "work in progress"}})
        );
    }

    #[test]
    fn verdict_becomes_a_worker_request() {
        let request =
            Cli::try_parse_from(["sloop", "verdict", "fail", "--reason", "changes requested"])
                .unwrap()
                .into_request()
                .unwrap();

        assert_eq!(request.capability(), Capability::Worker);
        assert_eq!(
            serde_json::to_value(request).unwrap(),
            json!({
                "verb": "verdict",
                "args": {"verdict": "fail", "reason": "changes requested"}
            })
        );
    }

    #[test]
    fn operator_requests_are_classified_separately() {
        let request = Cli::try_parse_from(["sloop", "status"])
            .unwrap()
            .into_request()
            .unwrap();

        assert_eq!(request.capability(), Capability::Operator);
        assert!(matches!(request, Request::Status(_)));
    }

    /// Drives the full entry point and returns stdout, so these tests prove
    /// the verb answers without a daemon: the test process has no repository,
    /// no socket, and no `SLOOP_TOKEN`, and any daemon path would fail.
    fn stdout_of(args: &[&str]) -> String {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut argv = vec!["sloop"];
        argv.extend_from_slice(args);
        let code = super::run(argv, &mut stdout, &mut stderr);

        assert_eq!(
            format!("{code:?}"),
            format!("{:?}", std::process::ExitCode::SUCCESS),
            "stderr: {}",
            String::from_utf8_lossy(&stderr)
        );
        String::from_utf8(stdout).expect("stdout is UTF-8")
    }

    #[test]
    fn every_template_kind_prints_verbatim_without_a_daemon() {
        for kind in ["ticket", "flow", "project", "config"] {
            let printed = stdout_of(&["template", kind]);
            let expected = super::TemplateKind::from_str(kind, false)
                .expect("kind is accepted")
                .text();
            assert_eq!(printed, expected, "`sloop template {kind}` was rewritten");
        }
    }

    #[test]
    fn template_json_mode_wraps_the_text_in_an_envelope() {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        super::run(
            ["sloop", "--json", "template", "flow"],
            &mut stdout,
            &mut stderr,
        );

        let envelope: Value = serde_json::from_slice(&stdout).expect("stdout carries an envelope");
        assert_eq!(envelope["ok"], true);
        assert_eq!(envelope["data"]["kind"], "flow");
        assert_eq!(
            envelope["data"]["template"].as_str(),
            Some(super::TemplateKind::Flow.text())
        );
    }

    #[test]
    fn an_unknown_template_kind_lists_the_valid_kinds() {
        let envelope = error_envelope(&["template", "readme"]);

        assert_eq!(envelope["error"]["code"], "invalid_arguments");
        let message = envelope["error"]["message"]
            .as_str()
            .expect("error message");
        for kind in ["ticket", "flow", "project", "config"] {
            assert!(message.contains(kind), "`{kind}` missing from: {message}");
        }
    }

    /// `template` answers from compiled-in content, so it deliberately has no
    /// protocol verb. Constructing a request must fail rather than silently
    /// routing it at the daemon.
    #[test]
    fn template_never_becomes_a_daemon_request() {
        let error = Cli::try_parse_from(["sloop", "template", "ticket"])
            .unwrap()
            .into_request()
            .expect_err("template has no daemon request");

        assert!(error.to_string().contains("printed locally"), "{error}");
    }

    #[test]
    fn post_help_points_at_the_ticket_template() {
        let help = stdout_of(&["post", "--help"]);

        assert!(help.contains("sloop template ticket"), "{help}");
        assert!(help.contains("sloop template flow"), "{help}");
    }

    #[test]
    fn invalid_time_and_duration_fail_during_cli_parsing() {
        assert!(Cli::try_parse_from(["sloop", "run", "--at", "25:00"]).is_err());
        assert!(Cli::try_parse_from(["sloop", "run", "--every", "later"]).is_err());
        assert!(Cli::try_parse_from(["sloop", "run", "--every", "0m"]).is_err());
    }

    #[test]
    fn run_accepts_only_one_activation_mode() {
        let result = Cli::try_parse_from(["sloop", "run", "--at", "03:00", "--every", "30m"]);
        assert!(result.is_err());
    }

    #[test]
    fn post_defaults_to_auto_and_accepts_only_one_explicit_activation_mode() {
        let request = Cli::try_parse_from(["sloop", "post", "ticket.md"])
            .unwrap()
            .into_request()
            .unwrap();
        assert_eq!(
            serde_json::to_value(request).unwrap(),
            json!({
                "verb": "post",
                "args": {
                    "file": "ticket.md",
                    "activation": {"kind": "auto"}
                }
            })
        );
        assert!(Cli::try_parse_from(["sloop", "post", "ticket.md", "--auto", "--manual"]).is_err());
        assert!(Cli::try_parse_from(["sloop", "post", "ticket.md", "--manual", "--hold"]).is_err());
    }
}
