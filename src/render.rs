//! Human rendering of response envelopes. JSON envelopes remain the internal
//! and `--json` representation; this module is a one-way translation applied
//! at the final write, so the socket API and agent-facing output are
//! unaffected by presentation changes here.

use std::fmt::Write;

use serde_json::Value;

use crate::protocol::{ErrorBody, ResponseEnvelope};

/// Renders a response envelope as human-readable text. `verb` selects a
/// verb-specific layout; unknown or absent verbs fall back to pretty JSON so
/// no response is ever silently dropped.
pub fn render(verb: Option<&str>, response: &ResponseEnvelope) -> String {
    if let Some(error) = &response.error {
        return render_error(error);
    }
    let data = response.data.as_ref().unwrap_or(&Value::Null);
    if data["implemented"] == Value::Bool(false) {
        let verb = data["verb"].as_str().unwrap_or("this verb");
        return format!("{verb} is not implemented by the daemon yet\n");
    }
    match verb {
        Some("daemon") => render_daemon(data),
        Some("restart") => render_restart(data),
        Some("init") => render_init(data),
        Some("post") => render_post(data),
        Some("run") => render_run(data),
        Some("retry" | "hold" | "ready") => render_ticket_transition(data),
        Some("list") => render_list(data),
        Some("status") => render_status(data),
        Some("pause" | "resume") => render_scheduler_transition(data),
        Some("stop") => render_stop(data),
        Some("wait") => render_wait(data),
        Some("cancel") => render_cancel(data),
        Some("logs") => render_logs(data),
        Some("reindex") => render_reindex(data),
        Some("show") => render_show(data),
        _ => fallback(data),
    }
}

pub fn render_error(error: &ErrorBody) -> String {
    let code = serde_json::to_value(error.code)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "error".into());
    let mut text = format!("error ({code}): {}\n", error.message);
    if error.details.as_object().is_some_and(|map| !map.is_empty()) {
        let _ = writeln!(text, "  details: {}", error.details);
    }
    text
}

fn render_daemon(data: &Value) -> String {
    let pid = &data["pid"];
    let state = if data["started"] == Value::Bool(true) {
        "started"
    } else {
        "running"
    };
    let mut text = format!("daemon {state} (pid {pid})\n");
    if let Some(socket) = data["socket"].as_str() {
        let _ = writeln!(text, "socket: {socket}");
    }
    if let Some(log) = data["log"].as_str() {
        let _ = writeln!(text, "log: {log}");
    }
    text
}

fn render_restart(data: &Value) -> String {
    let active = data["active_runs"].as_u64().unwrap_or(0);
    let noun = if active == 1 { "run" } else { "runs" };
    format!("daemon draining for restart ({active} {noun} active)\n")
}

fn render_init(data: &Value) -> String {
    let mut text = format!(
        "initialized sloop in {}\n",
        data["repository_root"].as_str().unwrap_or("?")
    );
    for (label, key) in [("created", "created"), ("existing", "existing")] {
        for path in string_items(&data[key]) {
            let _ = writeln!(text, "  {label}: {path}");
        }
    }
    // The scaffold shows the shape but not the grammar; point at the verb
    // that documents every field, since nothing else in an installed binary
    // does.
    text.push_str(
        "\nwrite a ticket with `sloop template ticket > .agents/sloop/tickets/<name>.md`\n\
         see also `sloop template flow|project|config`\n",
    );
    text
}

fn render_post(data: &Value) -> String {
    let ticket = &data["ticket"];
    let mut text = format!(
        "ticket {} registered from {} (project {}, {})\n",
        ticket["id"].as_str().unwrap_or("?"),
        ticket["file"].as_str().unwrap_or("?"),
        ticket["project"].as_str().unwrap_or("?"),
        ticket["state"].as_str().unwrap_or("?"),
    );
    text.push_str(&render_activation(&data["activation"]));
    text
}

fn render_run(data: &Value) -> String {
    render_activation(&data["activation"])
}

fn render_activation(activation: &Value) -> String {
    let Some(fields) = activation.as_object() else {
        return String::new();
    };
    let mut text = format!(
        "activation {} {} ({}",
        fields.get("id").and_then(Value::as_str).unwrap_or("?"),
        fields
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("queued"),
        fields.get("kind").and_then(Value::as_str).unwrap_or("?"),
    );
    for key in ["ticket", "project", "time"] {
        if let Some(value) = fields.get(key).and_then(Value::as_str) {
            let _ = write!(text, ", {key} {value}");
        }
    }
    text.push_str(")\n");
    text
}

fn render_status(data: &Value) -> String {
    let daemon = &data["daemon"];
    let state = if daemon["draining"] == Value::Bool(true) {
        let active = data["gate"]["active_agents"].as_u64().unwrap_or(0);
        let noun = if active == 1 { "run" } else { "runs" };
        format!(", draining for restart ({active} {noun} active)")
    } else if daemon["paused"] == Value::Bool(true) {
        ", paused".into()
    } else {
        String::new()
    };
    let mut text = format!("daemon: pid {}{state}\n", daemon["pid"]);
    let _ = writeln!(
        text,
        "agents: {} active of {} max",
        data["gate"]["active_agents"], data["gate"]["max_agents"]
    );
    if data["gate"]["storage"]["writable"] == Value::Bool(false) {
        text.push_str("storage: database full (dispatch blocked until space is available)\n");
    }
    if let Some(hours) = data["gate"]["running_hours"].as_object() {
        let state = if hours.get("open") == Some(&Value::Bool(true)) {
            "open"
        } else {
            "closed"
        };
        let _ = writeln!(
            text,
            "running hours: {}-{} ({state})",
            hours.get("start").and_then(Value::as_str).unwrap_or("?"),
            hours.get("end").and_then(Value::as_str).unwrap_or("?"),
        );
    }
    if let Some(next_wake) = data["next_wake"].as_str() {
        let _ = writeln!(text, "next wake: {next_wake}");
    }

    let tickets = &data["tickets"];
    let counts: Vec<String> = [
        "ready",
        "held",
        "blocked",
        "claimed",
        "merged",
        "failed",
        "needs_review",
    ]
    .iter()
    .map(|state| format!("{} {state}", tickets[*state]))
    .collect();
    let _ = writeln!(text, "tickets: {}", counts.join(", "));

    // Run lines lead with the alias and the ticket's name, so the line answers
    // "what is this working on" without a second command. Queued activations
    // are not runs and keep their own shape.
    let runs = data["runs"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    if runs.is_empty() {
        text.push_str("runs: none\n");
    } else {
        text.push_str("runs:\n");
        for run in runs {
            let _ = writeln!(
                text,
                "  {} {} — {} (project {})",
                run["alias"].as_str().unwrap_or("?"),
                run["state"].as_str().unwrap_or("?"),
                run["ticket_name"].as_str().unwrap_or("?"),
                run["project"].as_str().unwrap_or("-"),
            );
        }
    }

    let queued = data["queued_activations"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    if queued.is_empty() {
        text.push_str("queued: none\n");
    } else {
        text.push_str("queued:\n");
        for item in queued {
            let _ = writeln!(
                text,
                "  {} {} (ticket {}, project {})",
                item["id"].as_str().unwrap_or("?"),
                item["state"].as_str().unwrap_or("?"),
                item["ticket"].as_str().unwrap_or("-"),
                item["project"].as_str().unwrap_or("-"),
            );
        }
    }
    text
}

fn render_list(data: &Value) -> String {
    let tickets = data["tickets"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    if tickets.is_empty() {
        return "no tickets\n".into();
    }

    let id_width = tickets
        .iter()
        .filter_map(|ticket| ticket["id"].as_str())
        .map(str::len)
        .max()
        .unwrap_or(1);
    let state_width = tickets
        .iter()
        .filter_map(|ticket| ticket["state"].as_str())
        .map(str::len)
        .max()
        .unwrap_or(1);
    let mut text = String::new();
    for ticket in tickets {
        let id = ticket["id"].as_str().unwrap_or("?");
        let state = ticket["state"].as_str().unwrap_or("?");
        let project = ticket["project"].as_str().unwrap_or("?");
        let name = ticket["name"].as_str().unwrap_or("?");
        let _ = write!(
            text,
            "{id:id_width$}  {state:state_width$}  ({project})  {name}"
        );
        let terminal = matches!(state, "merged" | "needs_review");
        if ticket["run"].is_null()
            && !terminal
            && let Some(reason) = ticket["reason"].as_str()
        {
            let _ = write!(text, "  — {reason}");
        }
        text.push('\n');
    }
    text
}

fn render_ticket_transition(data: &Value) -> String {
    format!(
        "ticket {}: {} -> {}\n",
        data["ticket"].as_str().unwrap_or("?"),
        data["previous_state"].as_str().unwrap_or("?"),
        data["state"].as_str().unwrap_or("?"),
    )
}

fn render_scheduler_transition(data: &Value) -> String {
    if data["paused"] == Value::Bool(true) {
        "scheduler paused\n".into()
    } else {
        "scheduler resumed\n".into()
    }
}

fn render_stop(data: &Value) -> String {
    if data["running"] == Value::Bool(false) {
        return "daemon is not running\n".into();
    }
    let mut text = format!("daemon stopping (pid {})\n", data["pid"]);
    for run in string_items(&data["cancelled_runs"]) {
        let _ = writeln!(text, "  cancelled: {run}");
    }
    text
}

fn render_wait(data: &Value) -> String {
    let mut rendered = String::new();
    if let Some(note) = data["note"].as_str() {
        let _ = writeln!(rendered, "{note}");
    }
    let _ = writeln!(
        rendered,
        "run {} {}",
        data["alias"].as_str().unwrap_or("?"),
        data["state"].as_str().unwrap_or("?"),
    );
    if let Some(reason) = data["reason"].as_str() {
        let _ = writeln!(rendered, "reason: {reason}");
    }
    rendered
}

fn render_cancel(data: &Value) -> String {
    let mut text = format!("run {} cancelling\n", data["alias"].as_str().unwrap_or("?"));
    if let Some(worktree) = data["worktree"].as_str() {
        let _ = writeln!(text, "worktree preserved at {worktree}");
    }
    text
}

fn render_logs(data: &Value) -> String {
    let entries = data["entries"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    let mut text = String::new();
    if let Some(note) = data["note"].as_str() {
        let _ = writeln!(text, "{note}");
    }
    if entries.is_empty() {
        let _ = writeln!(
            text,
            "no output captured for run {}",
            data["alias"].as_str().unwrap_or("?")
        );
        return text;
    }
    for entry in entries {
        let timestamp = entry["timestamp"].as_str().unwrap_or("?");
        let mut origin = entry["source"].as_str().unwrap_or("?").to_owned();
        if let Some(stage) = entry["stage"].as_str() {
            let _ = write!(origin, ":{stage}");
        }
        // Bytes that failed UTF-8 decoding are stored as base64; a human
        // view labels them rather than printing garbage.
        let line = entry["text"].as_str().unwrap_or("<binary output>");
        let _ = writeln!(text, "[{timestamp}] [{origin}] {line}");
    }
    text
}

fn render_reindex(data: &Value) -> String {
    format!(
        "reindexed {} projects and {} tickets; {} ticket states changed; {} rows dropped\n",
        data["projects_indexed"],
        data["tickets_indexed"],
        data["tickets_state_changed"],
        data["rows_dropped"]
    )
}

fn render_show(data: &Value) -> String {
    match data["kind"].as_str() {
        Some("dashboard") => render_dashboard(data),
        Some("matches") => render_list(data),
        Some("ticket") => render_ticket_show(data),
        Some("run") => render_run_show(data),
        Some("project") => render_project_show(data),
        _ => fallback(data),
    }
}

fn render_dashboard(data: &Value) -> String {
    let daemon = &data["daemon"];
    let gate = &data["gate"];
    let state = if daemon["draining"] == Value::Bool(true) {
        "draining"
    } else if daemon["paused"] == Value::Bool(true) {
        "paused"
    } else {
        "running"
    };
    let mut text = format!(
        "daemon: pid {} {state} - {}/{} agents active",
        daemon["pid"], gate["active_agents"], gate["max_agents"]
    );
    if let Some(next_wake) = data["next_wake"].as_str() {
        let _ = write!(text, " - next wake {next_wake}");
    }
    text.push('\n');

    let tickets = &data["tickets"];
    let counts = [
        "ready",
        "held",
        "blocked",
        "claimed",
        "merged",
        "failed",
        "needs_review",
    ]
    .iter()
    .map(|state| format!("{} {state}", tickets[*state]))
    .collect::<Vec<_>>();
    let _ = writeln!(text, "tickets: {}", counts.join(", "));

    let runs = data["runs"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    if !runs.is_empty() {
        text.push_str("\nruns:\n");
        let alias_width = column_width(runs, "alias");
        let state_width = column_width(runs, "state");
        for run in runs {
            let _ = writeln!(
                text,
                "  {:alias_width$}  {:state_width$}  {}  {}  {}",
                run["alias"].as_str().unwrap_or("?"),
                run["state"].as_str().unwrap_or("?"),
                span(
                    run["started_at_ms"].as_i64(),
                    run["finished_at_ms"].as_i64()
                ),
                stage_strip(&run["stages"]),
                run["ticket_name"].as_str().unwrap_or("?"),
            );
        }
    }

    text.push_str("\nrecent:\n");
    text.push_str(&render_list(
        &serde_json::json!({"tickets": data["recent"]}),
    ));
    let shown = data["recent"].as_array().map_or(0, Vec::len);
    let total = data["recent_total"].as_u64().unwrap_or(shown as u64);
    let more = total.saturating_sub(shown as u64);
    if total == 0 {
        text.push_str("\n`sloop show <ref>` for detail\n");
    } else {
        let _ = writeln!(
            text,
            "\n{more} more - `sloop show -{total}` for all - `sloop show <ref>` for detail"
        );
    }
    text
}

/// A scannable frontmatter summary followed by the ticket body. The worker's
/// own `show` carries no `body`, so the same layout renders just the summary
/// for it — no worker-specific branch and no behavior change.
fn render_ticket_show(data: &Value) -> String {
    let value = &data["value"];
    let mut text = format!(
        "{}  {}  ({})\n",
        value["id"].as_str().unwrap_or("?"),
        value["name"].as_str().unwrap_or("?"),
        value["state"].as_str().unwrap_or("?"),
    );
    if let Some(project) = value["project"].as_str() {
        let _ = writeln!(text, "project: {project}");
    }
    if let Some(worktree) = value["worktree"].as_str() {
        let _ = writeln!(text, "worktree: {worktree}");
    }
    let blocked_by = string_items(&value["blocked_by"]).collect::<Vec<_>>();
    if !blocked_by.is_empty() {
        let _ = writeln!(text, "blocked_by: {}", blocked_by.join(", "));
    }
    for (label, key) in [
        ("target", "target"),
        ("model", "model"),
        ("effort", "effort"),
    ] {
        if let Some(field) = value[key].as_str() {
            let _ = writeln!(text, "{label}: {field}");
        }
    }
    if let Some(reason) = value["reason"].as_str() {
        let _ = writeln!(text, "reason: {reason}");
    }
    text.push_str(&ticket_runs(&value["runs"]));
    if let Some(body) = value["body"]
        .as_str()
        .map(str::trim)
        .filter(|body| !body.is_empty())
    {
        let _ = write!(text, "\n{body}\n");
    }
    text
}

/// The ticket's runs, newest first, one line each: alias, outcome, wall-clock
/// span, and a strip of stage markers. A ticket that has never run prints
/// `runs: none` rather than nothing, so "no runs yet" is distinguishable from
/// an older `sloop` that did not report runs at all.
fn ticket_runs(runs: &Value) -> String {
    let Some(runs) = runs.as_array() else {
        return String::new();
    };
    if runs.is_empty() {
        return "runs: none\n".to_owned();
    }
    // The alias and state columns are padded to the widest entry so the spans
    // and stage strips line up down the section and can be read as columns.
    let alias_width = column_width(runs, "alias");
    let state_width = column_width(runs, "state");
    let mut text = String::from("runs:\n");
    for run in runs {
        let _ = writeln!(
            text,
            "  {:alias_width$}  {:state_width$}  {}  {}",
            run["alias"].as_str().unwrap_or("?"),
            run["state"].as_str().unwrap_or("?"),
            span(
                run["started_at_ms"].as_i64(),
                run["finished_at_ms"].as_i64()
            ),
            stage_strip(&run["stages"]),
        );
    }
    text
}

fn column_width(runs: &[Value], key: &str) -> usize {
    runs.iter()
        .filter_map(|run| run[key].as_str())
        .map(str::len)
        .max()
        .unwrap_or(1)
}

/// The per-stage markers on a run's summary line. Deliberately ASCII: the rest
/// of this renderer is, and a stage strip is exactly the output most likely to
/// be piped through something that mangles glyphs.
fn stage_strip(stages: &Value) -> String {
    stages
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .map(|stage| {
            format!(
                "{}:{}",
                stage["stage"].as_str().unwrap_or("?"),
                stage_marker(stage["state"].as_str().unwrap_or("")),
            )
        })
        .collect::<Vec<_>>()
        .join("  ")
}

fn stage_marker(state: &str) -> &'static str {
    match state {
        "passed" => "ok",
        "failed" => "FAIL",
        "running" => "..",
        _ => "-",
    }
}

/// A run or stage's wall-clock span. An unfinished one is open-ended rather
/// than closed at the current instant — a running stage has no end yet, and
/// printing one would be an invention.
fn span(start_ms: Option<i64>, end_ms: Option<i64>) -> String {
    let Some(start) = start_ms.and_then(crate::clock::local_time) else {
        return "-".to_owned();
    };
    let end = end_ms.and_then(crate::clock::local_time);
    // `HH:MM` alone is ambiguous for anything that did not happen today or
    // that ran across midnight, so those two cases widen to include the date.
    let today = crate::clock::local_time(now_ms());
    let dated = today.is_some_and(|today| !start.same_day(&today))
        || end.is_some_and(|end| !start.same_day(&end));
    let opening = if dated { start.dated() } else { start.clock() };
    match end {
        None => format!("{opening}-..."),
        Some(end) if start.same_day(&end) => format!("{opening}-{}", end.clock()),
        Some(end) => format!("{opening}-{}", end.dated()),
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

/// A duration in the coarsest unit that still says something useful. Stage
/// durations range from milliseconds to hours, and `4m12s` is easier to
/// compare at a glance than `252000ms`.
fn duration(milliseconds: i64) -> String {
    let seconds = milliseconds / 1_000;
    match (seconds / 3_600, (seconds % 3_600) / 60, seconds % 60) {
        (0, 0, seconds) => format!("{seconds}s"),
        (0, minutes, seconds) => format!("{minutes}m{seconds}s"),
        (hours, minutes, _) => format!("{hours}h{minutes}m"),
    }
}

/// The run's stage table: how far the flow got, and on what evidence. This is
/// the view that answers "how did this run fail" without opening the database.
fn run_stages(stages: &Value) -> String {
    let Some(stages) = stages.as_array().filter(|stages| !stages.is_empty()) else {
        return String::new();
    };
    let name_width = stages
        .iter()
        .filter_map(|stage| stage["stage"].as_str())
        .map(str::len)
        .max()
        .unwrap_or(5)
        .max(5);
    let mut text = String::from("stages:\n");
    for stage in stages {
        let state = stage["state"].as_str().unwrap_or("?");
        let mut line = format!(
            "  {:name_width$}  {state:7}  {}",
            stage["stage"].as_str().unwrap_or("?"),
            span(
                stage["started_at_ms"].as_i64(),
                stage["finished_at_ms"].as_i64()
            ),
        );
        if let Some(elapsed) = stage["duration_ms"].as_i64() {
            let _ = write!(line, "  {}", duration(elapsed));
        }
        if let Some(exit_code) = stage["exit_code"].as_i64() {
            let _ = write!(line, "  exit {exit_code}");
        }
        // Attempts are only worth a column when a repair actually retried the
        // stage; every other stage ran exactly once and saying so is noise.
        if let Some(attempts) = stage["attempts"].as_u64().filter(|attempts| *attempts > 1) {
            let _ = write!(line, "  {attempts} attempts");
        }
        if let Some(source) = stage["verdict_source"].as_str() {
            let _ = write!(line, "  verdict from {source}");
        }
        let _ = writeln!(text, "{}", line.trim_end());
    }
    text
}

/// A run's identity and settled evidence, one fact per line.
fn render_run_show(data: &Value) -> String {
    let value = &data["value"];
    let mut text = String::new();
    if let Some(note) = value["note"].as_str() {
        let _ = writeln!(text, "{note}");
    }
    let _ = writeln!(
        text,
        "{}  ({})",
        value["alias"].as_str().unwrap_or("?"),
        value["state"].as_str().unwrap_or("?"),
    );
    let ticket = value["ticket"].as_str().unwrap_or("?");
    match value["ticket_name"].as_str() {
        Some(name) => {
            let _ = writeln!(text, "ticket: {ticket}  {name}");
        }
        None => {
            let _ = writeln!(text, "ticket: {ticket}");
        }
    }
    if let Some(branch) = value["branch"].as_str() {
        let _ = writeln!(text, "branch: {branch}");
    }
    if let Some(worktree) = value["worktree"].as_str() {
        let _ = writeln!(text, "worktree: {worktree}");
    }
    // Claim, start, and finish bound the run; a run still in flight simply
    // lacks the later fields rather than showing a guessed end.
    let timeline = [
        ("claimed", value["claimed_at_ms"].as_i64()),
        ("started", value["started_at_ms"].as_i64()),
        ("finished", value["finished_at_ms"].as_i64()),
    ]
    .into_iter()
    .filter_map(|(label, at_ms)| {
        let at = crate::clock::local_time(at_ms?)?;
        Some(format!("{label} {}", at.clock()))
    })
    .collect::<Vec<_>>();
    if !timeline.is_empty() {
        let _ = writeln!(text, "timeline: {}", timeline.join("  "));
    }
    // `exit: 0` read as "the run passed" even when a later stage had failed,
    // which is exactly how one smoke-test failure got misdiagnosed. The label
    // now says whose exit it is.
    if let Some(exit_code) = value["exit_code"].as_i64() {
        let _ = writeln!(text, "agent exit: {exit_code}");
    }
    if let Some(reason) = value["reason"].as_str() {
        let _ = writeln!(text, "reason: {reason}");
    }
    text.push_str(&run_stages(&value["stages"]));
    text
}

fn render_project_show(data: &Value) -> String {
    let project = &data["value"];
    let mut text = format!(
        "project {} ({})\n",
        project["title"].as_str().unwrap_or("?"),
        project["id"].as_str().unwrap_or("?"),
    );
    let tickets = project["tickets"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    if tickets.is_empty() {
        text.push_str("no tickets\n");
        return text;
    }
    for ticket in tickets {
        let _ = writeln!(
            text,
            "\n{}  {}  ({})",
            ticket["id"].as_str().unwrap_or("?"),
            ticket["name"].as_str().unwrap_or("?"),
            ticket["state"].as_str().unwrap_or("?"),
        );
        for note in ticket["notes"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or_default()
        {
            let _ = writeln!(
                text,
                "  note {} [{}]: {}",
                note["id"].as_str().unwrap_or("?"),
                note["run"].as_str().unwrap_or("?"),
                note["text"].as_str().unwrap_or("?"),
            );
        }
        for commit in ticket["commits"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or_default()
        {
            let _ = writeln!(
                text,
                "  commit {} [{}]: {}",
                commit["hash"].as_str().unwrap_or("?"),
                commit["run"].as_str().unwrap_or("?"),
                commit["message"].as_str().unwrap_or("?"),
            );
        }
    }
    text
}

fn fallback(data: &Value) -> String {
    let mut text = serde_json::to_string_pretty(data).unwrap_or_else(|_| data.to_string());
    text.push('\n');
    text
}

fn string_items(value: &Value) -> impl Iterator<Item = &str> {
    value
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .filter_map(Value::as_str)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::render;
    use crate::protocol::{ErrorBody, ErrorCode, ResponseEnvelope};

    #[test]
    fn errors_render_the_code_and_message() {
        let response = ResponseEnvelope::failure(
            None,
            ErrorBody {
                code: ErrorCode::Conflict,
                message: "run `R1` is `merged` and cannot be cancelled".into(),
                details: json!({}),
            },
        );

        let text = render(Some("cancel"), &response);
        assert_eq!(
            text,
            "error (conflict): run `R1` is `merged` and cannot be cancelled\n"
        );
    }

    #[test]
    fn status_renders_counts_and_active_runs() {
        let response = ResponseEnvelope::success(
            None,
            json!({
                "daemon": {"pid": 42, "paused": false},
                "gate": {
                    "active_agents": 1,
                    "max_agents": 2,
                    "running_hours": {"start": "22:00", "end": "06:00", "open": false}
                },
                "next_wake": "2026-07-15T22:00:00Z",
                "runs": [{
                    "id": "3f2a9c1b7d4e5061a2b3c4d5e6f70819", "alias": "T1-r1",
                    "state": "running", "ticket": "T1", "ticket_name": "Generalized stages",
                    "project": "default"
                }],
                "queued_activations": [],
                "tickets": {
                    "ready": 1, "held": 2, "blocked": 0, "claimed": 1,
                    "merged": 3, "failed": 0, "needs_review": 0
                }
            }),
        );

        let text = render(Some("status"), &response);
        assert!(text.contains("daemon: pid 42\n"), "{text}");
        assert!(text.contains("agents: 1 active of 2 max"), "{text}");
        assert!(
            text.contains("running hours: 22:00-06:00 (closed)"),
            "{text}"
        );
        assert!(text.contains("next wake: 2026-07-15T22:00:00Z"), "{text}");
        assert!(
            text.contains(
                "tickets: 1 ready, 2 held, 0 blocked, 1 claimed, 3 merged, 0 failed, 0 needs_review"
            ),
            "{text}"
        );
        assert!(
            text.contains("T1-r1 running — Generalized stages (project default)"),
            "{text}"
        );
        assert!(text.contains("queued: none"), "{text}");
    }

    #[test]
    fn status_renders_a_full_database_as_a_dispatch_gate() {
        let response = ResponseEnvelope::success(
            None,
            json!({
                "daemon": {"pid": 42, "paused": false},
                "gate": {
                    "active_agents": 0,
                    "max_agents": 1,
                    "storage": {"writable": false, "reason": "database_full"}
                },
                "runs": [],
                "queued_activations": [],
                "tickets": {}
            }),
        );

        let text = render(Some("status"), &response);
        assert!(
            text.contains("storage: database full (dispatch blocked"),
            "{text}"
        );
    }

    #[test]
    fn list_renders_reasons_but_not_running_or_terminal_reasons() {
        let response = ResponseEnvelope::success(
            None,
            json!({
                "tickets": [
                    {"id": "TICKET-1", "name": "Fix dispatch", "project": "default", "state": "ready", "run": null,
                     "reason": "scheduler is paused; resume with `sloop resume`"},
                    {"id": "T2", "name": "Add retries", "project": "default", "state": "claimed", "run": "R1",
                     "reason": "claimed by run R1"},
                    {"id": "T3", "name": "Polish the UI", "project": "web", "state": "merged", "run": null,
                     "reason": null}
                ]
            }),
        );

        assert_eq!(
            render(Some("list"), &response),
            "TICKET-1  ready    (default)  Fix dispatch  — scheduler is paused; resume with `sloop resume`\n\
             T2        claimed  (default)  Add retries\n\
             T3        merged   (web)  Polish the UI\n"
        );
    }

    #[test]
    fn empty_list_says_there_are_no_tickets() {
        let response = ResponseEnvelope::success(None, json!({"tickets": []}));
        assert_eq!(render(Some("list"), &response), "no tickets\n");
    }

    #[test]
    fn hold_and_ready_render_the_transition() {
        let response = ResponseEnvelope::success(
            None,
            json!({
                "ticket": "T1",
                "previous_state": "held",
                "state": "ready",
                "overridden": true,
            }),
        );

        for verb in ["hold", "ready"] {
            assert_eq!(render(Some(verb), &response), "ticket T1: held -> ready\n");
        }
    }

    #[test]
    fn retry_renders_the_transition() {
        let response = ResponseEnvelope::success(
            None,
            json!({
                "ticket": "T1",
                "previous_state": "failed",
                "state": "ready",
            }),
        );

        assert_eq!(
            render(Some("retry"), &response),
            "ticket T1: failed -> ready\n"
        );
    }

    #[test]
    fn unknown_shapes_fall_back_to_pretty_json() {
        let response = ResponseEnvelope::success(None, json!({"surprise": true}));
        let text = render(Some("mystery"), &response);
        assert!(text.contains("\"surprise\": true"), "{text}");
    }

    #[test]
    fn project_show_groups_activity_by_ticket() {
        let response = ResponseEnvelope::success(
            None,
            json!({
                "ref": "backend",
                "kind": "project",
                "value": {
                    "id": "backend",
                    "title": "Backend",
                    "tickets": [{
                        "id": "T1",
                        "name": "Persist cooldowns",
                        "state": "merged",
                        "notes": [{"id": "N1", "run": "R1", "text": "halfway"}],
                        "commits": [{"hash": "abc1234", "run": "R1", "message": "persist cooldowns"}]
                    }]
                }
            }),
        );

        assert_eq!(
            render(Some("show"), &response),
            concat!(
                "project Backend (backend)\n",
                "\n",
                "T1  Persist cooldowns  (merged)\n",
                "  note N1 [R1]: halfway\n",
                "  commit abc1234 [R1]: persist cooldowns\n",
            )
        );
    }

    #[test]
    fn ticket_show_renders_a_summary_then_the_body() {
        let response = ResponseEnvelope::success(
            None,
            json!({
                "ref": "TICK-1",
                "kind": "ticket",
                "value": {
                    "id": "TICK-1",
                    "name": "cooldown",
                    "state": "ready",
                    "project": "default",
                    "worktree": "sloop/TICK-1",
                    "blocked_by": ["TICK-0"],
                    "target": "claude",
                    "model": "opus",
                    "effort": "high",
                    "body": "# Persist cooldowns\n\nSurvive restarts.",
                }
            }),
        );

        assert_eq!(
            render(Some("show"), &response),
            concat!(
                "TICK-1  cooldown  (ready)\n",
                "project: default\n",
                "worktree: sloop/TICK-1\n",
                "blocked_by: TICK-0\n",
                "target: claude\n",
                "model: opus\n",
                "effort: high\n",
                "\n",
                "# Persist cooldowns\n\nSurvive restarts.\n",
            )
        );
    }

    #[test]
    fn ticket_show_without_a_body_renders_only_the_summary() {
        let response = ResponseEnvelope::success(
            None,
            json!({
                "ref": "T1",
                "kind": "ticket",
                "value": {"id": "T1", "name": "work", "state": "ready", "blocked_by": []}
            }),
        );

        assert_eq!(render(Some("show"), &response), "T1  work  (ready)\n");
    }

    #[test]
    fn run_show_renders_the_run_evidence_summary() {
        let response = ResponseEnvelope::success(
            None,
            json!({
                "ref": "R14",
                "kind": "run",
                "value": {
                    "id": "R14",
                    "alias": "TICK-1-r2",
                    "ticket": "TICK-1",
                    "ticket_name": "cooldown",
                    "state": "merged",
                    "terminal": true,
                    "branch": "sloop/R14-TICK-1",
                    "worktree": "/repo/.worktrees/R14",
                    "exit_code": 0,
                    "reason": null,
                    "classification": null,
                }
            }),
        );

        assert_eq!(
            render(Some("show"), &response),
            concat!(
                "TICK-1-r2  (merged)\n",
                "ticket: TICK-1  cooldown\n",
                "branch: sloop/R14-TICK-1\n",
                "worktree: /repo/.worktrees/R14\n",
                "agent exit: 0\n",
            )
        );
    }

    /// An instant today, so the span renders as bare `HH:MM` and the assertion
    /// does not depend on which day the suite runs.
    fn today_at(offset_ms: i64) -> i64 {
        let today = crate::clock::local_time(super::now_ms()).expect("local time");
        // Noon plus the offset: far enough from either midnight that a few
        // minutes either way cannot spill into another day.
        super::now_ms() - i64::from(today.hour) * 3_600_000 - i64::from(today.minute) * 60_000
            + 12 * 3_600_000
            + offset_ms
    }

    fn clock_at(offset_ms: i64) -> String {
        crate::clock::local_time(today_at(offset_ms))
            .expect("local time")
            .clock()
    }

    #[test]
    fn ticket_show_lists_runs_newest_first_with_a_stage_strip() {
        let response = ResponseEnvelope::success(
            None,
            json!({
                "ref": "TICK-1",
                "kind": "ticket",
                "value": {
                    "id": "TICK-1", "name": "cooldown", "state": "merged", "blocked_by": [],
                    "runs": [
                        {
                            "alias": "TICK-1-r2", "state": "merged",
                            "started_at_ms": today_at(0),
                            "finished_at_ms": today_at(360_000),
                            "stages": [
                                {"stage": "build", "state": "passed"},
                                {"stage": "test", "state": "passed"},
                                {"stage": "merge", "state": "passed"},
                            ],
                        },
                        {
                            "alias": "TICK-1-r1", "state": "needs_review",
                            "started_at_ms": today_at(-3_600_000),
                            "finished_at_ms": today_at(-3_240_000),
                            "stages": [
                                {"stage": "build", "state": "passed"},
                                {"stage": "test", "state": "failed"},
                                {"stage": "merge", "state": "pending"},
                            ],
                        },
                    ],
                }
            }),
        );

        assert_eq!(
            render(Some("show"), &response),
            format!(
                concat!(
                    "TICK-1  cooldown  (merged)\n",
                    "runs:\n",
                    "  TICK-1-r2  merged        {}-{}  build:ok  test:ok  merge:ok\n",
                    "  TICK-1-r1  needs_review  {}-{}  build:ok  test:FAIL  merge:-\n",
                ),
                clock_at(0),
                clock_at(360_000),
                clock_at(-3_600_000),
                clock_at(-3_240_000),
            )
        );
    }

    #[test]
    fn ticket_show_says_so_when_a_ticket_has_never_run() {
        let response = ResponseEnvelope::success(
            None,
            json!({
                "ref": "T1",
                "kind": "ticket",
                "value": {"id": "T1", "name": "work", "state": "ready", "blocked_by": [],
                          "runs": []}
            }),
        );

        assert_eq!(
            render(Some("show"), &response),
            "T1  work  (ready)\nruns: none\n"
        );
    }

    #[test]
    fn run_show_renders_stages_and_the_derived_reason() {
        let response = ResponseEnvelope::success(
            None,
            json!({
                "ref": "TICK-1-r1",
                "kind": "run",
                "value": {
                    "id": "R14", "alias": "TICK-1-r1", "ticket": "TICK-1",
                    "state": "needs_review", "terminal": true, "exit_code": 0,
                    "claimed_at_ms": today_at(0),
                    "started_at_ms": today_at(1_000),
                    "finished_at_ms": today_at(252_000),
                    "reason": "stage `test` failed (exit 1) after agent completed with commits",
                    "stages": [
                        {
                            "stage": "build", "state": "passed", "attempts": 1,
                            "started_at_ms": today_at(1_000),
                            "finished_at_ms": today_at(61_000),
                            "duration_ms": 60_000, "exit_code": 0,
                            "verdict_source": "exit_code",
                        },
                        {
                            "stage": "test", "state": "failed", "attempts": 2,
                            "started_at_ms": today_at(61_000),
                            "finished_at_ms": today_at(252_000),
                            "duration_ms": 191_000, "exit_code": 1,
                            "verdict_source": "exit_code",
                        },
                        {"stage": "merge", "state": "pending", "attempts": 0},
                    ],
                }
            }),
        );

        assert_eq!(
            render(Some("show"), &response),
            format!(
                concat!(
                    "TICK-1-r1  (needs_review)\n",
                    "ticket: TICK-1\n",
                    "timeline: claimed {}  started {}  finished {}\n",
                    "agent exit: 0\n",
                    "reason: stage `test` failed (exit 1) after agent completed with commits\n",
                    "stages:\n",
                    "  build  passed   {}-{}  1m0s  exit 0  verdict from exit_code\n",
                    "  test   failed   {}-{}  3m11s  exit 1  2 attempts  verdict from exit_code\n",
                    "  merge  pending  -\n",
                ),
                clock_at(0),
                clock_at(1_000),
                clock_at(252_000),
                clock_at(1_000),
                clock_at(61_000),
                clock_at(61_000),
                clock_at(252_000),
            )
        );
    }

    #[test]
    fn a_running_stage_renders_an_open_ended_span() {
        assert_eq!(
            super::span(Some(today_at(0)), None),
            format!("{}-...", clock_at(0))
        );
        assert_eq!(super::span(None, None), "-");
    }

    #[test]
    fn durations_use_the_coarsest_useful_unit() {
        assert_eq!(super::duration(9_400), "9s");
        assert_eq!(super::duration(191_000), "3m11s");
        assert_eq!(super::duration(3_900_000), "1h5m");
    }

    #[test]
    fn pause_and_resume_render_the_scheduler_state() {
        for (verb, paused, expected) in [
            ("pause", true, "scheduler paused\n"),
            ("resume", false, "scheduler resumed\n"),
        ] {
            let response = ResponseEnvelope::success(None, json!({"paused": paused}));
            assert_eq!(render(Some(verb), &response), expected);
        }
    }

    #[test]
    fn restart_renders_the_active_drain_count() {
        assert_eq!(
            render(
                Some("restart"),
                &ResponseEnvelope::success(None, json!({"active_runs": 1}))
            ),
            "daemon draining for restart (1 run active)\n"
        );
    }

    #[test]
    fn reindex_summarizes_the_rebuilt_state() {
        let response = ResponseEnvelope::success(
            None,
            json!({
                "projects_indexed": 2,
                "tickets_indexed": 5,
                "tickets_state_changed": 1,
                "state_changes": [
                    {"ticket": "T1", "previous_state": "ready", "state": "merged"}
                ],
                "rows_dropped": 7,
            }),
        );
        let text = render(Some("reindex"), &response);
        assert_eq!(
            text,
            "reindexed 2 projects and 5 tickets; 1 ticket states changed; 7 rows dropped\n"
        );
    }

    #[test]
    fn log_entries_render_timestamp_origin_and_text() {
        let response = ResponseEnvelope::success(
            None,
            json!({
                "run": "R1",
                "entries": [
                    {"timestamp": "2026-07-17T12:34:56Z", "source": "agent", "stage": null, "text": "hello"},
                    {"timestamp": "2026-07-17T12:34:57Z", "source": "aftercare", "stage": "test", "bytes_b64": "AAE="}
                ],
                "next_cursor": 2,
                "complete": true
            }),
        );

        let text = render(Some("logs"), &response);
        assert_eq!(
            text,
            "[2026-07-17T12:34:56Z] [agent] hello\n\
             [2026-07-17T12:34:57Z] [aftercare:test] <binary output>\n"
        );
    }
}
