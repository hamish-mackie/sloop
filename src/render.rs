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

    for (title, items) in [
        ("runs", &data["runs"]),
        ("queued", &data["queued_activations"]),
    ] {
        let items = items.as_array().map(Vec::as_slice).unwrap_or_default();
        if items.is_empty() {
            let _ = writeln!(text, "{title}: none");
            continue;
        }
        let _ = writeln!(text, "{title}:");
        for item in items {
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
    let mut rendered = format!(
        "run {} {}\n",
        data["run"].as_str().unwrap_or("?"),
        data["state"].as_str().unwrap_or("?"),
    );
    if let Some(reason) = data["reason"].as_str() {
        let _ = writeln!(rendered, "reason: {reason}");
    }
    rendered
}

fn render_cancel(data: &Value) -> String {
    let mut text = format!("run {} cancelling\n", data["run"].as_str().unwrap_or("?"));
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
    if entries.is_empty() {
        return format!(
            "no output captured for run {}\n",
            data["run"].as_str().unwrap_or("?")
        );
    }
    let mut text = String::new();
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
        Some("ticket") => render_ticket_show(data),
        Some("run") => render_run_show(data),
        Some("project") => render_project_show(data),
        _ => fallback(data),
    }
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
    if let Some(body) = value["body"]
        .as_str()
        .map(str::trim)
        .filter(|body| !body.is_empty())
    {
        let _ = write!(text, "\n{body}\n");
    }
    text
}

/// A run's identity and settled evidence, one fact per line.
fn render_run_show(data: &Value) -> String {
    let value = &data["value"];
    let mut text = format!(
        "{}  ({})\n",
        value["id"].as_str().unwrap_or("?"),
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
    if let Some(exit_code) = value["exit_code"].as_i64() {
        let _ = writeln!(text, "exit: {exit_code}");
    }
    if let Some(reason) = value["reason"].as_str() {
        let _ = writeln!(text, "reason: {reason}");
    }
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
                "runs": [{"id": "R1", "state": "running", "ticket": "T1", "project": "default"}],
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
            text.contains("R1 running (ticket T1, project default)"),
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
                "R14  (merged)\n",
                "ticket: TICK-1  cooldown\n",
                "branch: sloop/R14-TICK-1\n",
                "worktree: /repo/.worktrees/R14\n",
                "exit: 0\n",
            )
        );
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
