use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use serde::Deserialize;
use serde_json::json;

use crate::frontmatter::Frontmatter;
use crate::outcome::Outcome;

use super::{AuthoredTicket, SourceError, TicketSource};

pub struct ExecTicketSource {
    root: PathBuf,
    argv: Vec<String>,
}

impl ExecTicketSource {
    pub fn new(root: impl Into<PathBuf>, argv: Vec<String>) -> Self {
        Self {
            root: root.into(),
            argv,
        }
    }

    fn invoke(&self, request: &serde_json::Value) -> Result<std::process::Output, SourceError> {
        let (program, arguments) = self
            .argv
            .split_first()
            .ok_or_else(|| SourceError::new("sources.tickets.exec must name a command"))?;
        let mut child = Command::new(program)
            .args(arguments)
            .current_dir(&self.root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| {
                SourceError::new(format!("cannot start ticket source `{program}`: {error}"))
            })?;
        let input = serde_json::to_vec(request)
            .map_err(|error| SourceError::new(format!("cannot encode source request: {error}")))?;
        let write_result = child
            .stdin
            .take()
            .expect("piped source stdin is available")
            .write_all(&input);
        if let Err(error) = write_result {
            let _ = child.kill();
            let _ = child.wait();
            return Err(SourceError::new(format!(
                "cannot write to ticket source `{program}`: {error}"
            )));
        }
        child.wait_with_output().map_err(|error| {
            SourceError::new(format!(
                "cannot wait for ticket source `{program}`: {error}"
            ))
        })
    }
}

impl TicketSource for ExecTicketSource {
    fn pull(&self) -> Result<Vec<AuthoredTicket>, SourceError> {
        let output = self.invoke(&json!({ "verb": "pull" }))?;
        if !output.status.success() {
            return Err(command_failed(&self.argv, &output));
        }
        parse_tickets(&output.stdout)
    }

    fn report(&self, ticket_id: &str, outcome: &Outcome) -> Result<(), SourceError> {
        let output = self.invoke(&json!({
            "verb": "report",
            "ticket": ticket_id,
            "outcome": outcome,
        }))?;
        if output.status.success() {
            Ok(())
        } else {
            Err(command_failed(&self.argv, &output))
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecTicket {
    id: Option<String>,
    name: String,
    project: Option<String>,
    #[serde(default)]
    blocked_by: Vec<String>,
    target: Option<String>,
    model: Option<String>,
    effort: Option<String>,
    flow: Option<String>,
    body: String,
}

fn parse_tickets(output: &[u8]) -> Result<Vec<AuthoredTicket>, SourceError> {
    let tickets: Vec<ExecTicket> = serde_json::from_slice(output)
        .map_err(|error| SourceError::new(format!("invalid ticket source output: {error}")))?;
    Ok(tickets
        .into_iter()
        .enumerate()
        .map(|(index, ticket)| {
            let source_ref = ticket.id.clone().unwrap_or_else(|| format!("row:{index}"));
            let mut frontmatter = Frontmatter::sourced(ticket.name, ticket.blocked_by);
            frontmatter.id = ticket.id;
            frontmatter.project = ticket.project;
            frontmatter.target = ticket.target;
            frontmatter.model = ticket.model;
            frontmatter.effort = ticket.effort;
            frontmatter.flow = ticket.flow;
            AuthoredTicket {
                frontmatter,
                body: ticket.body,
                source: "exec".into(),
                source_ref,
                file_path: None,
                original_content: None,
                validation_error: None,
            }
        })
        .collect())
}

fn command_failed(argv: &[String], output: &std::process::Output) -> SourceError {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let detail = stderr.trim();
    let detail = if detail.is_empty() {
        String::new()
    } else {
        format!(": {detail}")
    };
    SourceError::new(format!(
        "ticket source `{}` exited with {}{detail}",
        argv.join(" "),
        output.status
    ))
}

#[cfg(test)]
mod tests {
    use super::parse_tickets;

    #[test]
    fn valid_rows_map_fields_and_apply_defaults() {
        let tickets = parse_tickets(
            br#"[
                {"id":"EXT-1","name":"One","project":"docs","blocked_by":["EXT-0"],"target":"codex","model":"o3","effort":"high","flow":"release","body":"First"},
                {"name":"Two","body":"Second"}
            ]"#,
        )
        .unwrap();

        assert_eq!(tickets[0].source_ref, "EXT-1");
        assert_eq!(tickets[0].frontmatter.blocked_by, ["EXT-0"]);
        assert_eq!(tickets[0].frontmatter.flow.as_deref(), Some("release"));
        assert_eq!(tickets[1].source_ref, "row:1");
        assert!(tickets[1].frontmatter.blocked_by.is_empty());
        assert!(tickets[1].frontmatter.has_blocked_by());
        assert_eq!(tickets[1].body, "Second");
    }

    #[test]
    fn malformed_json_is_rejected() {
        let error = parse_tickets(br#"[{"name":"One","body":"work"}"#).unwrap_err();
        assert!(error.to_string().contains("invalid ticket source output"));
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let error =
            parse_tickets(br#"[{"name":"One","body":"work","status":"ready"}]"#).unwrap_err();
        assert!(
            error.to_string().contains("unknown field `status`"),
            "{error}"
        );
    }
}
