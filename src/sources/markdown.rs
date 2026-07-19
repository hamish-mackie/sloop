use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::flow::Flow;
use crate::frontmatter::{self, Frontmatter};
use crate::outcome::Outcome;
use crate::post::parse_ticket_frontmatter;

use super::{AuthoredTicket, FlowSource, SourceError, TicketSource};

pub struct MarkdownTicketSource {
    root: PathBuf,
    ticket_dir: PathBuf,
}

impl MarkdownTicketSource {
    pub fn new(root: impl Into<PathBuf>, ticket_dir: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            ticket_dir: ticket_dir.into(),
        }
    }
}

impl TicketSource for MarkdownTicketSource {
    fn pull(&self) -> Result<Vec<AuthoredTicket>, SourceError> {
        let directory = self.root.join(&self.ticket_dir);
        let mut paths = Vec::new();
        collect_markdown_files(&directory, &mut paths)?;
        paths.sort();

        paths
            .into_iter()
            .map(|path| {
                let relative = path.strip_prefix(&self.root).map_err(|_| {
                    SourceError::new(format!(
                        "ticket path `{}` is outside repository `{}`",
                        path.display(),
                        self.root.display()
                    ))
                })?;
                let source_ref = relative.to_string_lossy().into_owned();
                let content = fs::read_to_string(&path).map_err(|error| io_error(&path, error))?;
                let (frontmatter, validation_error) =
                    match parse_ticket_frontmatter(&content, &source_ref) {
                        Ok(frontmatter) => (frontmatter, None),
                        Err(error) => (recover_frontmatter(&content), Some(error.to_string())),
                    };
                let body = frontmatter::body(&content).unwrap_or_default().to_owned();
                Ok(AuthoredTicket {
                    frontmatter,
                    body,
                    source: "local".into(),
                    source_ref,
                    file_path: Some(relative.to_path_buf()),
                    original_content: Some(content),
                    validation_error,
                })
            })
            .collect()
    }

    fn report(&self, _ticket_id: &str, _outcome: &Outcome) -> Result<(), SourceError> {
        Ok(())
    }
}

pub struct MarkdownFlowSource {
    root: PathBuf,
}

impl MarkdownFlowSource {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl FlowSource for MarkdownFlowSource {
    fn pull(&self) -> Result<Vec<Flow>, SourceError> {
        let directory = self.root.join(".agents/sloop/flows");
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(io_error(&directory, error)),
        };
        let mut paths = entries
            .map(|entry| {
                entry
                    .map(|entry| entry.path())
                    .map_err(|error| io_error(&directory, error))
            })
            .collect::<Result<Vec<_>, _>>()?;
        paths.retain(|path| path.is_file() && path.extension().is_some_and(|ext| ext == "yaml"));
        paths.sort();

        paths
            .into_iter()
            .map(|path| {
                let name = path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .ok_or_else(|| {
                        SourceError::new(format!(
                            "{}: flow filename must be valid UTF-8",
                            path.display()
                        ))
                    })?;
                let contents = fs::read_to_string(&path).map_err(|error| io_error(&path, error))?;
                crate::flow::parse(name, &contents)
                    .map_err(|message| SourceError::new(format!("{}: {message}", path.display())))
            })
            .collect()
    }
}

fn collect_markdown_files(directory: &Path, paths: &mut Vec<PathBuf>) -> Result<(), SourceError> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io_error(directory, error)),
    };
    for entry in entries {
        let path = entry.map_err(|error| io_error(directory, error))?.path();
        if path.is_dir() {
            collect_markdown_files(&path, paths)?;
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("md") {
            paths.push(path);
        }
    }
    Ok(())
}

fn recover_frontmatter(content: &str) -> Frontmatter {
    if let Ok(frontmatter) = frontmatter::parse(content) {
        return frontmatter;
    }
    let Some(yaml) = raw_yaml(content) else {
        return Frontmatter::default();
    };
    let Ok(value) = serde_yaml::from_str::<serde_yaml::Value>(yaml) else {
        return Frontmatter::default();
    };
    let Some(mapping) = value.as_mapping() else {
        return Frontmatter::default();
    };

    let blocked_by = mapping
        .get("blocked_by")
        .and_then(serde_yaml::Value::as_sequence)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_yaml::Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let mut recovered =
        Frontmatter::sourced(scalar(mapping, "name").unwrap_or_default(), blocked_by);
    recovered.id = scalar(mapping, "id");
    recovered.project = scalar(mapping, "project");
    recovered.title = scalar(mapping, "title");
    recovered.worktree = scalar(mapping, "worktree");
    recovered.target = scalar(mapping, "target");
    recovered.model = scalar(mapping, "model");
    recovered.effort = scalar(mapping, "effort");
    recovered.flow = scalar(mapping, "flow");
    recovered
}

fn raw_yaml(content: &str) -> Option<&str> {
    let after_open = content.strip_prefix("---\n")?;
    let end = after_open
        .split_inclusive('\n')
        .scan(0, |offset, line| {
            let start = *offset;
            *offset += line.len();
            Some((start, line))
        })
        .find_map(|(offset, line)| (line == "---\n" || line == "---").then_some(offset))
        .unwrap_or(after_open.len());
    Some(&after_open[..end])
}

fn scalar(mapping: &serde_yaml::Mapping, key: &str) -> Option<String> {
    match mapping.get(key) {
        Some(serde_yaml::Value::String(value)) => Some(value.clone()),
        Some(serde_yaml::Value::Number(value)) => Some(value.to_string()),
        _ => None,
    }
}

fn io_error(path: &Path, error: io::Error) -> SourceError {
    SourceError::new(format!("{}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::MarkdownTicketSource;
    use crate::sources::TicketSource;

    #[test]
    fn invalid_files_are_returned_with_recovered_identity() {
        let root = tempdir().unwrap();
        let directory = root.path().join("tickets");
        fs::create_dir(&directory).unwrap();
        let content = "---\nid: T1\nproject: example\nname: Broken\nblocked_by: T0\n---\nbody\n";
        fs::write(directory.join("invalid.md"), content).unwrap();

        let tickets = MarkdownTicketSource::new(root.path(), "tickets")
            .pull()
            .unwrap();

        assert_eq!(tickets.len(), 1);
        assert_eq!(tickets[0].frontmatter.id.as_deref(), Some("T1"));
        assert_eq!(tickets[0].frontmatter.project.as_deref(), Some("example"));
        assert!(tickets[0].validation_error.is_some());
        assert_eq!(tickets[0].source_ref, "tickets/invalid.md");
        assert_eq!(tickets[0].original_content.as_deref(), Some(content));
    }

    #[test]
    fn unterminated_parseable_yaml_still_recovers_the_id() {
        let root = tempdir().unwrap();
        let directory = root.path().join("tickets");
        fs::create_dir(&directory).unwrap();
        fs::write(directory.join("invalid.md"), "---\nid: T2\n").unwrap();

        let tickets = MarkdownTicketSource::new(root.path(), "tickets")
            .pull()
            .unwrap();

        assert_eq!(tickets[0].frontmatter.id.as_deref(), Some("T2"));
        assert!(tickets[0].validation_error.is_some());
    }
}
