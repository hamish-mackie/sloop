use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::flow::Flow;

use super::{FlowSource, SourceError};

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

fn io_error(path: &Path, error: io::Error) -> SourceError {
    SourceError::new(format!("{}: {error}", path.display()))
}
