use std::fmt;
use std::io;
use std::path::Path;

#[derive(Debug)]
pub struct ReindexError(pub(crate) String);

impl ReindexError {
    pub(crate) fn io(path: &Path, source: io::Error) -> Self {
        Self(format!("{}: {source}", path.display()))
    }
}

impl fmt::Display for ReindexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ReindexError {}
