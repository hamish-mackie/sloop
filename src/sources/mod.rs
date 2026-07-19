//! Authored ticket and flow definitions live in sources. Sloop borrows them
//! into its derived index: `post` validates a Markdown push against its author,
//! while `pull` keeps invalid tickets visible and held when they are upserted.

use std::fmt;
use std::path::PathBuf;

use crate::flow::Flow;
use crate::frontmatter::Frontmatter;
use crate::outcome::Outcome;

pub mod exec;
pub mod markdown;

#[derive(Debug, Clone)]
pub struct AuthoredTicket {
    pub frontmatter: Frontmatter,
    pub body: String,
    pub source: String,
    pub source_ref: String,
    pub file_path: Option<PathBuf>,
    pub original_content: Option<String>,
    pub validation_error: Option<String>,
}

pub trait TicketSource: Send + Sync {
    fn pull(&self) -> Result<Vec<AuthoredTicket>, SourceError>;
    fn report(&self, ticket_id: &str, outcome: &Outcome) -> Result<(), SourceError>;
}

pub trait FlowSource {
    fn pull(&self) -> Result<Vec<Flow>, SourceError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceError(String);

impl SourceError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for SourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for SourceError {}
