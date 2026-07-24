//! Authored flow definitions live in sources.

use std::fmt;

use crate::flow::Flow;

pub mod markdown;

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
