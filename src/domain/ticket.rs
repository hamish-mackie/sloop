use serde::{Deserialize, Serialize};

use crate::outcome::Outcome;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TicketSnapshot {
    pub id: String,
    pub name: String,
    pub blocked_by: Vec<String>,
    pub worktree: Option<String>,
    pub target: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub body: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TicketState {
    Ready,
    Held,
    Claimed,
    Merged,
    Failed,
    NeedsReview,
}

impl TicketState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Held => "held",
            Self::Claimed => "claimed",
            Self::Merged => "merged",
            Self::Failed => "failed",
            Self::NeedsReview => "needs_review",
        }
    }

    pub fn after_outcome(outcome: Outcome) -> Self {
        match outcome {
            Outcome::Merged => Self::Merged,
            Outcome::Failed => Self::Failed,
            Outcome::NeedsReview => Self::NeedsReview,
            Outcome::Cancelled | Outcome::RateLimited | Outcome::Orphaned => Self::Ready,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TicketState;
    use crate::outcome::Outcome;

    #[test]
    fn outcomes_determine_the_next_ticket_state() {
        for (outcome, expected) in [
            (Outcome::Merged, TicketState::Merged),
            (Outcome::Failed, TicketState::Failed),
            (Outcome::NeedsReview, TicketState::NeedsReview),
            (Outcome::Cancelled, TicketState::Ready),
            (Outcome::RateLimited, TicketState::Ready),
            (Outcome::Orphaned, TicketState::Ready),
        ] {
            assert_eq!(TicketState::after_outcome(outcome), expected);
        }
    }
}
