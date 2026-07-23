use serde::{Deserialize, Serialize};

use crate::domain::ticket::TicketState;
use crate::outcome::Outcome;

const NEEDS_REVIEW_REASON: &str = "needs-review";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceVersion(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TicketRef {
    pub id: String,
    pub source: String,
    pub source_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionHints {
    pub target: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub flow: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkTicket {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub body: String,
    pub state: WorkTicketState,
    pub blocked_by: Vec<String>,
    pub attempts: u32,
    pub hints: ExecutionHints,
    pub version: SourceVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkTicketState {
    Ready,
    Blocked,
    Held { reason: String },
    Claimed { by: OwnerId },
    Done,
    Failed,
}

impl WorkTicketState {
    pub fn from_ticket_state(
        state: TicketState,
        blocked: bool,
        held_reason: String,
        owner: OwnerId,
    ) -> Self {
        match state {
            TicketState::Ready if blocked => Self::Blocked,
            TicketState::Ready => Self::Ready,
            TicketState::Held => Self::Held {
                reason: held_reason,
            },
            TicketState::Claimed => Self::Claimed { by: owner },
            TicketState::Merged => Self::Done,
            TicketState::Failed => Self::Failed,
            TicketState::NeedsReview => Self::Held {
                reason: NEEDS_REVIEW_REASON.into(),
            },
        }
    }

    pub fn to_ticket_state(&self) -> TicketState {
        match self {
            Self::Ready | Self::Blocked => TicketState::Ready,
            Self::Held { reason } if reason == NEEDS_REVIEW_REASON => TicketState::NeedsReview,
            Self::Held { .. } => TicketState::Held,
            Self::Claimed { .. } => TicketState::Claimed,
            Self::Done => TicketState::Merged,
            Self::Failed => TicketState::Failed,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Disposition {
    Retry { not_before_ms: Option<i64> },
    Park { reason: String },
    Abandon,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkOutcome {
    pub ticket_id: String,
    pub owner: OwnerId,
    #[serde(with = "outcome_serde")]
    pub verdict: Outcome,
    pub branch: Option<String>,
    pub commit_count: u32,
    pub attempt: u32,
    pub finished_at_ms: i64,
}

mod outcome_serde {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    use crate::outcome::Outcome;

    pub fn serialize<S>(outcome: &Outcome, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(outcome.as_str())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Outcome, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        match value.as_str() {
            "merged" => Ok(Outcome::Merged),
            "failed" => Ok(Outcome::Failed),
            "needs_review" => Ok(Outcome::NeedsReview),
            "cancelled" => Ok(Outcome::Cancelled),
            "rate_limited" => Ok(Outcome::RateLimited),
            "orphaned" => Ok(Outcome::Orphaned),
            _ => Err(D::Error::unknown_variant(
                &value,
                &[
                    "merged",
                    "failed",
                    "needs_review",
                    "cancelled",
                    "rate_limited",
                    "orphaned",
                ],
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticket_states_round_trip_through_work_ticket_states() {
        let owner = OwnerId("daemon-1".into());
        for (state, expected) in [
            (TicketState::Ready, WorkTicketState::Ready),
            (
                TicketState::Held,
                WorkTicketState::Held {
                    reason: "operator".into(),
                },
            ),
            (
                TicketState::Claimed,
                WorkTicketState::Claimed { by: owner.clone() },
            ),
            (TicketState::Merged, WorkTicketState::Done),
            (TicketState::Failed, WorkTicketState::Failed),
            (
                TicketState::NeedsReview,
                WorkTicketState::Held {
                    reason: NEEDS_REVIEW_REASON.into(),
                },
            ),
        ] {
            let work_state =
                WorkTicketState::from_ticket_state(state, false, "operator".into(), owner.clone());
            assert_eq!(work_state, expected);
            assert_eq!(work_state.to_ticket_state(), state);
        }
    }

    #[test]
    fn blocked_is_the_portable_view_of_a_blocked_ready_ticket() {
        let state = WorkTicketState::from_ticket_state(
            TicketState::Ready,
            true,
            String::new(),
            OwnerId(String::new()),
        );

        assert_eq!(state, WorkTicketState::Blocked);
        assert_eq!(state.to_ticket_state(), TicketState::Ready);
    }

    #[test]
    fn work_outcomes_round_trip_all_verdicts_through_json() {
        for verdict in [
            Outcome::Merged,
            Outcome::Failed,
            Outcome::NeedsReview,
            Outcome::Cancelled,
            Outcome::RateLimited,
            Outcome::Orphaned,
        ] {
            let outcome = WorkOutcome {
                ticket_id: "T1".into(),
                owner: OwnerId("daemon-1".into()),
                verdict,
                branch: Some("sloop/t1".into()),
                commit_count: 2,
                attempt: 1,
                finished_at_ms: 42,
            };
            let json = serde_json::to_string(&outcome).unwrap();
            let restored: WorkOutcome = serde_json::from_str(&json).unwrap();

            assert_eq!(restored, outcome);
        }
    }
}
