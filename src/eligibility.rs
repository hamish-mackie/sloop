//! Why a ticket is not being dispatched right now. One pure decision used by
//! the dispatcher's gates and reported verbatim by the `list` verb, so the
//! operator sees exactly what the scheduler saw.

/// Global dispatcher gates, snapshotted at the moment of the question.
#[derive(Debug, Clone, Copy)]
pub struct Gates {
    pub paused: bool,
    pub storage_writable: bool,
    pub agent_configured: bool,
    pub hours_open: bool,
    pub at_capacity: bool,
    pub has_queued_activation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ineligible {
    Failed { attempts: i64 },
    Held,
    Blocked,
    Claimed { run: String },
    NoAgentConfigured,
    Paused,
    StorageFull,
    OutsideRunningHours,
    AtCapacity,
    NoActivation,
}

impl Ineligible {
    pub fn describe(&self) -> String {
        match self {
            Self::Failed { attempts } => {
                format!("failed after {attempts} attempt(s); requeue with `sloop retry`")
            }
            Self::Held => "held by operator; release with `sloop ready`".into(),
            Self::Blocked => "blocked".into(),
            Self::Claimed { run } => format!("claimed by run {run}"),
            Self::NoAgentConfigured => "no agent targets configured".into(),
            Self::Paused => "scheduler is paused; resume with `sloop resume`".into(),
            Self::StorageFull => {
                "database storage is full; free disk space to resume dispatch".into()
            }
            Self::OutsideRunningHours => "outside configured running hours".into(),
            Self::AtCapacity => "all agent slots are busy".into(),
            Self::NoActivation => "ready but no queued activation; enqueue with `sloop run`".into(),
        }
    }
}

/// Ticket-level reasons win over global gates: a failed ticket is failed
/// whether or not the scheduler is paused.
pub fn ticket_ineligibility(
    state: &str,
    attempts: i64,
    active_run: Option<&str>,
    gates: &Gates,
) -> Option<Ineligible> {
    match state {
        "failed" => return Some(Ineligible::Failed { attempts }),
        "held" => return Some(Ineligible::Held),
        "blocked" => return Some(Ineligible::Blocked),
        "claimed" => {
            return Some(Ineligible::Claimed {
                run: active_run.unwrap_or("?").to_owned(),
            });
        }
        "merged" | "needs_review" => return None,
        _ => {}
    }
    if !gates.agent_configured {
        Some(Ineligible::NoAgentConfigured)
    } else if gates.paused {
        Some(Ineligible::Paused)
    } else if !gates.storage_writable {
        Some(Ineligible::StorageFull)
    } else if !gates.hours_open {
        Some(Ineligible::OutsideRunningHours)
    } else if gates.at_capacity {
        Some(Ineligible::AtCapacity)
    } else if !gates.has_queued_activation {
        Some(Ineligible::NoActivation)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_gates() -> Gates {
        Gates {
            paused: false,
            storage_writable: true,
            agent_configured: true,
            hours_open: true,
            at_capacity: false,
            has_queued_activation: true,
        }
    }

    #[test]
    fn ticket_states_explain_themselves_before_global_gates() {
        let mut gates = open_gates();
        gates.paused = true; // ticket-level reasons must win over global ones
        assert!(matches!(
            ticket_ineligibility("failed", 2, None, &gates),
            Some(Ineligible::Failed { attempts: 2 })
        ));
        assert!(matches!(
            ticket_ineligibility("held", 0, None, &gates),
            Some(Ineligible::Held)
        ));
        assert!(matches!(
            ticket_ineligibility("blocked", 0, None, &gates),
            Some(Ineligible::Blocked)
        ));
        let claimed = ticket_ineligibility("claimed", 1, Some("R4"), &gates);
        assert!(matches!(claimed, Some(Ineligible::Claimed { ref run }) if run == "R4"));
    }

    #[test]
    fn terminal_states_need_no_reason() {
        let gates = open_gates();
        assert!(ticket_ineligibility("merged", 1, None, &gates).is_none());
        assert!(ticket_ineligibility("needs_review", 1, None, &gates).is_none());
    }

    #[test]
    fn global_gates_apply_to_ready_tickets_in_priority_order() {
        let mut gates = open_gates();
        gates.agent_configured = false;
        gates.paused = true;
        assert!(matches!(
            ticket_ineligibility("ready", 0, None, &gates),
            Some(Ineligible::NoAgentConfigured)
        ));

        let mut gates = open_gates();
        gates.paused = true;
        assert!(matches!(
            ticket_ineligibility("ready", 0, None, &gates),
            Some(Ineligible::Paused)
        ));

        let mut gates = open_gates();
        gates.storage_writable = false;
        assert!(matches!(
            ticket_ineligibility("ready", 0, None, &gates),
            Some(Ineligible::StorageFull)
        ));

        let mut gates = open_gates();
        gates.hours_open = false;
        assert!(matches!(
            ticket_ineligibility("ready", 0, None, &gates),
            Some(Ineligible::OutsideRunningHours)
        ));

        let mut gates = open_gates();
        gates.at_capacity = true;
        assert!(matches!(
            ticket_ineligibility("ready", 0, None, &gates),
            Some(Ineligible::AtCapacity)
        ));

        let mut gates = open_gates();
        gates.has_queued_activation = false;
        assert!(matches!(
            ticket_ineligibility("ready", 0, None, &gates),
            Some(Ineligible::NoActivation)
        ));
    }

    #[test]
    fn a_dispatchable_ready_ticket_has_no_reason() {
        assert!(ticket_ineligibility("ready", 0, None, &open_gates()).is_none());
    }

    #[test]
    fn descriptions_are_actionable() {
        assert_eq!(
            Ineligible::Failed { attempts: 2 }.describe(),
            "failed after 2 attempt(s); requeue with `sloop retry`"
        );
        assert_eq!(
            Ineligible::NoActivation.describe(),
            "ready but no queued activation; enqueue with `sloop run`"
        );
    }
}
