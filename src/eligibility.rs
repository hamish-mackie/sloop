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
    Blocked { blockers: Vec<String> },
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
            Self::Blocked { blockers } => {
                format!("blocked by unmerged {}", blockers.join(", "))
            }
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

/// Blocked is derived from dependency state rather than persisted on the
/// ticket, so display state changes as soon as the last blocker merges.
pub fn display_state<'a>(state: &'a str, reason: Option<&Ineligible>) -> &'a str {
    if matches!(reason, Some(Ineligible::Blocked { .. })) {
        "blocked"
    } else {
        state
    }
}

/// Ticket-level reasons win over global gates: a failed ticket is failed
/// whether or not the scheduler is paused.
pub fn ticket_ineligibility(
    state: &str,
    attempts: i64,
    active_run: Option<&str>,
    unmerged_blockers: &[String],
    gates: &Gates,
) -> Option<Ineligible> {
    match state {
        "failed" => return Some(Ineligible::Failed { attempts }),
        "held" => return Some(Ineligible::Held),
        "claimed" => {
            return Some(Ineligible::Claimed {
                run: active_run.unwrap_or("?").to_owned(),
            });
        }
        "merged" | "needs_review" => return None,
        _ => {}
    }
    if !unmerged_blockers.is_empty() {
        return Some(Ineligible::Blocked {
            blockers: unmerged_blockers.to_vec(),
        });
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

    fn ineligibility(
        state: &str,
        attempts: i64,
        active_run: Option<&str>,
        gates: &Gates,
    ) -> Option<Ineligible> {
        ticket_ineligibility(state, attempts, active_run, &[], gates)
    }

    #[test]
    fn ticket_states_explain_themselves_before_global_gates() {
        let mut gates = open_gates();
        gates.paused = true; // ticket-level reasons must win over global ones
        assert!(matches!(
            ineligibility("failed", 2, None, &gates),
            Some(Ineligible::Failed { attempts: 2 })
        ));
        assert!(matches!(
            ineligibility("held", 0, None, &gates),
            Some(Ineligible::Held)
        ));
        let claimed = ineligibility("claimed", 1, Some("R4"), &gates);
        assert!(matches!(claimed, Some(Ineligible::Claimed { ref run }) if run == "R4"));
    }

    #[test]
    fn blockers_are_named_before_global_gate_reasons() {
        let mut gates = open_gates();
        gates.paused = true;
        let blockers = vec!["T1".into(), "T2".into()];
        let reason = ticket_ineligibility("ready", 0, None, &blockers, &gates);

        assert_eq!(
            reason,
            Some(Ineligible::Blocked {
                blockers: blockers.clone()
            })
        );
        assert_eq!(
            reason.as_ref().unwrap().describe(),
            "blocked by unmerged T1, T2"
        );
        assert_eq!(display_state("ready", reason.as_ref()), "blocked");
    }

    #[test]
    fn terminal_states_need_no_reason() {
        let gates = open_gates();
        assert!(ineligibility("merged", 1, None, &gates).is_none());
        assert!(ineligibility("needs_review", 1, None, &gates).is_none());
    }

    #[test]
    fn global_gates_apply_to_ready_tickets_in_priority_order() {
        let mut gates = open_gates();
        gates.agent_configured = false;
        gates.paused = true;
        assert!(matches!(
            ineligibility("ready", 0, None, &gates),
            Some(Ineligible::NoAgentConfigured)
        ));

        let mut gates = open_gates();
        gates.paused = true;
        assert!(matches!(
            ineligibility("ready", 0, None, &gates),
            Some(Ineligible::Paused)
        ));

        let mut gates = open_gates();
        gates.storage_writable = false;
        assert!(matches!(
            ineligibility("ready", 0, None, &gates),
            Some(Ineligible::StorageFull)
        ));

        let mut gates = open_gates();
        gates.hours_open = false;
        assert!(matches!(
            ineligibility("ready", 0, None, &gates),
            Some(Ineligible::OutsideRunningHours)
        ));

        let mut gates = open_gates();
        gates.at_capacity = true;
        assert!(matches!(
            ineligibility("ready", 0, None, &gates),
            Some(Ineligible::AtCapacity)
        ));

        let mut gates = open_gates();
        gates.has_queued_activation = false;
        assert!(matches!(
            ineligibility("ready", 0, None, &gates),
            Some(Ineligible::NoActivation)
        ));
    }

    #[test]
    fn a_dispatchable_ready_ticket_has_no_reason() {
        assert!(ineligibility("ready", 0, None, &open_gates()).is_none());
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
