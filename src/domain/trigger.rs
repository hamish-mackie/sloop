//! The trigger: the durable record that demand for work exists.
//!
//! A trigger is what makes the dispatcher pick a ticket up. This module owns
//! the pure half of the concept: its kinds, its states, whether one is due,
//! the arithmetic that rearms a recurring one, and the transition every write
//! is derived from. Storage lives behind the coordination boundary in
//! `work_state::trigger`; nothing here reads a clock, a database, or a file.

/// What kind of demand a trigger records. The kind is also what decides how
/// the trigger becomes due and what firing it does next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerKind {
    /// `sloop run` — due the moment the gates allow it.
    Immediate,
    /// `sloop post --auto` — the ticket asked for itself; no schedule.
    Auto,
    /// `sloop run --at HH:MM` — due once, at or after an instant.
    At,
    /// `sloop run --every <interval>` — due repeatedly, rearming per fire.
    Every,
    /// `sloop run --overnight` — due once, at or after the next opening of
    /// running hours.
    Overnight,
}

impl TriggerKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Immediate => "immediate",
            Self::Auto => "auto",
            Self::At => "at",
            Self::Every => "every",
            Self::Overnight => "overnight",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "immediate" => Some(Self::Immediate),
            "auto" => Some(Self::Auto),
            "at" => Some(Self::At),
            "every" => Some(Self::Every),
            "overnight" => Some(Self::Overnight),
            _ => None,
        }
    }

    /// Whether this kind carries no schedule, so a queued trigger is due on
    /// sight. Kinds that do carry one are due only once their instant passes.
    pub fn fires_on_sight(self) -> bool {
        matches!(self, Self::Immediate | Self::Auto)
    }

    /// Whether firing rearms the trigger rather than retiring it.
    pub fn recurs(self) -> bool {
        matches!(self, Self::Every)
    }
}

/// A trigger's lifecycle state. `queued` is demand not yet met; the other two
/// are terminal and never dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerState {
    Queued,
    Completed,
    Cancelled,
}

impl TriggerState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "queued" => Some(Self::Queued),
            "completed" => Some(Self::Completed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

/// A trigger's schedulable state: everything due-ness and [`step`] need, and
/// nothing about which ticket or project the demand points at. Targeting is a
/// storage-side join; whether the demand is live is a decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trigger {
    pub state: TriggerState,
    pub kind: TriggerKind,
    pub eligible_at_ms: Option<i64>,
    pub interval_ms: Option<i64>,
}

impl Trigger {
    /// The definition of due-ness. The SQL scan in `work_state::trigger`
    /// carries a predicate that mirrors this so a large queue does not have to
    /// be read into memory to be filtered, but this function is the
    /// definition, and a test asserts the two agree over the whole matrix of
    /// kinds, states, and schedules.
    ///
    /// A schedule-carrying kind with no `eligible_at_ms` is never due: an
    /// unscheduled `at` is a corrupt row, and answering "due" would fire it
    /// immediately, which is the opposite of what it asked for. That matches
    /// SQL, where `NULL <= ?` is not true.
    pub fn is_due(&self, now_ms: i64) -> bool {
        self.state == TriggerState::Queued
            && (self.kind.fires_on_sight()
                || self
                    .eligible_at_ms
                    .is_some_and(|eligible_at_ms| eligible_at_ms <= now_ms))
    }
}

/// What happened to a trigger. Events are evidence the daemon observed, never
/// an intent to write a particular row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    /// A claim consumed the trigger.
    Fired,
    /// The demand can never be met — the ticket it is pinned to has merged.
    Completed,
    /// A run gave the ticket back and the trigger returns to the queue, re-timed
    /// to the retry's earliest instant.
    Requeued { eligible_at_ms: i64 },
}

/// What a transition asks storage to persist. One variant per named write in
/// `work_state::trigger`; there is no effect for "leave it alone", so an empty
/// result means the event was a no-op.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    /// Stay queued, with a new eligibility instant. Only a recurring trigger
    /// rearms.
    Rearm { eligible_at_ms: i64 },
    /// Retire the trigger.
    Complete,
    /// Return the trigger to the queue at this instant.
    Requeue { eligible_at_ms: i64 },
    /// The trigger cannot be honoured and no write should follow.
    Fault(Fault),
}

/// A trigger whose own fields contradict its kind. Faults are data problems,
/// not race outcomes, so they surface as corruption rather than denial.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// A recurring trigger with no usable `(eligible_at_ms, interval_ms)`
    /// pair: firing it could not compute a next instant, so it would either
    /// spin or silently stop recurring.
    InvalidCadence,
}

/// The trigger transition. `trigger` is advanced in place and the returned
/// effects are what storage must persist, in order.
///
/// Every terminal state is absorbing: replaying an event against a completed
/// or cancelled trigger yields no effects, which is what makes the recovery
/// and sweep paths idempotent.
pub fn step(trigger: &mut Trigger, event: Event, now_ms: i64) -> Vec<Effect> {
    match event {
        Event::Fired => {
            if trigger.state != TriggerState::Queued {
                return Vec::new();
            }
            if !trigger.kind.recurs() {
                trigger.state = TriggerState::Completed;
                return vec![Effect::Complete];
            }
            let rearmed = trigger.eligible_at_ms.zip(trigger.interval_ms).and_then(
                |(eligible_at_ms, interval_ms)| rearm_every_at(eligible_at_ms, interval_ms, now_ms),
            );
            match rearmed {
                Some(eligible_at_ms) => {
                    trigger.eligible_at_ms = Some(eligible_at_ms);
                    vec![Effect::Rearm { eligible_at_ms }]
                }
                None => vec![Effect::Fault(Fault::InvalidCadence)],
            }
        }
        // Kind is deliberately not consulted. A recurring trigger pinned to a
        // merged ticket is as unfireable as a one-shot one, and leaving it
        // queued is demand that can never be met but is still counted.
        Event::Completed => {
            if trigger.state != TriggerState::Queued {
                return Vec::new();
            }
            trigger.state = TriggerState::Completed;
            vec![Effect::Complete]
        }
        Event::Requeued { eligible_at_ms } => {
            if trigger.state == TriggerState::Cancelled {
                return Vec::new();
            }
            trigger.state = TriggerState::Queued;
            trigger.eligible_at_ms = Some(eligible_at_ms);
            vec![Effect::Requeue { eligible_at_ms }]
        }
    }
}

/// The next instant a recurring trigger becomes due, given the instant it was
/// due and its cadence.
///
/// Missed intervals collapse into one step rather than replaying: a daemon
/// asleep for an hour on a one-minute cadence owes one run, not sixty. The
/// result is always strictly after `now_ms`, so a rearm can never leave the
/// trigger due again in the same tick.
pub fn rearm_every_at(eligible_at_ms: i64, interval_ms: i64, now_ms: i64) -> Option<i64> {
    if interval_ms <= 0 || eligible_at_ms > now_ms {
        return None;
    }
    let missed = now_ms.checked_sub(eligible_at_ms)?.div_euclid(interval_ms);
    let steps = missed.checked_add(1)?;
    eligible_at_ms.checked_add(interval_ms.checked_mul(steps)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queued(kind: TriggerKind, eligible_at_ms: Option<i64>) -> Trigger {
        Trigger {
            state: TriggerState::Queued,
            kind,
            eligible_at_ms,
            interval_ms: None,
        }
    }

    #[test]
    fn kinds_and_states_round_trip_through_their_stored_spelling() {
        for kind in [
            TriggerKind::Immediate,
            TriggerKind::Auto,
            TriggerKind::At,
            TriggerKind::Every,
            TriggerKind::Overnight,
        ] {
            assert_eq!(TriggerKind::parse(kind.as_str()), Some(kind));
        }
        for state in [
            TriggerState::Queued,
            TriggerState::Completed,
            TriggerState::Cancelled,
        ] {
            assert_eq!(TriggerState::parse(state.as_str()), Some(state));
        }
        assert_eq!(TriggerKind::parse("eventually"), None);
        assert_eq!(TriggerState::parse("pending"), None);
    }

    #[test]
    fn scheduleless_kinds_are_due_on_sight_and_scheduled_kinds_wait() {
        assert!(queued(TriggerKind::Immediate, None).is_due(1_000));
        assert!(queued(TriggerKind::Auto, None).is_due(1_000));
        // A schedule on a scheduleless kind is ignored rather than obeyed.
        assert!(queued(TriggerKind::Immediate, Some(9_000)).is_due(1_000));

        for kind in [TriggerKind::At, TriggerKind::Every, TriggerKind::Overnight] {
            assert!(!queued(kind, Some(1_001)).is_due(1_000), "{kind:?} early");
            assert!(queued(kind, Some(1_000)).is_due(1_000), "{kind:?} on time");
            assert!(queued(kind, Some(999)).is_due(1_000), "{kind:?} late");
            // No schedule at all is corrupt, and firing is the wrong guess.
            assert!(!queued(kind, None).is_due(1_000), "{kind:?} unscheduled");
        }
    }

    #[test]
    fn only_queued_triggers_are_due() {
        for state in [TriggerState::Completed, TriggerState::Cancelled] {
            let trigger = Trigger {
                state,
                kind: TriggerKind::Immediate,
                eligible_at_ms: None,
                interval_ms: None,
            };
            assert!(!trigger.is_due(1_000), "{state:?}");
        }
    }

    #[test]
    fn firing_retires_a_one_shot_trigger() {
        for kind in [
            TriggerKind::Immediate,
            TriggerKind::Auto,
            TriggerKind::At,
            TriggerKind::Overnight,
        ] {
            let mut trigger = queued(kind, Some(500));
            assert_eq!(
                step(&mut trigger, Event::Fired, 1_000),
                [Effect::Complete],
                "{kind:?}"
            );
            assert_eq!(trigger.state, TriggerState::Completed);
            assert!(!trigger.is_due(1_000));
        }
    }

    #[test]
    fn firing_rearms_a_recurring_trigger_past_now() {
        let mut trigger = Trigger {
            state: TriggerState::Queued,
            kind: TriggerKind::Every,
            eligible_at_ms: Some(1_000),
            interval_ms: Some(60_000),
        };
        assert_eq!(
            step(&mut trigger, Event::Fired, 1_000),
            [Effect::Rearm {
                eligible_at_ms: 61_000
            }]
        );
        assert_eq!(trigger.state, TriggerState::Queued);
        assert_eq!(trigger.eligible_at_ms, Some(61_000));
        // The rearm is strictly in the future, so the same tick cannot fire it
        // a second time.
        assert!(!trigger.is_due(1_000));
    }

    #[test]
    fn a_recurring_trigger_without_a_cadence_faults_instead_of_writing() {
        for interval_ms in [None, Some(0), Some(-1)] {
            let mut trigger = Trigger {
                state: TriggerState::Queued,
                kind: TriggerKind::Every,
                eligible_at_ms: Some(1_000),
                interval_ms,
            };
            assert_eq!(
                step(&mut trigger, Event::Fired, 1_000),
                [Effect::Fault(Fault::InvalidCadence)],
                "{interval_ms:?}"
            );
            assert_eq!(trigger.state, TriggerState::Queued);
            assert_eq!(trigger.eligible_at_ms, Some(1_000));
        }
    }

    #[test]
    fn completion_ignores_kind_and_terminal_states_absorb_every_event() {
        let mut recurring = Trigger {
            state: TriggerState::Queued,
            kind: TriggerKind::Every,
            eligible_at_ms: Some(1_000),
            interval_ms: Some(60_000),
        };
        assert_eq!(
            step(&mut recurring, Event::Completed, 2_000),
            [Effect::Complete]
        );
        assert_eq!(recurring.state, TriggerState::Completed);
        // Replaying the sweep writes nothing, which is what lets it run on
        // every startup for free.
        assert_eq!(step(&mut recurring, Event::Completed, 3_000), []);
        assert_eq!(step(&mut recurring, Event::Fired, 3_000), []);
    }

    #[test]
    fn requeueing_revives_a_completed_trigger_at_the_retry_instant() {
        let mut trigger = Trigger {
            state: TriggerState::Completed,
            kind: TriggerKind::Immediate,
            eligible_at_ms: None,
            interval_ms: None,
        };
        assert_eq!(
            step(
                &mut trigger,
                Event::Requeued {
                    eligible_at_ms: 5_000
                },
                2_000
            ),
            [Effect::Requeue {
                eligible_at_ms: 5_000
            }]
        );
        assert_eq!(trigger.state, TriggerState::Queued);
        // `immediate` ignores its schedule, so the revived trigger is due at
        // once; the cooldown that set `not_before_ms` is a separate gate.
        assert!(trigger.is_due(2_000));

        let mut cancelled = Trigger {
            state: TriggerState::Cancelled,
            ..trigger
        };
        assert_eq!(
            step(
                &mut cancelled,
                Event::Requeued {
                    eligible_at_ms: 5_000
                },
                2_000
            ),
            []
        );
        assert_eq!(cancelled.state, TriggerState::Cancelled);
    }

    #[test]
    fn rearm_collapses_missed_intervals_into_one_step() {
        assert_eq!(rearm_every_at(1_000, 60_000, 1_000), Some(61_000));
        assert_eq!(rearm_every_at(1_000, 60_000, 60_999), Some(61_000));
        assert_eq!(rearm_every_at(1_000, 60_000, 61_000), Some(121_000));
        // Asleep for an hour on a one-minute cadence owes one run, not sixty.
        assert_eq!(rearm_every_at(1_000, 60_000, 3_601_000), Some(3_661_000));
    }

    #[test]
    fn rearm_refuses_impossible_arithmetic() {
        // Not yet due: rearming would skip the instant it was waiting for.
        assert_eq!(rearm_every_at(2_000, 60_000, 1_000), None);
        assert_eq!(rearm_every_at(1_000, 0, 2_000), None);
        assert_eq!(rearm_every_at(1_000, -60_000, 2_000), None);
        assert_eq!(rearm_every_at(0, i64::MAX, i64::MAX), None);
    }
}
