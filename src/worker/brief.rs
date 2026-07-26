//! *What* a worker was assigned: the parts of the brief that are policy rather
//! than a lookup of the run's own facts.
//!
//! One function authors the definition of done, over the executing stage's
//! `Check` and the role of the worker reading it. That is the whole point of
//! the module: while the text lived in a socket handler it was keyed on the
//! run, so every caller was told to commit — including the reviewers Sloop's
//! own prompts tell to change nothing. A stage's obligation is a property of
//! the stage, and deriving it here is what lets a test enumerate every stage
//! shape and every role at once.

use crate::flow::{Actor, Builtin, Check};

/// Which worker is reading the brief.
///
/// The distinction is not the stage's — a panel's seats and the action they
/// judge run against the same stage, so `Check` alone cannot separate them.
/// It comes from the credential the caller presented, which is the only thing
/// that knows whether it was minted for the stage or for a seat on its panel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerRole {
    /// The stage's own worker: the process the stage's action runs.
    Stage,
    /// One seat on a panel judging the stage. It reads and reports; the work
    /// under review is not its to change.
    PanelReviewer,
}

/// What the executing stage turns on, as the brief states it.
///
/// Facts about this stage only. The mechanics of the CLI — which verb to call,
/// how many times, what silence costs — belong to the prompt, and repeating
/// them here is how the two channels came to contradict each other.
pub fn definition_of_done(role: WorkerRole, check: &Check) -> Vec<String> {
    let obligation = match (role, check) {
        // A seat judges; it never satisfies the stage's check itself, whatever
        // that check happens to be. Its report is the only thing counted from
        // it, so it is the only thing the brief asks for.
        (WorkerRole::PanelReviewer, _) => {
            "This stage passes only on your reported verdict; the work under review is not yours to change"
        }
        (WorkerRole::Stage, Check::Reported) => {
            "This stage passes only on your reported verdict; nothing else you do can pass it"
        }
        (WorkerRole::Stage, Check::Actor(Actor::Builtin(Builtin::Commits))) => {
            "Commit your work to the run branch"
        }
        (WorkerRole::Stage, Check::None) => {
            "This stage passes on your own exit status: exit 0 only if the work succeeded"
        }
        // Every other check is an independent judge that runs after this
        // worker. It is named as a judge and not by name: what it is belongs to
        // the flow, and a worker is told about its own stage.
        (WorkerRole::Stage, Check::Actor(_) | Check::Panel(_)) => {
            "An independent check judges this stage's work after you exit"
        }
    };
    vec![obligation.to_owned()]
}

/// How a stage's `result_check` is named in the brief's stage block.
///
/// The kind of judge, never its argv or its target: a worker is entitled to
/// know what its own stage turns on, and not to the rest of the flow's shape.
pub fn check_label(check: &Check) -> &'static str {
    match check {
        Check::None => "none",
        Check::Reported => "reported",
        Check::Panel(_) => "panel",
        Check::Actor(Actor::Agent) => "agent",
        Check::Actor(Actor::Exec { .. }) => "exec",
        Check::Actor(Actor::Builtin(Builtin::Commits)) => "commits",
        Check::Actor(Actor::Builtin(Builtin::Merge)) => "merge",
        Check::Actor(Actor::Builtin(Builtin::Sync)) => "sync",
    }
}

#[cfg(test)]
mod tests {
    use crate::flow::{Actor, Builtin, Check, Panel, Reviewer};

    use super::{WorkerRole, check_label, definition_of_done};

    fn panel() -> Panel {
        Panel {
            prompt: "prompts/panel.md".into(),
            reviewers: vec![Reviewer {
                target: "alpha".into(),
                model: None,
                effort: None,
            }],
            quorum: 1,
        }
    }

    /// Every check a stage can carry, so the enumeration below is exhaustive
    /// rather than a sample of the ones that were easy to think of.
    fn every_check() -> Vec<Check> {
        vec![
            Check::None,
            Check::Reported,
            Check::Actor(Actor::Agent),
            Check::Actor(Actor::Exec {
                cmd: vec!["cargo".into(), "test".into()],
            }),
            Check::Actor(Actor::Builtin(Builtin::Commits)),
            Check::Actor(Actor::Builtin(Builtin::Merge)),
            Check::Actor(Actor::Builtin(Builtin::Sync)),
            Check::Panel(panel()),
        ]
    }

    /// The rule the old code made untestable: a worker is told to commit when,
    /// and only when, a commit is what its stage's check reads. Everything else
    /// — a reported stage, an exec check, any panel seat — is told what its own
    /// stage turns on instead, and a reviewer that trusts the brief over its
    /// prompt therefore does not start writing code.
    #[test]
    fn only_a_commits_checked_stage_worker_is_told_to_commit() {
        for role in [WorkerRole::Stage, WorkerRole::PanelReviewer] {
            for check in every_check() {
                let text = definition_of_done(role, &check).join("\n").to_lowercase();
                let expected = role == WorkerRole::Stage
                    && check == Check::Actor(Actor::Builtin(Builtin::Commits));
                assert_eq!(
                    text.contains("commit"),
                    expected,
                    "role {role:?} with check {check:?} said: {text}"
                );
            }
        }
    }

    /// A definition of done that says nothing is worse than none at all: it
    /// reads as "no obligation" to an agent scanning its brief.
    #[test]
    fn every_role_and_check_states_exactly_one_obligation() {
        for role in [WorkerRole::Stage, WorkerRole::PanelReviewer] {
            for check in every_check() {
                let obligation = definition_of_done(role, &check);
                assert_eq!(obligation.len(), 1, "{role:?} / {check:?}");
                assert!(!obligation[0].trim().is_empty(), "{role:?} / {check:?}");
            }
        }
    }

    /// The shipped default flow's builder is `result_check: { builtin:
    /// commits }`, and what it is told is unchanged by this module existing.
    #[test]
    fn the_builders_definition_of_done_is_unchanged() {
        assert_eq!(
            definition_of_done(
                WorkerRole::Stage,
                &Check::Actor(Actor::Builtin(Builtin::Commits))
            ),
            vec!["Commit your work to the run branch".to_owned()]
        );
    }

    /// A reported stage's worker is told what the stage turns on and nothing
    /// about the run branch: that is the contradiction this ticket removes.
    #[test]
    fn a_reported_stage_turns_only_on_the_verdict() {
        for role in [WorkerRole::Stage, WorkerRole::PanelReviewer] {
            let text = definition_of_done(role, &Check::Reported).join("\n");
            assert!(text.contains("reported verdict"), "{role:?}: {text}");
        }
    }

    /// The label names the kind of judge, so a worker can tell a reported stage
    /// from an exec-checked one without being handed the flow's commands.
    #[test]
    fn the_check_label_names_the_kind_and_not_the_command() {
        assert_eq!(check_label(&Check::None), "none");
        assert_eq!(check_label(&Check::Reported), "reported");
        assert_eq!(check_label(&Check::Panel(panel())), "panel");
        assert_eq!(
            check_label(&Check::Actor(Actor::Builtin(Builtin::Commits))),
            "commits"
        );
        let exec = Check::Actor(Actor::Exec {
            cmd: vec!["cargo".into(), "test".into()],
        });
        assert_eq!(check_label(&exec), "exec");
    }
}
