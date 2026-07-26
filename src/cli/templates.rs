//! Compiled-in authoring templates for the files operators write by hand.
//!
//! Users install Sloop as a bare binary, so nothing in this repository —
//! including `docs/` — is reachable from an installed `sloop`. These
//! templates are the grammar documentation that ships with the binary:
//! `sloop template <kind>` prints one to stdout, and the intended use is
//! redirection (`sloop template ticket > .agents/sloop/tickets/mine.md`).
//! Writing into `.agents/sloop/` directly is deliberately not offered: the
//! ticket directory is a live queue, so an example file dropped there is an
//! example file at risk of being posted.
//!
//! Every template is a working example annotated with comments, and the
//! tests below round-trip each one through the same parser the daemon uses.
//! A grammar change that these templates do not follow fails the build
//! rather than shipping documentation that lies.

use clap::ValueEnum;

const TICKET: &str = include_str!("templates/ticket.md");
const FLOW: &str = include_str!("templates/flow.yaml");
const PROJECT: &str = include_str!("templates/project.md");
const CONFIG: &str = include_str!("templates/config.yaml");

/// The file kinds `sloop template` can print. clap renders the variants as
/// the accepted values, so an unknown kind fails with the valid list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TemplateKind {
    Ticket,
    Flow,
    Project,
    Config,
}

impl TemplateKind {
    /// The name the operator typed, used as the envelope's `kind`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ticket => "ticket",
            Self::Flow => "flow",
            Self::Project => "project",
            Self::Config => "config",
        }
    }

    /// The template text, verbatim and newline-terminated.
    pub fn text(self) -> &'static str {
        match self {
            Self::Ticket => TICKET,
            Self::Flow => FLOW,
            Self::Project => PROJECT,
            Self::Config => CONFIG,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{CONFIG, FLOW, PROJECT, TICKET, TemplateKind};

    #[test]
    fn every_kind_prints_a_commented_template() {
        for kind in [
            TemplateKind::Ticket,
            TemplateKind::Flow,
            TemplateKind::Project,
            TemplateKind::Config,
        ] {
            let text = kind.text();
            assert!(!text.trim().is_empty(), "{} is empty", kind.as_str());
            assert!(
                text.ends_with('\n'),
                "{} lacks a final newline",
                kind.as_str()
            );
            assert!(
                text.lines().any(|line| line.trim_start().starts_with('#')),
                "{} carries no commentary",
                kind.as_str()
            );
        }
    }

    /// The ticket template must survive the exact validation `sloop post`
    /// applies, not merely the frontmatter parser: required fields and a
    /// non-empty body are part of the grammar it documents.
    #[test]
    fn the_ticket_template_posts_cleanly() {
        let frontmatter =
            crate::post::parse_ticket_frontmatter(TICKET, "template.md").expect("ticket template");

        assert_eq!(frontmatter.name, "Add request logging");
        assert!(frontmatter.has_blocked_by());
        assert!(frontmatter.blocked_by.is_empty());
        assert_eq!(frontmatter.id, None);
        assert_eq!(frontmatter.project, None);
        assert_eq!(frontmatter.worktree, None);
        assert!(
            !crate::frontmatter::body(TICKET)
                .expect("ticket body")
                .trim()
                .is_empty()
        );
    }

    #[test]
    fn the_flow_template_parses_through_the_flow_loader() {
        let flow = crate::flow::parse("example", FLOW).expect("flow template");

        let names: Vec<&str> = flow
            .stages
            .iter()
            .map(|stage| stage.name.as_str())
            .collect();
        assert_eq!(names, ["build", "test", "lint", "review", "sync", "merge"]);

        use crate::flow::{Actor, Builtin, Check};
        assert!(flow.stages.iter().any(|stage| stage.action == Actor::Agent));
        assert!(
            flow.stages
                .iter()
                .any(|stage| stage.action == Actor::Builtin(Builtin::Merge))
        );
        assert!(
            flow.stages
                .iter()
                .any(|stage| stage.action == Actor::Builtin(Builtin::Sync))
        );
        assert!(
            flow.stages
                .iter()
                .any(|stage| matches!(stage.action, Actor::Exec { .. }))
        );
        for check in [
            Check::Actor(Actor::Builtin(Builtin::Commits)),
            Check::None,
            Check::Reported,
        ] {
            assert!(
                flow.stages.iter().any(|stage| stage.result_check == check),
                "no stage demonstrates {check:?}"
            );
        }
        assert!(
            flow.stages
                .iter()
                .any(|stage| matches!(stage.result_check, Check::Actor(Actor::Exec { .. }))),
            "no stage demonstrates an exec result check"
        );

        use crate::flow::FailAction;
        let advisory: Vec<&str> = flow
            .stages
            .iter()
            .filter(|stage| stage.fail_action == FailAction::Continue)
            .map(|stage| stage.name.as_str())
            .collect();
        assert_eq!(advisory, ["lint"]);
        let returning: Vec<&str> = flow
            .stages
            .iter()
            .filter(|stage| matches!(stage.fail_action, FailAction::ReturnTo { .. }))
            .map(|stage| stage.name.as_str())
            .collect();
        assert_eq!(returning, ["test"]);

        for stage in &flow.stages {
            assert!(
                FLOW.contains(&format!("name: {}", stage.name)),
                "stage `{}` is not named in the template text",
                stage.name
            );
        }
        assert_eq!(
            FLOW.lines()
                .filter(|line| line.trim_start().starts_with("fail_action:"))
                .count(),
            flow.stages.len(),
            "every stage must write its own `fail_action`"
        );
    }

    #[test]
    fn the_project_template_parses_through_the_frontmatter_path() {
        let frontmatter = crate::frontmatter::parse(PROJECT).expect("project template");

        assert_eq!(frontmatter.id.as_deref(), Some("web"));
        assert_eq!(frontmatter.title.as_deref(), Some("Web frontend"));
        assert!(
            !crate::frontmatter::body(PROJECT)
                .expect("project body")
                .trim()
                .is_empty()
        );
    }

    #[test]
    fn the_config_template_loads_through_the_config_loader() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(root.path().join(".agents/sloop/config.yaml"), CONFIG).unwrap();

        let repository = crate::config::Repository::discover(root.path()).unwrap();
        let config = crate::config::Config::load(&repository).unwrap();

        let agent = config.agent.expect("template configures an agent");
        assert_eq!(agent.default_target, "claude");
        assert_eq!(
            agent.targets.keys().map(String::as_str).collect::<Vec<_>>(),
            ["claude", "codex", "opencode"]
        );
        assert_eq!(config.worktree_retention_ms, Some(7 * 24 * 60 * 60 * 1000));
        assert_eq!(config.max_parallel_tasks, 1);
        assert_eq!(config.stall_report_after_ms, 10 * 60 * 1000);
        assert_eq!(config.stall_after_ms, 2 * 60 * 60 * 1000);
        assert_eq!(config.ticket_prefix, "TICK");
    }

    /// The flow and config templates are meant to be dropped into the same
    /// repository, so the stage `test` the flow declares must not collide
    /// with a `flow.test_cmd` the config template leaves enabled.
    #[test]
    fn the_flow_and_config_templates_coexist() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop/flows")).unwrap();
        fs::write(root.path().join(".agents/sloop/config.yaml"), CONFIG).unwrap();
        fs::write(root.path().join(".agents/sloop/flows/default.yaml"), FLOW).unwrap();

        let repository = crate::config::Repository::discover(root.path()).unwrap();
        let config = crate::config::Config::load(&repository).unwrap();

        assert_eq!(config.flows["default"].stages.len(), 6);
    }
}
