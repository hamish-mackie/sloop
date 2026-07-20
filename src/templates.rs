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
        // Stamped fields stay commented out, so `sloop post` allocates them.
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
        assert_eq!(names, ["build", "test", "lint", "review", "merge"]);

        // One example of every stage kind and every verdict policy, which is
        // the whole point of this template.
        assert!(
            flow.stages
                .iter()
                .any(|stage| stage.kind == crate::flow::StageKind::Agent)
        );
        assert!(
            flow.stages
                .iter()
                .any(|stage| stage.kind == crate::flow::StageKind::Merge)
        );
        assert!(
            flow.stages
                .iter()
                .any(|stage| matches!(stage.kind, crate::flow::StageKind::Exec { .. }))
        );
        for policy in [
            crate::flow::VerdictPolicy::Commits,
            crate::flow::VerdictPolicy::Exit,
            crate::flow::VerdictPolicy::Reported,
        ] {
            assert!(
                flow.stages.iter().any(|stage| stage.verdict == policy),
                "no stage demonstrates {policy:?}"
            );
        }
        assert!(
            flow.stages
                .iter()
                .any(|stage| matches!(stage.verdict, crate::flow::VerdictPolicy::Check { .. })),
            "no stage demonstrates a check verdict"
        );

        // `on_fail` is shown on both stage kinds that accept it.
        let repaired: Vec<&str> = flow
            .stages
            .iter()
            .filter(|stage| stage.on_fail.is_some())
            .map(|stage| stage.name.as_str())
            .collect();
        assert_eq!(repaired, ["test", "merge"]);
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
        assert_eq!(config.ticket_prefix, "TICK");
    }

    /// The flow and config templates are meant to be dropped into the same
    /// repository, so the stage `test` the flow declares must not collide
    /// with an `aftercare.test_cmd` the config template leaves enabled.
    #[test]
    fn the_flow_and_config_templates_coexist() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop/flows")).unwrap();
        fs::write(root.path().join(".agents/sloop/config.yaml"), CONFIG).unwrap();
        fs::write(root.path().join(".agents/sloop/flows/default.yaml"), FLOW).unwrap();

        let repository = crate::config::Repository::discover(root.path()).unwrap();
        let config = crate::config::Config::load(&repository).unwrap();

        // Loading also validates every `on_fail.target` against the config's
        // agent targets, which a flow template naming a target must satisfy.
        assert_eq!(config.flows["default"].stages.len(), 5);
    }
}
