//! Frontmatter parsing must survive any committed Markdown file, and
//! stamping must never corrupt a file it accepted.

use proptest::prelude::*;
use sloop::frontmatter;

use crate::support::splice;

const SEED: &str = "---\nid: t-123\ntitle: Fix the flaky test\nblocked_by: [t-100, t-101]\ntarget: claude\n---\n\n# Fix the flaky test\n\nBody text.\n";

fn contents() -> impl Strategy<Value = String> {
    prop_oneof![
        // Raw garbage.
        ".*",
        // Frontmatter-shaped: delimiters with arbitrary filling.
        (".{0,40}", ".{0,40}").prop_map(|(yaml, body)| format!("---\n{yaml}\n---\n{body}")),
        // A valid file with random damage spliced in.
        (0..SEED.len(), 0..24usize, ".{0,16}")
            .prop_map(|(position, replaced, injected)| splice(SEED, position, replaced, &injected)),
    ]
}

proptest! {
    #[test]
    fn parse_and_body_never_panic(contents in contents()) {
        let parsed = frontmatter::parse(&contents);
        let _ = frontmatter::body(&contents);

        // Stamping is only defined on files that parse. Its contract: the
        // stamped file still parses, now carries the values it lacked, and
        // `None` means every value was already present.
        if parsed.is_ok() {
            match frontmatter::stamp(&contents, "id-1", "proj-1", "wt-1", "flow-1") {
                Ok(Some(stamped)) => {
                    let after = frontmatter::parse(&stamped)
                        .expect("stamping must never corrupt a parseable file");
                    prop_assert!(after.id.is_some());
                    prop_assert!(after.project.is_some());
                    prop_assert!(after.worktree.is_some());
                    prop_assert!(after.flow.is_some());
                }
                Ok(None) => {
                    let current = frontmatter::parse(&contents).expect("checked above");
                    prop_assert!(current.id.is_some(), "None promises nothing was missing");
                }
                Err(_) => {}
            }
        }
    }
}
