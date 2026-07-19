//! `flow::parse` must survive any committed flow file, however damaged.

use proptest::prelude::*;
use sloop::flow::StageKind;

use crate::support::splice;

/// A valid flow file to damage. Kept deliberately simple; the point of this
/// seed is that mutations of it stay *nearly* valid.
const SEED: &str = "stages:\n  - name: build\n    kind: agent\n    verdict: reported\n  - name: test\n    kind: exec\n    cmd: [cargo, test]\n    on_fail:\n      agent: fix the tests\n      attempts: 2\n  - name: merge\n    kind: merge\n";

proptest! {
    /// Tier 1: arbitrary text, YAML-flavored soup, and recursion probes.
    #[test]
    fn arbitrary_text_never_panics(contents in prop_oneof![
        ".*",
        r#"[-a-z0-9\[\]{}:,&*#?|>'" \n]*"#,
        (1..400usize).prop_map(|depth| "[".repeat(depth)),
        (1..200usize).prop_map(|depth| "a:\n".repeat(depth)),
    ]) {
        if let Ok(flow) = sloop::flow::parse("fuzz", &contents) {
            // Whatever parses must have survived validation: stage names are
            // unique, the walk starts at the only agent stage, and a merge
            // stage can only be last.
            let names: std::collections::HashSet<_> =
                flow.stages.iter().map(|stage| &stage.name).collect();
            prop_assert_eq!(names.len(), flow.stages.len());
            prop_assert_eq!(flow.stages.first().map(|s| &s.kind), Some(&StageKind::Agent));
            for stage in &flow.stages[..flow.stages.len() - 1] {
                prop_assert!(stage.kind != StageKind::Merge, "merge stage must be last");
            }
        }
    }

    /// Tier 2: a valid file with random text spliced in at a random spot.
    #[test]
    fn damaged_valid_files_never_panic(
        position in 0..SEED.len(),
        replaced in 0..24usize,
        injected in prop_oneof![".{0,16}", r#"[-\[\]{}:,&*#?|>'" ]{0,8}"#],
    ) {
        let contents = splice(SEED, position, replaced, &injected);
        let _ = sloop::flow::parse("fuzz", &contents);
    }
}
