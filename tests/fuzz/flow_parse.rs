//! `flow::parse` must survive any committed flow file, however damaged.

use proptest::prelude::*;
use sloop::flow::{Actor, Builtin, FailAction};

use crate::support::splice;

/// A valid flow file to damage. Kept deliberately simple; the point of this
/// seed is that mutations of it stay *nearly* valid.
const SEED: &str = "stages:\n  - name: build\n    action: agent\n    result_check: reported\n  - name: test\n    action: { exec: [cargo, test] }\n    fail_action: { return_to: build, attempts: 2 }\n  - name: merge\n    action: { builtin: merge }\n";

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
            // Whatever parses must have survived validation: there is at least
            // one stage, stage names are unique, a merge stage can only be
            // last, and every backward edge names a stage before its own.
            // Nothing constrains where an agent action sits — one driver walks
            // every stage alike, so any position and any count is legal.
            prop_assert!(!flow.stages.is_empty());
            let names: Vec<_> = flow.stages.iter().map(|stage| &stage.name).collect();
            let unique: std::collections::HashSet<_> = names.iter().collect();
            prop_assert_eq!(unique.len(), flow.stages.len());
            for (index, stage) in flow.stages.iter().enumerate() {
                prop_assert!(
                    stage.action != Actor::Builtin(Builtin::Merge)
                        || index == flow.stages.len() - 1,
                    "merge stage must be last"
                );
                if let FailAction::ReturnTo { stage: target, .. } = &stage.fail_action {
                    prop_assert!(
                        names[..index].contains(&target),
                        "return_to must name an earlier stage"
                    );
                }
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
