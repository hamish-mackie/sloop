//! Coverage-guided fuzzing of frontmatter parsing and stamping. See
//! `envelope.rs` for the run and triage workflow.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(contents) = std::str::from_utf8(data) {
        let parsed = sloop::frontmatter::parse(contents);
        let _ = sloop::frontmatter::body(contents);
        if parsed.is_ok() {
            if let Ok(Some(stamped)) =
                sloop::frontmatter::stamp(contents, "id-1", "proj-1", "wt-1", "flow-1")
            {
                sloop::frontmatter::parse(&stamped)
                    .expect("stamping must never corrupt a parseable file");
            }
        }
    }
});
