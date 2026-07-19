//! Coverage-guided fuzzing of flow file parsing. See `envelope.rs` for the
//! run and triage workflow.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(contents) = std::str::from_utf8(data) {
        let _ = sloop::flow::parse("fuzz", contents);
    }
});
