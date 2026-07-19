//! Coverage-guided fuzzing of the NDJSON request envelope decoder.
//!
//! Run with `cargo +nightly fuzz run envelope -- -max_total_time=600` from
//! the repository root. Crashing inputs land in `fuzz/artifacts/`; minimize
//! with `cargo +nightly fuzz tmin envelope <artifact>` and promote the result
//! to a regression test in `tests/fuzz/`.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(line) = std::str::from_utf8(data) {
        if let Ok(envelope) = sloop::protocol::RequestEnvelope::decode(line) {
            let encoded = envelope.encode().expect("accepted envelopes re-encode");
            sloop::protocol::RequestEnvelope::decode(&encoded)
                .expect("re-encoded envelopes decode");
        }
    }
});
