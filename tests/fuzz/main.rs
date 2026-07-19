//! Fuzz-style no-panic tests for the parsing surfaces, on the stable
//! toolchain so they run in every `cargo test`.
//!
//! The contract under test is narrow and absolute: parsers given untrusted
//! bytes return `Err`, they never panic. Each module works two tiers — raw
//! garbage, and mutations of *valid* inputs, which get past serde's first
//! hurdle into the hand-written validation where bugs actually hide.
//!
//! Deeper, coverage-guided exploration of the same surfaces lives in the
//! `fuzz/` crate at the repository root (`cargo +nightly fuzz`); crashes
//! found there get minimized and promoted to regression tests here.

mod flow_parse;
mod frontmatter;
mod protocol;
mod support;
