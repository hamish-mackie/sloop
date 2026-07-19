//! Property tests: mechanical exploration of state spaces and input spaces.
//!
//! `model` drives the real `Store` + `Coordination` over a tempfile SQLite
//! database with random operation sequences, checked against an in-memory
//! reference model. `flow_walk` and `roundtrip` cover the pure seams.
//!
//! Failing inputs are shrunk and persisted under
//! `tests/property/proptest-regressions/`; commit those files — each one is a
//! minimal regression test.

mod flow_gen;
mod flow_walk;
mod model;
mod races;
mod roundtrip;
