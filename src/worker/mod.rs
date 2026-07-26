//! Everything an agent process is told, in one place.
//!
//! A worker's assignment arrives over two channels, and they answer different
//! questions. The *prompt* is **how**: the standing rules the worker operates
//! under, delivered as argv when the process is spawned. The *brief* is
//! **what**: this ticket, this branch, this stage, served over the worker
//! socket and re-readable at any time, which is what makes it the recovery
//! path for an agent that has compacted its prompt away. `prompt` holds the
//! first, `brief` the second.
//!
//! The dependency rule is one-way. This module composes text from facts handed
//! to it and from committed files in the repository; it reads no database,
//! takes no clock, and spawns no process. The daemon edge resolves the facts
//! and calls in, never the reverse.

mod brief;
mod prompt;

pub use brief::definition_of_done;
pub use prompt::{
    BACKWARD_CONTEXT_LINES, FailureContext, PANEL_REVIEWER_INSTRUCTION, REVIEW_PROMPT_PATH,
    compose_worker_prompt, panel_prompt, previous_attempt_block,
};
