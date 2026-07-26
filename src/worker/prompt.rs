//! *How* a worker operates: the prompt text handed to an agent process as
//! argv when it is spawned.
//!
//! Every string here is either compiled into the binary or read from a
//! committed file the caller names. Nothing in this file learns anything about
//! the run itself — the facts a prompt is built around arrive as arguments.

use std::fs;
use std::io;
use std::path::Path;

use crate::flow::{PANEL_PROMPT_ROOT, Panel};

const WORKER_BOOTSTRAP_PROMPT: &str = include_str!("instructions.md").trim_ascii();

pub const REVIEW_PROMPT_PATH: &str = ".agents/sloop/prompts/review.md";

/// The bootstrap prepended to a panel reviewer's prompt. A reviewer is not the
/// ticket's worker: it reads, it does not write, and its one job is the report.
pub const PANEL_REVIEWER_INSTRUCTION: &str = "You are one reviewer on a panel judging the work in this git worktree. Run `sloop brief` to read the assignment under review. Read and run whatever you need, but change nothing. Report your verdict with `sloop verdict pass|fail --reason <text> [--confidence low|medium|high]` exactly once; that call, not your prose, is your report, and finishing without it counts as a fail.";

/// How many lines of a failed stage's output a re-entered stage is shown. Long
/// enough for a test failure's tail, short enough that the block cannot crowd
/// the ticket out of the prompt.
pub const BACKWARD_CONTEXT_LINES: usize = 100;

pub fn compose_worker_prompt(root: &Path) -> Result<String, String> {
    let path = root.join(".agents/sloop/instructions.md");
    match fs::read_to_string(&path) {
        Ok(instructions) => Ok(format!("{WORKER_BOOTSTRAP_PROMPT}\n\n{instructions}")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Ok(WORKER_BOOTSTRAP_PROMPT.to_owned())
        }
        Err(error) => Err(format!("cannot read {}: {error}", path.display())),
    }
}

/// The prompt every seat is handed, read from the committed file the panel
/// names. One prompt for the whole panel: reviewers who were asked
/// different questions do not produce comparable answers, so a quorum over
/// them would count nothing in particular.
pub fn panel_prompt(root: &Path, panel: &Panel) -> Result<String, String> {
    let path = root.join(PANEL_PROMPT_ROOT).join(&panel.prompt);
    let text = fs::read_to_string(&path)
        .map_err(|error| format!("cannot read panel prompt {}: {error}", path.display()))?;
    Ok(format!("{PANEL_REVIEWER_INSTRUCTION}\n\n{text}"))
}

/// The failure a backward edge jumped on, recovered from the run's persisted
/// evidence.
///
/// A re-entered agent must be told why it is running again or the loop cannot
/// converge — it would reproduce the same work and fail the same check. The
/// facts are read back from the stage log and the captured output rather than
/// carried in memory, so a daemon that resumes mid-loop composes byte-for-byte
/// the prompt the daemon that took the jump would have.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureContext {
    pub stage: String,
    pub attempt: u32,
    pub reason: String,
    /// The tail of that execution's captured output, already capped.
    pub output: String,
}

/// The "previous attempt failed" block appended to a re-entered agent's
/// prompt. Delimited on both sides because everything inside it is untrusted
/// process output that must not read as further instructions.
pub fn previous_attempt_block(context: &FailureContext) -> String {
    let mut block = format!(
        "--- previous attempt failed ---\n\
         Stage `{}` (attempt {}) failed: {}\n\
         You are running again to address that failure.",
        context.stage, context.attempt, context.reason,
    );
    if !context.output.is_empty() {
        block.push_str(&format!(
            "\n\nThe last {BACKWARD_CONTEXT_LINES} lines of that stage's output:\n{}",
            context.output,
        ));
    }
    block.push_str("\n--- end previous attempt ---");
    block
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{
        FailureContext, WORKER_BOOTSTRAP_PROMPT, compose_worker_prompt, previous_attempt_block,
    };

    #[test]
    fn worker_prompt_uses_the_builtin_when_instructions_are_absent() {
        let root = tempdir().unwrap();

        assert_eq!(
            compose_worker_prompt(root.path()).unwrap(),
            WORKER_BOOTSTRAP_PROMPT
        );
    }

    #[test]
    fn worker_prompt_appends_repository_instructions() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".agents/sloop")).unwrap();
        fs::write(
            root.path().join(".agents/sloop/instructions.md"),
            "Use repository conventions.\n",
        )
        .unwrap();

        assert_eq!(
            compose_worker_prompt(root.path()).unwrap(),
            format!("{WORKER_BOOTSTRAP_PROMPT}\n\nUse repository conventions.\n")
        );
    }

    /// The block is delimited on both sides because everything between the
    /// markers is untrusted process output.
    #[test]
    fn the_previous_attempt_block_names_the_failure_and_fences_its_output() {
        let block = previous_attempt_block(&FailureContext {
            stage: "test".into(),
            attempt: 2,
            reason: "exit 101".into(),
            output: "assertion failed\n  left: 1".into(),
        });

        assert_eq!(
            block,
            concat!(
                "--- previous attempt failed ---\n",
                "Stage `test` (attempt 2) failed: exit 101\n",
                "You are running again to address that failure.\n",
                "\n",
                "The last 100 lines of that stage's output:\n",
                "assertion failed\n",
                "  left: 1\n",
                "--- end previous attempt ---",
            )
        );
    }

    /// A stage that failed silently still explains itself; an empty output
    /// section would only be noise.
    #[test]
    fn the_previous_attempt_block_omits_an_empty_output_section() {
        let block = previous_attempt_block(&FailureContext {
            stage: "review".into(),
            attempt: 1,
            reason: "no verdict reported".into(),
            output: String::new(),
        });

        assert_eq!(
            block,
            concat!(
                "--- previous attempt failed ---\n",
                "Stage `review` (attempt 1) failed: no verdict reported\n",
                "You are running again to address that failure.\n",
                "--- end previous attempt ---",
            )
        );
    }
}
