//! Every flow example printed in the documentation must parse as a flow.
//!
//! Documentation drifts silently: the grammar moves, the prose is updated, and
//! a YAML block three sections away keeps a spelling nothing accepts any more.
//! This test closes that gap by reading the shipped Markdown, pulling out the
//! fenced blocks that claim to be flows, and running each one through the same
//! `flow::parse` a `sloop post` would.
//!
//! The classifier is deliberately textual rather than structural. A block is
//! treated as a flow because it *looks* like one to a reader — it opens with
//! `stages:` or lists `- name:` entries — not because it happens to be valid
//! YAML. A block mangled into something unparseable is exactly the drift worth
//! catching, so it must fail here rather than quietly fall out of the corpus.

use std::fs;
use std::path::{Path, PathBuf};

/// The documented corpus: every Markdown file under `docs/`, plus the README.
fn markdown_sources() -> Vec<PathBuf> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut sources = vec![root.join("README.md")];
    collect_markdown(&root.join("docs"), &mut sources);
    sources.sort();
    sources
}

fn collect_markdown(dir: &Path, found: &mut Vec<PathBuf>) {
    let entries =
        fs::read_dir(dir).unwrap_or_else(|error| panic!("read {}: {error}", dir.display()));
    for entry in entries {
        let path = entry.expect("directory entry").path();
        if path.is_dir() {
            collect_markdown(&path, found);
        } else if path.extension().is_some_and(|extension| extension == "md") {
            found.push(path);
        }
    }
}

/// One fenced block, carrying enough position to name the offender.
struct Block {
    file: PathBuf,
    line: usize,
    body: String,
}

impl Block {
    fn location(&self) -> String {
        let name = self
            .file
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        format!("{name}:{}", self.line)
    }
}

/// Extracts the ```yaml blocks from one Markdown file, tracking the opening
/// fence's length so a longer fence inside a block cannot end it early.
fn yaml_blocks(file: &Path) -> Vec<Block> {
    let text =
        fs::read_to_string(file).unwrap_or_else(|error| panic!("read {}: {error}", file.display()));
    let mut blocks = Vec::new();
    let mut open: Option<(usize, usize, Vec<String>)> = None;
    for (index, line) in text.lines().enumerate() {
        let trimmed = line.trim_start();
        let ticks = trimmed
            .chars()
            .take_while(|character| *character == '`')
            .count();
        match &mut open {
            // Inside a block: only a fence at least as long as the opener,
            // with nothing after it, closes it.
            Some((fence, start, body)) => {
                if ticks >= *fence && trimmed[ticks..].trim().is_empty() {
                    blocks.push(Block {
                        file: file.to_path_buf(),
                        line: *start,
                        body: body.join("\n"),
                    });
                    open = None;
                } else {
                    body.push(line.to_owned());
                }
            }
            None if ticks >= 3 && trimmed[ticks..].trim() == "yaml" => {
                open = Some((ticks, index + 1, Vec::new()));
            }
            None => {}
        }
    }
    assert!(
        open.is_none(),
        "{}: unterminated fenced block",
        file.display()
    );
    blocks
}

/// Whether a YAML block presents itself to a reader as a flow definition: a
/// `stages:` map, or the bare stage list the parser also accepts.
fn looks_like_a_flow(body: &str) -> bool {
    body.lines().any(|line| {
        let trimmed = line.trim_start();
        trimmed == "stages:" || trimmed.starts_with("- name:")
    })
}

/// The floor on how many flow examples the documentation carries. It exists so
/// that a classifier that stopped matching — or a section quietly deleted —
/// fails loudly instead of leaving this test passing over an empty corpus.
const MINIMUM_DOCUMENTED_FLOWS: usize = 7;

#[test]
fn every_documented_flow_parses() {
    let sources = markdown_sources();
    assert!(
        sources.len() >= 5,
        "expected the documentation set, found {sources:?}"
    );

    let mut parsed = 0;
    for file in &sources {
        for block in yaml_blocks(file) {
            if !looks_like_a_flow(&block.body) {
                continue;
            }
            let name = block.location();
            if let Err(error) = sloop::flow::parse(&name, &block.body) {
                panic!(
                    "{}: documented flow does not parse: {error}\n{}",
                    block.file.display(),
                    block.body
                );
            }
            parsed += 1;
        }
    }

    assert!(
        parsed >= MINIMUM_DOCUMENTED_FLOWS,
        "only {parsed} documented flows were found; expected at least \
         {MINIMUM_DOCUMENTED_FLOWS}"
    );
}
