use std::fmt;

/// The fields Sloop understands in a committed Markdown file. Unknown keys
/// are preserved on disk and simply ignored here.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Frontmatter {
    pub id: Option<String>,
    pub project: Option<String>,
    pub title: Option<String>,
    pub name: String,
    pub blocked_by: Vec<String>,
    pub worktree: Option<String>,
    pub target: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub flow: Option<String>,
    blocked_by_present: bool,
}

impl Frontmatter {
    pub fn has_blocked_by(&self) -> bool {
        self.blocked_by_present
    }

    pub(crate) fn sourced(name: String, blocked_by: Vec<String>) -> Self {
        Self {
            name,
            blocked_by,
            blocked_by_present: true,
            ..Self::default()
        }
    }
}

/// Parses the leading `---` frontmatter block. A file without a block parses
/// to an empty `Frontmatter`; a malformed block is an error so a typo never
/// silently registers a ticket under the wrong identity.
///
/// Reports only the first problem. Callers that want every problem at once
/// use [`parse_collecting`].
pub fn parse(content: &str) -> Result<Frontmatter, FrontmatterError> {
    let (frontmatter, problems) = parse_collecting(content)?;
    match problems.into_iter().next() {
        Some(problem) => Err(problem),
        None => Ok(frontmatter),
    }
}

/// Parses the block, accumulating field-level problems rather than stopping
/// at the first one.
///
/// The returned `Err` is reserved for failures that make further validation
/// meaningless — no block at all, an unterminated block, YAML that does not
/// parse, or a block that is not a mapping. Past that point every field is
/// read independently, so a bad value for one key says nothing about the
/// others and each problem is collected into the returned list. A field that
/// fails takes its default value in the returned `Frontmatter`, which callers
/// must therefore only trust when the list is empty.
pub fn parse_collecting(
    content: &str,
) -> Result<(Frontmatter, Vec<FrontmatterError>), FrontmatterError> {
    let Some(block) = split(content)? else {
        return Ok((Frontmatter::default(), Vec::new()));
    };

    let mapping: serde_yaml::Value = serde_yaml::from_str(block.yaml)
        .map_err(|error| FrontmatterError::InvalidYaml(error.to_string()))?;
    if mapping.is_null() {
        // Null covers both a genuinely empty block and explicit null content
        // (`~`, `null`). Only the former is an empty frontmatter; the latter
        // is content that is not a mapping, and accepting it would let
        // stamping append keys after a scalar and corrupt the file.
        let blank = block
            .yaml
            .lines()
            .all(|line| line.trim().is_empty() || line.trim_start().starts_with('#'));
        if !blank {
            return Err(FrontmatterError::InvalidYaml(
                "frontmatter must be a mapping".into(),
            ));
        }
        return Ok((Frontmatter::default(), Vec::new()));
    }
    let mapping = mapping
        .as_mapping()
        .ok_or_else(|| FrontmatterError::InvalidYaml("frontmatter must be a mapping".into()))?;

    let mut problems = Vec::new();
    let (blocked_by, blocked_by_present) = match string_list_field(mapping, "blocked_by") {
        Ok(value) => value,
        Err(error) => {
            problems.push(error);
            (Vec::new(), false)
        }
    };
    let mut string = |key| match string_field(mapping, key) {
        Ok(value) => value,
        Err(error) => {
            problems.push(error);
            None
        }
    };
    let frontmatter = Frontmatter {
        id: string("id"),
        project: string("project"),
        title: string("title"),
        name: string("name").unwrap_or_default(),
        blocked_by,
        worktree: string("worktree"),
        target: string("target"),
        model: string("model"),
        effort: string("effort"),
        flow: string("flow"),
        blocked_by_present,
    };
    Ok((frontmatter, problems))
}

/// Returns the Markdown body after the leading frontmatter block.
pub fn body(content: &str) -> Result<&str, FrontmatterError> {
    Ok(match split(content)? {
        Some(block) => &content[block.body_at..],
        None => content,
    })
}

/// Writes `id`, `project`, `worktree`, and `flow` into the frontmatter
/// without disturbing any other byte of the file. Returns `None` when the
/// file already carries all four values, so callers can skip the write
/// entirely.
///
/// Callers must resolve conflicts first: stamping never overwrites an
/// existing `id`, `project`, or `flow` value.
pub fn stamp(
    content: &str,
    id: &str,
    project: &str,
    worktree: &str,
    flow: &str,
) -> Result<Option<String>, FrontmatterError> {
    let current = parse(content)?;
    let mut lines = String::new();
    if current.id.is_none() {
        lines.push_str(&format!("id: {id}\n"));
    }
    if current.project.is_none() {
        lines.push_str(&format!("project: {project}\n"));
    }
    if current.worktree.is_none() {
        lines.push_str(&format!("worktree: {worktree}\n"));
    }
    if current.flow.is_none() {
        lines.push_str(&format!("flow: {flow}\n"));
    }
    insert_lines(content, lines)
}

/// Writes only `id`, for project files. Existing IDs return `None` so startup
/// can leave an already identified project byte-for-byte untouched.
pub fn stamp_id(content: &str, id: &str) -> Result<Option<String>, FrontmatterError> {
    if parse(content)?.id.is_some() {
        return Ok(None);
    }
    insert_lines(content, format!("id: {id}\n"))
}

fn insert_lines(content: &str, lines: String) -> Result<Option<String>, FrontmatterError> {
    if lines.is_empty() {
        return Ok(None);
    }
    let stamped = match split(content)? {
        Some(block) => {
            let mut stamped = String::with_capacity(content.len() + lines.len());
            stamped.push_str(&content[..block.close_at]);
            stamped.push_str(&lines);
            stamped.push_str(&content[block.close_at..]);
            stamped
        }
        None => format!("---\n{lines}---\n{content}"),
    };
    // Line insertion assumes a block-style mapping; exotic-but-parseable
    // blocks (a flow mapping like `{id: x}`) would end up invalid. Verify
    // the write before handing it back: refusing with an error is always
    // preferable to corrupting a user's committed file.
    parse(&stamped)?;
    Ok(Some(stamped))
}

struct RawBlock<'a> {
    yaml: &'a str,
    /// Byte offset of the closing `---` line, where new keys are inserted.
    close_at: usize,
    /// Byte offset immediately after the closing `---` line.
    body_at: usize,
}

fn split(content: &str) -> Result<Option<RawBlock<'_>>, FrontmatterError> {
    let Some(after_open) = content.strip_prefix("---\n") else {
        return Ok(None);
    };
    let yaml_start = "---\n".len();
    let mut offset = 0;
    for line in after_open.split_inclusive('\n') {
        if line == "---\n" || line == "---" {
            let yaml = &after_open[..offset];
            // This module reads the block as LF-separated lines, but YAML
            // also breaks lines on CR, NEL, LS, and PS. A block containing
            // one would make `parse` see keys at positions `stamp`'s byte
            // model does not, so stamping could write a duplicate key and
            // corrupt the file. Reject the ambiguity instead.
            if yaml.contains(['\r', '\u{0085}', '\u{2028}', '\u{2029}']) {
                return Err(FrontmatterError::ForeignLineBreak);
            }
            return Ok(Some(RawBlock {
                yaml,
                close_at: yaml_start + offset,
                body_at: yaml_start + offset + line.len(),
            }));
        }
        offset += line.len();
    }
    Err(FrontmatterError::Unterminated)
}

fn string_list_field(
    mapping: &serde_yaml::Mapping,
    key: &str,
) -> Result<(Vec<String>, bool), FrontmatterError> {
    let Some(value) = mapping.get(key) else {
        return Ok((Vec::new(), false));
    };
    let Some(values) = value.as_sequence() else {
        return Err(FrontmatterError::InvalidBlockedBy);
    };
    let values = values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or(FrontmatterError::InvalidBlockedBy)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((values, true))
}

fn string_field(
    mapping: &serde_yaml::Mapping,
    key: &str,
) -> Result<Option<String>, FrontmatterError> {
    match mapping.get(key) {
        None | Some(serde_yaml::Value::Null) => Ok(None),
        Some(serde_yaml::Value::String(value)) => Ok(Some(value.clone())),
        Some(serde_yaml::Value::Number(value)) => Ok(Some(value.to_string())),
        Some(_) => Err(FrontmatterError::InvalidFieldType {
            key: key.to_owned(),
        }),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrontmatterError {
    Unterminated,
    InvalidYaml(String),
    /// A known scalar field holds a sequence or mapping.
    InvalidFieldType {
        key: String,
    },
    InvalidBlockedBy,
    /// The block contains a line break other than LF (CR, NEL, LS, or PS).
    ForeignLineBreak,
}

impl fmt::Display for FrontmatterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unterminated => formatter.write_str("frontmatter block is not terminated"),
            Self::InvalidYaml(message) => write!(formatter, "invalid frontmatter: {message}"),
            Self::InvalidFieldType { key } => write!(
                formatter,
                "invalid frontmatter: frontmatter field `{key}` must be a scalar"
            ),
            Self::InvalidBlockedBy => {
                formatter.write_str("frontmatter field `blocked_by` must be a YAML list of strings")
            }
            Self::ForeignLineBreak => formatter.write_str(
                "frontmatter block contains a line break other than LF \
                 (carriage return, NEL, LS, or PS); use Unix line endings",
            ),
        }
    }
}

impl std::error::Error for FrontmatterError {}

#[cfg(test)]
mod tests {
    use super::{FrontmatterError, parse, parse_collecting, stamp, stamp_id};

    #[test]
    fn every_bad_field_is_collected_and_parse_reports_the_first() {
        let content = "---\nblocked_by: T1\nname: [a]\ntarget: claude\n---\nbody\n";
        let (frontmatter, problems) = parse_collecting(content).unwrap();
        assert_eq!(
            problems,
            [
                FrontmatterError::InvalidBlockedBy,
                FrontmatterError::InvalidFieldType { key: "name".into() },
            ]
        );
        // Failed fields fall back to defaults; readable ones still parse.
        assert_eq!(frontmatter.name, "");
        assert!(!frontmatter.has_blocked_by());
        assert_eq!(frontmatter.target.as_deref(), Some("claude"));
        assert_eq!(parse(content), Err(FrontmatterError::InvalidBlockedBy));
    }

    #[test]
    fn a_block_that_cannot_be_read_is_fatal_rather_than_collected() {
        for content in ["---\nname: [oops\n---\nbody\n", "---\nid: T1\n"] {
            assert!(parse_collecting(content).is_err(), "{content:?}");
        }
    }

    #[test]
    fn a_file_without_frontmatter_parses_to_empty_fields() {
        let frontmatter = parse("# Title\nbody\n").unwrap();
        assert_eq!(frontmatter.id, None);
        assert_eq!(frontmatter.project, None);
    }

    #[test]
    fn known_fields_are_extracted_and_unknown_fields_are_ignored() {
        let frontmatter =
            parse(
                 "---\nid: T1\nproject: default\nname: Work\nblocked_by: [T0]\nworktree: topic/t1\ntarget: claude\nmodel: sonnet\neffort: medium\nflow: release\npriority: 3\n---\n# Body\n",
            )
            .unwrap();
        assert_eq!(frontmatter.id.as_deref(), Some("T1"));
        assert_eq!(frontmatter.project.as_deref(), Some("default"));
        assert_eq!(frontmatter.name, "Work");
        assert_eq!(frontmatter.blocked_by, ["T0"]);
        assert!(frontmatter.has_blocked_by());
        assert_eq!(frontmatter.worktree.as_deref(), Some("topic/t1"));
        assert_eq!(frontmatter.target.as_deref(), Some("claude"));
        assert_eq!(frontmatter.model.as_deref(), Some("sonnet"));
        assert_eq!(frontmatter.effort.as_deref(), Some("medium"));
        assert_eq!(frontmatter.flow.as_deref(), Some("release"));
    }

    #[test]
    fn stamping_a_bare_file_prepends_a_complete_block() {
        let stamped = stamp(
            "# Persist cooldowns\n",
            "cooldown",
            "default",
            "sloop/cooldown",
            "default",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            stamped,
            "---\nid: cooldown\nproject: default\nworktree: sloop/cooldown\nflow: default\n---\n# Persist cooldowns\n"
        );
    }

    #[test]
    fn stamping_preserves_existing_keys_and_body_bytes() {
        let content = "---\ntitle: Cooldowns\nid: T9\n---\nbody stays   untouched\n";
        let stamped = stamp(content, "ignored", "default", "sloop/T9", "default")
            .unwrap()
            .unwrap();
        assert_eq!(
            stamped,
            "---\ntitle: Cooldowns\nid: T9\nproject: default\nworktree: sloop/T9\nflow: default\n---\nbody stays   untouched\n"
        );
    }

    #[test]
    fn a_fully_stamped_file_needs_no_rewrite() {
        let content = "---\nid: T1\nproject: default\nworktree: topic/t1\nflow: default\n---\n";
        assert_eq!(
            stamp(content, "T1", "default", "sloop/T1", "default").unwrap(),
            None
        );
    }

    #[test]
    fn blocked_by_list_and_empty_list_round_trip_without_rewriting() {
        for (content, expected) in [
            (
                "---\nblocked_by:\n  - T1\n  - T2\nworktree: topic/t3\nid: T3\nproject: default\nflow: default\n---\nbody\n",
                &["T1", "T2"][..],
            ),
            (
                "---\nblocked_by: []\nworktree: topic/t3\nid: T3\nproject: default\nflow: default\n---\nbody\n",
                &[][..],
            ),
        ] {
            let parsed = parse(content).unwrap();
            assert!(parsed.has_blocked_by());
            assert_eq!(parsed.blocked_by, expected);
            assert_eq!(
                stamp(content, "T3", "default", "sloop/T3", "default").unwrap(),
                None
            );
        }
    }

    #[test]
    fn scalar_blocked_by_is_rejected() {
        assert_eq!(
            parse("---\nblocked_by: T1\n---\n"),
            Err(FrontmatterError::InvalidBlockedBy)
        );
    }

    #[test]
    fn explicit_worktree_is_left_byte_for_byte_untouched() {
        let content = "---\nid: T1\nproject: default\nworktree: releases/T1\nflow: default\n---\nbody stays untouched\n";
        assert_eq!(
            stamp(content, "T1", "default", "sloop/T1", "default").unwrap(),
            None
        );
    }

    #[test]
    fn an_explicit_flow_is_left_byte_for_byte_untouched() {
        let content =
            "---\nid: T1\nproject: default\nworktree: sloop/T1\nflow: release\n---\nbody\n";
        assert_eq!(
            stamp(content, "T1", "default", "sloop/T1", "default").unwrap(),
            None
        );
    }

    #[test]
    fn a_missing_flow_is_stamped_with_the_default() {
        let content = "---\nid: T1\nproject: default\nworktree: sloop/T1\n---\nbody\n";
        let stamped = stamp(content, "T1", "default", "sloop/T1", "release")
            .unwrap()
            .unwrap();
        assert_eq!(
            stamped,
            "---\nid: T1\nproject: default\nworktree: sloop/T1\nflow: release\n---\nbody\n"
        );
    }

    #[test]
    fn project_stamping_writes_only_an_id_and_preserves_every_other_byte() {
        let content = "---\ntitle: Agent team\ncolor: blue\n---\nbody stays   untouched\n";
        let stamped = stamp_id(content, "PROJ-1").unwrap().unwrap();
        assert_eq!(
            stamped,
            "---\ntitle: Agent team\ncolor: blue\nid: PROJ-1\n---\nbody stays   untouched\n"
        );
        assert!(!stamped.contains("project:"));
    }

    #[test]
    fn a_project_with_an_id_needs_no_rewrite() {
        let content = "---\nid: explicit\ntitle: Existing\n---\n";
        assert_eq!(stamp_id(content, "PROJ-1").unwrap(), None);
    }

    #[test]
    fn an_unterminated_block_is_rejected() {
        assert_eq!(parse("---\nid: T1\n"), Err(FrontmatterError::Unterminated));
    }

    /// Found by fuzzing: YAML breaks lines on CR/NEL/LS/PS where this module
    /// only breaks on LF, so YAML saw an empty `id:` here while stamping's
    /// byte model did not — and stamping then wrote a second `id`, corrupting
    /// the file. Such blocks are rejected outright.
    #[test]
    fn non_lf_line_breaks_in_the_block_are_rejected() {
        for content in [
            "---\nid:\r¡title: Fix the flaky test\n---\nbody\n",
            "---\nid:\r\ntitle: t\n---\n",
            "---\nid:\u{0085}title: t\n---\n",
            "---\nid:\u{2028}title: t\n---\n",
            "---\nid:\u{2029}title: t\n---\n",
        ] {
            assert_eq!(parse(content), Err(FrontmatterError::ForeignLineBreak));
            assert_eq!(
                stamp(content, "id-1", "default", "sloop/t", "default"),
                Err(FrontmatterError::ForeignLineBreak)
            );
        }
        // Body bytes are user Markdown and stay unrestricted.
        assert!(parse("---\nid: T1\n---\nbody\rwith\u{0085}breaks\n").is_ok());
    }

    /// Found by fuzzing: `~` is YAML for null, which the empty-block shortcut
    /// accepted — and stamping keys after a null scalar corrupts the file.
    /// Null now only passes for genuinely blank or comment-only blocks.
    #[test]
    fn explicit_null_content_is_rejected_but_blank_blocks_are_not() {
        for content in ["---\n~\n---\n", "---\nnull\n---\n"] {
            assert!(matches!(
                parse(content),
                Err(FrontmatterError::InvalidYaml(_))
            ));
        }
        for content in ["---\n---\n", "---\n\n---\n", "---\n# note\n---\nbody\n"] {
            assert!(parse(content).is_ok(), "blank-ish block: {content:?}");
            let stamped = stamp(content, "id-1", "default", "sloop/t", "default")
                .unwrap()
                .unwrap();
            assert_eq!(parse(&stamped).unwrap().id.as_deref(), Some("id-1"));
        }
    }

    /// A flow-style mapping parses but cannot take line-inserted keys;
    /// stamping must refuse with an error rather than corrupt the file.
    #[test]
    fn unstampable_blocks_error_instead_of_corrupting() {
        let content = "---\n{title: Flow style}\n---\nbody\n";
        assert!(parse(content).is_ok());
        assert!(stamp(content, "id-1", "default", "sloop/t", "default").is_err());
    }
}
