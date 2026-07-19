use std::fmt;

pub const DEFAULT_TICKET_PREFIX: &str = "TICK";
pub const DEFAULT_PROJECT_PREFIX: &str = "PROJ";

/// Prefixes stay safe as unquoted frontmatter scalars while still allowing
/// readable multi-part names such as `MY-WORK`.
pub fn valid_prefix(prefix: &str) -> bool {
    let bytes = prefix.as_bytes();
    !bytes.is_empty()
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

/// Allocates one greater than the greatest positive numeric suffix for the
/// active prefix. IDs for other prefixes and malformed suffixes do not count.
pub fn next_id<'a>(
    prefix: &str,
    existing: impl IntoIterator<Item = &'a str>,
) -> Result<String, IdError> {
    let greatest = existing
        .into_iter()
        .filter_map(|id| numeric_suffix(prefix, id))
        .max()
        .unwrap_or(0);
    let ordinal = greatest.checked_add(1).ok_or(IdError::Exhausted)?;
    Ok(format!("{prefix}-{ordinal}"))
}

/// Worktree slugs are what a ticket file stem must look like to name a
/// branch: lowercase alphanumeric segments separated by single hyphens,
/// `abc-def`.
pub fn valid_slug(value: &str) -> bool {
    !value.is_empty()
        && value.split('-').all(|segment| {
            !segment.is_empty()
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        })
}

/// Chooses the worktree branch for a ticket whose frontmatter does not name
/// one. `stem` is the ticket file's stem; exec-sourced tickets have none and
/// always fall back to `sloop/<ticket_id>`. `Err` refuses the ticket with the
/// given reason: reindex holds it, `sloop post` rejects it.
pub fn default_worktree(stem: Option<&str>, ticket_id: &str) -> Result<String, String> {
    match stem {
        None => Ok(format!("sloop/{ticket_id}")),
        Some(stem) if valid_slug(stem) => Ok(format!("sloop/{stem}")),
        Some(stem) => Err(format!(
            "file stem `{stem}` is not a valid worktree slug; \
             rename the file to `abc-def` form or set `worktree:` explicitly"
        )),
    }
}

fn numeric_suffix(prefix: &str, id: &str) -> Option<u64> {
    let suffix = id.strip_prefix(prefix)?.strip_prefix('-')?;
    if suffix.is_empty() || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    suffix.parse::<u64>().ok().filter(|ordinal| *ordinal > 0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdError {
    Exhausted,
}

impl fmt::Display for IdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exhausted => formatter.write_str("ID counter is exhausted"),
        }
    }
}

impl std::error::Error for IdError {}

#[cfg(test)]
mod tests {
    use super::{default_worktree, next_id, valid_prefix, valid_slug};

    #[test]
    fn allocation_uses_the_greatest_matching_numeric_suffix() {
        let ids = ["TICK-2", "TICK-9", "TICK-4", "OTHER-100", "TICK-nope"];
        assert_eq!(next_id("TICK", ids).unwrap(), "TICK-10");
    }

    #[test]
    fn allocation_starts_at_one_and_accepts_a_configured_prefix() {
        assert_eq!(next_id("WORK", []).unwrap(), "WORK-1");
    }

    #[test]
    fn slug_validation_accepts_only_kebab_case() {
        for slug in ["abc", "abc-def", "a-2-c", "fix-login2"] {
            assert!(valid_slug(slug), "{slug}");
        }
        for slug in ["", "-abc", "abc-", "a--b", "Fix-Login", "fix_login", "a b"] {
            assert!(!valid_slug(slug), "{slug}");
        }
    }

    #[test]
    fn worktree_defaults_to_the_stem_and_refuses_invalid_stems() {
        assert_eq!(
            default_worktree(Some("admission-snapshots"), "TICK-19").unwrap(),
            "sloop/admission-snapshots"
        );
        assert_eq!(default_worktree(None, "TICK-19").unwrap(), "sloop/TICK-19");
        let refusal = default_worktree(Some("Fix_Login"), "TICK-19").unwrap_err();
        assert!(refusal.contains("`Fix_Login`"), "{refusal}");
        assert!(refusal.contains("abc-def"), "{refusal}");
    }

    #[test]
    fn prefix_validation_rejects_empty_or_unsafe_values() {
        for prefix in ["WORK", "my-work", "TEAM_2"] {
            assert!(valid_prefix(prefix), "{prefix}");
        }
        for prefix in ["", "-WORK", "WORK-", "two words", "work/queue"] {
            assert!(!valid_prefix(prefix), "{prefix}");
        }
    }
}
