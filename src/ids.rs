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
    use super::{next_id, valid_prefix};

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
    fn prefix_validation_rejects_empty_or_unsafe_values() {
        for prefix in ["WORK", "my-work", "TEAM_2"] {
            assert!(valid_prefix(prefix), "{prefix}");
        }
        for prefix in ["", "-WORK", "WORK-", "two words", "work/queue"] {
            assert!(!valid_prefix(prefix), "{prefix}");
        }
    }
}
