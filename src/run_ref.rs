//! Run identity: minted internal ids, ticket-derived aliases, and the shapes a
//! run reference can take.
//!
//! A run has two names. The internal id is 128 random bits in lowercase hex;
//! it is what the store joins on and what names directories on disk. The alias
//! is `<ticket-id>-r<attempt>`, derived from two columns that are frozen at
//! claim time, and it is the only name humans are shown. Nothing here reads the
//! clock or the store: minting is a trait so the effect stays at the boundary,
//! and every other function is a pure string judgement.

use std::fs::File;
use std::io::Read;

/// Internal run ids are 128 bits rendered as lowercase hex.
pub const RUN_ID_HEX_LEN: usize = 32;

/// How much of an internal id is shown wherever one surfaces at all, matching
/// Git's short-object convention.
pub const SHORT_RUN_ID_LEN: usize = 8;

/// The shortest prefix accepted as a reference. Below this a prefix says so
/// little that the ambiguity error would be the only likely outcome.
pub const MIN_PREFIX_LEN: usize = 4;

/// Mints internal run ids. Implemented over the operating system CSPRNG in
/// production and over a fixed sequence in tests, so claim-time logic can be
/// exercised with predictable identities.
pub trait RunIdSource: Send + Sync {
    fn mint(&self) -> Result<String, String>;
}

/// The production source: 128 bits straight from the kernel.
#[derive(Debug, Default)]
pub struct RandomRunIds;

impl RunIdSource for RandomRunIds {
    fn mint(&self) -> Result<String, String> {
        let mut bytes = [0_u8; RUN_ID_HEX_LEN / 2];
        random_bytes(&mut bytes)?;
        Ok(hex(&bytes))
    }
}

/// A source that hands out a fixed sequence, then refuses. Exists so tests can
/// drive claim-time logic with identities they chose in advance.
#[derive(Debug)]
pub struct FixedRunIds {
    remaining: std::sync::Mutex<std::collections::VecDeque<String>>,
}

impl FixedRunIds {
    pub fn new(ids: impl IntoIterator<Item = String>) -> Self {
        Self {
            remaining: std::sync::Mutex::new(ids.into_iter().collect()),
        }
    }
}

impl RunIdSource for FixedRunIds {
    fn mint(&self) -> Result<String, String> {
        self.remaining
            .lock()
            .expect("the fixed id queue is not poisoned")
            .pop_front()
            .ok_or_else(|| "the fixed run id source is exhausted".into())
    }
}

fn random_bytes(buffer: &mut [u8]) -> Result<(), String> {
    // `getentropy` is the portable entry point to the kernel CSPRNG on both
    // Linux and macOS; `/dev/urandom` covers kernels that lack the syscall.
    if unsafe { libc::getentropy(buffer.as_mut_ptr().cast(), buffer.len()) } == 0 {
        return Ok(());
    }
    File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(buffer))
        .map_err(|source| format!("cannot read random bytes for a run id: {source}"))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The displayed form of an internal id. Legacy `R<n>` ids are shorter than the
/// window and pass through whole.
pub fn short(id: &str) -> &str {
    id.get(..SHORT_RUN_ID_LEN).unwrap_or(id)
}

/// The human-facing name of a run. Both components are frozen at claim, so the
/// alias never changes for the life of the run.
pub fn alias(ticket_id: &str, attempt: i64) -> String {
    format!("{ticket_id}-r{attempt}")
}

/// Splits an alias back into the ticket id and attempt it was built from.
/// Ticket ids may contain `-`, so the split is taken from the right.
pub fn parse_alias(reference: &str) -> Option<(&str, i64)> {
    let (ticket_id, attempt) = reference.rsplit_once("-r")?;
    if ticket_id.is_empty() || attempt.is_empty() {
        return None;
    }
    if !attempt.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    attempt
        .parse()
        .ok()
        .filter(|attempt| *attempt > 0)
        .map(|attempt| (ticket_id, attempt))
}

/// Whether a reference could name internal ids by prefix. Ticket and project
/// ids always carry a `-`, so they can never be mistaken for one.
pub fn as_id_prefix(reference: &str) -> Option<String> {
    let long_enough = (MIN_PREFIX_LEN..=RUN_ID_HEX_LEN).contains(&reference.len());
    let hexadecimal = reference.bytes().all(|byte| byte.is_ascii_hexdigit());
    (long_enough && hexadecimal).then(|| reference.to_ascii_lowercase())
}

/// The accepted forms, named in every unresolvable-reference error so a dead
/// end carries its own remedy.
pub const ACCEPTED_RUN_REFERENCES: &str = "an alias like `TICK-20-r1`, a ticket id or name for \
     its latest run, or at least 4 characters of a run id";

#[cfg(test)]
mod tests {
    use super::{
        FixedRunIds, RUN_ID_HEX_LEN, RandomRunIds, RunIdSource, alias, as_id_prefix, parse_alias,
        short,
    };
    use std::collections::HashSet;

    #[test]
    fn minted_ids_are_full_width_lowercase_hex_and_do_not_repeat() {
        let source = RandomRunIds;
        let mut seen = HashSet::new();
        for _ in 0..512 {
            let id = source.mint().expect("mint a run id");
            assert_eq!(id.len(), RUN_ID_HEX_LEN, "{id}");
            assert!(
                id.bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
                "{id}"
            );
            assert!(seen.insert(id.clone()), "minted `{id}` twice");
        }
    }

    #[test]
    fn an_injected_source_decides_the_identities_and_reports_its_own_exhaustion() {
        let source = FixedRunIds::new(["aaaa1111".into(), "bbbb2222".into()]);

        assert_eq!(source.mint().unwrap(), "aaaa1111");
        assert_eq!(source.mint().unwrap(), "bbbb2222");
        assert!(source.mint().is_err());
    }

    #[test]
    fn display_shortens_minted_ids_and_leaves_legacy_ids_whole() {
        assert_eq!(short("3f2a9c1b7d4e5061a2b3c4d5e6f70819"), "3f2a9c1b");
        assert_eq!(short("R14"), "R14");
    }

    #[test]
    fn aliases_round_trip_through_parsing() {
        let alias = alias("TICK-20", 3);
        assert_eq!(alias, "TICK-20-r3");
        assert_eq!(parse_alias(&alias), Some(("TICK-20", 3)));
    }

    #[test]
    fn alias_parsing_rejects_bare_tickets_and_malformed_attempts() {
        for reference in [
            "TICK-20",
            "TICK-20-r",
            "TICK-20-r0",
            "TICK-20-rx",
            "-r1",
            "TICK-20-r+1",
        ] {
            assert_eq!(parse_alias(reference), None, "{reference}");
        }
    }

    #[test]
    fn prefixes_are_hexadecimal_of_a_useful_length_and_never_ticket_ids() {
        assert_eq!(as_id_prefix("3F2a"), Some("3f2a".into()));
        for reference in ["3f2", "TICK-20", "TICK-20-r1", "zzzz"] {
            assert_eq!(as_id_prefix(reference), None, "{reference}");
        }
    }
}
