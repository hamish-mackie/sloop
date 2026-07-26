//! Data-driven classification of rejected agent requests. Catalogs describe
//! evidence only; outcome and cooldown policy remain in the scheduler.

use std::collections::HashSet;
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};

const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VendorErrorClass {
    AuthenticationRequired,
    InvalidConfiguration,
    RateLimited,
    UnknownRejection,
}

impl VendorErrorClass {
    pub fn requires_cooldown(self) -> bool {
        matches!(self, Self::RateLimited | Self::UnknownRejection)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VendorErrorMatch {
    pub class: VendorErrorClass,
    pub vendor: String,
    pub rule_id: String,
    pub diagnostic: String,
}

impl VendorErrorMatch {
    pub fn evidence_json(&self, cooldown_until_ms: Option<i64>) -> String {
        serde_json::json!({
            "class": self.class,
            "vendor": self.vendor,
            "rule_id": self.rule_id,
            "diagnostic": self.diagnostic,
            "cooldown_until_ms": cooldown_until_ms,
        })
        .to_string()
    }
}

#[derive(Debug, Clone)]
pub struct VendorErrorClassifier {
    rules: Vec<Rule>,
}

impl VendorErrorClassifier {
    pub fn built_in() -> Result<Self, CatalogError> {
        Self::from_yaml(&[
            ("codex", include_str!("codex/errors.yaml")),
            ("opencode", include_str!("opencode/errors.yaml")),
            ("claude", include_str!("claude/errors.yaml")),
        ])
    }

    fn from_yaml(catalogs: &[(&str, &str)]) -> Result<Self, CatalogError> {
        let mut rules = Vec::new();
        let mut ids = HashSet::new();
        for (expected_vendor, yaml) in catalogs {
            let catalog: Catalog = serde_yaml::from_str(yaml).map_err(|error| {
                CatalogError(format!("invalid {expected_vendor} error catalog: {error}"))
            })?;
            if catalog.version != SCHEMA_VERSION {
                return Err(CatalogError(format!(
                    "unsupported {} error catalog schema version {}; expected {SCHEMA_VERSION}",
                    catalog.vendor, catalog.version
                )));
            }
            if catalog.vendor != *expected_vendor {
                return Err(CatalogError(format!(
                    "error catalog vendor `{}` does not match `{expected_vendor}`",
                    catalog.vendor
                )));
            }
            for raw in catalog.rules {
                if raw.id.trim().is_empty() {
                    return Err(CatalogError(format!(
                        "{expected_vendor} error catalog contains an empty rule ID"
                    )));
                }
                if !ids.insert(raw.id.clone()) {
                    return Err(CatalogError(format!(
                        "duplicate vendor error rule ID `{}`",
                        raw.id
                    )));
                }
                if raw.diagnostic.trim().is_empty() {
                    return Err(CatalogError(format!(
                        "vendor error rule `{}` has an empty diagnostic",
                        raw.id
                    )));
                }
                if raw.conditions.message_signatures.is_empty() {
                    return Err(CatalogError(format!(
                        "vendor error rule `{}` has no message signatures",
                        raw.id
                    )));
                }
                if raw
                    .conditions
                    .message_signatures
                    .iter()
                    .any(|signature| signature.is_empty())
                {
                    return Err(CatalogError(format!(
                        "vendor error rule `{}` has an empty message signature",
                        raw.id
                    )));
                }
                if !raw.conditions.record_any_of.is_empty() && catalog.framing.is_none() {
                    return Err(CatalogError(format!(
                        "vendor error rule `{}` selects records, but the {expected_vendor} \
                         catalog declares no framing",
                        raw.id
                    )));
                }
                if raw
                    .conditions
                    .record_any_of
                    .iter()
                    .any(|predicate| predicate.is_empty())
                {
                    return Err(CatalogError(format!(
                        "vendor error rule `{}` has an empty record predicate, which would \
                         select every record",
                        raw.id
                    )));
                }
                rules.push(Rule {
                    vendor: catalog.vendor.clone(),
                    id: raw.id,
                    class: raw.class,
                    diagnostic: raw.diagnostic,
                    framing: catalog.framing,
                    conditions: raw.conditions,
                });
            }
        }
        Ok(Self { rules })
    }

    pub fn classify(
        &self,
        exit_status: Option<i32>,
        stdout: &[u8],
        stderr: &[u8],
    ) -> Option<VendorErrorMatch> {
        let mut scanner = self.scanner(exit_status);
        scanner.feed_stdout(stdout);
        scanner.feed_stderr(stderr);
        scanner.finish()
    }

    pub fn scanner(&self, exit_status: Option<i32>) -> VendorErrorScanner<'_> {
        let framed = self.rules.iter().any(|rule| rule.framing.is_some());
        VendorErrorScanner {
            classifier: self,
            exit_status,
            states: self
                .rules
                .iter()
                .map(|rule| RuleStates {
                    stdout: ConditionState::new(&rule.conditions),
                    stderr: ConditionState::new(&rule.conditions),
                })
                .collect(),
            pending: framed.then(PendingLines::default),
        }
    }
}

pub struct VendorErrorScanner<'a> {
    classifier: &'a VendorErrorClassifier,
    exit_status: Option<i32>,
    states: Vec<RuleStates>,
    /// Partial trailing line per stream, kept only when some rule is framed.
    pending: Option<PendingLines>,
}

#[derive(Default)]
struct PendingLines {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl PendingLines {
    fn get_mut(&mut self, stream: Stream) -> &mut Vec<u8> {
        match stream {
            Stream::Stdout => &mut self.stdout,
            Stream::Stderr => &mut self.stderr,
        }
    }
}

impl VendorErrorScanner<'_> {
    pub fn feed_stdout(&mut self, bytes: &[u8]) {
        self.feed(Stream::Stdout, bytes);
    }

    pub fn feed_stderr(&mut self, bytes: &[u8]) {
        self.feed(Stream::Stderr, bytes);
    }

    fn feed(&mut self, stream: Stream, bytes: &[u8]) {
        self.feed_rules(stream, bytes, |rule| rule.framing.is_none(), None);
        if self.pending.is_none() {
            return;
        }
        let mut lines = Vec::new();
        let buffer = self
            .pending
            .as_mut()
            .expect("checked above")
            .get_mut(stream);
        buffer.extend_from_slice(bytes);
        while let Some(index) = buffer.iter().position(|byte| *byte == b'\n') {
            let mut line: Vec<u8> = buffer.drain(..=index).collect();
            line.pop();
            lines.push(line);
        }
        for line in lines {
            self.feed_framed_line(stream, &line);
        }
    }

    /// Feed one framed record to the rules allowed to read it.
    fn feed_framed_line(&mut self, stream: Stream, line: &[u8]) {
        let record = serde_json::from_slice::<JsonMap<String, JsonValue>>(line).ok();
        self.feed_rules(
            stream,
            line,
            |rule| rule.framing.is_some(),
            Some(record.as_ref()),
        );
    }

    fn feed_rules(
        &mut self,
        stream: Stream,
        bytes: &[u8],
        selects: impl Fn(&Rule) -> bool,
        record: Option<Option<&JsonMap<String, JsonValue>>>,
    ) {
        for (rule, states) in self.classifier.rules.iter().zip(&mut self.states) {
            if !selects(rule) {
                continue;
            }
            if rule.conditions.stream.is_some() && rule.conditions.stream != Some(stream) {
                continue;
            }
            if let Some(record) = record
                && !rule.record_eligible(record)
            {
                continue;
            }
            states.get_mut(stream).feed(&rule.conditions, bytes, false);
        }
    }

    pub fn finish(mut self) -> Option<VendorErrorMatch> {
        if self.pending.is_some() {
            for stream in [Stream::Stdout, Stream::Stderr] {
                let rest = std::mem::take(
                    self.pending
                        .as_mut()
                        .expect("checked above")
                        .get_mut(stream),
                );
                if !rest.is_empty() {
                    self.feed_framed_line(stream, &rest);
                }
            }
        }
        for (rule, states) in self.classifier.rules.iter().zip(&mut self.states) {
            if !rule.matches_exit(self.exit_status) {
                continue;
            }
            states.stdout.feed(&rule.conditions, &[], true);
            states.stderr.feed(&rule.conditions, &[], true);
            let matched = match rule.conditions.stream {
                Some(Stream::Stdout) => states.stdout.matched(),
                Some(Stream::Stderr) => states.stderr.matched(),
                None => states.stdout.matched() || states.stderr.matched(),
            };
            if matched {
                return Some(VendorErrorMatch {
                    class: rule.class,
                    vendor: rule.vendor.clone(),
                    rule_id: rule.id.clone(),
                    diagnostic: rule.diagnostic.clone(),
                });
            }
        }
        None
    }
}

#[derive(Debug)]
pub struct CatalogError(String);

impl fmt::Display for CatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for CatalogError {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Catalog {
    version: u32,
    vendor: String,
    #[serde(default)]
    framing: Option<Framing>,
    rules: Vec<RawRule>,
}

/// How a target frames its output.
///
/// Without this the classifier sees one undifferentiated byte stream, so an
/// agent that merely *reads* a file containing a vendor's rejection wording
/// looks identical to the vendor rejecting the request. Declaring the framing
/// lets a rule say which records the vendor itself authored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Framing {
    /// One JSON object per line. Lines that do not parse are plain text and
    /// stay eligible, so buffered output classifies exactly as before.
    Ndjson,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRule {
    id: String,
    class: VendorErrorClass,
    diagnostic: String,
    #[serde(rename = "match")]
    conditions: MatchConditions,
}

#[derive(Debug, Clone)]
struct Rule {
    vendor: String,
    id: String,
    class: VendorErrorClass,
    diagnostic: String,
    framing: Option<Framing>,
    conditions: MatchConditions,
}

impl Rule {
    fn matches_exit(&self, exit_status: Option<i32>) -> bool {
        self.conditions
            .exit_status
            .is_none_or(|expected| exit_status == Some(expected))
    }

    /// Whether one framed record's bytes may be classified by this rule.
    ///
    /// `record` is `None` for a line that is not a JSON object, which is plain
    /// text rather than a stream record and stays eligible. Predicates compare
    /// against *top-level* fields only: that is what separates the vendor
    /// speaking from an agent quoting it, because a quoted rejection arrives
    /// as an escaped string inside some other record's field, never as fields
    /// of the enclosing record.
    fn record_eligible(&self, record: Option<&JsonMap<String, JsonValue>>) -> bool {
        let Some(record) = record else {
            return true;
        };
        self.conditions.record_any_of.is_empty()
            || self.conditions.record_any_of.iter().any(|predicate| {
                predicate
                    .iter()
                    .all(|(field, expected)| record.get(field) == Some(expected))
            })
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct MatchConditions {
    #[serde(default)]
    exit_status: Option<i32>,
    #[serde(default)]
    stream: Option<Stream>,
    #[serde(default)]
    status_code: Option<u16>,
    #[serde(default)]
    error_code: Option<String>,
    /// Top-level field predicates selecting the records this rule may read.
    /// Empty means every record; otherwise any one predicate matching is
    /// enough, and all of that predicate's fields must be equal.
    #[serde(default)]
    record_any_of: Vec<JsonMap<String, JsonValue>>,
    message_signatures: Vec<String>,
}

impl MatchConditions {
    fn overlap_len(&self) -> usize {
        let signatures = self
            .message_signatures
            .iter()
            .map(|signature| signature.len());
        let status = self
            .status_code
            .into_iter()
            .flat_map(status_code_patterns)
            .map(|pattern| pattern.len());
        let error = self
            .error_code
            .as_deref()
            .into_iter()
            .flat_map(error_code_patterns)
            .map(|pattern| pattern.len());
        signatures.chain(status).chain(error).max().unwrap_or(1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Stream {
    Stdout,
    Stderr,
}

struct RuleStates {
    stdout: ConditionState,
    stderr: ConditionState,
}

impl RuleStates {
    fn get_mut(&mut self, stream: Stream) -> &mut ConditionState {
        match stream {
            Stream::Stdout => &mut self.stdout,
            Stream::Stderr => &mut self.stderr,
        }
    }
}

struct ConditionState {
    status_found: bool,
    error_found: bool,
    signatures_found: Vec<bool>,
    tail: Vec<u8>,
    overlap_len: usize,
}

impl ConditionState {
    fn new(conditions: &MatchConditions) -> Self {
        Self {
            status_found: conditions.status_code.is_none(),
            error_found: conditions.error_code.is_none(),
            signatures_found: vec![false; conditions.message_signatures.len()],
            tail: Vec::new(),
            overlap_len: conditions.overlap_len(),
        }
    }

    fn feed(&mut self, conditions: &MatchConditions, bytes: &[u8], final_chunk: bool) {
        let mut window = std::mem::take(&mut self.tail);
        window.extend_from_slice(bytes);
        if !self.status_found
            && let Some(code) = conditions.status_code
        {
            self.status_found = contains_status_code(&window, code, final_chunk);
        }
        if !self.error_found
            && let Some(code) = conditions.error_code.as_deref()
        {
            self.error_found = contains_error_code(&window, code, final_chunk);
        }
        for (found, signature) in self
            .signatures_found
            .iter_mut()
            .zip(&conditions.message_signatures)
        {
            *found |= contains(&window, signature.as_bytes());
        }
        let keep = self.overlap_len.min(window.len());
        self.tail.extend_from_slice(&window[window.len() - keep..]);
    }

    fn matched(&self) -> bool {
        self.status_found && self.error_found && self.signatures_found.iter().all(|found| *found)
    }
}

fn contains(bytes: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty() && bytes.windows(needle.len()).any(|window| window == needle)
}

fn status_code_patterns(code: u16) -> Vec<String> {
    let code = code.to_string();
    vec![
        format!("status {code}"),
        format!("status: {code}"),
        format!("status={code}"),
        format!("\"status\":{code}"),
        format!("\"status\": {code}"),
        format!("\"status_code\":{code}"),
        format!("\"statuscode\":{code}"),
    ]
}

fn contains_status_code(bytes: &[u8], code: u16, allow_end: bool) -> bool {
    let text = String::from_utf8_lossy(bytes).to_ascii_lowercase();
    status_code_patterns(code).iter().any(|pattern| {
        contains_with_boundary(&text, pattern, allow_end, |byte| byte.is_ascii_digit())
    })
}

fn error_code_patterns(code: &str) -> Vec<String> {
    let code = code.to_ascii_lowercase();
    vec![
        format!("code {code}"),
        format!("code: {code}"),
        format!("code={code}"),
        format!("\"code\":\"{code}\""),
        format!("\"code\": \"{code}\""),
        format!("\"error_code\":\"{code}\""),
    ]
}

fn contains_error_code(bytes: &[u8], code: &str, allow_end: bool) -> bool {
    let text = String::from_utf8_lossy(bytes).to_ascii_lowercase();
    error_code_patterns(code).iter().any(|pattern| {
        contains_with_boundary(&text, pattern, allow_end, |byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')
        })
    })
}

fn contains_with_boundary(
    haystack: &str,
    needle: &str,
    allow_end: bool,
    continues_value: impl Fn(u8) -> bool,
) -> bool {
    haystack.match_indices(needle).any(|(start, _)| {
        haystack
            .as_bytes()
            .get(start + needle.len())
            .map_or(allow_end, |byte| !continues_value(*byte))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
version: 1
vendor: test
rules:
  - id: test.rate-limit
    class: rate_limited
    diagnostic: Request rate limited
    match:
      exit_status: 1
      stream: stderr
      status_code: 429
      error_code: rate_limit_exceeded
      message_signatures: ["try again later"]
"#;

    #[test]
    fn built_in_catalogs_validate_and_match_captured_vendor_fixtures() {
        let classifier = VendorErrorClassifier::built_in().unwrap();
        let cases = [
            (
                br#"Error: status 401 Missing bearer or basic authentication in header"#.as_slice(),
                VendorErrorClass::AuthenticationRequired,
                "codex.authentication.missing-header",
            ),
            (
                br#"request failed: status: 400 model is not supported when using Codex with a ChatGPT account"#.as_slice(),
                VendorErrorClass::InvalidConfiguration,
                "codex.configuration.unsupported-chatgpt-model",
            ),
            (
                br#"UnknownError: Unexpected server error. Check server logs for details."#.as_slice(),
                VendorErrorClass::UnknownRejection,
                "opencode.rejection.unexpected-server-error",
            ),
            (
                br#"API Error: You've hit your limit; resets 12am (UTC)"#.as_slice(),
                VendorErrorClass::RateLimited,
                "claude.rate-limit.usage-limit",
            ),
            (
                br#"API Error: 529 Overloaded. This is a server-side issue, usually temporary"#
                    .as_slice(),
                VendorErrorClass::RateLimited,
                "claude.rate-limit.api-overloaded",
            ),
        ];
        for (stderr, class, id) in cases {
            let matched = classifier.classify(Some(1), b"", stderr).unwrap();
            assert_eq!(matched.class, class);
            assert_eq!(matched.rule_id, id);
        }
    }

    /// Verbatim stdout line captured from a rejected `claude` run, whose
    /// `--output-format stream-json` command reports the vendor's refusal as a
    /// synthetic assistant message rather than as stderr text.
    const CLAUDE_SESSION_LIMIT_STDOUT: &str = r#"{"type":"assistant","message":{"id":"462199bc-1a76-4c09-aaff-df681c45b92c","model":"<synthetic>","role":"assistant","stop_reason":"stop_sequence","stop_sequence":"","type":"message","content":[{"type":"text","text":"You've hit your session limit · resets 12:50pm (Australia/Sydney)"}]},"session_id":"7f4420d2-0557-4535-849c-975a1adda3bc","error":"rate_limit","is_api_error_message":true}"#;

    /// The signature is searched in the raw captured bytes, so it has to be
    /// found inside the JSON payload without the catalog knowing the envelope.
    #[test]
    fn a_claude_session_limit_on_stdout_is_rate_limited() {
        let classifier = VendorErrorClassifier::built_in().unwrap();
        let matched = classifier
            .classify(Some(1), CLAUDE_SESSION_LIMIT_STDOUT.as_bytes(), b"")
            .expect("the captured session limit classifies");
        assert_eq!(matched.class, VendorErrorClass::RateLimited);
        assert_eq!(matched.rule_id, "claude.rate-limit.usage-limit");
        assert!(matched.class.requires_cooldown());

        let plain = b"API Error: You've hit your limit; resets 12am (UTC)".as_slice();
        assert!(classifier.classify(Some(1), plain, b"").is_some());
        assert!(classifier.classify(Some(1), b"", plain).is_some());
        assert!(
            classifier
                .classify(Some(1), b"error: could not compile `sloop`", b"")
                .is_none()
        );
    }

    /// The vendor's own verdict record, which accompanies the synthetic
    /// message above and carries the same wording as its whole result.
    const CLAUDE_LIMIT_RESULT: &str = r#"{"type":"result","subtype":"success","is_error":true,"num_turns":49,"result":"You've hit your session limit · resets 12:50pm (Australia/Sydney)"}"#;

    /// Records a *successful* run produces when its agent works on this file.
    /// Each quotes the limit wording; none of them is the vendor speaking.
    /// Shapes and nesting are taken from runs that were wrongly discarded.
    const AGENT_QUOTING_THE_LIMIT: &[&str] = &[
        r##"{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_01","content":"const CLAUDE_SESSION_LIMIT_STDOUT: &str = r#\"{\"type\":\"assistant\",\"message\":{\"content\":[{\"text\":\"You've hit your session limit\"}]},\"error\":\"rate_limit\",\"is_api_error_message\":true}\"#;"}]}}"##,
        r#"{"type":"assistant","message":{"role":"assistant","model":"claude-opus-5","content":[{"type":"tool_use","id":"toolu_02","name":"Bash","input":{"command":"rg -n \"You've hit your\" src/","description":"Find the limit signature"}}]}}"#,
        r#"{"type":"assistant","message":{"role":"assistant","model":"claude-opus-5","content":[{"type":"text","text":"The catalog matches on \"You've hit your\", which the repository also contains."}]}}"#,
        r#"{"type":"result","subtype":"success","is_error":false,"num_turns":113,"result":"Done. The rule keys on \"You've hit your\" and now also checks record structure."}"#,
    ];

    /// The bug this guards: an agent that reads a rejection is not a rejection.
    ///
    /// Both runs that provoked this classified as rate limited, were discarded
    /// despite exiting cleanly with work committed, and put the whole target
    /// into cooldown. Every match they produced was tool traffic.
    #[test]
    fn an_agent_reading_or_quoting_a_limit_is_not_a_limit() {
        let classifier = VendorErrorClassifier::built_in().unwrap();
        for record in AGENT_QUOTING_THE_LIMIT {
            let stdout = format!("{record}\n");
            assert!(
                classifier
                    .classify(Some(0), stdout.as_bytes(), b"")
                    .is_none(),
                "agent-authored record classified as a vendor rejection: {record}"
            );
        }

        let transcript = format!("{}\n", AGENT_QUOTING_THE_LIMIT.join("\n"));
        let mut scanner = classifier.scanner(Some(0));
        for chunk in transcript.as_bytes().chunks(17) {
            scanner.feed_stdout(chunk);
        }
        assert!(scanner.finish().is_none());
    }

    /// The vendor's records still classify, including when the run exits 0 and
    /// when the transcript around them is full of agent quotations.
    #[test]
    fn a_vendor_authored_limit_still_classifies_among_agent_traffic() {
        let classifier = VendorErrorClassifier::built_in().unwrap();
        for genuine in [CLAUDE_SESSION_LIMIT_STDOUT, CLAUDE_LIMIT_RESULT] {
            let matched = classifier
                .classify(Some(0), format!("{genuine}\n").as_bytes(), b"")
                .expect("a vendor-authored limit classifies");
            assert_eq!(matched.rule_id, "claude.rate-limit.usage-limit");

            let mut transcript = AGENT_QUOTING_THE_LIMIT.join("\n");
            transcript.push('\n');
            transcript.push_str(genuine);
            assert!(
                classifier
                    .classify(Some(0), transcript.as_bytes(), b"")
                    .is_some(),
                "a real limit was masked by surrounding agent traffic"
            );
        }
    }

    /// Targets that do not stream JSON emit the rejection as a bare line, and
    /// unframed catalogs keep scanning the whole stream.
    #[test]
    fn unframed_output_and_catalogs_are_unaffected_by_framing() {
        let classifier = VendorErrorClassifier::built_in().unwrap();
        let plain = b"You've hit your session limit \xc2\xb7 resets 12am (UTC)".as_slice();
        assert!(classifier.classify(Some(1), plain, b"").is_some());
        assert!(classifier.classify(Some(1), b"", plain).is_some());

        let opencode = b"UnknownError:\nUnexpected server error. Check server logs for details.";
        assert!(classifier.classify(Some(1), b"", opencode).is_some());
    }

    #[test]
    fn schema_validation_rejects_record_predicates_without_framing() {
        let selecting = VALID.replace(
            "      message_signatures:",
            "      record_any_of:\n        - is_api_error_message: true\n      message_signatures:",
        );
        let error = VendorErrorClassifier::from_yaml(&[("test", &selecting)])
            .unwrap_err()
            .to_string();
        assert!(error.contains("declares no framing"), "{error}");

        let framed = format!("framing: ndjson\n{selecting}");
        assert!(VendorErrorClassifier::from_yaml(&[("test", &framed)]).is_ok());

        let empty = framed.replace("- is_api_error_message: true", "- {}");
        let error = VendorErrorClassifier::from_yaml(&[("test", &empty)])
            .unwrap_err()
            .to_string();
        assert!(error.contains("empty record predicate"), "{error}");
    }

    #[test]
    fn schema_validation_rejects_unknown_classes_duplicate_ids_and_empty_signatures() {
        let unknown = VALID.replace("rate_limited", "retry_someday");
        assert!(VendorErrorClassifier::from_yaml(&[("test", &unknown)]).is_err());

        let duplicate = format!("{VALID}\n{}", "");
        assert!(
            VendorErrorClassifier::from_yaml(&[("test", &duplicate), ("test", VALID)]).is_err()
        );

        let empty = VALID.replace("[\"try again later\"]", "[\"\"]");
        let error = VendorErrorClassifier::from_yaml(&[("test", &empty)])
            .unwrap_err()
            .to_string();
        assert!(error.contains("empty message signature"), "{error}");
    }

    #[test]
    fn all_conditions_must_match_in_one_selected_stream() {
        let classifier = VendorErrorClassifier::from_yaml(&[("test", VALID)]).unwrap();
        let matching =
            br#"{"status":429,"code":"rate_limit_exceeded","message":"try again later"}"#;
        assert!(classifier.classify(Some(1), b"", matching).is_some());
        assert!(classifier.classify(Some(0), b"", matching).is_none());
        assert!(classifier.classify(Some(1), matching, b"").is_none());
        assert!(
            classifier
                .classify(
                    Some(1),
                    br#"{"status":429,"code":"rate_limit_exceeded"}"#,
                    b"try again later"
                )
                .is_none()
        );

        let mut scanner = classifier.scanner(Some(1));
        for chunk in [
            b"{\"sta".as_slice(),
            b"tus\":429,\"code\":\"rate_",
            b"limit_exceeded\",\"message\":\"try ag",
            b"ain later\"}",
        ] {
            scanner.feed_stderr(chunk);
        }
        assert!(scanner.finish().is_some());
    }

    #[test]
    fn duplicate_signatures_binary_bytes_and_near_matches_are_safe() {
        let classifier = VendorErrorClassifier::from_yaml(&[("test", VALID)]).unwrap();
        let mut bytes = vec![0xff, 0x00];
        bytes.extend_from_slice(
            br#"status 429 code rate_limit_exceeded try again later try again later"#,
        );
        assert!(classifier.classify(Some(1), b"", &bytes).is_some());
        assert!(
            classifier
                .classify(
                    Some(1),
                    b"",
                    b"status 429 code rate_limit_exceeded try again much later"
                )
                .is_none()
        );
        assert!(
            classifier
                .classify(Some(1), b"", b"plain number 429")
                .is_none()
        );
        assert!(
            classifier
                .classify(
                    Some(1),
                    b"",
                    b"status 4290 code rate_limit_exceeded try again later"
                )
                .is_none()
        );
    }
}
