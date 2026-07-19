//! Encode/decode identities on the public surfaces: flow files and the
//! NDJSON socket envelope.

use proptest::prelude::*;
use sloop::protocol::{EmptyArgs, Request, RequestEnvelope, RequestId, TicketReferenceArgs};

use crate::flow_gen::flow_file;

fn request() -> impl Strategy<Value = Request> {
    prop_oneof![
        Just(Request::Init(EmptyArgs {})),
        Just(Request::Daemon(EmptyArgs {})),
        ".*".prop_map(|ticket| Request::Hold(TicketReferenceArgs { ticket })),
        ".*".prop_map(|ticket| Request::Ready(TicketReferenceArgs { ticket })),
        ".*".prop_map(|ticket| Request::Retry(TicketReferenceArgs { ticket })),
    ]
}

proptest! {
    /// A rendered flow file parses to exactly the flow it was rendered from,
    /// including back-filled default verdicts.
    #[test]
    fn generated_flow_files_parse_to_their_flow((yaml, expected) in flow_file()) {
        let parsed = sloop::flow::parse(&expected.name, &yaml)
            .map_err(|error| TestCaseError::fail(format!("{error}\n---\n{yaml}")))?;
        prop_assert_eq!(parsed, expected);
    }

    /// Envelope encoding and decoding are inverses for arbitrary ids,
    /// tokens, and ticket references — including unicode and empty strings.
    #[test]
    fn envelopes_roundtrip(
        id in ".*",
        token in prop::option::of(".*"),
        request in request(),
    ) {
        let envelope = RequestEnvelope::new(RequestId::new(id), request, token);
        let line = envelope.encode().expect("encode");
        prop_assert!(!line.contains('\n'), "an NDJSON line must stay one line");
        let decoded = RequestEnvelope::decode(&line)
            .map_err(|error| TestCaseError::fail(format!("{error:?}\n---\n{line}")))?;
        prop_assert_eq!(decoded, envelope);
    }
}
