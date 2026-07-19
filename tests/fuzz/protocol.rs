//! `RequestEnvelope::decode` must survive anything a socket client sends.

use proptest::prelude::*;
use serde_json::{Map, Value, json};
use sloop::protocol::RequestEnvelope;

/// One structural mutation applied to a valid envelope object.
#[derive(Debug, Clone)]
enum Mutation {
    RemoveKey(usize),
    /// Replace a key's value with a differently-typed one.
    Retype(usize, Value),
    /// Insert or overwrite an arbitrary key.
    Inject(String, Value),
    SetVersion(u64),
    SetVerb(String),
}

fn small_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        ".{0,12}".prop_map(Value::from),
        Just(json!([])),
        Just(json!({})),
        Just(json!([1, "two", null])),
        Just(json!({"nested": {"deep": []}})),
    ]
}

fn mutation() -> impl Strategy<Value = Mutation> {
    prop_oneof![
        (0..8usize).prop_map(Mutation::RemoveKey),
        (0..8usize, small_value()).prop_map(|(key, value)| Mutation::Retype(key, value)),
        ("[a-z_]{1,10}", small_value()).prop_map(|(key, value)| Mutation::Inject(key, value)),
        any::<u64>().prop_map(Mutation::SetVersion),
        ".{0,16}".prop_map(Mutation::SetVerb),
    ]
}

/// A valid envelope as a JSON object, the mutation starting point.
fn valid_envelope() -> impl Strategy<Value = Map<String, Value>> {
    (".{0,8}", prop::option::of(".{0,8}"), "[a-z ._-]{0,12}").prop_map(|(id, token, ticket)| {
        let value = json!({
            "v": 1,
            "id": id,
            "verb": "hold",
            "args": {"ticket": ticket},
            "token": token,
        });
        value.as_object().expect("literal object").clone()
    })
}

fn apply(mutations: &[Mutation], mut object: Map<String, Value>) -> String {
    for mutation in mutations {
        match mutation {
            Mutation::RemoveKey(index) => {
                if let Some(key) = object.keys().nth(index % object.len().max(1)).cloned() {
                    object.remove(&key);
                }
            }
            Mutation::Retype(index, value) => {
                if let Some(key) = object.keys().nth(index % object.len().max(1)).cloned() {
                    object.insert(key, value.clone());
                }
            }
            Mutation::Inject(key, value) => {
                object.insert(key.clone(), value.clone());
            }
            Mutation::SetVersion(version) => {
                object.insert("v".into(), Value::from(*version));
            }
            Mutation::SetVerb(verb) => {
                object.insert("verb".into(), Value::from(verb.clone()));
            }
        }
    }
    Value::Object(object).to_string()
}

proptest! {
    /// Tier 1: arbitrary text. Any answer but a panic is acceptable, and an
    /// accepted envelope must re-encode and decode to itself.
    #[test]
    fn arbitrary_text_never_panics(line in prop_oneof![
        ".*",
        // JSON-flavored soup reaches deeper than uniform noise.
        r#"[{}\[\]":,0-9a-z \\.-]*"#,
        // Deep nesting probes recursion limits.
        (1..600usize, prop::bool::ANY).prop_map(|(depth, close)| {
            let mut s = "[".repeat(depth);
            if close { s.push_str(&"]".repeat(depth)); }
            s
        }),
    ]) {
        if let Ok(envelope) = RequestEnvelope::decode(&line) {
            let encoded = envelope.encode().expect("accepted envelopes re-encode");
            let again = RequestEnvelope::decode(&encoded).expect("re-encoded envelopes decode");
            prop_assert_eq!(again, envelope);
        }
    }

    /// Tier 2: valid envelopes with structural damage — missing fields,
    /// retyped fields, foreign keys, hostile versions and verbs.
    #[test]
    fn mutated_envelopes_never_panic(
        object in valid_envelope(),
        mutations in prop::collection::vec(mutation(), 0..4),
    ) {
        let line = apply(&mutations, object);
        if let Ok(envelope) = RequestEnvelope::decode(&line) {
            let encoded = envelope.encode().expect("accepted envelopes re-encode");
            RequestEnvelope::decode(&encoded).expect("re-encoded envelopes decode");
        }
    }
}
