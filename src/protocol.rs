use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RequestId(String);

impl RequestId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestEnvelope {
    pub v: u32,
    pub id: RequestId,
    #[serde(flatten)]
    pub request: Request,
    pub token: Option<String>,
}

impl RequestEnvelope {
    pub fn new(id: RequestId, request: Request, token: Option<String>) -> Self {
        Self {
            v: PROTOCOL_VERSION,
            id,
            request,
            token,
        }
    }

    pub fn decode(line: &str) -> Result<Self, ProtocolError> {
        let value: Value = serde_json::from_str(line)
            .map_err(|error| ProtocolError::invalid_request(format!("malformed JSON: {error}")))?;
        let object = value
            .as_object()
            .ok_or_else(|| ProtocolError::invalid_request("request must be a JSON object"))?;

        let version = object.get("v").and_then(Value::as_u64).ok_or_else(|| {
            ProtocolError::invalid_request("request field `v` must be an integer")
        })?;
        if version != u64::from(PROTOCOL_VERSION) {
            return Err(ProtocolError::new(
                ErrorCode::UnsupportedVersion,
                format!("unsupported protocol version {version}"),
                json!({"supported": [PROTOCOL_VERSION], "received": version}),
            ));
        }

        let verb = object.get("verb").and_then(Value::as_str).ok_or_else(|| {
            ProtocolError::invalid_request("request field `verb` must be a string")
        })?;
        if !Request::is_known_verb(verb) {
            return Err(ProtocolError::new(
                ErrorCode::UnknownVerb,
                format!("unknown verb `{verb}`"),
                json!({"verb": verb}),
            ));
        }

        serde_json::from_value(value).map_err(|error| {
            ProtocolError::invalid_request(format!("invalid request envelope: {error}"))
        })
    }

    pub fn encode(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "verb", content = "args", rename_all = "snake_case")]
pub enum Request {
    Init(EmptyArgs),
    Daemon(EmptyArgs),
    Post(PostArgs),
    Run(RunArgs),
    Retry(TicketReferenceArgs),
    Hold(TicketReferenceArgs),
    Ready(TicketReferenceArgs),
    List(EmptyArgs),
    Status(EmptyArgs),
    Pause(EmptyArgs),
    Resume(EmptyArgs),
    Stop(StopArgs),
    Cancel(RunReferenceArgs),
    Logs(RunReferenceArgs),
    Wait(RunReferenceArgs),
    Reindex(EmptyArgs),
    Brief(EmptyArgs),
    Show(ShowArgs),
    Note(NoteArgs),
}

impl Request {
    pub fn verb(&self) -> &'static str {
        match self {
            Self::Init(_) => "init",
            Self::Daemon(_) => "daemon",
            Self::Post(_) => "post",
            Self::Run(_) => "run",
            Self::Retry(_) => "retry",
            Self::Hold(_) => "hold",
            Self::Ready(_) => "ready",
            Self::List(_) => "list",
            Self::Status(_) => "status",
            Self::Pause(_) => "pause",
            Self::Resume(_) => "resume",
            Self::Stop(_) => "stop",
            Self::Cancel(_) => "cancel",
            Self::Logs(_) => "logs",
            Self::Wait(_) => "wait",
            Self::Reindex(_) => "reindex",
            Self::Brief(_) => "brief",
            Self::Show(_) => "show",
            Self::Note(_) => "note",
        }
    }

    pub fn capability(&self) -> Capability {
        match self {
            Self::Brief(_) | Self::Note(_) => Capability::Worker,
            Self::Show(_) => Capability::Both,
            _ => Capability::Operator,
        }
    }

    fn is_known_verb(verb: &str) -> bool {
        matches!(
            verb,
            "init"
                | "daemon"
                | "post"
                | "run"
                | "retry"
                | "hold"
                | "ready"
                | "list"
                | "status"
                | "pause"
                | "resume"
                | "stop"
                | "cancel"
                | "logs"
                | "wait"
                | "reindex"
                | "brief"
                | "show"
                | "note"
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    Operator,
    Worker,
    Both,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmptyArgs {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostArgs {
    pub file: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow: Option<String>,
    pub activation: PostActivation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PostActivation {
    Auto,
    At { time: String },
    Manual,
    Hold,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunArgs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ticket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    pub activation: RunActivation,
    pub only: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunActivation {
    Now,
    At { local_time: String },
    Every { interval_ms: u64 },
    Overnight,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopArgs {
    #[serde(default)]
    pub force: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunReferenceArgs {
    pub run: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TicketReferenceArgs {
    pub ticket: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShowArgs {
    #[serde(rename = "ref")]
    pub reference: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoteArgs {
    pub text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidArguments,
    InvalidRequest,
    UnsupportedVersion,
    UnknownVerb,
    DaemonUnavailable,
    Unauthorized,
    NotFound,
    Conflict,
    CooldownActive,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub code: ErrorCode,
    pub message: String,
    pub details: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolError {
    pub body: ErrorBody,
}

impl ProtocolError {
    pub fn new(code: ErrorCode, message: impl Into<String>, details: Value) -> Self {
        Self {
            body: ErrorBody {
                code,
                message: message.into(),
                details,
            },
        }
    }

    fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidRequest, message, json!({}))
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.body.message)
    }
}

impl std::error::Error for ProtocolError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResponseEnvelope {
    pub id: Option<RequestId>,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorBody>,
}

impl ResponseEnvelope {
    pub fn success(id: Option<RequestId>, data: Value) -> Self {
        Self {
            id,
            ok: true,
            data: Some(data),
            error: None,
        }
    }

    pub fn failure(id: Option<RequestId>, error: ErrorBody) -> Self {
        Self {
            id,
            ok: false,
            data: None,
            error: Some(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{
        EmptyArgs, ErrorBody, ErrorCode, Request, RequestEnvelope, RequestId, ResponseEnvelope,
        RunActivation, RunArgs,
    };

    #[test]
    fn request_envelope_serializes_to_the_public_wire_shape() {
        let envelope = RequestEnvelope::new(
            RequestId::new("req-123"),
            Request::Run(RunArgs {
                ticket: Some("T1".into()),
                project: None,
                activation: RunActivation::Now,
                only: Vec::new(),
            }),
            None,
        );

        let value: Value = serde_json::from_str(&envelope.encode().unwrap()).unwrap();
        assert_eq!(
            value,
            json!({
                "v": 1,
                "id": "req-123",
                "verb": "run",
                "args": {
                    "ticket": "T1",
                    "activation": {"kind": "now"},
                    "only": []
                },
                "token": null
            })
        );
    }

    #[test]
    fn request_envelope_round_trips() {
        let expected = RequestEnvelope::new(
            RequestId::new("req-1"),
            Request::Brief(EmptyArgs::default()),
            Some("worker-token".into()),
        );

        let decoded = RequestEnvelope::decode(&expected.encode().unwrap()).unwrap();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn malformed_json_is_an_invalid_request() {
        let error = RequestEnvelope::decode("{").unwrap_err();
        assert_eq!(error.body.code, ErrorCode::InvalidRequest);
    }

    #[test]
    fn unsupported_versions_have_a_stable_error_code() {
        let error = RequestEnvelope::decode(
            r#"{"v":2,"id":"req-1","verb":"status","args":{},"token":null}"#,
        )
        .unwrap_err();

        assert_eq!(error.body.code, ErrorCode::UnsupportedVersion);
        assert_eq!(error.body.details["received"], 2);
    }

    #[test]
    fn unknown_verbs_have_a_stable_error_code() {
        let error = RequestEnvelope::decode(
            r#"{"v":1,"id":"req-1","verb":"merge","args":{},"token":null}"#,
        )
        .unwrap_err();

        assert_eq!(error.body.code, ErrorCode::UnknownVerb);
        assert_eq!(error.body.details["verb"], "merge");
    }

    #[test]
    fn known_verbs_reject_invalid_arguments() {
        let error = RequestEnvelope::decode(
            r#"{"v":1,"id":"req-1","verb":"show","args":{},"token":"token"}"#,
        )
        .unwrap_err();

        assert_eq!(error.body.code, ErrorCode::InvalidRequest);
    }

    #[test]
    fn response_envelopes_have_exclusive_success_and_error_payloads() {
        let success = serde_json::to_value(ResponseEnvelope::success(
            Some(RequestId::new("req-1")),
            json!({"paused": false}),
        ))
        .unwrap();
        assert_eq!(
            success,
            json!({"id": "req-1", "ok": true, "data": {"paused": false}})
        );

        let failure = serde_json::to_value(ResponseEnvelope::failure(
            Some(RequestId::new("req-2")),
            ErrorBody {
                code: ErrorCode::Conflict,
                message: "ticket is already claimed".into(),
                details: json!({"ticket": "T1"}),
            },
        ))
        .unwrap();
        assert_eq!(
            failure,
            json!({
                "id": "req-2",
                "ok": false,
                "error": {
                    "code": "conflict",
                    "message": "ticket is already claimed",
                    "details": {"ticket": "T1"}
                }
            })
        );
    }
}
