//! JSON-RPC 2.0 messages, as MCP uses them (`design/50-MCP-Client.md`,
//! "Messages"): requests with integer ids, and what a server can send back.

use serde_json::{Map, Value, json};

/// A server's answer to one request.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Answer {
    Result(Value),
    Error(RpcError),
}

/// A JSON-RPC error object.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RpcError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

impl RpcError {
    pub(crate) fn from_value(error: &Value) -> Option<Self> {
        let code = error.get("code")?.as_i64()?;
        Some(Self {
            code,
            message: error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            data: error.get("data").cloned(),
        })
    }
}

/// One message from the server.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Incoming {
    /// The answer to the request with this id. A string id that is all
    /// digits is read as the integer Strata sent.
    Response {
        id: Option<u64>,
        answer: Answer,
    },
    /// A request from the server (legacy only), to be answered.
    Request {
        id: Value,
        method: String,
    },
    Notification {
        method: String,
    },
    /// JSON, but not a JSON-RPC message.
    Invalid,
}

impl Incoming {
    pub(crate) fn parse(message: &Value) -> Self {
        let Some(object) = message.as_object() else {
            return Self::Invalid;
        };
        if let Some(method) = object.get("method").and_then(Value::as_str) {
            return match object.get("id") {
                Some(id) if !id.is_null() => Self::Request {
                    id: id.clone(),
                    method: method.to_string(),
                },
                _ => Self::Notification {
                    method: method.to_string(),
                },
            };
        }
        let id = match object.get("id") {
            Some(Value::Number(n)) => n.as_u64(),
            Some(Value::String(s)) => s.parse().ok(),
            _ => None,
        };
        if let Some(result) = object.get("result") {
            return Self::Response {
                id,
                answer: Answer::Result(result.clone()),
            };
        }
        match object.get("error").and_then(RpcError::from_value) {
            Some(error) => Self::Response {
                id,
                answer: Answer::Error(error),
            },
            None => Self::Invalid,
        }
    }
}

pub(crate) fn request(id: u64, method: &str, params: Map<String, Value>) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })
}

pub(crate) fn notification(method: &str, params: Option<Map<String, Value>>) -> Value {
    match params {
        Some(params) => json!({ "jsonrpc": "2.0", "method": method, "params": params }),
        None => json!({ "jsonrpc": "2.0", "method": method }),
    }
}

/// The answer to a server's request: `ping` gets an empty result, anything
/// else method not found, so the server doesn't wait forever.
pub(crate) fn answer_server_request(id: &Value, method: &str) -> Value {
    if method == "ping" {
        json!({ "jsonrpc": "2.0", "id": id, "result": {} })
    } else {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "code": -32601, "message": format!("Method not found: {method}") },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_are_told_apart() {
        assert_eq!(
            Incoming::parse(&json!({"jsonrpc":"2.0","id":3,"result":{"a":1}})),
            Incoming::Response {
                id: Some(3),
                answer: Answer::Result(json!({"a":1}))
            }
        );
        assert_eq!(
            Incoming::parse(
                &json!({"jsonrpc":"2.0","id":"4","error":{"code":-32601,"message":"nope"}})
            ),
            Incoming::Response {
                id: Some(4),
                answer: Answer::Error(RpcError {
                    code: -32601,
                    message: "nope".into(),
                    data: None
                })
            }
        );
        assert_eq!(
            Incoming::parse(&json!({"jsonrpc":"2.0","id":"x","method":"ping"})),
            Incoming::Request {
                id: json!("x"),
                method: "ping".into()
            }
        );
        assert_eq!(
            Incoming::parse(&json!({"jsonrpc":"2.0","method":"notifications/progress"})),
            Incoming::Notification {
                method: "notifications/progress".into()
            }
        );
        assert_eq!(Incoming::parse(&json!([1])), Incoming::Invalid);
        assert_eq!(Incoming::parse(&json!({"id":1})), Incoming::Invalid);
    }

    #[test]
    fn server_requests_are_answered() {
        assert_eq!(
            answer_server_request(&json!(9), "ping"),
            json!({"jsonrpc":"2.0","id":9,"result":{}})
        );
        assert_eq!(
            answer_server_request(&json!(9), "sampling/createMessage")["error"]["code"],
            -32601
        );
    }
}
