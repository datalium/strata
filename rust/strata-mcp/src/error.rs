//! What can go wrong talking to an MCP server, and how a tool failure reads
//! to an agent (`design/50-MCP-Client.md`, "Errors").

use std::fmt;
use std::time::Duration;

use datalium_strata::StrataError;
use datalium_strata::graph::NodeError;

/// Why connecting to an MCP server, or a request to it, failed.
///
/// Through a `ToolRegistry`, an MCP tool's failure is a
/// [`StrataError::ToolCallFailed`] whose message is this error's `Display`,
/// and a cancellation is [`StrataError::ToolCancelled`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum McpError {
    /// The connection couldn't be opened: the era probe, `initialize` or
    /// `tools/list` failed, or the server speaks no version Strata knows.
    Connect(String),
    /// The server answered with a JSON-RPC error.
    Protocol { code: i64, message: String },
    /// The link failed: the process exited, a connection broke, a message
    /// was too big or wasn't JSON.
    Transport(String),
    /// An HTTP status that isn't a JSON-RPC answer (`5xx`, a `4xx` without
    /// a JSON-RPC body).
    Http { status: u16, message: String },
    /// HTTP `401` or `403`. Strata doesn't start a sign-in.
    Unauthorized(u16),
    /// No answer within the request's timeout. The request was cancelled.
    Timeout { method: String, after: Duration },
    /// The server answered `input_required`, asking for input Strata
    /// doesn't provide (it declares no client capabilities).
    InputRequired(String),
    /// The server's answer isn't valid MCP: an unknown `resultType`, a
    /// result of the wrong shape.
    Invalid(String),
    /// The caller's token fired, or its future was dropped.
    Cancelled,
    /// The client was closed.
    Closed,
}

impl McpError {
    /// Worth retrying: the link failed, a `5xx` or `429`, or a timeout.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Transport(_) | Self::Timeout { .. } => true,
            Self::Http { status, .. } => *status >= 500 || *status == 429,
            _ => false,
        }
    }

    /// What `error.type` says on the request's span and metric.
    pub(crate) fn error_type(&self) -> String {
        match self {
            Self::Protocol { code, .. } => code.to_string(),
            Self::Http { status, .. } => status.to_string(),
            Self::Unauthorized(status) => status.to_string(),
            Self::Timeout { .. } => "timeout".to_string(),
            Self::InputRequired(_) | Self::Invalid(_) => "protocol".to_string(),
            Self::Cancelled => "cancelled".to_string(),
            Self::Connect(_) | Self::Transport(_) | Self::Closed => "transport".to_string(),
        }
    }
}

impl fmt::Display for McpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(message) => write!(f, "mcp connect failed: {message}"),
            Self::Protocol { code, message } => write!(f, "mcp error {code}: {message}"),
            Self::Transport(message) => write!(f, "{TRANSPORT}{message}"),
            Self::Http { status, message } if self.is_transient() => {
                write!(f, "{TRANSPORT}HTTP {status}: {message}")
            }
            Self::Http { status, message } => write!(f, "mcp http error {status}: {message}"),
            Self::Unauthorized(status) => write!(f, "mcp unauthorized ({status})"),
            Self::Timeout { method, after } => {
                write!(f, "mcp request {method}{TIMED_OUT}{}", seconds(*after))
            }
            Self::InputRequired(what) => write!(
                f,
                "mcp server asked for input Strata doesn't provide (input_required): {what}"
            ),
            Self::Invalid(what) => write!(f, "mcp server answered with an invalid message: {what}"),
            Self::Cancelled => write!(f, "mcp request cancelled"),
            Self::Closed => write!(f, "mcp connection closed"),
        }
    }
}

impl std::error::Error for McpError {}

const TRANSPORT: &str = "mcp transport error: ";
const TIMED_OUT: &str = " timed out after ";

fn seconds(after: Duration) -> String {
    if after.subsec_nanos() == 0 {
        format!("{}s", after.as_secs())
    } else {
        format!("{}s", after.as_secs_f64())
    }
}

/// An MCP tool's failure as an agent's fault classifier reads it
/// (`AgentOptions::with_fault_classifier` in `datalium-strata-ai`): a link
/// failure, a `5xx` / `429` or a timeout is [`NodeError::Transient`], so the
/// node's recovery policy retries it. Anything else is `None`: the agent's
/// default (fatal).
///
/// A tool's error carries no kind in the Rust engine, so this reads the
/// message `datalium-strata-mcp` wrote. .NET's default classifier already
/// makes an HTTP fault transient.
pub fn mcp_fault_classifier(error: &StrataError) -> Option<NodeError> {
    let StrataError::ToolCallFailed { tool, message } = error else {
        return None;
    };
    let transient = message.starts_with(TRANSPORT)
        || (message.starts_with("mcp request ") && message.contains(TIMED_OUT));
    transient.then(|| NodeError::Transient(format!("tool '{tool}' failed: {message}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failed(error: McpError) -> StrataError {
        StrataError::ToolCallFailed {
            tool: "t".into(),
            message: error.to_string(),
        }
    }

    #[test]
    fn the_classifier_makes_link_failures_and_timeouts_transient() {
        let transient = [
            McpError::Transport("the server exited".into()),
            McpError::Http {
                status: 503,
                message: "busy".into(),
            },
            McpError::Http {
                status: 429,
                message: "slow down".into(),
            },
            McpError::Timeout {
                method: "tools/call".into(),
                after: Duration::from_secs(30),
            },
        ];
        for error in transient {
            assert!(error.is_transient(), "{error}");
            assert!(
                matches!(mcp_fault_classifier(&failed(error.clone())), Some(NodeError::Transient(m)) if m.starts_with("tool 't' failed: mcp ")),
                "{error}"
            );
        }
        let fatal = [
            McpError::Protocol {
                code: -32602,
                message: "Unknown tool: x".into(),
            },
            McpError::Http {
                status: 400,
                message: "bad".into(),
            },
            McpError::Unauthorized(401),
            McpError::Closed,
            McpError::InputRequired("elicitation".into()),
        ];
        for error in fatal {
            assert!(!error.is_transient(), "{error}");
            assert!(
                mcp_fault_classifier(&failed(error.clone())).is_none(),
                "{error}"
            );
        }
        assert!(mcp_fault_classifier(&StrataError::ToolNotFound("x".into())).is_none());
    }

    #[test]
    fn messages_read_as_the_design_says() {
        assert_eq!(
            McpError::Protocol {
                code: -32602,
                message: "Unknown tool: x".into()
            }
            .to_string(),
            "mcp error -32602: Unknown tool: x"
        );
        assert_eq!(
            McpError::Unauthorized(401).to_string(),
            "mcp unauthorized (401)"
        );
        assert_eq!(
            McpError::Timeout {
                method: "tools/call".into(),
                after: Duration::from_millis(1500)
            }
            .to_string(),
            "mcp request tools/call timed out after 1.5s"
        );
        assert_eq!(McpError::Closed.to_string(), "mcp connection closed");
    }
}
