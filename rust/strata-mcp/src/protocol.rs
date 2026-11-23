//! The protocol versions Strata speaks and how it tells a server's era from
//! the answer to its `server/discover` probe (`design/50-MCP-Client.md`,
//! "Opening a connection"; `schema/fixtures/mcp/era-detection.json`).

use serde_json::Value;

use crate::McpError;
use crate::jsonrpc::{Answer, RpcError};

/// The modern (stateless) version: `_meta` on every request, no handshake.
pub const MODERN_VERSION: &str = "2026-07-28";

/// The legacy versions Strata speaks with the `initialize` handshake, newest
/// first. `2024-11-05` is spoken on stdio only: its HTTP+SSE transport is
/// deprecated and not built.
pub const LEGACY_VERSIONS: [&str; 4] = ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

pub(crate) const META_PROTOCOL_VERSION: &str = "io.modelcontextprotocol/protocolVersion";
pub(crate) const META_CLIENT_CAPABILITIES: &str = "io.modelcontextprotocol/clientCapabilities";
pub(crate) const META_CLIENT_INFO: &str = "io.modelcontextprotocol/clientInfo";
pub(crate) const META_SERVER_INFO: &str = "io.modelcontextprotocol/serverInfo";

pub(crate) const HEADER_MISMATCH: i64 = -32020;
pub(crate) const MISSING_CLIENT_CAPABILITY: i64 = -32021;
pub(crate) const UNSUPPORTED_PROTOCOL_VERSION: i64 = -32022;

/// How the server is reached, as far as versions go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Wire {
    /// stdio, or any stream pair with stdio's framing.
    Pipe,
    Http,
}

impl Wire {
    pub(crate) fn legacy_versions(self) -> &'static [&'static str] {
        match self {
            Wire::Pipe => &LEGACY_VERSIONS,
            Wire::Http => &LEGACY_VERSIONS[..3],
        }
    }

    /// `network.transport`, as the OpenTelemetry conventions name it.
    pub(crate) fn network_transport(self) -> &'static str {
        match self {
            Wire::Pipe => "pipe",
            Wire::Http => "tcp",
        }
    }
}

/// What the probe found.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Probe {
    /// Modern, at this version.
    Modern(String),
    /// Legacy: `initialize` with this version.
    Legacy(String),
    Fail(McpError),
}

fn is_modern_error(code: i64) -> bool {
    matches!(
        code,
        HEADER_MISMATCH | MISSING_CLIENT_CAPABILITY | UNSUPPORTED_PROTOCOL_VERSION
    )
}

/// The newest legacy version Strata knows among `listed`.
fn legacy_among(listed: &[&str], wire: Wire) -> Option<&'static str> {
    wire.legacy_versions()
        .iter()
        .copied()
        .find(|version| listed.contains(version))
}

fn versions_of(value: Option<&Value>) -> Vec<&str> {
    value
        .and_then(Value::as_array)
        .map(|versions| versions.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default()
}

fn nothing_in_common(listed: &[&str], wire: Wire) -> McpError {
    let ours: Vec<&str> = std::iter::once(MODERN_VERSION)
        .chain(wire.legacy_versions().iter().copied())
        .collect();
    McpError::Connect(format!(
        "the server supports protocol versions [{}], and Strata supports {}",
        listed.join(", "),
        ours.join(", ")
    ))
}

/// The probe's outcome from what `server/discover` got back: an answer, or
/// the error the link or the timeout gave.
pub(crate) fn probe_outcome(answer: Result<Answer, McpError>, wire: Wire) -> Probe {
    let legacy = || Probe::Legacy(LEGACY_VERSIONS[0].to_string());
    match answer {
        Ok(Answer::Result(result)) => {
            let Some(listed) = result.get("supportedVersions").filter(|v| v.is_array()) else {
                // Whatever this is, it isn't a modern server's answer.
                return legacy();
            };
            let listed = versions_of(Some(listed));
            if listed.contains(&MODERN_VERSION) {
                Probe::Modern(MODERN_VERSION.to_string())
            } else if let Some(version) = legacy_among(&listed, wire) {
                Probe::Legacy(version.to_string())
            } else {
                Probe::Fail(nothing_in_common(&listed, wire))
            }
        }
        Ok(Answer::Error(error)) => from_error(&error, wire).unwrap_or_else(legacy),
        // No answer in time: a legacy server that ignores requests before
        // `initialize`. On HTTP the probe isn't bounded by the probe timeout.
        Err(McpError::Timeout { .. }) if wire == Wire::Pipe => legacy(),
        Err(McpError::Http { status, .. }) if (400..500).contains(&status) => legacy(),
        Err(error) => Probe::Fail(match error {
            McpError::Connect(_) => error,
            other => McpError::Connect(other.to_string()),
        }),
    }
}

/// A recognized modern error's outcome; `None` for any other error.
fn from_error(error: &RpcError, wire: Wire) -> Option<Probe> {
    if !is_modern_error(error.code) {
        return None;
    }
    if error.code != UNSUPPORTED_PROTOCOL_VERSION {
        return Some(Probe::Fail(McpError::Connect(
            McpError::Protocol {
                code: error.code,
                message: error.message.clone(),
            }
            .to_string(),
        )));
    }
    let listed = versions_of(error.data.as_ref().and_then(|data| data.get("supported")));
    Some(match legacy_among(&listed, wire) {
        Some(version) => Probe::Legacy(version.to_string()),
        None => Probe::Fail(nothing_in_common(&listed, wire)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures;

    fn stdio_answer(answer: &Value) -> Result<Answer, McpError> {
        if let Some(result) = answer.get("result") {
            Ok(Answer::Result(result.clone()))
        } else if let Some(error) = answer.get("error") {
            Ok(Answer::Error(RpcError::from_value(error).unwrap()))
        } else {
            assert_eq!(answer["silence"], true);
            Err(McpError::Timeout {
                method: "server/discover".into(),
                after: std::time::Duration::from_secs(5),
            })
        }
    }

    #[test]
    fn the_shared_era_fixture() {
        let fixture = fixtures::load("era-detection.json");
        let cases = fixture["cases"].as_array().unwrap();
        assert!(cases.len() >= 30);
        for case in cases {
            let name = case["name"].as_str().unwrap();
            let answer = &case["answer"];
            let probe = match case["transport"].as_str().unwrap() {
                "stdio" => probe_outcome(stdio_answer(answer), Wire::Pipe),
                #[cfg(feature = "http")]
                "http" => probe_outcome(
                    crate::http::reply_of(
                        answer["status"].as_u64().unwrap() as u16,
                        answer["contentType"].as_str(),
                        answer["body"].as_str().unwrap().as_bytes(),
                        1,
                    ),
                    Wire::Http,
                ),
                #[cfg(not(feature = "http"))]
                "http" => continue,
                other => panic!("{other}"),
            };
            let outcome = &case["outcome"];
            match (outcome["era"].as_str().unwrap(), &probe) {
                ("modern", Probe::Modern(version)) | ("legacy", Probe::Legacy(version)) => {
                    assert_eq!(version, outcome["version"].as_str().unwrap(), "{name}")
                }
                ("fail", Probe::Fail(error)) => {
                    let message = error.to_string();
                    for word in outcome["contains"].as_array().unwrap() {
                        assert!(
                            message.contains(word.as_str().unwrap()),
                            "{name}: {message} lacks {word}"
                        );
                    }
                }
                (era, probe) => panic!("{name}: expected {era}, got {probe:?}"),
            }
        }
    }
}
