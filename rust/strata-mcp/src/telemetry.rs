//! A client span per MCP request and the `mcp.client.operation.duration`
//! histogram, following the OpenTelemetry MCP conventions
//! (`design/50-MCP-Client.md`, "Telemetry").

use std::sync::Once;
use std::time::Instant;

use tracing::Span;
use tracing::field::Empty;

use crate::McpError;
use crate::jsonrpc::Answer;
use crate::protocol::Wire;

/// Histogram, seconds: each MCP request, by `mcp.method.name`,
/// `gen_ai.tool.name` (calls), `mcp.protocol.version`, `network.transport`
/// and `error.type` (failures).
pub const OPERATION_DURATION: &str = "mcp.client.operation.duration";

fn describe() {
    static DESCRIBED: Once = Once::new();
    DESCRIBED.call_once(|| {
        metrics::describe_histogram!(
            OPERATION_DURATION,
            metrics::Unit::Seconds,
            "How long an MCP request took, from the client's side."
        );
    });
}

/// One request's span and clock.
pub(crate) struct RequestScope {
    pub span: Span,
    started: Instant,
    method: String,
    tool: Option<String>,
    version: String,
    wire: Wire,
}

pub(crate) struct RequestInfo<'a> {
    pub method: &'a str,
    pub tool: Option<&'a str>,
    pub version: &'a str,
    pub id: u64,
    pub wire: Wire,
    pub address: Option<&'a (String, u16)>,
    pub session: Option<String>,
}

impl RequestScope {
    /// A child of the current span: the `strata.tool` span for a call the
    /// registry dispatched.
    pub(crate) fn start(info: RequestInfo<'_>) -> Self {
        describe();
        let name = match info.tool {
            Some(tool) => format!("{} {tool}", info.method),
            None => info.method.to_string(),
        };
        let span = tracing::info_span!(
            target: "datalium_strata_mcp",
            "mcp.request",
            otel.name = %name,
            otel.kind = "client",
            mcp.method.name = %info.method,
            mcp.protocol.version = %info.version,
            jsonrpc.request.id = info.id,
            network.transport = info.wire.network_transport(),
            server.address = Empty,
            server.port = Empty,
            mcp.session.id = Empty,
            gen_ai.tool.name = Empty,
            gen_ai.operation.name = Empty,
            error.type = Empty,
            rpc.status_code = Empty,
            otel.status_code = Empty,
            otel.status_message = Empty,
        );
        if let Some((host, port)) = info.address {
            span.record("server.address", host.as_str());
            span.record("server.port", *port);
        }
        if let Some(session) = &info.session {
            span.record("mcp.session.id", session.as_str());
        }
        if let Some(tool) = info.tool {
            span.record("gen_ai.tool.name", tool);
            span.record("gen_ai.operation.name", "execute_tool");
        }
        Self {
            span,
            started: Instant::now(),
            method: info.method.to_string(),
            tool: info.tool.map(str::to_string),
            version: info.version.to_string(),
            wire: info.wire,
        }
    }

    /// Records the outcome: `error.type` for a JSON-RPC error (its code), a
    /// failure (`McpError::error_type`) or a tool's own error
    /// (`tool_error`). A cancellation isn't an error.
    pub(crate) fn finish(self, outcome: &Result<Answer, McpError>) {
        let (error_type, message): (Option<String>, Option<String>) = match outcome {
            Ok(Answer::Result(result)) => {
                let tool_error = self.method == "tools/call"
                    && result.get("isError").and_then(serde_json::Value::as_bool) == Some(true);
                (tool_error.then(|| "tool_error".to_string()), None)
            }
            Ok(Answer::Error(error)) => {
                self.span.record("rpc.status_code", error.code);
                (Some(error.code.to_string()), Some(error.message.clone()))
            }
            Err(McpError::Cancelled) => (None, None),
            Err(error) => {
                if let McpError::Protocol { code, .. } = error {
                    self.span.record("rpc.status_code", *code);
                }
                (Some(error.error_type()), Some(error.to_string()))
            }
        };
        if let Some(error_type) = &error_type {
            self.span.record("error.type", error_type.as_str());
        }
        if let Some(message) = message {
            self.span.record("otel.status_code", "ERROR");
            self.span.record("otel.status_message", message.as_str());
        }
        let mut labels = vec![
            ("mcp.method.name", self.method.clone()),
            ("mcp.protocol.version", self.version.clone()),
            (
                "network.transport",
                self.wire.network_transport().to_string(),
            ),
        ];
        if let Some(tool) = &self.tool {
            labels.push(("gen_ai.tool.name", tool.clone()));
        }
        if let Some(error_type) = error_type {
            labels.push(("error.type", error_type));
        }
        let labels: Vec<metrics::Label> = labels
            .into_iter()
            .map(|(key, value)| metrics::Label::new(key, value))
            .collect();
        metrics::histogram!(OPERATION_DURATION, labels)
            .record(self.started.elapsed().as_secs_f64());
    }
}
