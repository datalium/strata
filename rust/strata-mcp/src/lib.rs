//! Strata's own MCP client (`design/50-MCP-Client.md`, STR-92): an MCP
//! server's tools as Datalium Strata tools.
//!
//! Strata implements the Model Context Protocol itself, with no MCP
//! dependency: the modern stateless version (`2026-07-28`) and the legacy
//! `initialize` versions (`2025-11-25`, `2025-06-18`, `2025-03-26`, and on
//! stdio `2024-11-05`), found per connection with the spec's
//! `server/discover` probe. It is a client, for tools.
//!
//! - [`McpServer`] describes a server: [`McpServer::stdio`] (a child
//!   process), [`McpServer::http`] (Streamable HTTP, with a token hook) or
//!   [`McpServer::streams`] (any stream pair).
//! - [`McpClient::connect`] opens the connection and lists the tools;
//!   [`McpClient::tool_set`] makes Strata tools of them, which
//!   [`McpRegistryExt::register_mcp`] registers all or nothing. Subsets,
//!   middleware, the `strata.tool` span and the run events apply unchanged.
//! - A tool's own error (`isError`) reaches the model as `{"error": "…"}`;
//!   a protocol or transport failure is [`StrataError::ToolCallFailed`](datalium_strata::StrataError),
//!   and [`mcp_fault_classifier`] makes the transient ones transient for an
//!   agent.
//! - Each request gets an `mcp.request` client span (target
//!   `datalium_strata_mcp`) and the [`OPERATION_DURATION`] histogram.
//!
//! ```no_run
//! use datalium_strata::tools::ToolRegistry;
//! use datalium_strata_mcp::{McpClient, McpRegistryExt, McpServer, McpToolOptions};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let files = McpClient::connect(
//!     McpServer::stdio("npx").args(["-y", "@modelcontextprotocol/server-filesystem", "/data"]),
//! )
//! .await?;
//!
//! let mut tools = ToolRegistry::new();
//! // `StrataError` isn't a `std::error::Error`: a startup mistake, found once.
//! let read_file = files.tool_set(McpToolOptions::new().include(["read_file"])).expect("listed");
//! tools.register_mcp(read_file).expect("the names are free");
//!
//! // … at shutdown
//! files.close().await;
//! # Ok(())
//! # }
//! ```

mod client;
mod error;
mod headers;
mod jsonrpc;
mod link;
mod names;
mod options;
mod protocol;
mod result;
mod telemetry;
mod tool;

#[cfg(feature = "http")]
mod http;
#[cfg(feature = "http")]
mod sse;
#[cfg(feature = "stdio")]
mod stdio;

pub use client::{McpClient, McpServer, StreamServer};
pub use error::{McpError, mcp_fault_classifier};
#[cfg(feature = "http")]
pub use http::{AccessTokenHook, HttpServer};
pub use options::{
    McpClientOptions, McpImplementation, McpLog, McpLogEntry, McpLogLevel, TraceContext,
    TraceContextHook,
};
pub use protocol::{LEGACY_VERSIONS, MODERN_VERSION};
pub use result::McpToolResult;
#[cfg(feature = "stdio")]
pub use stdio::StdioServer;
pub use telemetry::OPERATION_DURATION;
pub use tool::{McpRegistryExt, McpTool, McpToolOptions, McpToolSet};

#[cfg(test)]
mod fixtures {
    /// A shared fixture in `schema/fixtures/mcp/`.
    pub(crate) fn load(name: &str) -> serde_json::Value {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../schema/fixtures/mcp")
            .join(name);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        serde_json::from_str(&text).unwrap()
    }
}
