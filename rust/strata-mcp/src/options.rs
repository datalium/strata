//! How a client behaves (`design/50-MCP-Client.md`, "The API"), what it
//! reports, and what a server says about itself.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

/// Something the client noticed that isn't an error of a call: a server's
/// stderr line, a tool left out of the list, a message it ignored.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct McpLogEntry {
    pub level: McpLogLevel,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum McpLogLevel {
    /// A server's stderr, a notification or a late answer ignored.
    Debug,
    /// A stdio server started again.
    Information,
    /// A tool left out of the list (an invalid `x-mcp-header`, a repeated name).
    Warning,
}

/// Receives the client's [`McpLogEntry`]s.
pub type McpLog = Arc<dyn Fn(&McpLogEntry) + Send + Sync>;

/// The W3C trace context to put in a request's `_meta`
/// (`design/50-MCP-Client.md`, "Telemetry").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceContext {
    pub traceparent: String,
    pub tracestate: Option<String>,
}

/// Returns the current trace context, called inside each request's span.
pub type TraceContextHook = Arc<dyn Fn() -> Option<TraceContext> + Send + Sync>;

/// How an [`McpClient`](crate::McpClient) behaves. Mirrors .NET's
/// `McpClientOptions`.
#[derive(Clone)]
pub struct McpClientOptions {
    pub(crate) client_name: String,
    pub(crate) client_version: String,
    pub(crate) request_timeout: Duration,
    pub(crate) call_timeout: Option<Duration>,
    pub(crate) probe_timeout: Duration,
    pub(crate) restart_on_exit: bool,
    pub(crate) max_message_bytes: usize,
    pub(crate) log: Option<McpLog>,
    pub(crate) trace_context: Option<TraceContextHook>,
}

impl Default for McpClientOptions {
    fn default() -> Self {
        Self {
            client_name: "datalium-strata".to_string(),
            client_version: env!("CARGO_PKG_VERSION").to_string(),
            request_timeout: Duration::from_secs(30),
            call_timeout: None,
            probe_timeout: Duration::from_secs(5),
            restart_on_exit: true,
            max_message_bytes: 16 * 1024 * 1024,
            log: None,
            trace_context: None,
        }
    }
}

impl McpClientOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// `clientInfo.name`: `datalium-strata` by default.
    pub fn with_client_name(mut self, name: impl Into<String>) -> Self {
        self.client_name = name.into();
        self
    }

    /// `clientInfo.version`: Strata's version by default.
    pub fn with_client_version(mut self, version: impl Into<String>) -> Self {
        self.client_version = version.into();
        self
    }

    /// Bounds the requests the client makes on its own: `server/discover`
    /// on HTTP, `initialize`, `tools/list`. 30 s by default.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Bounds a `tools/call`. None by default: a call is already bounded by
    /// its node's timeout and the run's deadline, through its token.
    pub fn with_call_timeout(mut self, timeout: Duration) -> Self {
        self.call_timeout = Some(timeout);
        self
    }

    /// How long the stdio era probe waits before taking the server for a
    /// legacy one. 5 s by default.
    pub fn with_probe_timeout(mut self, timeout: Duration) -> Self {
        self.probe_timeout = timeout;
        self
    }

    /// Whether a stdio server that exited is started again on the next
    /// call. On by default.
    pub fn with_restart_on_exit(mut self, restart: bool) -> Self {
        self.restart_on_exit = restart;
        self
    }

    /// The largest message the client reads: 16 MiB by default.
    pub fn with_max_message_bytes(mut self, bytes: usize) -> Self {
        self.max_message_bytes = bytes;
        self
    }

    /// Where the client's [`McpLogEntry`]s go. Nowhere by default.
    pub fn with_log(mut self, log: impl Fn(&McpLogEntry) + Send + Sync + 'static) -> Self {
        self.log = Some(Arc::new(log));
        self
    }

    /// Where the `traceparent` / `tracestate` put in each request's `_meta`
    /// come from. The Rust engine has `tracing` spans but no OpenTelemetry
    /// context, so an application that uses `tracing-opentelemetry` supplies
    /// them; without a hook nothing is sent.
    pub fn with_trace_context(
        mut self,
        hook: impl Fn() -> Option<TraceContext> + Send + Sync + 'static,
    ) -> Self {
        self.trace_context = Some(Arc::new(hook));
        self
    }
}

impl fmt::Debug for McpClientOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpClientOptions")
            .field("client_name", &self.client_name)
            .field("client_version", &self.client_version)
            .field("request_timeout", &self.request_timeout)
            .field("call_timeout", &self.call_timeout)
            .field("probe_timeout", &self.probe_timeout)
            .field("restart_on_exit", &self.restart_on_exit)
            .field("max_message_bytes", &self.max_message_bytes)
            .finish_non_exhaustive()
    }
}

/// The client's log, which may go nowhere.
#[derive(Clone, Default)]
pub(crate) struct Log(Option<McpLog>);

impl Log {
    pub(crate) fn new(log: Option<McpLog>) -> Self {
        Self(log)
    }

    pub(crate) fn debug(&self, message: impl FnOnce() -> String) {
        if let Some(log) = &self.0 {
            log(&McpLogEntry {
                level: McpLogLevel::Debug,
                message: message(),
            });
        }
    }

    pub(crate) fn information(&self, message: String) {
        if let Some(log) = &self.0 {
            log(&McpLogEntry {
                level: McpLogLevel::Information,
                message,
            });
        }
    }

    pub(crate) fn warning(&self, message: String) {
        tracing::warn!(target: "datalium_strata_mcp", "{message}");
        if let Some(log) = &self.0 {
            log(&McpLogEntry {
                level: McpLogLevel::Warning,
                message,
            });
        }
    }
}

/// A server's or a client's name and version (`serverInfo`), self-reported:
/// for display and logs only.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct McpImplementation {
    pub name: String,
    pub version: String,
    pub title: Option<String>,
}

impl McpImplementation {
    pub(crate) fn from_value(value: Option<&Value>) -> Option<Self> {
        let value = value?;
        let text = |name: &str| value.get(name).and_then(Value::as_str).map(str::to_string);
        Some(Self {
            name: text("name")?,
            version: text("version").unwrap_or_default(),
            title: text("title"),
        })
    }
}
