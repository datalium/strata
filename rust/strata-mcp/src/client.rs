//! The client: how a server is described ([`McpServer`]), how a connection
//! is opened (the era probe, `initialize`, `tools/list`), and how each
//! request is sent, timed out, cancelled, traced and retried
//! (`design/50-MCP-Client.md`, "The protocol").

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;

use datalium_strata::CancellationToken;
use serde_json::{Map, Value, json};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::Instrument;

use crate::McpError;
use crate::headers::{self, HeaderParam};
use crate::jsonrpc::{self, Answer};
use crate::link::{Link, Outgoing, PipeLink, Reader, Writer};
use crate::options::{Log, McpClientOptions, McpImplementation};
use crate::protocol::{
    self, HEADER_MISMATCH, META_CLIENT_CAPABILITIES, META_CLIENT_INFO, META_PROTOCOL_VERSION,
    META_SERVER_INFO, MODERN_VERSION, Probe, Wire,
};
use crate::result::McpToolResult;
use crate::telemetry::{RequestInfo, RequestScope};
use crate::tool::McpTool;

#[cfg(feature = "http")]
use crate::http::{HttpLink, HttpServer, SESSION_ENDED};
#[cfg(feature = "stdio")]
use crate::stdio::{self, StdioProcess, StdioServer};

/// How to reach one MCP server. [`McpClient::connect`] connects to it.
pub struct McpServer {
    kind: ServerKind,
}

enum ServerKind {
    #[cfg(feature = "stdio")]
    Stdio(StdioServer),
    #[cfg(feature = "http")]
    Http(HttpServer),
    Streams(StreamServer),
}

impl McpServer {
    /// A server Strata starts as a child process and speaks to over its
    /// stdin and stdout.
    #[cfg(feature = "stdio")]
    pub fn stdio(command: impl Into<std::ffi::OsString>) -> StdioServer {
        StdioServer::new(command.into())
    }

    /// A server reached over Streamable HTTP at `endpoint`.
    #[cfg(feature = "http")]
    pub fn http(endpoint: impl AsRef<str>) -> Result<HttpServer, McpError> {
        HttpServer::new(endpoint.as_ref())
    }

    /// A server on the other end of a stream pair, framed as on stdio: a
    /// socket, or an in-memory pipe in tests. It can be connected once, and
    /// isn't restarted.
    pub fn streams(
        from_server: impl AsyncRead + Send + Unpin + 'static,
        to_server: impl AsyncWrite + Send + Unpin + 'static,
    ) -> StreamServer {
        StreamServer {
            pair: Mutex::new(Some((Box::new(from_server), Box::new(to_server)))),
        }
    }
}

/// A server on the other end of a stream pair. Built with
/// [`McpServer::streams`].
pub struct StreamServer {
    pair: Mutex<Option<(Reader, Writer)>>,
}

#[cfg(feature = "stdio")]
impl From<StdioServer> for McpServer {
    fn from(server: StdioServer) -> Self {
        Self {
            kind: ServerKind::Stdio(server),
        }
    }
}

#[cfg(feature = "http")]
impl From<HttpServer> for McpServer {
    fn from(server: HttpServer) -> Self {
        Self {
            kind: ServerKind::Http(server),
        }
    }
}

impl From<StreamServer> for McpServer {
    fn from(server: StreamServer) -> Self {
        Self {
            kind: ServerKind::Streams(server),
        }
    }
}

/// The era a connection speaks, found once per connection.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Era {
    Modern(String),
    Legacy(String),
}

impl Era {
    fn version(&self) -> &str {
        match self {
            Era::Modern(version) | Era::Legacy(version) => version,
        }
    }
}

/// One open connection: a link, the era it speaks, and the process behind
/// it on stdio.
struct Live {
    link: Arc<dyn Link>,
    era: Era,
    #[cfg(feature = "stdio")]
    process: Option<StdioProcess>,
}

/// What a server said about itself when the connection opened.
struct Opened {
    server_info: Option<McpImplementation>,
    instructions: Option<String>,
}

struct Connected {
    protocol_version: String,
    server_info: Option<McpImplementation>,
    instructions: Option<String>,
    tools: Vec<McpTool>,
}

pub(crate) struct Inner {
    kind: ServerKind,
    wire: Wire,
    options: McpClientOptions,
    log: Log,
    address: Option<(String, u16)>,
    next_id: AtomicU64,
    live: tokio::sync::Mutex<Option<Arc<Live>>>,
    closed: AtomicBool,
    connected: OnceLock<Connected>,
    /// The `x-mcp-header` annotations of each tool, by the server's name
    /// (modern HTTP only).
    header_params: RwLock<HashMap<String, Vec<HeaderParam>>>,
}

/// A connection to one MCP server, and its tools. Cheap to clone: clones
/// share the connection, and so do the tools made from it
/// ([`Self::tool_set`]), which keep it open. Mirrors .NET's `McpClient`.
#[derive(Clone)]
pub struct McpClient {
    inner: Arc<Inner>,
}

/// Options for one request.
struct Request<'a> {
    method: &'a str,
    params: Map<String, Value>,
    timeout: Option<Duration>,
    token: Option<&'a CancellationToken>,
    tool: Option<&'a str>,
    /// Abandoning it sends `notifications/cancelled` (where the transport
    /// needs one). Not for the probe.
    cancellable: bool,
}

impl McpClient {
    /// Connects with the default options. See [`Self::connect_with`].
    pub async fn connect(server: impl Into<McpServer>) -> Result<McpClient, McpError> {
        Self::connect_with(server, McpClientOptions::default()).await
    }

    /// Starts or reaches the server, finds its era (the `server/discover`
    /// probe, then `initialize` for a legacy server) and lists its tools.
    /// A server that is down, or speaks no version Strata knows, fails here.
    pub async fn connect_with(
        server: impl Into<McpServer>,
        options: McpClientOptions,
    ) -> Result<McpClient, McpError> {
        let server = server.into();
        let (wire, address) = match &server.kind {
            #[cfg(feature = "stdio")]
            ServerKind::Stdio(_) => (Wire::Pipe, None),
            #[cfg(feature = "http")]
            ServerKind::Http(http) => (Wire::Http, http.address()),
            ServerKind::Streams(_) => (Wire::Pipe, None),
        };
        let inner = Arc::new(Inner {
            kind: server.kind,
            wire,
            log: Log::new(options.log.clone()),
            options,
            address,
            next_id: AtomicU64::new(1),
            live: tokio::sync::Mutex::new(None),
            closed: AtomicBool::new(false),
            connected: OnceLock::new(),
            header_params: RwLock::new(HashMap::new()),
        });
        let (live, opened) = inner.open().await?;
        let protocol_version = live.era.version().to_string();
        let modern_http = wire == Wire::Http && matches!(live.era, Era::Modern(_));
        *inner.live.lock().await = Some(Arc::new(live));
        let tools = match inner.list_tools(modern_http).await {
            Ok(tools) => tools,
            Err(error) => {
                inner.shut_down().await;
                return Err(match error {
                    McpError::Connect(_) => error,
                    other => McpError::Connect(format!("tools/list failed: {other}")),
                });
            }
        };
        let _ = inner.connected.set(Connected {
            protocol_version,
            server_info: opened.server_info,
            instructions: opened.instructions,
            tools,
        });
        Ok(McpClient { inner })
    }

    fn connected(&self) -> &Connected {
        self.inner.connected.get().expect("set when connecting")
    }

    /// The version the connection opened with: `2026-07-28`, `2025-11-25`, ….
    pub fn protocol_version(&self) -> &str {
        &self.connected().protocol_version
    }

    /// What the server said it is: self-reported, for display only.
    pub fn server_info(&self) -> Option<&McpImplementation> {
        self.connected().server_info.as_ref()
    }

    /// The server's guidance for models, if it gave any.
    pub fn instructions(&self) -> Option<&str> {
        self.connected().instructions.as_deref()
    }

    /// The server's tools as listed at connect, in its order (a tool with an
    /// invalid `x-mcp-header`, or a repeated name, left out).
    pub fn tools(&self) -> &[McpTool] {
        &self.connected().tools
    }

    pub(crate) fn inner(&self) -> &Arc<Inner> {
        &self.inner
    }

    /// Calls the tool `name` (the server's name) outside any registry, and
    /// returns its whole result.
    pub async fn call_tool(&self, name: &str, arguments: Value) -> Result<McpToolResult, McpError> {
        self.inner.call_tool(name, arguments, None).await
    }

    /// Closes the connection: stdio closes the server's stdin, waits for it
    /// to exit (`shutdown_timeout`), then stops its process tree; legacy
    /// HTTP deletes its session. Later calls fail with
    /// [`McpError::Closed`]. Dropping the last handle without closing stops
    /// a stdio server's process tree at once.
    pub async fn close(&self) {
        self.inner.close().await;
    }
}

impl Inner {
    /// Opens a link and finds its era.
    async fn open(&self) -> Result<(Live, Opened), McpError> {
        #[cfg(feature = "stdio")]
        let mut process = None;
        let link: Arc<dyn Link> = match &self.kind {
            #[cfg(feature = "stdio")]
            ServerKind::Stdio(server) => {
                let (link, started) = stdio::spawn(server, &self.options, &self.log)?;
                process = Some(started);
                Arc::new(link)
            }
            #[cfg(feature = "http")]
            ServerKind::Http(server) => Arc::new(HttpLink::new(server, &self.options, &self.log)?),
            ServerKind::Streams(server) => {
                let (from_server, to_server) =
                    server.pair.lock().unwrap().take().ok_or_else(|| {
                        McpError::Connect("a streams server can be connected once".to_string())
                    })?;
                Arc::new(PipeLink::start(
                    from_server,
                    to_server,
                    self.options.max_message_bytes,
                    self.log.clone(),
                    "the server closed its stream",
                ))
            }
        };
        match self.handshake(&link).await {
            Ok((era, opened)) => Ok((
                Live {
                    link,
                    era,
                    #[cfg(feature = "stdio")]
                    process,
                },
                opened,
            )),
            Err(error) => {
                link.close().await;
                #[cfg(feature = "stdio")]
                if let Some(process) = process {
                    process.shutdown().await;
                }
                Err(error)
            }
        }
    }

    /// The probe, then `initialize` when it says legacy.
    async fn handshake(&self, link: &Arc<dyn Link>) -> Result<(Era, Opened), McpError> {
        let modern = Era::Modern(MODERN_VERSION.to_string());
        let timeout = match self.wire {
            Wire::Pipe => self.options.probe_timeout,
            Wire::Http => self.options.request_timeout,
        };
        let answer = self
            .send(
                link,
                &modern,
                Request {
                    method: "server/discover",
                    params: Map::new(),
                    timeout: Some(timeout),
                    token: None,
                    tool: None,
                    cancellable: false,
                },
            )
            .await;
        match protocol::probe_outcome(answer.clone(), self.wire) {
            Probe::Modern(version) => {
                let Ok(Answer::Result(result)) = answer else {
                    unreachable!("a modern probe has a result")
                };
                Ok((
                    Era::Modern(version),
                    Opened {
                        server_info: McpImplementation::from_value(
                            result
                                .get("_meta")
                                .and_then(|meta| meta.get(META_SERVER_INFO)),
                        ),
                        instructions: text(&result, "instructions"),
                    },
                ))
            }
            Probe::Legacy(version) => self.initialize(link, version).await,
            Probe::Fail(error) => Err(error),
        }
    }

    /// The legacy handshake: `initialize`, then `notifications/initialized`.
    async fn initialize(
        &self,
        link: &Arc<dyn Link>,
        version: String,
    ) -> Result<(Era, Opened), McpError> {
        let mut params = Map::new();
        params.insert("protocolVersion".into(), json!(version));
        params.insert("capabilities".into(), json!({}));
        params.insert("clientInfo".into(), self.client_info());
        let answer = self
            .send(
                link,
                &Era::Legacy(version),
                Request {
                    method: "initialize",
                    params,
                    timeout: Some(self.options.request_timeout),
                    token: None,
                    tool: None,
                    cancellable: true,
                },
            )
            .await;
        let result = match answer {
            Ok(Answer::Result(result)) => result,
            Ok(Answer::Error(error)) => {
                return Err(McpError::Connect(format!(
                    "initialize failed: {}",
                    McpError::Protocol {
                        code: error.code,
                        message: error.message
                    }
                )));
            }
            Err(McpError::Connect(message)) => return Err(McpError::Connect(message)),
            Err(error) => return Err(McpError::Connect(format!("initialize failed: {error}"))),
        };
        let answered = text(&result, "protocolVersion").unwrap_or_default();
        if !self.wire.legacy_versions().contains(&answered.as_str()) {
            return Err(McpError::Connect(format!(
                "the server answered initialize with protocol version '{answered}', which Strata doesn't speak here (it speaks {})",
                self.wire.legacy_versions().join(", ")
            )));
        }
        let era = Era::Legacy(answered);
        let initialized = jsonrpc::notification("notifications/initialized", None);
        link.notify(
            initialized,
            self.base_headers(&era, "notifications/initialized"),
        )
        .await
        .map_err(|error| McpError::Connect(format!("notifications/initialized failed: {error}")))?;
        Ok((
            era,
            Opened {
                server_info: McpImplementation::from_value(result.get("serverInfo")),
                instructions: text(&result, "instructions"),
            },
        ))
    }

    fn client_info(&self) -> Value {
        json!({ "name": self.options.client_name, "version": self.options.client_version })
    }

    /// A request's `_meta`: the modern keys, and the trace context. Called
    /// inside the request's span.
    fn meta(&self, era: &Era) -> Map<String, Value> {
        let mut meta = Map::new();
        if let Era::Modern(version) = era {
            meta.insert(META_PROTOCOL_VERSION.into(), json!(version));
            meta.insert(META_CLIENT_CAPABILITIES.into(), json!({}));
            meta.insert(META_CLIENT_INFO.into(), self.client_info());
        }
        if let Some(trace) = self.options.trace_context.as_ref().and_then(|hook| hook()) {
            meta.insert("traceparent".into(), json!(trace.traceparent));
            if let Some(state) = trace.tracestate {
                meta.insert("tracestate".into(), json!(state));
            }
        }
        meta
    }

    /// `MCP-Protocol-Version` (not on `initialize`, whose version isn't
    /// agreed yet) and, modern, `Mcp-Method`. HTTP only.
    fn base_headers(&self, era: &Era, method: &str) -> Vec<(String, String)> {
        let mut headers = Vec::new();
        if self.wire != Wire::Http {
            return headers;
        }
        if method != "initialize" {
            headers.push(("MCP-Protocol-Version".into(), era.version().to_string()));
        }
        if matches!(era, Era::Modern(_)) {
            headers.push(("Mcp-Method".into(), headers::encode_text(method)));
        }
        headers
    }

    /// Sends one request on `link` and waits, under its span, its timeout
    /// and its token. A result asking for input is a failure.
    async fn send(
        &self,
        link: &Arc<dyn Link>,
        era: &Era,
        request: Request<'_>,
    ) -> Result<Answer, McpError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let scope = RequestScope::start(RequestInfo {
            method: request.method,
            tool: request.tool,
            version: era.version(),
            id,
            wire: self.wire,
            address: self.address.as_ref(),
            session: link.session_id(),
        });
        let span = scope.span.clone();
        let outcome = async {
            let Request {
                method,
                mut params,
                timeout,
                token,
                cancellable,
                ..
            } = request;
            let mut headers = self.base_headers(era, method);
            let meta = self.meta(era);
            let cancel_meta = meta.clone();
            if !meta.is_empty() {
                params.insert("_meta".into(), Value::Object(meta));
            }
            let mirror = self.mirrored(era, method, &params);
            headers.extend(mirror);
            // Modern HTTP cancels by aborting the request alone.
            let cancel = (cancellable
                && !(self.wire == Wire::Http && matches!(era, Era::Modern(_))))
            .then(|| {
                let mut cancel = Map::new();
                cancel.insert("requestId".into(), json!(id));
                cancel.insert("reason".into(), json!("cancelled by the caller"));
                if matches!(era, Era::Modern(_)) && !cancel_meta.is_empty() {
                    cancel.insert("_meta".into(), Value::Object(cancel_meta));
                }
                jsonrpc::notification("notifications/cancelled", Some(cancel))
            });
            let body = jsonrpc::request(id, method, params);
            let answer = link.request(Outgoing {
                id,
                body,
                headers,
                cancel,
            });
            let timed = async {
                match timeout {
                    Some(after) => tokio::time::timeout(after, answer)
                        .await
                        .unwrap_or_else(|_| {
                            Err(McpError::Timeout {
                                method: method.to_string(),
                                after,
                            })
                        }),
                    None => answer.await,
                }
            };
            let answer = match token {
                Some(token) => tokio::select! {
                    biased;
                    () = token.cancelled() => Err(McpError::Cancelled),
                    answer = timed => answer,
                },
                None => timed.await,
            };
            match answer {
                Ok(Answer::Result(result)) => complete(result),
                other => other,
            }
        }
        .instrument(span)
        .await;
        scope.finish(&outcome);
        outcome
    }

    /// `Mcp-Name` and the `Mcp-Param-*` headers of a modern HTTP call.
    fn mirrored(
        &self,
        era: &Era,
        method: &str,
        params: &Map<String, Value>,
    ) -> Vec<(String, String)> {
        if self.wire != Wire::Http || !matches!(era, Era::Modern(_)) || method != "tools/call" {
            return Vec::new();
        }
        let Some(name) = params.get("name").and_then(Value::as_str) else {
            return Vec::new();
        };
        let mut headers = vec![("Mcp-Name".to_string(), headers::encode_text(name))];
        if let Some(annotated) = self.header_params.read().unwrap().get(name) {
            let arguments = params.get("arguments").cloned().unwrap_or(Value::Null);
            headers.extend(headers::param_headers(annotated, &arguments));
        }
        headers
    }

    /// The live connection, started again first if a stdio server exited.
    async fn live(&self) -> Result<Arc<Live>, McpError> {
        let mut slot = self.live.lock().await;
        if self.closed.load(Ordering::Acquire) {
            return Err(McpError::Closed);
        }
        let ended = match &*slot {
            Some(live) => match live.link.ended() {
                None => return Ok(live.clone()),
                Some(ended) => ended,
            },
            None => return Err(McpError::Closed),
        };
        if !self.restarts() {
            return Err(ended);
        }
        self.log
            .information(format!("starting the MCP server again: {ended}"));
        // Dropping the old one stops whatever is left of its process tree.
        slot.take();
        let (live, _) = self.open().await?;
        let live = Arc::new(live);
        *slot = Some(live.clone());
        Ok(live)
    }

    fn restarts(&self) -> bool {
        #[cfg(feature = "stdio")]
        if matches!(self.kind, ServerKind::Stdio(_)) {
            return self.options.restart_on_exit;
        }
        false
    }

    /// A request on the live connection, with the retries the spec
    /// recommends: a `HeaderMismatch` lists the tools again and retries a
    /// modern HTTP call once; a legacy HTTP session that ended is
    /// initialized again and the request retried once.
    async fn request(
        &self,
        method: &str,
        params: Map<String, Value>,
        timeout: Option<Duration>,
        token: Option<&CancellationToken>,
        tool: Option<&str>,
    ) -> Result<Value, McpError> {
        let mut relisted = false;
        #[cfg_attr(not(feature = "http"), allow(unused_mut, unused_variables))]
        let mut reinitialized = false;
        loop {
            let live = self.live().await?;
            let answer = self
                .send(
                    &live.link,
                    &live.era,
                    Request {
                        method,
                        params: params.clone(),
                        timeout,
                        token,
                        tool,
                        cancellable: true,
                    },
                )
                .await;
            let modern_http = self.wire == Wire::Http && matches!(live.era, Era::Modern(_));
            match answer {
                Ok(Answer::Result(result)) => return Ok(result),
                Ok(Answer::Error(error))
                    if error.code == HEADER_MISMATCH
                        && modern_http
                        && method == "tools/call"
                        && !relisted =>
                {
                    relisted = true;
                    self.log.debug(|| {
                        format!("{method} got a header mismatch; listing the tools again")
                    });
                    // Boxed: listing is itself a request.
                    Box::pin(self.list_tools(modern_http)).await?;
                }
                Ok(Answer::Error(error)) => {
                    return Err(McpError::Protocol {
                        code: error.code,
                        message: error.message,
                    });
                }
                #[cfg(feature = "http")]
                Err(McpError::Http {
                    status: 404,
                    message,
                }) if message == SESSION_ENDED && !reinitialized => {
                    reinitialized = true;
                    self.log
                        .debug(|| "the session ended; initializing again".to_string());
                    self.reinitialize(&live).await?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// A legacy HTTP session ended: `initialize` again on the same link.
    #[cfg(feature = "http")]
    async fn reinitialize(&self, live: &Arc<Live>) -> Result<(), McpError> {
        let mut slot = self.live.lock().await;
        if !slot
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, live))
        {
            // Another request did it already.
            return Ok(());
        }
        let (era, _) = self
            .initialize(&live.link, live.era.version().to_string())
            .await?;
        *slot = Some(Arc::new(Live {
            link: live.link.clone(),
            era,
            #[cfg(feature = "stdio")]
            process: None,
        }));
        Ok(())
    }

    /// `tools/list`, every page, as Strata keeps them: a repeated name keeps
    /// the first, and on modern HTTP a tool with an invalid `x-mcp-header`
    /// is left out. Refreshes the header annotations.
    async fn list_tools(&self, modern_http: bool) -> Result<Vec<McpTool>, McpError> {
        let mut listed = Vec::new();
        let mut cursors = HashSet::new();
        let mut cursor: Option<String> = None;
        for page in 0.. {
            if page == 1000 {
                return Err(McpError::Connect(
                    "tools/list gave more than 1,000 pages".to_string(),
                ));
            }
            let mut params = Map::new();
            if let Some(cursor) = &cursor {
                params.insert("cursor".into(), json!(cursor));
            }
            let result = self
                .request(
                    "tools/list",
                    params,
                    Some(self.options.request_timeout),
                    None,
                    None,
                )
                .await?;
            for tool in result
                .get("tools")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                match McpTool::from_value(tool) {
                    Some(tool) => listed.push(tool),
                    None => self
                        .log
                        .warning("left out a listed tool with no name".to_string()),
                }
            }
            match result.get("nextCursor").and_then(Value::as_str) {
                Some(next) => {
                    if !cursors.insert(next.to_string()) {
                        return Err(McpError::Connect(format!(
                            "tools/list gave the cursor '{next}' twice"
                        )));
                    }
                    cursor = Some(next.to_string());
                }
                None => break,
            }
        }
        let mut names = HashSet::new();
        let mut tools = Vec::new();
        let mut annotations = HashMap::new();
        for tool in listed {
            if !names.insert(tool.name.clone()) {
                self.log.warning(format!(
                    "the server listed the tool '{}' twice; the first is kept",
                    tool.name
                ));
                continue;
            }
            if modern_http {
                match headers::header_params(&tool.input_schema) {
                    Ok(params) => {
                        if !params.is_empty() {
                            annotations.insert(tool.name.clone(), params);
                        }
                    }
                    Err(reason) => {
                        self.log
                            .warning(format!("left out the tool '{}': {reason}", tool.name));
                        continue;
                    }
                }
            }
            tools.push(tool);
        }
        *self.header_params.write().unwrap() = annotations;
        Ok(tools)
    }

    pub(crate) async fn call_tool(
        &self,
        name: &str,
        arguments: Value,
        token: Option<&CancellationToken>,
    ) -> Result<McpToolResult, McpError> {
        let mut params = Map::new();
        params.insert("name".into(), json!(name));
        params.insert("arguments".into(), arguments);
        let result = self
            .request(
                "tools/call",
                params,
                self.options.call_timeout,
                token,
                Some(name),
            )
            .await?;
        Ok(McpToolResult::from_value(&result))
    }

    async fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.shut_down().await;
    }

    async fn shut_down(&self) {
        let live = self.live.lock().await.take();
        if let Some(live) = live {
            let _ = tokio::time::timeout(self.options.request_timeout, live.link.close()).await;
            #[cfg(feature = "stdio")]
            if let Some(process) = &live.process {
                process.shutdown().await;
            }
        }
    }
}

/// A result's `resultType`: `complete` or absent is the result;
/// `input_required` asks for input Strata doesn't provide.
fn complete(result: Value) -> Result<Answer, McpError> {
    match result.get("resultType").and_then(Value::as_str) {
        None | Some("complete") => Ok(Answer::Result(result)),
        Some("input_required") => {
            let asked: Vec<String> = result
                .get("inputRequests")
                .and_then(Value::as_object)
                .map(|requests| {
                    requests
                        .values()
                        .filter_map(|request| request.get("method").and_then(Value::as_str))
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            Err(McpError::InputRequired(if asked.is_empty() {
                "unspecified input".to_string()
            } else {
                asked.join(", ")
            }))
        }
        Some(other) => Err(McpError::Invalid(format!(
            "an unknown resultType '{other}'"
        ))),
    }
}

fn text(value: &Value, name: &str) -> Option<String> {
    value.get(name).and_then(Value::as_str).map(str::to_string)
}
