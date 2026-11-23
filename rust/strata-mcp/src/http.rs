//! Streamable HTTP, both eras (`design/50-MCP-Client.md`, "Streamable
//! HTTP"): a POST per message, answered with JSON or an SSE stream; the
//! token hook; and legacy sessions.

use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderName, HeaderValue};
use reqwest::{Response, StatusCode, Url};
use serde_json::Value;

use crate::McpError;
use crate::jsonrpc::{Answer, Incoming, answer_server_request};
use crate::link::{BoxFuture, Link, Outgoing};
use crate::options::{Log, McpClientOptions};
use crate::sse;

/// Returns the token sent as `Authorization: Bearer <token>`, called before
/// each request; `Ok(None)` sends none.
pub type AccessTokenHook =
    Arc<dyn Fn() -> BoxFuture<'static, Result<Option<String>, String>> + Send + Sync>;

/// A server reached over Streamable HTTP. Built with
/// [`McpServer::http`](crate::McpServer::http).
#[derive(Clone)]
pub struct HttpServer {
    endpoint: Url,
    headers: Vec<(String, String)>,
    access_token: Option<AccessTokenHook>,
    client: Option<reqwest::Client>,
}

impl HttpServer {
    pub(crate) fn new(endpoint: &str) -> Result<Self, McpError> {
        let endpoint = Url::parse(endpoint)
            .map_err(|error| McpError::Connect(format!("'{endpoint}' isn't a URL: {error}")))?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            return Err(McpError::Connect(format!(
                "'{endpoint}' isn't an http or https URL"
            )));
        }
        Ok(Self {
            endpoint,
            headers: Vec::new(),
            access_token: None,
            client: None,
        })
    }

    /// A header sent on every request, after `Authorization`.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Called before each request for the bearer token, so a token that
    /// expires is refreshed by the code that owns it. An error fails the
    /// request.
    pub fn access_token<F, Fut, E>(mut self, hook: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<String>, E>> + Send + 'static,
        E: fmt::Display,
    {
        self.access_token = Some(Arc::new(move || {
            let token = hook();
            Box::pin(async move { token.await.map_err(|error| error.to_string()) })
        }));
        self
    }

    /// The client to send with (its own redirect policy applies), else one
    /// of the client's own, which follows no redirects.
    pub fn client(mut self, client: reqwest::Client) -> Self {
        self.client = Some(client);
        self
    }

    pub(crate) fn address(&self) -> Option<(String, u16)> {
        Some((
            self.endpoint.host_str()?.to_string(),
            self.endpoint.port_or_known_default()?,
        ))
    }
}

impl fmt::Debug for HttpServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpServer")
            .field("endpoint", &self.endpoint.as_str())
            .field(
                "headers",
                &self
                    .headers
                    .iter()
                    .map(|(name, _)| name)
                    .collect::<Vec<_>>(),
            )
            .field("access_token", &self.access_token.is_some())
            .finish_non_exhaustive()
    }
}

/// What a 404 to a request with a session id fails with: the session ended.
pub(crate) const SESSION_ENDED: &str = "the session ended";

const SESSION_HEADER: &str = "mcp-session-id";

struct Inner {
    client: reqwest::Client,
    endpoint: Url,
    headers: Vec<(HeaderName, HeaderValue)>,
    access_token: Option<AccessTokenHook>,
    max_message_bytes: usize,
    log: Log,
    session: Mutex<Option<String>>,
    /// The `MCP-Protocol-Version` last sent, for the session's DELETE.
    version: Mutex<Option<String>>,
    closed: AtomicBool,
}

pub(crate) struct HttpLink {
    inner: Arc<Inner>,
}

impl HttpLink {
    pub(crate) fn new(
        server: &HttpServer,
        options: &McpClientOptions,
        log: &Log,
    ) -> Result<Self, McpError> {
        let mut headers = Vec::new();
        for (name, value) in &server.headers {
            let parsed = HeaderName::try_from(name.as_str())
                .ok()
                .zip(HeaderValue::try_from(value.as_str()).ok());
            match parsed {
                Some(header) => headers.push(header),
                None => {
                    return Err(McpError::Connect(format!(
                        "the header '{name}' isn't a valid HTTP header"
                    )));
                }
            }
        }
        let client = match &server.client {
            Some(client) => client.clone(),
            None => own_client()?,
        };
        Ok(Self {
            inner: Arc::new(Inner {
                client,
                endpoint: server.endpoint.clone(),
                headers,
                access_token: server.access_token.clone(),
                max_message_bytes: options.max_message_bytes,
                log: log.clone(),
                session: Mutex::new(None),
                version: Mutex::new(None),
                closed: AtomicBool::new(false),
            }),
        })
    }
}

/// Redirects aren't followed, so a token isn't sent anywhere else.
fn own_client() -> Result<reqwest::Client, McpError> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| McpError::Connect(format!("couldn't build an HTTP client: {error}")))
}

impl Inner {
    fn session(&self) -> Option<String> {
        self.session.lock().unwrap().clone()
    }

    async fn post(&self, body: &Value, headers: &[(String, String)]) -> Result<Response, McpError> {
        let mut request = self
            .client
            .post(self.endpoint.clone())
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json, text/event-stream");
        request = self.authorized(request).await?;
        for (name, value) in headers {
            request = request.header(name.as_str(), value.as_str());
        }
        let body = serde_json::to_vec(body).expect("a JSON value serializes");
        request
            .body(body)
            .send()
            .await
            .map_err(|error| McpError::Transport(describe(&error)))
    }

    /// The token, the configured headers and the session.
    async fn authorized(
        &self,
        mut request: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, McpError> {
        if let Some(hook) = &self.access_token {
            match hook().await {
                Ok(Some(token)) => request = request.bearer_auth(token),
                Ok(None) => {}
                Err(error) => {
                    return Err(McpError::Transport(format!(
                        "the access token hook failed: {error}"
                    )));
                }
            }
        }
        for (name, value) in &self.headers {
            request = request.header(name, value);
        }
        if let Some(session) = self.session() {
            request = request.header(SESSION_HEADER, session);
        }
        Ok(request)
    }

    /// A body, read up to the message limit.
    async fn body(&self, mut response: Response) -> Result<Vec<u8>, McpError> {
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| McpError::Transport(describe(&error)))?
        {
            if body.len() + chunk.len() > self.max_message_bytes {
                return Err(McpError::Transport(format!(
                    "the server sent a message bigger than {} bytes",
                    self.max_message_bytes
                )));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }

    /// Reads an SSE response until the answer to `id`. Requests before it
    /// (legacy) are answered with a POST; notifications are ignored.
    async fn events(self: &Arc<Self>, mut response: Response, id: u64) -> Result<Answer, McpError> {
        let mut parser = sse::Parser::new(self.max_message_bytes);
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| McpError::Transport(describe(&error)))?
        {
            let events = parser.feed(&chunk).map_err(|_| {
                McpError::Transport(format!(
                    "the server sent a message bigger than {} bytes",
                    self.max_message_bytes
                ))
            })?;
            for event in events {
                if let Some(answer) = self.event(&event, Some(id))? {
                    return Ok(answer);
                }
            }
        }
        Err(McpError::Transport(
            "the response stream ended without an answer".to_string(),
        ))
    }

    /// One SSE event: the answer to `id`, or something handled on the side.
    fn event(
        self: &Arc<Self>,
        event: &sse::Event,
        id: Option<u64>,
    ) -> Result<Option<Answer>, McpError> {
        if event.kind != "message" {
            return Ok(None);
        }
        let message: Value = serde_json::from_str(&event.data).map_err(|_| {
            McpError::Transport("the server sent an event that isn't JSON".to_string())
        })?;
        match Incoming::parse(&message) {
            Incoming::Response {
                id: answered,
                answer,
            } if id.is_none() || answered == id => {
                return Ok(Some(answer));
            }
            Incoming::Request { id, method } => {
                self.log
                    .debug(|| format!("answered the server's request {method}"));
                let reply = answer_server_request(&id, &method);
                let inner = self.clone();
                tokio::spawn(async move {
                    let _ = inner.post(&reply, &[]).await;
                });
            }
            Incoming::Notification { method } => {
                self.log
                    .debug(|| format!("ignored the notification {method}"));
            }
            _ => self
                .log
                .debug(|| "ignored an event that isn't an answer to this request".to_string()),
        }
        Ok(None)
    }
}

/// A reqwest error without anything secret: reqwest's messages hold the URL,
/// never a header.
fn describe(error: &reqwest::Error) -> String {
    let mut text = error.to_string();
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}

fn is_event_stream(content_type: Option<&str>) -> bool {
    content_type.is_some_and(|value| {
        value
            .trim_start()
            .to_ascii_lowercase()
            .starts_with("text/event-stream")
    })
}

/// A whole response's answer. 401 and 403 are unauthorized; a 2xx is the
/// JSON-RPC message in its body (JSON, or the last answer in an SSE
/// stream); another 4xx with a JSON-RPC error body is that error, else the
/// status; a 5xx is the status.
pub(crate) fn reply_of(
    status: u16,
    content_type: Option<&str>,
    body: &[u8],
    id: u64,
) -> Result<Answer, McpError> {
    if status == 401 || status == 403 {
        return Err(McpError::Unauthorized(status));
    }
    let answer = if is_event_stream(content_type) {
        let mut parser = sse::Parser::new(usize::MAX);
        parser
            .feed(body)
            .unwrap_or_default()
            .into_iter()
            .rev()
            .find_map(|event| match serde_json::from_str::<Value>(&event.data) {
                Ok(message) => match Incoming::parse(&message) {
                    Incoming::Response { answer, .. } => Some(answer),
                    _ => None,
                },
                Err(_) => None,
            })
    } else {
        serde_json::from_slice::<Value>(body)
            .ok()
            .and_then(|message| match Incoming::parse(&message) {
                Incoming::Response { answer, .. } => Some(answer),
                _ => None,
            })
    };
    let snippet = || {
        let text = String::from_utf8_lossy(body);
        let text = text.trim();
        let cut: String = text.chars().take(200).collect();
        if cut.is_empty() {
            StatusCode::from_u16(status)
                .ok()
                .and_then(|status| status.canonical_reason())
                .unwrap_or("no body")
                .to_string()
        } else {
            cut
        }
    };
    let _ = id;
    match (status, answer) {
        (200..=299, Some(answer)) => Ok(answer),
        (200..=299, None) => Err(McpError::Transport(format!(
            "the server answered with HTTP {status} and no JSON-RPC answer"
        ))),
        (400..=499, Some(Answer::Error(error))) => Ok(Answer::Error(error)),
        (500..=599, Some(Answer::Error(error))) => Err(McpError::Http {
            status,
            message: error.message,
        }),
        _ => Err(McpError::Http {
            status,
            message: snippet(),
        }),
    }
}

/// Posts a request's `notifications/cancelled` when it is abandoned (legacy
/// HTTP); dropping the request aborts it either way.
struct Abandon {
    inner: Arc<Inner>,
    cancel: Option<Value>,
    answered: bool,
}

impl Drop for Abandon {
    fn drop(&mut self) {
        if self.answered {
            return;
        }
        if let (Some(cancel), Ok(runtime)) =
            (self.cancel.take(), tokio::runtime::Handle::try_current())
        {
            let inner = self.inner.clone();
            runtime.spawn(async move {
                let _ = inner.post(&cancel, &[]).await;
            });
        }
    }
}

impl Link for HttpLink {
    fn request(&self, outgoing: Outgoing) -> BoxFuture<'_, Result<Answer, McpError>> {
        Box::pin(async move {
            let inner = &self.inner;
            if inner.closed.load(Ordering::Acquire) {
                return Err(McpError::Closed);
            }
            let mut abandon = Abandon {
                inner: inner.clone(),
                cancel: outgoing.cancel,
                answered: false,
            };
            let sent_session = inner.session();
            if let Some((_, version)) = outgoing
                .headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("MCP-Protocol-Version"))
            {
                *inner.version.lock().unwrap() = Some(version.clone());
            }
            let response = inner.post(&outgoing.body, &outgoing.headers).await;
            let response = match response {
                Ok(response) => response,
                Err(error) => {
                    abandon.answered = true;
                    return Err(error);
                }
            };
            let status = response.status().as_u16();
            if status == 404 && sent_session.is_some() {
                abandon.answered = true;
                *inner.session.lock().unwrap() = None;
                return Err(McpError::Http {
                    status,
                    message: SESSION_ENDED.to_string(),
                });
            }
            if let Some(session) = response
                .headers()
                .get(SESSION_HEADER)
                .and_then(|value| value.to_str().ok())
            {
                *inner.session.lock().unwrap() = Some(session.to_string());
            }
            let content_type = response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let answer = if response.status().is_success()
                && is_event_stream(content_type.as_deref())
            {
                inner.events(response, outgoing.id).await
            } else {
                let body = inner.body(response).await;
                body.and_then(|body| reply_of(status, content_type.as_deref(), &body, outgoing.id))
            };
            abandon.answered = true;
            answer
        })
    }

    fn notify(
        &self,
        body: Value,
        headers: Vec<(String, String)>,
    ) -> BoxFuture<'_, Result<(), McpError>> {
        Box::pin(async move {
            let response = self.inner.post(&body, &headers).await?;
            let status = response.status().as_u16();
            if response.status().is_success() {
                return Ok(());
            }
            let content_type = response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let body = self.inner.body(response).await?;
            match reply_of(status, content_type.as_deref(), &body, 0)? {
                Answer::Error(error) => Err(McpError::Protocol {
                    code: error.code,
                    message: error.message,
                }),
                Answer::Result(_) => Ok(()),
            }
        })
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let inner = &self.inner;
            if inner.closed.swap(true, Ordering::AcqRel) {
                return;
            }
            // A legacy session is ended with a DELETE; a 405 means the
            // server doesn't let clients end them, which is fine.
            if inner.session().is_some() {
                let mut request = inner.client.delete(inner.endpoint.clone());
                let version = inner.version.lock().unwrap().clone();
                if let Some(version) = version {
                    request = request.header("MCP-Protocol-Version", version);
                }
                if let Ok(request) = inner.authorized(request).await {
                    let _ = request.send().await;
                }
                *inner.session.lock().unwrap() = None;
            }
        })
    }

    fn ended(&self) -> Option<McpError> {
        self.inner
            .closed
            .load(Ordering::Acquire)
            .then_some(McpError::Closed)
    }

    fn session_id(&self) -> Option<String> {
        self.inner.session()
    }
}
