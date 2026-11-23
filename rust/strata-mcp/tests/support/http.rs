//! A Streamable HTTP MCP server on loopback for the tests, written on a raw
//! socket so the tests see every header and every aborted request: modern
//! (validating the mirrored headers, as the spec requires a server to) or
//! legacy (with sessions), answering with JSON or SSE.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mode {
    Modern,
    Legacy(String),
}

#[derive(Debug, Clone)]
pub struct Seen {
    pub verb: String,
    pub headers: HashMap<String, String>,
    pub body: Value,
}

impl Seen {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(&name.to_lowercase()).map(String::as_str)
    }

    pub fn method(&self) -> &str {
        self.body["method"].as_str().unwrap_or_default()
    }
}

#[derive(Default)]
pub struct State {
    pub seen: Vec<Seen>,
    /// Answer requests with an SSE stream.
    pub sse: bool,
    pub lists: usize,
    /// Annotate `execute_sql` only from the second `tools/list` on.
    pub annotate_later: bool,
    pub session: Option<String>,
    pub sessions: usize,
    /// End the session after this many requests in it.
    pub expire_after: Option<usize>,
    pub in_session: usize,
    pub require_token: Option<String>,
    /// Requests whose connection closed before their answer.
    pub aborted: Vec<Value>,
    /// Answers the client posted to the server's requests.
    pub answers: Vec<Value>,
    pub initialized: bool,
}

pub struct HttpFixture {
    pub url: String,
    pub mode: Mode,
    pub state: Arc<Mutex<State>>,
}

impl HttpFixture {
    pub async fn start(mode: Mode) -> Self {
        Self::start_with(mode, State::default()).await
    }

    pub async fn start_with(mode: Mode, state: State) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/mcp", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(state));
        let shared = state.clone();
        let served = mode.clone();
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                tokio::spawn(handle(socket, served.clone(), shared.clone()));
            }
        });
        Self { url, mode, state }
    }

    pub fn seen(&self) -> Vec<Seen> {
        self.state.lock().unwrap().seen.clone()
    }

    pub fn posts(&self, method: &str) -> Vec<Seen> {
        self.seen()
            .into_iter()
            .filter(|s| s.verb == "POST" && s.method() == method)
            .collect()
    }
}

fn tools(state: &State) -> Value {
    let region = if state.annotate_later && state.lists < 2 {
        json!({"type": "string"})
    } else {
        json!({"type": "string", "x-mcp-header": "Region"})
    };
    json!([
        {"name": "echo", "inputSchema": {"type": "object"}},
        {"name": "fail", "inputSchema": {"type": "object"}},
        {"name": "slow", "inputSchema": {"type": "object", "properties": {"ms": {"type": "integer"}}}},
        {"name": "boom", "inputSchema": {"type": "object"}},
        {"name": "ping_me", "inputSchema": {"type": "object"}},
        {"name": "execute_sql", "inputSchema": {"type": "object", "properties": {"region": region, "query": {"type": "string"}}}},
        {"name": "bad_header", "inputSchema": {"type": "object", "properties": {"price": {"type": "number", "x-mcp-header": "Price"}}}},
    ])
}

fn decode(value: &str) -> String {
    match value
        .strip_prefix("=?base64?")
        .and_then(|v| v.strip_suffix("?="))
    {
        Some(encoded) => String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .unwrap(),
        )
        .unwrap(),
        None => value.to_string(),
    }
}

enum Body {
    Json(u16, Value, Vec<(String, String)>),
    Text(u16, String),
    Empty(u16),
}

async fn handle(mut socket: TcpStream, mode: Mode, state: Arc<Mutex<State>>) {
    let Some(seen) = read(&mut socket).await else {
        return;
    };
    state.lock().unwrap().seen.push(seen.clone());
    let required = state.lock().unwrap().require_token.clone();
    if let Some(token) = required
        && seen.header("authorization") != Some(format!("Bearer {token}").as_str())
    {
        return respond(&mut socket, Body::Empty(401)).await;
    }
    if seen.verb == "DELETE" {
        return respond(&mut socket, Body::Empty(200)).await;
    }
    let body = &seen.body;
    // An answer to one of the server's requests, or a notification.
    if body.get("method").is_none() {
        state.lock().unwrap().answers.push(body.clone());
        return respond(&mut socket, Body::Empty(202)).await;
    }
    if body.get("id").is_none() {
        if body["method"] == "notifications/initialized" {
            state.lock().unwrap().initialized = true;
        }
        return respond(&mut socket, Body::Empty(202)).await;
    }
    let id = body["id"].clone();
    let reply = match &mode {
        Mode::Modern => modern(&seen, &state),
        Mode::Legacy(version) => legacy(&seen, &state, version),
    };
    let reply = match reply {
        Err(body) => body,
        Ok(Call::Answer(result)) => answer(&state, &id, result),
        Ok(Call::Error(code, message)) => Body::Json(
            200,
            json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}}),
            vec![],
        ),
        Ok(Call::Slow(ms)) => {
            let mut buffer = [0u8; 16];
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(ms)) => answer(&state, &id, json!({"content": [{"type": "text", "text": "slept"}]})),
                _ = socket.read(&mut buffer) => {
                    state.lock().unwrap().aborted.push(id);
                    return;
                }
            }
        }
        Ok(Call::PingFirst) => {
            // A legacy server's request on the response stream, answered
            // by a POST of the client's.
            let head =
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n";
            let _ = socket.write_all(head.as_bytes()).await;
            let ping = json!({"jsonrpc": "2.0", "id": "srv-1", "method": "ping"});
            let _ = socket
                .write_all(format!("data: {ping}\n\n").as_bytes())
                .await;
            let _ = socket.flush().await;
            let mut answered = Value::Null;
            for _ in 0..200 {
                let found = state
                    .lock()
                    .unwrap()
                    .answers
                    .iter()
                    .find(|a| a["id"] == "srv-1")
                    .cloned();
                if let Some(found) = found {
                    answered = found;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let done = json!({"jsonrpc": "2.0", "id": id, "result": {"content": [{"type": "text", "text": answered.to_string()}]}});
            let _ = socket
                .write_all(format!("data: {done}\n\n").as_bytes())
                .await;
            let _ = socket.shutdown().await;
            return;
        }
    };
    respond(&mut socket, reply).await;
}

enum Call {
    Answer(Value),
    Error(i64, String),
    Slow(u64),
    PingFirst,
}

fn mismatch(message: &str) -> Body {
    Body::Json(
        400,
        json!({"jsonrpc": "2.0", "id": null, "error": {"code": -32020, "message": message}}),
        vec![],
    )
}

fn modern(seen: &Seen, state: &Arc<Mutex<State>>) -> Result<Call, Body> {
    let body = &seen.body;
    let method = seen.method();
    let version = body["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"]
        .as_str()
        .unwrap_or_default();
    if seen.header("mcp-protocol-version") != Some(version) || version.is_empty() {
        return Err(mismatch("Header mismatch: MCP-Protocol-Version"));
    }
    if version != "2026-07-28" {
        return Err(Body::Json(
            400,
            json!({"jsonrpc": "2.0", "id": body["id"], "error": {"code": -32022, "message": "Unsupported protocol version", "data": {"supported": ["2026-07-28"]}}}),
            vec![],
        ));
    }
    if seen.header("mcp-method") != Some(method) {
        return Err(mismatch("Header mismatch: Mcp-Method"));
    }
    match method {
        "server/discover" => Ok(Call::Answer(json!({
            "supportedVersions": ["2026-07-28"],
            "capabilities": {"tools": {}},
            "_meta": {"io.modelcontextprotocol/serverInfo": {"name": "http-fixture", "version": "2.0.0"}},
        }))),
        "tools/list" => {
            let mut state = state.lock().unwrap();
            state.lists += 1;
            Ok(Call::Answer(json!({"tools": tools(&state)})))
        }
        "tools/call" => {
            let name = body["params"]["name"].as_str().unwrap_or_default();
            if seen.header("mcp-name").map(decode).as_deref() != Some(name) {
                return Err(mismatch("Header mismatch: Mcp-Name"));
            }
            // The server always wants the header, even when it listed the
            // tool without its annotation (`annotate_later`).
            if name == "execute_sql" {
                let expected = body["params"]["arguments"]["region"].as_str();
                let sent = seen.header("mcp-param-region").map(decode);
                if expected.is_some() && sent.as_deref() != expected {
                    return Err(mismatch("Header mismatch: Mcp-Param-Region"));
                }
            }
            Ok(call(body))
        }
        _ => Ok(Call::Error(-32601, format!("Method not found: {method}"))),
    }
}

fn legacy(seen: &Seen, state: &Arc<Mutex<State>>, version: &str) -> Result<Call, Body> {
    let method = seen.method();
    let mut guard = state.lock().unwrap();
    match method {
        "server/discover" if seen.header("mcp-session-id").is_none() => Err(Body::Text(
            400,
            "Bad Request: Server not initialized".into(),
        )),
        "initialize" => {
            guard.sessions += 1;
            let session = format!("session-{}", guard.sessions);
            guard.session = Some(session.clone());
            guard.in_session = 0;
            Err(Body::Json(
                200,
                json!({"jsonrpc": "2.0", "id": seen.body["id"], "result": {
                    "protocolVersion": version,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "legacy-http", "version": "1.0.0"},
                }}),
                vec![("mcp-session-id".into(), session)],
            ))
        }
        _ => {
            let Some(sent) = seen.header("mcp-session-id") else {
                return Err(Body::Text(
                    400,
                    "Bad Request: No valid session ID provided".into(),
                ));
            };
            if guard.session.as_deref() != Some(sent) {
                return Err(Body::Empty(404));
            }
            guard.in_session += 1;
            if guard
                .expire_after
                .is_some_and(|after| guard.in_session > after)
            {
                guard.session = None;
                guard.expire_after = None;
                return Err(Body::Empty(404));
            }
            drop(guard);
            match method {
                "tools/list" => {
                    let mut state = state.lock().unwrap();
                    state.lists += 1;
                    Ok(Call::Answer(json!({"tools": tools(&state)})))
                }
                "tools/call" => Ok(call(&seen.body)),
                _ => Ok(Call::Error(-32601, format!("Method not found: {method}"))),
            }
        }
    }
}

fn call(body: &Value) -> Call {
    let arguments = &body["params"]["arguments"];
    let text = |t: String| json!({"content": [{"type": "text", "text": t}]});
    match body["params"]["name"].as_str().unwrap_or_default() {
        "echo" => Call::Answer(text(arguments.to_string())),
        "fail" => Call::Answer(
            json!({"content": [{"type": "text", "text": "it failed"}], "isError": true}),
        ),
        "slow" => Call::Slow(arguments["ms"].as_u64().unwrap_or(5000)),
        "ping_me" => Call::PingFirst,
        "execute_sql" => Call::Answer(text(format!("ran in {}", arguments["region"]))),
        "boom" => Call::Error(0, "boom".into()),
        other => Call::Error(-32602, format!("Unknown tool: {other}")),
    }
}

fn answer(state: &Arc<Mutex<State>>, id: &Value, result: Value) -> Body {
    let message = json!({"jsonrpc": "2.0", "id": id, "result": result});
    if state.lock().unwrap().sse {
        let progress = json!({"jsonrpc": "2.0", "method": "notifications/progress", "params": {"progress": 1}});
        Body::Text(
            200,
            format!(": hello\n\ndata: {progress}\n\nevent: message\ndata: {message}\n\n"),
        )
    } else {
        Body::Json(200, message, vec![])
    }
}

async fn respond(socket: &mut TcpStream, body: Body) {
    let (status, content_type, text, extra) = match body {
        Body::Json(_, value, extra) if value["error"]["code"] == 0 => {
            (503, "text/plain", "busy".to_string(), extra)
        }
        Body::Json(status, value, extra) => (status, "application/json", value.to_string(), extra),
        Body::Text(200, text) => (200, "text/event-stream", text, vec![]),
        Body::Text(status, text) => (status, "text/plain", text, vec![]),
        Body::Empty(status) => (status, "text/plain", String::new(), vec![]),
    };
    let mut head = format!(
        "HTTP/1.1 {status} X\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n",
        text.len()
    );
    for (name, value) in extra {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    let _ = socket.write_all(head.as_bytes()).await;
    let _ = socket.write_all(text.as_bytes()).await;
    let _ = socket.shutdown().await;
}

async fn read(socket: &mut TcpStream) -> Option<Seen> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        let read = socket.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            break end;
        }
    };
    let head = String::from_utf8_lossy(&buffer[..head_end]).to_string();
    let mut lines = head.lines();
    let verb = lines.next()?.split_whitespace().next()?.to_string();
    let headers: HashMap<String, String> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_lowercase(), value.trim().to_string()))
        .collect();
    let length: usize = headers
        .get("content-length")
        .and_then(|l| l.parse().ok())
        .unwrap_or(0);
    let mut body = buffer[head_end + 4..].to_vec();
    while body.len() < length {
        let read = socket.read(&mut chunk).await.ok()?;
        if read == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..read]);
    }
    Some(Seen {
        verb,
        headers,
        body: serde_json::from_slice(&body).unwrap_or(Value::Null),
    })
}
