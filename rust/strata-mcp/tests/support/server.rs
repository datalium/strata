//! A scripted MCP server over a line-framed stream pair, for the tests
//! (`design/50-MCP-Client.md`, "Tests"): in memory behind
//! `McpServer::streams`, and over stdin / stdout in the
//! `mcp_fixture_server` example. No SDK: it reads and writes the JSON-RPC
//! itself, and records what it received.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::oneshot;

/// Which era the server speaks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Era {
    /// `2026-07-28` only: `initialize` is an unknown method.
    Modern,
    /// `initialize` at this version; `server/discover` is method not found.
    Legacy(String),
    /// A legacy server that doesn't answer before `initialize`.
    SilentLegacy(String),
    /// Both.
    Dual,
    /// Answers the probe with UnsupportedProtocolVersion, listing these.
    Unsupported(Vec<String>),
}

impl Era {
    pub fn parse(text: &str) -> Self {
        match text {
            "modern" => Era::Modern,
            "dual" => Era::Dual,
            other => match other.split_once(':') {
                Some(("legacy", version)) => Era::Legacy(version.to_string()),
                Some(("silent", version)) => Era::SilentLegacy(version.to_string()),
                Some(("unsupported", versions)) => {
                    Era::Unsupported(versions.split(',').map(str::to_string).collect())
                }
                _ => panic!("unknown era {other}"),
            },
        }
    }
}

#[derive(Clone)]
pub struct Script {
    pub era: Era,
    /// `tools/list` pages of this many tools; 0 is one page.
    pub page_size: usize,
    /// Gives the same `nextCursor` forever.
    pub repeat_cursor: bool,
    /// Lists `echo` twice.
    pub duplicate: bool,
    /// Ignores the end of its input, so closing must stop it.
    pub stubborn: bool,
    /// Everything received, in order.
    pub received: Arc<Mutex<Vec<Value>>>,
}

impl Script {
    pub fn new(era: Era) -> Self {
        Self {
            era,
            page_size: 0,
            repeat_cursor: false,
            duplicate: false,
            stubborn: false,
            received: Arc::default(),
        }
    }

    pub fn received(&self) -> Vec<Value> {
        self.received.lock().unwrap().clone()
    }

    pub fn methods(&self) -> Vec<String> {
        self.received()
            .iter()
            .filter_map(|m| m["method"].as_str().map(str::to_string))
            .collect()
    }
}

pub fn tools() -> Vec<Value> {
    vec![
        json!({"name": "echo", "description": "Echoes its arguments", "inputSchema": {"type": "object", "properties": {"text": {"type": "string"}}}}),
        json!({"name": "fail", "title": "Always fails", "inputSchema": {"type": "object"}}),
        json!({"name": "slow", "description": "Waits ms milliseconds", "inputSchema": {"type": "object", "properties": {"ms": {"type": "integer"}}}}),
        json!({"name": "crash", "description": "Exits", "inputSchema": {"type": "object"}}),
        json!({"name": "big", "description": "Answers with bytes characters", "inputSchema": {"type": "object", "properties": {"bytes": {"type": "integer"}}}}),
        json!({"name": "garbage", "description": "Writes a line that isn't JSON", "inputSchema": {"type": "object"}}),
        json!({"name": "ask", "description": "Asks for input", "inputSchema": {"type": "object"}}),
        json!({"name": "ping_me", "description": "Asks the client things first", "inputSchema": {"type": "object"}}),
        json!({"name": "structured", "description": "Structured content", "inputSchema": {"type": "object"}}),
        json!({"name": "image", "description": "An image", "inputSchema": {"type": "object"}}),
        json!({"name": "meta", "description": "Answers with the _meta it got", "inputSchema": {"type": "object"}}),
        json!({"name": "env", "description": "Answers with an environment variable", "inputSchema": {"type": "object", "properties": {"name": {"type": "string"}}}}),
    ]
}

type Out = Arc<tokio::sync::Mutex<Box<dyn AsyncWrite + Send + Unpin>>>;

async fn write(out: &Out, message: &Value) {
    let mut line = serde_json::to_vec(message).unwrap();
    line.push(b'\n');
    let mut out = out.lock().await;
    let _ = out.write_all(&line).await;
    let _ = out.flush().await;
}

/// How serving ended.
#[derive(Debug, PartialEq, Eq)]
pub enum Ended {
    /// The input ended.
    Eof,
    /// A `crash` call.
    Crashed,
}

/// Serves `script` until the input ends or a `crash` call.
pub async fn serve(
    script: Script,
    from_client: impl AsyncRead + Send + Unpin + 'static,
    to_client: impl AsyncWrite + Send + Unpin + 'static,
) -> Ended {
    let out: Out = Arc::new(tokio::sync::Mutex::new(Box::new(to_client)));
    let asked: Arc<Mutex<HashMap<String, oneshot::Sender<Value>>>> = Arc::default();
    let initialized = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (crash_tx, mut crash_rx) = tokio::sync::mpsc::channel::<()>(1);
    let mut lines = BufReader::new(from_client).lines();
    loop {
        let line = tokio::select! {
            line = lines.next_line() => line,
            _ = crash_rx.recv() => return Ended::Crashed,
        };
        let Ok(Some(line)) = line else {
            if script.stubborn {
                std::future::pending::<()>().await;
            }
            return Ended::Eof;
        };
        let message: Value = serde_json::from_str(&line).expect("the client writes JSON");
        script.received.lock().unwrap().push(message.clone());
        // An answer to one of our requests.
        if message.get("method").is_none() {
            let id = message["id"].to_string();
            if let Some(waiting) = asked.lock().unwrap().remove(&id) {
                let _ = waiting.send(message);
            }
            continue;
        }
        let method = message["method"].as_str().unwrap().to_string();
        let Some(id) = message.get("id").cloned() else {
            if method == "notifications/initialized" {
                initialized.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            continue;
        };
        let script = script.clone();
        let out = out.clone();
        let asked = asked.clone();
        let initialized = initialized.clone();
        let crash = crash_tx.clone();
        tokio::spawn(async move {
            let reply = answer(&script, &method, &message, &out, &asked, &initialized).await;
            match reply {
                Reply::Answer(body) => {
                    write(&out, &json!({"jsonrpc": "2.0", "id": id, "result": body})).await
                }
                Reply::Error(code, text) => write(
                    &out,
                    &json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": text}}),
                )
                .await,
                Reply::ErrorWith(error) => {
                    write(&out, &json!({"jsonrpc": "2.0", "id": id, "error": error})).await
                }
                Reply::Raw(line) => {
                    let mut out = out.lock().await;
                    let _ = out.write_all(line.as_bytes()).await;
                    let _ = out.flush().await;
                }
                Reply::Nothing => {}
                Reply::Crash => {
                    let _ = crash.send(()).await;
                }
            }
        });
    }
}

enum Reply {
    Answer(Value),
    Error(i64, String),
    ErrorWith(Value),
    Raw(String),
    Nothing,
    Crash,
}

fn modern_meta_ok(message: &Value) -> bool {
    let meta = &message["params"]["_meta"];
    meta["io.modelcontextprotocol/protocolVersion"] == "2026-07-28"
        && meta["io.modelcontextprotocol/clientCapabilities"].is_object()
        && meta["io.modelcontextprotocol/clientInfo"]["name"].is_string()
}

async fn answer(
    script: &Script,
    method: &str,
    message: &Value,
    out: &Out,
    asked: &Arc<Mutex<HashMap<String, oneshot::Sender<Value>>>>,
    initialized: &std::sync::atomic::AtomicBool,
) -> Reply {
    let speaks_modern = matches!(script.era, Era::Modern | Era::Dual);
    let speaks_legacy = matches!(
        script.era,
        Era::Legacy(_) | Era::SilentLegacy(_) | Era::Dual
    );
    let legacy_version = match &script.era {
        Era::Legacy(v) | Era::SilentLegacy(v) => v.clone(),
        _ => "2025-11-25".to_string(),
    };
    match method {
        "server/discover" => match &script.era {
            Era::Unsupported(versions) => Reply::ErrorWith(json!({
                "code": -32022,
                "message": "Unsupported protocol version",
                "data": {"supported": versions, "requested": message["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"]},
            })),
            Era::SilentLegacy(_) => Reply::Nothing,
            _ if speaks_modern => Reply::Answer(json!({
                "resultType": "complete",
                "supportedVersions": if script.era == Era::Dual { json!(["2026-07-28", "2025-11-25"]) } else { json!(["2026-07-28"]) },
                "capabilities": {"tools": {}},
                "_meta": {"io.modelcontextprotocol/serverInfo": {"name": "fixture", "version": "1.2.3"}},
                "instructions": "Use echo to echo.",
            })),
            _ => Reply::Error(-32601, "Method not found".into()),
        },
        "initialize" => {
            if !speaks_legacy && !matches!(script.era, Era::Unsupported(_)) {
                return Reply::Error(-32601, "Method not found: initialize".into());
            }
            let requested = message["params"]["protocolVersion"]
                .as_str()
                .unwrap_or_default();
            let version = if requested == legacy_version
                || matches!(script.era, Era::Dual | Era::Unsupported(_))
            {
                requested.to_string()
            } else {
                legacy_version
            };
            Reply::Answer(json!({
                "protocolVersion": version,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "legacy-fixture", "version": "0.1.0"},
                "instructions": "Legacy instructions.",
            }))
        }
        _ => {
            let modern_request = message["params"]["_meta"]
                .get("io.modelcontextprotocol/protocolVersion")
                .is_some();
            if modern_request {
                if !speaks_modern || !modern_meta_ok(message) {
                    return Reply::Error(-32602, "missing or invalid _meta".into());
                }
            } else if !initialized.load(std::sync::atomic::Ordering::SeqCst) {
                return Reply::Error(-32002, "Server not initialized".into());
            }
            match method {
                "tools/list" => list(script, message),
                "tools/call" => call(script, message, out, asked).await,
                "ping" => Reply::Answer(json!({})),
                _ => Reply::Error(-32601, format!("Method not found: {method}")),
            }
        }
    }
}

fn list(script: &Script, message: &Value) -> Reply {
    let mut all = tools();
    if script.duplicate {
        all.push(json!({"name": "echo", "description": "A second echo", "inputSchema": {"type": "object"}}));
    }
    if script.page_size == 0 {
        return Reply::Answer(json!({"tools": all}));
    }
    let page: usize = message["params"]["cursor"]
        .as_str()
        .and_then(|c| c.strip_prefix("page-"))
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);
    let start = page * script.page_size;
    let chunk: Vec<Value> = all
        .iter()
        .skip(start)
        .take(script.page_size)
        .cloned()
        .collect();
    let mut result = json!({"tools": chunk});
    if script.repeat_cursor {
        result["nextCursor"] = json!("page-1");
    } else if start + script.page_size < all.len() {
        result["nextCursor"] = json!(format!("page-{}", page + 1));
    }
    Reply::Answer(result)
}

async fn call(
    script: &Script,
    message: &Value,
    out: &Out,
    asked: &Arc<Mutex<HashMap<String, oneshot::Sender<Value>>>>,
) -> Reply {
    let _ = script;
    let name = message["params"]["name"].as_str().unwrap_or_default();
    let arguments = &message["params"]["arguments"];
    let text = |t: String| json!({"content": [{"type": "text", "text": t}]});
    match name {
        "echo" => Reply::Answer(text(arguments.to_string())),
        "fail" => Reply::Answer(
            json!({"content": [{"type": "text", "text": "it failed"}], "isError": true}),
        ),
        "slow" => {
            let ms = arguments["ms"].as_u64().unwrap_or(1000);
            tokio::time::sleep(Duration::from_millis(ms)).await;
            Reply::Answer(text(format!("slept {ms}")))
        }
        "crash" => Reply::Crash,
        "big" => {
            let bytes = arguments["bytes"].as_u64().unwrap_or(10) as usize;
            Reply::Answer(text("x".repeat(bytes)))
        }
        "garbage" => Reply::Raw("this is not json\n".into()),
        "ask" => Reply::Answer(json!({
            "resultType": "input_required",
            "inputRequests": {"q1": {"method": "elicitation/create", "params": {}}},
        })),
        "ping_me" => {
            let mut answers = Vec::new();
            for (id, method) in [("s-1", "ping"), ("s-2", "sampling/createMessage")] {
                let (tx, rx) = oneshot::channel();
                asked.lock().unwrap().insert(json!(id).to_string(), tx);
                write(
                    out,
                    &json!({"jsonrpc": "2.0", "id": id, "method": method, "params": {}}),
                )
                .await;
                let reply = tokio::time::timeout(Duration::from_secs(5), rx).await;
                answers.push(reply.ok().and_then(Result::ok).unwrap_or(Value::Null));
            }
            write(out, &json!({"jsonrpc": "2.0", "method": "notifications/message", "params": {"level": "info", "data": "hi"}})).await;
            Reply::Answer(text(json!(answers).to_string()))
        }
        "structured" => Reply::Answer(json!({
            "content": [{"type": "text", "text": "{\"temperature\": 22.5}"}],
            "structuredContent": {"temperature": 22.5},
        })),
        "image" => Reply::Answer(
            json!({"content": [{"type": "image", "data": "iVBORw0KGgo=", "mimeType": "image/png"}]}),
        ),
        "meta" => Reply::Answer(text(message["params"]["_meta"].to_string())),
        "env" => Reply::Answer(text(
            std::env::var(arguments["name"].as_str().unwrap_or_default())
                .unwrap_or_else(|_| "<unset>".into()),
        )),
        other => Reply::Error(-32602, format!("Unknown tool: {other}")),
    }
}
