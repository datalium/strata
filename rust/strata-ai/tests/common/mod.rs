//! Test support for `datalium-strata-ai` (`design/44-LLM-Integration.md`,
//! "Tests"): a scripted chat client, the tests' model in place of a provider
//! (mirrors the .NET tests' `ScriptedChatClient`), and a raw local HTTP server
//! that answers each request with bytes from a script, so the two clients are
//! tested against real requests, real server-sent events, and connections
//! that close mid-answer.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::BoxFuture;
use datalium_strata_ai::{
    ChatClient, ChatError, ChatRequest, ChatResponse, ChatStream, ChatUpdate, ChatUpdates,
    Embedder, EmbeddingRequest, EmbeddingResponse, ModelUsage,
};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub const MODEL: &str = "scripted-model";

/// One scripted answer: the text chunks to stream (one update each), the tool
/// calls to ask for (`(id, name, arguments as the model's JSON text)`), the
/// usage and finish reason to report, and optionally a failure, returned
/// before anything (`fail_after` 0) or after that many chunks.
#[derive(Clone)]
pub struct Scripted {
    pub chunks: Vec<String>,
    pub tool_calls: Vec<(String, String, String)>,
    pub usage: ModelUsage,
    pub finish_reason: String,
    pub failure: Option<ChatError>,
    pub fail_after: usize,
    /// Never answers: only a cancellation ends the call.
    pub hang: bool,
}

impl Scripted {
    pub fn text(chunks: &[&str]) -> Self {
        Self {
            chunks: chunks.iter().map(|c| c.to_string()).collect(),
            tool_calls: Vec::new(),
            usage: ModelUsage::new(Some(12), Some(5)),
            finish_reason: "stop".to_string(),
            failure: None,
            fail_after: 0,
            hang: false,
        }
    }

    /// An answer asking for these tool calls: `(id, name, arguments)`.
    pub fn tools(calls: &[(&str, &str, &str)]) -> Self {
        Self {
            tool_calls: calls
                .iter()
                .map(|(id, name, arguments)| {
                    (id.to_string(), name.to_string(), arguments.to_string())
                })
                .collect(),
            finish_reason: "tool_calls".to_string(),
            ..Self::text(&[])
        }
    }

    /// This answer, also asking for a tool call.
    pub fn with_tool_call(mut self, id: &str, name: &str, arguments: &str) -> Self {
        self.tool_calls
            .push((id.to_string(), name.to_string(), arguments.to_string()));
        self.finish_reason = "tool_calls".to_string();
        self
    }

    pub fn fails(error: ChatError) -> Self {
        Self {
            failure: Some(error),
            ..Self::text(&[])
        }
    }

    pub fn with_finish(mut self, reason: &str) -> Self {
        self.finish_reason = reason.to_string();
        self
    }

    pub fn with_usage(mut self, input: u64, output: u64) -> Self {
        self.usage = ModelUsage::new(Some(input), Some(output));
        self
    }

    pub fn failing_after(mut self, chunks: usize, error: ChatError) -> Self {
        self.failure = Some(error);
        self.fail_after = chunks;
        self
    }

    pub fn hanging() -> Self {
        Self {
            hang: true,
            ..Self::text(&[])
        }
    }
}

/// A request as the scripted client saw it.
#[derive(Clone)]
pub struct SeenRequest {
    pub request: ChatRequest,
    pub streamed: bool,
}

#[derive(Default)]
pub struct ScriptedChatClient {
    script: Mutex<VecDeque<Scripted>>,
    pub requests: Mutex<Vec<SeenRequest>>,
}

impl ScriptedChatClient {
    pub fn new(script: Vec<Scripted>) -> Arc<Self> {
        Arc::new(Self {
            script: Mutex::new(script.into()),
            requests: Mutex::new(Vec::new()),
        })
    }

    pub fn requests(&self) -> Vec<SeenRequest> {
        self.requests.lock().unwrap().clone()
    }

    fn next(&self, request: ChatRequest, streamed: bool) -> Scripted {
        self.requests
            .lock()
            .unwrap()
            .push(SeenRequest { request, streamed });
        self.script
            .lock()
            .unwrap()
            .pop_front()
            .expect("the script has no more answers")
    }
}

impl ChatClient for ScriptedChatClient {
    fn provider(&self) -> &str {
        "scripted"
    }

    fn default_model(&self) -> Option<&str> {
        Some(MODEL)
    }

    fn complete(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatResponse, ChatError>> {
        let model = request.model.clone().unwrap_or_else(|| MODEL.to_string());
        let step = self.next(request, false);
        Box::pin(async move {
            if step.hang {
                std::future::pending::<()>().await;
            }
            if let Some(failure) = step.failure {
                return Err(failure);
            }
            let mut updates = ChatUpdates::new();
            for chunk in step.chunks {
                updates.push(ChatUpdate::Text(chunk));
            }
            for update in tool_call_updates(&step.tool_calls) {
                updates.push(update);
            }
            updates.push(ChatUpdate::Usage(step.usage));
            updates.push(ChatUpdate::Finish(step.finish_reason));
            updates.push(ChatUpdate::Meta {
                model: Some(model),
                id: Some("resp-1".to_string()),
            });
            updates.finish()
        })
    }

    fn stream(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatStream, ChatError>> {
        let model = request.model.clone().unwrap_or_else(|| MODEL.to_string());
        let step = self.next(request, true);
        Box::pin(async move {
            if step.hang {
                std::future::pending::<()>().await;
            }
            if step.fail_after == 0
                && let Some(failure) = step.failure.clone()
            {
                return Err(failure);
            }
            let mut items: Vec<Result<ChatUpdate, ChatError>> = vec![Ok(ChatUpdate::Meta {
                model: Some(model),
                id: Some("resp-1".to_string()),
            })];
            for (i, chunk) in step.chunks.iter().enumerate() {
                if step.failure.is_some() && i == step.fail_after {
                    break;
                }
                items.push(Ok(ChatUpdate::Text(chunk.clone())));
            }
            match step.failure {
                Some(failure) => items.push(Err(failure)),
                None => {
                    items.extend(tool_call_updates(&step.tool_calls).into_iter().map(Ok));
                    items.push(Ok(ChatUpdate::Usage(step.usage)));
                    items.push(Ok(ChatUpdate::Finish(step.finish_reason)));
                }
            }
            let stream: ChatStream = Box::pin(futures_util::stream::iter(items));
            Ok(stream)
        })
    }
}

/// A scripted embedder (`design/45-Long-Term-Memory.md`, "Tests"), the
/// tests' embedding model in place of a provider (mirrors the .NET tests'
/// scripted `IEmbeddingGenerator`): each input gets its vector from
/// `vectors`; a call with an input in `fails` fails as a whole with that
/// error; an input in `unanswered` gets no vector. It reports
/// `input_tokens_per_input` per input and `output_tokens` (an embedding has
/// none, so the call must drop any), and model `model`. Every call's request
/// is kept.
#[derive(Default)]
pub struct ScriptedEmbedder {
    pub provider: String,
    pub model: Option<String>,
    pub vectors: HashMap<String, Vec<f32>>,
    pub fails: HashMap<String, ChatError>,
    pub unanswered: HashSet<String>,
    pub input_tokens_per_input: u64,
    pub output_tokens: Option<u64>,
    /// Never answers: only a cancellation ends the call.
    pub hang: bool,
    pub requests: Mutex<Vec<EmbeddingRequest>>,
}

impl ScriptedEmbedder {
    pub fn new(provider: &str, model: Option<&str>, vectors: &[(&str, &[f32])]) -> Self {
        Self {
            provider: provider.to_string(),
            model: model.map(str::to_string),
            vectors: vectors
                .iter()
                .map(|(text, vector)| (text.to_string(), vector.to_vec()))
                .collect(),
            input_tokens_per_input: 4,
            ..Self::default()
        }
    }

    pub fn failing(mut self, input: &str, error: ChatError) -> Self {
        self.fails.insert(input.to_string(), error);
        self
    }

    /// The inputs of every call, in order.
    pub fn calls(&self) -> Vec<Vec<String>> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|request| request.inputs.clone())
            .collect()
    }
}

impl Embedder for ScriptedEmbedder {
    fn provider(&self) -> &str {
        &self.provider
    }

    fn default_model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    fn embed(
        &self,
        request: EmbeddingRequest,
    ) -> BoxFuture<'_, Result<EmbeddingResponse, ChatError>> {
        self.requests.lock().unwrap().push(request.clone());
        Box::pin(async move {
            if self.hang {
                std::future::pending::<()>().await;
            }
            if let Some(error) = request
                .inputs
                .iter()
                .find_map(|input| self.fails.get(input))
            {
                return Err(error.clone());
            }
            let vectors = request
                .inputs
                .iter()
                .filter(|input| !self.unanswered.contains(*input))
                .map(|input| {
                    self.vectors
                        .get(input)
                        .unwrap_or_else(|| panic!("the script has no vector for '{input}'"))
                        .clone()
                })
                .collect();
            let mut response = EmbeddingResponse::new(vectors).with_usage(ModelUsage::new(
                Some(self.input_tokens_per_input * request.inputs.len() as u64),
                self.output_tokens,
            ));
            if let Some(model) = request.model.as_ref().or(self.model.as_ref()) {
                response = response.with_model(model.clone());
            }
            Ok(response)
        })
    }
}

/// Tool calls as a provider streams them: the id and name first, then the
/// arguments in two pieces.
fn tool_call_updates(calls: &[(String, String, String)]) -> Vec<ChatUpdate> {
    let mut updates = Vec::new();
    for (index, (id, name, arguments)) in calls.iter().enumerate() {
        let split = arguments
            .char_indices()
            .nth(arguments.chars().count() / 2)
            .map_or(arguments.len(), |(i, _)| i);
        updates.push(ChatUpdate::ToolCall {
            index,
            id: Some(id.clone()),
            name: Some(name.clone()),
            arguments: arguments[..split].to_string(),
        });
        updates.push(ChatUpdate::ToolCall {
            index,
            id: None,
            name: None,
            arguments: arguments[split..].to_string(),
        });
    }
    updates
}

/// A request as the raw server read it.
#[derive(Debug, Clone)]
pub struct Received {
    pub method: String,
    pub path: String,
    pub headers: HashMap<String, String>,
    pub body: Value,
}

impl Received {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(&name.to_lowercase()).map(String::as_str)
    }
}

/// What the raw server sends back: the status line's code, headers, and the
/// body in pieces (each written and flushed on its own, a short pause
/// between), then the connection closes. `content_length: false` sends no
/// length, so the body ends when the connection does.
#[derive(Clone)]
pub struct Answer {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub pieces: Vec<String>,
}

impl Answer {
    pub fn json(status: u16, body: Value) -> Self {
        Self {
            status,
            headers: vec![("content-type".into(), "application/json".into())],
            pieces: vec![body.to_string()],
        }
    }

    /// Server-sent events, one piece per event, exactly as written.
    pub fn events(pieces: &[&str]) -> Self {
        Self {
            status: 200,
            headers: vec![("content-type".into(), "text/event-stream".into())],
            pieces: pieces.iter().map(|p| p.to_string()).collect(),
        }
    }

    /// A fixture file under `tests/fixtures/`, split into events at blank
    /// lines, so each event arrives on its own.
    pub fn fixture(name: &str) -> Self {
        let text = std::fs::read_to_string(
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures")
                .join(name),
        )
        .unwrap()
        .replace("\r\n", "\n");
        let pieces: Vec<String> = text.split_inclusive("\n\n").map(str::to_string).collect();
        Self {
            status: 200,
            headers: vec![("content-type".into(), "text/event-stream".into())],
            pieces,
        }
    }

    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

pub struct RawServer {
    pub url: String,
    pub received: Arc<Mutex<Vec<Received>>>,
}

impl RawServer {
    /// Serves `answers` to successive connections, in order.
    pub async fn start(answers: Vec<Answer>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let received = Arc::new(Mutex::new(Vec::new()));
        let seen = received.clone();
        let answers = Arc::new(Mutex::new(VecDeque::from(answers)));
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let seen = seen.clone();
                let answers = answers.clone();
                tokio::spawn(async move {
                    let Some(request) = read_request(&mut socket).await else {
                        return;
                    };
                    seen.lock().unwrap().push(request);
                    let Some(answer) = answers.lock().unwrap().pop_front() else {
                        return;
                    };
                    let mut head = format!("HTTP/1.1 {} X\r\nconnection: close\r\n", answer.status);
                    for (name, value) in &answer.headers {
                        head.push_str(&format!("{name}: {value}\r\n"));
                    }
                    head.push_str("\r\n");
                    if socket.write_all(head.as_bytes()).await.is_err() {
                        return;
                    }
                    for piece in &answer.pieces {
                        if socket.write_all(piece.as_bytes()).await.is_err() {
                            return;
                        }
                        let _ = socket.flush().await;
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                    let _ = socket.shutdown().await;
                });
            }
        });
        Self { url, received }
    }

    pub fn received(&self) -> Vec<Received> {
        self.received.lock().unwrap().clone()
    }
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> Option<Received> {
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
    let mut request_line = lines.next()?.split_whitespace();
    let method = request_line.next()?.to_string();
    let path = request_line.next()?.to_string();
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
    Some(Received {
        method,
        path,
        headers,
        body: serde_json::from_slice(&body).unwrap_or(Value::Null),
    })
}
