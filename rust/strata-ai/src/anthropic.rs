//! An Anthropic Messages client (`design/44-LLM-Integration.md`):
//! `POST /v1/messages`, streamed as server-sent events.

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use datalium_strata::BoxFuture;
use datalium_strata::observability::ModelUsage;
use serde_json::{Map, Value, json};

use crate::client::{ChatClient, ChatError, ChatStream};
use crate::http::{Decoded, Event, events, http_client, json, send};
use crate::types::{
    ChatRequest, ChatResponse, ChatUpdate, Content, Message, ResponseFormat, Role, ToolChoice,
};

/// The API version every request names.
pub const ANTHROPIC_VERSION: &str = "2023-06-01";

/// An [`AnthropicChatClient`]'s settings. The API key is never printed.
#[derive(Clone)]
#[non_exhaustive]
pub struct AnthropicOptions {
    /// The API's base URL, without `/v1` (`https://api.anthropic.com`).
    pub base_url: String,
    /// Sent as `x-api-key`.
    pub api_key: String,
    /// The model asked for when a request names none.
    pub model: Option<String>,
    /// The output-token limit when a request sets none (the API requires one).
    pub default_max_tokens: u32,
    pub connect_timeout: Duration,
    /// How long a response may go without sending anything.
    pub read_timeout: Duration,
    /// Sent with every request (`anthropic-beta`, …).
    pub headers: Vec<(String, String)>,
}

impl AnthropicOptions {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            base_url: "https://api.anthropic.com".to_string(),
            api_key: api_key.into(),
            model: None,
            default_max_tokens: 4096,
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(120),
            headers: Vec::new(),
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    pub fn with_default_max_tokens(mut self, max_tokens: u32) -> Self {
        self.default_max_tokens = max_tokens;
        self
    }

    pub fn with_timeouts(mut self, connect: Duration, read: Duration) -> Self {
        self.connect_timeout = connect;
        self.read_timeout = read;
        self
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

impl fmt::Debug for AnthropicOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AnthropicOptions")
            .field("base_url", &self.base_url)
            .field("api_key", &"<redacted>")
            .field("model", &self.model)
            .field("default_max_tokens", &self.default_max_tokens)
            .finish_non_exhaustive()
    }
}

/// A Messages client. Finish reasons are mapped to OpenAI's words (`stop`,
/// `length`, `tool_calls`, `content_filter`), so a caller reads one set.
/// Anthropic has no response-format parameter here: a JSON-schema format is
/// put in the system prompt.
#[derive(Debug, Clone)]
pub struct AnthropicChatClient {
    options: AnthropicOptions,
    http: reqwest::Client,
}

impl AnthropicChatClient {
    /// # Panics
    ///
    /// When the HTTP client can't be built (no TLS backend), which is a build
    /// problem, not a runtime one.
    pub fn new(options: AnthropicOptions) -> Self {
        let http = http_client(options.connect_timeout, options.read_timeout)
            .expect("the HTTP client builds");
        Self { options, http }
    }

    pub fn options(&self) -> &AnthropicOptions {
        &self.options
    }

    fn post(&self, body: &Value) -> reqwest::RequestBuilder {
        let url = format!(
            "{}/v1/messages",
            self.options.base_url.trim_end_matches('/')
        );
        let mut request = self
            .http
            .post(url)
            .header("x-api-key", &self.options.api_key)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .json(body);
        for (name, value) in &self.options.headers {
            request = request.header(name, value);
        }
        request
    }

    /// The request body this client sends for `request`.
    pub fn body(&self, request: &ChatRequest, stream: bool) -> Value {
        let mut body = Map::new();
        let model = request
            .model
            .clone()
            .or_else(|| self.options.model.clone())
            .unwrap_or_default();
        body.insert("model".into(), Value::String(model));
        body.insert(
            "max_tokens".into(),
            json!(
                request
                    .max_tokens
                    .unwrap_or(self.options.default_max_tokens)
            ),
        );
        let mut system: Vec<String> = request
            .messages
            .iter()
            .filter(|message| message.role == Role::System)
            .map(Message::text)
            .collect();
        if let Some(ResponseFormat::JsonSchema { schema, .. }) = &request.response_format {
            system.push(format!(
                "Answer with one JSON value that matches this JSON schema, and nothing else:\n{schema}"
            ));
        } else if let Some(ResponseFormat::Json) = &request.response_format {
            system.push("Answer with one JSON object, and nothing else.".to_string());
        }
        if !system.is_empty() {
            body.insert("system".into(), Value::String(system.join("\n\n")));
        }
        body.insert(
            "messages".into(),
            Value::Array(wire_messages(&request.messages)),
        );
        if !request.tools.is_empty() {
            body.insert(
                "tools".into(),
                request
                    .tools
                    .iter()
                    .map(|tool| {
                        json!({
                            "name": tool.name,
                            "description": tool.description,
                            "input_schema": tool.parameters,
                        })
                    })
                    .collect(),
            );
        }
        if let Some(choice) = &request.tool_choice {
            body.insert(
                "tool_choice".into(),
                match choice {
                    ToolChoice::Auto => json!({ "type": "auto" }),
                    ToolChoice::None => json!({ "type": "none" }),
                    ToolChoice::Required => json!({ "type": "any" }),
                    ToolChoice::Tool(name) => json!({ "type": "tool", "name": name }),
                },
            );
        }
        if let Some(temperature) = request.temperature {
            body.insert("temperature".into(), json!(temperature));
        }
        if stream {
            body.insert("stream".into(), json!(true));
        }
        for (key, value) in &request.extra {
            body.insert(key.clone(), value.clone());
        }
        Value::Object(body)
    }
}

impl ChatClient for AnthropicChatClient {
    fn provider(&self) -> &str {
        "anthropic"
    }

    fn default_model(&self) -> Option<&str> {
        self.options.model.as_deref()
    }

    fn complete(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatResponse, ChatError>> {
        Box::pin(async move {
            let body = self.body(&request, false);
            let response = send(self.post(&body)).await?;
            parse_response(&json(response).await?)
        })
    }

    fn stream(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatStream, ChatError>> {
        Box::pin(async move {
            let body = self.body(&request, true);
            let response = send(self.post(&body)).await?;
            let mut decoder = StreamDecoder::default();
            let stream: ChatStream = Box::pin(events(response, move |event| decoder.decode(event)));
            Ok(stream)
        })
    }
}

/// Strata's messages as Anthropic's: system messages go in `system`; a tool
/// message becomes `tool_result` blocks in a user message; consecutive
/// messages of one role are merged, as the API requires alternation.
fn wire_messages(messages: &[Message]) -> Vec<Value> {
    let mut wire: Vec<(&'static str, Vec<Value>)> = Vec::new();
    for message in messages {
        let role = match message.role {
            Role::System => continue,
            Role::User | Role::Tool => "user",
            Role::Assistant => "assistant",
        };
        let blocks: Vec<Value> = message
            .content
            .iter()
            .filter_map(|content| match content {
                Content::Text(text) if text.is_empty() => None,
                Content::Text(text) => Some(json!({ "type": "text", "text": text })),
                Content::ToolCall {
                    id,
                    name,
                    arguments,
                } => {
                    // Messages takes an object only; arguments the model sent
                    // that weren't one go back as none.
                    let input = if arguments.is_object() {
                        arguments.clone()
                    } else {
                        json!({})
                    };
                    Some(json!({ "type": "tool_use", "id": id, "name": name, "input": input }))
                }
                Content::ToolResult { call_id, content } => Some(json!({
                    "type": "tool_result",
                    "tool_use_id": call_id,
                    "content": content,
                })),
                // Thinking goes back only with its signature, which proves it is Anthropic's own.
                Content::Reasoning {
                    text,
                    signature: Some(signature),
                } => Some(json!({ "type": "thinking", "thinking": text, "signature": signature })),
                Content::Reasoning { .. } => None,
            })
            .collect();
        if blocks.is_empty() {
            continue;
        }
        match wire.last_mut() {
            Some((last, existing)) if *last == role => existing.extend(blocks),
            _ => wire.push((role, blocks)),
        }
    }
    wire.into_iter()
        .map(|(role, content)| json!({ "role": role, "content": content }))
        .collect()
}

/// Anthropic's stop reason in OpenAI's words.
fn finish_reason(stop_reason: &str) -> String {
    match stop_reason {
        "end_turn" | "stop_sequence" | "pause_turn" => "stop",
        "max_tokens" | "model_context_window_exceeded" => "length",
        "tool_use" => "tool_calls",
        "refusal" => "content_filter",
        other => other,
    }
    .to_string()
}

fn usage(value: &Value) -> ModelUsage {
    ModelUsage::new(
        value.get("input_tokens").and_then(Value::as_u64),
        value.get("output_tokens").and_then(Value::as_u64),
    )
}

fn string(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_string)
}

fn invalid(message: impl Into<String>) -> ChatError {
    ChatError::fatal("invalid_response", message)
}

/// A whole answer.
fn parse_response(value: &Value) -> Result<ChatResponse, ChatError> {
    let blocks = value
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("the answer has no content"))?;
    let mut content = Vec::new();
    for block in blocks {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => content.push(Content::Text(string(block, "text").unwrap_or_default())),
            Some("tool_use") => content.push(Content::ToolCall {
                id: string(block, "id").unwrap_or_default(),
                name: string(block, "name").unwrap_or_default(),
                arguments: block
                    .get("input")
                    .cloned()
                    .filter(Value::is_object)
                    .unwrap_or_else(|| Value::Object(Map::new())),
            }),
            Some("thinking") => content.push(Content::Reasoning {
                text: string(block, "thinking").unwrap_or_default(),
                signature: string(block, "signature"),
            }),
            _ => {}
        }
    }
    Ok(ChatResponse {
        message: Message::new(Role::Assistant, content),
        finish_reason: value
            .get("stop_reason")
            .and_then(Value::as_str)
            .map(finish_reason),
        model: string(value, "model"),
        id: string(value, "id"),
        usage: value.get("usage").map(usage).unwrap_or_default(),
    })
}

/// The streamed events as updates. Content blocks are numbered by the API;
/// tool calls are renumbered from 0 in the order they start.
#[derive(Default)]
struct StreamDecoder {
    /// A content block's index → its tool call's index, for `tool_use` blocks.
    tool_calls: HashMap<u64, usize>,
}

impl StreamDecoder {
    fn decode(&mut self, event: Event) -> Result<Decoded<ChatUpdate>, ChatError> {
        let data: Value = serde_json::from_str(&event.data)
            .map_err(|error| invalid(format!("a streamed event isn't JSON: {error}")))?;
        let kind = event
            .event
            .clone()
            .or_else(|| string(&data, "type"))
            .unwrap_or_default();
        let mut updates = Vec::new();
        match kind.as_str() {
            "message_start" => {
                let message = data.get("message").cloned().unwrap_or(Value::Null);
                updates.push(ChatUpdate::Meta {
                    model: string(&message, "model"),
                    id: string(&message, "id"),
                });
                if let Some(usage) = message.get("usage") {
                    updates.push(ChatUpdate::Usage(self::usage(usage)));
                }
            }
            "content_block_start" => {
                let block = data.get("content_block").cloned().unwrap_or(Value::Null);
                if block.get("type").and_then(Value::as_str) == Some("tool_use") {
                    let position = data.get("index").and_then(Value::as_u64).unwrap_or(0);
                    let index = self.tool_calls.len();
                    self.tool_calls.insert(position, index);
                    updates.push(ChatUpdate::ToolCall {
                        index,
                        id: string(&block, "id"),
                        name: string(&block, "name"),
                        arguments: String::new(),
                    });
                }
            }
            "content_block_delta" => {
                let delta = data.get("delta").cloned().unwrap_or(Value::Null);
                match delta.get("type").and_then(Value::as_str) {
                    Some("text_delta") => {
                        updates.push(ChatUpdate::Text(string(&delta, "text").unwrap_or_default()));
                    }
                    Some("input_json_delta") => {
                        let position = data.get("index").and_then(Value::as_u64).unwrap_or(0);
                        let index = *self.tool_calls.get(&position).ok_or_else(|| {
                            invalid("a tool call's arguments came before the call")
                        })?;
                        updates.push(ChatUpdate::ToolCall {
                            index,
                            id: None,
                            name: None,
                            arguments: string(&delta, "partial_json").unwrap_or_default(),
                        });
                    }
                    Some("thinking_delta") => updates.push(ChatUpdate::Reasoning {
                        text: string(&delta, "thinking").unwrap_or_default(),
                        signature: None,
                    }),
                    Some("signature_delta") => updates.push(ChatUpdate::Reasoning {
                        text: String::new(),
                        signature: string(&delta, "signature"),
                    }),
                    _ => {}
                }
            }
            "message_delta" => {
                if let Some(reason) = data.pointer("/delta/stop_reason").and_then(Value::as_str) {
                    updates.push(ChatUpdate::Finish(finish_reason(reason)));
                }
                if let Some(usage) = data.get("usage") {
                    updates.push(ChatUpdate::Usage(self::usage(usage)));
                }
            }
            "message_stop" => return Ok(Decoded::Done(updates)),
            "error" => {
                let kind = data
                    .pointer("/error/type")
                    .and_then(Value::as_str)
                    .unwrap_or("error");
                let message = data
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("the provider failed mid-stream");
                return Err(match kind {
                    "overloaded_error" => ChatError::status(529, None, message),
                    "rate_limit_error" => ChatError::status(429, None, message),
                    "api_error" => ChatError::status(500, None, message),
                    "invalid_request_error" => ChatError::status(400, None, message),
                    _ => ChatError::transient("stream_error", message),
                });
            }
            _ => {}
        }
        Ok(Decoded::Items(updates))
    }
}
