//! An OpenAI-compatible Chat Completions client (`design/44-LLM-Integration.md`):
//! OpenAI itself, Azure OpenAI's compatible endpoint, Ollama, vLLM, and any
//! other server that speaks `POST /chat/completions`.

use std::fmt;
use std::time::Duration;

use datalium_strata::BoxFuture;
use datalium_strata::observability::ModelUsage;
use serde_json::{Map, Value, json};

use crate::client::{ChatClient, ChatError, ChatStream};
use crate::collect::tool_arguments;
use crate::http::{Decoded, Event, events, http_client, json, send};
use crate::types::{
    ChatRequest, ChatResponse, ChatUpdate, Content, Message, ResponseFormat, Role, ToolChoice,
};

/// Which body field carries a request's output-token limit. OpenAI's current
/// models take `max_completion_tokens` and refuse `max_tokens`; many
/// compatible servers know only `max_tokens`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaxTokensField {
    MaxCompletionTokens,
    MaxTokens,
}

/// An [`OpenAiChatClient`]'s (or an [`OpenAiEmbedder`](crate::OpenAiEmbedder)'s)
/// settings. The API key is never printed.
#[derive(Clone)]
#[non_exhaustive]
pub struct OpenAiOptions {
    /// The API's base URL, up to and including `/v1`
    /// (`https://api.openai.com/v1`).
    pub base_url: String,
    /// Sent as `Authorization: Bearer <key>`; none for a local server.
    pub api_key: Option<String>,
    /// The model asked for when a request names none.
    pub model: Option<String>,
    pub connect_timeout: Duration,
    /// How long a response may go without sending anything.
    pub read_timeout: Duration,
    pub max_tokens_field: MaxTokensField,
    /// Sent with every request (`OpenAI-Organization`, an `api-key` for Azure, …).
    pub headers: Vec<(String, String)>,
    /// `gen_ai.provider.name`: `openai` unless set.
    pub provider: String,
}

impl OpenAiOptions {
    /// OpenAI's API with `api_key`.
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            base_url: "https://api.openai.com/v1".to_string(),
            api_key: Some(api_key.into()),
            model: None,
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(120),
            max_tokens_field: MaxTokensField::MaxCompletionTokens,
            headers: Vec::new(),
            provider: "openai".to_string(),
        }
    }

    /// Another OpenAI-compatible server at `base_url` (up to `/v1`), with no
    /// key and the widely understood `max_tokens`.
    pub fn compatible(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            api_key: None,
            max_tokens_field: MaxTokensField::MaxTokens,
            ..Self::new("")
        }
    }

    /// Ollama at `url` (`http://localhost:11434`), provider `ollama`.
    pub fn ollama(url: impl Into<String>) -> Self {
        let url = url.into();
        Self {
            provider: "ollama".to_string(),
            ..Self::compatible(format!("{}/v1", url.trim_end_matches('/')))
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    pub fn with_timeouts(mut self, connect: Duration, read: Duration) -> Self {
        self.connect_timeout = connect;
        self.read_timeout = read;
        self
    }

    pub fn with_max_tokens_field(mut self, field: MaxTokensField) -> Self {
        self.max_tokens_field = field;
        self
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn with_provider(mut self, provider: impl Into<String>) -> Self {
        self.provider = provider.into();
        self
    }
}

impl fmt::Debug for OpenAiOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiOptions")
            .field("base_url", &self.base_url)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field("model", &self.model)
            .field("max_tokens_field", &self.max_tokens_field)
            .field("provider", &self.provider)
            .finish_non_exhaustive()
    }
}

/// A Chat Completions client. Streams with `stream_options.include_usage`, so
/// a streamed answer reports its usage too.
#[derive(Debug, Clone)]
pub struct OpenAiChatClient {
    options: OpenAiOptions,
    http: reqwest::Client,
}

impl OpenAiChatClient {
    /// # Panics
    ///
    /// When the HTTP client can't be built (no TLS backend), which is a build
    /// problem, not a runtime one.
    pub fn new(options: OpenAiOptions) -> Self {
        let http = http_client(options.connect_timeout, options.read_timeout)
            .expect("the HTTP client builds");
        Self { options, http }
    }

    pub fn options(&self) -> &OpenAiOptions {
        &self.options
    }

    fn post(&self, body: &Value) -> reqwest::RequestBuilder {
        post(&self.http, &self.options, "chat/completions", body)
    }

    /// The request body this client sends for `request` (public for tests and
    /// for anyone checking what goes over the wire).
    pub fn body(&self, request: &ChatRequest, stream: bool) -> Value {
        let mut body = Map::new();
        let model = request
            .model
            .clone()
            .or_else(|| self.options.model.clone())
            .unwrap_or_default();
        body.insert("model".into(), Value::String(model));
        body.insert(
            "messages".into(),
            Value::Array(request.messages.iter().flat_map(wire_messages).collect()),
        );
        if !request.tools.is_empty() {
            body.insert(
                "tools".into(),
                request
                    .tools
                    .iter()
                    .map(|tool| {
                        json!({
                            "type": "function",
                            "function": {
                                "name": tool.name,
                                "description": tool.description,
                                "parameters": tool.parameters,
                            },
                        })
                    })
                    .collect(),
            );
        }
        if let Some(choice) = &request.tool_choice {
            body.insert(
                "tool_choice".into(),
                match choice {
                    ToolChoice::Auto => json!("auto"),
                    ToolChoice::None => json!("none"),
                    ToolChoice::Required => json!("required"),
                    ToolChoice::Tool(name) => {
                        json!({ "type": "function", "function": { "name": name } })
                    }
                },
            );
        }
        match &request.response_format {
            None | Some(ResponseFormat::Text) => {}
            Some(ResponseFormat::Json) => {
                body.insert("response_format".into(), json!({ "type": "json_object" }));
            }
            Some(ResponseFormat::JsonSchema {
                name,
                schema,
                strict,
            }) => {
                body.insert(
                    "response_format".into(),
                    json!({
                        "type": "json_schema",
                        "json_schema": { "name": name, "schema": schema, "strict": strict },
                    }),
                );
            }
        }
        if let Some(max_tokens) = request.max_tokens {
            let field = match self.options.max_tokens_field {
                MaxTokensField::MaxCompletionTokens => "max_completion_tokens",
                MaxTokensField::MaxTokens => "max_tokens",
            };
            body.insert(field.into(), json!(max_tokens));
        }
        if let Some(temperature) = request.temperature {
            body.insert("temperature".into(), json!(temperature));
        }
        if stream {
            body.insert("stream".into(), json!(true));
            body.insert("stream_options".into(), json!({ "include_usage": true }));
        }
        for (key, value) in &request.extra {
            body.insert(key.clone(), value.clone());
        }
        Value::Object(body)
    }
}

impl ChatClient for OpenAiChatClient {
    fn provider(&self) -> &str {
        &self.options.provider
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
            let stream: ChatStream = Box::pin(events(response, decode_event));
            Ok(stream)
        })
    }
}

/// A `POST {base_url}/{path}` of `body` with the options' key (as a bearer
/// token, when there is one) and headers: what the chat client and
/// [`OpenAiEmbedder`](crate::OpenAiEmbedder) send.
pub(crate) fn post(
    http: &reqwest::Client,
    options: &OpenAiOptions,
    path: &str,
    body: &Value,
) -> reqwest::RequestBuilder {
    let url = format!("{}/{path}", options.base_url.trim_end_matches('/'));
    let mut request = http.post(url).json(body);
    if let Some(key) = options.api_key.as_deref().filter(|key| !key.is_empty()) {
        request = request.bearer_auth(key);
    }
    for (name, value) in &options.headers {
        request = request.header(name, value);
    }
    request
}

/// One Strata message as Chat Completions messages: a tool message becomes
/// one `tool` message per result; reasoning isn't sent back.
fn wire_messages(message: &Message) -> Vec<Value> {
    match message.role {
        Role::Tool => message
            .content
            .iter()
            .filter_map(|content| match content {
                Content::ToolResult { call_id, content } => Some(json!({
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": content,
                })),
                _ => None,
            })
            .collect(),
        Role::Assistant => {
            let text = message.text();
            let calls: Vec<Value> = message
                .tool_calls()
                .map(|(id, name, arguments)| {
                    json!({
                        "id": id,
                        "type": "function",
                        "function": { "name": name, "arguments": wire_arguments(arguments) },
                    })
                })
                .collect();
            let mut wire = Map::new();
            wire.insert("role".into(), json!("assistant"));
            wire.insert(
                "content".into(),
                if text.is_empty() && !calls.is_empty() {
                    Value::Null
                } else {
                    Value::String(text)
                },
            );
            if !calls.is_empty() {
                wire.insert("tool_calls".into(), Value::Array(calls));
            }
            vec![Value::Object(wire)]
        }
        Role::System | Role::User => vec![json!({
            "role": if message.role == Role::System { "system" } else { "user" },
            "content": message.text(),
        })],
    }
}

fn usage(value: &Value) -> Option<ModelUsage> {
    let usage = value.get("usage").filter(|usage| usage.is_object())?;
    Some(ModelUsage::new(
        usage.get("prompt_tokens").and_then(Value::as_u64),
        usage.get("completion_tokens").and_then(Value::as_u64),
    ))
}

/// A tool call's arguments as Chat Completions carries them: JSON text, the
/// model's own text when it wasn't an object.
fn wire_arguments(arguments: &Value) -> String {
    match arguments {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

fn string(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_string)
}

fn invalid(message: impl Into<String>) -> ChatError {
    ChatError::fatal("invalid_response", message)
}

/// A whole answer.
fn parse_response(value: &Value) -> Result<ChatResponse, ChatError> {
    let choice = value
        .pointer("/choices/0")
        .ok_or_else(|| invalid("the answer has no choices"))?;
    let message = choice
        .get("message")
        .ok_or_else(|| invalid("the answer's choice has no message"))?;
    let mut content = Vec::new();
    if let Some(reasoning) = message.get("reasoning_content").and_then(Value::as_str)
        && !reasoning.is_empty()
    {
        content.push(Content::Reasoning {
            text: reasoning.to_string(),
            signature: None,
        });
    }
    if let Some(text) = message.get("content").and_then(Value::as_str)
        && !text.is_empty()
    {
        content.push(Content::Text(text.to_string()));
    }
    for call in message
        .get("tool_calls")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let name = call
            .pointer("/function/name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let arguments = call
            .pointer("/function/arguments")
            .and_then(Value::as_str)
            .unwrap_or_default();
        content.push(Content::ToolCall {
            arguments: tool_arguments(arguments),
            id: string(call, "id").unwrap_or_default(),
            name,
        });
    }
    Ok(ChatResponse {
        message: Message::new(Role::Assistant, content),
        finish_reason: string(choice, "finish_reason"),
        model: string(value, "model"),
        id: string(value, "id"),
        usage: usage(value).unwrap_or_default(),
    })
}

/// One streamed chunk as updates; `[DONE]` ends the answer.
fn decode_event(event: Event) -> Result<Decoded<ChatUpdate>, ChatError> {
    if event.data.trim() == "[DONE]" {
        return Ok(Decoded::Done(Vec::new()));
    }
    let chunk: Value = serde_json::from_str(&event.data)
        .map_err(|error| invalid(format!("a streamed chunk isn't JSON: {error}")))?;
    if let Some(error) = chunk.get("error") {
        // A server that fails mid-stream says so in a chunk of its own.
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("the provider failed mid-stream");
        return Err(ChatError::transient("stream_error", message));
    }
    let mut updates = Vec::new();
    let model = string(&chunk, "model");
    let id = string(&chunk, "id");
    if model.is_some() || id.is_some() {
        updates.push(ChatUpdate::Meta { model, id });
    }
    if let Some(choice) = chunk.pointer("/choices/0") {
        let delta = choice.get("delta").cloned().unwrap_or(Value::Null);
        if let Some(reasoning) = delta.get("reasoning_content").and_then(Value::as_str)
            && !reasoning.is_empty()
        {
            updates.push(ChatUpdate::Reasoning {
                text: reasoning.to_string(),
                signature: None,
            });
        }
        if let Some(text) = delta.get("content").and_then(Value::as_str)
            && !text.is_empty()
        {
            updates.push(ChatUpdate::Text(text.to_string()));
        }
        for (position, call) in delta
            .get("tool_calls")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
        {
            updates.push(ChatUpdate::ToolCall {
                index: call
                    .get("index")
                    .and_then(Value::as_u64)
                    .map_or(position, |index| index as usize),
                id: string(call, "id"),
                name: call
                    .pointer("/function/name")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                arguments: call
                    .pointer("/function/arguments")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            });
        }
        if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
            updates.push(ChatUpdate::Finish(reason.to_string()));
        }
    }
    if let Some(usage) = usage(&chunk) {
        updates.push(ChatUpdate::Usage(usage));
    }
    Ok(Decoded::Items(updates))
}
