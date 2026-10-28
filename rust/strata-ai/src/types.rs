//! The messages, requests and answers a [`ChatClient`](crate::ChatClient)
//! speaks (`design/44-LLM-Integration.md`), shaped after the same model as
//! .NET's `Microsoft.Extensions.AI`: a message is a role and a list of
//! contents; an answer is one assistant message, a finish reason and usage.

use datalium_strata::observability::ModelUsage;
use serde_json::{Map, Value};

/// Who a message is from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    System,
    User,
    Assistant,
    /// Tool results, answering an assistant message's tool calls.
    Tool,
}

/// One part of a message.
#[derive(Debug, Clone, PartialEq)]
pub enum Content {
    Text(String),
    /// The model asks for a tool to be called. `arguments` is a JSON object,
    /// or, when the model sent something that isn't one, its text as a
    /// `Value::String` (an agent refuses such a call back to the model).
    ToolCall {
        id: String,
        name: String,
        arguments: Value,
    },
    /// A tool's answer to the call with `call_id`.
    ToolResult {
        call_id: String,
        content: String,
    },
    /// The model's reasoning, when the provider returns it. `signature` is the
    /// provider's opaque proof of it (Anthropic's), which must go back with it
    /// to that provider; another provider is never sent it.
    Reasoning {
        text: String,
        signature: Option<String>,
    },
}

/// A message: a role and its contents.
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    pub role: Role,
    pub content: Vec<Content>,
}

impl Message {
    pub fn new(role: Role, content: Vec<Content>) -> Self {
        Self { role, content }
    }

    pub fn system(text: impl Into<String>) -> Self {
        Self::new(Role::System, vec![Content::Text(text.into())])
    }

    pub fn user(text: impl Into<String>) -> Self {
        Self::new(Role::User, vec![Content::Text(text.into())])
    }

    pub fn assistant(text: impl Into<String>) -> Self {
        Self::new(Role::Assistant, vec![Content::Text(text.into())])
    }

    /// One tool message carrying every result, in the order given.
    pub fn tool_results(results: impl IntoIterator<Item = (String, String)>) -> Self {
        Self::new(
            Role::Tool,
            results
                .into_iter()
                .map(|(call_id, content)| Content::ToolResult { call_id, content })
                .collect(),
        )
    }

    /// The text contents, concatenated.
    pub fn text(&self) -> String {
        self.content
            .iter()
            .filter_map(|content| match content {
                Content::Text(text) => Some(text.as_str()),
                _ => None,
            })
            .collect()
    }

    /// The tool calls, in order: `(id, name, arguments)`.
    pub fn tool_calls(&self) -> impl Iterator<Item = (&str, &str, &Value)> {
        self.content.iter().filter_map(|content| match content {
            Content::ToolCall {
                id,
                name,
                arguments,
            } => Some((id.as_str(), name.as_str(), arguments)),
            _ => None,
        })
    }
}

/// A tool the model may call: its name, description and JSON-Schema
/// parameters (a manifest entry, `design/20`).
#[derive(Debug, Clone, PartialEq)]
pub struct ToolDeclaration {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

impl ToolDeclaration {
    pub fn new(name: impl Into<String>, description: impl Into<String>, parameters: Value) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            parameters,
        }
    }
}

/// Whether the model may, must or mustn't call tools.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolChoice {
    Auto,
    None,
    /// At least one tool call.
    Required,
    /// This tool.
    Tool(String),
}

/// The shape the answer must take.
#[derive(Debug, Clone, PartialEq)]
pub enum ResponseFormat {
    Text,
    /// Any JSON object.
    Json,
    /// A JSON value matching `schema`. `strict` asks a provider that can to
    /// enforce it.
    JsonSchema {
        name: String,
        schema: Value,
        strict: bool,
    },
}

/// A request for one answer.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ChatRequest {
    pub messages: Vec<Message>,
    /// The model; the client's default when `None`.
    pub model: Option<String>,
    pub tools: Vec<ToolDeclaration>,
    pub tool_choice: Option<ToolChoice>,
    pub response_format: Option<ResponseFormat>,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f64>,
    /// Merged into the provider's request body as it is (a provider's own
    /// parameters: `top_p`, `seed`, `thinking`, …).
    pub extra: Map<String, Value>,
}

impl ChatRequest {
    pub fn new(messages: Vec<Message>) -> Self {
        Self {
            messages,
            model: None,
            tools: Vec::new(),
            tool_choice: None,
            response_format: None,
            max_tokens: None,
            temperature: None,
            extra: Map::new(),
        }
    }

    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    pub fn with_tools(mut self, tools: Vec<ToolDeclaration>) -> Self {
        self.tools = tools;
        self
    }

    pub fn with_tool_choice(mut self, choice: ToolChoice) -> Self {
        self.tool_choice = Some(choice);
        self
    }

    pub fn with_response_format(mut self, format: ResponseFormat) -> Self {
        self.response_format = Some(format);
        self
    }

    pub fn with_max_tokens(mut self, max_tokens: u32) -> Self {
        self.max_tokens = Some(max_tokens);
        self
    }

    pub fn with_temperature(mut self, temperature: f64) -> Self {
        self.temperature = Some(temperature);
        self
    }

    pub fn with_extra(mut self, key: impl Into<String>, value: Value) -> Self {
        self.extra.insert(key.into(), value);
        self
    }
}

/// One answer: an assistant message, why the model stopped (`stop`,
/// `length`, `tool_calls`, `content_filter`: OpenAI's words, which the
/// Anthropic client maps to), the model that answered, the provider's id for
/// it, and the tokens it used.
#[derive(Debug, Clone, PartialEq)]
pub struct ChatResponse {
    pub message: Message,
    pub finish_reason: Option<String>,
    pub model: Option<String>,
    pub id: Option<String>,
    pub usage: ModelUsage,
}

impl ChatResponse {
    /// The answer's text.
    pub fn text(&self) -> String {
        self.message.text()
    }

    /// The answer's tool calls, in order.
    pub fn tool_calls(&self) -> impl Iterator<Item = (&str, &str, &Value)> {
        self.message.tool_calls()
    }
}

/// One piece of a streamed answer. [`ChatUpdates`](crate::ChatUpdates)
/// assembles them into a [`ChatResponse`].
#[derive(Debug, Clone, PartialEq)]
pub enum ChatUpdate {
    /// More of the answer's text.
    Text(String),
    /// More of the model's reasoning, or its signature.
    Reasoning {
        text: String,
        signature: Option<String>,
    },
    /// More of the tool call at `index` (the call's position in the answer):
    /// its id and name when they first appear, then fragments of its JSON
    /// arguments, in order.
    ToolCall {
        index: usize,
        id: Option<String>,
        name: Option<String>,
        arguments: String,
    },
    /// Token counts; a later count replaces an earlier one, field by field.
    Usage(ModelUsage),
    /// Why the model stopped.
    Finish(String),
    /// The model that answered and the provider's id for the answer.
    Meta {
        model: Option<String>,
        id: Option<String>,
    },
}
