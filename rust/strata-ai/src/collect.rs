//! Assembling a streamed answer (`design/44-LLM-Integration.md`).

use datalium_strata::observability::ModelUsage;
use serde_json::Value;

use crate::client::ChatError;
use crate::types::{ChatResponse, ChatUpdate, Content, Message, Role};

#[derive(Default)]
struct ToolCallParts {
    id: String,
    name: String,
    arguments: String,
}

/// Assembles [`ChatUpdate`]s into a [`ChatResponse`], as .NET's
/// `ToChatResponse` does: text concatenated, reasoning kept before it, tool
/// calls by index with their argument fragments joined and parsed, usage
/// field by field (a later count replaces an earlier one), the last finish
/// reason, model and id.
#[derive(Default)]
pub struct ChatUpdates {
    text: String,
    reasoning: String,
    signature: Option<String>,
    tool_calls: Vec<ToolCallParts>,
    usage: ModelUsage,
    finish_reason: Option<String>,
    model: Option<String>,
    id: Option<String>,
}

impl ChatUpdates {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, update: ChatUpdate) {
        match update {
            ChatUpdate::Text(text) => self.text.push_str(&text),
            ChatUpdate::Reasoning { text, signature } => {
                self.reasoning.push_str(&text);
                if signature.is_some() {
                    self.signature = signature;
                }
            }
            ChatUpdate::ToolCall {
                index,
                id,
                name,
                arguments,
            } => {
                if self.tool_calls.len() <= index {
                    self.tool_calls
                        .resize_with(index + 1, ToolCallParts::default);
                }
                let call = &mut self.tool_calls[index];
                if let Some(id) = id {
                    call.id = id;
                }
                if let Some(name) = name {
                    call.name.push_str(&name);
                }
                call.arguments.push_str(&arguments);
            }
            ChatUpdate::Usage(usage) => {
                if usage.input_tokens.is_some() {
                    self.usage.input_tokens = usage.input_tokens;
                }
                if usage.output_tokens.is_some() {
                    self.usage.output_tokens = usage.output_tokens;
                }
            }
            ChatUpdate::Finish(reason) => self.finish_reason = Some(reason),
            ChatUpdate::Meta { model, id } => {
                if model.is_some() {
                    self.model = model;
                }
                if id.is_some() {
                    self.id = id;
                }
            }
        }
    }

    /// The whole answer. Empty tool-call arguments are `{}`; arguments that
    /// aren't a JSON object are kept as the text the model sent
    /// ([`Content::ToolCall`]).
    pub fn finish(self) -> Result<ChatResponse, ChatError> {
        let mut content = Vec::new();
        if !self.reasoning.is_empty() || self.signature.is_some() {
            content.push(Content::Reasoning {
                text: self.reasoning,
                signature: self.signature,
            });
        }
        if !self.text.is_empty() {
            content.push(Content::Text(self.text));
        }
        for call in self.tool_calls {
            if call.name.is_empty() {
                continue;
            }
            content.push(Content::ToolCall {
                arguments: tool_arguments(&call.arguments),
                id: call.id,
                name: call.name,
            });
        }
        Ok(ChatResponse {
            message: Message::new(Role::Assistant, content),
            finish_reason: self.finish_reason,
            model: self.model,
            id: self.id,
            usage: self.usage,
        })
    }
}

/// A tool call's arguments as the provider sent them: a JSON object, `{}`
/// when empty, or the text itself (a `Value::String`) when it isn't a JSON
/// object, so an agent can tell the model (`design/44`, "A refused or failed
/// tool call") rather than the whole answer being lost.
pub(crate) fn tool_arguments(text: &str) -> Value {
    if text.trim().is_empty() {
        return Value::Object(Default::default());
    }
    serde_json::from_str::<Value>(text)
        .ok()
        .filter(Value::is_object)
        .unwrap_or_else(|| Value::String(text.to_string()))
}
