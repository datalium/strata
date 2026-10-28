//! The agent (`design/44-LLM-Integration.md`, "The agent's loop"): turns of
//! a model call, then the tool calls it asks for through a [`ToolRegistry`],
//! until the model answers without tools; the answer parsed into a type,
//! validated, and repaired.

use std::any::TypeId;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use datalium_strata::context::NodeContext;
use datalium_strata::graph::NodeError;
use datalium_strata::observability::{ModelUsage, ToolCallResult};
use datalium_strata::tools::{StrataSchema, ToolCallContext, ToolRegistry};
use datalium_strata::{BoxFuture, StrataError};
use futures_util::{StreamExt, TryStreamExt};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::call::{CallOptions, complete_with};
use crate::client::ChatClient;
use crate::declarations::{ToolDeclarations, provider_name};
use crate::types::{ChatRequest, Message, ResponseFormat};

/// What a tool failure that isn't a refusal does (`design/44`, "A refused or
/// failed tool call").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ToolFailures {
    /// The node fails: transient or fatal as the agent's fault classifier
    /// says ([`AgentOptions::with_fault_classifier`]), fatal when it says
    /// nothing.
    #[default]
    FailNode,
    /// The model is told, as `{"error": "…"}`, and the loop goes on.
    ReturnToModel,
}

/// Checks a parsed answer: the problems with it, none when it is valid.
pub type Validate<O> = Arc<dyn Fn(&O) -> Vec<String> + Send + Sync>;

/// Reads a tool's failure: the node failure it is, or `None` for the
/// default (fatal).
pub type ToolFaultClassifier = Arc<dyn Fn(&StrataError) -> Option<NodeError> + Send + Sync>;

/// Changes one turn's request before its model call
/// ([`AgentOptions::with_configure_turn`]).
pub type ConfigureTurn = Arc<dyn Fn(&AgentTurn, &mut ChatRequest) + Send + Sync>;

/// A turn of an agent's run, as [`AgentOptions::with_configure_turn`] sees
/// it before the model call; the messages it sends are the request's.
/// Mirrors .NET's `AgentTurn`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct AgentTurn {
    /// The turn's number, from 1; repair turns count.
    pub number: u32,
    /// How many turns before this one answered with tool calls.
    pub tool_turns: u32,
    /// How many repair requests have been sent, this turn's included.
    pub repairs: u32,
    /// Whether this turn answers a repair request.
    pub is_repair: bool,
}

/// An agent's options. Mirrors .NET's `StrataAgentOptions<TOutput>`.
#[non_exhaustive]
pub struct AgentOptions<O> {
    /// The agent's id: the tool calls' caller id, the turns' `strata.agent`,
    /// and the name its failures give.
    pub id: String,
    /// Every turn's request, copied (model, `max_tokens`, `temperature`,
    /// `tool_choice`, `extra`); its messages, tools and response format are
    /// the agent's.
    pub request: ChatRequest,
    /// The most turns with tool calls; one more fails the node transiently.
    /// Repair turns don't count. 8.
    pub max_turns: u32,
    /// How many times an answer that doesn't parse or validate is sent back
    /// to be repaired, over the whole run; then the node fails transiently. 1.
    pub max_repairs: u32,
    /// How many tool calls of one turn run at once: 1 (the default) runs them
    /// one after another, in the model's order.
    pub tool_concurrency: usize,
    pub tool_failures: ToolFailures,
    pub validate: Option<Validate<O>>,
    pub fault_classifier: Option<ToolFaultClassifier>,
    pub configure_turn: Option<ConfigureTurn>,
}

impl<O> AgentOptions<O> {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            request: ChatRequest::new(Vec::new()),
            max_turns: 8,
            max_repairs: 1,
            tool_concurrency: 1,
            tool_failures: ToolFailures::FailNode,
            validate: None,
            fault_classifier: None,
            configure_turn: None,
        }
    }

    /// Every turn's request template (its messages are ignored).
    pub fn with_request(mut self, request: ChatRequest) -> Self {
        self.request = request;
        self
    }

    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.request.model = Some(model.into());
        self
    }

    pub fn with_max_tokens(mut self, max_tokens: u32) -> Self {
        self.request.max_tokens = Some(max_tokens);
        self
    }

    pub fn with_temperature(mut self, temperature: f64) -> Self {
        self.request.temperature = Some(temperature);
        self
    }

    pub fn with_max_turns(mut self, max_turns: u32) -> Self {
        self.max_turns = max_turns;
        self
    }

    pub fn with_max_repairs(mut self, max_repairs: u32) -> Self {
        self.max_repairs = max_repairs;
        self
    }

    /// Runs up to `concurrency` tool calls of a turn at once; their results
    /// still go back in the model's order. Panics when it is 0.
    pub fn with_tool_concurrency(mut self, concurrency: usize) -> Self {
        assert!(concurrency > 0, "tool concurrency must be at least 1");
        self.tool_concurrency = concurrency;
        self
    }

    pub fn with_tool_failures(mut self, failures: ToolFailures) -> Self {
        self.tool_failures = failures;
        self
    }

    /// Checks every parsed answer; problems are sent back to be repaired.
    pub fn with_validate(
        mut self,
        validate: impl Fn(&O) -> Vec<String> + Send + Sync + 'static,
    ) -> Self {
        self.validate = Some(Arc::new(validate));
        self
    }

    /// Reads a tool's failure (`ToolFailures::FailNode`): a Rust tool's
    /// error carries no kind, so this is where one that is worth retrying
    /// (a timeout, an unreachable backend) says so.
    pub fn with_fault_classifier(
        mut self,
        classifier: impl Fn(&StrataError) -> Option<NodeError> + Send + Sync + 'static,
    ) -> Self {
        self.fault_classifier = Some(Arc::new(classifier));
        self
    }

    /// Changes one turn's request before its model call: the turn's own copy
    /// of [`Self::request`], with the conversation, the agent's tools and its
    /// response format already set. For example, require a tool call on the
    /// first turn only, which the request template can't do, since it applies
    /// to every turn and a turn that must call a tool never answers:
    ///
    /// ```ignore
    /// .with_configure_turn(|turn, request| {
    ///     if turn.number == 1 {
    ///         request.tool_choice = Some(ToolChoice::Required);
    ///     }
    /// })
    /// ```
    ///
    /// Replacing the tools or the response format is the caller's to get
    /// right: the agent still maps the model's tool names through its
    /// declarations and reads the answer as `O`.
    pub fn with_configure_turn(
        mut self,
        configure: impl Fn(&AgentTurn, &mut ChatRequest) + Send + Sync + 'static,
    ) -> Self {
        self.configure_turn = Some(Arc::new(configure));
        self
    }
}

impl<O> Clone for AgentOptions<O> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            request: self.request.clone(),
            max_turns: self.max_turns,
            max_repairs: self.max_repairs,
            tool_concurrency: self.tool_concurrency,
            tool_failures: self.tool_failures,
            validate: self.validate.clone(),
            fault_classifier: self.fault_classifier.clone(),
            configure_turn: self.configure_turn.clone(),
        }
    }
}

impl<O> fmt::Debug for AgentOptions<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentOptions")
            .field("id", &self.id)
            .field("max_turns", &self.max_turns)
            .field("max_repairs", &self.max_repairs)
            .field("tool_concurrency", &self.tool_concurrency)
            .field("tool_failures", &self.tool_failures)
            .finish_non_exhaustive()
    }
}

/// One tool call the agent made, as the model saw it answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentToolCall {
    /// The Strata name (the model's name when no declaration had it).
    pub name: String,
    /// The model's id for the call.
    pub call_id: String,
    /// `Succeeded`; `Refused` (not found, not permitted, denied, arguments
    /// that don't parse); `Failed` (a failure sent back to the model under
    /// [`ToolFailures::ReturnToModel`]).
    pub result: ToolCallResult,
}

/// What an agent's run produced. Mirrors .NET's `AgentResult<TOutput>`.
#[derive(Debug, Clone)]
pub struct AgentResult<O> {
    /// The parsed, validated answer (a text agent's: the answer's text).
    pub output: O,
    /// The whole conversation: the messages given, then every turn's answer,
    /// tool results and repair request.
    pub messages: Vec<Message>,
    /// Summed over the turns; a count no turn reported stays `None`.
    pub usage: ModelUsage,
    /// Model calls made, repair turns included.
    pub turns: u32,
    /// Every tool call, in the order the model asked for them.
    pub tool_calls: Vec<AgentToolCall>,
}

/// An agent (`design/44-LLM-Integration.md`): a value a node runs, not a
/// node kind. Mirrors .NET's `StrataAgent<TOutput>`.
///
/// Each turn is one model call through [`complete_with`] (so it streams when
/// the run does, is observed, and fails as any call fails) with the
/// registry's tools under provider-safe names ([`ToolDeclarations`]). The
/// tool calls in an answer are mapped back to their Strata names and
/// dispatched through the registry, its middleware included, with the node's
/// context, the agent's id as the caller id and the model's id as the call
/// id; their results go back in one tool message, in the model's order. An
/// answer without tool calls is the answer: parsed into `O` (a surrounding
/// Markdown fence removed), validated, and sent back to be repaired when it
/// is bad.
///
/// `O` is the answer's type: `#[derive(Deserialize, StrataSchema)]`, whose
/// schema is the response format; `Agent<String>` is a text agent, whose
/// answer is its text.
///
/// The conversation lives for one run of the agent (one attempt of its
/// node): a retry starts over.
pub struct Agent<O> {
    client: Arc<dyn ChatClient>,
    tools: Arc<ToolRegistry>,
    declarations: ToolDeclarations,
    options: AgentOptions<O>,
    format: Option<ResponseFormat>,
    _output: PhantomData<fn() -> O>,
}

impl<O> Agent<O>
where
    O: DeserializeOwned + StrataSchema + Send + 'static,
{
    pub fn new(
        client: Arc<dyn ChatClient>,
        tools: Arc<ToolRegistry>,
        options: AgentOptions<O>,
    ) -> Self {
        assert!(
            options.tool_concurrency > 0,
            "tool concurrency must be at least 1"
        );
        let declarations = ToolDeclarations::for_registry(&tools);
        let format = (!is_text::<O>()).then(|| {
            let (schema, strict) = strict_schema(O::json_schema());
            ResponseFormat::JsonSchema {
                name: provider_name(short_type_name::<O>()),
                schema,
                strict,
            }
        });
        Self {
            client,
            tools,
            declarations,
            options,
            format,
            _output: PhantomData,
        }
    }

    pub fn id(&self) -> &str {
        &self.options.id
    }

    pub fn options(&self) -> &AgentOptions<O> {
        &self.options
    }

    /// The tools as the model sees them.
    pub fn declarations(&self) -> &ToolDeclarations {
        &self.declarations
    }

    /// The response format every turn asks for: `None` for a text agent.
    pub fn response_format(&self) -> Option<&ResponseFormat> {
        self.format.as_ref()
    }

    /// Runs the agent on `messages` from inside the node `ctx` is: see
    /// [`Agent`].
    ///
    /// Fails the node with a [`NodeError`]: a model call's fault as
    /// [`complete_with`] says; transient after more than `max_turns` turns
    /// with tool calls (`agent 'x' made 8 tool turns without answering`) or
    /// an answer still bad after `max_repairs` repairs (`agent 'x' gave no
    /// valid answer: …`); a tool's failure under [`ToolFailures::FailNode`].
    /// When the node's token fires, the engine drops this future.
    pub async fn run(
        &self,
        messages: Vec<Message>,
        ctx: &NodeContext,
    ) -> Result<AgentResult<O>, NodeError> {
        let id = &self.options.id;
        let call_options = CallOptions::new().with_agent(id.clone());
        let mut conversation = messages;
        let mut usage = ModelUsage::default();
        let (mut turns, mut tool_turns, mut repairs) = (0u32, 0u32, 0u32);
        let mut repairing = false;
        let mut tool_calls = Vec::new();
        loop {
            let mut request = self.options.request.clone();
            request.messages = conversation.clone();
            request.tools = self.declarations.declarations().to_vec();
            request.response_format = self.format.clone();
            if let Some(configure) = &self.options.configure_turn {
                let turn = AgentTurn {
                    number: turns + 1,
                    tool_turns,
                    repairs,
                    is_repair: repairing,
                };
                configure(&turn, &mut request);
            }
            let response = complete_with(&*self.client, request, Some(ctx), &call_options).await?;
            turns += 1;
            usage = add(usage, response.usage);
            let calls: Vec<(String, String, Value)> = response
                .tool_calls()
                .map(|(call_id, name, arguments)| {
                    (call_id.to_string(), name.to_string(), arguments.clone())
                })
                .collect();
            let text = response.text();
            conversation.push(response.message);

            if !calls.is_empty() {
                tool_turns += 1;
                if tool_turns > self.options.max_turns {
                    return Err(NodeError::Transient(format!(
                        "agent '{id}' made {} tool turns without answering",
                        self.options.max_turns
                    )));
                }
                // In the model's order, up to `tool_concurrency` at once; the
                // first failure in that order ends the turn, dropping the rest.
                let pending: Vec<BoxFuture<'_, Result<Answered, NodeError>>> = calls
                    .iter()
                    .map(|(call_id, name, arguments)| {
                        Box::pin(self.call_tool(call_id, name, arguments, ctx)) as BoxFuture<'_, _>
                    })
                    .collect();
                let answered: Vec<Answered> = futures_util::stream::iter(pending)
                    .buffered(self.options.tool_concurrency)
                    .try_collect()
                    .await?;
                conversation.push(Message::tool_results(
                    answered
                        .iter()
                        .map(|a| (a.record.call_id.clone(), a.content.clone())),
                ));
                tool_calls.extend(answered.into_iter().map(|a| a.record));
                repairing = false;
                continue;
            }

            match self.read(&text) {
                Ok(output) => {
                    return Ok(AgentResult {
                        output,
                        messages: conversation,
                        usage,
                        turns,
                        tool_calls,
                    });
                }
                Err(problems) => {
                    let problems = problems.join("; ");
                    if repairs >= self.options.max_repairs {
                        return Err(NodeError::Transient(format!(
                            "agent '{id}' gave no valid answer: {problems}"
                        )));
                    }
                    repairs += 1;
                    repairing = true;
                    conversation.push(Message::user(if self.format.is_some() {
                        format!(
                            "Your answer was not valid: {problems}. Answer again with one JSON object that matches the schema."
                        )
                    } else {
                        format!("Your answer was not valid: {problems}. Answer again.")
                    }));
                }
            }
        }
    }

    /// The answer's text as `O`, or what is wrong with it.
    fn read(&self, text: &str) -> Result<O, Vec<String>> {
        let output: O = if self.format.is_none() {
            serde_json::from_value(Value::String(text.to_string()))
                .map_err(|error| vec![format!("it can't be read as text ({error})")])?
        } else {
            let body = unfenced(text);
            if body.is_empty() {
                return Err(vec!["it is empty".to_string()]);
            }
            serde_json::from_str(body).map_err(|error| {
                vec![format!("it is not JSON that matches the schema ({error})")]
            })?
        };
        match &self.options.validate {
            Some(validate) => match validate(&output) {
                problems if problems.is_empty() => Ok(output),
                problems => Err(problems),
            },
            None => Ok(output),
        }
    }

    /// One tool call: what goes back to the model and the record of it, or
    /// the node's failure.
    async fn call_tool(
        &self,
        call_id: &str,
        name: &str,
        arguments: &Value,
        ctx: &NodeContext,
    ) -> Result<Answered, NodeError> {
        let strata = self.declarations.strata_name(name).to_string();
        let answer = |result: ToolCallResult, content: String| Answered {
            record: AgentToolCall {
                name: strata.clone(),
                call_id: call_id.to_string(),
                result,
            },
            content,
        };
        let refused = |message: String| Ok(answer(ToolCallResult::Refused, error_json(&message)));
        if !arguments.is_object() {
            return refused(format!(
                "the arguments for tool '{name}' aren't a JSON object"
            ));
        }
        let mut tool_ctx = ToolCallContext::from(ctx).with_caller_id(self.options.id.as_str());
        if !call_id.is_empty() {
            tool_ctx = tool_ctx.with_call_id(call_id);
        }
        match self
            .tools
            .dispatch_with(&strata, &arguments.to_string(), tool_ctx)
            .await
        {
            Ok(result) => Ok(answer(ToolCallResult::Succeeded, result)),
            Err(StrataError::ToolNotFound(_)) => refused(format!("tool '{name}' was not found")),
            Err(StrataError::ToolNotPermitted(_)) => {
                refused(format!("tool '{name}' is not permitted here"))
            }
            Err(StrataError::ToolCallDenied { reason, .. }) => refused(format!("denied: {reason}")),
            Err(StrataError::ToolArgumentParseFailure(detail)) => {
                refused(format!("invalid arguments for tool '{name}': {detail}"))
            }
            // The node's token fired: the engine is dropping this node's
            // future, and a failure returned now would race it and fail the
            // node instead of cancelling it.
            Err(StrataError::ToolCancelled(_)) => std::future::pending().await,
            Err(error) => {
                let message = format!("tool '{name}' failed: {}", failure_message(&error));
                match self.options.tool_failures {
                    ToolFailures::ReturnToModel => {
                        Ok(answer(ToolCallResult::Failed, error_json(&message)))
                    }
                    ToolFailures::FailNode => Err(self
                        .options
                        .fault_classifier
                        .as_ref()
                        .and_then(|classify| classify(&error))
                        .unwrap_or_else(|| {
                            NodeError::Fatal(format!(
                                "tool '{strata}' failed: {}",
                                failure_message(&error)
                            ))
                        })),
                }
            }
        }
    }
}

impl<O> fmt::Debug for Agent<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Agent")
            .field("id", &self.options.id)
            .field("provider", &self.client.provider())
            .field("declarations", &self.declarations)
            .finish_non_exhaustive()
    }
}

struct Answered {
    record: AgentToolCall,
    content: String,
}

fn error_json(message: &str) -> String {
    json!({ "error": message }).to_string()
}

/// What a tool's failure says. A `#[strata_tool]` function's own error is
/// not kept by the macro, so its failure says only that it failed.
fn failure_message(error: &StrataError) -> String {
    match error {
        StrataError::ToolCallFailed { message, .. } => message.clone(),
        StrataError::ToolExecutionFailure(_) => "the tool returned an error".to_string(),
        other => format!("{other:?}"),
    }
}

fn add(total: ModelUsage, turn: ModelUsage) -> ModelUsage {
    let sum = |a: Option<u64>, b: Option<u64>| match (a, b) {
        (None, None) => None,
        (a, b) => Some(a.unwrap_or(0) + b.unwrap_or(0)),
    };
    ModelUsage::new(
        sum(total.input_tokens, turn.input_tokens),
        sum(total.output_tokens, turn.output_tokens),
    )
}

fn is_text<O: 'static>() -> bool {
    TypeId::of::<O>() == TypeId::of::<String>()
}

/// `my_crate::answers::Diagnosis<T>` → `Diagnosis`.
fn short_type_name<O>() -> &'static str {
    let full = std::any::type_name::<O>();
    let base = full.split('<').next().unwrap_or(full);
    base.rsplit("::").next().unwrap_or(base)
}

/// The text inside a Markdown code fence, if the model wrapped its answer in
/// one; the text trimmed otherwise.
pub(crate) fn unfenced(text: &str) -> &str {
    let trimmed = text.trim();
    if !trimmed.starts_with("```") {
        return trimmed;
    }
    match (trimmed.find('\n'), trimmed.rfind("```")) {
        (Some(start), Some(end)) if end > start => trimmed[start + 1..end].trim(),
        _ => trimmed,
    }
}

/// `schema` in the form a strict provider (OpenAI's structured outputs)
/// accepts, and whether it can be strict at all: every object with
/// `properties` closed (`additionalProperties: false`) with every property
/// required, an optional one as itself or `null`. A map (`additionalProperties`
/// with a schema) can't be strict, so its presence turns `strict` off and the
/// parse and repair do the rest. As .NET's agent asks `AIJsonUtilities` for
/// (`DisallowAdditionalProperties`, `RequireAllProperties`).
pub(crate) fn strict_schema(mut schema: Value) -> (Value, bool) {
    // A strict answer is an object at the top.
    let mut strict = schema.get("properties").is_some_and(Value::is_object);
    close(&mut schema, &mut strict);
    (schema, strict)
}

fn close(schema: &mut Value, strict: &mut bool) {
    let Value::Object(object) = schema else {
        return;
    };
    if let Some(Value::Object(_)) = object.get("additionalProperties") {
        *strict = false;
    }
    for key in ["items", "additionalProperties"] {
        if let Some(inner) = object.get_mut(key) {
            close(inner, strict);
        }
    }
    let required: Vec<String> = match object.get("required") {
        Some(Value::Array(names)) => names
            .iter()
            .filter_map(|n| n.as_str().map(str::to_string))
            .collect(),
        _ => Vec::new(),
    };
    let Some(Value::Object(properties)) = object.get_mut("properties") else {
        return;
    };
    let mut names = Vec::with_capacity(properties.len());
    for (name, property) in properties.iter_mut() {
        close(property, strict);
        if !required.contains(name) {
            *property = json!({ "anyOf": [property.take(), { "type": "null" }] });
        }
        names.push(Value::String(name.clone()));
    }
    object.insert("required".into(), Value::Array(names));
    object.insert("additionalProperties".into(), Value::Bool(false));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fence_is_removed() {
        assert_eq!(unfenced("```json\n{\"a\":1}\n```"), "{\"a\":1}");
        assert_eq!(unfenced("  {\"a\":1}  "), "{\"a\":1}");
        assert_eq!(unfenced("```\n[1]\n```\n"), "[1]");
        assert_eq!(unfenced("```{}"), "```{}");
    }

    #[test]
    fn a_schema_is_closed_and_every_property_required() {
        let (schema, strict) = strict_schema(json!({
            "type": "object",
            "properties": {
                "a": { "type": "string" },
                "b": { "type": "array", "items": {
                    "type": "object", "properties": { "c": { "type": "integer" } }, "required": [],
                } },
            },
            "required": ["a"],
        }));
        assert!(strict);
        assert_eq!(
            schema,
            json!({
                "type": "object",
                "properties": {
                    "a": { "type": "string" },
                    "b": { "anyOf": [
                        { "type": "array", "items": {
                            "type": "object",
                            "properties": { "c": { "anyOf": [{ "type": "integer" }, { "type": "null" }] } },
                            "required": ["c"],
                            "additionalProperties": false,
                        } },
                        { "type": "null" },
                    ] },
                },
                "required": ["a", "b"],
                "additionalProperties": false,
            })
        );
        let (_, strict) = strict_schema(json!({
            "type": "object",
            "properties": { "m": { "type": "object", "additionalProperties": { "type": "string" } } },
            "required": ["m"],
        }));
        assert!(!strict, "a map can't be strict");
    }

    #[test]
    fn a_type_name_is_its_last_segment() {
        assert_eq!(short_type_name::<String>(), "String");
        assert_eq!(short_type_name::<Vec<u8>>(), "Vec");
    }
}
