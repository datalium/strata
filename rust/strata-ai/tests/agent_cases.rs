//! The agent's shared cases (`design/44-LLM-Integration.md`, "Tests"):
//! `schema/fixtures/ai/agent-cases.json`, which
//! `dotnet/tests/Datalium.Strata.Tests/AI/AgentCaseTests.cs` replays too.
//! Keep the two in step.

mod common;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{Scripted, ScriptedChatClient};
use datalium_strata::BoxFuture;
use datalium_strata::StrataError;
use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{NodeError, StateGraph};
use datalium_strata::observability::{RunEvent, RunEventChannel, RunEventKind, ToolCallResult};
use datalium_strata::prelude::StrataSchema;
use datalium_strata::tools::{Next, StrataTool, ToolCall, ToolMiddleware, ToolRegistry};
use datalium_strata_ai::{
    Agent, AgentOptions, AgentResult, ChatError, Content, Role, ToolFailures,
};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const PATIENCE: Duration = Duration::from_secs(10);

fn cases() -> Vec<Value> {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/fixtures/ai/agent-cases.json");
    let json: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    json["cases"].as_array().unwrap().clone()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, StrataSchema)]
struct Diagnosis {
    cause: String,
    confidence: f64,
    evidence: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Job {
    done: bool,
}

/// A tool as a case describes it.
struct CaseTool {
    name: &'static str,
    spec: Value,
}

impl StrataTool for CaseTool {
    fn name(&self) -> &'static str {
        self.name
    }

    fn schema(&self) -> Value {
        json!({
            "name": self.name,
            "description": format!("The case's {} tool.", self.name),
            "parameters": { "type": "object", "properties": {}, "required": [] },
        })
    }

    fn execute<'a>(&'a self, raw_args: &'a str) -> BoxFuture<'a, Result<String, StrataError>> {
        Box::pin(async move {
            if let Some(ms) = self.spec["delay_ms"].as_u64() {
                tokio::time::sleep(Duration::from_millis(ms)).await;
            }
            if let Some(field) = self.spec["expects"].as_str() {
                let args: Value = serde_json::from_str(raw_args).unwrap();
                if !args[field].is_string() {
                    // What a #[strata_tool]'s generated dispatch says.
                    return Err(StrataError::ToolArgumentParseFailure(format!(
                        "argument '{field}' must be a string"
                    )));
                }
            }
            if let Some(message) = self.spec["fails"]["message"].as_str() {
                return Err(StrataError::ToolCallFailed {
                    tool: self.name.to_string(),
                    message: message.to_string(),
                });
            }
            Ok(self.spec["answers"]
                .as_str()
                .unwrap_or_default()
                .to_string())
        })
    }
}

/// Denies the calls to the tools a case marks `denied`.
struct Deny(Vec<(String, String)>);

impl ToolMiddleware for Deny {
    fn invoke<'a>(
        &'a self,
        call: ToolCall,
        next: Next<'a>,
    ) -> BoxFuture<'a, Result<String, StrataError>> {
        Box::pin(async move {
            match self.0.iter().find(|(tool, _)| tool == call.name()) {
                Some((tool, reason)) => Err(StrataError::ToolCallDenied {
                    tool: tool.clone(),
                    reason: reason.clone(),
                }),
                None => next.run(call).await,
            }
        })
    }
}

fn scripted(turn: &Value) -> Scripted {
    if let Some(text) = turn["text"].as_str() {
        return Scripted::text(&[text]);
    }
    if let Some(calls) = turn["tool_calls"].as_array() {
        let calls: Vec<(String, String, String)> = calls
            .iter()
            .map(|call| {
                let arguments = match &call["arguments"] {
                    Value::String(raw) => raw.clone(),
                    other => other.to_string(),
                };
                (
                    call["id"].as_str().unwrap().to_string(),
                    call["name"].as_str().unwrap().to_string(),
                    arguments,
                )
            })
            .collect();
        let borrowed: Vec<(&str, &str, &str)> = calls
            .iter()
            .map(|(id, name, arguments)| (id.as_str(), name.as_str(), arguments.as_str()))
            .collect();
        return Scripted::tools(&borrowed);
    }
    let fails = &turn["fails"];
    Scripted::fails(ChatError::status(
        fails["status"].as_u64().unwrap() as u16,
        None,
        fails["message"].as_str().unwrap(),
    ))
}

fn result_name(result: ToolCallResult) -> &'static str {
    match result {
        ToolCallResult::Succeeded => "succeeded",
        ToolCallResult::Failed => "failed",
        ToolCallResult::Cancelled => "cancelled",
        ToolCallResult::Refused => "refused",
    }
}

fn describe(event: &RunEvent) -> Option<String> {
    Some(match &event.kind {
        RunEventKind::ModelCallStarted { .. } => "ModelCallStarted".to_string(),
        RunEventKind::ModelCallCompleted { result, .. } => format!("ModelCallCompleted {result:?}"),
        RunEventKind::ToolCallStarted {
            tool,
            call_id,
            caller_id,
        } => format!(
            "ToolCallStarted {tool} {call_id} {}",
            caller_id.as_deref().unwrap_or_default()
        ),
        RunEventKind::ToolCallCompleted {
            tool,
            call_id,
            result,
            ..
        } => format!("ToolCallCompleted {tool} {call_id} {result:?}"),
        _ => return None,
    })
}

/// A run's output, tool calls, tool results by turn, repair requests, turns
/// and usage.
type Produced = (
    Value,
    Vec<Value>,
    Vec<Vec<(String, String)>>,
    Vec<String>,
    u32,
    [u64; 2],
);

/// What a case's run produced.
struct Outcome {
    declarations: Vec<String>,
    result: Option<Produced>,
    failure: Option<NodeError>,
    events: Vec<String>,
}

async fn play<O>(case: &Value, output: fn(&O) -> Value) -> Outcome
where
    O: DeserializeOwned + StrataSchema + Send + Sync + 'static,
{
    let options_json = &case["options"];
    let mut registry = ToolRegistry::new();
    let mut denied = Vec::new();
    let mut transient = HashSet::new();
    let mut subset = Vec::new();
    for tool in case["tools"].as_array().unwrap() {
        let name: &'static str =
            Box::leak(tool["name"].as_str().unwrap().to_string().into_boxed_str());
        registry.register(Box::new(CaseTool {
            name,
            spec: tool.clone(),
        }));
        if let Some(reason) = tool["denied"].as_str() {
            denied.push((name.to_string(), reason.to_string()));
        }
        if tool["fails"]["transient"].as_bool() == Some(true) {
            transient.insert(name.to_string());
        }
        if tool["subset"].as_bool() != Some(false) {
            subset.push(name);
        }
    }
    registry.use_middleware(Deny(denied));
    let tools = Arc::new(registry.subset(subset).unwrap());

    let mut options =
        AgentOptions::<O>::new("investigator").with_fault_classifier(move |error| match error {
            StrataError::ToolCallFailed { tool, message } if transient.contains(tool) => {
                Some(NodeError::Transient(message.clone()))
            }
            _ => None,
        });
    if let Some(n) = options_json["max_turns"].as_u64() {
        options = options.with_max_turns(n as u32);
    }
    if let Some(n) = options_json["max_repairs"].as_u64() {
        options = options.with_max_repairs(n as u32);
    }
    if let Some(n) = options_json["tool_concurrency"].as_u64() {
        options = options.with_tool_concurrency(n as usize);
    }
    if options_json["tool_failures"].as_str() == Some("return_to_model") {
        options = options.with_tool_failures(ToolFailures::ReturnToModel);
    }
    if options_json["validate"].as_str() == Some("confidence") {
        options = options.with_validate(move |o: &O| {
            let confidence = output(o)["confidence"].as_f64().unwrap();
            if (0.0..=1.0).contains(&confidence) {
                vec![]
            } else {
                vec!["confidence must be between 0 and 1".to_string()]
            }
        });
    }

    let chat = ScriptedChatClient::new(
        case["model"]
            .as_array()
            .unwrap()
            .iter()
            .map(scripted)
            .collect(),
    );
    let agent = Arc::new(Agent::new(chat, tools, options));
    let declarations = agent
        .declarations()
        .declarations()
        .iter()
        .map(|d| d.name.clone())
        .collect();

    let produced: Arc<Mutex<Option<AgentResult<O>>>> = Arc::default();
    let failed: Arc<Mutex<Option<NodeError>>> = Arc::default();
    let (graph_agent, graph_produced, graph_failed) =
        (agent.clone(), produced.clone(), failed.clone());
    let graph = StateGraph::<Job>::new()
        .add_async_fallible_node("Investigate", move |_: Job, ctx| {
            let (agent, produced, failed) = (
                graph_agent.clone(),
                graph_produced.clone(),
                graph_failed.clone(),
            );
            async move {
                match agent
                    .run(
                        vec![
                            datalium_strata_ai::Message::system("Investigate."),
                            datalium_strata_ai::Message::user("Why is checkout failing?"),
                        ],
                        &ctx,
                    )
                    .await
                {
                    Ok(result) => {
                        *produced.lock().unwrap() = Some(result);
                        Ok(Job { done: true })
                    }
                    Err(error) => {
                        *failed.lock().unwrap() = Some(error.clone());
                        Err(error)
                    }
                }
            }
        })
        .compile()
        .unwrap();
    let (observer, mut events) = RunEventChannel::new();
    let run = graph
        .run_with(
            "case",
            Job::default(),
            Arc::new(InMemoryCheckpointStore::new()),
            RunConfig::new().with_observer(observer),
        )
        .await;
    let mut described = Vec::new();
    while let Some(event) = tokio::time::timeout(PATIENCE, events.recv()).await.unwrap() {
        described.extend(describe(&event));
    }

    let failure = failed.lock().unwrap().take();
    let result = produced.lock().unwrap().take().map(|result| {
        assert!(matches!(run, GraphExecutionResult::Completed(_)));
        let tool_calls = result
            .tool_calls
            .iter()
            .map(|call| json!({ "name": call.name, "call_id": call.call_id, "result": result_name(call.result) }))
            .collect();
        let mut tool_results = Vec::new();
        let mut repairs = Vec::new();
        for message in result.messages.iter().skip(2) {
            match message.role {
                Role::Tool => tool_results.push(
                    message
                        .content
                        .iter()
                        .filter_map(|content| match content {
                            Content::ToolResult { call_id, content } => Some((call_id.clone(), content.clone())),
                            _ => None,
                        })
                        .collect(),
                ),
                Role::User => repairs.push(message.text()),
                _ => {}
            }
        }
        (
            output(&result.output),
            tool_calls,
            tool_results,
            repairs,
            result.turns,
            [
                result.usage.input_tokens.unwrap_or_default(),
                result.usage.output_tokens.unwrap_or_default(),
            ],
        )
    });
    if failure.is_some() {
        assert!(
            matches!(run, GraphExecutionResult::Failed(_)),
            "the node's failure fails the run"
        );
    }
    Outcome {
        declarations,
        result,
        failure,
        events: described,
    }
}

fn check(case: &Value, outcome: Outcome) {
    let name = case["name"].as_str().unwrap();
    let expect = &case["expect"];
    if let Some(declarations) = expect.get("declarations") {
        assert_eq!(
            json!(outcome.declarations),
            *declarations,
            "{name}: declarations"
        );
    }
    if let Some(events) = expect.get("events") {
        assert_eq!(json!(outcome.events), *events, "{name}: events");
    }
    if let Some(failure) = expect.get("failure") {
        let error = outcome
            .failure
            .unwrap_or_else(|| panic!("{name}: expected the node to fail"));
        assert_eq!(
            error.is_transient(),
            failure["transient"].as_bool().unwrap(),
            "{name}: kind"
        );
        assert_eq!(
            error.message(),
            failure["message"].as_str().unwrap(),
            "{name}: message"
        );
        return;
    }
    assert!(
        outcome.failure.is_none(),
        "{name}: unexpected failure {:?}",
        outcome.failure
    );
    let (output, tool_calls, tool_results, repairs, turns, usage) = outcome.result.unwrap();
    assert_eq!(output, expect["output"], "{name}: output");
    assert_eq!(
        turns as u64,
        expect["turns"].as_u64().unwrap(),
        "{name}: turns"
    );
    assert_eq!(json!(usage), expect["usage"], "{name}: usage");
    if let Some(expected) = expect.get("tool_calls") {
        assert_eq!(json!(tool_calls), *expected, "{name}: tool calls");
    }
    if let Some(expected) = expect.get("tool_results").and_then(Value::as_array) {
        assert_eq!(tool_results.len(), expected.len(), "{name}: tool turns");
        for (turn, expected) in tool_results.iter().zip(expected) {
            let expected = expected.as_array().unwrap();
            assert_eq!(turn.len(), expected.len(), "{name}: results in a turn");
            for ((call_id, content), expected) in turn.iter().zip(expected) {
                assert_eq!(
                    call_id,
                    expected["call_id"].as_str().unwrap(),
                    "{name}: call id"
                );
                match expected.get("content") {
                    Some(text) => assert_eq!(content, text.as_str().unwrap(), "{name}: content"),
                    None => {
                        let prefix = expected["content_starts_with"].as_str().unwrap();
                        assert!(
                            content.starts_with(prefix),
                            "{name}: {content} starts with {prefix}"
                        );
                    }
                }
            }
        }
    }
    if let Some(expected) = expect.get("repairs").and_then(Value::as_array) {
        assert_eq!(repairs.len(), expected.len(), "{name}: repairs {repairs:?}");
        for (repair, prefix) in repairs.iter().zip(expected) {
            let prefix = prefix.as_str().unwrap();
            assert!(
                repair.starts_with(prefix),
                "{name}: {repair} starts with {prefix}"
            );
        }
    }
}

#[tokio::test]
async fn every_shared_case_plays_as_the_fixture_says() {
    let cases = cases();
    assert!(cases.len() >= 20);
    for case in &cases {
        let outcome = match case["options"]["output"].as_str() {
            Some("diagnosis") => {
                play::<Diagnosis>(case, |d| serde_json::to_value(d).unwrap()).await
            }
            _ => play::<String>(case, |text| json!(text)).await,
        };
        check(case, outcome);
    }
}
