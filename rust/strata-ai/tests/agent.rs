//! The agent (`design/44-LLM-Integration.md`, PR 2) beyond the shared cases
//! (`agent_cases.rs`): what middleware sees, sequential and concurrent tool
//! calls, the requests each turn sends, streaming, cancellation, the agent
//! node, and a `#[strata_tool]`'s own argument failure. Mirrors the .NET
//! suite's `AgentTests.cs`.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{Scripted, ScriptedChatClient};
use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::context::RunConfig;
use datalium_strata::graph::StateGraph;
use datalium_strata::observability::{RunEventChannel, RunEventKind, ToolCallResult};
use datalium_strata::prelude::*;
use datalium_strata::streaming::StreamChunkType;
use datalium_strata::tools::{Next, ToolCall, ToolMiddleware};
use datalium_strata::{BoxFuture, CancellationToken};
use datalium_strata_ai::{
    Agent, AgentOptions, AgentResult, AgentTurn, ChatClient, Message, ResponseFormat, Role,
    StateGraphAiExt, ToolChoice,
};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Notify;

const PATIENCE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Job {
    answer: String,
    turns: u32,
}

#[derive(Debug, Clone, PartialEq, Deserialize, StrataSchema)]
struct Diagnosis {
    cause: String,
    confidence: f64,
    evidence: Vec<String>,
    severity: Option<String>,
}

#[derive(Deserialize, StrataSchema)]
struct PodsArgs {
    namespace: String,
}

#[strata_tool(name = "k8s.pods", description = "Lists a namespace's pods.")]
async fn k8s_pods(args: PodsArgs) -> Result<String, StrataError> {
    Ok(format!("3 pods in {}", args.namespace))
}

fn agent_node<O>(agent: Arc<Agent<O>>) -> Arc<datalium_strata::graph::CompiledGraph<Job>>
where
    O: serde::de::DeserializeOwned + StrataSchema + Send + Sync + std::fmt::Debug + 'static,
{
    Arc::new(
        StateGraph::<Job>::new()
            .add_agent_node(
                "Investigate",
                agent,
                |_: &Job, _| vec![Message::system("Investigate."), Message::user("Why?")],
                |_: Job, result: AgentResult<O>| Job {
                    answer: format!("{:?}", result.output),
                    turns: result.turns,
                },
            )
            .compile()
            .unwrap(),
    )
}

fn store() -> Arc<InMemoryCheckpointStore> {
    Arc::new(InMemoryCheckpointStore::new())
}

/// Records what each call's context says.
#[derive(Default)]
struct Witness(Mutex<Vec<String>>);

impl ToolMiddleware for Witness {
    fn invoke<'a>(
        &'a self,
        call: ToolCall,
        next: Next<'a>,
    ) -> BoxFuture<'a, Result<String, StrataError>> {
        Box::pin(async move {
            let ctx = call.context();
            self.0.lock().unwrap().push(format!(
                "{} caller={} call={} node={} attempt={}",
                call.name(),
                ctx.caller_id().unwrap_or_default(),
                ctx.call_id().unwrap_or_default(),
                ctx.node_path().unwrap_or_default(),
                ctx.attempt().unwrap_or_default(),
            ));
            next.run(call).await
        })
    }
}

#[tokio::test]
async fn middleware_sees_the_strata_name_the_agent_as_caller_and_the_model_s_call_id() {
    let witness = Arc::new(Witness::default());
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(K8sPodsTool));
    registry.use_middleware(witness.clone());
    let chat = ScriptedChatClient::new(vec![
        Scripted::tools(&[("toolu_01", "k8s_pods", r#"{"namespace":"shop"}"#)]),
        Scripted::text(&["done"]),
    ]);
    let agent = Arc::new(Agent::<String>::new(
        chat,
        Arc::new(registry),
        AgentOptions::new("kubernetes-investigator"),
    ));

    let result = agent_node(agent)
        .run("witnessed", Job::default(), store())
        .await;

    assert!(matches!(result, GraphExecutionResult::Completed(_)));
    assert_eq!(
        *witness.0.lock().unwrap(),
        vec!["k8s.pods caller=kubernetes-investigator call=toolu_01 node=Investigate attempt=1"]
    );
}

#[tokio::test]
async fn a_strata_tool_that_can_t_read_its_arguments_is_refused_back_to_the_model() {
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(K8sPodsTool));
    let chat = ScriptedChatClient::new(vec![
        Scripted::tools(&[("1", "k8s_pods", r#"{"namespace":5}"#)]),
        Scripted::tools(&[("2", "k8s_pods", r#"{"namespace":"shop"}"#)]),
        Scripted::text(&["done"]),
    ]);
    let agent = Agent::<String>::new(chat, Arc::new(registry), AgentOptions::new("a"));
    let ctx = datalium_strata::context::NodeContext::new("thread", "Node");

    let result = agent.run(vec![Message::user("?")], &ctx).await.unwrap();

    let results: Vec<String> = result
        .messages
        .iter()
        .filter(|m| m.role == Role::Tool)
        .map(|m| format!("{:?}", m.content))
        .collect();
    assert!(
        results[0].contains(
            "invalid arguments for tool 'k8s_pods': argument 'namespace' must be a string"
        ),
        "{results:?}"
    );
    assert!(results[1].contains("3 pods in shop"), "{results:?}");
    assert_eq!(result.tool_calls[0].result, ToolCallResult::Refused);
    assert_eq!(result.tool_calls[1].result, ToolCallResult::Succeeded);
}

#[tokio::test]
async fn configure_turn_changes_one_turn_s_request_and_sees_where_the_run_is() {
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(K8sPodsTool));
    let chat = ScriptedChatClient::new(vec![
        Scripted::tools(&[("1", "k8s_pods", r#"{"namespace":"shop"}"#)]),
        Scripted::text(&["not json"]),
        Scripted::text(&[r#"{"cause":"pods","confidence":0.5,"evidence":["1"],"severity":null}"#]),
    ]);
    let seen: Arc<Mutex<Vec<(AgentTurn, usize)>>> = Arc::default();
    let witness = seen.clone();
    let agent = Agent::<Diagnosis>::new(
        chat.clone(),
        Arc::new(registry),
        AgentOptions::new("a")
            .with_temperature(0.2)
            .with_configure_turn(move |turn, request| {
                assert_eq!(request.tools.len(), 1);
                assert!(request.response_format.is_some());
                witness
                    .lock()
                    .unwrap()
                    .push((*turn, request.messages.len()));
                if turn.number == 1 {
                    request.tool_choice = Some(ToolChoice::Required);
                }
            }),
    );
    let ctx = datalium_strata::context::NodeContext::new("thread", "Node");

    agent.run(vec![Message::user("?")], &ctx).await.unwrap();

    let seen: Vec<_> = seen
        .lock()
        .unwrap()
        .iter()
        .map(|(t, messages)| (t.number, t.tool_turns, t.repairs, t.is_repair, *messages))
        .collect();
    assert_eq!(
        seen,
        vec![(1, 0, 0, false, 1), (2, 1, 0, false, 3), (3, 1, 1, true, 5)]
    );
    let requests = chat.requests();
    let choices: Vec<_> = requests
        .iter()
        .map(|r| r.request.tool_choice.clone())
        .collect();
    assert_eq!(choices, vec![Some(ToolChoice::Required), None, None]);
    assert!(requests.iter().all(|r| r.request.temperature == Some(0.2)));
}

/// A tool that notes how many calls are in flight, and waits for `release`
/// (when given) before answering.
struct Gate {
    name: &'static str,
    in_flight: Arc<AtomicUsize>,
    most: Arc<AtomicUsize>,
    started: Arc<Notify>,
    release: Option<Arc<Notify>>,
}

impl StrataTool for Gate {
    fn name(&self) -> &'static str {
        self.name
    }

    fn schema(&self) -> serde_json::Value {
        json!({ "name": self.name, "description": "", "parameters": { "type": "object", "properties": {}, "required": [] } })
    }

    fn execute<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<String, StrataError>> {
        Box::pin(async move {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.most.fetch_max(now, Ordering::SeqCst);
            self.started.notify_one();
            match &self.release {
                Some(release) => release.notified().await,
                None => tokio::time::sleep(Duration::from_millis(20)).await,
            }
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok(self.name.to_string())
        })
    }
}

fn gates(waiting: bool) -> (ToolRegistry, Arc<AtomicUsize>) {
    let (in_flight, most) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let b_started = Arc::new(Notify::new());
    let mut registry = ToolRegistry::new();
    // `a` answers only once `b` has started: sequentially, it never would.
    registry.register(Box::new(Gate {
        name: "a",
        in_flight: in_flight.clone(),
        most: most.clone(),
        started: Arc::new(Notify::new()),
        release: waiting.then(|| b_started.clone()),
    }));
    registry.register(Box::new(Gate {
        name: "b",
        in_flight,
        most: most.clone(),
        started: b_started,
        release: None,
    }));
    (registry, most)
}

#[tokio::test]
async fn a_turn_s_calls_run_one_at_a_time_by_default() {
    let (registry, most) = gates(false);
    let chat = ScriptedChatClient::new(vec![
        Scripted::tools(&[("1", "a", "{}"), ("2", "b", "{}")]),
        Scripted::text(&["done"]),
    ]);
    let agent = Agent::<String>::new(chat, Arc::new(registry), AgentOptions::new("a"));
    let ctx = datalium_strata::context::NodeContext::new("thread", "Node");

    agent.run(vec![Message::user("?")], &ctx).await.unwrap();

    assert_eq!(most.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn with_tool_concurrency_a_turn_s_calls_run_at_once_and_answer_in_the_model_s_order() {
    let (registry, most) = gates(true);
    let chat = ScriptedChatClient::new(vec![
        Scripted::tools(&[("1", "a", "{}"), ("2", "b", "{}")]),
        Scripted::text(&["done"]),
    ]);
    let agent = Agent::<String>::new(
        chat,
        Arc::new(registry),
        AgentOptions::new("a").with_tool_concurrency(2),
    );
    let ctx = datalium_strata::context::NodeContext::new("thread", "Node");

    let result = tokio::time::timeout(PATIENCE, agent.run(vec![Message::user("?")], &ctx))
        .await
        .expect("`a` waits for `b`, so they ran at once")
        .unwrap();

    assert_eq!(most.load(Ordering::SeqCst), 2);
    let ids: Vec<&str> = result
        .tool_calls
        .iter()
        .map(|c| c.call_id.as_str())
        .collect();
    assert_eq!(ids, ["1", "2"]);
}

#[tokio::test]
async fn every_turn_sends_the_declarations_and_a_typed_agent_asks_for_its_schema() {
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(K8sPodsTool));
    let chat = ScriptedChatClient::new(vec![
        Scripted::tools(&[("1", "k8s_pods", r#"{"namespace":"shop"}"#)]),
        Scripted::text(&[r#"{"cause":"pods","confidence":0.5,"evidence":["1"],"severity":null}"#]),
    ]);
    let agent = Agent::<Diagnosis>::new(
        chat.clone(),
        Arc::new(registry),
        AgentOptions::new("a")
            .with_model("gpt-test")
            .with_max_tokens(500),
    );
    let ctx = datalium_strata::context::NodeContext::new("thread", "Node");

    let result = agent.run(vec![Message::user("?")], &ctx).await.unwrap();

    assert_eq!(
        result.output,
        Diagnosis {
            cause: "pods".into(),
            confidence: 0.5,
            evidence: vec!["1".into()],
            severity: None,
        }
    );
    let requests = chat.requests();
    assert_eq!(requests.len(), 2);
    for seen in &requests {
        let request = &seen.request;
        assert_eq!(request.model.as_deref(), Some("gpt-test"));
        assert_eq!(request.max_tokens, Some(500));
        assert_eq!(request.tools.len(), 1);
        assert_eq!(request.tools[0].name, "k8s_pods");
        assert_eq!(
            request.tools[0].parameters["properties"]["namespace"],
            json!({ "type": "string" })
        );
        let Some(ResponseFormat::JsonSchema {
            name,
            schema,
            strict,
        }) = &request.response_format
        else {
            panic!("a typed agent asks for its schema");
        };
        assert_eq!(name, "Diagnosis");
        assert!(strict);
        assert_eq!(schema["additionalProperties"], json!(false));
        assert_eq!(
            schema["required"],
            json!(["cause", "confidence", "evidence", "severity"])
        );
        assert_eq!(
            schema["properties"]["severity"],
            json!({ "anyOf": [{ "type": "string" }, { "type": "null" }] })
        );
    }
    assert_eq!(
        requests[1].request.messages.len(),
        3,
        "the question, the call, its result"
    );
    let text = Agent::<String>::new(
        ScriptedChatClient::new(vec![]) as Arc<dyn ChatClient>,
        Arc::new(ToolRegistry::new()),
        AgentOptions::new("t"),
    );
    assert!(
        text.response_format().is_none(),
        "a text agent asks for text"
    );
}

#[tokio::test]
async fn under_run_streaming_every_turn_streams_its_text() {
    let chat = ScriptedChatClient::new(vec![
        Scripted::text(&["Checking", " pods."]).with_tool_call(
            "1",
            "k8s_pods",
            r#"{"namespace":"shop"}"#,
        ),
        Scripted::text(&["Roll ", "back."]),
    ]);
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(K8sPodsTool));
    let agent = Arc::new(Agent::<String>::new(
        chat.clone(),
        Arc::new(registry),
        AgentOptions::new("a"),
    ));

    let (handle, mut chunks) = agent_node(agent).run_streaming("streamed", Job::default(), store());
    let mut tokens = Vec::new();
    while let Some(chunk) = tokio::time::timeout(PATIENCE, chunks.recv()).await.unwrap() {
        if chunk.chunk_type == StreamChunkType::Token {
            tokens.push(chunk.content.to_string());
        }
    }

    let GraphExecutionResult::Completed(job) = handle.await.unwrap() else {
        panic!("expected completion")
    };
    assert_eq!(job.answer, "\"Roll back.\"");
    assert_eq!(tokens, ["Checking", " pods.", "Roll ", "back."]);
    assert!(chat.requests().iter().all(|seen| seen.streamed));
}

/// Never answers.
struct Hang(Arc<Notify>);

impl StrataTool for Hang {
    fn name(&self) -> &'static str {
        "hang"
    }

    fn schema(&self) -> serde_json::Value {
        json!({ "name": "hang", "description": "", "parameters": { "type": "object", "properties": {}, "required": [] } })
    }

    fn execute<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<String, StrataError>> {
        Box::pin(async move {
            self.0.notify_one();
            std::future::pending().await
        })
    }
}

#[tokio::test]
async fn cancelling_the_run_while_a_tool_runs_ends_the_node_cancelled() {
    let started = Arc::new(Notify::new());
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(Hang(started.clone())));
    let chat = ScriptedChatClient::new(vec![Scripted::tools(&[("1", "hang", "{}")])]);
    let agent = Arc::new(Agent::<String>::new(
        chat,
        Arc::new(registry),
        AgentOptions::new("a"),
    ));
    let token = CancellationToken::new();
    let (observer, mut events) = RunEventChannel::new();
    let graph = agent_node(agent);
    let run = tokio::spawn({
        let token = token.clone();
        async move {
            graph
                .run_with(
                    "cancelled",
                    Job::default(),
                    store(),
                    RunConfig::new()
                        .with_cancellation(token)
                        .with_observer(observer),
                )
                .await
        }
    });

    tokio::time::timeout(PATIENCE, started.notified())
        .await
        .unwrap();
    token.cancel();

    assert!(matches!(
        tokio::time::timeout(PATIENCE, run).await.unwrap().unwrap(),
        GraphExecutionResult::Cancelled { .. }
    ));
    let mut completed = Vec::new();
    while let Some(event) = tokio::time::timeout(PATIENCE, events.recv()).await.unwrap() {
        if let RunEventKind::ToolCallCompleted { result, .. } = event.kind {
            completed.push(result);
        }
    }
    assert_eq!(completed, [ToolCallResult::Cancelled]);
}

#[tokio::test]
async fn the_agent_node_is_an_ordinary_node_and_folds_the_result_into_the_state() {
    let chat = ScriptedChatClient::new(vec![Scripted::text(&["ok"])]);
    let agent = Arc::new(Agent::<String>::new(
        chat,
        Arc::new(ToolRegistry::new()),
        AgentOptions::new("a"),
    ));
    let graph = agent_node(agent);

    let definition = serde_json::to_value(graph.to_definition()).unwrap();
    assert_eq!(definition["nodes"][0]["name"], "Investigate");
    assert!(definition["nodes"][0].get("kind").is_none());
    let GraphExecutionResult::Completed(job) = graph.run("node", Job::default(), store()).await
    else {
        panic!("expected completion")
    };
    assert_eq!(job.turns, 1);
}
