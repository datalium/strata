//! What a tool call's `ToolCallContext` carries, and how cancellation reaches
//! a tool (`design/20-Tool-Dispatch-And-Registry.md`, SG-21). The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/Tools/ToolCallContextTests.cs` — keep
//! the two in step.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::BoxFuture;
use datalium_strata::CancellationToken;
use datalium_strata::checkpoint::{CheckpointStore, CheckpointStoreError, GraphExecutionResult};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::parallel::ParallelBranches;
use datalium_strata::prelude::*;
use datalium_strata::schema::Checkpoint;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

const PATIENCE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Doc {
    steps: Vec<String>,
}

fn doc() -> Doc {
    Doc { steps: Vec::new() }
}

#[derive(Default)]
struct TestStore {
    checkpoints: Mutex<HashMap<String, Checkpoint>>,
}

impl CheckpointStore for TestStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        self.checkpoints
            .lock()
            .unwrap()
            .insert(checkpoint.thread_id.clone(), checkpoint);
        Box::pin(std::future::ready(Ok(())))
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        let found = self.checkpoints.lock().unwrap().get(thread_id).cloned();
        Box::pin(std::future::ready(Ok(found)))
    }
}

fn store() -> Arc<dyn CheckpointStore> {
    Arc::new(TestStore::default())
}

fn completed(result: GraphExecutionResult<Doc>) -> Doc {
    match result {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(err) => panic!("expected Completed, got Failed({err:?})"),
        GraphExecutionResult::Interrupted { .. } => panic!("expected Completed, got Interrupted"),
        GraphExecutionResult::Cancelled { node, .. } => {
            panic!("expected Completed, got Cancelled at {node}")
        }
    }
}

/// What a tool saw of its context.
#[derive(Debug, Clone, PartialEq)]
struct Seen {
    thread_id: Option<String>,
    run_id: Option<String>,
    node_path: Option<String>,
    attempt: Option<u32>,
    tenant: Option<String>,
    caller_id: Option<String>,
    call_id: Option<String>,
}

/// A service the tools reach through their context, recording what they saw.
#[derive(Default)]
struct Probe {
    seen: Mutex<Vec<Seen>>,
    calls: AtomicU32,
    started: Notify,
}

#[derive(Deserialize, StrataSchema)]
struct Empty {}

#[strata_tool(name = "probe", description = "Records its context.")]
async fn probe(_args: Empty, ctx: ToolCallContext) -> Result<String, StrataError> {
    let probe = ctx.service::<Probe>().expect("the run supplies a Probe");
    probe.calls.fetch_add(1, Ordering::SeqCst);
    probe.seen.lock().unwrap().push(Seen {
        thread_id: ctx.thread_id().map(str::to_string),
        run_id: ctx.run_id().map(str::to_string),
        node_path: ctx.node_path().map(str::to_string),
        attempt: ctx.attempt(),
        tenant: ctx.metadata().get("tenant").cloned(),
        caller_id: ctx.caller_id().map(str::to_string),
        call_id: ctx.call_id().map(str::to_string),
    });
    Ok("probed".to_string())
}

#[strata_tool(name = "wait", description = "Waits for its token.")]
async fn wait(_args: Empty, ctx: ToolCallContext) -> Result<String, StrataError> {
    let probe = ctx.service::<Probe>().expect("the run supplies a Probe");
    probe.calls.fetch_add(1, Ordering::SeqCst);
    probe.started.notify_one();
    ctx.cancellation().cancelled().await;
    Err(StrataError::ExecutionAborted("stopped".to_string()))
}

#[strata_tool(name = "plain", description = "Takes no context.")]
async fn plain(_args: Empty) -> Result<String, StrataError> {
    Ok("plain".to_string())
}

fn registry() -> Arc<ToolRegistry> {
    let mut tools = ToolRegistry::new();
    tools
        .register(Box::new(ProbeTool))
        .register(Box::new(WaitTool))
        .register(Box::new(PlainTool));
    Arc::new(tools)
}

/// An async node that calls `probe` as `caller`.
fn calls_probe(
    tools: &Arc<ToolRegistry>,
    caller: &'static str,
) -> impl Fn(Doc, NodeContext) -> BoxFuture<'static, Doc> + Send + Sync + 'static {
    let tools = tools.clone();
    move |doc, ctx| {
        let tools = tools.clone();
        Box::pin(async move {
            let call = ToolCallContext::from(&ctx).with_caller_id(caller);
            tools.dispatch_with("probe", "{}", call).await.unwrap();
            doc
        })
    }
}

#[tokio::test]
async fn from_a_node_the_context_carries_the_run_the_node_and_the_services() {
    let tools = registry();
    let probe = Arc::new(Probe::default());
    let graph = StateGraph::<Doc>::new()
        .add_async_node("Investigate", calls_probe(&tools, "k8s-investigator"))
        .compile()
        .unwrap();
    let config = RunConfig::new()
        .with_run_id("run-1")
        .with_metadata("tenant", "acme")
        .with_service(probe.clone());

    completed(graph.run_with("thread_tools", doc(), store(), config).await);

    let seen = probe.seen.lock().unwrap();
    let [seen] = seen.as_slice() else {
        panic!("expected one call, got {seen:?}")
    };
    assert_eq!(seen.thread_id.as_deref(), Some("thread_tools"));
    assert_eq!(seen.run_id.as_deref(), Some("run-1"));
    assert_eq!(seen.node_path.as_deref(), Some("Investigate"));
    assert_eq!(seen.attempt, Some(1));
    assert_eq!(seen.tenant.as_deref(), Some("acme"));
    assert_eq!(seen.caller_id.as_deref(), Some("k8s-investigator"));
    assert!(seen.call_id.is_some(), "the registry generates a call id");
}

#[tokio::test]
async fn paths_and_attempts_are_the_calling_nodes_at_every_depth() {
    let tools = registry();
    let probe = Arc::new(Probe::default());
    let failures = Arc::new(AtomicU32::new(0));
    let branch_tools = tools.clone();
    let child = StateGraph::<Doc>::new()
        .add_parallel_group(
            "Gather",
            ParallelBranches::new().async_branch("Acme", move |doc: Doc, ctx: NodeContext| {
                let tools = branch_tools.clone();
                async move {
                    tools
                        .dispatch_with("probe", "{}", ToolCallContext::from(&ctx))
                        .await
                        .unwrap();
                    Ok(doc)
                }
            }),
            |entry: &Doc, _| entry.clone(),
        )
        .compile()
        .unwrap();
    let retry_tools = tools.clone();
    let graph = StateGraph::<Doc>::new()
        .add_subgraph("Work", child, |d: &Doc| d.clone(), |_: &Doc, r| r)
        .add_async_fallible_node("Charge", move |doc: Doc, ctx: NodeContext| {
            let tools = retry_tools.clone();
            let failures = failures.clone();
            async move {
                tools
                    .dispatch_with("probe", "{}", ToolCallContext::from(&ctx))
                    .await
                    .unwrap();
                if failures.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(NodeError::Transient("gateway timeout".to_string()))
                } else {
                    Ok(doc)
                }
            }
        })
        .set_recovery_policy("Charge", RecoveryPolicy::retry(1))
        .add_edge("Work", "Charge")
        .compile()
        .unwrap();

    completed(
        graph
            .run_with(
                "outer",
                doc(),
                store(),
                RunConfig::new().with_service(probe.clone()),
            )
            .await,
    );

    let seen: Vec<(Option<String>, Option<u32>, Option<String>)> = probe
        .seen
        .lock()
        .unwrap()
        .iter()
        .map(|s| (s.node_path.clone(), s.attempt, s.thread_id.clone()))
        .collect();
    let outer = Some("outer".to_string());
    assert_eq!(
        seen,
        vec![
            (Some("Work/Gather/Acme".to_string()), Some(1), outer.clone()),
            (Some("Charge".to_string()), Some(1), outer.clone()),
            (Some("Charge".to_string()), Some(2), outer),
        ]
    );
}

#[tokio::test]
async fn a_call_id_is_generated_when_missing_and_kept_when_given() {
    let tools = registry();
    let probe = Arc::new(Probe::default());
    let base = ToolCallContext::new().with_service(probe.clone());

    tools
        .dispatch_with("probe", "{}", base.clone())
        .await
        .unwrap();
    tools
        .dispatch_with("probe", "{}", base.clone())
        .await
        .unwrap();
    tools
        .dispatch_with("probe", "{}", base.with_call_id("call_42"))
        .await
        .unwrap();

    let ids: Vec<String> = probe
        .seen
        .lock()
        .unwrap()
        .iter()
        .map(|s| s.call_id.clone().unwrap())
        .collect();
    assert_ne!(ids[0], ids[1], "a fresh id per call");
    assert_eq!(uuid_version(&ids[0]), Some('7'));
    assert_eq!(ids[2], "call_42");
}

/// The version digit of a hyphenated UUID.
fn uuid_version(id: &str) -> Option<char> {
    (id.len() == 36).then(|| id.chars().nth(14)).flatten()
}

#[tokio::test]
async fn an_already_cancelled_token_stops_the_call_before_the_tool_runs() {
    let tools = registry();
    let probe = Arc::new(Probe::default());
    let token = CancellationToken::new();
    token.cancel();
    let ctx = ToolCallContext::new()
        .with_service(probe.clone())
        .with_cancellation(token);

    let err = tools.dispatch_with("probe", "{}", ctx).await.unwrap_err();

    assert!(matches!(err, StrataError::ToolCancelled(name) if name == "probe"));
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_tool_that_fails_after_its_token_fired_is_cancelled() {
    let tools = registry();
    let probe = Arc::new(Probe::default());
    let token = CancellationToken::new();
    let ctx = ToolCallContext::new()
        .with_service(probe.clone())
        .with_cancellation(token.clone());

    let call = tokio::spawn(async move { tools.dispatch_with("wait", "{}", ctx).await });
    tokio::time::timeout(PATIENCE, probe.started.notified())
        .await
        .unwrap();
    token.cancel();
    let err = tokio::time::timeout(PATIENCE, call)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();

    assert!(matches!(err, StrataError::ToolCancelled(name) if name == "wait"));
}

#[tokio::test]
async fn cancelling_the_run_reaches_a_tool_through_the_nodes_token() {
    let tools = registry();
    let probe = Arc::new(Probe::default());
    let token = CancellationToken::new();
    let node_tools = tools.clone();
    let graph = StateGraph::<Doc>::new()
        .add_async_fallible_node("Investigate", move |doc: Doc, ctx: NodeContext| {
            let tools = node_tools.clone();
            async move {
                match tools
                    .dispatch_with("wait", "{}", ToolCallContext::from(&ctx))
                    .await
                {
                    Ok(_) => Ok(doc),
                    // Let the engine see the cancellation: an error returned in
                    // the same poll would win the race and fail the node
                    // (design/17's rule, the same in .NET for a NodeException).
                    Err(StrataError::ToolCancelled(_)) => std::future::pending().await,
                    Err(e) => Err(NodeError::Transient(format!("{e:?}"))),
                }
            }
        })
        .compile()
        .unwrap();
    let config = RunConfig::new()
        .with_service(probe.clone())
        .with_cancellation(token.clone());

    let run = tokio::spawn(async move { graph.run_with("t", doc(), store(), config).await });
    tokio::time::timeout(PATIENCE, probe.started.notified())
        .await
        .unwrap();
    token.cancel();
    let result = tokio::time::timeout(PATIENCE, run).await.unwrap().unwrap();

    assert!(
        matches!(result, GraphExecutionResult::Cancelled { ref node, .. } if node == "Investigate")
    );
}

#[tokio::test]
async fn a_tool_without_a_context_parameter_runs_through_either_dispatch() {
    let tools = registry();

    assert_eq!(tools.dispatch("plain", "{}").await.unwrap(), "plain");
    assert_eq!(
        tools
            .dispatch_with("plain", "{}", ToolCallContext::new().with_caller_id("a"))
            .await
            .unwrap(),
        "plain"
    );
    // And one that takes a context gets the default one through `execute`.
    let probe = Arc::new(Probe::default());
    let direct = ProbeTool.execute_with("{}", ToolCallContext::new().with_service(probe.clone()));
    assert_eq!(direct.await.unwrap(), "probed");
    assert_eq!(
        probe.seen.lock().unwrap()[0].call_id,
        None,
        "no registry, no call id"
    );
}

#[test]
fn a_context_built_by_hand_carries_what_it_was_given() {
    let ctx = ToolCallContext::new()
        .with_thread_id("t")
        .with_run_id("r")
        .with_node_path("Work/Send")
        .with_attempt(2)
        .with_metadata("tenant", "acme")
        .with_caller_id("agent")
        .with_call_id("c");

    assert_eq!(ctx.thread_id(), Some("t"));
    assert_eq!(ctx.run_id(), Some("r"));
    assert_eq!(ctx.node_path(), Some("Work/Send"));
    assert_eq!(ctx.attempt(), Some(2));
    assert_eq!(
        ctx.metadata().get("tenant").map(String::as_str),
        Some("acme")
    );
    assert_eq!(ctx.caller_id(), Some("agent"));
    assert_eq!(ctx.call_id(), Some("c"));
    assert!(!ctx.cancellation().is_cancelled());

    let nothing = ToolCallContext::default();
    assert_eq!(
        (nothing.thread_id(), nothing.node_path(), nothing.attempt()),
        (None, None, None)
    );
    assert!(nothing.metadata().is_empty());
    assert!(nothing.service::<Probe>().is_none());
}

#[test]
fn from_a_hand_built_node_context_the_run_id_is_absent_rather_than_empty() {
    let node = NodeContext::new("t", "Draft").with_attempt(3);

    let ctx = ToolCallContext::from(&node);

    assert_eq!(ctx.thread_id(), Some("t"));
    assert_eq!(ctx.node_path(), Some("Draft"));
    assert_eq!(ctx.attempt(), Some(3));
    assert_eq!(ctx.run_id(), None);
}
