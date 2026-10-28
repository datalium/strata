//! Inline sync nodes (`design/39-Inline-Sync-Handlers.md`): `add_inline_node`
//! and `add_inline_fallible_node` run a sync handler on the executor instead
//! of tokio's blocking pool. They are nodes like any other (recovery policy,
//! panics, export), except that `.compile()` refuses a timeout on one and a
//! cancellation that arrives while one runs takes effect when it returns.
//! Rust only: the .NET engine has no counterpart.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;
use std::time::Duration;

use datalium_strata::checkpoint::{
    CancelReason, CheckpointStore, CheckpointStoreError, GraphExecutionResult,
};
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::schema::{Checkpoint, CheckpointStatus};
use datalium_strata::{CancellationToken, StrataError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Doc {
    steps: Vec<String>,
}

fn doc() -> Doc {
    Doc { steps: Vec::new() }
}

fn did(step: &'static str) -> impl Fn(Doc) -> Doc + Send + Sync + 'static {
    move |mut doc| {
        doc.steps.push(step.to_string());
        doc
    }
}

#[derive(Default)]
struct TestStore {
    checkpoints: Mutex<HashMap<String, Checkpoint>>,
}

impl TestStore {
    fn latest(&self, thread_id: &str) -> Option<Checkpoint> {
        self.checkpoints.lock().unwrap().get(thread_id).cloned()
    }
}

impl CheckpointStore for TestStore {
    fn save(
        &self,
        checkpoint: Checkpoint,
    ) -> datalium_strata::BoxFuture<'_, Result<(), CheckpointStoreError>> {
        self.checkpoints
            .lock()
            .unwrap()
            .insert(checkpoint.thread_id.clone(), checkpoint);
        Box::pin(std::future::ready(Ok(())))
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> datalium_strata::BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        Box::pin(std::future::ready(Ok(self.latest(thread_id))))
    }
}

fn store() -> Arc<TestStore> {
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

/// Records the thread a handler ran on under `name`.
fn on_thread(
    seen: &Arc<Mutex<HashMap<&'static str, ThreadId>>>,
    name: &'static str,
) -> impl Fn(Doc) -> Doc + Send + Sync + 'static {
    let seen = seen.clone();
    move |doc| {
        seen.lock()
            .unwrap()
            .insert(name, std::thread::current().id());
        did(name)(doc)
    }
}

#[tokio::test]
async fn inline_nodes_run_on_the_thread_that_polls_the_run() {
    // A current-thread runtime: the run is polled on the test's own thread,
    // and the blocking pool is other threads.
    let seen: Arc<Mutex<HashMap<&'static str, ThreadId>>> = Arc::default();
    let fallible = on_thread(&seen, "fallible");
    let graph = StateGraph::<Doc>::new()
        .add_inline_node("Inline", on_thread(&seen, "inline"))
        .add_inline_fallible_node("Fallible", move |doc: &Doc| Ok(fallible(doc.clone())))
        .add_node("Pooled", on_thread(&seen, "pooled"))
        .add_edge("Inline", "Fallible")
        .add_edge("Fallible", "Pooled")
        .compile()
        .unwrap();

    let state = completed(graph.run("t", doc(), store()).await);

    assert_eq!(state.steps, vec!["inline", "fallible", "pooled"]);
    let here = std::thread::current().id();
    let seen = seen.lock().unwrap();
    assert_eq!(seen["inline"], here);
    assert_eq!(seen["fallible"], here);
    assert_ne!(seen["pooled"], here);
}

#[tokio::test]
async fn an_inline_fallible_node_is_retried_under_its_policy() {
    let calls = Arc::new(AtomicU32::new(0));
    let counted = calls.clone();
    let graph = StateGraph::<Doc>::new()
        .add_inline_fallible_node("Parse", move |doc: &Doc| {
            if counted.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(NodeError::Transient("not yet".to_string()));
            }
            Ok(did("parse")(doc.clone()))
        })
        .set_recovery_policy("Parse", RecoveryPolicy::retry(2))
        .compile()
        .unwrap();

    let state = completed(graph.run("t", doc(), store()).await);

    assert_eq!(state.steps, vec!["parse"]);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn an_inline_fallible_node_falls_back_with_the_state_before_it() {
    let graph = StateGraph::<Doc>::new()
        .add_inline_node("Prepare", did("prepare"))
        .add_inline_fallible_node("Parse", |_: &Doc| {
            Err(NodeError::Fatal("malformed".to_string()))
        })
        .add_inline_node("Repair", did("repair"))
        .add_edge("Prepare", "Parse")
        .set_recovery_policy("Parse", RecoveryPolicy::retry(0).with_fallback("Repair"))
        .compile()
        .unwrap();

    let state = completed(graph.run("t", doc(), store()).await);

    assert_eq!(state.steps, vec!["prepare", "repair"]);
}

#[tokio::test]
async fn a_failing_inline_node_fails_the_run_with_a_failed_checkpoint() {
    let store = store();
    let graph = StateGraph::<Doc>::new()
        .add_inline_fallible_node("Parse", |_: &Doc| {
            Err(NodeError::Fatal("malformed".to_string()))
        })
        .compile()
        .unwrap();

    let result = graph.run("t", doc(), store.clone()).await;

    assert!(matches!(
        result,
        GraphExecutionResult::Failed(StrataError::NodeFailed { ref node, attempts: 1, ref message })
            if node == "Parse" && message == "malformed"
    ));
    let checkpoint = store.latest("t").unwrap();
    assert_eq!(checkpoint.status, CheckpointStatus::Failed);
    assert_eq!(checkpoint.suspended_node.as_deref(), Some("Parse"));
}

#[tokio::test]
async fn a_panic_in_an_inline_node_unwinds_through_the_run() {
    let graph = StateGraph::<Doc>::new()
        .add_inline_node("Load", |_: Doc| panic!("node bug"))
        .compile()
        .unwrap();

    let outcome = tokio::spawn(async move { graph.run("t", doc(), store()).await }).await;

    assert!(outcome.is_err_and(|error| error.is_panic()));
}

#[test]
fn compile_refuses_a_timeout_on_an_inline_node() {
    let plain = StateGraph::<Doc>::new()
        .add_inline_node("A", did("a"))
        .set_timeout("A", Duration::from_secs(1))
        .compile();
    assert!(
        matches!(&plain, Err(StrataError::CompilationFailed(m)) if m == "node 'A' runs inline and can't be timed out: add it with add_node or add_fallible_node to have its timeout honoured")
    );

    let fallible = StateGraph::<Doc>::new()
        .add_inline_fallible_node("B", |doc: &Doc| Ok(doc.clone()))
        .set_timeout("B", Duration::from_secs(1))
        .compile();
    assert!(
        matches!(&fallible, Err(StrataError::CompilationFailed(m)) if m.starts_with("node 'B' runs inline"))
    );

    // Re-adding the node off the executor makes the timeout good again.
    let readded = StateGraph::<Doc>::new()
        .add_inline_node("A", did("a"))
        .add_node("A", did("a"))
        .set_timeout("A", Duration::from_secs(1))
        .compile();
    assert!(readded.is_ok());
}

#[tokio::test]
async fn a_cancellation_during_an_inline_node_takes_effect_when_it_returns() {
    let store = store();
    let token = CancellationToken::new();
    let cancel = token.clone();
    let graph = StateGraph::<Doc>::new()
        .add_inline_node("Prepare", move |doc: Doc| {
            // The run's token fires while the handler runs; nothing can stop it.
            cancel.cancel();
            did("prepare")(doc)
        })
        .add_inline_node("Finish", did("finish"))
        .add_edge("Prepare", "Finish")
        .compile()
        .unwrap();

    let result = graph
        .run_with(
            "t",
            doc(),
            store.clone(),
            RunConfig::new().with_cancellation(token),
        )
        .await;

    let GraphExecutionResult::Cancelled {
        node,
        reason,
        state,
        ..
    } = result
    else {
        panic!("expected Cancelled");
    };
    // Its result is committed, and the run stops before the next node.
    assert_eq!(
        (node.as_str(), reason, state.steps),
        (
            "Finish",
            CancelReason::Requested,
            vec!["prepare".to_string()]
        )
    );
    let checkpoint = store.latest("t").unwrap();
    assert_eq!(checkpoint.status, CheckpointStatus::Cancelled);
    assert_eq!(checkpoint.suspended_node.as_deref(), Some("Finish"));
    assert_eq!(
        checkpoint.frozen_state,
        serde_json::json!({ "steps": ["prepare"] })
    );
}

#[test]
fn an_inline_node_exports_as_the_node_it_is() {
    let pooled = StateGraph::<Doc>::new()
        .add_node("A", did("a"))
        .add_fallible_node("B", |doc: &Doc| Ok(doc.clone()))
        .add_edge("A", "B")
        .to_definition();
    let inline = StateGraph::<Doc>::new()
        .add_inline_node("A", did("a"))
        .add_inline_fallible_node("B", |doc: &Doc| Ok(doc.clone()))
        .add_edge("A", "B")
        .to_definition();

    assert_eq!(
        serde_json::to_value(&inline).unwrap(),
        serde_json::to_value(&pooled).unwrap()
    );
}
