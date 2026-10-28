//! What an async handler's `NodeContext` says, at every depth
//! (`design/17-Async-Execution.md`, SG-02). The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/NodeContextTests.cs` — keep the two in
//! step.

use std::collections::HashMap;
use std::future::{Ready, ready};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use datalium_strata::BoxFuture;

use datalium_strata::checkpoint::{CheckpointStore, CheckpointStoreError, GraphExecutionResult};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::history::{ChatMessage, HistoryStore, HistoryStoreError};
use datalium_strata::parallel::ParallelBranches;
use datalium_strata::schema::Checkpoint;
use serde::{Deserialize, Serialize};

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

impl TestStore {
    fn save(&self, checkpoint: Checkpoint) -> Result<(), CheckpointStoreError> {
        self.checkpoints
            .lock()
            .unwrap()
            .insert(checkpoint.thread_id.clone(), checkpoint);
        Ok(())
    }

    fn load(&self, thread_id: &str) -> Result<Option<Checkpoint>, CheckpointStoreError> {
        Ok(self.checkpoints.lock().unwrap().get(thread_id).cloned())
    }
}

impl CheckpointStore for TestStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        Box::pin(std::future::ready(TestStore::save(self, checkpoint)))
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        Box::pin(std::future::ready(TestStore::load(self, thread_id)))
    }
}

/// A history store nothing reads; only its identity matters here.
struct TestHistory;

impl HistoryStore for TestHistory {
    fn append<'a>(
        &'a self,
        _: &'a str,
        _: ChatMessage,
    ) -> BoxFuture<'a, Result<(), HistoryStoreError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn get_history<'a>(
        &'a self,
        _: &'a str,
        _: usize,
    ) -> BoxFuture<'a, Result<Vec<ChatMessage>, HistoryStoreError>> {
        Box::pin(std::future::ready(Ok(Vec::new())))
    }
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

type Seen = Arc<Mutex<Vec<NodeContext>>>;

/// A node handler that records the context it was given and passes the state on.
fn record(seen: &Seen) -> impl Fn(Doc, NodeContext) -> Ready<Doc> + Send + Sync + 'static {
    let seen = seen.clone();
    move |doc, ctx| {
        seen.lock().unwrap().push(ctx);
        ready(doc)
    }
}

/// [`record`] for a branch.
fn record_branch(
    seen: &Seen,
) -> impl Fn(Doc, NodeContext) -> Ready<Result<Doc, NodeError>> + Send + Sync + 'static {
    let seen = seen.clone();
    move |doc, ctx| {
        seen.lock().unwrap().push(ctx);
        ready(Ok(doc))
    }
}

fn store() -> Arc<dyn CheckpointStore> {
    Arc::new(TestStore::default())
}

#[tokio::test]
async fn a_node_sees_the_thread_its_own_name_and_path() {
    let seen = Seen::default();
    let graph = StateGraph::<Doc>::new()
        .add_async_node("Load", record(&seen))
        .compile()
        .unwrap();

    completed(graph.run("thread_ctx", doc(), store()).await);

    let seen = seen.lock().unwrap();
    let [ctx] = seen.as_slice() else {
        panic!("expected one context, got {}", seen.len())
    };
    assert_eq!(
        (ctx.thread_id(), ctx.node(), ctx.node_path(), ctx.attempt()),
        ("thread_ctx", "Load", "Load", 1)
    );
}

#[tokio::test]
async fn the_attempt_counts_retries_from_one() {
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let recorded = attempts.clone();
    let graph = StateGraph::<Doc>::new()
        .add_async_fallible_node("Charge", move |doc: Doc, ctx| {
            recorded.lock().unwrap().push(ctx.attempt());
            ready(if ctx.attempt() < 3 {
                Err(NodeError::Transient("timeout".to_string()))
            } else {
                Ok(doc)
            })
        })
        .set_recovery_policy("Charge", RecoveryPolicy::retry(2))
        .compile()
        .unwrap();

    completed(graph.run("t", doc(), store()).await);

    assert_eq!(*attempts.lock().unwrap(), vec![1, 2, 3]);
}

#[tokio::test]
async fn paths_qualify_subgraph_nodes_and_branches_and_the_thread_is_the_outermost() {
    let seen = Seen::default();
    let child = StateGraph::<Doc>::new()
        .add_async_node("Draft", record(&seen))
        .add_parallel_group(
            "Gather",
            ParallelBranches::new().async_branch("Acme", record_branch(&seen)),
            |entry: &Doc, _| entry.clone(),
        )
        .add_edge("Draft", "Gather")
        .compile()
        .unwrap();
    let graph = StateGraph::<Doc>::new()
        .add_async_node("Prepare", record(&seen))
        .add_subgraph(
            "Work",
            child,
            |doc: &Doc| doc.clone(),
            |_: &Doc, result| result,
        )
        .add_parallel_group(
            "Quotes",
            ParallelBranches::new().async_branch("Globex", record_branch(&seen)),
            |entry: &Doc, _| entry.clone(),
        )
        .add_edge("Prepare", "Work")
        .add_edge("Work", "Quotes")
        .compile()
        .unwrap();

    completed(graph.run("thread_outer", doc(), store()).await);

    let seen = seen.lock().unwrap();
    let names: Vec<(&str, &str)> = seen.iter().map(|c| (c.node(), c.node_path())).collect();
    assert_eq!(
        names,
        vec![
            ("Prepare", "Prepare"),
            ("Draft", "Work/Draft"),
            ("Acme", "Work/Gather/Acme"),
            ("Globex", "Quotes/Globex"),
        ]
    );
    assert!(seen.iter().all(|c| c.thread_id() == "thread_outer"));
}

#[tokio::test]
async fn a_branch_reports_its_groups_attempt() {
    let branch_attempts = Arc::new(Mutex::new(Vec::new()));
    let recorded = branch_attempts.clone();
    let group_attempts = Arc::new(AtomicU32::new(0));
    let counted = group_attempts.clone();
    let branches = ParallelBranches::<Doc>::new()
        .async_branch("Acme", move |doc: Doc, ctx| {
            recorded.lock().unwrap().push(ctx.attempt());
            ready(Ok(doc))
        })
        .branch("Flaky", move |doc: &Doc| {
            if counted.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(NodeError::Transient("timeout".to_string()))
            } else {
                Ok(doc.clone())
            }
        });
    let graph = StateGraph::<Doc>::new()
        .add_parallel_group("Gather", branches, |entry: &Doc, _| entry.clone())
        .set_recovery_policy("Gather", RecoveryPolicy::retry(1))
        .compile()
        .unwrap();

    completed(graph.run("t", doc(), store()).await);

    // The first attempt's Acme may have been cancelled before it ran; the second always runs.
    let attempts = branch_attempts.lock().unwrap();
    assert!(attempts.contains(&2), "{attempts:?}");
    assert!(attempts.iter().all(|a| *a == 1 || *a == 2), "{attempts:?}");
}

#[tokio::test]
async fn history_and_services_come_from_run_config_at_every_depth() {
    struct Greeter(&'static str);

    let history: Arc<dyn HistoryStore> = Arc::new(TestHistory);
    let seen = Seen::default();
    let child = StateGraph::<Doc>::new()
        .add_async_node("Inner", record(&seen))
        .compile()
        .unwrap();
    let graph = StateGraph::<Doc>::new()
        .add_async_node("Outer", record(&seen))
        .add_subgraph(
            "Work",
            child,
            |doc: &Doc| doc.clone(),
            |_: &Doc, result| result,
        )
        .add_edge("Outer", "Work")
        .compile()
        .unwrap();
    let config = RunConfig::new()
        .with_history(history.clone())
        .with_service(Arc::new(Greeter("hello")));

    completed(graph.run_with("t", doc(), store(), config).await);

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    for ctx in seen.iter() {
        assert!(Arc::ptr_eq(&ctx.history().unwrap(), &history));
        assert_eq!(ctx.service::<Greeter>().unwrap().0, "hello");
        let execution = ctx.execution_context().unwrap();
        assert_eq!(execution.thread_id, "t");
    }
}

#[tokio::test]
async fn without_a_history_store_there_is_no_execution_context() {
    let seen = Seen::default();
    let graph = StateGraph::<Doc>::new()
        .add_async_node("Load", record(&seen))
        .compile()
        .unwrap();

    completed(graph.run("t", doc(), store()).await);

    let seen = seen.lock().unwrap();
    assert!(seen[0].history().is_none());
    assert!(seen[0].service::<String>().is_none());
    assert!(seen[0].execution_context().is_none());
}

#[tokio::test]
async fn the_stream_is_present_exactly_when_the_run_streams() {
    let seen = Seen::default();
    let graph = Arc::new(
        StateGraph::<Doc>::new()
            .add_async_node("Load", record(&seen))
            .compile()
            .unwrap(),
    );

    completed(graph.run("t", doc(), store()).await);
    let (handle, mut receiver) = graph.clone().run_streaming("t", doc(), store());
    while receiver.recv().await.is_some() {}
    completed(handle.await.unwrap());

    let streams: Vec<bool> = seen
        .lock()
        .unwrap()
        .iter()
        .map(|c| c.stream().is_some())
        .collect();
    assert_eq!(streams, vec![false, true]);
}

/// A context makes its idempotency key the first time it is asked for
/// (`design/38-Rust-Executor-Overhead.md`), which may be after its run has
/// ended: it is the key its node had, whatever the context is told later.
#[tokio::test]
async fn the_idempotency_key_is_the_nodes_even_when_it_is_asked_for_late() {
    let seen = Seen::default();
    let graph = StateGraph::<Doc>::new()
        .add_async_node("Load", record(&seen))
        .add_parallel_group(
            "Fan",
            ParallelBranches::new().async_branch("b1", record_branch(&seen)),
            |entry: &Doc, _results: Vec<Doc>| entry.clone(),
        )
        .add_edge("Load", "Fan")
        .compile()
        .unwrap();

    let config = RunConfig::new().with_run_id("run-1");
    completed(graph.run_with("t", doc(), store(), config).await);

    let seen = seen.lock().unwrap();
    let keys: Vec<&str> = seen.iter().map(NodeContext::idempotency_key).collect();
    assert_eq!(keys, ["t/Load#1@run-1", "t/Fan/b1#2@run-1"]);

    let renamed = seen[0]
        .clone()
        .with_node_path("Elsewhere")
        .with_run_id("run-2");
    assert_eq!(renamed.idempotency_key(), "t/Load#1@run-1");
}

#[test]
fn a_handler_can_be_unit_tested_with_a_context_built_by_hand() {
    let ctx = NodeContext::new("t", "Draft").with_attempt(2);

    assert_eq!((ctx.node_path(), ctx.attempt()), ("Draft", 2));
    assert!(ctx.stream().is_none());
    assert!(!ctx.cancellation().is_cancelled());
}
