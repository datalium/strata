//! Async node handlers (`design/17-Async-Execution.md`, SG-01): every handler
//! kind has an async form that is awaited, not blocked on; sync and async
//! handlers mix freely; async branches don't hold a thread each. The .NET twin
//! is `dotnet/tests/Datalium.Strata.Tests/AsyncExecutionTests.cs` — keep the
//! two in step.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::BoxFuture;

use datalium_strata::StrataError;
use datalium_strata::checkpoint::{
    CheckpointStore, CheckpointStoreError, GraphExecutionResult, HumanSignal,
};
use datalium_strata::graph::{NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::parallel::ParallelBranches;
use datalium_strata::schema::{Checkpoint, CheckpointStatus, SCHEMA_VERSION};
use datalium_strata::streaming::{StreamChunk, StreamChunkType};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Doc {
    text: String,
    steps: Vec<String>,
}

fn doc() -> Doc {
    Doc {
        text: String::new(),
        steps: Vec::new(),
    }
}

fn did(mut doc: Doc, step: &str) -> Doc {
    doc.steps.push(step.to_string());
    doc
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

async fn drain(mut receiver: tokio::sync::mpsc::Receiver<StreamChunk>) -> Vec<StreamChunk> {
    let mut chunks = Vec::new();
    while let Some(chunk) = receiver.recv().await {
        chunks.push(chunk);
    }
    chunks
}

#[tokio::test]
async fn async_plain_and_fallible_nodes_are_awaited_and_mix_with_sync_ones() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let counted = attempts.clone();
    let graph = StateGraph::<Doc>::new()
        .add_async_node("Load", |doc: Doc, _| async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            did(doc, "load")
        })
        .add_node("Sync", |doc: Doc| did(doc, "sync"))
        .add_async_fallible_node("Charge", move |doc: Doc, _| {
            let counted = counted.clone();
            async move {
                tokio::task::yield_now().await;
                if counted.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(NodeError::Transient("gateway timeout".to_string()))
                } else {
                    Ok(did(doc, "charge"))
                }
            }
        })
        .set_recovery_policy("Charge", RecoveryPolicy::retry(1))
        .add_edge("Load", "Sync")
        .add_edge("Sync", "Charge")
        .compile()
        .unwrap();

    let doc = completed(graph.run("t", doc(), Arc::new(TestStore::default())).await);

    assert_eq!(doc.steps, vec!["load", "sync", "charge"]);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn an_async_fallible_node_that_gives_up_persists_a_failed_checkpoint() {
    let graph = StateGraph::<Doc>::new()
        .add_node("Load", |doc: Doc| did(doc, "load"))
        .add_async_fallible_node("Charge", |_: Doc, _| async {
            tokio::task::yield_now().await;
            Err(NodeError::Fatal("card declined".to_string()))
        })
        .add_edge("Load", "Charge")
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());

    match graph.run("t", doc(), store.clone()).await {
        GraphExecutionResult::Failed(StrataError::NodeFailed {
            node,
            attempts,
            message,
        }) => assert_eq!(
            (node.as_str(), attempts, message.as_str()),
            ("Charge", 1, "card declined")
        ),
        _ => panic!("expected Failed(NodeFailed)"),
    }
    let checkpoint = store.load("t").unwrap().unwrap();
    assert_eq!(checkpoint.status, CheckpointStatus::Failed);
    assert_eq!(checkpoint.schema_version, SCHEMA_VERSION);
    assert_eq!(checkpoint.metadata.unwrap()["error_kind"], "fatal");
}

#[tokio::test]
async fn an_async_streaming_node_emits_through_its_context() {
    let graph = Arc::new(
        StateGraph::<Doc>::new()
            .add_async_streaming_node("Draft", |mut doc: Doc, ctx| async move {
                let stream = ctx.stream().expect("a streaming node always has a stream");
                for word in ["hello", " ", "world"] {
                    stream.emit_token(word).await.unwrap();
                }
                doc.text = "hello world".to_string();
                doc
            })
            .compile()
            .unwrap(),
    );

    let (handle, receiver) = graph.run_streaming("t", doc(), Arc::new(TestStore::default()));
    let chunks = drain(receiver).await;

    assert_eq!(completed(handle.await.unwrap()).text, "hello world");
    let shape: Vec<(StreamChunkType, String, String)> = chunks
        .iter()
        .map(|c| (c.chunk_type, c.node_name.to_string(), c.content.to_string()))
        .collect();
    let expect = |t, content: &str| (t, "Draft".to_string(), content.to_string());
    assert_eq!(
        shape,
        vec![
            expect(StreamChunkType::NodeStart, "Draft"),
            expect(StreamChunkType::Token, "hello"),
            expect(StreamChunkType::Token, " "),
            expect(StreamChunkType::Token, "world"),
            expect(StreamChunkType::NodeEnd, "Draft"),
        ]
    );
}

#[tokio::test]
async fn a_plain_run_refuses_an_async_streaming_node_before_running_it() {
    let ran = Arc::new(AtomicBool::new(false));
    let flag = ran.clone();
    let graph = StateGraph::<Doc>::new()
        .add_async_streaming_node("Draft", move |doc: Doc, _| {
            flag.store(true, Ordering::SeqCst);
            async move { doc }
        })
        .compile()
        .unwrap();

    match graph.run("t", doc(), Arc::new(TestStore::default())).await {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
            assert!(message.contains("is a streaming node"), "{message}");
        }
        _ => panic!("expected Failed(ExecutionAborted)"),
    }
    assert!(!ran.load(Ordering::SeqCst));
}

#[tokio::test]
async fn sync_and_async_branches_mix_in_one_group_and_merge_in_declaration_order() {
    let branches = ParallelBranches::<Doc>::new()
        .async_branch("slow-async", |doc: Doc, _| async move {
            tokio::time::sleep(Duration::from_millis(120)).await;
            Ok(did(doc, "slow-async"))
        })
        .branch("sync", |doc: &Doc| Ok(did(doc.clone(), "sync")))
        .async_branch("fast-async", |doc: Doc, _| async move {
            Ok(did(doc, "fast-async"))
        });
    let graph = StateGraph::<Doc>::new()
        .add_parallel_group("Gather", branches, |entry: &Doc, outputs: Vec<Doc>| Doc {
            steps: outputs.into_iter().flat_map(|o| o.steps).collect(),
            ..entry.clone()
        })
        .compile()
        .unwrap();

    let doc = completed(graph.run("t", doc(), Arc::new(TestStore::default())).await);

    assert_eq!(doc.steps, vec!["slow-async", "sync", "fast-async"]);
}

/// Two runtime workers and 1000 branches all waiting at once: they can only
/// all be in flight if none of them holds a thread.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_branches_do_not_hold_a_thread_each() {
    const RUNS: usize = 200;
    const BRANCHES_PER_RUN: usize = 5;
    let (gate_tx, gate_rx) = tokio::sync::watch::channel(false);
    let waiting = Arc::new(AtomicUsize::new(0));
    let all_waiting = Arc::new(tokio::sync::Notify::new());

    let mut branches = ParallelBranches::<Doc>::new();
    for i in 0..BRANCHES_PER_RUN {
        let (gate_rx, waiting, all_waiting) =
            (gate_rx.clone(), waiting.clone(), all_waiting.clone());
        branches = branches.async_branch(format!("b{i}"), move |doc: Doc, _| {
            let (mut gate_rx, waiting, all_waiting) =
                (gate_rx.clone(), waiting.clone(), all_waiting.clone());
            async move {
                if waiting.fetch_add(1, Ordering::SeqCst) + 1 == RUNS * BRANCHES_PER_RUN {
                    all_waiting.notify_one();
                }
                gate_rx.wait_for(|open| *open).await.unwrap();
                Ok(doc)
            }
        });
    }
    let graph = Arc::new(
        StateGraph::<Doc>::new()
            .add_parallel_group("Gather", branches, |entry: &Doc, _| entry.clone())
            .compile()
            .unwrap(),
    );
    let store: Arc<dyn CheckpointStore> = Arc::new(TestStore::default());
    let runs: Vec<_> = (0..RUNS)
        .map(|i| {
            let (graph, store) = (graph.clone(), store.clone());
            tokio::spawn(async move { graph.run(format!("t{i}"), doc(), store).await })
        })
        .collect();

    tokio::time::timeout(Duration::from_secs(30), all_waiting.notified())
        .await
        .expect("every branch of every run should be in flight at once");

    gate_tx.send(true).unwrap();
    for run in runs {
        completed(run.await.unwrap());
    }
}

#[tokio::test]
async fn an_async_streaming_branch_streams_as_group_slash_branch() {
    let branches =
        ParallelBranches::<Doc>::new().async_streaming_branch("Acme", |doc: Doc, ctx| async move {
            ctx.stream().unwrap().emit_token("quote").await.unwrap();
            Ok(doc)
        });
    let graph = Arc::new(
        StateGraph::<Doc>::new()
            .add_parallel_group("Gather", branches, |entry: &Doc, _| entry.clone())
            .compile()
            .unwrap(),
    );

    let (handle, receiver) = graph.run_streaming("t", doc(), Arc::new(TestStore::default()));
    let tokens: Vec<(String, String)> = drain(receiver)
        .await
        .into_iter()
        .filter(|c| c.chunk_type == StreamChunkType::Token)
        .map(|c| (c.node_name.to_string(), c.content.to_string()))
        .collect();

    completed(handle.await.unwrap());
    assert_eq!(
        tokens,
        vec![("Gather/Acme".to_string(), "quote".to_string())]
    );
}

#[tokio::test]
async fn a_panic_in_an_async_node_unwinds_through_the_run() {
    let graph = StateGraph::<Doc>::new()
        .add_async_node("Load", |_: Doc, _| async {
            tokio::task::yield_now().await;
            panic!("node bug")
        })
        .compile()
        .unwrap();

    let outcome =
        tokio::spawn(async move { graph.run("t", doc(), Arc::new(TestStore::default())).await })
            .await;

    assert!(outcome.is_err_and(|error| error.is_panic()));
}

#[tokio::test]
async fn dropping_the_run_future_is_not_a_cancellation_and_persists_nothing() {
    let graph = StateGraph::<Doc>::new()
        .add_async_node("Slow", |doc: Doc, _| async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            doc
        })
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());

    let outcome = tokio::time::timeout(
        Duration::from_millis(50),
        graph.run("t", doc(), store.clone()),
    )
    .await;

    assert!(outcome.is_err());
    assert!(store.load("t").unwrap().is_none());
}

#[tokio::test]
async fn async_nodes_run_inside_subgraphs_at_any_depth() {
    let grandchild = StateGraph::<Doc>::new()
        .add_async_node("Deep", |doc: Doc, _| async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            did(doc, "deep")
        })
        .compile()
        .unwrap();
    let child = StateGraph::<Doc>::new()
        .add_async_node("Before", |doc: Doc, _| async move {
            tokio::task::yield_now().await;
            did(doc, "before")
        })
        .add_subgraph(
            "Inner",
            grandchild,
            |doc: &Doc| doc.clone(),
            |_: &Doc, result| result,
        )
        .add_edge("Before", "Inner")
        .compile()
        .unwrap();
    let graph = StateGraph::<Doc>::new()
        .add_subgraph(
            "Work",
            child,
            |doc: &Doc| doc.clone(),
            |_: &Doc, result| result,
        )
        .compile()
        .unwrap();

    let doc = completed(graph.run("t", doc(), Arc::new(TestStore::default())).await);

    assert_eq!(doc.steps, vec!["before", "deep"]);
}

#[tokio::test]
async fn an_interrupt_boundary_on_an_async_node_freezes_and_resumes() {
    let graph = StateGraph::<Doc>::new()
        .add_async_node("Draft", |doc: Doc, _| async move { did(doc, "draft") })
        .add_interrupt_boundary("Draft")
        .add_async_node("Send", |doc: Doc, _| async move { did(doc, "send") })
        .add_edge("Draft", "Send")
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());

    assert!(matches!(
        graph.run("t", doc(), store.clone()).await,
        GraphExecutionResult::Interrupted { .. }
    ));
    let doc = completed(
        graph
            .resume("t", HumanSignal::default(), store.clone())
            .await,
    );

    assert_eq!(doc.steps, vec!["draft", "send"]);
}
