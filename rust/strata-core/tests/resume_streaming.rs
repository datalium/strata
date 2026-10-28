//! Streaming a resume (`resume_streaming`): the resumed half of an interrupted
//! run streams like a fresh `run_streaming`, for a flat graph and through
//! nested subgraphs. The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/ResumeStreamingTests.cs` — keep the two in step.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

use datalium_strata::BoxFuture;

use datalium_strata::StrataError;
use datalium_strata::checkpoint::{
    CheckpointStore, CheckpointStoreError, GraphExecutionResult, HumanSignal,
};
use datalium_strata::graph::{CompiledGraph, NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::parallel::ParallelBranches;
use datalium_strata::schema::Checkpoint;
use datalium_strata::streaming::{StreamChunk, StreamChunkType, StreamContext};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Job {
    log: Vec<String>,
    who: String,
}

fn job() -> Job {
    Job {
        log: Vec::new(),
        who: String::new(),
    }
}

fn concat(base: &Job, outputs: Vec<Job>) -> Job {
    let mut merged = base.clone();
    for output in outputs {
        merged.log.extend(output.log);
    }
    merged
}

fn note(label: &'static str) -> impl Fn(Job) -> Job + Send + Sync + 'static {
    move |mut job| {
        job.log.push(label.to_string());
        job
    }
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

/// A streaming node that emits each of `tokens`, then records `label`.
fn talk(
    label: &'static str,
    tokens: &'static [&'static str],
) -> impl Fn(Job, &StreamContext) -> Job + Send + Sync + 'static {
    move |mut job, ctx| {
        for token in tokens {
            ctx.emit_token_blocking(token).unwrap();
        }
        job.log.push(label.to_string());
        job
    }
}

fn store() -> Arc<dyn CheckpointStore> {
    Arc::new(TestStore::default())
}

async fn drain(mut receiver: tokio::sync::mpsc::Receiver<StreamChunk>) -> Vec<StreamChunk> {
    let mut chunks = Vec::new();
    while let Some(chunk) = receiver.recv().await {
        chunks.push(chunk);
    }
    chunks
}

/// Runs `graph` under `run_streaming` up to its first interrupt.
async fn freeze(graph: &Arc<CompiledGraph<Job>>, store: &Arc<dyn CheckpointStore>) {
    let (handle, receiver) = graph.clone().run_streaming("t", job(), store.clone());
    drain(receiver).await;
    assert!(
        matches!(
            handle.await.unwrap(),
            GraphExecutionResult::Interrupted { .. }
        ),
        "expected the run to freeze"
    );
}

fn approve() -> HumanSignal {
    HumanSignal {
        is_approved: true,
        reviewer_token: "r".to_string(),
        field_overrides: HashMap::new(),
        ..Default::default()
    }
}

/// Resumes `t` under `resume_streaming` and collects every chunk.
async fn resume(
    graph: &Arc<CompiledGraph<Job>>,
    store: &Arc<dyn CheckpointStore>,
    signal: HumanSignal,
) -> (Vec<StreamChunk>, GraphExecutionResult<Job>) {
    let (handle, receiver) = graph.clone().resume_streaming("t", signal, store.clone());
    (drain(receiver).await, handle.await.unwrap())
}

fn completed(result: GraphExecutionResult<Job>) -> Job {
    match result {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(err) => panic!("expected Completed, got Failed({err:?})"),
        GraphExecutionResult::Interrupted { .. } => panic!("expected Completed, got Interrupted"),
        GraphExecutionResult::Cancelled { node, .. } => {
            panic!("expected Completed, got Cancelled at {node}")
        }
    }
}

fn shape(chunks: &[StreamChunk]) -> Vec<(StreamChunkType, String, String)> {
    chunks
        .iter()
        .map(|c| (c.chunk_type, c.node_name.to_string(), c.content.to_string()))
        .collect()
}

fn shape_under(chunks: &[StreamChunk], node: &str) -> Vec<(StreamChunkType, String, String)> {
    let prefix = format!("{node}/");
    shape(chunks)
        .into_iter()
        .filter(|(_, name, _)| name == node || name.starts_with(&prefix))
        .collect()
}

fn tokens_from(chunks: &[StreamChunk], node: &str) -> Vec<String> {
    chunks
        .iter()
        .filter(|c| c.chunk_type == StreamChunkType::Token && &*c.node_name == node)
        .map(|c| c.content.to_string())
        .collect()
}

fn item(kind: StreamChunkType, node: &str, content: &str) -> (StreamChunkType, String, String) {
    (kind, node.to_string(), content.to_string())
}

// --- A flat graph ----------------------------------------------------------

/// `Start` -> `Review` (boundary) -> `Talk` (streams) -> `Finish`.
fn flat_graph() -> Arc<CompiledGraph<Job>> {
    Arc::new(
        StateGraph::<Job>::new()
            .add_node("Start", note("start"))
            .add_node("Review", note("review"))
            .add_streaming_node("Talk", talk("talk", &["Hel", "lo"]))
            .add_node("Finish", note("finish"))
            .add_edge("Start", "Review")
            .add_edge("Review", "Talk")
            .add_edge("Talk", "Finish")
            .add_interrupt_boundary("Review")
            .compile()
            .unwrap(),
    )
}

#[tokio::test]
async fn a_streaming_node_after_the_boundary_streams_on_resume() {
    let graph = flat_graph();
    let store = store();
    freeze(&graph, &store).await;

    let (chunks, result) = resume(&graph, &store, approve()).await;

    assert_eq!(
        completed(result).log,
        vec!["start", "review", "talk", "finish"]
    );
    // The stream starts at the node resumed into; `Start` and `Review` ran
    // before the freeze and are not repeated.
    assert_eq!(
        shape(&chunks),
        vec![
            item(StreamChunkType::NodeStart, "Talk", "Talk"),
            item(StreamChunkType::Token, "Talk", "Hel"),
            item(StreamChunkType::Token, "Talk", "lo"),
            item(StreamChunkType::NodeEnd, "Talk", "Talk"),
            item(StreamChunkType::NodeStart, "Finish", "Finish"),
            item(StreamChunkType::NodeEnd, "Finish", "Finish"),
        ]
    );
}

#[tokio::test]
async fn the_humans_field_overrides_are_applied_before_the_streaming_node_runs() {
    let graph = Arc::new(
        StateGraph::<Job>::new()
            .add_node("Review", note("review"))
            .add_streaming_node("Talk", |job: Job, ctx: &StreamContext| {
                ctx.emit_token_blocking(&job.who).unwrap();
                job
            })
            .add_edge("Review", "Talk")
            .add_interrupt_boundary("Review")
            .compile()
            .unwrap(),
    );
    let store = store();
    freeze(&graph, &store).await;
    let signal = HumanSignal {
        field_overrides: HashMap::from([("who".to_string(), "alice".to_string())]),
        ..approve()
    };

    let (chunks, result) = resume(&graph, &store, signal).await;

    assert_eq!(completed(result).who, "alice");
    assert_eq!(tokens_from(&chunks, "Talk"), vec!["alice"]);
}

#[tokio::test]
async fn resuming_a_thread_with_no_checkpoint_fails_and_the_stream_ends_empty() {
    let graph = flat_graph();

    let (chunks, result) = resume(&graph, &store(), approve()).await;

    assert!(chunks.is_empty());
    match result {
        GraphExecutionResult::Failed(StrataError::NodeNotFound(message)) => {
            assert!(message.contains("no checkpoint found"), "{message}");
        }
        _ => panic!("expected Failed(NodeNotFound)"),
    }
}

#[tokio::test]
async fn a_checkpoint_can_only_be_resumed_once() {
    let graph = flat_graph();
    let store = store();
    freeze(&graph, &store).await;
    completed(resume(&graph, &store, approve()).await.1);

    let (chunks, result) = resume(&graph, &store, approve()).await;

    assert!(chunks.is_empty());
    match result {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
            assert!(message.contains("already resumed"), "{message}");
        }
        _ => panic!("expected Failed(ExecutionAborted)"),
    }
}

#[tokio::test]
async fn a_streamed_resume_that_reaches_a_second_boundary_freezes_and_can_be_resumed_again() {
    // `Review` -> `Draft` (streams, boundary) -> `Publish` (streams).
    let graph = Arc::new(
        StateGraph::<Job>::new()
            .add_node("Review", note("review"))
            .add_streaming_node("Draft", talk("draft", &["v1"]))
            .add_streaming_node("Publish", talk("publish", &["final"]))
            .add_edge("Review", "Draft")
            .add_edge("Draft", "Publish")
            .add_interrupt_boundary("Review")
            .add_interrupt_boundary("Draft")
            .compile()
            .unwrap(),
    );
    let store = store();
    freeze(&graph, &store).await;

    let (first, result) = resume(&graph, &store, approve()).await;
    match result {
        GraphExecutionResult::Interrupted { suspended_node, .. } => {
            assert_eq!(suspended_node, "Draft");
        }
        _ => panic!("expected Interrupted"),
    }
    assert_eq!(
        shape(&first),
        vec![
            item(StreamChunkType::NodeStart, "Draft", "Draft"),
            item(StreamChunkType::Token, "Draft", "v1"),
            item(StreamChunkType::NodeEnd, "Draft", "Draft"),
        ]
    );

    let (second, result) = resume(&graph, &store, approve()).await;
    assert_eq!(completed(result).log, vec!["review", "draft", "publish"]);
    assert_eq!(tokens_from(&second, "Publish"), vec!["final"]);
}

#[tokio::test]
async fn a_resumed_streaming_node_past_the_channel_bound_loses_no_tokens() {
    // 3000 tokens is well past the 1024-chunk channel and the consumer holds
    // off reading, so the node is blocked in a send. Backpressure must pause
    // it, not drop tokens.
    const TOKENS: usize = 3000;
    let graph = Arc::new(
        StateGraph::<Job>::new()
            .add_node("Review", note("review"))
            .add_streaming_node("Talk", |job: Job, ctx: &StreamContext| {
                for i in 0..TOKENS {
                    ctx.emit_token_blocking(&i.to_string()).unwrap();
                }
                job
            })
            .add_edge("Review", "Talk")
            .add_interrupt_boundary("Review")
            .compile()
            .unwrap(),
    );
    let store = store();
    freeze(&graph, &store).await;

    let (handle, receiver) = graph
        .clone()
        .resume_streaming("t", approve(), store.clone());
    sleep(Duration::from_millis(200));
    let chunks = drain(receiver).await;

    completed(handle.await.unwrap());
    let expected: Vec<String> = (0..TOKENS).map(|i| i.to_string()).collect();
    assert_eq!(tokens_from(&chunks, "Talk"), expected);
}

// --- Through a subgraph ----------------------------------------------------

/// `Before` -> `Work` (a subgraph running `child`) -> `Finish`.
fn parent_around(
    child: CompiledGraph<Job>,
    policy: Option<RecoveryPolicy>,
) -> Arc<CompiledGraph<Job>> {
    let mut graph = StateGraph::<Job>::new()
        .add_node("Before", note("before"))
        .add_subgraph("Work", child, Clone::clone, |_: &Job, child: Job| child)
        .add_node("Finish", note("finish"))
        .add_edge("Before", "Work")
        .add_edge("Work", "Finish");
    if let Some(policy) = policy {
        graph = graph.set_recovery_policy("Work", policy);
    }
    Arc::new(graph.compile().unwrap())
}

/// `Review` (boundary) -> `Talk` (streams).
fn reviewed_child() -> CompiledGraph<Job> {
    StateGraph::<Job>::new()
        .add_node("Review", note("review"))
        .add_streaming_node("Talk", talk("talk", &["Hel", "lo"]))
        .add_edge("Review", "Talk")
        .add_interrupt_boundary("Review")
        .compile()
        .unwrap()
}

#[tokio::test]
async fn a_child_streaming_node_after_its_boundary_streams_on_resume_named_under_the_subgraph() {
    let graph = parent_around(reviewed_child(), None);
    let store = store();
    freeze(&graph, &store).await;

    let (chunks, result) = resume(&graph, &store, approve()).await;

    assert_eq!(
        completed(result).log,
        vec!["before", "review", "talk", "finish"]
    );
    assert_eq!(
        shape(&chunks),
        vec![
            // The subgraph node starts again: its first NodeStart, from the
            // run that froze, never got a NodeEnd.
            item(StreamChunkType::NodeStart, "Work", "Work"),
            // The child resumes at `Talk`; `Review` is not repeated.
            item(StreamChunkType::NodeStart, "Work/Talk", "Talk"),
            item(StreamChunkType::Token, "Work/Talk", "Hel"),
            item(StreamChunkType::Token, "Work/Talk", "lo"),
            item(StreamChunkType::NodeEnd, "Work/Talk", "Talk"),
            item(StreamChunkType::NodeEnd, "Work", "Work"),
            item(StreamChunkType::NodeStart, "Finish", "Finish"),
            item(StreamChunkType::NodeEnd, "Finish", "Finish"),
        ]
    );
}

#[tokio::test]
async fn names_nest_through_a_grandchild_on_resume() {
    let child = StateGraph::<Job>::new()
        .add_subgraph("Inner", reviewed_child(), Clone::clone, |_: &Job, c: Job| c)
        .compile()
        .unwrap();
    let graph = parent_around(child, None);
    let store = store();
    freeze(&graph, &store).await;

    let (chunks, result) = resume(&graph, &store, approve()).await;

    completed(result);
    assert_eq!(tokens_from(&chunks, "Work/Inner/Talk"), vec!["Hel", "lo"]);
    assert_eq!(
        shape_under(&chunks, "Work")
            .into_iter()
            .map(|(kind, node, _)| (kind, node))
            .collect::<Vec<_>>(),
        vec![
            (StreamChunkType::NodeStart, "Work".to_string()),
            (StreamChunkType::NodeStart, "Work/Inner".to_string()),
            (StreamChunkType::NodeStart, "Work/Inner/Talk".to_string()),
            (StreamChunkType::Token, "Work/Inner/Talk".to_string()),
            (StreamChunkType::Token, "Work/Inner/Talk".to_string()),
            (StreamChunkType::NodeEnd, "Work/Inner/Talk".to_string()),
            (StreamChunkType::NodeEnd, "Work/Inner".to_string()),
            (StreamChunkType::NodeEnd, "Work".to_string()),
        ]
    );
}

#[tokio::test]
async fn the_humans_overrides_reach_the_childs_state_before_it_streams() {
    let child = StateGraph::<Job>::new()
        .add_node("Review", note("review"))
        .add_streaming_node("Talk", |job: Job, ctx: &StreamContext| {
            ctx.emit_token_blocking(&job.who).unwrap();
            job
        })
        .add_edge("Review", "Talk")
        .add_interrupt_boundary("Review")
        .compile()
        .unwrap();
    let graph = parent_around(child, None);
    let store = store();
    freeze(&graph, &store).await;
    let signal = HumanSignal {
        field_overrides: HashMap::from([("who".to_string(), "alice".to_string())]),
        ..approve()
    };

    let (chunks, result) = resume(&graph, &store, signal).await;

    completed(result);
    assert_eq!(tokens_from(&chunks, "Work/Talk"), vec!["alice"]);
}

#[tokio::test]
async fn a_retry_after_a_streamed_resume_reruns_the_child_from_the_boundary_not_the_start() {
    let reviews = Arc::new(AtomicU32::new(0));
    let attempts = Arc::new(AtomicU32::new(0));
    let (counted, tried) = (reviews.clone(), attempts.clone());
    // `Review` (boundary, counted) -> `Fan` (a streaming branch that fails once).
    let child = StateGraph::<Job>::new()
        .add_node("Review", move |job: Job| {
            counted.fetch_add(1, Ordering::SeqCst);
            job
        })
        .add_parallel_group(
            "Fan",
            ParallelBranches::new().streaming_branch(
                "flaky",
                move |s: &Job, ctx: &StreamContext| {
                    let attempt = tried.fetch_add(1, Ordering::SeqCst) + 1;
                    ctx.emit_token_blocking(&format!("try{attempt}")).unwrap();
                    if attempt < 2 {
                        return Err(NodeError::Transient("timeout".to_string()));
                    }
                    Ok(s.clone())
                },
            ),
            concat,
        )
        .add_edge("Review", "Fan")
        .add_interrupt_boundary("Review")
        .compile()
        .unwrap();
    let graph = parent_around(child, Some(RecoveryPolicy::retry(2)));
    let store = store();
    freeze(&graph, &store).await;

    let (chunks, result) = resume(&graph, &store, approve()).await;

    completed(result);
    assert_eq!(
        reviews.load(Ordering::SeqCst),
        1,
        "the human is not asked twice"
    );
    assert_eq!(
        shape_under(&chunks, "Work"),
        vec![
            item(StreamChunkType::NodeStart, "Work", "Work"),
            item(StreamChunkType::NodeStart, "Work/Fan", "Fan"),
            item(StreamChunkType::Token, "Work/Fan/flaky", "try1"),
            item(
                StreamChunkType::Error,
                "Work/Fan",
                "branch 'flaky': timeout"
            ),
            item(
                StreamChunkType::Retry,
                "Work",
                "subgraph 'Work' failed at 'Fan' after 1 attempt(s): branch 'flaky': timeout"
            ),
            item(StreamChunkType::NodeStart, "Work/Fan", "Fan"),
            item(StreamChunkType::Token, "Work/Fan/flaky", "try2"),
            item(StreamChunkType::NodeEnd, "Work/Fan", "Fan"),
            item(StreamChunkType::NodeEnd, "Work", "Work"),
        ]
    );
}

#[tokio::test]
async fn a_child_that_freezes_again_after_a_streamed_resume_can_be_resumed_streaming_again() {
    // `Review` (boundary) -> `Draft` (streams, boundary) -> `Publish` (streams).
    let child = StateGraph::<Job>::new()
        .add_node("Review", note("review"))
        .add_streaming_node("Draft", talk("draft", &["v1"]))
        .add_streaming_node("Publish", talk("publish", &["final"]))
        .add_edge("Review", "Draft")
        .add_edge("Draft", "Publish")
        .add_interrupt_boundary("Review")
        .add_interrupt_boundary("Draft")
        .compile()
        .unwrap();
    let graph = parent_around(child, None);
    let store = store();
    freeze(&graph, &store).await;

    let (first, result) = resume(&graph, &store, approve()).await;
    match result {
        GraphExecutionResult::Interrupted { suspended_node, .. } => {
            assert_eq!(suspended_node, "Work");
        }
        _ => panic!("expected Interrupted"),
    }
    assert_eq!(tokens_from(&first, "Work/Draft"), vec!["v1"]);
    assert!(
        !first
            .iter()
            .any(|c| c.chunk_type == StreamChunkType::NodeEnd && &*c.node_name == "Work")
    );

    let (second, result) = resume(&graph, &store, approve()).await;
    assert_eq!(
        completed(result).log,
        vec!["before", "review", "draft", "publish", "finish"]
    );
    assert_eq!(tokens_from(&second, "Work/Publish"), vec!["final"]);
}
