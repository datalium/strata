//! Streaming inside a subgraph (`design/15-Subgraph-Composition.md`): a child
//! graph's streaming nodes, parallel-group branches, lifecycle chunks and
//! `Retry`/`Error` chunks reach the parent's `run_streaming` receiver, named
//! `"<subgraph>/<child node>"`. The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/SubgraphStreamingTests.cs` — keep the two in step.

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
}

fn job() -> Job {
    Job { log: Vec::new() }
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

/// `Before` -> `Work` (a subgraph running `child`) -> `Finish`.
fn parent_around(child: CompiledGraph<Job>, policy: Option<RecoveryPolicy>) -> CompiledGraph<Job> {
    let mut graph = StateGraph::<Job>::new()
        .add_node("Before", note("before"))
        .add_subgraph("Work", child, Clone::clone, |_: &Job, child: Job| child)
        .add_node("Finish", note("finish"))
        .add_edge("Before", "Work")
        .add_edge("Work", "Finish");
    if let Some(policy) = policy {
        graph = graph.set_recovery_policy("Work", policy);
    }
    graph.compile().unwrap()
}

/// Runs `graph` under `run_streaming` and collects every chunk it emits.
async fn stream(graph: CompiledGraph<Job>) -> (Vec<StreamChunk>, GraphExecutionResult<Job>) {
    let store: Arc<dyn CheckpointStore> = Arc::new(TestStore::default());
    stream_with(graph, store).await
}

async fn stream_with(
    graph: CompiledGraph<Job>,
    store: Arc<dyn CheckpointStore>,
) -> (Vec<StreamChunk>, GraphExecutionResult<Job>) {
    let (handle, mut receiver) = Arc::new(graph).run_streaming("t", job(), store);
    let mut chunks = Vec::new();
    while let Some(chunk) = receiver.recv().await {
        chunks.push(chunk);
    }
    (chunks, handle.await.unwrap())
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

/// The `(type, node, content)` of every chunk, for asserting on a sequence.
fn shape(chunks: &[StreamChunk]) -> Vec<(StreamChunkType, String, String)> {
    chunks
        .iter()
        .map(|c| (c.chunk_type, c.node_name.to_string(), c.content.to_string()))
        .collect()
}

/// [`shape`] of the chunks attributed to `node` or anything under it.
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

fn count(chunks: &[StreamChunk], wanted: StreamChunkType, node: &str) -> usize {
    chunks
        .iter()
        .filter(|c| c.chunk_type == wanted && &*c.node_name == node)
        .count()
}

fn item(kind: StreamChunkType, node: &str, content: &str) -> (StreamChunkType, String, String) {
    (kind, node.to_string(), content.to_string())
}

#[tokio::test]
async fn a_child_streaming_node_reaches_the_consumer_named_subgraph_slash_node() {
    let child = StateGraph::<Job>::new()
        .add_streaming_node("Talk", talk("talk", &["Hel", "lo"]))
        .compile()
        .unwrap();

    let (chunks, result) = stream(parent_around(child, None)).await;

    assert_eq!(completed(result).log, vec!["before", "talk", "finish"]);
    assert_eq!(
        shape(&chunks),
        vec![
            item(StreamChunkType::NodeStart, "Before", "Before"),
            item(StreamChunkType::NodeEnd, "Before", "Before"),
            item(StreamChunkType::NodeStart, "Work", "Work"),
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
async fn a_child_parallel_groups_streaming_branches_are_named_under_the_subgraph() {
    let child = StateGraph::<Job>::new()
        .add_parallel_group(
            "Fan",
            ParallelBranches::new()
                .streaming_branch("a", |s: &Job, ctx: &StreamContext| {
                    ctx.emit_token_blocking("a0").unwrap();
                    ctx.emit_token_blocking("a1").unwrap();
                    Ok(s.clone())
                })
                .streaming_branch("b", |s: &Job, ctx: &StreamContext| {
                    ctx.emit_token_blocking("b0").unwrap();
                    Ok(s.clone())
                }),
            concat,
        )
        .compile()
        .unwrap();

    let (chunks, result) = stream(parent_around(child, None)).await;

    completed(result);
    assert_eq!(tokens_from(&chunks, "Work/Fan/a"), vec!["a0", "a1"]);
    assert_eq!(tokens_from(&chunks, "Work/Fan/b"), vec!["b0"]);
    // The group is a node of the child, so it has its own lifecycle pair.
    assert_eq!(count(&chunks, StreamChunkType::NodeStart, "Work/Fan"), 1);
    assert_eq!(count(&chunks, StreamChunkType::NodeEnd, "Work/Fan"), 1);
}

#[tokio::test]
async fn names_nest_through_a_grandchild() {
    let grandchild = StateGraph::<Job>::new()
        .add_streaming_node("Talk", talk("talk", &["deep"]))
        .compile()
        .unwrap();
    let child = StateGraph::<Job>::new()
        .add_subgraph("Inner", grandchild, Clone::clone, |_: &Job, c: Job| c)
        .compile()
        .unwrap();

    let (chunks, result) = stream(parent_around(child, None)).await;

    completed(result);
    assert_eq!(tokens_from(&chunks, "Work/Inner/Talk"), vec!["deep"]);
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
            (StreamChunkType::NodeEnd, "Work/Inner/Talk".to_string()),
            (StreamChunkType::NodeEnd, "Work/Inner".to_string()),
            (StreamChunkType::NodeEnd, "Work".to_string()),
        ]
    );
}

#[tokio::test]
async fn one_child_graph_reused_by_two_nodes_names_each_run_by_its_own_node() {
    let child = Arc::new(
        StateGraph::<Job>::new()
            .add_streaming_node("Talk", talk("talk", &["hi"]))
            .compile()
            .unwrap(),
    );
    let graph = StateGraph::<Job>::new()
        .add_subgraph("First", child.clone(), Clone::clone, |_: &Job, c: Job| c)
        .add_subgraph("Second", child, Clone::clone, |_: &Job, c: Job| c)
        .add_edge("First", "Second")
        .compile()
        .unwrap();

    let (chunks, result) = stream(graph).await;

    completed(result);
    assert_eq!(tokens_from(&chunks, "First/Talk"), vec!["hi"]);
    assert_eq!(tokens_from(&chunks, "Second/Talk"), vec!["hi"]);
}

#[tokio::test]
async fn a_plain_run_refuses_a_subgraph_whose_child_streams_before_running_any_of_it() {
    let ran = Arc::new(AtomicU32::new(0));
    let counted = ran.clone();
    let child = StateGraph::<Job>::new()
        .add_node("Setup", move |job: Job| {
            counted.fetch_add(1, Ordering::SeqCst);
            job
        })
        .add_streaming_node("Talk", talk("talk", &["x"]))
        .add_edge("Setup", "Talk")
        .compile()
        .unwrap();

    match parent_around(child, None)
        .run("t", job(), Arc::new(TestStore::default()))
        .await
    {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
            assert!(message.contains("'Work'"), "{message}");
            assert!(message.contains("run_streaming"), "{message}");
        }
        _ => panic!("expected Failed(ExecutionAborted)"),
    }
    assert_eq!(ran.load(Ordering::SeqCst), 0, "no part of the child ran");
}

#[tokio::test]
async fn a_plain_run_refuses_a_streaming_grandchild_at_the_outermost_subgraph_node() {
    let ran = Arc::new(AtomicU32::new(0));
    let counted = ran.clone();
    let grandchild = StateGraph::<Job>::new()
        .add_streaming_node("Talk", talk("talk", &["x"]))
        .compile()
        .unwrap();
    let child = StateGraph::<Job>::new()
        .add_node("Setup", move |job: Job| {
            counted.fetch_add(1, Ordering::SeqCst);
            job
        })
        .add_subgraph("Inner", grandchild, Clone::clone, |_: &Job, c: Job| c)
        .add_edge("Setup", "Inner")
        .compile()
        .unwrap();

    match parent_around(child, None)
        .run("t", job(), Arc::new(TestStore::default()))
        .await
    {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
            assert!(message.contains("'Work'"), "{message}");
        }
        _ => panic!("expected Failed(ExecutionAborted)"),
    }
    assert_eq!(ran.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_child_group_with_only_plain_branches_still_runs_under_a_plain_run() {
    let child = StateGraph::<Job>::new()
        .add_parallel_group(
            "Fan",
            ParallelBranches::new().branch("a", |_: &Job| Ok(job())),
            concat,
        )
        .compile()
        .unwrap();

    let state = completed(
        parent_around(child, None)
            .run("t", job(), Arc::new(TestStore::default()))
            .await,
    );

    assert_eq!(state.log, vec!["before", "finish"]);
}

#[tokio::test]
async fn re_adding_the_name_replaces_a_streaming_subgraph_and_its_stream_requirement() {
    let child = StateGraph::<Job>::new()
        .add_streaming_node("Talk", talk("talk", &["x"]))
        .compile()
        .unwrap();
    let graph = StateGraph::<Job>::new()
        .add_subgraph("Work", child, Clone::clone, |_: &Job, c: Job| c)
        .add_node("Work", note("plain"))
        .compile()
        .unwrap();

    let state = completed(graph.run("t", job(), Arc::new(TestStore::default())).await);

    assert_eq!(state.log, vec!["plain"]);
}

/// A child whose group's one streaming branch emits `try<n>` and fails
/// transiently on its first `failures` attempts.
fn flaky_child(failures: u32, child_policy: Option<RecoveryPolicy>) -> CompiledGraph<Job> {
    let attempts = Arc::new(AtomicU32::new(0));
    let mut child = StateGraph::<Job>::new().add_parallel_group(
        "Fan",
        ParallelBranches::new().streaming_branch("flaky", move |s: &Job, ctx: &StreamContext| {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
            ctx.emit_token_blocking(&format!("try{attempt}")).unwrap();
            if attempt <= failures {
                return Err(NodeError::Transient("timeout".to_string()));
            }
            Ok(s.clone())
        }),
        concat,
    );
    if let Some(policy) = child_policy {
        child = child.set_recovery_policy("Fan", policy);
    }
    child.compile().unwrap()
}

#[tokio::test]
async fn a_retry_of_the_subgraph_is_announced_before_the_child_streams_again() {
    let graph = parent_around(flaky_child(1, None), Some(RecoveryPolicy::retry(2)));

    let (chunks, result) = stream(graph).await;

    completed(result);
    assert_eq!(
        shape_under(&chunks, "Work"),
        vec![
            item(StreamChunkType::NodeStart, "Work", "Work"),
            item(StreamChunkType::NodeStart, "Work/Fan", "Fan"),
            item(StreamChunkType::Token, "Work/Fan/flaky", "try1"),
            // The child's group gave up (it has no policy of its own)...
            item(
                StreamChunkType::Error,
                "Work/Fan",
                "branch 'flaky': timeout"
            ),
            // ...and the parent's policy re-runs the whole child. The Retry is
            // the parent's own node, carrying the child's failure message.
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
async fn a_childs_own_retry_is_announced_under_the_childs_node_not_the_subgraphs() {
    let graph = parent_around(flaky_child(1, Some(RecoveryPolicy::retry(1))), None);

    let (chunks, result) = stream(graph).await;

    completed(result);
    assert_eq!(count(&chunks, StreamChunkType::Retry, "Work/Fan"), 1);
    assert_eq!(count(&chunks, StreamChunkType::Retry, "Work"), 0);
    assert_eq!(tokens_from(&chunks, "Work/Fan/flaky"), vec!["try1", "try2"]);
}

#[tokio::test]
async fn a_child_that_gives_up_emits_an_error_for_its_node_then_one_for_the_subgraph() {
    let graph = parent_around(flaky_child(5, None), None);

    let (chunks, result) = stream(graph).await;

    assert!(matches!(
        result,
        GraphExecutionResult::Failed(StrataError::NodeFailed { ref node, .. }) if node == "Work"
    ));
    let errors: Vec<_> = shape(&chunks)
        .into_iter()
        .filter(|(kind, _, _)| *kind == StreamChunkType::Error)
        .collect();
    assert_eq!(
        errors,
        vec![
            item(
                StreamChunkType::Error,
                "Work/Fan",
                "branch 'flaky': timeout"
            ),
            item(
                StreamChunkType::Error,
                "Work",
                "subgraph 'Work' failed at 'Fan' after 1 attempt(s): branch 'flaky': timeout"
            ),
        ]
    );
    // Neither node completed.
    assert_eq!(count(&chunks, StreamChunkType::NodeEnd, "Work/Fan"), 0);
    assert_eq!(count(&chunks, StreamChunkType::NodeEnd, "Work"), 0);
}

#[tokio::test]
async fn a_child_streaming_past_the_channel_bound_loses_no_tokens() {
    // 3000 tokens is well past the 1024-chunk channel and the consumer holds
    // off reading, so the child is blocked in a send while the subgraph node
    // is mid-run. Backpressure must pause it, not drop tokens.
    const TOKENS: usize = 3000;
    let child = StateGraph::<Job>::new()
        .add_streaming_node("Talk", |job: Job, ctx: &StreamContext| {
            for i in 0..TOKENS {
                ctx.emit_token_blocking(&i.to_string()).unwrap();
            }
            job
        })
        .compile()
        .unwrap();
    let graph = Arc::new(parent_around(child, None));
    let store: Arc<dyn CheckpointStore> = Arc::new(TestStore::default());

    let (handle, mut receiver) = graph.run_streaming("t", job(), store);
    sleep(Duration::from_millis(200));
    let mut chunks = Vec::new();
    while let Some(chunk) = receiver.recv().await {
        chunks.push(chunk);
    }

    completed(handle.await.unwrap());
    let expected: Vec<String> = (0..TOKENS).map(|i| i.to_string()).collect();
    assert_eq!(tokens_from(&chunks, "Work/Talk"), expected);
}

// --- Streaming and nested interrupts ---------------------------------------

/// `Draft` (streams, and is a boundary) -> `Publish`.
fn reviewed_child() -> CompiledGraph<Job> {
    StateGraph::<Job>::new()
        .add_streaming_node("Draft", talk("draft", &["v1"]))
        .add_node("Publish", note("publish"))
        .add_edge("Draft", "Publish")
        .add_interrupt_boundary("Draft")
        .compile()
        .unwrap()
}

#[tokio::test]
async fn a_child_that_freezes_has_streamed_up_to_the_boundary_and_the_subgraph_node_never_ends() {
    let (chunks, result) = stream(parent_around(reviewed_child(), None)).await;

    match result {
        GraphExecutionResult::Interrupted { suspended_node, .. } => {
            assert_eq!(suspended_node, "Work")
        }
        _ => panic!("expected Interrupted"),
    }
    assert_eq!(
        shape_under(&chunks, "Work"),
        vec![
            item(StreamChunkType::NodeStart, "Work", "Work"),
            item(StreamChunkType::NodeStart, "Work/Draft", "Draft"),
            item(StreamChunkType::Token, "Work/Draft", "v1"),
            item(StreamChunkType::NodeEnd, "Work/Draft", "Draft"),
        ]
    );
}

#[tokio::test]
async fn a_streamed_child_that_froze_after_its_streaming_can_be_resumed() {
    let graph = Arc::new(parent_around(reviewed_child(), None));
    let store: Arc<dyn CheckpointStore> = Arc::new(TestStore::default());
    let (handle, mut receiver) = graph.clone().run_streaming("t", job(), store.clone());
    while receiver.recv().await.is_some() {}
    assert!(matches!(
        handle.await.unwrap(),
        GraphExecutionResult::Interrupted { .. }
    ));

    // A plain `resume` is not streamed, but the child's streaming node already ran.
    let state = completed(
        graph
            .resume("t", HumanSignal::default(), store.clone())
            .await,
    );

    assert_eq!(state.log, vec!["before", "draft", "publish", "finish"]);
}

#[tokio::test]
async fn a_resume_that_reaches_a_streaming_child_node_fails_there_like_a_flat_graph() {
    // The boundary comes first, so the streaming node is still ahead when the
    // plain resume (which has no StreamContext) re-enters the child.
    let child = StateGraph::<Job>::new()
        .add_node("Review", note("review"))
        .add_streaming_node("Talk", talk("talk", &["x"]))
        .add_edge("Review", "Talk")
        .add_interrupt_boundary("Review")
        .compile()
        .unwrap();
    let graph = Arc::new(parent_around(child, None));
    let store: Arc<dyn CheckpointStore> = Arc::new(TestStore::default());
    let (handle, mut receiver) = graph.clone().run_streaming("t", job(), store.clone());
    while receiver.recv().await.is_some() {}
    assert!(matches!(
        handle.await.unwrap(),
        GraphExecutionResult::Interrupted { .. }
    ));

    match graph
        .resume("t", HumanSignal::default(), store.clone())
        .await
    {
        GraphExecutionResult::Failed(StrataError::NodeFailed { node, message, .. }) => {
            assert_eq!(node, "Work");
            assert!(message.contains("'Talk'"), "{message}");
            assert!(message.contains("no StreamContext"), "{message}");
        }
        _ => panic!("expected Failed(NodeFailed)"),
    }
}
