//! Streaming from the branches of a parallel group
//! (`design/14-Parallel-Execution.md`), plus the `Retry` chunk a retried
//! fallible node emits. The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/ParallelStreamingTests.cs` — keep the two in step.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

use datalium_strata::BoxFuture;

use datalium_strata::StrataError;
use datalium_strata::checkpoint::{CheckpointStore, CheckpointStoreError, GraphExecutionResult};
use datalium_strata::graph::{CompiledGraph, NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::parallel::ParallelBranches;
use datalium_strata::schema::Checkpoint;
use datalium_strata::streaming::{StreamChunk, StreamChunkType};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Report {
    results: Vec<String>,
}

fn report() -> Report {
    Report {
        results: Vec::new(),
    }
}

fn concat(base: &Report, outputs: Vec<Report>) -> Report {
    let mut merged = base.clone();
    for output in outputs {
        merged.results.extend(output.results);
    }
    merged
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

/// `Gather` (the parallel group) -> `Finish`.
fn graph_around(
    branches: ParallelBranches<Report>,
    policy: Option<RecoveryPolicy>,
) -> Result<CompiledGraph<Report>, StrataError> {
    let mut graph = StateGraph::<Report>::new()
        .add_parallel_group("Gather", branches, concat)
        .add_node("Finish", |s: Report| s)
        .add_edge("Gather", "Finish");
    if let Some(policy) = policy {
        graph = graph.set_recovery_policy("Gather", policy);
    }
    graph.compile()
}

/// Runs `graph` under `run_streaming` and collects every chunk it emits.
async fn stream(graph: CompiledGraph<Report>) -> (Vec<StreamChunk>, GraphExecutionResult<Report>) {
    let store: Arc<dyn CheckpointStore> = Arc::new(TestStore::default());
    let (handle, mut receiver) = Arc::new(graph).run_streaming("t", report(), store);
    let mut chunks = Vec::new();
    while let Some(chunk) = receiver.recv().await {
        chunks.push(chunk);
    }
    (chunks, handle.await.unwrap())
}

fn completed(result: GraphExecutionResult<Report>) -> Report {
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

/// The tokens one node (or `"<group>/<branch>"`) emitted, in arrival order.
fn tokens_from(chunks: &[StreamChunk], node: &str) -> Vec<String> {
    chunks
        .iter()
        .filter(|c| c.chunk_type == StreamChunkType::Token && &*c.node_name == node)
        .map(|c| c.content.to_string())
        .collect()
}

/// A streaming branch that emits `count` numbered tokens, then records `label`.
fn talker(
    label: &'static str,
    count: usize,
) -> impl Fn(&Report, &datalium_strata::streaming::StreamContext) -> Result<Report, NodeError>
+ Send
+ Sync
+ 'static {
    move |s, ctx| {
        for i in 0..count {
            ctx.emit_token_blocking(&format!("{label}{i}")).unwrap();
        }
        let mut next = s.clone();
        next.results.push(label.to_string());
        Ok(next)
    }
}

#[tokio::test]
async fn every_streaming_branch_reaches_the_consumer_attributed_to_group_slash_branch() {
    let branches = ParallelBranches::new()
        .streaming_branch("a", talker("a", 3))
        .streaming_branch("b", talker("b", 3));
    let graph = graph_around(branches, None).unwrap();

    let (chunks, result) = stream(graph).await;

    // Each branch's own tokens arrive whole and in order, however the two interleave.
    assert_eq!(tokens_from(&chunks, "Gather/a"), vec!["a0", "a1", "a2"]);
    assert_eq!(tokens_from(&chunks, "Gather/b"), vec!["b0", "b1", "b2"]);
    // The reducer still sees declaration order.
    assert_eq!(completed(result).results, vec!["a", "b"]);
}

#[tokio::test]
async fn branch_tokens_arrive_between_the_groups_own_start_and_end() {
    let branches = ParallelBranches::new()
        .streaming_branch("a", talker("a", 2))
        .streaming_branch("b", talker("b", 2));
    let graph = graph_around(branches, None).unwrap();

    let (chunks, _) = stream(graph).await;

    let position = |wanted: StreamChunkType, node: &str| {
        chunks
            .iter()
            .position(|c| c.chunk_type == wanted && &*c.node_name == node)
            .unwrap()
    };
    let start = position(StreamChunkType::NodeStart, "Gather");
    let end = position(StreamChunkType::NodeEnd, "Gather");
    let token_positions: Vec<usize> = chunks
        .iter()
        .enumerate()
        .filter(|(_, c)| c.chunk_type == StreamChunkType::Token)
        .map(|(i, _)| i)
        .collect();
    assert_eq!(token_positions.len(), 4);
    assert!(token_positions.iter().all(|&i| start < i && i < end));
    // Branches are not nodes: the group is still the only lifecycle pair.
    assert_eq!(
        chunks
            .iter()
            .filter(|c| c.chunk_type == StreamChunkType::NodeStart)
            .map(|c| c.node_name.to_string())
            .collect::<Vec<_>>(),
        vec!["Gather", "Finish"]
    );
}

#[tokio::test]
async fn a_plain_branch_can_sit_beside_a_streaming_one_and_says_nothing() {
    let branches = ParallelBranches::new()
        .streaming_branch("talk", talker("talk", 2))
        .branch("quiet", |s: &Report| {
            let mut next = s.clone();
            next.results.push("quiet".to_string());
            Ok(next)
        });
    let graph = graph_around(branches, None).unwrap();

    let (chunks, result) = stream(graph).await;

    assert_eq!(tokens_from(&chunks, "Gather/talk"), vec!["talk0", "talk1"]);
    assert!(tokens_from(&chunks, "Gather/quiet").is_empty());
    assert_eq!(completed(result).results, vec!["talk", "quiet"]);
}

#[tokio::test]
async fn concurrent_producers_overrun_the_bounded_channel_and_nothing_is_dropped() {
    // 3 producers x 2000 tokens is well past the 1024-chunk channel, and the
    // consumer holds off reading, so every branch ends up blocked in a send.
    // Backpressure must pause them, not drop tokens.
    const PER_BRANCH: usize = 2000;
    let branches = ParallelBranches::new()
        .streaming_branch("a", talker("a", PER_BRANCH))
        .streaming_branch("b", talker("b", PER_BRANCH))
        .streaming_branch("c", talker("c", PER_BRANCH));
    let graph = Arc::new(graph_around(branches, None).unwrap());
    let store: Arc<dyn CheckpointStore> = Arc::new(TestStore::default());

    let (handle, mut receiver) = graph.run_streaming("t", report(), store);
    sleep(Duration::from_millis(200));
    let mut chunks = Vec::new();
    while let Some(chunk) = receiver.recv().await {
        chunks.push(chunk);
    }

    assert_eq!(
        completed(handle.await.unwrap()).results,
        vec!["a", "b", "c"]
    );
    for label in ["a", "b", "c"] {
        let expected: Vec<String> = (0..PER_BRANCH).map(|i| format!("{label}{i}")).collect();
        assert_eq!(tokens_from(&chunks, &format!("Gather/{label}")), expected);
    }
}

#[tokio::test]
async fn a_plain_run_refuses_a_group_with_a_streaming_branch_before_running_any_of_it() {
    let ran = Arc::new(AtomicU32::new(0));
    let plain_ran = ran.clone();
    let branches = ParallelBranches::new()
        .branch("plain", move |s: &Report| {
            plain_ran.fetch_add(1, Ordering::SeqCst);
            Ok(s.clone())
        })
        .streaming_branch("talk", talker("talk", 1));
    let graph = graph_around(branches, None).unwrap();

    match graph
        .run("t", report(), Arc::new(TestStore::default()))
        .await
    {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
            assert!(message.contains("'Gather'"), "{message}");
            assert!(message.contains("run_streaming"), "{message}");
        }
        _ => panic!("expected Failed(ExecutionAborted)"),
    }
    assert_eq!(ran.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_retry_is_announced_before_the_streaming_branches_run_again() {
    let attempts = Arc::new(AtomicU32::new(0));
    let counter = attempts.clone();
    let branches = ParallelBranches::new().streaming_branch("flaky", move |s: &Report, ctx| {
        let attempt = counter.fetch_add(1, Ordering::SeqCst) + 1;
        ctx.emit_token_blocking(&format!("try{attempt}")).unwrap();
        if attempt < 2 {
            return Err(NodeError::Transient("timeout".to_string()));
        }
        Ok(s.clone())
    });
    let graph = graph_around(branches, Some(RecoveryPolicy::retry(2))).unwrap();

    let (chunks, result) = stream(graph).await;

    completed(result);
    let gather: Vec<_> = shape(&chunks)
        .into_iter()
        .filter(|(_, node, _)| node.starts_with("Gather"))
        .collect();
    assert_eq!(
        gather,
        vec![
            (StreamChunkType::NodeStart, "Gather".into(), "Gather".into()),
            (StreamChunkType::Token, "Gather/flaky".into(), "try1".into()),
            // The group's own name, and the failed attempt's message.
            (
                StreamChunkType::Retry,
                "Gather".into(),
                "branch 'flaky': timeout".into()
            ),
            (StreamChunkType::Token, "Gather/flaky".into(), "try2".into()),
            (StreamChunkType::NodeEnd, "Gather".into(), "Gather".into()),
        ]
    );
}

#[tokio::test]
async fn a_group_that_gives_up_emits_error_not_another_retry() {
    let branches = ParallelBranches::new().streaming_branch("down", |_: &Report, ctx| {
        ctx.emit_token_blocking("dialling").unwrap();
        Err(NodeError::Transient("outage".to_string()))
    });
    let graph = graph_around(branches, Some(RecoveryPolicy::retry(1))).unwrap();

    let (chunks, result) = stream(graph).await;

    assert!(matches!(
        result,
        GraphExecutionResult::Failed(StrataError::NodeFailed { attempts: 2, .. })
    ));
    let count = |wanted| chunks.iter().filter(|c| c.chunk_type == wanted).count();
    // Two attempts: one retry between them, and one Error once the second gave up.
    assert_eq!(count(StreamChunkType::Retry), 1);
    assert_eq!(count(StreamChunkType::Error), 1);
    assert_eq!(tokens_from(&chunks, "Gather/down"), vec!["dialling"; 2]);
}

#[tokio::test]
async fn any_retried_fallible_node_emits_a_retry_chunk_not_only_groups() {
    let attempts = Arc::new(AtomicU32::new(0));
    let counter = attempts.clone();
    let graph = StateGraph::<Report>::new()
        .add_fallible_node("Call", move |s: &Report| {
            if counter.fetch_add(1, Ordering::SeqCst) < 2 {
                Err(NodeError::Transient("rate limited".to_string()))
            } else {
                Ok(s.clone())
            }
        })
        .set_recovery_policy("Call", RecoveryPolicy::retry(2))
        .compile()
        .unwrap();

    let (chunks, result) = stream(graph).await;

    completed(result);
    let retries: Vec<_> = shape(&chunks)
        .into_iter()
        .filter(|(kind, _, _)| *kind == StreamChunkType::Retry)
        .collect();
    assert_eq!(
        retries,
        vec![
            (StreamChunkType::Retry, "Call".into(), "rate limited".into()),
            (StreamChunkType::Retry, "Call".into(), "rate limited".into()),
        ]
    );
}

#[tokio::test]
async fn a_retry_that_is_never_needed_emits_no_retry_chunk() {
    let branches = ParallelBranches::new().streaming_branch("a", talker("a", 1));
    let graph = graph_around(branches, Some(RecoveryPolicy::retry(3))).unwrap();

    let (chunks, _) = stream(graph).await;

    assert!(
        chunks
            .iter()
            .all(|c| c.chunk_type != StreamChunkType::Retry)
    );
}

#[test]
fn a_child_whose_group_has_only_plain_branches_is_still_fine() {
    let child = StateGraph::<Report>::new()
        .add_parallel_group(
            "Fan",
            ParallelBranches::new().branch("a", |s: &Report| Ok(s.clone())),
            concat,
        )
        .compile()
        .unwrap();

    let graph = StateGraph::<Report>::new()
        .add_subgraph("Work", child, |s: &Report| s.clone(), |_, child| child)
        .compile();

    assert!(graph.is_ok());
}

#[tokio::test]
async fn re_adding_a_name_replaces_a_streaming_group_and_its_stream_requirement() {
    let graph = StateGraph::<Report>::new()
        .add_parallel_group(
            "Gather",
            ParallelBranches::new().streaming_branch("talk", talker("talk", 1)),
            concat,
        )
        .add_node("Gather", |mut s: Report| {
            s.results.push("plain".to_string());
            s
        })
        .compile()
        .unwrap();

    // A plain `run` is fine now: nothing in the graph needs a StreamContext.
    let state = completed(
        graph
            .run("t", report(), Arc::new(TestStore::default()))
            .await,
    );

    assert_eq!(state.results, vec!["plain"]);
}
