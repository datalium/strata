//! The loop circuit breaker (`design/24-Loop-Circuit-Breaker.md`), exercised
//! through the public API only: a compiled hop limit stops a thread before the
//! node that would be one hop too many, with a `Failed` checkpoint whose
//! `error_kind` is `hop_limit`, and the count survives interrupts and resumes
//! through `strata.hops`. The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/HopLimitTests.cs` — keep the two in step.
//! The span and metric checks live in `telemetry.rs`, which owns the
//! process-wide subscriber and recorder.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::checkpoint::{
    CheckpointStore, CheckpointStoreError, GraphExecutionResult, HumanSignal,
};
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{
    CompileOptions, CompiledGraph, NodeError, RecoveryPolicy, StateGraph,
};
use datalium_strata::observability::{RunEvent, RunEventChannel, RunEventKind};
use datalium_strata::parallel::ParallelBranches;
use datalium_strata::schema::{Checkpoint, CheckpointStatus};
use datalium_strata::streaming::StreamChunkType;
use datalium_strata::{BoxFuture, CancellationToken, StrataError};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

const PATIENCE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Job {
    n: i32,
    trail: Vec<String>,
}

fn job() -> Job {
    Job {
        n: 0,
        trail: Vec::new(),
    }
}

#[derive(Default)]
struct TestStore {
    checkpoints: Mutex<HashMap<String, Checkpoint>>,
}

impl TestStore {
    fn latest(&self, thread_id: &str) -> Checkpoint {
        self.checkpoints.lock().unwrap()[thread_id].clone()
    }

    fn put(&self, checkpoint: Checkpoint) {
        self.checkpoints
            .lock()
            .unwrap()
            .insert(checkpoint.thread_id.clone(), checkpoint);
    }
}

impl CheckpointStore for TestStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        self.put(checkpoint);
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

fn store() -> Arc<TestStore> {
    Arc::new(TestStore::default())
}

fn go() -> HumanSignal {
    HumanSignal::approve("oncall")
}

/// Counts every time a node's handler runs, by node.
#[derive(Clone, Default)]
struct Calls(Arc<Mutex<HashMap<&'static str, u32>>>);

impl Calls {
    fn count(
        &self,
        node: &'static str,
        handler: impl Fn(Job) -> Job + Send + Sync + 'static,
    ) -> impl Fn(Job) -> Job + Send + Sync + 'static {
        let calls = self.0.clone();
        move |job| {
            *calls.lock().unwrap().entry(node).or_default() += 1;
            handler(job)
        }
    }

    fn of(&self, node: &str) -> u32 {
        self.0
            .lock()
            .unwrap()
            .get(node)
            .copied()
            .unwrap_or_default()
    }
}

fn step(label: &'static str) -> impl Fn(Job) -> Job + Send + Sync + 'static {
    move |mut job: Job| {
        job.trail.push(label.to_string());
        job
    }
}

fn bump(mut job: Job) -> Job {
    job.n += 1;
    job.trail.push("A".to_string());
    job
}

/// `A` (n + 1) -> `B` -> back to `A` while n < `stop_at`, else `Done`. With
/// `stop_at` s the run is 2s + 1 hops: A B A B … A B Done.
fn looping(calls: &Calls, stop_at: i32, boundary_at_b: bool) -> StateGraph<Job> {
    let graph = StateGraph::<Job>::new()
        .add_node("A", calls.count("A", bump))
        .add_node("B", calls.count("B", step("B")))
        .add_node("Done", calls.count("Done", step("done")))
        .add_edge("A", "B")
        .add_conditional_edge("B", "A", move |job: &Job| job.n < stop_at)
        .add_edge("B", "Done");
    if boundary_at_b {
        graph.add_interrupt_boundary("B")
    } else {
        graph
    }
}

fn limit(hops: u32) -> CompileOptions {
    CompileOptions::new().with_max_node_hops(hops)
}

/// The `(node, hops, limit)` of a hop-limit result.
fn tripped<S>(result: GraphExecutionResult<S>) -> (String, u32, u32) {
    match result {
        GraphExecutionResult::Failed(StrataError::HopLimitExceeded { node, hops, limit }) => {
            (node, hops, limit)
        }
        GraphExecutionResult::Failed(other) => panic!("expected HopLimitExceeded, got {other:?}"),
        _ => panic!("expected HopLimitExceeded"),
    }
}

fn completed<S>(result: GraphExecutionResult<S>) -> S {
    match result {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(err) => panic!("expected Completed, got Failed({err:?})"),
        _ => panic!("expected Completed"),
    }
}

fn interrupted<S>(result: GraphExecutionResult<S>) -> Vec<String> {
    match result {
        GraphExecutionResult::Interrupted { waiting_at, .. } => waiting_at,
        GraphExecutionResult::Failed(err) => panic!("expected Interrupted, got Failed({err:?})"),
        _ => panic!("expected Interrupted"),
    }
}

fn meta<'a>(checkpoint: &'a Checkpoint, key: &str) -> Option<&'a str> {
    checkpoint.metadata.as_ref()?.get(key).map(String::as_str)
}

fn hops(checkpoint: &Checkpoint) -> Option<&str> {
    meta(checkpoint, "strata.hops")
}

#[tokio::test]
async fn without_a_limit_a_long_loop_completes_and_checkpoints_carry_no_hop_count() {
    let graph = looping(&Calls::default(), 25, false).compile().unwrap();

    let job = completed(graph.run("t", job(), store()).await);

    assert_eq!(job.trail.len(), 51);
    assert_eq!(graph.max_node_hops(), None);

    let store = store();
    looping(&Calls::default(), 25, true)
        .compile()
        .unwrap()
        .run("t", self::job(), store.clone())
        .await;
    assert_eq!(store.latest("t").metadata, None);
}

#[tokio::test]
async fn a_loop_that_never_exits_is_stopped_before_hop_n_plus_one() {
    let calls = Calls::default();
    let graph = looping(&calls, i32::MAX, false)
        .compile_with(limit(5))
        .unwrap();
    let store = store();

    let result = graph.run("t", job(), store.clone()).await;

    assert!(matches!(&result, GraphExecutionResult::Failed(_)));
    assert_eq!(tripped(result), ("B".to_string(), 5, 5));
    assert_eq!(graph.max_node_hops(), Some(5));

    // Five nodes ran (A B A B A); the sixth never started.
    assert_eq!((calls.of("A"), calls.of("B")), (3, 2));

    let checkpoint = store.latest("t");
    assert_eq!(checkpoint.status, CheckpointStatus::Failed);
    assert_eq!(checkpoint.suspended_node.as_deref(), Some("B"));
    assert_eq!(checkpoint.next_node.as_deref(), Some("B"));
    assert_eq!(meta(&checkpoint, "error_kind"), Some("hop_limit"));
    assert_eq!(
        meta(&checkpoint, "error"),
        Some("hop limit of 5 reached: node 'B' would be hop 6")
    );
    assert_eq!(hops(&checkpoint), Some("5"));
    assert_eq!(meta(&checkpoint, "attempts"), None);
    assert_eq!(checkpoint.frozen_state["n"], 3);
}

#[tokio::test]
async fn a_loop_that_ends_at_or_before_the_limit_is_untouched() {
    for max in [5, 6] {
        // stop_at 2: A B A B Done, five hops.
        let graph = looping(&Calls::default(), 2, false)
            .compile_with(limit(max))
            .unwrap();

        let job = completed(graph.run("t", job(), store()).await);

        assert_eq!(job.trail, ["A", "B", "A", "B", "done"]);
    }
}

#[tokio::test]
async fn retries_are_not_hops() {
    let attempts = Arc::new(Mutex::new(0));
    let flaky = |attempts: Arc<Mutex<i32>>| {
        StateGraph::<Job>::new()
            .add_fallible_node("Flaky", move |job: &Job| {
                let mut attempts = attempts.lock().unwrap();
                *attempts += 1;
                if *attempts % 3 != 0 {
                    Err(NodeError::Transient("blip".to_string()))
                } else {
                    Ok(job.clone())
                }
            })
            .add_node("End", step("end"))
            .add_edge("Flaky", "End")
            .set_recovery_policy("Flaky", RecoveryPolicy::retry(3))
    };

    completed(
        flaky(attempts.clone())
            .compile_with(limit(2))
            .unwrap()
            .run("t", job(), store())
            .await,
    );
    assert_eq!(*attempts.lock().unwrap(), 3);

    let (node, hops, _) = tripped(
        flaky(attempts)
            .compile_with(limit(1))
            .unwrap()
            .run("t", job(), store())
            .await,
    );
    assert_eq!((node.as_str(), hops), ("End", 1));
}

#[tokio::test]
async fn a_fallback_node_is_a_hop() {
    let rescued = || {
        StateGraph::<Job>::new()
            .add_fallible_node("Broken", |_: &Job| {
                Err(NodeError::Fatal("down".to_string()))
            })
            .add_node("Rescue", step("rescue"))
            .add_node("End", step("end"))
            .add_edge("Broken", "End")
            .add_edge("Rescue", "End")
            .set_recovery_policy("Broken", RecoveryPolicy::default().with_fallback("Rescue"))
    };

    completed(
        rescued()
            .compile_with(limit(3))
            .unwrap()
            .run("t", job(), store())
            .await,
    );

    let (node, hops, _) = tripped(
        rescued()
            .compile_with(limit(2))
            .unwrap()
            .run("t", job(), store())
            .await,
    );
    assert_eq!((node.as_str(), hops), ("End", 2));
}

/// Four nodes in a row, so a child that runs is four hops at its own level.
fn four_steps() -> StateGraph<Job> {
    StateGraph::<Job>::new()
        .add_node("S1", step("s1"))
        .add_node("S2", step("s2"))
        .add_node("S3", step("s3"))
        .add_node("S4", step("s4"))
        .add_edge("S1", "S2")
        .add_edge("S2", "S3")
        .add_edge("S3", "S4")
}

fn branch(label: &'static str) -> impl Fn(&Job) -> Result<Job, NodeError> + Send + Sync + 'static {
    move |job: &Job| {
        let mut job = job.clone();
        job.trail.push(label.to_string());
        Ok(job)
    }
}

fn gather_work_end(child: Arc<CompiledGraph<Job>>) -> StateGraph<Job> {
    StateGraph::<Job>::new()
        .add_parallel_group(
            "Gather",
            ParallelBranches::new()
                .branch("X", branch("x"))
                .branch("Y", branch("y"))
                .branch("Z", branch("z")),
            |entry: &Job, results: Vec<Job>| {
                let mut merged = entry.clone();
                merged
                    .trail
                    .extend(results.into_iter().flat_map(|r| r.trail));
                merged
            },
        )
        .add_subgraph("Work", child, Job::clone, |_: &Job, job: Job| job)
        .add_node("End", step("end"))
        .add_edge("Gather", "Work")
        .add_edge("Work", "End")
}

#[tokio::test]
async fn a_parallel_group_and_a_subgraph_are_one_hop_each() {
    // Gather, Work and End: three hops, whatever runs inside the first two.
    // The child counts its four nodes against its own limit of 4.
    let child = Arc::new(four_steps().compile_with(limit(4)).unwrap());
    completed(
        gather_work_end(child.clone())
            .compile_with(limit(3))
            .unwrap()
            .run("t", job(), store())
            .await,
    );

    let (node, hops, _) = tripped(
        gather_work_end(child)
            .compile_with(limit(2))
            .unwrap()
            .run("t", job(), store())
            .await,
    );
    assert_eq!((node.as_str(), hops), ("End", 2));
}

#[tokio::test]
async fn the_count_survives_an_interrupt_boundary_inside_the_loop() {
    let calls = Calls::default();
    let graph = looping(&calls, i32::MAX, true)
        .compile_with(limit(5))
        .unwrap();
    let store = store();

    interrupted(graph.run("t", job(), store.clone()).await);
    assert_eq!(hops(&store.latest("t")), Some("2"));

    interrupted(graph.resume("t", go(), store.clone()).await);
    assert_eq!(hops(&store.latest("t")), Some("4"));

    // The same hop a run without interrupts trips at.
    assert_eq!(
        tripped(graph.resume("t", go(), store.clone()).await),
        ("B".to_string(), 5, 5)
    );
    assert_eq!((calls.of("A"), calls.of("B")), (3, 2));
}

/// `Plan` -> `Approve` (a boundary) -> `Apply`: StrataOps' remediation.
fn remediation(calls: &Calls) -> Arc<CompiledGraph<Job>> {
    Arc::new(
        StateGraph::<Job>::new()
            .add_node("Plan", calls.count("Plan", step("plan")))
            .add_node("Approve", calls.count("Approve", step("approve")))
            .add_interrupt_boundary("Approve")
            .add_node("Apply", calls.count("Apply", step("apply")))
            .add_edge("Plan", "Approve")
            .add_edge("Approve", "Apply")
            .compile()
            .unwrap(),
    )
}

/// `Check` (n + 1) -> `Remediate` (the subgraph) -> back to `Check`, forever.
fn investigation_loop(calls: &Calls, remediation: Arc<CompiledGraph<Job>>) -> StateGraph<Job> {
    StateGraph::<Job>::new()
        .add_node(
            "Check",
            calls.count("Check", |mut job: Job| {
                job.n += 1;
                job
            }),
        )
        .add_subgraph("Remediate", remediation, Job::clone, |_: &Job, job: Job| {
            job
        })
        .add_node("Done", step("done"))
        .add_edge("Check", "Remediate")
        .add_conditional_edge("Remediate", "Check", |_: &Job| true)
        .add_edge("Remediate", "Done")
}

#[tokio::test]
async fn the_count_survives_an_interrupt_nested_in_a_subgraph_inside_the_loop() {
    let calls = Calls::default();
    let graph = investigation_loop(&calls, remediation(&calls))
        .compile_with(limit(5))
        .unwrap();
    let store = store();

    // Check (1), Remediate (2) freezes inside: the subgraph node is
    // re-entered, so the count is the hops before it.
    let waiting_at = interrupted(graph.run("t", job(), store.clone()).await);
    assert_eq!(waiting_at, ["Remediate", "Approve"]);
    let frozen = store.latest("t");
    assert_eq!(hops(&frozen), Some("1"));
    assert_eq!(frozen.nested.as_ref().unwrap().metadata, None);

    // Remediate is hop 2 again, then Check (3), Remediate (4) freezes.
    interrupted(graph.resume("t", go(), store.clone()).await);
    assert_eq!(hops(&store.latest("t")), Some("3"));

    // Remediate (4) again, Check (5), and the third Remediate would be hop 6.
    assert_eq!(
        tripped(graph.resume("t", go(), store.clone()).await),
        ("Remediate".to_string(), 5, 5)
    );
    assert_eq!(
        (calls.of("Check"), calls.of("Plan"), calls.of("Apply")),
        (3, 2, 2)
    );
}

/// Every event the channel delivers, until it ends after `RunCompleted`.
async fn drain(mut events: mpsc::UnboundedReceiver<RunEvent>) -> Vec<RunEvent> {
    let mut all = Vec::new();
    while let Some(event) = tokio::time::timeout(PATIENCE, events.recv()).await.unwrap() {
        all.push(event);
    }
    all
}

#[tokio::test]
async fn a_child_that_hits_its_own_limit_fails_the_subgraph_node_fatally() {
    let calls = Calls::default();
    let child = Arc::new(
        looping(&calls, i32::MAX, false)
            .compile_with(limit(3))
            .unwrap(),
    );
    let graph = StateGraph::<Job>::new()
        .add_subgraph("Work", child, Job::clone, |_: &Job, job: Job| job)
        .add_node("End", step("end"))
        .add_edge("Work", "End")
        .compile_with(limit(10))
        .unwrap();
    let store = store();
    let (observer, events) = RunEventChannel::new();

    let result = graph
        .run_with(
            "t",
            job(),
            store.clone(),
            RunConfig::new().with_observer(observer),
        )
        .await;

    match result {
        GraphExecutionResult::Failed(StrataError::NodeFailed {
            node,
            attempts,
            message,
        }) => {
            assert_eq!((node.as_str(), attempts), ("Work", 1));
            assert_eq!(
                message,
                "subgraph 'Work' hit its hop limit: hop limit of 3 reached: node 'B' would be hop 4"
            );
        }
        _ => panic!("expected NodeFailed"),
    }

    let checkpoint = store.latest("t");
    assert_eq!(
        (meta(&checkpoint, "error_kind"), hops(&checkpoint)),
        (Some("fatal"), Some("0"))
    );
    let nested = checkpoint.nested.as_deref().unwrap();
    assert_eq!(nested.status, CheckpointStatus::Failed);
    assert_eq!(nested.next_node.as_deref(), Some("B"));
    assert_eq!(
        (meta(nested, "error_kind"), hops(nested)),
        (Some("hop_limit"), Some("3"))
    );

    let hit: Vec<_> = drain(events)
        .await
        .into_iter()
        .filter_map(|e| match e.kind {
            RunEventKind::HopLimitExceeded { hops, limit } => Some((
                e.node_path.as_deref().unwrap_or_default().to_string(),
                hops,
                limit,
            )),
            _ => None,
        })
        .collect();
    assert_eq!(hit, [("Work/B".to_string(), 3, 3)]);

    // A resume re-enters the child at its hop-limit checkpoint, which trips again.
    assert!(matches!(
        graph.resume("t", go(), store.clone()).await,
        GraphExecutionResult::Failed(StrataError::NodeFailed { .. })
    ));
    assert_eq!((calls.of("A"), calls.of("B")), (2, 1));
}

#[tokio::test]
async fn resuming_a_hop_limit_checkpoint_trips_again_unless_the_resume_raises_the_limit() {
    let calls = Calls::default();
    let graph = looping(&calls, i32::MAX, false)
        .compile_with(limit(5))
        .unwrap();
    let store = store();
    tripped(graph.run("t", job(), store.clone()).await);

    assert_eq!(
        tripped(graph.resume("t", go(), store.clone()).await),
        ("B".to_string(), 5, 5)
    );
    assert_eq!((calls.of("A"), calls.of("B")), (3, 2));
    let latest = store.latest("t");
    assert_eq!(meta(&latest, "error_kind"), Some("hop_limit"));
    assert_eq!(latest.resumed_at, None);

    // Two more hops (B, A), counted on from the stored five.
    assert_eq!(
        tripped(
            graph
                .resume_with(
                    "t",
                    go(),
                    store.clone(),
                    RunConfig::new().with_max_node_hops(7)
                )
                .await
        ),
        ("B".to_string(), 7, 7)
    );
    assert_eq!((calls.of("A"), calls.of("B")), (4, 3));
}

#[tokio::test]
async fn cancellation_wins_and_a_cancelled_checkpoint_counts_the_hops_before_its_node() {
    let graph = looping(&Calls::default(), i32::MAX, false)
        .compile_with(limit(5))
        .unwrap();
    let store = store();
    tripped(graph.run("t", job(), store.clone()).await);
    let token = CancellationToken::new();
    token.cancel();

    let result = graph
        .resume_with(
            "t",
            go(),
            store.clone(),
            RunConfig::new().with_cancellation(token),
        )
        .await;

    assert!(matches!(result, GraphExecutionResult::Cancelled { .. }));
    let checkpoint = store.latest("t");
    assert_eq!(checkpoint.status, CheckpointStatus::Cancelled);
    assert_eq!(checkpoint.next_node.as_deref(), Some("B"));
    assert_eq!(hops(&checkpoint), Some("5"));
}

#[tokio::test]
async fn a_malformed_hop_count_refuses_the_resume_before_anything_is_stamped() {
    let graph = looping(&Calls::default(), i32::MAX, true)
        .compile_with(limit(5))
        .unwrap();
    let store = store();
    graph.run("t", job(), store.clone()).await;
    let mut edited = store.latest("t");
    edited.metadata = Some(HashMap::from([(
        "strata.hops".to_string(),
        "-1".to_string(),
    )]));
    store.put(edited);

    let result = graph.resume("t", go(), store.clone()).await;

    match result {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => assert_eq!(
            message,
            "checkpoint for thread 't' has an invalid 'strata.hops' value '-1'"
        ),
        _ => panic!("expected the resume to be refused"),
    }
    assert_eq!(store.latest("t").resumed_at, None);
}

#[tokio::test]
async fn a_per_run_limit_replaces_the_compiled_one_at_the_outermost_graph_only() {
    let lower = looping(&Calls::default(), i32::MAX, false)
        .compile_with(limit(10))
        .unwrap()
        .run_with("t", job(), store(), RunConfig::new().with_max_node_hops(3))
        .await;
    assert_eq!(tripped(lower), ("B".to_string(), 3, 3));

    let set = looping(&Calls::default(), i32::MAX, false)
        .compile()
        .unwrap()
        .run_with("t", job(), store(), RunConfig::new().with_max_node_hops(2))
        .await;
    assert_eq!(tripped(set), ("A".to_string(), 2, 2));

    // The child's four nodes don't count against the run's limit of 3, and it
    // has none of its own.
    completed(
        gather_work_end(Arc::new(four_steps().compile().unwrap()))
            .compile()
            .unwrap()
            .run_with("t", job(), store(), RunConfig::new().with_max_node_hops(3))
            .await,
    );
}

#[tokio::test]
async fn a_limit_of_zero_is_refused() {
    match looping(&Calls::default(), 1, false).compile_with(limit(0)) {
        Err(StrataError::CompilationFailed(message)) => {
            assert_eq!(message, "max_node_hops must be at least 1")
        }
        _ => panic!("expected CompilationFailed"),
    }

    let refused = looping(&Calls::default(), 1, false)
        .compile()
        .unwrap()
        .run_with("t", job(), store(), RunConfig::new().with_max_node_hops(0))
        .await;
    match refused {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
            assert_eq!(message, "a run's max_node_hops must be at least 1")
        }
        _ => panic!("expected the run to be refused"),
    }
}

#[tokio::test]
async fn the_trip_is_a_run_event_between_the_last_node_and_the_checkpoint() {
    let graph = looping(&Calls::default(), i32::MAX, false)
        .compile_with(limit(3))
        .unwrap();
    let (observer, events) = RunEventChannel::new();

    graph
        .run_with(
            "t",
            job(),
            store(),
            RunConfig::new().with_observer(observer),
        )
        .await;

    let described: Vec<String> = drain(events)
        .await
        .iter()
        .map(|e| {
            let path = e.node_path.as_deref().unwrap_or_default();
            match &e.kind {
                RunEventKind::RunStarted { .. } => "RunStarted".to_string(),
                RunEventKind::NodeStarted => format!("NodeStarted {path}"),
                RunEventKind::NodeCompleted { .. } => format!("NodeCompleted {path}"),
                RunEventKind::HopLimitExceeded { hops, limit } => format!(
                    "HopLimitExceeded {path} {hops}/{limit} {}",
                    e.attempt.map_or("-".to_string(), |a| a.to_string())
                ),
                RunEventKind::CheckpointSaved { status, .. } => {
                    format!("CheckpointSaved {status:?}")
                }
                RunEventKind::RunCompleted { outcome, error, .. } => {
                    format!(
                        "RunCompleted {outcome:?} {}",
                        error.as_deref().unwrap_or("")
                    )
                }
                _ => "other".to_string(),
            }
        })
        .collect();

    assert_eq!(
        described,
        [
            "RunStarted",
            "NodeStarted A",
            "NodeCompleted A",
            "NodeStarted B",
            "NodeCompleted B",
            "NodeStarted A",
            "NodeCompleted A",
            "HopLimitExceeded B 3/3 -",
            "CheckpointSaved Failed",
            "RunCompleted Failed hop limit of 3 reached: node 'B' would be hop 4",
        ]
    );
}

#[tokio::test]
async fn a_streaming_run_sends_nothing_for_the_refused_node() {
    let graph = Arc::new(
        looping(&Calls::default(), i32::MAX, false)
            .compile_with(limit(2))
            .unwrap(),
    );

    let (handle, mut chunks) = graph.run_streaming("t", job(), store());
    let mut started = Vec::new();
    while let Some(chunk) = chunks.recv().await {
        if chunk.chunk_type == StreamChunkType::NodeStart {
            started.push(chunk.node_name.to_string());
        }
    }

    tripped(handle.await.unwrap());
    assert_eq!(started, ["A", "B"]);
}
