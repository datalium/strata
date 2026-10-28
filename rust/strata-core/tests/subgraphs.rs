//! Subgraph composition (`design/15-Subgraph-Composition.md`), exercised
//! through the public API only. The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/SubgraphTests.cs` — keep the two in step.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use datalium_strata::BoxFuture;

use datalium_strata::StrataError;
use datalium_strata::checkpoint::{
    CheckpointStore, CheckpointStoreError, GraphExecutionResult, HumanSignal,
};
use datalium_strata::graph::{CompiledGraph, NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::schema::{Checkpoint, CheckpointStatus};
use datalium_strata::streaming::StreamChunkType;
use serde::{Deserialize, Serialize};

/// The parent's state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Order {
    amount: i32,
    log: Vec<String>,
}

fn order(amount: i32) -> Order {
    Order {
        amount,
        log: Vec::new(),
    }
}

/// A child's state — a different type from the parent's on purpose.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Job {
    n: i32,
    trail: Vec<String>,
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

impl TestStore {
    fn thread_ids(&self) -> Vec<String> {
        let mut ids: Vec<_> = self.checkpoints.lock().unwrap().keys().cloned().collect();
        ids.sort();
        ids
    }
}

fn step(label: &'static str, add: i32) -> impl Fn(Job) -> Job + Send + Sync + 'static {
    move |mut job: Job| {
        job.n += add;
        job.trail.push(label.to_string());
        job
    }
}

/// `Double` -> `AddOne`: `n -> 2n + 1`.
fn arithmetic_child() -> CompiledGraph<Job> {
    StateGraph::<Job>::new()
        .add_node("Double", |mut job: Job| {
            job.n *= 2;
            job.trail.push("double".to_string());
            job
        })
        .add_node("AddOne", step("add_one", 1))
        .add_edge("Double", "AddOne")
        .compile()
        .unwrap()
}

fn to_job(order: &Order) -> Job {
    Job {
        n: order.amount,
        trail: Vec::new(),
    }
}

fn from_job(entry: &Order, job: Job) -> Order {
    let mut next = entry.clone();
    next.amount = job.n;
    next.log.extend(job.trail);
    next
}

fn note(text: &'static str) -> impl Fn(Order) -> Order + Send + Sync + 'static {
    move |mut order: Order| {
        order.log.push(text.to_string());
        order
    }
}

/// `Prepare` -> `Work` (the subgraph) -> `Finish`.
fn graph_around(
    child: impl Into<Arc<CompiledGraph<Job>>>,
    policy: Option<RecoveryPolicy>,
) -> Result<CompiledGraph<Order>, StrataError> {
    let mut graph = StateGraph::<Order>::new()
        .add_node("Prepare", note("prepared"))
        .add_subgraph("Work", child, to_job, from_job)
        .add_node("Finish", note("finished"))
        .add_edge("Prepare", "Work")
        .add_edge("Work", "Finish");
    if let Some(policy) = policy {
        // A fallback node has no ordinary edge into it, so it's only added
        // when a policy actually routes there (otherwise it'd be an orphan).
        if policy.fallback.is_some() {
            graph = graph.add_node("Fallback", note("fell back"));
        }
        graph = graph.set_recovery_policy("Work", policy);
    }
    graph.compile()
}

fn completed<S>(result: GraphExecutionResult<S>) -> S {
    match result {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(err) => panic!("expected Completed, got Failed({err:?})"),
        GraphExecutionResult::Interrupted { .. } => panic!("expected Completed, got Interrupted"),
        GraphExecutionResult::Cancelled { node, .. } => {
            panic!("expected Completed, got Cancelled at {node}")
        }
    }
}

/// A child whose only node fails `failures` times (with `error`), then succeeds.
/// `calls` counts every attempt.
fn flaky_child(
    calls: Arc<AtomicU32>,
    failures: u32,
    error: fn(String) -> NodeError,
    own_retries: u32,
) -> CompiledGraph<Job> {
    StateGraph::<Job>::new()
        .add_fallible_node("Flaky", move |job: &Job| {
            let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call <= failures {
                return Err(error(format!("call {call} failed")));
            }
            let mut next = job.clone();
            next.n += 100;
            next.trail.push("flaky".to_string());
            Ok(next)
        })
        .set_recovery_policy("Flaky", RecoveryPolicy::retry(own_retries))
        .compile()
        .unwrap()
}

#[tokio::test]
async fn a_child_graph_with_its_own_state_type_runs_as_one_node() {
    let graph = graph_around(arithmetic_child(), None).unwrap();

    let result = completed(
        graph
            .run("t", order(5), Arc::new(TestStore::default()))
            .await,
    );

    assert_eq!(result.amount, 11);
    assert_eq!(
        result.log,
        vec!["prepared", "double", "add_one", "finished"]
    );
}

#[tokio::test]
async fn the_output_mapper_receives_the_state_the_node_started_with() {
    let child = StateGraph::<Job>::new()
        .add_node("Only", step("only", 1))
        .compile()
        .unwrap();
    let graph = StateGraph::<Order>::new()
        .add_node("Prepare", note("prepared"))
        .add_subgraph("Work", child, to_job, |entry: &Order, job: Job| Order {
            // Keeps the parent's own log rather than the child's.
            amount: job.n,
            log: entry.log.clone(),
        })
        .add_edge("Prepare", "Work")
        .compile()
        .unwrap();

    let result = completed(
        graph
            .run("t", order(1), Arc::new(TestStore::default()))
            .await,
    );

    assert_eq!(result.amount, 2);
    assert_eq!(result.log, vec!["prepared"]);
}

#[tokio::test]
async fn the_childs_own_conditional_edges_run_inside_the_node() {
    // A child that loops (guarded) until n >= 10.
    let child = StateGraph::<Job>::new()
        .add_node("Grow", step("grow", 4))
        .add_node("Done", step("done", 0))
        .add_conditional_edge("Grow", "Grow", |job: &Job| job.n < 10)
        .add_edge("Grow", "Done")
        .compile()
        .unwrap();
    let graph = graph_around(child, None).unwrap();

    let result = completed(
        graph
            .run("t", order(1), Arc::new(TestStore::default()))
            .await,
    );

    assert_eq!(result.amount, 13);
    assert_eq!(
        result.log,
        vec!["prepared", "grow", "grow", "grow", "done", "finished"]
    );
}

#[tokio::test]
async fn a_child_recovers_itself_and_the_parent_never_sees_the_failure() {
    let calls = Arc::new(AtomicU32::new(0));
    // Fails twice, succeeds on the third attempt; the child's own policy allows two retries.
    let child = flaky_child(calls.clone(), 2, NodeError::Transient, 2);
    let graph = graph_around(child, Some(RecoveryPolicy::default())).unwrap();
    let store = Arc::new(TestStore::default());

    let result = completed(graph.run("t", order(1), store.clone()).await);

    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(result.amount, 101);
    assert_eq!(result.log, vec!["prepared", "flaky", "finished"]);
    assert!(store.thread_ids().is_empty(), "no checkpoint was written");
}

#[tokio::test]
async fn a_fatal_child_failure_fails_the_parent_node_and_persists_the_parents_state() {
    let calls = Arc::new(AtomicU32::new(0));
    let child = flaky_child(calls.clone(), u32::MAX, NodeError::Fatal, 5);
    let graph = graph_around(child, None).unwrap();
    let store = Arc::new(TestStore::default());

    match graph.run("order-1", order(1), store.clone()).await {
        GraphExecutionResult::Failed(StrataError::NodeFailed {
            node,
            attempts,
            message,
        }) => {
            assert_eq!(node, "Work");
            assert_eq!(attempts, 1);
            assert_eq!(
                message,
                "subgraph 'Work' failed at 'Flaky' after 1 attempt(s): call 1 failed"
            );
        }
        other => panic!(
            "expected NodeFailed, got {}",
            match other {
                GraphExecutionResult::Completed(_) => "Completed".to_string(),
                GraphExecutionResult::Interrupted { .. } => "Interrupted".to_string(),
                GraphExecutionResult::Cancelled { .. } => "Cancelled".to_string(),
                GraphExecutionResult::Failed(err) => format!("{err:?}"),
            }
        ),
    }

    // The child's own policy would allow retries, but a fatal error skips them.
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // The one checkpoint is the *parent's*, holding the parent's state from
    // before the subgraph node ran.
    assert_eq!(store.thread_ids(), vec!["order-1"]);
    let checkpoint = store.load("order-1").unwrap().unwrap();
    assert_eq!(checkpoint.status, CheckpointStatus::Failed);
    assert_eq!(checkpoint.suspended_node.as_deref(), Some("Work"));
    let frozen: Order = serde_json::from_value(checkpoint.frozen_state).unwrap();
    assert_eq!(
        frozen,
        Order {
            amount: 1,
            log: vec!["prepared".to_string()],
        }
    );
    let metadata = checkpoint.metadata.unwrap();
    assert_eq!(metadata["error_kind"], "fatal");
}

#[tokio::test]
async fn a_transient_child_failure_is_transient_to_the_parent_and_a_retry_reruns_the_whole_child() {
    let calls = Arc::new(AtomicU32::new(0));
    // Every attempt fails. The child retries once (2 calls per child run); the
    // parent retries twice (3 child runs) => 6 calls.
    let child = flaky_child(calls.clone(), u32::MAX, NodeError::Transient, 1);
    let graph = graph_around(child, Some(RecoveryPolicy::retry(2))).unwrap();
    let store = Arc::new(TestStore::default());

    match graph.run("t", order(1), store.clone()).await {
        GraphExecutionResult::Failed(StrataError::NodeFailed { attempts, .. }) => {
            assert_eq!(attempts, 3);
        }
        _ => panic!("expected NodeFailed"),
    }

    assert_eq!(calls.load(Ordering::SeqCst), 6);
    let checkpoint = store.load("t").unwrap().unwrap();
    assert_eq!(checkpoint.metadata.unwrap()["error_kind"], "transient");
}

#[tokio::test]
async fn a_fatal_child_failure_is_not_retried_by_the_parents_policy() {
    let calls = Arc::new(AtomicU32::new(0));
    let child = flaky_child(calls.clone(), u32::MAX, NodeError::Fatal, 0);
    let graph = graph_around(child, Some(RecoveryPolicy::retry(5))).unwrap();

    let result = graph
        .run("t", order(1), Arc::new(TestStore::default()))
        .await;

    assert!(matches!(
        result,
        GraphExecutionResult::Failed(StrataError::NodeFailed { attempts: 1, .. })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_transient_child_failure_recovers_when_the_parent_retries() {
    let calls = Arc::new(AtomicU32::new(0));
    // The child has no policy of its own; the first run fails, the second succeeds.
    let child = flaky_child(calls.clone(), 1, NodeError::Transient, 0);
    let graph = graph_around(child, Some(RecoveryPolicy::retry(1))).unwrap();

    let result = completed(
        graph
            .run("t", order(1), Arc::new(TestStore::default()))
            .await,
    );

    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(result.log, vec!["prepared", "flaky", "finished"]);
}

#[tokio::test]
async fn a_fallback_runs_on_the_state_from_before_the_subgraph() {
    // The child does real work (Double), then fails: none of it may leak.
    let child = StateGraph::<Job>::new()
        .add_node("Double", step("double", 50))
        .add_fallible_node("Boom", |_: &Job| Err(NodeError::Fatal("boom".to_string())))
        .add_edge("Double", "Boom")
        .compile()
        .unwrap();
    let graph = graph_around(
        child,
        Some(RecoveryPolicy::default().with_fallback("Fallback")),
    )
    .unwrap();
    let store = Arc::new(TestStore::default());

    let result = completed(graph.run("t", order(5), store.clone()).await);

    assert_eq!(result.amount, 5, "the child's partial work is discarded");
    assert_eq!(result.log, vec!["prepared", "fell back"]);
    assert!(store.thread_ids().is_empty());
}

#[tokio::test]
async fn a_child_routing_gap_is_a_fatal_failure() {
    let child = StateGraph::<Job>::new()
        .add_node("Start", step("start", 0))
        .add_node("Never", step("never", 0))
        .add_conditional_edge("Start", "Never", |_: &Job| false)
        .compile()
        .unwrap();
    let graph = graph_around(child, Some(RecoveryPolicy::retry(3))).unwrap();
    let store = Arc::new(TestStore::default());

    match graph.run("t", order(1), store.clone()).await {
        GraphExecutionResult::Failed(StrataError::NodeFailed {
            attempts, message, ..
        }) => {
            assert_eq!(attempts, 1, "retrying can't fix a routing gap");
            assert_eq!(
                message,
                "subgraph 'Work': no outgoing edge condition matched for node 'Start'"
            );
        }
        _ => panic!("expected NodeFailed"),
    }
}

#[tokio::test]
async fn subgraphs_nest() {
    let inner = arithmetic_child();
    // The middle graph runs `inner` and then adds 1000.
    let middle = StateGraph::<Job>::new()
        .add_subgraph("Inner", inner, Clone::clone, |_: &Job, job: Job| job)
        .add_node("Bonus", step("bonus", 1000))
        .add_edge("Inner", "Bonus")
        .compile()
        .unwrap();
    let graph = graph_around(middle, None).unwrap();

    let result = completed(
        graph
            .run("t", order(5), Arc::new(TestStore::default()))
            .await,
    );

    assert_eq!(result.amount, 1011);
    assert_eq!(
        result.log,
        vec!["prepared", "double", "add_one", "bonus", "finished"]
    );
}

#[tokio::test]
async fn a_failure_deep_in_a_nested_subgraph_reaches_the_top_with_its_kind() {
    let calls = Arc::new(AtomicU32::new(0));
    let inner = flaky_child(calls.clone(), u32::MAX, NodeError::Fatal, 0);
    let middle = StateGraph::<Job>::new()
        .add_subgraph("Inner", inner, Clone::clone, |_: &Job, job: Job| job)
        .compile()
        .unwrap();
    let graph = graph_around(middle, Some(RecoveryPolicy::retry(4))).unwrap();

    match graph
        .run("t", order(1), Arc::new(TestStore::default()))
        .await
    {
        GraphExecutionResult::Failed(StrataError::NodeFailed {
            attempts, message, ..
        }) => {
            assert_eq!(attempts, 1, "still fatal two levels up");
            assert_eq!(
                message,
                "subgraph 'Work' failed at 'Inner' after 1 attempt(s): \
                 subgraph 'Inner' failed at 'Flaky' after 1 attempt(s): call 1 failed"
            );
        }
        _ => panic!("expected NodeFailed"),
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn one_child_graph_can_back_several_nodes() {
    let child = Arc::new(arithmetic_child());
    let graph = StateGraph::<Order>::new()
        .add_subgraph("First", child.clone(), to_job, from_job)
        .add_subgraph("Second", child, to_job, from_job)
        .add_edge("First", "Second")
        .compile()
        .unwrap();

    let result = completed(
        graph
            .run("t", order(1), Arc::new(TestStore::default()))
            .await,
    );

    // 1 -> 3 -> 7
    assert_eq!(result.amount, 7);
}

#[tokio::test]
async fn a_subgraph_can_be_the_parents_interrupt_boundary() {
    let graph = StateGraph::<Order>::new()
        .add_node("Prepare", note("prepared"))
        .add_subgraph("Work", arithmetic_child(), to_job, from_job)
        .add_node("Finish", note("finished"))
        .add_edge("Prepare", "Work")
        .add_edge("Work", "Finish")
        .add_interrupt_boundary("Work")
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());

    match graph.run("t", order(5), store.clone()).await {
        GraphExecutionResult::Interrupted {
            suspended_node,
            next_node,
            frozen_state,
            ..
        } => {
            assert_eq!(suspended_node, "Work");
            assert_eq!(next_node, "Finish");
            assert_eq!(frozen_state.amount, 11);
        }
        _ => panic!("expected Interrupted"),
    }

    let result = completed(
        graph
            .resume("t", HumanSignal::default(), store.clone())
            .await,
    );
    assert_eq!(result.amount, 11);
    assert_eq!(
        result.log,
        vec!["prepared", "double", "add_one", "finished"]
    );
}

#[tokio::test]
async fn a_supervisor_routes_to_one_of_several_subgraphs() {
    let small = StateGraph::<Job>::new()
        .add_node("Small", step("small", 1))
        .compile()
        .unwrap();
    let large = StateGraph::<Job>::new()
        .add_node("Large", step("large", 1000))
        .compile()
        .unwrap();
    let graph = StateGraph::<Order>::new()
        .add_node("Supervisor", note("routed"))
        .add_subgraph("SmallAgent", small, to_job, from_job)
        .add_subgraph("LargeAgent", large, to_job, from_job)
        .add_conditional_edge("Supervisor", "LargeAgent", |o: &Order| o.amount >= 100)
        .add_edge("Supervisor", "SmallAgent")
        .compile()
        .unwrap();

    let small_result = completed(
        graph
            .run("a", order(5), Arc::new(TestStore::default()))
            .await,
    );
    let large_result = completed(
        graph
            .run("b", order(500), Arc::new(TestStore::default()))
            .await,
    );

    assert_eq!(small_result.log, vec!["routed", "small"]);
    assert_eq!(large_result.log, vec!["routed", "large"]);
    assert_eq!(large_result.amount, 1500);
}

#[tokio::test]
async fn a_boundary_naming_a_node_the_child_does_not_have_never_triggers_so_it_is_accepted() {
    let child = StateGraph::<Job>::new()
        .add_node("Only", step("only", 1))
        .add_interrupt_boundary("Ghost")
        .compile()
        .unwrap();

    let graph = graph_around(child, None).unwrap();

    assert_eq!(
        completed(
            graph
                .run("t", order(1), Arc::new(TestStore::default()))
                .await
        )
        .amount,
        2
    );
}

#[test]
fn a_recovery_policy_can_be_set_on_a_subgraph_node() {
    // A subgraph counts as fallible, so `.compile()` accepts a policy on it.
    assert!(graph_around(arithmetic_child(), Some(RecoveryPolicy::retry(1))).is_ok());
}

#[tokio::test]
async fn run_streaming_reports_a_plain_childs_nodes_named_under_the_subgraph() {
    let graph = Arc::new(graph_around(arithmetic_child(), None).unwrap());
    let store: Arc<dyn CheckpointStore> = Arc::new(TestStore::default());

    let (handle, mut receiver) = graph.run_streaming("t", order(5), store);
    let mut chunks = Vec::new();
    while let Some(chunk) = receiver.recv().await {
        chunks.push((chunk.chunk_type, chunk.node_name.to_string()));
    }

    assert_eq!(completed(handle.await.unwrap()).amount, 11);
    // The child's nodes are visible, qualified by the subgraph node, and sit
    // between its own NodeStart and NodeEnd. `design/15` has the rules.
    let work: Vec<_> = chunks
        .iter()
        .filter(|(_, node)| node == "Work" || node.starts_with("Work/"))
        .cloned()
        .collect();
    assert_eq!(
        work,
        vec![
            (StreamChunkType::NodeStart, "Work".to_string()),
            (StreamChunkType::NodeStart, "Work/Double".to_string()),
            (StreamChunkType::NodeEnd, "Work/Double".to_string()),
            (StreamChunkType::NodeStart, "Work/AddOne".to_string()),
            (StreamChunkType::NodeEnd, "Work/AddOne".to_string()),
            (StreamChunkType::NodeEnd, "Work".to_string()),
        ]
    );
}

#[tokio::test]
async fn a_panic_in_a_child_unwinds_through_the_engine_and_is_not_retried() {
    let calls = Arc::new(AtomicU32::new(0));
    let counted = calls.clone();
    let child = StateGraph::<Job>::new()
        .add_node("Bug", move |_: Job| {
            counted.fetch_add(1, Ordering::SeqCst);
            panic!("child bug")
        })
        .compile()
        .unwrap();
    let graph = graph_around(child, Some(RecoveryPolicy::retry(3))).unwrap();

    let outcome = tokio::spawn(async move {
        graph
            .run("t", order(1), Arc::new(TestStore::default()))
            .await
    })
    .await;

    assert!(
        outcome.is_err_and(|error| error.is_panic()),
        "the panic must not be swallowed or retried"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
