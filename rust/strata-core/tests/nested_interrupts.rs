//! Interrupt boundaries inside a subgraph (`design/16-Nested-Interrupts.md`),
//! exercised through the public API only. The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/NestedInterruptTests.cs` — keep the two in step.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use datalium_strata::BoxFuture;

use datalium_strata::StrataError;
use datalium_strata::checkpoint::{
    CheckpointStore, CheckpointStoreError, GraphExecutionResult, HumanSignal, LevelSignals,
};
use datalium_strata::graph::{CompiledGraph, NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::schema::{
    Checkpoint, CheckpointStatus, DECISION_CHECKPOINT_SCHEMA_VERSION,
    NESTED_CHECKPOINT_SCHEMA_VERSION, SCHEMA_VERSION,
};
use datalium_strata::streaming::StreamChunkType;
use serde::{Deserialize, Serialize};

/// The parent's state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Order {
    amount: i32,
    log: Vec<String>,
    approved_by: Option<String>,
}

fn order(amount: i32) -> Order {
    Order {
        amount,
        log: Vec::new(),
        approved_by: None,
    }
}

/// A child's state — a different type from the parent's on purpose.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Job {
    n: i32,
    trail: Vec<String>,
    approved_by: Option<String>,
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

fn step(label: &'static str) -> impl Fn(Job) -> Job + Send + Sync + 'static {
    move |mut job: Job| {
        job.trail.push(label.to_string());
        job
    }
}

fn note(text: &'static str) -> impl Fn(Order) -> Order + Send + Sync + 'static {
    move |mut order: Order| {
        order.log.push(text.to_string());
        order
    }
}

fn to_job(order: &Order) -> Job {
    Job {
        n: order.amount,
        trail: Vec::new(),
        approved_by: None,
    }
}

fn from_job(entry: &Order, job: Job) -> Order {
    let mut next = entry.clone();
    next.amount = job.n;
    next.log.extend(job.trail);
    next.approved_by = job.approved_by;
    next
}

/// `Draft` (n * 2) -> `Approve` (an interrupt boundary) -> `Send` (n + 1).
fn review_child() -> CompiledGraph<Job> {
    StateGraph::<Job>::new()
        .add_node("Draft", |mut job: Job| {
            job.n *= 2;
            job.trail.push("draft".to_string());
            job
        })
        .add_node("Approve", step("approve"))
        .add_interrupt_boundary("Approve")
        .add_node("Send", |mut job: Job| {
            job.n += 1;
            job.trail.push("send".to_string());
            job
        })
        .add_edge("Draft", "Approve")
        .add_edge("Approve", "Send")
        .compile()
        .unwrap()
}

/// `Prepare` -> `Work` (the subgraph) -> `Finish`.
fn parent_around(child: impl Into<Arc<CompiledGraph<Job>>>) -> StateGraph<Order> {
    StateGraph::<Order>::new()
        .add_node("Prepare", note("prepared"))
        .add_subgraph("Work", child, to_job, from_job)
        .add_node("Finish", note("finished"))
        .add_edge("Prepare", "Work")
        .add_edge("Work", "Finish")
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

/// The `(suspended_node, next_node, frozen_state)` of an interrupted run.
fn interrupted<S>(result: GraphExecutionResult<S>) -> (String, String, S) {
    match result {
        GraphExecutionResult::Interrupted {
            suspended_node,
            next_node,
            frozen_state,
            ..
        } => (suspended_node, next_node, frozen_state),
        GraphExecutionResult::Completed(_) => panic!("expected Interrupted, got Completed"),
        GraphExecutionResult::Failed(err) => panic!("expected Interrupted, got Failed({err:?})"),
        GraphExecutionResult::Cancelled { node, .. } => {
            panic!("expected Interrupted, got Cancelled at {node}")
        }
    }
}

fn approve_as(who: &str) -> HumanSignal {
    HumanSignal {
        is_approved: true,
        reviewer_token: "token".to_string(),
        field_overrides: HashMap::from([("approved_by".to_string(), who.to_string())]),
        ..Default::default()
    }
}

#[tokio::test]
async fn a_child_boundary_freezes_the_whole_run_at_the_subgraph_node() {
    let graph = parent_around(review_child()).compile().unwrap();

    let (suspended, next, frozen) = interrupted(
        graph
            .run("t", order(5), Arc::new(TestStore::default()))
            .await,
    );

    assert_eq!(suspended, "Work");
    // A resume re-enters the subgraph node, so that is where it continues.
    assert_eq!(next, "Work");
    // The state the node started with, not the child's.
    assert_eq!(frozen.amount, 5);
    assert_eq!(frozen.log, vec!["prepared"]);
}

#[tokio::test]
async fn the_parent_checkpoint_nests_the_childs() {
    let graph = parent_around(review_child()).compile().unwrap();
    let store = Arc::new(TestStore::default());
    graph.run("t", order(5), store.clone()).await;

    let parent = store.load("t").unwrap().unwrap();
    assert_eq!(parent.schema_version, NESTED_CHECKPOINT_SCHEMA_VERSION);
    assert_eq!(parent.status, CheckpointStatus::Interrupted);
    assert_eq!(parent.suspended_node.as_deref(), Some("Work"));
    assert_eq!(parent.next_node.as_deref(), Some("Work"));
    assert_eq!(parent.frozen_state["amount"], 5);

    let child = parent.nested.expect("the child's checkpoint is nested");
    // A nested checkpoint is named after the subgraph node it belongs to.
    assert_eq!(child.thread_id, "Work");
    assert_eq!(child.schema_version, SCHEMA_VERSION);
    assert_eq!(child.status, CheckpointStatus::Interrupted);
    assert_eq!(child.suspended_node.as_deref(), Some("Approve"));
    assert_eq!(child.next_node.as_deref(), Some("Send"));
    assert_eq!(child.frozen_state["n"], 10);
    assert_eq!(
        child.frozen_state["trail"],
        serde_json::json!(["draft", "approve"])
    );
    assert!(child.nested.is_none());
}

#[tokio::test]
async fn a_run_with_no_child_freeze_writes_the_same_checkpoint_as_before() {
    // A parent's own boundary still writes a plain 1.0.0 checkpoint with no
    // `nested` key at all, so nothing that predates nesting changes.
    let graph = StateGraph::<Order>::new()
        .add_node("Prepare", note("prepared"))
        .add_interrupt_boundary("Prepare")
        .add_node("Finish", note("finished"))
        .add_edge("Prepare", "Finish")
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    graph.run("t", order(1), store.clone()).await;

    let checkpoint = store.load("t").unwrap().unwrap();

    assert_eq!(checkpoint.schema_version, SCHEMA_VERSION);
    assert!(checkpoint.nested.is_none());
    assert!(
        !serde_json::to_string(&checkpoint)
            .unwrap()
            .contains("nested")
    );
}

#[tokio::test]
async fn resume_continues_the_child_from_where_it_froze_then_the_parent() {
    let inputs = Arc::new(AtomicU32::new(0));
    let counted = inputs.clone();
    let drafts = Arc::new(AtomicU32::new(0));
    let counted_drafts = drafts.clone();
    let child = StateGraph::<Job>::new()
        .add_node("Draft", move |mut job: Job| {
            counted_drafts.fetch_add(1, Ordering::SeqCst);
            job.n *= 2;
            job.trail.push("draft".to_string());
            job
        })
        .add_node("Approve", step("approve"))
        .add_interrupt_boundary("Approve")
        .add_node("Send", |mut job: Job| {
            job.n += 1;
            job.trail.push("send".to_string());
            job
        })
        .add_edge("Draft", "Approve")
        .add_edge("Approve", "Send")
        .compile()
        .unwrap();
    let graph = StateGraph::<Order>::new()
        .add_node("Prepare", note("prepared"))
        .add_subgraph(
            "Work",
            child,
            move |order: &Order| {
                counted.fetch_add(1, Ordering::SeqCst);
                to_job(order)
            },
            from_job,
        )
        .add_node("Finish", note("finished"))
        .add_edge("Prepare", "Work")
        .add_edge("Work", "Finish")
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", order(5), store.clone()).await);

    let state = completed(
        graph
            .resume("t", HumanSignal::default(), store.clone())
            .await,
    );

    assert_eq!(state.amount, 11);
    assert_eq!(
        state.log,
        vec!["prepared", "draft", "approve", "send", "finished"]
    );
    // The child picked up after `Approve`: neither its input mapper nor
    // `Draft` ran a second time.
    assert_eq!(inputs.load(Ordering::SeqCst), 1);
    assert_eq!(drafts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn field_overrides_reach_the_childs_state_and_not_the_parents() {
    let graph = parent_around(review_child()).compile().unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", order(5), store.clone()).await);
    let signal = HumanSignal {
        is_approved: true,
        reviewer_token: "token".to_string(),
        field_overrides: HashMap::from([
            ("approved_by".to_string(), "admin_01".to_string()),
            // Only the parent has a `log`, and it is not what was reviewed.
            ("log".to_string(), "tampered".to_string()),
        ]),
        ..Default::default()
    };

    let state = completed(graph.resume("t", signal, store.clone()).await);

    // `approved_by` was written into the child's state and mapped up; the
    // parent's own `log` was left alone.
    assert_eq!(state.approved_by.as_deref(), Some("admin_01"));
    assert_eq!(
        state.log,
        vec!["prepared", "draft", "approve", "send", "finished"]
    );
}

#[tokio::test]
async fn the_resumed_checkpoint_keeps_its_nested_child_and_is_stamped() {
    let graph = parent_around(review_child()).compile().unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", order(5), store.clone()).await);

    completed(
        graph
            .resume("t", HumanSignal::default(), store.clone())
            .await,
    );

    let resumed = store.load("t").unwrap().unwrap();
    assert!(resumed.resumed_at.is_some());
    assert!(resumed.nested.is_some());
}

#[tokio::test]
async fn a_second_resume_of_the_same_nested_checkpoint_is_refused() {
    let graph = parent_around(review_child()).compile().unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", order(5), store.clone()).await);
    completed(
        graph
            .resume("t", HumanSignal::default(), store.clone())
            .await,
    );

    match graph
        .resume("t", HumanSignal::default(), store.clone())
        .await
    {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
            assert!(message.contains("already resumed"), "{message}")
        }
        _ => panic!("expected Failed(ExecutionAborted)"),
    }
}

#[tokio::test]
async fn a_second_boundary_in_the_child_freezes_again_with_a_fresh_nested_checkpoint() {
    let child = StateGraph::<Job>::new()
        .add_node("First", step("first"))
        .add_interrupt_boundary("First")
        .add_node("Second", step("second"))
        .add_interrupt_boundary("Second")
        .add_node("Last", step("last"))
        .add_edge("First", "Second")
        .add_edge("Second", "Last")
        .compile()
        .unwrap();
    let graph = parent_around(child).compile().unwrap();
    let store = Arc::new(TestStore::default());

    interrupted(graph.run("t", order(1), store.clone()).await);
    assert_eq!(
        store
            .load("t")
            .unwrap()
            .unwrap()
            .nested
            .unwrap()
            .suspended_node
            .as_deref(),
        Some("First")
    );

    let (suspended, _, _) = interrupted(
        graph
            .resume("t", HumanSignal::default(), store.clone())
            .await,
    );
    assert_eq!(suspended, "Work");
    let second = store.load("t").unwrap().unwrap();
    assert!(second.resumed_at.is_none(), "a new freeze, not the old one");
    assert_eq!(
        second.nested.unwrap().suspended_node.as_deref(),
        Some("Second")
    );

    let state = completed(
        graph
            .resume("t", HumanSignal::default(), store.clone())
            .await,
    );
    assert_eq!(
        state.log,
        vec!["prepared", "first", "second", "last", "finished"]
    );
}

#[tokio::test]
async fn nesting_goes_to_any_depth() {
    // Work (subgraph) -> child graph's `Inner` (subgraph) -> grandchild `Review`.
    let child = StateGraph::<Job>::new()
        .add_node("Open", step("open"))
        .add_subgraph(
            "Inner",
            review_child(),
            |job: &Job| Job {
                n: job.n,
                trail: Vec::new(),
                approved_by: None,
            },
            |entry: &Job, inner: Job| {
                let mut next = entry.clone();
                next.n = inner.n;
                next.trail.extend(inner.trail);
                next.approved_by = inner.approved_by;
                next
            },
        )
        .add_node("Close", step("close"))
        .add_edge("Open", "Inner")
        .add_edge("Inner", "Close")
        .compile()
        .unwrap();
    let graph = parent_around(child).compile().unwrap();
    let store = Arc::new(TestStore::default());

    let (suspended, next, _) = interrupted(graph.run("t", order(5), store.clone()).await);
    assert_eq!((suspended.as_str(), next.as_str()), ("Work", "Work"));

    let parent = store.load("t").unwrap().unwrap();
    let child_checkpoint = parent.nested.unwrap();
    assert_eq!(child_checkpoint.thread_id, "Work");
    assert_eq!(child_checkpoint.suspended_node.as_deref(), Some("Inner"));
    assert_eq!(
        child_checkpoint.schema_version,
        NESTED_CHECKPOINT_SCHEMA_VERSION
    );
    let grandchild = child_checkpoint.nested.unwrap();
    assert_eq!(grandchild.thread_id, "Inner");
    assert_eq!(grandchild.suspended_node.as_deref(), Some("Approve"));
    assert_eq!(grandchild.schema_version, SCHEMA_VERSION);

    let state = completed(
        graph
            .resume("t", approve_as("admin_02"), store.clone())
            .await,
    );
    assert_eq!(
        state.log,
        vec![
            "prepared", "open", "draft", "approve", "send", "close", "finished"
        ]
    );
    assert_eq!(state.amount, 11);
    // The override travelled all the way down to the grandchild and back up.
    assert_eq!(state.approved_by.as_deref(), Some("admin_02"));
}

#[tokio::test]
async fn the_subgraph_node_can_itself_be_a_parent_boundary_after_its_child_completes() {
    let graph = parent_around(review_child())
        .add_interrupt_boundary("Work")
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());

    // First stop: inside the child.
    interrupted(graph.run("t", order(5), store.clone()).await);
    assert!(store.load("t").unwrap().unwrap().nested.is_some());

    // The child completes on resume, so now the parent's own boundary on the
    // node applies: an ordinary checkpoint, with nothing nested.
    let (suspended, next, frozen) = interrupted(
        graph
            .resume("t", HumanSignal::default(), store.clone())
            .await,
    );
    assert_eq!((suspended.as_str(), next.as_str()), ("Work", "Finish"));
    assert_eq!(frozen.amount, 11);
    let plain = store.load("t").unwrap().unwrap();
    assert_eq!(plain.schema_version, SCHEMA_VERSION);
    assert!(plain.nested.is_none());

    let state = completed(
        graph
            .resume("t", HumanSignal::default(), store.clone())
            .await,
    );
    assert_eq!(
        state.log,
        vec!["prepared", "draft", "approve", "send", "finished"]
    );
}

#[tokio::test]
async fn a_child_freeze_is_not_a_failure_so_the_recovery_policy_stays_out_of_it() {
    let graph = parent_around(review_child())
        .add_node("Fallback", note("fell back"))
        .set_recovery_policy("Work", RecoveryPolicy::retry(3).with_fallback("Fallback"))
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());

    let (suspended, _, frozen) = interrupted(graph.run("t", order(5), store.clone()).await);

    assert_eq!(suspended, "Work");
    // No fallback ran and nothing was retried.
    assert_eq!(frozen.log, vec!["prepared"]);
    assert_eq!(
        store.load("t").unwrap().unwrap().status,
        CheckpointStatus::Interrupted
    );
}

#[tokio::test]
async fn a_retry_of_a_resumed_child_resumes_from_the_same_checkpoint_so_nobody_is_asked_twice() {
    let approvals = Arc::new(AtomicU32::new(0));
    let counted = approvals.clone();
    let sends = Arc::new(AtomicU32::new(0));
    let counted_sends = sends.clone();
    let child = StateGraph::<Job>::new()
        .add_node("Approve", move |mut job: Job| {
            counted.fetch_add(1, Ordering::SeqCst);
            job.trail.push("approve".to_string());
            job
        })
        .add_interrupt_boundary("Approve")
        .add_fallible_node("Send", move |job: &Job| {
            if counted_sends.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(NodeError::Transient("mail server busy".to_string()));
            }
            let mut next = job.clone();
            next.trail.push("send".to_string());
            Ok(next)
        })
        .add_edge("Approve", "Send")
        .compile()
        .unwrap();
    let graph = parent_around(child)
        .set_recovery_policy("Work", RecoveryPolicy::retry(1))
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", order(5), store.clone()).await);

    let state = completed(
        graph
            .resume("t", HumanSignal::default(), store.clone())
            .await,
    );

    // `Send` failed once and was retried; the parent's retry re-entered the
    // child at the same point instead of running `Approve` again.
    assert_eq!(sends.load(Ordering::SeqCst), 2);
    assert_eq!(approvals.load(Ordering::SeqCst), 1);
    assert_eq!(state.log, vec!["prepared", "approve", "send", "finished"]);
}

#[tokio::test]
async fn a_child_that_fails_after_resume_persists_a_failed_checkpoint_at_the_subgraph_node() {
    let child = StateGraph::<Job>::new()
        .add_node("Approve", step("approve"))
        .add_interrupt_boundary("Approve")
        .add_fallible_node("Send", |_: &Job| {
            Err(NodeError::Fatal("mailbox closed".to_string()))
        })
        .add_edge("Approve", "Send")
        .compile()
        .unwrap();
    let graph = parent_around(child).compile().unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", order(5), store.clone()).await);

    match graph
        .resume("t", HumanSignal::default(), store.clone())
        .await
    {
        GraphExecutionResult::Failed(StrataError::NodeFailed { node, message, .. }) => {
            assert_eq!(node, "Work");
            assert!(message.contains("mailbox closed"), "{message}");
        }
        _ => panic!("expected Failed(NodeFailed)"),
    }

    let failed = store.load("t").unwrap().unwrap();
    assert_eq!(failed.status, CheckpointStatus::Failed);
    assert_eq!(failed.suspended_node.as_deref(), Some("Work"));
    // It keeps the child's own `Failed` checkpoint, so it can be resumed
    // (see "Resuming a failed thread" below). The child had answered its
    // boundary, so it carries that decision (design/21-Typed-Human-Signals.md),
    // and both levels are written as 1.3.0.
    assert_eq!(failed.schema_version, DECISION_CHECKPOINT_SCHEMA_VERSION);
    let child = failed.nested.expect("the child's failure is nested");
    assert_eq!(child.status, CheckpointStatus::Failed);
    assert_eq!(child.suspended_node.as_deref(), Some("Send"));
}

#[tokio::test]
async fn a_checkpoint_with_an_unsupported_schema_version_is_refused_on_resume() {
    let graph = parent_around(review_child()).compile().unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", order(5), store.clone()).await);
    let mut checkpoint = store.load("t").unwrap().unwrap();
    checkpoint.schema_version = "2.0.0".to_string();
    store.save(checkpoint).unwrap();

    match graph
        .resume("t", HumanSignal::default(), store.clone())
        .await
    {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
            assert!(
                message.contains("unsupported schema_version '2.0.0'"),
                "{message}"
            )
        }
        _ => panic!("expected Failed(ExecutionAborted)"),
    }
}

#[tokio::test]
async fn a_checkpoint_nesting_a_child_under_a_node_that_is_not_a_subgraph_is_refused() {
    let graph = parent_around(review_child()).compile().unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", order(5), store.clone()).await);
    let mut checkpoint = store.load("t").unwrap().unwrap();
    checkpoint.next_node = Some("Prepare".to_string());
    store.save(checkpoint).unwrap();

    match graph
        .resume("t", HumanSignal::default(), store.clone())
        .await
    {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
            assert!(message.contains("'Prepare'"), "{message}");
            assert!(message.contains("not a subgraph node"), "{message}");
        }
        _ => panic!("expected Failed(ExecutionAborted)"),
    }
}

#[tokio::test]
async fn run_streaming_starts_the_subgraph_node_but_does_not_end_it_when_its_child_freezes() {
    let graph = Arc::new(parent_around(review_child()).compile().unwrap());
    let store: Arc<dyn CheckpointStore> = Arc::new(TestStore::default());

    let (handle, mut receiver) = graph.run_streaming("t", order(5), store);
    let mut chunks = Vec::new();
    while let Some(chunk) = receiver.recv().await {
        chunks.push((chunk.chunk_type, chunk.node_name.to_string()));
    }

    let (suspended, _, _) = interrupted(handle.await.unwrap());
    assert_eq!(suspended, "Work");
    assert!(chunks.contains(&(StreamChunkType::NodeStart, "Work".to_string())));
    assert!(!chunks.contains(&(StreamChunkType::NodeEnd, "Work".to_string())));
}

// --- The shared cross-language fixture -------------------------------------
//
// `schema/fixtures/checkpoint-lifecycle/nested.json` is the interop artifact
// for a checkpoint nesting a child's. This proves the engine both produces a
// checkpoint matching it (freeze) and consumes it to resume correctly;
// `NestedInterruptTests.cs` proves the same in C# against the identical file.

fn load_fixture() -> Checkpoint {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/fixtures/checkpoint-lifecycle/nested.json");
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {}: {}", path.display(), e));
    serde_json::from_str(&raw).unwrap()
}

/// Blanks every level's `created_at` (the fixture's is a fixed historical
/// value; a live freeze uses the real current time).
fn without_timestamps(mut checkpoint: Checkpoint) -> Checkpoint {
    checkpoint.created_at = String::new();
    if let Some(decision) = checkpoint.decision.as_mut() {
        decision.decided_at = String::new();
    }
    checkpoint.nested = checkpoint
        .nested
        .map(|child| Box::new(without_timestamps(*child)));
    checkpoint
}

#[tokio::test]
async fn freezing_matches_the_shared_nested_fixture_at_every_level() {
    let graph = parent_around(review_child()).compile().unwrap();
    let store = Arc::new(TestStore::default());

    graph
        .run("thread_nested_demo", order(5), store.clone())
        .await;

    let persisted = store.load("thread_nested_demo").unwrap().unwrap();
    assert_eq!(
        without_timestamps(persisted),
        without_timestamps(load_fixture())
    );
}

#[tokio::test]
async fn resuming_the_shared_nested_fixture_produces_the_expected_completed_state() {
    let graph = parent_around(review_child()).compile().unwrap();
    let store = Arc::new(TestStore::default());
    store.save(load_fixture()).unwrap();

    let state = completed(
        graph
            .resume("thread_nested_demo", approve_as("admin_01"), store.clone())
            .await,
    );

    assert_eq!(
        state,
        Order {
            amount: 11,
            log: vec![
                "prepared".into(),
                "draft".into(),
                "approve".into(),
                "send".into(),
                "finished".into()
            ],
            approved_by: Some("admin_01".to_string()),
        }
    );
}

// --- Where a nested thread is waiting (`Interrupted::waiting_at`) ------------

fn waiting_at<S>(result: &GraphExecutionResult<S>) -> Vec<String> {
    match result {
        GraphExecutionResult::Interrupted { waiting_at, .. } => waiting_at.clone(),
        _ => panic!("expected Interrupted"),
    }
}

/// `Open` -> `Inner` (a subgraph running `grandchild`) -> `Close`. Its `output`
/// mapper keeps an `approved_by` this level was given over the grandchild's, so
/// a signal addressed to this level shows in the result.
fn middle_child(grandchild: CompiledGraph<Job>) -> CompiledGraph<Job> {
    StateGraph::<Job>::new()
        .add_node("Open", step("open"))
        .add_subgraph(
            "Inner",
            grandchild,
            |job: &Job| Job {
                n: job.n,
                trail: Vec::new(),
                approved_by: None,
            },
            |entry: &Job, inner: Job| {
                let mut next = entry.clone();
                next.n = inner.n;
                next.trail.extend(inner.trail);
                next.approved_by = entry.approved_by.clone().or(inner.approved_by);
                next
            },
        )
        .add_node("Close", step("close"))
        .add_edge("Open", "Inner")
        .add_edge("Inner", "Close")
        .compile()
        .unwrap()
}

#[tokio::test]
async fn a_flat_freeze_reports_the_boundary_node_as_where_it_is_waiting() {
    let result = review_child()
        .run("t", to_job(&order(5)), Arc::new(TestStore::default()))
        .await;

    assert_eq!(waiting_at(&result), ["Approve"]);
}

#[tokio::test]
async fn a_nested_freeze_reports_one_node_per_level_from_the_outside_in() {
    let graph = parent_around(review_child()).compile().unwrap();
    let store = Arc::new(TestStore::default());

    let result = graph.run("t", order(5), store.clone()).await;

    assert_eq!(waiting_at(&result), ["Work", "Approve"]);
    // `suspended_node` still names only the subgraph node...
    let (suspended, _, _) = interrupted(result);
    assert_eq!(suspended, "Work");
    // ...and the path is what the stored checkpoint says, too.
    assert_eq!(
        store.load("t").unwrap().unwrap().waiting_at(),
        ["Work", "Approve"]
    );
}

#[tokio::test]
async fn a_freeze_three_levels_down_reports_the_whole_path() {
    let graph = parent_around(middle_child(review_child()))
        .compile()
        .unwrap();

    let result = graph
        .run("t", order(5), Arc::new(TestStore::default()))
        .await;

    assert_eq!(waiting_at(&result), ["Work", "Inner", "Approve"]);
}

#[tokio::test]
async fn a_second_boundary_after_resume_reports_the_new_place() {
    let child = StateGraph::<Job>::new()
        .add_node("First", step("first"))
        .add_interrupt_boundary("First")
        .add_node("Second", step("second"))
        .add_interrupt_boundary("Second")
        .add_node("Done", step("done"))
        .add_edge("First", "Second")
        .add_edge("Second", "Done")
        .compile()
        .unwrap();
    let graph = parent_around(child).compile().unwrap();
    let store = Arc::new(TestStore::default());

    assert_eq!(
        waiting_at(&graph.run("t", order(5), store.clone()).await),
        ["Work", "First"]
    );
    assert_eq!(
        waiting_at(
            &graph
                .resume("t", HumanSignal::default(), store.clone())
                .await
        ),
        ["Work", "Second"]
    );
}

// --- A different signal per level ---------------------------------------------

/// A parent state with a field of its own (`escalated_by`) that its subgraph
/// node's `output` mapper keeps, unlike `Order`'s.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Desk {
    amount: i32,
    log: Vec<String>,
    escalated_by: Option<String>,
    approved_by: Option<String>,
}

fn desk_around(child: CompiledGraph<Job>) -> CompiledGraph<Desk> {
    StateGraph::<Desk>::new()
        .add_node("Prepare", |mut desk: Desk| {
            desk.log.push("prepared".to_string());
            desk
        })
        .add_subgraph(
            "Work",
            child,
            |desk: &Desk| Job {
                n: desk.amount,
                trail: Vec::new(),
                approved_by: None,
            },
            |entry: &Desk, job: Job| {
                let mut next = entry.clone();
                next.amount = job.n;
                next.log.extend(job.trail);
                next.approved_by = job.approved_by;
                next
            },
        )
        .add_edge("Prepare", "Work")
        .compile()
        .unwrap()
}

fn a_desk() -> Desk {
    Desk {
        amount: 5,
        log: Vec::new(),
        escalated_by: None,
        approved_by: None,
    }
}

fn escalate_as(who: &str) -> HumanSignal {
    HumanSignal {
        is_approved: true,
        reviewer_token: "token".to_string(),
        field_overrides: HashMap::from([("escalated_by".to_string(), who.to_string())]),
        ..Default::default()
    }
}

#[tokio::test]
async fn one_resume_can_send_different_overrides_to_the_parent_and_the_child() {
    let graph = desk_around(review_child());
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", a_desk(), store.clone()).await);

    let signals = LevelSignals::new(approve_as("reviewer")).at(&[], escalate_as("ops"));
    let desk = completed(graph.resume("t", signals, store.clone()).await);

    // Each override reached only the level it was addressed to: the parent's
    // reached the state its subgraph node started with (and survived the
    // `output` mapper); the child's was written into the child's state.
    assert_eq!(desk.escalated_by.as_deref(), Some("ops"));
    assert_eq!(desk.approved_by.as_deref(), Some("reviewer"));
}

#[tokio::test]
async fn a_signal_can_address_a_level_in_the_middle() {
    let graph = parent_around(middle_child(review_child()))
        .compile()
        .unwrap();

    // Without a signal for the middle level the grandchild's answer wins...
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("plain", order(5), store.clone()).await);
    let plain = completed(
        graph
            .resume("plain", approve_as("inner"), store.clone())
            .await,
    );
    assert_eq!(plain.approved_by.as_deref(), Some("inner"));

    // ...and with one, the middle level's own state carries it instead.
    interrupted(graph.run("levels", order(5), store.clone()).await);
    let signals = LevelSignals::new(approve_as("inner")).at(&["Work"], approve_as("middle"));
    let levelled = completed(graph.resume("levels", signals, store.clone()).await);
    assert_eq!(levelled.approved_by.as_deref(), Some("middle"));
}

#[tokio::test]
async fn a_bare_human_signal_still_goes_to_the_innermost_level_only() {
    // The same as `LevelSignals::new(signal)`, which is what `resume` turns it into.
    let graph = desk_around(review_child());
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", a_desk(), store.clone()).await);

    let desk = completed(graph.resume("t", escalate_as("ops"), store.clone()).await);

    // `escalated_by` is not a field of the child, so nothing was addressed to
    // the parent and nothing arrived there.
    assert_eq!(desk.escalated_by, None);
}

#[tokio::test]
async fn a_signal_for_a_level_the_checkpoint_does_not_have_is_refused_and_consumes_nothing() {
    let graph = parent_around(review_child()).compile().unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", order(5), store.clone()).await);

    let signals = LevelSignals::new(approve_as("admin_01")).at(&["Nope"], approve_as("x"));
    match graph.resume("t", signals, store.clone()).await {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
            assert!(message.contains("[Nope]"), "{message}");
            assert!(
                message.contains("[], [Work]"),
                "the levels are named: {message}"
            );
        }
        _ => panic!("expected Failed(ExecutionAborted)"),
    }

    // Nothing was stamped or run, so the thread can still be resumed.
    assert!(store.load("t").unwrap().unwrap().resumed_at.is_none());
    completed(
        graph
            .resume("t", approve_as("admin_01"), store.clone())
            .await,
    );
}

#[tokio::test]
async fn addressing_the_innermost_level_twice_is_refused() {
    let graph = parent_around(review_child()).compile().unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", order(5), store.clone()).await);

    // `["Work"]` is the child, which is the innermost level here.
    let signals = LevelSignals::new(approve_as("a")).at(&["Work"], approve_as("b"));
    match graph.resume("t", signals, store.clone()).await {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
            assert!(message.contains("innermost"), "{message}");
        }
        _ => panic!("expected Failed(ExecutionAborted)"),
    }
    assert!(store.load("t").unwrap().unwrap().resumed_at.is_none());
}

#[tokio::test]
async fn on_a_flat_thread_the_only_level_is_the_innermost_one() {
    let child = review_child();
    let store = Arc::new(TestStore::default());
    interrupted(child.run("t", to_job(&order(5)), store.clone()).await);

    let signals = LevelSignals::new(approve_as("a")).at(&[], approve_as("b"));
    match child.resume("t", signals, store.clone()).await {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
            assert!(message.contains("innermost"), "{message}");
        }
        _ => panic!("expected Failed(ExecutionAborted)"),
    }
}

// --- Resuming a failed thread ---------------------------------------------------

/// `Approve` (a boundary) -> `Send`, which fails fatally for its first
/// `failing_sends` calls. Both are counted.
fn flaky_child(
    approvals: Arc<AtomicU32>,
    sends: Arc<AtomicU32>,
    failing_sends: u32,
) -> CompiledGraph<Job> {
    StateGraph::<Job>::new()
        .add_node("Approve", move |mut job: Job| {
            approvals.fetch_add(1, Ordering::SeqCst);
            job.trail.push("approve".to_string());
            job
        })
        .add_interrupt_boundary("Approve")
        .add_fallible_node("Send", move |job: &Job| {
            let call = sends.fetch_add(1, Ordering::SeqCst) + 1;
            if call <= failing_sends {
                return Err(NodeError::Fatal("mailbox closed".to_string()));
            }
            let mut next = job.clone();
            next.n += 1;
            next.trail.push("send".to_string());
            Ok(next)
        })
        .add_edge("Approve", "Send")
        .compile()
        .unwrap()
}

fn counters() -> (Arc<AtomicU32>, Arc<AtomicU32>) {
    (Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0)))
}

#[tokio::test]
async fn a_failed_child_leaves_the_childs_failed_checkpoint_nested_in_the_parents() {
    let (approvals, sends) = counters();
    let graph = parent_around(flaky_child(approvals, sends, 1))
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", order(5), store.clone()).await);

    assert!(matches!(
        graph
            .resume("t", approve_as("admin_01"), store.clone())
            .await,
        GraphExecutionResult::Failed(StrataError::NodeFailed { .. })
    ));

    let failed = store.load("t").unwrap().unwrap();
    assert_eq!(failed.status, CheckpointStatus::Failed);
    assert_eq!(failed.schema_version, DECISION_CHECKPOINT_SCHEMA_VERSION);
    assert_eq!(failed.suspended_node.as_deref(), Some("Work"));
    assert_eq!(failed.next_node.as_deref(), Some("Work"));
    // The parent's is the state the subgraph node started with...
    assert_eq!(failed.frozen_state["log"], serde_json::json!(["prepared"]));
    assert!(failed.resumed_at.is_none());
    assert!(
        failed.metadata.as_ref().unwrap()["error"].contains("mailbox closed"),
        "{:?}",
        failed.metadata
    );
    // ...and the child's holds what the human answered, and where it stopped.
    let child = failed.nested.as_deref().unwrap();
    assert_eq!(child.thread_id, "Work");
    assert_eq!(child.status, CheckpointStatus::Failed);
    assert_eq!(child.schema_version, DECISION_CHECKPOINT_SCHEMA_VERSION);
    assert_eq!(child.decision.as_ref().unwrap().boundary, "Approve");
    assert!(failed.decision.is_none());
    assert_eq!(child.suspended_node.as_deref(), Some("Send"));
    assert_eq!(child.next_node.as_deref(), Some("Send"));
    assert_eq!(child.frozen_state["approved_by"], "admin_01");
    assert_eq!(child.frozen_state["trail"], serde_json::json!(["approve"]));
    assert_eq!(child.metadata.as_ref().unwrap()["error"], "mailbox closed");
    assert_eq!(failed.waiting_at(), ["Work", "Send"]);
}

#[tokio::test]
async fn resuming_a_failed_nested_thread_re_enters_the_child_without_asking_again() {
    let (approvals, sends) = counters();
    let graph = parent_around(flaky_child(approvals.clone(), sends.clone(), 1))
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", order(5), store.clone()).await);
    assert!(matches!(
        graph
            .resume("t", approve_as("admin_01"), store.clone())
            .await,
        GraphExecutionResult::Failed(_)
    ));

    // No signal this time: the human's earlier answer is in the checkpoint.
    let state = completed(
        graph
            .resume("t", HumanSignal::default(), store.clone())
            .await,
    );

    assert_eq!(approvals.load(Ordering::SeqCst), 1, "not asked twice");
    assert_eq!(sends.load(Ordering::SeqCst), 2);
    assert_eq!(state.amount, 6);
    assert_eq!(state.approved_by.as_deref(), Some("admin_01"));
    assert_eq!(state.log, vec!["prepared", "approve", "send", "finished"]);
    // The failure it recovered from is spent.
    assert!(store.load("t").unwrap().unwrap().resumed_at.is_some());
    match graph
        .resume("t", HumanSignal::default(), store.clone())
        .await
    {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
            assert!(message.contains("already resumed"), "{message}");
        }
        _ => panic!("expected Failed(ExecutionAborted)"),
    }
}

#[tokio::test]
async fn a_signal_on_a_failed_resume_reaches_the_childs_committed_state() {
    let (approvals, sends) = counters();
    let graph = parent_around(flaky_child(approvals, sends, 1))
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", order(5), store.clone()).await);
    graph
        .resume("t", approve_as("admin_01"), store.clone())
        .await;

    let state = completed(
        graph
            .resume("t", approve_as("admin_02"), store.clone())
            .await,
    );

    assert_eq!(state.approved_by.as_deref(), Some("admin_02"));
}

#[tokio::test]
async fn a_child_that_fails_again_on_resume_leaves_a_fresh_failed_checkpoint() {
    let (approvals, sends) = counters();
    let graph = parent_around(flaky_child(approvals, sends.clone(), 2))
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", order(5), store.clone()).await);
    graph
        .resume("t", approve_as("admin_01"), store.clone())
        .await;

    assert!(matches!(
        graph
            .resume("t", HumanSignal::default(), store.clone())
            .await,
        GraphExecutionResult::Failed(StrataError::NodeFailed { .. })
    ));

    // Still resumable, and still remembering the answer.
    let again = store.load("t").unwrap().unwrap();
    assert_eq!(again.status, CheckpointStatus::Failed);
    assert!(again.resumed_at.is_none());
    assert_eq!(
        again.nested.unwrap().frozen_state["approved_by"],
        "admin_01"
    );
    let state = completed(
        graph
            .resume("t", HumanSignal::default(), store.clone())
            .await,
    );
    assert_eq!(sends.load(Ordering::SeqCst), 3);
    assert_eq!(state.approved_by.as_deref(), Some("admin_01"));
}

#[tokio::test]
async fn a_failure_three_levels_down_nests_a_failed_checkpoint_at_every_level_and_resumes() {
    let (approvals, sends) = counters();
    let graph = parent_around(middle_child(flaky_child(
        approvals.clone(),
        sends.clone(),
        1,
    )))
    .compile()
    .unwrap();
    let store = Arc::new(TestStore::default());
    interrupted(graph.run("t", order(5), store.clone()).await);
    assert!(matches!(
        graph
            .resume("t", approve_as("admin_01"), store.clone())
            .await,
        GraphExecutionResult::Failed(_)
    ));

    let failed = store.load("t").unwrap().unwrap();
    assert_eq!(failed.waiting_at(), ["Work", "Inner", "Send"]);
    let middle = failed.nested.as_deref().unwrap();
    assert_eq!(middle.status, CheckpointStatus::Failed);
    assert_eq!(middle.schema_version, DECISION_CHECKPOINT_SCHEMA_VERSION);
    assert!(middle.decision.is_none());
    let grandchild = middle.nested.as_deref().unwrap();
    assert_eq!(grandchild.status, CheckpointStatus::Failed);
    assert_eq!(
        grandchild.schema_version,
        DECISION_CHECKPOINT_SCHEMA_VERSION
    );
    assert_eq!(grandchild.decision.as_ref().unwrap().boundary, "Approve");

    let state = completed(
        graph
            .resume("t", HumanSignal::default(), store.clone())
            .await,
    );

    assert_eq!(approvals.load(Ordering::SeqCst), 1);
    assert_eq!(sends.load(Ordering::SeqCst), 2);
    assert_eq!(state.approved_by.as_deref(), Some("admin_01"));
    assert_eq!(
        state.log,
        vec!["prepared", "open", "approve", "send", "close", "finished"]
    );
}

#[tokio::test]
async fn a_child_failure_with_no_checkpoint_of_its_own_nests_nothing() {
    // A routing gap fails the run without any node failing, so the child
    // persists no `Failed` checkpoint.
    let child = StateGraph::<Job>::new()
        .add_node("A", step("a"))
        .add_node("B", step("b"))
        .add_conditional_edge("A", "B", |_: &Job| false)
        .compile()
        .unwrap();
    let graph = parent_around(child).compile().unwrap();
    let store = Arc::new(TestStore::default());

    assert!(matches!(
        graph.run("t", order(5), store.clone()).await,
        GraphExecutionResult::Failed(_)
    ));

    // The subgraph node fails as a whole and the parent records that, but
    // there is no child checkpoint to nest.
    let failed = store.load("t").unwrap().unwrap();
    assert_eq!(failed.status, CheckpointStatus::Failed);
    assert_eq!(failed.suspended_node.as_deref(), Some("Work"));
    assert!(failed.nested.is_none());
    assert_eq!(failed.schema_version, SCHEMA_VERSION);
}

// --- The shared cross-language fixture for a failed nested thread -------------------
//
// `schema/fixtures/checkpoint-lifecycle/nested-failed.json`, as for `nested.json`
// above: `NestedInterruptTests.cs` proves the same in C# against the same file.

fn load_failed_fixture() -> Checkpoint {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/fixtures/checkpoint-lifecycle/nested-failed.json");
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {}: {}", path.display(), e));
    serde_json::from_str(&raw).unwrap()
}

/// `review_child`, but `Send` fails fatally: what the failed fixture records.
fn review_child_whose_send_fails() -> CompiledGraph<Job> {
    StateGraph::<Job>::new()
        .add_node("Draft", |mut job: Job| {
            job.n *= 2;
            job.trail.push("draft".to_string());
            job
        })
        .add_node("Approve", step("approve"))
        .add_interrupt_boundary("Approve")
        .add_fallible_node("Send", |_: &Job| {
            Err(NodeError::Fatal("mailbox closed".to_string()))
        })
        .add_edge("Draft", "Approve")
        .add_edge("Approve", "Send")
        .compile()
        .unwrap()
}

#[tokio::test]
async fn failing_matches_the_shared_nested_failed_fixture_at_every_level() {
    let graph = parent_around(review_child_whose_send_fails())
        .compile()
        .unwrap();
    let store = Arc::new(TestStore::default());
    graph
        .run("thread_nested_failed_demo", order(5), store.clone())
        .await;

    graph
        .resume(
            "thread_nested_failed_demo",
            approve_as("admin_01"),
            store.clone(),
        )
        .await;

    let persisted = store.load("thread_nested_failed_demo").unwrap().unwrap();
    assert_eq!(
        without_timestamps(persisted),
        without_timestamps(load_failed_fixture())
    );
}

#[tokio::test]
async fn resuming_the_shared_nested_failed_fixture_completes_from_the_failed_node() {
    // The same graph, with `Send` working now.
    let graph = parent_around(review_child()).compile().unwrap();
    let store = Arc::new(TestStore::default());
    store.save(load_failed_fixture()).unwrap();

    let state = completed(
        graph
            .resume(
                "thread_nested_failed_demo",
                HumanSignal::default(),
                store.clone(),
            )
            .await,
    );

    assert_eq!(
        state,
        Order {
            amount: 11,
            log: vec![
                "prepared".into(),
                "draft".into(),
                "approve".into(),
                "send".into(),
                "finished".into()
            ],
            approved_by: Some("admin_01".to_string()),
        }
    );
}
