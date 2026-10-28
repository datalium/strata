//! Ending a thread where it stands (`design/28-Ending-A-Thread.md`, SG-34),
//! through the public API only. The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/EndThreadTests.cs` — keep the two in step.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::BoxFuture;
use datalium_strata::CancellationToken;
use datalium_strata::checkpoint::{
    CancelReason, CheckpointStore, CheckpointStoreError, GraphExecutionResult, HumanSignal,
    LatestCheckpoint, ThreadEnd,
};
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{CompiledGraph, NodeError, StateGraph};
use datalium_strata::observability::{
    RunEvent, RunEventChannel, RunEventKind, RunMode, RunOutcome,
};
use datalium_strata::schema::{
    Checkpoint, CheckpointStatus, ENDED_CHECKPOINT_SCHEMA_VERSION, SCHEMA_VERSION,
};
use serde::{Deserialize, Serialize};

const PATIENCE: Duration = Duration::from_secs(10);

/// Keeps every thread's whole log and offers the conditional save, as the
/// `strata-memory` stores do.
#[derive(Default)]
struct LogStore {
    logs: Mutex<HashMap<String, Vec<Checkpoint>>>,
}

impl LogStore {
    fn log(&self, thread_id: &str) -> Vec<Checkpoint> {
        self.logs
            .lock()
            .unwrap()
            .get(thread_id)
            .cloned()
            .unwrap_or_default()
    }

    fn latest(&self, thread_id: &str) -> Checkpoint {
        self.log(thread_id).last().cloned().expect("a checkpoint")
    }

    fn push(&self, checkpoint: Checkpoint) {
        self.logs
            .lock()
            .unwrap()
            .entry(checkpoint.thread_id.clone())
            .or_default()
            .push(checkpoint);
    }
}

impl CheckpointStore for LogStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        self.push(checkpoint);
        Box::pin(std::future::ready(Ok(())))
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        Box::pin(std::future::ready(Ok(self.log(thread_id).last().cloned())))
    }

    fn load_latest<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<LatestCheckpoint>, CheckpointStoreError>> {
        let log = self.log(thread_id);
        let latest = log.last().map(|checkpoint| LatestCheckpoint {
            checkpoint: checkpoint.clone(),
            sequence: Some(log.len() as i64 - 1),
        });
        Box::pin(std::future::ready(Ok(latest)))
    }

    fn save_if_latest(
        &self,
        checkpoint: Checkpoint,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'_, Result<bool, CheckpointStoreError>> {
        let mut logs = self.logs.lock().unwrap();
        let log = logs.entry(checkpoint.thread_id.clone()).or_default();
        let saved = expected_sequence.is_none_or(|expected| log.len() as i64 - 1 == expected);
        if saved {
            log.push(checkpoint);
        }
        Box::pin(std::future::ready(Ok(saved)))
    }
}

/// A [`LogStore`] that lets another writer in just before the first
/// conditional save: what a resume or an end racing this one looks like.
struct Interloper {
    inner: Arc<LogStore>,
    sneak_in: Mutex<Option<Checkpoint>>,
}

impl Interloper {
    fn new(inner: Arc<LogStore>, sneak_in: Checkpoint) -> Self {
        Self {
            inner,
            sneak_in: Mutex::new(Some(sneak_in)),
        }
    }
}

impl CheckpointStore for Interloper {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        self.inner.save(checkpoint)
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        self.inner.load(thread_id)
    }

    fn load_latest<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<LatestCheckpoint>, CheckpointStoreError>> {
        self.inner.load_latest(thread_id)
    }

    fn save_if_latest(
        &self,
        checkpoint: Checkpoint,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'_, Result<bool, CheckpointStoreError>> {
        if let Some(other) = self.sneak_in.lock().unwrap().take() {
            self.inner.push(other);
        }
        self.inner.save_if_latest(checkpoint, expected_sequence)
    }
}

/// Only `save` and `load`: the trait's defaults supply the rest.
#[derive(Default)]
struct PlainStore(Mutex<HashMap<String, Checkpoint>>);

impl CheckpointStore for PlainStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        self.0
            .lock()
            .unwrap()
            .insert(checkpoint.thread_id.clone(), checkpoint);
        Box::pin(std::future::ready(Ok(())))
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        Box::pin(std::future::ready(Ok(self
            .0
            .lock()
            .unwrap()
            .get(thread_id)
            .cloned())))
    }
}

// --- Graphs ------------------------------------------------------------------

/// `frozen.json`'s graph: `PrepareWireTransfer` (a boundary) -> `ExecuteWireTransfer`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct WireTransfer {
    invoice_generated: bool,
    amount: f64,
    currency: String,
    approved_by: Option<String>,
    is_completed: bool,
}

fn wire_transfer() -> WireTransfer {
    WireTransfer {
        invoice_generated: false,
        amount: 4200.5,
        currency: "USD".to_string(),
        approved_by: None,
        is_completed: false,
    }
}

fn wire_transfer_graph(executed: Arc<AtomicU32>) -> CompiledGraph<WireTransfer> {
    StateGraph::<WireTransfer>::new()
        .add_node("PrepareWireTransfer", |mut s: WireTransfer| {
            s.invoice_generated = true;
            s
        })
        .add_interrupt_boundary("PrepareWireTransfer")
        .add_node("ExecuteWireTransfer", move |mut s: WireTransfer| {
            executed.fetch_add(1, Ordering::SeqCst);
            s.is_completed = true;
            s
        })
        .add_edge("PrepareWireTransfer", "ExecuteWireTransfer")
        .compile()
        .unwrap()
}

/// `nested.json`'s graph: `Prepare` -> `Work` (`Draft` -> `Approve`, a
/// boundary -> `Send`) -> `Finish`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Order {
    amount: i32,
    log: Vec<String>,
    approved_by: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Job {
    n: i32,
    trail: Vec<String>,
    approved_by: Option<String>,
}

fn order_graph() -> CompiledGraph<Order> {
    let child = StateGraph::<Job>::new()
        .add_node("Draft", |mut job: Job| {
            job.n *= 2;
            job.trail.push("draft".to_string());
            job
        })
        .add_node("Approve", |mut job: Job| {
            job.trail.push("approve".to_string());
            job
        })
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
    StateGraph::<Order>::new()
        .add_node("Prepare", |mut order: Order| {
            order.log.push("prepared".to_string());
            order
        })
        .add_subgraph(
            "Work",
            child,
            |order: &Order| Job {
                n: order.amount,
                trail: Vec::new(),
                approved_by: None,
            },
            |entry: &Order, job: Job| {
                let mut next = entry.clone();
                next.amount = job.n;
                next.log.extend(job.trail);
                next.approved_by = job.approved_by;
                next
            },
        )
        .add_node("Finish", |mut order: Order| {
            order.log.push("finished".to_string());
            order
        })
        .add_edge("Prepare", "Work")
        .add_edge("Work", "Finish")
        .compile()
        .unwrap()
}

fn order() -> Order {
    Order {
        amount: 5,
        log: Vec::new(),
        approved_by: None,
    }
}

/// `Approve` (a boundary) -> `Apply`, which fails fatally.
fn failing_apply_graph() -> CompiledGraph<WireTransfer> {
    StateGraph::<WireTransfer>::new()
        .add_node("Approve", |s: WireTransfer| s)
        .add_interrupt_boundary("Approve")
        .add_fallible_node("Apply", |_: &WireTransfer| {
            Err(NodeError::Fatal("gateway refused".to_string()))
        })
        .add_edge("Approve", "Apply")
        .compile()
        .unwrap()
}

/// `Charge`, an async node that never finishes on its own.
fn hanging_graph() -> CompiledGraph<WireTransfer> {
    StateGraph::<WireTransfer>::new()
        .add_async_node("Charge", |s: WireTransfer, _| async move {
            std::future::pending::<()>().await;
            s
        })
        .compile()
        .unwrap()
}

// --- Helpers -----------------------------------------------------------------

fn lifecycle_fixture(name: &str) -> Checkpoint {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/fixtures/checkpoint-lifecycle")
        .join(name);
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// Blanks every level's `created_at`: the fixture's is fixed, a live end's the
/// real time.
fn without_timestamps(mut checkpoint: Checkpoint) -> Checkpoint {
    checkpoint.created_at = String::new();
    checkpoint.nested = checkpoint
        .nested
        .map(|child| Box::new(without_timestamps(*child)));
    checkpoint
}

/// The `(node, state, stopped_at)` of an ended result; panics on anything else.
fn ended<S>(result: GraphExecutionResult<S>) -> (String, S, Vec<String>) {
    match result {
        GraphExecutionResult::Cancelled {
            reason: CancelReason::Ended,
            node,
            state,
            stopped_at,
            ..
        } => (node, state, stopped_at),
        GraphExecutionResult::Failed(error) => panic!("expected an ended thread, got: {error:?}"),
        _ => panic!("expected an ended thread"),
    }
}

fn refused<S>(result: GraphExecutionResult<S>) -> String {
    match result {
        GraphExecutionResult::Failed(error) => format!("{error:?}"),
        _ => panic!("expected a refusal"),
    }
}

fn metadata_of(checkpoint: &Checkpoint) -> HashMap<String, String> {
    checkpoint.metadata.clone().unwrap_or_default()
}

async fn events_of(mut events: tokio::sync::mpsc::UnboundedReceiver<RunEvent>) -> Vec<RunEvent> {
    let mut all = Vec::new();
    while let Some(event) = tokio::time::timeout(PATIENCE, events.recv()).await.unwrap() {
        all.push(event);
    }
    all
}

async fn frozen_wire_transfer(
    thread_id: &str,
    store: Arc<LogStore>,
) -> (CompiledGraph<WireTransfer>, Arc<AtomicU32>) {
    let executed = Arc::new(AtomicU32::new(0));
    let graph = wire_transfer_graph(executed.clone());
    assert!(matches!(
        graph.run(thread_id, wire_transfer(), store).await,
        GraphExecutionResult::Interrupted { .. }
    ));
    (graph, executed)
}

// --- Ending ------------------------------------------------------------------

#[tokio::test]
async fn ending_an_interrupted_thread_writes_an_ended_checkpoint_where_it_stood() {
    let store = Arc::new(LogStore::default());
    let executed = Arc::new(AtomicU32::new(0));
    let graph = wire_transfer_graph(executed.clone());
    graph
        .run_with(
            "t",
            wire_transfer(),
            store.clone(),
            RunConfig::new().with_metadata("tenant", "acme"),
        )
        .await;

    let (node, state, stopped_at) = ended(
        graph
            .end_with(
                "t",
                ThreadEnd::by("oncall"),
                store.clone(),
                RunConfig::new().with_metadata("incident", "INC-7"),
            )
            .await,
    );

    assert_eq!(node, "PrepareWireTransfer");
    assert!(state.invoice_generated);
    assert_eq!(stopped_at, ["PrepareWireTransfer"]);
    let checkpoint = store.latest("t");
    assert_eq!(checkpoint.status, CheckpointStatus::Ended);
    assert_eq!(checkpoint.schema_version, ENDED_CHECKPOINT_SCHEMA_VERSION);
    assert_eq!(
        checkpoint.suspended_node.as_deref(),
        Some("PrepareWireTransfer")
    );
    assert_eq!(checkpoint.next_node, None);
    assert_eq!(checkpoint.resumed_at, None);
    // The stored caller keys, the end's laid over them, and the end's own.
    assert_eq!(
        metadata_of(&checkpoint),
        HashMap::from([
            ("tenant".to_string(), "acme".to_string()),
            ("incident".to_string(), "INC-7".to_string()),
            ("strata.end_reason".to_string(), "requested".to_string()),
            ("strata.ended_by".to_string(), "oncall".to_string()),
        ])
    );
    assert_eq!(store.log("t").len(), 2);
    assert_eq!(executed.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn ending_matches_the_shared_ended_fixture() {
    let store = Arc::new(LogStore::default());
    let (graph, _) = frozen_wire_transfer("thread_ended_demo", store.clone()).await;

    ended(
        graph
            .end_with(
                "thread_ended_demo",
                ThreadEnd::by("oncall"),
                store.clone(),
                RunConfig::new(),
            )
            .await,
    );

    assert_eq!(
        without_timestamps(store.latest("thread_ended_demo")),
        without_timestamps(lifecycle_fixture("ended.json"))
    );
}

#[tokio::test]
async fn ending_a_nested_wait_ends_every_level_and_matches_the_shared_fixture() {
    let store = Arc::new(LogStore::default());
    let graph = order_graph();
    graph
        .run_with(
            "thread_nested_ended_demo",
            order(),
            store.clone(),
            RunConfig::new().with_metadata("tenant", "acme"),
        )
        .await;

    let (node, state, stopped_at) =
        ended(graph.end("thread_nested_ended_demo", store.clone()).await);

    assert_eq!(node, "Work");
    assert_eq!(state.log, ["prepared"]);
    assert_eq!(stopped_at, ["Work", "Approve"]);
    let checkpoint = store.latest("thread_nested_ended_demo");
    let child = checkpoint.nested.as_deref().unwrap();
    assert_eq!(child.status, CheckpointStatus::Ended);
    assert_eq!(child.next_node, None);
    assert_eq!(
        without_timestamps(checkpoint),
        without_timestamps(lifecycle_fixture("nested-ended.json"))
    );
}

#[tokio::test]
async fn ending_a_failed_thread_keeps_the_decision_and_drops_the_failure_keys() {
    let store = Arc::new(LogStore::default());
    let graph = failing_apply_graph();
    graph.run("t", wire_transfer(), store.clone()).await;
    assert!(matches!(
        graph
            .resume("t", HumanSignal::approve("alice"), store.clone())
            .await,
        GraphExecutionResult::Failed(_)
    ));
    let failed = store.latest("t");
    assert_eq!(failed.status, CheckpointStatus::Failed);

    let (node, _, _) = ended(graph.end("t", store.clone()).await);

    assert_eq!(node, "Apply");
    let checkpoint = store.latest("t");
    assert_eq!(checkpoint.status, CheckpointStatus::Ended);
    assert_eq!(checkpoint.decision, failed.decision);
    assert_eq!(checkpoint.decision.as_ref().unwrap().reviewer, "alice");
    assert_eq!(
        metadata_of(&checkpoint),
        HashMap::from([("strata.end_reason".to_string(), "requested".to_string())])
    );
}

#[tokio::test]
async fn ending_a_cancelled_thread_drops_the_cancel_reason() {
    let store = Arc::new(LogStore::default());
    let graph = hanging_graph();
    let token = CancellationToken::new();
    token.cancel();
    assert!(matches!(
        graph
            .run_with(
                "t",
                wire_transfer(),
                store.clone(),
                RunConfig::new().with_cancellation(token),
            )
            .await,
        GraphExecutionResult::Cancelled {
            reason: CancelReason::Requested,
            ..
        }
    ));

    let (node, _, _) = ended(graph.end("t", store.clone()).await);

    assert_eq!(node, "Charge");
    let checkpoint = store.latest("t");
    assert_eq!(checkpoint.status, CheckpointStatus::Ended);
    assert!(!metadata_of(&checkpoint).contains_key("cancel_reason"));
}

#[tokio::test]
async fn ending_an_ended_thread_returns_the_same_result_and_writes_nothing() {
    let store = Arc::new(LogStore::default());
    let (graph, _) = frozen_wire_transfer("t", store.clone()).await;
    let first = ended(
        graph
            .end_with(
                "t",
                ThreadEnd::by("oncall"),
                store.clone(),
                RunConfig::new(),
            )
            .await,
    );
    let log = store.log("t");

    let second = ended(
        graph
            .end_with(
                "t",
                ThreadEnd::by("someone-else"),
                store.clone(),
                RunConfig::new(),
            )
            .await,
    );

    assert_eq!(second, first);
    assert_eq!(store.log("t"), log);
}

// --- Resuming an ended thread ---------------------------------------------------

#[tokio::test]
async fn a_resume_of_an_ended_thread_is_refused_and_writes_nothing() {
    let store = Arc::new(LogStore::default());
    let (graph, executed) = frozen_wire_transfer("t", store.clone()).await;
    ended(graph.end("t", store.clone()).await);
    let log = store.log("t");

    let message = refused(
        graph
            .resume("t", HumanSignal::approve("alice"), store.clone())
            .await,
    );
    assert!(message.contains("was ended"), "{message}");

    let (handle, mut chunks) =
        Arc::new(graph).resume_streaming("t", HumanSignal::approve("alice"), store.clone());
    assert!(matches!(
        handle.await.unwrap(),
        GraphExecutionResult::Failed(_)
    ));
    assert!(chunks.recv().await.is_none());
    assert_eq!(store.log("t"), log);
    assert_eq!(executed.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_resume_of_an_ended_nested_thread_is_refused() {
    let store = Arc::new(LogStore::default());
    let graph = order_graph();
    graph.run("t", order(), store.clone()).await;
    ended(graph.end("t", store.clone()).await);

    let message = refused(
        graph
            .resume("t", HumanSignal::approve("alice"), store.clone())
            .await,
    );
    assert!(message.contains("was ended"), "{message}");
}

// --- Refusals ----------------------------------------------------------------------

#[tokio::test]
async fn an_unknown_thread_is_refused() {
    let store = Arc::new(LogStore::default());
    let message = refused(order_graph().end("nobody", store.clone()).await);
    assert!(message.contains("no checkpoint found"), "{message}");
}

#[tokio::test]
async fn a_resumed_thread_is_refused_and_nothing_is_written() {
    let store = Arc::new(LogStore::default());
    let (graph, _) = frozen_wire_transfer("t", store.clone()).await;
    let mut stamp = store.latest("t");
    stamp.resumed_at = Some("2026-10-06T01:00:00Z".to_string());
    store.push(stamp);
    let log = store.log("t");

    let message = refused(graph.end("t", store.clone()).await);

    assert!(message.contains("may still be running"), "{message}");
    assert_eq!(store.log("t"), log);
}

#[tokio::test]
async fn a_completed_thread_is_refused_and_nothing_is_written() {
    let store = Arc::new(LogStore::default());
    let graph = wire_transfer_graph(Arc::new(AtomicU32::new(0)));
    store.push(Checkpoint {
        schema_version: SCHEMA_VERSION.to_string(),
        thread_id: "t".to_string(),
        status: CheckpointStatus::Completed,
        suspended_node: None,
        next_node: None,
        frozen_state: serde_json::to_value(wire_transfer()).unwrap(),
        metadata: None,
        created_at: "2026-10-06T00:00:00Z".to_string(),
        resumed_at: None,
        nested: None,
        decision: None,
        wait: None,
        graph: None,
    });

    let message = refused(graph.end("t", store.clone()).await);

    assert!(message.contains("has completed"), "{message}");
    assert_eq!(store.log("t").len(), 1);
}

// --- Races ----------------------------------------------------------------------

#[tokio::test]
async fn an_end_that_loses_to_a_resume_is_refused() {
    let log = Arc::new(LogStore::default());
    let (graph, _) = frozen_wire_transfer("t", log.clone()).await;
    let mut stamp = log.latest("t");
    stamp.resumed_at = Some("2026-10-06T01:00:00Z".to_string());
    let store = Arc::new(Interloper::new(log.clone(), stamp.clone()));

    let message = refused(graph.end("t", store).await);

    assert!(message.contains("may still be running"), "{message}");
    assert_eq!(log.latest("t"), stamp);
}

#[tokio::test]
async fn an_end_that_loses_to_another_end_returns_its_result() {
    let log = Arc::new(LogStore::default());
    let (graph, _) = frozen_wire_transfer("t", log.clone()).await;
    // What the other end wrote.
    let other = Arc::new(LogStore::default());
    other.push(log.latest("t"));
    ended(
        graph
            .end_with("t", ThreadEnd::by("first"), other.clone(), RunConfig::new())
            .await,
    );
    let theirs = other.latest("t");
    let store = Arc::new(Interloper::new(log.clone(), theirs.clone()));

    ended(
        graph
            .end_with("t", ThreadEnd::by("second"), store, RunConfig::new())
            .await,
    );

    assert_eq!(log.latest("t"), theirs);
    assert_eq!(log.log("t").len(), 2);
}

#[tokio::test]
async fn a_resume_that_loses_to_an_end_runs_nothing() {
    let log = Arc::new(LogStore::default());
    let (graph, executed) = frozen_wire_transfer("t", log.clone()).await;
    let other = Arc::new(LogStore::default());
    other.push(log.latest("t"));
    ended(graph.end("t", other.clone()).await);
    let store = Arc::new(Interloper::new(log.clone(), other.latest("t")));

    let message = refused(
        graph
            .resume("t", HumanSignal::approve("alice"), store)
            .await,
    );

    assert!(
        message.contains("changed before it could be resumed"),
        "{message}"
    );
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert_eq!(log.latest("t").status, CheckpointStatus::Ended);
}

#[tokio::test]
async fn a_store_without_the_conditional_save_still_ends_a_thread() {
    let store = Arc::new(PlainStore::default());
    let graph = wire_transfer_graph(Arc::new(AtomicU32::new(0)));
    graph.run("t", wire_transfer(), store.clone()).await;

    ended(graph.end("t", store.clone()).await);

    let checkpoint = store.load("t").await.unwrap().unwrap();
    assert_eq!(checkpoint.status, CheckpointStatus::Ended);
}

// --- Observers ----------------------------------------------------------------------

fn describe(event: &RunEvent) -> String {
    match &event.kind {
        RunEventKind::RunStarted { mode, node, .. } => format!("RunStarted {mode:?} {node}"),
        RunEventKind::CheckpointSaved {
            status, waiting_at, ..
        } => format!("CheckpointSaved {status:?} {}", waiting_at.join("/")),
        RunEventKind::RunCompleted {
            outcome,
            cancel_reason,
            ..
        } => format!("RunCompleted {outcome:?} {cancel_reason:?}"),
        other => format!("{other:?}"),
    }
}

#[tokio::test]
async fn an_end_is_observed_as_a_run_that_was_cancelled_because_it_ended() {
    let store = Arc::new(LogStore::default());
    let graph = order_graph();
    graph.run("t", order(), store.clone()).await;

    let (observer, events) = RunEventChannel::new();
    ended(
        graph
            .end_with(
                "t",
                ThreadEnd::new(),
                store.clone(),
                RunConfig::new().with_observer(observer),
            )
            .await,
    );
    let described: Vec<String> = events_of(events).await.iter().map(describe).collect();
    assert_eq!(
        described,
        [
            format!("RunStarted {:?} Work", RunMode::End),
            "CheckpointSaved Ended Work/Approve".to_string(),
            format!(
                "RunCompleted {:?} {:?}",
                RunOutcome::Cancelled,
                Some(CancelReason::Ended)
            ),
        ]
    );

    // The idempotent end writes nothing, so it saves nothing.
    let (observer, events) = RunEventChannel::new();
    ended(
        graph
            .end_with(
                "t",
                ThreadEnd::new(),
                store.clone(),
                RunConfig::new().with_observer(observer),
            )
            .await,
    );
    let described: Vec<String> = events_of(events).await.iter().map(describe).collect();
    assert_eq!(described.len(), 2, "{described:?}");
}
