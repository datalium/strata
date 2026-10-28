//! Which graph wrote a thread (`design/31-Graph-Identity.md`, SG-13), through
//! the public API only. The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/GraphIdentityTests.cs` — keep the two in
//! step. The run span's tags are checked in `telemetry.rs`, which owns this
//! binary's process-wide subscriber.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::BoxFuture;
use datalium_strata::CancellationToken;
use datalium_strata::StrataError;
use datalium_strata::checkpoint::{
    CheckpointLogPage, CheckpointQuery, CheckpointStore, CheckpointStoreError, EventSignal,
    GraphExecutionResult, HumanSignal, LatestCheckpoint, ThreadCursor, ThreadPage, ThreadQuery,
    ThreadSummary,
};
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{CompileOptions, CompiledGraph, NodeError, StateGraph};
use datalium_strata::observability::{RunEvent, RunEventChannel, RunEventKind};
use datalium_strata::schema::{
    Checkpoint, CheckpointStatus, GRAPH_CHECKPOINT_SCHEMA_VERSION, GraphDefinition, GraphIdentity,
    SCHEMA_VERSION,
};
use datalium_strata::store::updated_at_text;
use datalium_strata::wait::WakeOptions;
use serde::{Deserialize, Serialize};
use serde_json::Value;

const PATIENCE: Duration = Duration::from_secs(10);

/// Keeps every thread's whole log, offers the conditional save, and lists
/// threads in code, as the `strata-memory` in-memory store does.
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

impl CheckpointQuery for LogStore {
    fn list_threads<'a>(
        &'a self,
        query: &'a ThreadQuery,
    ) -> BoxFuture<'a, Result<ThreadPage, CheckpointStoreError>> {
        let page = (|| {
            let cursor = query.validate()?;
            let logs = self.logs.lock().unwrap();
            let mut latest: Vec<(i64, Checkpoint)> = logs
                .values()
                .filter_map(|log| Some((log.len() as i64 - 1, log.last()?.clone())))
                .filter(|(_, checkpoint)| query.matches(checkpoint))
                .filter(|(_, checkpoint)| {
                    cursor.as_ref().is_none_or(|cursor| {
                        cursor.admits(updated_at_text(checkpoint), &checkpoint.thread_id)
                    })
                })
                .collect();
            latest.sort_by(|(_, a), (_, b)| {
                (updated_at_text(b), &b.thread_id).cmp(&(updated_at_text(a), &a.thread_id))
            });
            let more = latest.len() > query.limit;
            latest.truncate(query.limit);
            let next_cursor = more
                .then(|| latest.last())
                .flatten()
                .map(|(_, c)| ThreadCursor::new(updated_at_text(c), &c.thread_id).encode());
            Ok(ThreadPage {
                threads: latest
                    .iter()
                    .map(|(sequence, checkpoint)| ThreadSummary::of(*sequence, checkpoint))
                    .collect::<Result<_, _>>()?,
                next_cursor,
            })
        })();
        Box::pin(std::future::ready(page))
    }

    fn get_log<'a>(
        &'a self,
        _thread_id: &'a str,
        _after_sequence: Option<i64>,
        limit: usize,
    ) -> BoxFuture<'a, Result<CheckpointLogPage, CheckpointStoreError>> {
        Box::pin(std::future::ready(Ok(CheckpointLogPage::new(
            Vec::new(),
            limit,
        ))))
    }
}

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Order {
    amount: i64,
    log: Vec<String>,
    approved_by: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Job {
    n: i64,
    trail: Vec<String>,
    approved_by: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Tick {
    ticks: u32,
}

// --- Graphs ---

fn named(id: &str, version: Option<&str>) -> CompileOptions {
    let options = CompileOptions::new().with_graph_id(id);
    match version {
        Some(version) => options.with_graph_version(version),
        None => options,
    }
}

/// `frozen.json`'s graph: `PrepareWireTransfer` (a boundary) -> `ExecuteWireTransfer`.
fn wire_transfer_graph(options: CompileOptions) -> CompiledGraph<WireTransfer> {
    StateGraph::<WireTransfer>::new()
        .add_node("PrepareWireTransfer", |mut s: WireTransfer| {
            s.invoice_generated = true;
            s
        })
        .add_interrupt_boundary("PrepareWireTransfer")
        .add_node("ExecuteWireTransfer", |mut s: WireTransfer| {
            s.is_completed = true;
            s
        })
        .add_edge("PrepareWireTransfer", "ExecuteWireTransfer")
        .compile_with(options)
        .unwrap()
}

/// Version 2 of the wire transfer, whose last node is called `Send`.
fn renamed_wire_transfer_graph(options: CompileOptions) -> CompiledGraph<WireTransfer> {
    StateGraph::<WireTransfer>::new()
        .add_node("PrepareWireTransfer", |mut s: WireTransfer| {
            s.invoice_generated = true;
            s
        })
        .add_interrupt_boundary("PrepareWireTransfer")
        .add_node("Send", |mut s: WireTransfer| {
            s.is_completed = true;
            s.approved_by = Some(format!("{} via Send", s.approved_by.unwrap_or_default()));
            s
        })
        .add_edge("PrepareWireTransfer", "Send")
        .compile_with(options)
        .unwrap()
}

fn approval_graph(options: CompileOptions) -> CompiledGraph<Job> {
    StateGraph::<Job>::new()
        .add_node("Draft", |mut j: Job| {
            j.n *= 2;
            j.trail.push("draft".into());
            j
        })
        .add_node("Approve", |mut j: Job| {
            j.trail.push("approve".into());
            j
        })
        .add_interrupt_boundary("Approve")
        .add_node("Send", |mut j: Job| {
            j.n += 1;
            j.trail.push("send".into());
            j
        })
        .add_edge("Draft", "Approve")
        .add_edge("Approve", "Send")
        .compile_with(options)
        .unwrap()
}

/// `nested.json`'s graph: `Prepare` -> `Work` (`child`) -> `Finish`.
fn incident_graph(child: CompiledGraph<Job>, options: CompileOptions) -> CompiledGraph<Order> {
    StateGraph::<Order>::new()
        .add_node("Prepare", |mut o: Order| {
            o.log.push("prepared".into());
            o
        })
        .add_subgraph(
            "Work",
            child,
            |o: &Order| Job {
                n: o.amount,
                trail: Vec::new(),
                approved_by: None,
            },
            |entry: &Order, job: Job| {
                let mut out = entry.clone();
                out.amount = job.n;
                out.log.extend(job.trail);
                out.approved_by = job.approved_by;
                out
            },
        )
        .add_node("Finish", |mut o: Order| {
            o.log.push("finished".into());
            o
        })
        .add_edge("Prepare", "Work")
        .add_edge("Work", "Finish")
        .compile_with(options)
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
fn failing_apply_graph(options: CompileOptions) -> CompiledGraph<WireTransfer> {
    StateGraph::<WireTransfer>::new()
        .add_node("Approve", |s: WireTransfer| s)
        .add_interrupt_boundary("Approve")
        .add_fallible_node("Apply", |_: &WireTransfer| {
            Err(NodeError::Fatal("gateway refused".to_string()))
        })
        .add_edge("Approve", "Apply")
        .compile_with(options)
        .unwrap()
}

/// `Check` -> `Window` (a five-minute timer) -> `Done`.
fn ticker_graph(options: CompileOptions) -> CompiledGraph<Tick> {
    StateGraph::<Tick>::new()
        .add_node("Check", |mut t: Tick| {
            t.ticks += 1;
            t
        })
        .add_timer("Window", Duration::from_secs(5 * 60))
        .add_node("Done", |t: Tick| t)
        .add_edge("Check", "Window")
        .add_edge("Window", "Done")
        .compile_with(options)
        .unwrap()
}

/// `Start` -> `Deploying` (waits for `deployment.finished`) -> `Done`.
fn deployer_graph(options: CompileOptions) -> CompiledGraph<Tick> {
    StateGraph::<Tick>::new()
        .add_node("Start", |t: Tick| t)
        .add_wait_for_event("Deploying", "deployment.finished")
        .add_node("Done", |mut t: Tick| {
            t.ticks += 1;
            t
        })
        .add_edge("Start", "Deploying")
        .add_edge("Deploying", "Done")
        .compile_with(options)
        .unwrap()
}

// --- Helpers ---

fn fixture(relative: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/fixtures")
        .join(relative);
    serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap()
}

fn lifecycle_fixture(name: &str) -> Checkpoint {
    serde_json::from_value(fixture(&format!("checkpoint-lifecycle/{name}"))).unwrap()
}

fn without_timestamps(mut checkpoint: Checkpoint) -> Checkpoint {
    checkpoint.created_at = String::new();
    checkpoint.nested = checkpoint
        .nested
        .map(|child| Box::new(without_timestamps(*child)));
    checkpoint
}

fn assert_matches_fixture(name: &str, actual: &Checkpoint) {
    assert_eq!(
        serde_json::to_value(without_timestamps(actual.clone())).unwrap(),
        serde_json::to_value(without_timestamps(lifecycle_fixture(name))).unwrap()
    );
}

fn identity(id: &str, version: Option<&str>) -> Option<GraphIdentity> {
    Some(GraphIdentity::new(id, version.map(str::to_string)))
}

fn refused<S>(result: GraphExecutionResult<S>) -> String {
    match result {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => message,
        GraphExecutionResult::Failed(other) => panic!("expected a refusal, got {other:?}"),
        _ => panic!("expected a refusal"),
    }
}

fn completed<S>(result: GraphExecutionResult<S>) -> S {
    match result {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(error) => {
            panic!("expected the run to complete, got {error:?}")
        }
        _ => panic!("expected the run to complete"),
    }
}

fn approve() -> HumanSignal {
    HumanSignal::approve("alice")
}

async fn events_of(mut events: tokio::sync::mpsc::UnboundedReceiver<RunEvent>) -> Vec<RunEvent> {
    let mut all = Vec::new();
    while let Some(event) = tokio::time::timeout(PATIENCE, events.recv()).await.unwrap() {
        all.push(event);
    }
    all
}

// --- What is written ---

#[tokio::test]
async fn a_named_graph_writes_its_identity_and_matches_the_shared_fixture() {
    let store = Arc::new(LogStore::default());
    let graph = wire_transfer_graph(named("wire-transfer", Some("2")));

    graph
        .run("thread_hitl_demo", wire_transfer(), store.clone())
        .await;

    let checkpoint = store.latest("thread_hitl_demo");
    assert_eq!(checkpoint.graph, identity("wire-transfer", Some("2")));
    assert_eq!(checkpoint.schema_version, GRAPH_CHECKPOINT_SCHEMA_VERSION);
    assert_matches_fixture("identified.json", &checkpoint);
    assert_eq!(
        graph.identity(),
        identity("wire-transfer", Some("2")).as_ref()
    );
}

#[tokio::test]
async fn every_level_carries_its_own_graph_and_matches_the_nested_fixture() {
    let store = Arc::new(LogStore::default());
    let graph = incident_graph(
        approval_graph(named("approval", Some("3"))),
        named("incident", Some("1")),
    );

    graph
        .run("thread_nested_demo", order(), store.clone())
        .await;

    let checkpoint = store.latest("thread_nested_demo");
    assert_eq!(checkpoint.graph, identity("incident", Some("1")));
    assert_eq!(
        checkpoint.nested.as_ref().unwrap().graph,
        identity("approval", Some("3"))
    );
    assert_matches_fixture("nested-identified.json", &checkpoint);
}

#[tokio::test]
async fn a_child_without_an_identity_writes_none_on_its_level() {
    let store = Arc::new(LogStore::default());
    incident_graph(
        approval_graph(CompileOptions::new()),
        named("incident", None),
    )
    .run("t", order(), store.clone())
    .await;

    let checkpoint = store.latest("t");
    assert_eq!(checkpoint.graph, identity("incident", None));
    assert_eq!(checkpoint.nested.as_ref().unwrap().graph, None);
    assert_eq!(checkpoint.schema_version, GRAPH_CHECKPOINT_SCHEMA_VERSION);
    let wire = serde_json::to_value(&checkpoint).unwrap();
    assert!(wire["nested"].get("graph").is_none());
    assert!(wire["graph"]["version"].is_null());
}

#[tokio::test]
async fn failed_cancelled_and_hop_limited_checkpoints_carry_it() {
    let store = Arc::new(LogStore::default());
    let payments = identity("payments", Some("7"));

    let failing = failing_apply_graph(named("payments", Some("7")));
    failing.run("failed", wire_transfer(), store.clone()).await;
    refused_or_failed(failing.resume("failed", approve(), store.clone()).await);
    assert_eq!(store.latest("failed").status, CheckpointStatus::Failed);
    assert_eq!(store.latest("failed").graph, payments);

    let token = CancellationToken::new();
    token.cancel();
    let cancelled = wire_transfer_graph(named("payments", Some("7")))
        .run_with(
            "cancelled",
            wire_transfer(),
            store.clone(),
            RunConfig::new().with_cancellation(token),
        )
        .await;
    assert!(matches!(cancelled, GraphExecutionResult::Cancelled { .. }));
    assert_eq!(store.latest("cancelled").graph, payments);

    let limited = StateGraph::<WireTransfer>::new()
        .add_node("A", |s: WireTransfer| s)
        .add_node("B", |s: WireTransfer| s)
        .add_edge("A", "B")
        .compile_with(named("payments", Some("7")).with_max_node_hops(1))
        .unwrap();
    assert!(matches!(
        limited.run("limited", wire_transfer(), store.clone()).await,
        GraphExecutionResult::Failed(StrataError::HopLimitExceeded { .. })
    ));
    assert_eq!(store.latest("limited").graph, payments);
}

fn refused_or_failed<S>(result: GraphExecutionResult<S>) {
    assert!(matches!(result, GraphExecutionResult::Failed(_)));
}

#[tokio::test]
async fn an_unnamed_graph_writes_no_graph_and_keeps_its_version() {
    let store = Arc::new(LogStore::default());
    let graph = wire_transfer_graph(CompileOptions::new());

    graph
        .run("thread_hitl_demo", wire_transfer(), store.clone())
        .await;

    let checkpoint = store.latest("thread_hitl_demo");
    assert_eq!(graph.identity(), None);
    assert_eq!(checkpoint.graph, None);
    assert_eq!(checkpoint.schema_version, SCHEMA_VERSION);
    assert!(
        serde_json::to_value(&checkpoint)
            .unwrap()
            .get("graph")
            .is_none()
    );
}

#[tokio::test]
async fn ending_keeps_every_levels_graph() {
    let store = Arc::new(LogStore::default());
    let graph = incident_graph(
        approval_graph(named("approval", Some("3"))),
        named("incident", Some("1")),
    );
    graph.run("t", order(), store.clone()).await;

    assert!(matches!(
        graph.end("t", store.clone()).await,
        GraphExecutionResult::Cancelled { .. }
    ));

    let ended = store.latest("t");
    assert_eq!(ended.status, CheckpointStatus::Ended);
    assert_eq!(ended.schema_version, GRAPH_CHECKPOINT_SCHEMA_VERSION);
    assert_eq!(ended.graph, identity("incident", Some("1")));
    assert_eq!(
        ended.nested.as_ref().unwrap().graph,
        identity("approval", Some("3"))
    );
}

// --- Refusals ---

#[tokio::test]
async fn a_resume_under_another_id_is_refused_and_writes_nothing() {
    let store = Arc::new(LogStore::default());
    wire_transfer_graph(named("wire-transfer", Some("2")))
        .run("t", wire_transfer(), store.clone())
        .await;
    let log = store.log("t");

    let message = refused(
        wire_transfer_graph(named("payroll", Some("2")))
            .resume("t", approve(), store.clone())
            .await,
    );

    assert_eq!(
        message,
        "thread 't' was written by graph 'wire-transfer' version 2, not 'payroll' version 2"
    );
    assert_eq!(store.log("t"), log);

    // The right graph still resumes it.
    assert!(
        completed(
            wire_transfer_graph(named("wire-transfer", Some("2")))
                .resume("t", approve(), store.clone())
                .await
        )
        .is_completed
    );
}

#[tokio::test]
async fn a_resume_under_another_version_without_a_migration_is_refused() {
    let store = Arc::new(LogStore::default());
    wire_transfer_graph(named("wire-transfer", Some("2")))
        .run("t", wire_transfer(), store.clone())
        .await;
    let log = store.log("t");

    let message = refused(
        wire_transfer_graph(named("wire-transfer", Some("3")))
            .resume("t", approve(), store.clone())
            .await,
    );

    assert_eq!(
        message,
        "thread 't' was written by graph 'wire-transfer' version 2, not version 3, and this graph has no migration"
    );
    assert_eq!(store.log("t"), log);

    let message = refused(
        wire_transfer_graph(named("wire-transfer", None))
            .resume("t", approve(), store.clone())
            .await,
    );
    assert_eq!(
        message,
        "thread 't' was written by graph 'wire-transfer' version 2, not unversioned, and this graph has no migration"
    );
}

#[tokio::test]
async fn a_graph_without_an_identity_can_not_resume_a_named_thread() {
    let store = Arc::new(LogStore::default());
    wire_transfer_graph(named("wire-transfer", Some("2")))
        .run("t", wire_transfer(), store.clone())
        .await;

    let message = refused(
        wire_transfer_graph(CompileOptions::new())
            .resume("t", approve(), store.clone())
            .await,
    );

    assert_eq!(
        message,
        "thread 't' was written by graph 'wire-transfer' version 2, and this graph has no identity"
    );
}

#[tokio::test]
async fn a_nested_level_is_checked_by_its_own_graph() {
    let store = Arc::new(LogStore::default());
    incident_graph(
        approval_graph(named("approval", Some("3"))),
        named("incident", Some("1")),
    )
    .run("t", order(), store.clone())
    .await;
    let log = store.log("t");

    let message = refused(
        incident_graph(
            approval_graph(named("approval", Some("4"))),
            named("incident", Some("1")),
        )
        .resume("t", approve(), store.clone())
        .await,
    );

    assert_eq!(
        message,
        "thread 't' at level [Work] was written by graph 'approval' version 3, not version 4, and this graph has no migration"
    );
    assert_eq!(store.log("t"), log);
}

#[tokio::test]
async fn end_and_signal_under_another_graph_are_refused() {
    let store = Arc::new(LogStore::default());
    wire_transfer_graph(named("wire-transfer", Some("2")))
        .run("t", wire_transfer(), store.clone())
        .await;
    deployer_graph(named("deployer", Some("1")))
        .run("d", Tick { ticks: 0 }, store.clone())
        .await;
    let (transfers, deploys) = (store.log("t"), store.log("d"));

    assert_eq!(
        refused(
            wire_transfer_graph(named("payroll", Some("2")))
                .end("t", store.clone())
                .await
        ),
        "thread 't' was written by graph 'wire-transfer' version 2, not 'payroll' version 2"
    );
    assert_eq!(
        refused(
            deployer_graph(named("other", Some("1")))
                .signal("d", EventSignal::new("deployment.finished"), store.clone())
                .await
        ),
        "thread 'd' was written by graph 'deployer' version 1, not 'other' version 1"
    );
    assert_eq!(store.log("t"), transfers);
    assert_eq!(store.log("d"), deploys);

    // Its own graph does both.
    assert_eq!(
        completed(
            deployer_graph(named("deployer", Some("1")))
                .signal("d", EventSignal::new("deployment.finished"), store.clone())
                .await
        )
        .ticks,
        1
    );
    assert!(matches!(
        wire_transfer_graph(named("wire-transfer", Some("2")))
            .end("t", store.clone())
            .await,
        GraphExecutionResult::Cancelled { .. }
    ));
}

// --- Migration ---

#[tokio::test]
async fn a_migration_takes_another_versions_thread_and_the_stamp_records_the_new_identity() {
    let store = Arc::new(LogStore::default());
    wire_transfer_graph(named("wire-transfer", Some("1")))
        .run("t", wire_transfer(), store.clone())
        .await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let saw = seen.clone();
    let v2 = renamed_wire_transfer_graph(named("wire-transfer", Some("2")).with_migration(
        move |mut checkpoint: Checkpoint| {
            saw.lock().unwrap().push(checkpoint.graph.clone());
            if checkpoint.graph.as_ref()?.version.as_deref() != Some("1") {
                return None;
            }
            if checkpoint.next_node.as_deref() == Some("ExecuteWireTransfer") {
                checkpoint.next_node = Some("Send".to_string());
            }
            checkpoint.frozen_state["currency"] = "EUR".into();
            checkpoint.thread_id = "ignored: the engine keeps the stored thread id".to_string();
            Some(checkpoint)
        },
    ));

    let state = completed(
        v2.resume(
            "t",
            approve().with_override("approved_by", "alice"),
            store.clone(),
        )
        .await,
    );

    assert_eq!(state.currency, "EUR");
    assert_eq!(state.approved_by.as_deref(), Some("alice via Send"));
    assert_eq!(
        *seen.lock().unwrap(),
        [identity("wire-transfer", Some("1"))]
    );
    let log = store.log("t");
    assert_eq!(log.len(), 2);
    let stamp = &log[1];
    assert_eq!(stamp.thread_id, "t");
    assert!(stamp.resumed_at.is_some());
    assert_eq!(stamp.next_node.as_deref(), Some("Send"));
    assert_eq!(stamp.graph, identity("wire-transfer", Some("2")));
    assert_eq!(stamp.schema_version, GRAPH_CHECKPOINT_SCHEMA_VERSION);
    assert_eq!(log[0].graph, identity("wire-transfer", Some("1")));
}

#[tokio::test]
async fn a_migration_that_refuses_leaves_the_thread_resumable() {
    let store = Arc::new(LogStore::default());
    wire_transfer_graph(named("wire-transfer", Some("1")))
        .run("t", wire_transfer(), store.clone())
        .await;
    let log = store.log("t");

    let message = refused(
        wire_transfer_graph(named("wire-transfer", Some("2")).with_migration(|_| None))
            .resume("t", approve(), store.clone())
            .await,
    );

    assert_eq!(
        message,
        "thread 't' was written by graph 'wire-transfer' version 1, not version 2, and this graph's migration refused it"
    );
    assert_eq!(store.log("t"), log);
    assert!(
        completed(
            wire_transfer_graph(named("wire-transfer", Some("1")))
                .resume("t", approve(), store.clone())
                .await
        )
        .is_completed
    );
}

#[tokio::test]
async fn a_nested_level_is_migrated_by_its_own_graph_once() {
    let store = Arc::new(LogStore::default());
    incident_graph(
        approval_graph(named("approval", Some("3"))),
        named("incident", Some("1")),
    )
    .run("t", order(), store.clone())
    .await;
    let calls = Arc::new(AtomicU32::new(0));
    let counted = calls.clone();
    let child = approval_graph(
        named("approval", Some("4")).with_migration(move |checkpoint| {
            counted.fetch_add(1, Ordering::SeqCst);
            Some(checkpoint)
        }),
    );

    let order = completed(
        incident_graph(child, named("incident", Some("1")))
            .resume("t", approve(), store.clone())
            .await,
    );

    assert_eq!(
        order.log,
        ["prepared", "draft", "approve", "send", "finished"]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let stamp = &store.log("t")[1];
    assert_eq!(stamp.graph, identity("incident", Some("1")));
    assert_eq!(
        stamp.nested.as_ref().unwrap().graph,
        identity("approval", Some("4"))
    );
}

#[tokio::test]
async fn an_unknown_thread_resumes_under_a_named_graph_which_then_writes_its_identity() {
    let store = Arc::new(LogStore::default());

    // frozen.json predates identities: 1.0.0, no graph.
    let unknown = lifecycle_fixture("frozen.json");
    assert_eq!(unknown.graph, None);
    store.push(unknown.clone());
    assert!(
        completed(
            wire_transfer_graph(named("wire-transfer", Some("2")))
                .resume(unknown.thread_id.clone(), approve(), store.clone())
                .await
        )
        .is_completed
    );

    // A failure after an unknown checkpoint is written with the resuming
    // graph's identity.
    let failing = failing_apply_graph(named("payments", Some("7")));
    let mut parked = unknown;
    parked.thread_id = "f".to_string();
    parked.suspended_node = Some("Approve".to_string());
    parked.next_node = Some("Apply".to_string());
    store.push(parked);
    refused_or_failed(failing.resume("f", approve(), store.clone()).await);
    let log = store.log("f");
    assert_eq!(log[1].graph, None);
    assert_eq!(log[2].graph, identity("payments", Some("7")));
}

// --- Owns ---

#[test]
fn owns_says_which_threads_a_graph_takes() {
    let unknown = lifecycle_fixture("frozen.json");
    let mut v2 = unknown.clone();
    v2.graph = identity("wire-transfer", Some("2"));

    assert!(wire_transfer_graph(CompileOptions::new()).owns(&unknown));
    assert!(!wire_transfer_graph(CompileOptions::new()).owns(&v2));
    assert!(wire_transfer_graph(named("wire-transfer", Some("2"))).owns(&unknown));
    assert!(wire_transfer_graph(named("wire-transfer", Some("2"))).owns(&v2));
    assert!(!wire_transfer_graph(named("payroll", Some("2"))).owns(&v2));
    assert!(!wire_transfer_graph(named("wire-transfer", Some("3"))).owns(&v2));
    assert!(wire_transfer_graph(named("wire-transfer", Some("3")).with_migration(Some)).owns(&v2));
}

// --- Compiling ---

#[test]
fn compile_refuses_a_blank_identity_or_parts_without_an_id() {
    let refusal = |options: CompileOptions| match StateGraph::<Tick>::new()
        .add_node("A", |t: Tick| t)
        .compile_with(options)
    {
        Err(StrataError::CompilationFailed(message)) => message,
        Err(other) => panic!("expected a compile refusal, got {other:?}"),
        Ok(_) => panic!("expected a compile refusal"),
    };

    assert_eq!(
        refusal(CompileOptions::new().with_graph_id(" ")),
        "graph_id must not be blank"
    );
    assert_eq!(
        refusal(
            CompileOptions::new()
                .with_graph_id("a")
                .with_graph_version("")
        ),
        "graph_version must not be blank"
    );
    assert_eq!(
        refusal(CompileOptions::new().with_graph_version("1")),
        "graph_version needs a graph_id"
    );
    assert_eq!(
        refusal(CompileOptions::new().with_migration(Some)),
        "a migration needs a graph_id"
    );
}

// --- The drivers ---

#[tokio::test]
async fn wake_due_lists_only_its_own_graphs_threads() {
    let store = Arc::new(LogStore::default());
    let a = ticker_graph(named("ticker-a", Some("1")));
    let b = ticker_graph(named("ticker-b", Some("1")));
    a.run("a", Tick { ticks: 0 }, store.clone()).await;
    b.run("b", Tick { ticks: 0 }, store.clone()).await;
    ticker_graph(CompileOptions::new())
        .run("unknown", Tick { ticks: 0 }, store.clone())
        .await;
    let later = chrono::Utc::now() + chrono::Duration::minutes(6);

    // Same node names, same waits: only identity tells them apart, and the
    // listing already does.
    let report = a.wake_due(store.clone(), later).await.unwrap();
    let woken: Vec<&str> = report.woken.iter().map(|w| w.thread_id.as_str()).collect();
    assert_eq!(woken, ["a"]);
    assert!(report.skipped.is_empty());

    // Without the match, the unknown thread is woken and the other graph's
    // skipped, with why.
    let report = a
        .wake_due_with(
            store.clone(),
            later,
            WakeOptions::new().with_match_graph(false),
        )
        .await
        .unwrap();
    let woken: Vec<&str> = report.woken.iter().map(|w| w.thread_id.as_str()).collect();
    assert_eq!(woken, ["unknown"]);
    assert_eq!(report.skipped.len(), 1);
    assert_eq!(report.skipped[0].thread_id, "b");
    assert_eq!(
        report.skipped[0].reason,
        "thread 'b' was written by graph 'ticker-b' version 1, not 'ticker-a' version 1"
    );
}

#[tokio::test]
async fn wake_due_with_a_migration_wakes_an_older_versions_thread() {
    let store = Arc::new(LogStore::default());
    ticker_graph(named("ticker", Some("1")))
        .run("old", Tick { ticks: 0 }, store.clone())
        .await;
    let later = chrono::Utc::now() + chrono::Duration::minutes(6);

    assert!(
        ticker_graph(named("ticker", Some("2")))
            .wake_due(store.clone(), later)
            .await
            .unwrap()
            .woken
            .is_empty()
    );

    let report = ticker_graph(named("ticker", Some("2")).with_migration(Some))
        .wake_due(store.clone(), later)
        .await
        .unwrap();
    assert_eq!(report.woken.len(), 1);
    let woken = report.woken.into_iter().next().unwrap();
    assert_eq!(woken.thread_id, "old");
    assert_eq!(completed(woken.result).ticks, 1);
}

// --- Run events ---

#[tokio::test]
async fn run_started_carries_the_graph() {
    let store = Arc::new(LogStore::default());
    let (observer, events) = RunEventChannel::new();
    wire_transfer_graph(named("wire-transfer", Some("2")))
        .run_with(
            "t",
            wire_transfer(),
            store.clone(),
            RunConfig::new().with_observer(observer),
        )
        .await;
    let (unnamed, unnamed_events) = RunEventChannel::new();
    wire_transfer_graph(CompileOptions::new())
        .run_with(
            "u",
            wire_transfer(),
            store.clone(),
            RunConfig::new().with_observer(unnamed),
        )
        .await;

    let started = |events: Vec<RunEvent>| {
        events.into_iter().find_map(|event| match event.kind {
            RunEventKind::RunStarted { graph, .. } => Some(graph),
            _ => None,
        })
    };
    assert_eq!(
        started(events_of(events).await),
        Some(identity("wire-transfer", Some("2")))
    );
    assert_eq!(started(events_of(unnamed_events).await), Some(None));
}

// --- The query port ---

#[tokio::test]
async fn a_query_with_a_version_and_no_id_is_refused() {
    let store = LogStore::default();
    let mut query = ThreadQuery::default();
    query.graph_version = Some("2".to_string());

    let refused = store.list_threads(&query).await.unwrap_err();

    assert_eq!(refused.0, "a thread query's graph_version needs a graph_id");
}

#[tokio::test]
async fn a_summary_says_which_graph_wrote_the_thread() {
    let store = Arc::new(LogStore::default());
    incident_graph(
        approval_graph(named("approval", Some("3"))),
        named("incident", Some("1")),
    )
    .run("t", order(), store.clone())
    .await;

    let page = store
        .list_threads(&ThreadQuery::default().with_graph("incident"))
        .await
        .unwrap();

    assert_eq!(page.threads.len(), 1);
    assert_eq!(page.threads[0].graph, identity("incident", Some("1")));
    assert!(
        store
            .list_threads(&ThreadQuery::default().with_graph("approval"))
            .await
            .unwrap()
            .threads
            .is_empty()
    );
}

// --- Definitions ---

fn identity_fixture_graph() -> CompiledGraph<Tick> {
    let check = StateGraph::<Tick>::new()
        .add_node("Probe", |t: Tick| t)
        .compile()
        .unwrap();
    let remediation = StateGraph::<Tick>::new()
        .add_node("Apply", |t: Tick| t)
        .add_subgraph("Check", check, |t: &Tick| t.clone(), |_: &Tick, t: Tick| t)
        .add_edge("Apply", "Check")
        .compile_with(named("remediation", None))
        .unwrap();
    StateGraph::<Tick>::new()
        .add_node("Plan", |t: Tick| t)
        .add_subgraph(
            "Remediate",
            remediation,
            |t: &Tick| t.clone(),
            |_: &Tick, t: Tick| t,
        )
        .add_node("Report", |t: Tick| t)
        .add_edge("Plan", "Remediate")
        .add_edge("Remediate", "Report")
        .compile_with(named("incident", Some("1")))
        .unwrap()
}

#[test]
fn a_compiled_graph_exports_every_levels_identity_as_the_shared_fixture() {
    let exported = serde_json::to_value(identity_fixture_graph().to_definition()).unwrap();

    assert_eq!(exported, fixture("graph-export/identity.json"));
}

#[test]
fn the_identity_fixture_round_trips_and_its_shape_is_checked() {
    let raw = fixture("graph-export/identity.json");
    let definition: GraphDefinition = serde_json::from_value(raw.clone()).unwrap();

    assert_eq!(definition.check_shape(), Ok(()));
    assert_eq!(serde_json::to_value(&definition).unwrap(), raw);
    assert_eq!(definition.identity, identity("incident", Some("1")));

    // The builder has no identity: it is given at compile time.
    assert_eq!(
        StateGraph::<Tick>::new()
            .add_node("A", |t: Tick| t)
            .to_definition()
            .identity,
        None
    );

    let mut old = raw.clone();
    old["schema_version"] = "1.4.0".into();
    assert_eq!(
        serde_json::from_value::<GraphDefinition>(old)
            .unwrap()
            .check_shape(),
        Err("the definition has an identity, which schema_version 1.4.0 doesn't have".into())
    );
    let mut blank = raw;
    blank["nodes"][1]["graph"]["identity"]["id"] = "".into();
    assert_eq!(
        serde_json::from_value::<GraphDefinition>(blank)
            .unwrap()
            .check_shape(),
        Err("the definition (in 'Remediate') has an identity with an empty id".into())
    );
}
