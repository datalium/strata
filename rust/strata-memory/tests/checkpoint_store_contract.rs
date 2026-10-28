//! Shared `CheckpointStore` contract test suite: the exact same assertions —
//! including a real freeze → resume through the execution engine — run against
//! every backend: `InMemoryCheckpointStore`, `SqliteCheckpointStore`, and (when
//! `STRATA_TEST_POSTGRES_URL` / `STRATA_TEST_REDIS_URL` are set; see
//! `tests/common`) `PostgresCheckpointStore` and `RedisCheckpointStore`.
//!
//! This is the Rust twin of `HistoryStoreContractTests`'s idea for
//! `HistoryStore`, and what "the checkpoint store abstraction is genuinely
//! pluggable" means in a test: nothing below knows which backend it is running
//! against. Every store also answers the shared query cases
//! (`schema/fixtures/checkpoint-query/cases.json`, `design/19-Async-Queryable-Store.md`)
//! and keeps the whole log.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use common::{PgSandbox, RedisSandbox, backend_url, fixture, run_query_cases, save_query_seed};
use datalium_strata::checkpoint::{
    BlockingCheckpointStore, CancelReason, CheckpointQuery, CheckpointStore, CheckpointStoreError,
    EventSignal, GraphExecutionResult, HumanSignal, LatestCheckpoint, SyncCheckpointStore,
    ThreadEnd, WakeReason,
};
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{CompiledGraph, StateGraph};
use datalium_strata::schema::{
    CANCELLED_CHECKPOINT_SCHEMA_VERSION, Checkpoint, CheckpointStatus,
    DECISION_CHECKPOINT_SCHEMA_VERSION, ENDED_CHECKPOINT_SCHEMA_VERSION, HumanDecision,
    NESTED_CHECKPOINT_SCHEMA_VERSION, SCHEMA_VERSION,
};
use datalium_strata::wait::{WaitOptions, WakeOptions};
use datalium_strata_memory::checkpoint::{InMemoryCheckpointStore, SqliteCheckpointStore};
use datalium_strata_memory::postgres_store::PostgresCheckpointStore;
use datalium_strata_memory::redis_store::RedisCheckpointStore;
use datalium_strata_memory::{
    Deltas, FileSystemBlobStore, GroupCommit, Offload, PostgresStoreOptions, RedisStoreOptions,
    SqliteStoreOptions,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

fn checkpoint(thread_id: &str, status: CheckpointStatus, marker: &str) -> Checkpoint {
    Checkpoint {
        schema_version: SCHEMA_VERSION.to_string(),
        thread_id: thread_id.to_string(),
        status,
        suspended_node: Some("PrepareWireTransfer".to_string()),
        next_node: Some("ExecuteWireTransfer".to_string()),
        frozen_state: json!({
            "marker": marker,
            "amount": 4200.5,
            "count": 9_007_199_254_740_993_u64,
            "flags": [true, false, null],
            "nested": { "text": "say \"hi\" \\ é 日本語 🚀\nnewline", "empty": "" },
            "approved_by": null,
        }),
        metadata: None,
        created_at: "2026-09-16T00:00:00Z".to_string(),
        resumed_at: None,
        nested: None,
        decision: None,
        wait: None,
        graph: None,
    }
}

async fn run_contract(store: &dyn CheckpointStore) {
    // Unknown thread -> `None`, not an error.
    assert_eq!(store.load("missing").await.unwrap(), None);

    // Save then load returns exactly what was saved.
    let first = checkpoint("t1", CheckpointStatus::Interrupted, "first");
    store.save(first.clone()).await.unwrap();
    assert_eq!(store.load("t1").await.unwrap(), Some(first.clone()));

    // A second save for the same thread becomes the one `load` returns —
    // including the fields a resume sets (`resumed_at`, `metadata`).
    let mut second = first.clone();
    second.resumed_at = Some("2026-09-16T01:00:00Z".to_string());
    second.metadata = Some(HashMap::from([
        ("approved_by".to_string(), "admin_01".to_string()),
        ("reviewer_token".to_string(), "token-abc".to_string()),
    ]));
    store.save(second.clone()).await.unwrap();
    assert_eq!(store.load("t1").await.unwrap(), Some(second.clone()));

    // Saving again never resurrects an older checkpoint.
    let third = checkpoint("t1", CheckpointStatus::Completed, "third");
    store.save(third.clone()).await.unwrap();
    assert_eq!(store.load("t1").await.unwrap(), Some(third));

    // Threads are isolated from each other, and every status round-trips.
    let failed = checkpoint("t2", CheckpointStatus::Failed, "other thread");
    store.save(failed.clone()).await.unwrap();
    assert_eq!(store.load("t2").await.unwrap(), Some(failed));
    assert_ne!(
        store.load("t1").await.unwrap(),
        store.load("t2").await.unwrap()
    );

    // A checkpoint with no nodes at all (a completed thread) round-trips too.
    let mut bare = checkpoint("t3", CheckpointStatus::Completed, "bare");
    bare.suspended_node = None;
    bare.next_node = None;
    store.save(bare.clone()).await.unwrap();
    assert_eq!(store.load("t3").await.unwrap(), Some(bare));

    // A checkpoint nesting a child's — which nests a grandchild's — round-trips
    // whole, with each level's own fields intact (`design/16-Nested-Interrupts.md`).
    let mut grandchild = checkpoint("Inner", CheckpointStatus::Interrupted, "grandchild");
    grandchild.metadata = Some(HashMap::from([("note".to_string(), "deep".to_string())]));
    let mut child = checkpoint("Work", CheckpointStatus::Interrupted, "child");
    child.schema_version = NESTED_CHECKPOINT_SCHEMA_VERSION.to_string();
    child.nested = Some(Box::new(grandchild));
    let mut parent = checkpoint("t4", CheckpointStatus::Interrupted, "parent");
    parent.schema_version = NESTED_CHECKPOINT_SCHEMA_VERSION.to_string();
    parent.nested = Some(Box::new(child));
    store.save(parent.clone()).await.unwrap();
    assert_eq!(store.load("t4").await.unwrap(), Some(parent.clone()));

    // A later save without one doesn't inherit the earlier row's child.
    let mut resumed = checkpoint("t4", CheckpointStatus::Completed, "resumed");
    resumed.resumed_at = Some("2026-09-16T01:00:00Z".to_string());
    store.save(resumed.clone()).await.unwrap();
    assert_eq!(store.load("t4").await.unwrap(), Some(resumed));

    // A failed parent keeps the child's failed checkpoint the same way
    // (`design/16-Nested-Interrupts.md`, "Resuming a failed thread").
    let mut failed_child = checkpoint("Work", CheckpointStatus::Failed, "failed child");
    failed_child.metadata = Some(HashMap::from([
        ("error".to_string(), "mailbox closed".to_string()),
        ("error_kind".to_string(), "fatal".to_string()),
        ("attempts".to_string(), "1".to_string()),
    ]));
    let mut failed_parent = checkpoint("t5", CheckpointStatus::Failed, "failed parent");
    failed_parent.schema_version = NESTED_CHECKPOINT_SCHEMA_VERSION.to_string();
    failed_parent.nested = Some(Box::new(failed_child));
    store.save(failed_parent.clone()).await.unwrap();
    assert_eq!(store.load("t5").await.unwrap(), Some(failed_parent.clone()));

    // A cancelled checkpoint round-trips, flat and nesting a cancelled child;
    // so does a failed parent nesting a cancelled child, stopped by the parent
    // node's timeout (`design/17-Async-Execution.md`).
    let mut cancelled = checkpoint("t6", CheckpointStatus::Cancelled, "cancelled");
    cancelled.schema_version = CANCELLED_CHECKPOINT_SCHEMA_VERSION.to_string();
    cancelled.metadata = Some(HashMap::from([(
        "cancel_reason".to_string(),
        "requested".to_string(),
    )]));
    store.save(cancelled.clone()).await.unwrap();
    assert_eq!(store.load("t6").await.unwrap(), Some(cancelled.clone()));

    let mut cancelled_child = cancelled.clone();
    cancelled_child.thread_id = "Work".to_string();
    let mut cancelled_parent = cancelled.clone();
    cancelled_parent.thread_id = "t7".to_string();
    cancelled_parent.nested = Some(Box::new(cancelled_child.clone()));
    store.save(cancelled_parent.clone()).await.unwrap();
    assert_eq!(store.load("t7").await.unwrap(), Some(cancelled_parent));

    cancelled_child.metadata = Some(HashMap::from([(
        "cancel_reason".to_string(),
        "timeout".to_string(),
    )]));
    let mut timed_out_parent = failed_parent;
    timed_out_parent.thread_id = "t8".to_string();
    timed_out_parent.schema_version = CANCELLED_CHECKPOINT_SCHEMA_VERSION.to_string();
    timed_out_parent.nested = Some(Box::new(cancelled_child));
    store.save(timed_out_parent.clone()).await.unwrap();
    assert_eq!(
        store.load("t8").await.unwrap(),
        Some(timed_out_parent.clone())
    );

    // A decision round-trips, with typed overrides, at the top level and nested
    // inside a failed parent; a later save without one doesn't inherit it
    // (design/21-Typed-Human-Signals.md).
    let decision: HumanDecision = serde_json::from_value(json!({
        "boundary": "Approve",
        "is_approved": false,
        "reviewer": "bob@acme.io",
        "overrides": {
            "plan": { "steps": ["drain"] },
            "replicas": 3,
            "dry_run": true,
            "note": null,
            "reason": "no"
        },
        "decided_at": "2026-10-04T00:00:00Z"
    }))
    .unwrap();
    let mut decided = checkpoint("t9", CheckpointStatus::Failed, "decided");
    decided.schema_version = DECISION_CHECKPOINT_SCHEMA_VERSION.to_string();
    decided.decision = Some(decision.clone());
    store.save(decided.clone()).await.unwrap();
    assert_eq!(store.load("t9").await.unwrap(), Some(decided));

    let mut decided_child = checkpoint("Work", CheckpointStatus::Failed, "decided child");
    decided_child.schema_version = DECISION_CHECKPOINT_SCHEMA_VERSION.to_string();
    decided_child.decision = Some(decision);
    let mut decided_parent = timed_out_parent;
    decided_parent.thread_id = "t10".to_string();
    decided_parent.schema_version = DECISION_CHECKPOINT_SCHEMA_VERSION.to_string();
    decided_parent.nested = Some(Box::new(decided_child));
    store.save(decided_parent.clone()).await.unwrap();
    assert_eq!(store.load("t10").await.unwrap(), Some(decided_parent));

    let undecided = checkpoint("t9", CheckpointStatus::Interrupted, "undecided");
    store.save(undecided.clone()).await.unwrap();
    assert_eq!(store.load("t9").await.unwrap(), Some(undecided));
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct WireTransferState {
    invoice_generated: bool,
    amount: f64,
    currency: String,
    approved_by: Option<String>,
    is_completed: bool,
}

fn wire_transfer_graph() -> CompiledGraph<WireTransferState> {
    StateGraph::<WireTransferState>::new()
        .add_node("PrepareWireTransfer", |s: WireTransferState| {
            WireTransferState {
                invoice_generated: true,
                ..s
            }
        })
        .add_interrupt_boundary("PrepareWireTransfer")
        .add_node("ExecuteWireTransfer", |s: WireTransferState| {
            WireTransferState {
                is_completed: true,
                ..s
            }
        })
        .add_edge("PrepareWireTransfer", "ExecuteWireTransfer")
        .compile()
        .unwrap()
}

/// The human-in-the-loop lifecycle end to end against `store`: the graph
/// freezes, a *different* graph instance (standing in for another process)
/// resumes it from the store, and a second resume is refused.
async fn run_freeze_resume(store: Arc<dyn CheckpointStore>) {
    let initial = WireTransferState {
        invoice_generated: false,
        amount: 4200.5,
        currency: "USD".to_string(),
        approved_by: None,
        is_completed: false,
    };

    match wire_transfer_graph()
        .run("wire-1", initial, store.clone())
        .await
    {
        GraphExecutionResult::Interrupted { .. } => {}
        _ => panic!("expected the graph to freeze at its interrupt boundary"),
    }
    let frozen = store.load("wire-1").await.unwrap().unwrap();
    assert_eq!(frozen.status, CheckpointStatus::Interrupted);
    assert_eq!(frozen.next_node.as_deref(), Some("ExecuteWireTransfer"));

    let signal = HumanSignal {
        is_approved: true,
        reviewer_token: "token-abc".to_string(),
        field_overrides: HashMap::from([("approved_by".to_string(), "admin_01".to_string())]),
        ..Default::default()
    };
    match wire_transfer_graph()
        .resume("wire-1", signal.clone(), store.clone())
        .await
    {
        GraphExecutionResult::Completed(state) => assert_eq!(
            state,
            WireTransferState {
                invoice_generated: true,
                amount: 4200.5,
                currency: "USD".to_string(),
                approved_by: Some("admin_01".to_string()),
                is_completed: true,
            }
        ),
        _ => panic!("expected Completed after resume"),
    }

    // The store now holds the resumed checkpoint, and resuming it again is
    // refused rather than replayed.
    let resumed = store.load("wire-1").await.unwrap().unwrap();
    assert!(resumed.resumed_at.is_some());
    assert!(!matches!(
        wire_transfer_graph()
            .resume("wire-1", signal, store.clone())
            .await,
        GraphExecutionResult::Completed(_)
    ));
}

/// The conditional save (`design/28-Ending-A-Thread.md`, "Races"): `load_latest`
/// reports the sequence the log has, `save_if_latest` appends only on that
/// sequence and writes nothing otherwise, and a `None` sequence saves anyway.
async fn run_conditional_save_contract(store: &dyn CheckpointStore) {
    assert_eq!(store.load_latest("cas").await.unwrap(), None);

    let first = checkpoint("cas", CheckpointStatus::Interrupted, "first");
    store.save(first.clone()).await.unwrap();
    assert_eq!(
        store.load_latest("cas").await.unwrap(),
        Some(LatestCheckpoint {
            checkpoint: first.clone(),
            sequence: Some(0),
        })
    );

    let mut stamp = first.clone();
    stamp.resumed_at = Some("2026-09-16T01:00:00Z".to_string());
    assert!(store.save_if_latest(stamp.clone(), Some(0)).await.unwrap());
    assert_eq!(
        store.load_latest("cas").await.unwrap(),
        Some(LatestCheckpoint {
            checkpoint: stamp.clone(),
            sequence: Some(1),
        })
    );

    // A stale sequence loses, and leaves the log as it was.
    let late = checkpoint("cas", CheckpointStatus::Failed, "late");
    assert!(!store.save_if_latest(late.clone(), Some(0)).await.unwrap());
    assert_eq!(store.load("cas").await.unwrap(), Some(stamp));

    // No sequence: an unconditional save.
    assert!(store.save_if_latest(late.clone(), None).await.unwrap());
    assert_eq!(
        store.load_latest("cas").await.unwrap(),
        Some(LatestCheckpoint {
            checkpoint: late,
            sequence: Some(2),
        })
    );
}

/// Ending a frozen thread against `store` (`design/28-Ending-A-Thread.md`):
/// the end writes one `ended` checkpoint, a resume is refused, and a second
/// end returns the same answer without writing.
async fn run_freeze_end(store: Arc<dyn CheckpointStore>) {
    let initial = WireTransferState {
        invoice_generated: false,
        amount: 4200.5,
        currency: "USD".to_string(),
        approved_by: None,
        is_completed: false,
    };
    let graph = wire_transfer_graph();
    assert!(matches!(
        graph.run("wire-end", initial, store.clone()).await,
        GraphExecutionResult::Interrupted { .. }
    ));

    for _ in 0..2 {
        match graph
            .end_with(
                "wire-end",
                ThreadEnd::by("oncall"),
                store.clone(),
                Default::default(),
            )
            .await
        {
            GraphExecutionResult::Cancelled {
                reason: CancelReason::Ended,
                node,
                state,
                ..
            } => {
                assert_eq!(node, "PrepareWireTransfer");
                assert!(state.invoice_generated);
            }
            _ => panic!("expected the thread to end"),
        }
    }
    let ended = store.load("wire-end").await.unwrap().unwrap();
    assert_eq!(ended.status, CheckpointStatus::Ended);
    assert_eq!(ended.schema_version, ENDED_CHECKPOINT_SCHEMA_VERSION);
    assert_eq!(ended.next_node, None);
    let ended_by = ended
        .metadata
        .as_ref()
        .and_then(|m| m.get("strata.ended_by"));
    assert_eq!(ended_by.map(String::as_str), Some("oncall"));

    assert!(matches!(
        graph
            .resume("wire-end", HumanSignal::approve("alice"), store.clone())
            .await,
        GraphExecutionResult::Failed(_)
    ));
    assert_eq!(store.load("wire-end").await.unwrap(), Some(ended));
}

/// Waits against `store` (`design/29-Event-Waits-And-Timers.md`): the waiting
/// lifecycle fixtures round-trip, a timer is listed as due by the store itself
/// and woken once, and an event wait is signalled.
async fn run_freeze_wake<T: CheckpointStore + CheckpointQuery + 'static>(store: Arc<T>) {
    for name in [
        "waiting-event.json",
        "waiting-approval.json",
        "nested-timer.json",
    ] {
        let waiting: Checkpoint =
            serde_json::from_value(fixture(&format!("checkpoint-lifecycle/{name}"))).unwrap();
        store.save(waiting.clone()).await.unwrap();
        assert_eq!(store.load(&waiting.thread_id).await.unwrap(), Some(waiting));
    }

    let graph = StateGraph::<i32>::new()
        .add_timer("Window", std::time::Duration::from_secs(60))
        .add_wait_for_event_with(
            "Deploying",
            "deployment.finished",
            WaitOptions::new().with_timeout(std::time::Duration::from_secs(3600)),
        )
        .add_node("After", |s: i32| s + 1)
        .add_edge("Window", "Deploying")
        .add_edge("Deploying", "After")
        .compile()
        .unwrap();
    let as_store: Arc<dyn CheckpointStore> = store.clone();
    let config = RunConfig::new().with_metadata("graph", "contract");
    assert!(matches!(
        graph
            .run_with("wait-contract", 1, as_store.clone(), config)
            .await,
        GraphExecutionResult::Interrupted { .. }
    ));
    // Only this graph's threads: the fixtures' are left alone.
    let options = WakeOptions::new().with_metadata("graph", "contract");
    let later = chrono::Utc::now() + chrono::Duration::minutes(2);
    let report = graph
        .wake_due_with(store.clone(), later, options.clone())
        .await
        .unwrap();
    let woken: Vec<(&str, WakeReason)> = report
        .woken
        .iter()
        .map(|w| (w.thread_id.as_str(), w.reason))
        .collect();
    assert_eq!(woken, [("wait-contract", WakeReason::Timer)]);
    // It froze again at the event wait, which isn't due for an hour.
    assert!(matches!(
        report.woken[0].result,
        GraphExecutionResult::Interrupted { .. }
    ));
    assert!(
        graph
            .wake_due_with(store.clone(), later, options)
            .await
            .unwrap()
            .woken
            .is_empty()
    );

    match graph
        .signal(
            "wait-contract",
            EventSignal::new("deployment.finished"),
            as_store,
        )
        .await
    {
        GraphExecutionResult::Completed(state) => assert_eq!(state, 2),
        _ => panic!("expected the signal to complete the thread"),
    }
}

/// A store whose save and load really suspend: they complete only after a
/// timer, on a later poll. The engine must wait for both at every save point.
#[derive(Default)]
struct SlowStore(InMemoryCheckpointStore);

impl CheckpointStore for SlowStore {
    fn save(
        &self,
        checkpoint: Checkpoint,
    ) -> datalium_strata::BoxFuture<'_, Result<(), CheckpointStoreError>> {
        Box::pin(async move {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            self.0.save(checkpoint).await
        })
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> datalium_strata::BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        Box::pin(async move {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            self.0.load(thread_id).await
        })
    }
}

/// A store with a blocking API, the shape every store had before SG-11.
#[derive(Default)]
struct LegacyStore(std::sync::Mutex<HashMap<String, Checkpoint>>);

impl SyncCheckpointStore for LegacyStore {
    fn save(&self, checkpoint: Checkpoint) -> Result<(), CheckpointStoreError> {
        // Blocking here is the point: the adapter keeps it off the runtime.
        std::thread::sleep(std::time::Duration::from_millis(1));
        self.0
            .lock()
            .unwrap()
            .insert(checkpoint.thread_id.clone(), checkpoint);
        Ok(())
    }

    fn load(&self, thread_id: &str) -> Result<Option<Checkpoint>, CheckpointStoreError> {
        Ok(self.0.lock().unwrap().get(thread_id).cloned())
    }
}

/// Every save is kept, in order, and `get_log` reads them back.
async fn run_log_contract(store: &(impl CheckpointStore + CheckpointQuery)) {
    let first = checkpoint("logged", CheckpointStatus::Interrupted, "first");
    let mut stamp = first.clone();
    stamp.resumed_at = Some("2026-09-16T01:00:00Z".to_string());
    let failed = checkpoint("logged", CheckpointStatus::Failed, "failed");
    for saved in [&first, &stamp, &failed] {
        store.save(saved.clone()).await.unwrap();
    }

    let log = store.get_log("logged", None, 10).await.unwrap();
    let entries: Vec<(i64, Checkpoint)> = log
        .entries
        .into_iter()
        .map(|entry| (entry.sequence, entry.checkpoint))
        .collect();
    assert_eq!(entries, [(0, first), (1, stamp), (2, failed.clone())]);
    assert_eq!(log.next_after_sequence, None);
    assert_eq!(store.load("logged").await.unwrap(), Some(failed));
}

async fn postgres_store() -> Option<(PgSandbox, PostgresCheckpointStore)> {
    let url = backend_url(common::POSTGRES_VAR)?;
    let sandbox = PgSandbox::new(&url, "checkpoint");
    let store = PostgresCheckpointStore::from_client(sandbox.client().await)
        .await
        .unwrap();
    Some((sandbox, store))
}

async fn redis_store() -> Option<(RedisSandbox, RedisCheckpointStore)> {
    let url = backend_url(common::REDIS_VAR)?;
    let sandbox = RedisSandbox::new(&url, "checkpoint");
    let store = RedisCheckpointStore::connect_with_prefix(&url, &sandbox.prefix)
        .await
        .unwrap();
    Some((sandbox, store))
}

#[tokio::test]
async fn in_memory_store_satisfies_the_checkpoint_store_contract() {
    run_contract(&InMemoryCheckpointStore::new()).await;
    run_log_contract(&InMemoryCheckpointStore::new()).await;
    run_conditional_save_contract(&InMemoryCheckpointStore::new()).await;
    run_freeze_resume(Arc::new(InMemoryCheckpointStore::new())).await;
    run_freeze_end(Arc::new(InMemoryCheckpointStore::new())).await;
    run_freeze_wake(Arc::new(InMemoryCheckpointStore::new())).await;
}

#[tokio::test]
async fn sqlite_store_satisfies_the_checkpoint_store_contract() {
    run_contract(&SqliteCheckpointStore::open_in_memory().unwrap()).await;
    run_log_contract(&SqliteCheckpointStore::open_in_memory().unwrap()).await;
    run_conditional_save_contract(&SqliteCheckpointStore::open_in_memory().unwrap()).await;
    run_freeze_resume(Arc::new(SqliteCheckpointStore::open_in_memory().unwrap())).await;
    run_freeze_end(Arc::new(SqliteCheckpointStore::open_in_memory().unwrap())).await;
    run_freeze_wake(Arc::new(SqliteCheckpointStore::open_in_memory().unwrap())).await;
}

/// A SQLite store whose saves go through group commit
/// (`design/41-Checkpoint-Efficiency.md`).
fn batched_sqlite() -> SqliteCheckpointStore {
    SqliteCheckpointStore::open_in_memory_with(
        SqliteStoreOptions::new().with_group_commit(GroupCommit::new()),
    )
    .unwrap()
}

#[tokio::test]
async fn sqlite_store_with_group_commit_satisfies_the_checkpoint_store_contract() {
    run_contract(&batched_sqlite()).await;
    run_log_contract(&batched_sqlite()).await;
    run_conditional_save_contract(&batched_sqlite()).await;
    run_freeze_resume(Arc::new(batched_sqlite())).await;
    run_freeze_end(Arc::new(batched_sqlite())).await;
    run_freeze_wake(Arc::new(batched_sqlite())).await;
}

#[tokio::test]
async fn postgres_store_with_group_commit_satisfies_the_checkpoint_store_contract() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "batched");
    let store = Arc::new(
        PostgresCheckpointStore::connect_with(
            &sandbox.url(None),
            PostgresStoreOptions::new().with_group_commit(GroupCommit::new()),
        )
        .await
        .unwrap(),
    );
    run_contract(&*store).await;
    run_log_contract(&*store).await;
    run_conditional_save_contract(&*store).await;
    run_freeze_resume(store.clone()).await;
    run_freeze_end(store.clone()).await;
    run_freeze_wake(store).await;
}

/// A SQLite store that stores running checkpoints as deltas
/// (`design/41-Checkpoint-Efficiency.md`, "Deltas").
fn delta_sqlite() -> SqliteCheckpointStore {
    SqliteCheckpointStore::open_in_memory_with(SqliteStoreOptions::new().with_deltas(Deltas::new()))
        .unwrap()
}

#[tokio::test]
async fn sqlite_store_with_deltas_satisfies_the_checkpoint_store_contract() {
    run_contract(&delta_sqlite()).await;
    run_log_contract(&delta_sqlite()).await;
    run_conditional_save_contract(&delta_sqlite()).await;
    run_freeze_resume(Arc::new(delta_sqlite())).await;
    run_freeze_end(Arc::new(delta_sqlite())).await;
    run_freeze_wake(Arc::new(delta_sqlite())).await;
}

#[tokio::test]
async fn postgres_store_with_deltas_satisfies_the_checkpoint_store_contract() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "deltas");
    let store = Arc::new(
        PostgresCheckpointStore::connect_with(
            &sandbox.url(None),
            PostgresStoreOptions::new()
                .with_deltas(Deltas::new())
                .with_group_commit(GroupCommit::new()),
        )
        .await
        .unwrap(),
    );
    run_contract(&*store).await;
    run_log_contract(&*store).await;
    run_conditional_save_contract(&*store).await;
    run_freeze_resume(store.clone()).await;
    run_freeze_end(store.clone()).await;
    run_freeze_wake(store).await;
}

#[tokio::test]
async fn redis_store_with_deltas_satisfies_the_checkpoint_store_contract() {
    let Some(url) = backend_url(common::REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "deltas");
    let store = Arc::new(
        RedisCheckpointStore::connect_with(
            &url,
            RedisStoreOptions::new()
                .with_prefix(&sandbox.prefix)
                .with_deltas(Deltas::new()),
        )
        .await
        .unwrap(),
    );
    run_contract(&*store).await;
    run_log_contract(&*store).await;
    run_conditional_save_contract(&*store).await;
    run_freeze_resume(store.clone()).await;
    run_freeze_end(store.clone()).await;
    run_freeze_wake(store).await;
}

/// Every value offloaded (`design/41-Checkpoint-Efficiency.md`, "Offload"):
/// the contracts' small states put every top-level value in the blob store.
fn everything() -> Offload {
    Offload::new().with_threshold(1)
}

/// A SQLite store that offloads every value, to its own file or to `blobs`.
fn offload_sqlite(blobs: Option<&Arc<FileSystemBlobStore>>) -> SqliteCheckpointStore {
    let offload = match blobs {
        Some(blobs) => everything().with_blobs(blobs.clone()),
        None => everything(),
    };
    SqliteCheckpointStore::open_in_memory_with(
        SqliteStoreOptions::new()
            .with_deltas(Deltas::new())
            .with_offload(offload),
    )
    .unwrap()
}

#[tokio::test]
async fn sqlite_store_with_offload_satisfies_the_checkpoint_store_contract() {
    let root = std::env::temp_dir().join(common::unique_name("contract-blobs"));
    let files = Arc::new(FileSystemBlobStore::new(&root));
    for blobs in [None, Some(&files)] {
        run_contract(&offload_sqlite(blobs)).await;
        run_log_contract(&offload_sqlite(blobs)).await;
        run_conditional_save_contract(&offload_sqlite(blobs)).await;
        run_freeze_resume(Arc::new(offload_sqlite(blobs))).await;
        run_freeze_end(Arc::new(offload_sqlite(blobs))).await;
        run_freeze_wake(Arc::new(offload_sqlite(blobs))).await;
    }
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn postgres_store_with_offload_satisfies_the_checkpoint_store_contract() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "offload");
    let store = Arc::new(
        PostgresCheckpointStore::connect_with(
            &sandbox.url(None),
            PostgresStoreOptions::new()
                .with_deltas(Deltas::new())
                .with_offload(everything())
                .with_group_commit(GroupCommit::new()),
        )
        .await
        .unwrap(),
    );
    run_contract(&*store).await;
    run_log_contract(&*store).await;
    run_conditional_save_contract(&*store).await;
    run_freeze_resume(store.clone()).await;
    run_freeze_end(store.clone()).await;
    run_freeze_wake(store).await;
}

#[tokio::test]
async fn redis_store_with_offload_satisfies_the_checkpoint_store_contract() {
    let Some(url) = backend_url(common::REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "offload");
    let store = Arc::new(
        RedisCheckpointStore::connect_with(
            &url,
            RedisStoreOptions::new()
                .with_prefix(&sandbox.prefix)
                .with_deltas(Deltas::new())
                .with_offload(everything()),
        )
        .await
        .unwrap(),
    );
    run_contract(&*store).await;
    run_log_contract(&*store).await;
    run_conditional_save_contract(&*store).await;
    run_freeze_resume(store.clone()).await;
    run_freeze_end(store.clone()).await;
    run_freeze_wake(store).await;
}

#[tokio::test]
async fn postgres_store_satisfies_the_checkpoint_store_contract() {
    let Some((_sandbox, store)) = postgres_store().await else {
        return;
    };
    run_contract(&store).await;
    run_log_contract(&store).await;
    run_conditional_save_contract(&store).await;
}

#[tokio::test]
async fn postgres_store_runs_a_freeze_and_resume_through_the_engine() {
    let Some((_sandbox, store)) = postgres_store().await else {
        return;
    };
    let store = Arc::new(store);
    run_freeze_resume(store.clone()).await;
    run_freeze_end(store.clone()).await;
    run_freeze_wake(store).await;
}

#[tokio::test]
async fn redis_store_satisfies_the_checkpoint_store_contract() {
    let Some((_sandbox, store)) = redis_store().await else {
        return;
    };
    run_contract(&store).await;
    run_log_contract(&store).await;
    run_conditional_save_contract(&store).await;
}

#[tokio::test]
async fn redis_store_runs_a_freeze_and_resume_through_the_engine() {
    let Some((_sandbox, store)) = redis_store().await else {
        return;
    };
    let store = Arc::new(store);
    run_freeze_resume(store.clone()).await;
    run_freeze_end(store.clone()).await;
    run_freeze_wake(store).await;
}

#[tokio::test]
async fn a_store_that_really_suspends_drives_a_freeze_and_resume() {
    run_freeze_resume(Arc::new(SlowStore::default())).await;
}

#[tokio::test]
async fn a_blocking_store_drives_the_engine_through_the_blocking_adapter() {
    let legacy = Arc::new(LegacyStore::default());
    run_freeze_resume(Arc::new(BlockingCheckpointStore::from_arc(legacy.clone()))).await;
    assert!(legacy.load("wire-1").unwrap().unwrap().resumed_at.is_some());
}

#[tokio::test]
async fn a_store_without_the_conditional_save_still_resumes_and_ends() {
    // The trait's defaults: no sequence, an unconditional save.
    run_freeze_resume(Arc::new(SlowStore::default())).await;
    run_freeze_end(Arc::new(SlowStore::default())).await;
    let legacy = Arc::new(LegacyStore::default());
    let adapted = BlockingCheckpointStore::from_arc(legacy.clone());
    assert_eq!(adapted.load_latest("missing").await.unwrap(), None);
    run_freeze_end(Arc::new(adapted)).await;
    assert_eq!(
        legacy.load("wire-end").unwrap().unwrap().status,
        CheckpointStatus::Ended
    );
}

// --- The query port (`design/19-Async-Queryable-Store.md`): every store answers
// the shared cases identically. Redis may page short, so it is held to the
// threads and their order, not the page boundaries.

#[tokio::test]
async fn in_memory_store_answers_the_shared_query_cases() {
    let store = InMemoryCheckpointStore::new();
    save_query_seed(&store).await;
    run_query_cases(&store, true).await;
}

#[tokio::test]
async fn sqlite_store_answers_the_shared_query_cases() {
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    save_query_seed(&store).await;
    run_query_cases(&store, true).await;
}

#[tokio::test]
async fn postgres_store_answers_the_shared_query_cases() {
    let Some((_sandbox, store)) = postgres_store().await else {
        return;
    };
    save_query_seed(&store).await;
    run_query_cases(&store, true).await;
}

#[tokio::test]
async fn redis_store_answers_the_shared_query_cases() {
    let Some((_sandbox, store)) = redis_store().await else {
        return;
    };
    save_query_seed(&store).await;
    run_query_cases(&store, false).await;
}
