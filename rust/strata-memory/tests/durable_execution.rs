//! A durable run and a recovery in every store
//! (`design/33-Durable-Execution.md`): a checkpoint per node, the `running`
//! and `completed` thread states, a dead thread a live lease protects, and the
//! driver recovering it once the lease is gone. The xUnit twin is
//! `StoreContracts.DurableRunContractAsync`.
//!
//! The Postgres and Redis tests need `STRATA_TEST_POSTGRES_URL` /
//! `STRATA_TEST_REDIS_URL` (see `common`).

mod common;

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use common::{PgSandbox, RedisSandbox, backend_url};
use datalium_strata::checkpoint::{
    CheckpointQuery, CheckpointStore, GraphExecutionResult, LeaseAttempt, LeaseOptions,
    ThreadQuery, ThreadState,
};
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{CheckpointPolicy, CompileOptions, CompiledGraph, StateGraph};
use datalium_strata::recovery::RecoverOptions;
use datalium_strata::schema::CheckpointStatus;
use datalium_strata::store::LeasedSave;
use datalium_strata_memory::checkpoint::{InMemoryCheckpointStore, SqliteCheckpointStore};
use datalium_strata_memory::postgres_store::PostgresCheckpointStore;
use datalium_strata_memory::redis_store::RedisCheckpointStore;
use datalium_strata_memory::{
    Deltas, GroupCommit, PostgresStoreOptions, RedisStoreOptions, SqliteStoreOptions,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct WireTransferState {
    invoice_generated: bool,
    is_completed: bool,
}

/// `Draft` -> `Send`, with a checkpoint per node.
fn wire_transfer_graph() -> CompiledGraph<WireTransferState> {
    StateGraph::new()
        .add_node("Draft", |mut s: WireTransferState| {
            s.invoice_generated = true;
            s
        })
        .add_node("Send", |mut s: WireTransferState| {
            s.is_completed = true;
            s
        })
        .add_edge("Draft", "Send")
        .compile_with(CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode))
        .unwrap()
}

async fn statuses<T: CheckpointQuery>(store: &T, thread_id: &str) -> Vec<CheckpointStatus> {
    store
        .get_log(thread_id, None, 100)
        .await
        .unwrap()
        .entries
        .into_iter()
        .map(|entry| entry.checkpoint.status)
        .collect()
}

async fn running<T: CheckpointQuery>(store: &T) -> Vec<String> {
    store
        .list_threads(&ThreadQuery::default().with_states([ThreadState::Running]))
        .await
        .unwrap()
        .threads
        .into_iter()
        .map(|thread| thread.thread_id)
        .collect()
}

/// A durable run and a recovery, end to end
/// (`design/33-Durable-Execution.md`): the run writes a row per node and says
/// it finished; a thread whose process died is listed as running, left alone
/// while a lease holds it, and recovered by the driver once the lease is gone.
async fn run_durable_run<T>(store: Arc<T>)
where
    T: CheckpointStore + CheckpointQuery + 'static,
{
    use CheckpointStatus::{Completed, Running};
    let graph = wire_transfer_graph();
    let leased =
        || RunConfig::new().with_lease(LeaseOptions::new("worker-1", Duration::from_secs(60)));
    let initial = WireTransferState {
        invoice_generated: false,
        is_completed: false,
    };

    assert!(matches!(
        graph
            .run_with("wire-durable", initial, store.clone(), leased())
            .await,
        GraphExecutionResult::Completed(_)
    ));
    assert_eq!(
        statuses(&*store, "wire-durable").await,
        [Running, Running, Completed]
    );
    assert!(running(&*store).await.is_empty());

    // A thread whose process died after `Draft`, as its last checkpoint left
    // it, still held by the dead worker's lease.
    let mut dead = store
        .get_log("wire-durable", None, 100)
        .await
        .unwrap()
        .entries
        .remove(1)
        .checkpoint;
    dead.thread_id = "wire-dead".to_string();
    let lease = match store
        .try_acquire_lease("wire-dead", "worker-9", Duration::from_secs(60))
        .await
        .unwrap()
    {
        LeaseAttempt::Acquired(lease) => lease,
        LeaseAttempt::Held(holder) => panic!("wire-dead is held by {}", holder.owner),
    };
    assert_eq!(
        store.save_under_lease(dead, &lease, None).await.unwrap(),
        LeasedSave::Saved
    );
    assert_eq!(running(&*store).await, ["wire-dead"]);

    let options = || RecoverOptions::new().with_config(leased());
    let held = graph
        .recover_stale(store.clone(), Utc::now(), options())
        .await
        .unwrap();
    assert!(held.recovered.is_empty());
    assert_eq!(held.skipped.len(), 1);
    assert!(
        held.skipped[0]
            .reason
            .starts_with("it is held by 'worker-9' until "),
        "{}",
        held.skipped[0].reason
    );

    // The lease is let go, as if it had run out, and the thread is nobody's.
    assert!(store.release_lease(&lease).await.unwrap());
    let mut report = graph
        .recover_stale(store.clone(), Utc::now(), options())
        .await
        .unwrap();

    assert_eq!(report.recovered.len(), 1);
    let recovered = report.recovered.remove(0);
    assert_eq!(
        (recovered.thread_id.as_str(), recovered.recoveries),
        ("wire-dead", 1)
    );
    assert_eq!(recovered.at, ["Send"]);
    match recovered.result {
        GraphExecutionResult::Completed(state) => assert!(state.is_completed),
        _ => panic!("the recovered run completes"),
    }
    assert_eq!(
        statuses(&*store, "wire-dead").await,
        [Running, Running, Completed]
    );
    let claim = store
        .get_log("wire-dead", None, 100)
        .await
        .unwrap()
        .entries
        .remove(1)
        .checkpoint;
    assert_eq!(
        claim
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("strata.recoveries"))
            .map(String::as_str),
        Some("1")
    );
    assert!(running(&*store).await.is_empty());
    assert_eq!(store.get_lease("wire-dead").await.unwrap(), None);
}

#[tokio::test]
async fn in_memory_store_runs_durably() {
    run_durable_run(Arc::new(InMemoryCheckpointStore::new())).await;
}

#[tokio::test]
async fn sqlite_store_runs_durably() {
    run_durable_run(Arc::new(SqliteCheckpointStore::open_in_memory().unwrap())).await;
}

#[tokio::test]
async fn postgres_store_runs_durably() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "durable_run");
    let store = PostgresCheckpointStore::from_client(sandbox.client().await)
        .await
        .unwrap();
    run_durable_run(Arc::new(store)).await;
}

#[tokio::test]
async fn sqlite_store_runs_durably_with_group_commit() {
    let store = SqliteCheckpointStore::open_in_memory_with(
        SqliteStoreOptions::new().with_group_commit(GroupCommit::new()),
    )
    .unwrap();
    run_durable_run(Arc::new(store)).await;
}

#[tokio::test]
async fn postgres_store_runs_durably_with_group_commit() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "durable_batched");
    let store = PostgresCheckpointStore::connect_with(
        &sandbox.url(None),
        PostgresStoreOptions::new().with_group_commit(GroupCommit::new()),
    )
    .await
    .unwrap();
    run_durable_run(Arc::new(store)).await;
}

#[tokio::test]
async fn sqlite_store_runs_durably_with_deltas() {
    let store = SqliteCheckpointStore::open_in_memory_with(
        SqliteStoreOptions::new()
            .with_deltas(Deltas::new())
            .with_group_commit(GroupCommit::new()),
    )
    .unwrap();
    run_durable_run(Arc::new(store)).await;
}

#[tokio::test]
async fn postgres_store_runs_durably_with_deltas() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "durable_deltas");
    let store = PostgresCheckpointStore::connect_with(
        &sandbox.url(None),
        PostgresStoreOptions::new().with_deltas(Deltas::new()),
    )
    .await
    .unwrap();
    run_durable_run(Arc::new(store)).await;
}

#[tokio::test]
async fn redis_store_runs_durably_with_deltas() {
    let Some(url) = backend_url(common::REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "durable_deltas");
    let store = RedisCheckpointStore::connect_with(
        &url,
        RedisStoreOptions::new()
            .with_prefix(&sandbox.prefix)
            .with_deltas(Deltas::new()),
    )
    .await
    .unwrap();
    run_durable_run(Arc::new(store)).await;
}

#[tokio::test]
async fn redis_store_runs_durably() {
    let Some(url) = backend_url(common::REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "durable_run");
    let store = RedisCheckpointStore::connect_with_prefix(&url, &sandbox.prefix)
        .await
        .unwrap();
    run_durable_run(Arc::new(store)).await;
}
