//! Storage hardening (`design/34-Storage-Hardening.md`, SG-30): retries after
//! transient failures, a save that may have happened, reconnecting, the
//! schema-version table, and retention. The policy itself is tested in
//! `src/retry.rs`. The xUnit twin is `StorageHardeningTests`.
//!
//! The Postgres and Redis tests need `STRATA_TEST_POSTGRES_URL` /
//! `STRATA_TEST_REDIS_URL` (see `common`).

mod common;

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::time::Duration;

use chrono::{TimeZone, Utc};
use common::{PgSandbox, RedisSandbox, backend_url, fixture, redis_connection, unique_name};
use datalium_strata::checkpoint::{
    CheckpointQuery, CheckpointRetention, CheckpointStore, GraphExecutionResult, LeaseAttempt,
    MAX_PAGE_SIZE, PurgeStates, ThreadLease, ThreadQuery, ThreadState,
};
use datalium_strata::graph::{CheckpointPolicy, CompileOptions, CompiledGraph, StateGraph};
use datalium_strata::history::{ChatMessage, HistoryStore, MessageRole};
use datalium_strata::schema::{Checkpoint, CheckpointStatus, SCHEMA_VERSION};
use datalium_strata::store::LeasedSave;
use datalium_strata_memory::checkpoint::{InMemoryCheckpointStore, SqliteCheckpointStore};
use datalium_strata_memory::postgres_store::{
    PostgresCheckpointStore, PostgresHistoryStore, is_transient_sqlstate,
};
use datalium_strata_memory::redis_store::{RedisCheckpointStore, is_transient_reply};
use datalium_strata_memory::retry::{OPERATION_LABEL, RETRIES_METRIC, STORE_LABEL};
use datalium_strata_memory::schema_versions::{self, SchemaStep};
use datalium_strata_memory::{PostgresStoreOptions, RedisStoreOptions, StoreRetry};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const LASTING: Duration = Duration::from_secs(60);

fn checkpoint(thread_id: &str, status: CheckpointStatus, created_at: &str) -> Checkpoint {
    Checkpoint {
        schema_version: SCHEMA_VERSION.to_string(),
        thread_id: thread_id.to_string(),
        status,
        suspended_node: Some("PrepareWireTransfer".to_string()),
        next_node: Some("ExecuteWireTransfer".to_string()),
        frozen_state: json!({ "payee": "Initech", "amount": 4200.5, "attempts": 2 }),
        metadata: None,
        created_at: created_at.to_string(),
        resumed_at: None,
        nested: None,
        decision: None,
        wait: None,
        graph: None,
    }
}

fn waiting(thread_id: &str, created_at: &str) -> Checkpoint {
    checkpoint(thread_id, CheckpointStatus::Interrupted, created_at)
}

/// Fails `point` the first `times` times it is reached.
fn fault_at(point: &'static str, times: i32) -> impl Fn(&str) -> bool + Send + Sync + 'static {
    let left = AtomicI32::new(times);
    move |reached| reached == point && left.fetch_sub(1, Ordering::SeqCst) > 0
}

/// Counts `strata.store.retries` on this thread while it lives: a
/// `#[tokio::test]` runs its store calls on the test's own thread.
struct RetryCounter {
    snapshotter: Snapshotter,
    seen: Cell<u64>,
    last: RefCell<Option<(String, String)>>,
    _guard: metrics::LocalRecorderGuard<'static>,
}

impl RetryCounter {
    fn new() -> Self {
        let recorder: &'static DebuggingRecorder = Box::leak(Box::new(DebuggingRecorder::new()));
        Self {
            snapshotter: recorder.snapshotter(),
            seen: Cell::new(0),
            last: RefCell::new(None),
            _guard: metrics::set_default_local_recorder(recorder),
        }
    }

    fn count(&self) -> u64 {
        // A snapshot takes the counts with it.
        for (key, _, _, value) in self.snapshotter.snapshot().into_vec() {
            if key.key().name() == RETRIES_METRIC
                && let DebugValue::Counter(count) = value
            {
                self.seen.set(self.seen.get() + count);
                let label = |name: &str| {
                    key.key()
                        .labels()
                        .find(|label| label.key() == name)
                        .map(|label| label.value().to_string())
                        .unwrap_or_default()
                };
                *self.last.borrow_mut() = Some((label(STORE_LABEL), label(OPERATION_LABEL)));
            }
        }
        self.seen.get()
    }

    /// The store and the operation of the repeats counted last.
    fn last(&self) -> Option<(String, String)> {
        self.last.borrow().clone()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct WireTransferState {
    invoice_generated: bool,
    is_completed: bool,
}

fn initial_state() -> WireTransferState {
    WireTransferState {
        invoice_generated: false,
        is_completed: false,
    }
}

type Hook = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// `Draft` -> `Send` with a checkpoint per node; `while_drafting` runs between
/// the run's first save and its second.
fn draft_then_send(while_drafting: Hook) -> CompiledGraph<WireTransferState> {
    StateGraph::new()
        .add_async_node("Draft", move |mut s: WireTransferState, _ctx| {
            let while_drafting = while_drafting.clone();
            async move {
                while_drafting().await;
                s.invoice_generated = true;
                s
            }
        })
        .add_node("Send", |mut s: WireTransferState| {
            s.is_completed = true;
            s
        })
        .add_edge("Draft", "Send")
        .compile_with(CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode))
        .unwrap()
}

async fn statuses(store: &dyn CheckpointQuery, thread_id: &str) -> Vec<CheckpointStatus> {
    store
        .get_log(thread_id, None, 100)
        .await
        .unwrap()
        .entries
        .into_iter()
        .map(|entry| entry.checkpoint.status)
        .collect()
}

async fn log_length(store: &dyn CheckpointQuery, thread_id: &str) -> usize {
    store
        .get_log(thread_id, None, 100)
        .await
        .unwrap()
        .entries
        .len()
}

async fn threads(store: &dyn CheckpointQuery) -> Vec<String> {
    let mut threads: Vec<String> = store
        .list_threads(&ThreadQuery::default())
        .await
        .unwrap()
        .threads
        .into_iter()
        .map(|thread| thread.thread_id)
        .collect();
    threads.sort();
    threads
}

async fn acquire(store: &dyn CheckpointStore, thread_id: &str, owner: &str) -> ThreadLease {
    match store
        .try_acquire_lease(thread_id, owner, LASTING)
        .await
        .unwrap()
    {
        LeaseAttempt::Acquired(lease) => lease,
        LeaseAttempt::Held(holder) => panic!("{thread_id} is held by {}", holder.owner),
    }
}

// --- Classification ---

#[test]
fn each_store_knows_its_transient_failures() {
    for transient in [
        "08006", "08001", "57P01", "57P02", "57P03", "40001", "40P01", "53300",
    ] {
        assert!(is_transient_sqlstate(transient), "{transient}");
    }
    for last in ["23505", "42P01", "22001", "28P01"] {
        assert!(!is_transient_sqlstate(last), "{last}");
    }

    for transient in ["LOADING", "BUSY", "TRYAGAIN"] {
        assert!(is_transient_reply(transient), "{transient}");
    }
    for last in ["WRONGTYPE", "ERR", "NOSCRIPT", "NOPERM"] {
        assert!(!is_transient_reply(last), "{last}");
    }
}

// --- Postgres: a dropped connection ---

/// Terminates every backend named `application_name`; returns how many.
async fn terminate(admin: &tokio_postgres::Client, application_name: &str) -> i64 {
    admin
        .query_one(
            "SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1) t",
            &[&application_name],
        )
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
async fn a_dropped_postgres_connection_does_not_fail_a_run() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "dropped");
    let name = unique_name("drop");
    let admin = Arc::new(sandbox.client().await);
    let store = Arc::new(
        PostgresCheckpointStore::connect(&sandbox.url(Some(&name)))
            .await
            .unwrap(),
    );
    let terminated = Arc::new(AtomicI64::new(0));
    let graph = draft_then_send({
        let (admin, name, terminated) = (admin.clone(), name.clone(), terminated.clone());
        Arc::new(move || {
            let (admin, name, terminated) = (admin.clone(), name.clone(), terminated.clone());
            Box::pin(async move {
                terminated.store(terminate(&admin, &name).await, Ordering::SeqCst);
            })
        })
    });

    let result = graph.run("t", initial_state(), store.clone()).await;

    // The store's connection was terminated between two saves, and the run
    // went on, on a new one.
    assert!(terminated.load(Ordering::SeqCst) > 0);
    let GraphExecutionResult::Completed(state) = result else {
        panic!("the run did not complete");
    };
    assert!(state.is_completed);
    assert_eq!(
        statuses(&*store, "t").await,
        [
            CheckpointStatus::Running,
            CheckpointStatus::Running,
            CheckpointStatus::Completed
        ]
    );
    // (Whether a save was repeated depends on when the drop was noticed: a
    // call that finds the connection already closed just makes a new one.)

    // A load and a listing ride through one too.
    assert!(terminate(&admin, &name).await > 0);
    assert_eq!(
        store.load("t").await.unwrap().map(|c| c.status),
        Some(CheckpointStatus::Completed)
    );
    assert!(terminate(&admin, &name).await > 0);
    assert_eq!(threads(&*store).await, ["t"]);
}

/// Calls `load` until it gives `wanted`, for at most ten seconds: the
/// connection task notices a terminated backend a moment after the server
/// says so.
async fn eventually(
    store: &PostgresCheckpointStore,
    wanted: impl Fn(&Result<bool, String>) -> bool,
) {
    let started = std::time::Instant::now();
    loop {
        let found = store
            .load("t")
            .await
            .map(|checkpoint| checkpoint.is_some())
            .map_err(|refused| refused.0);
        if wanted(&found) {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "still {found:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_store_connects_again_when_its_connection_has_dropped_even_without_retries() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "reconnect");
    let name = unique_name("drop");
    let admin = sandbox.client().await;
    let store = PostgresCheckpointStore::connect_with(
        &sandbox.url(Some(&name)),
        PostgresStoreOptions::new().with_retry(StoreRetry::none()),
    )
    .await
    .unwrap();
    store
        .save(waiting("t", "2026-09-16T00:00:00Z"))
        .await
        .unwrap();

    assert!(terminate(&admin, &name).await > 0);

    // A call the drop interrupts is that call's failure, with no retries to
    // repeat it. A call that finds the connection closed makes a new one.
    eventually(&store, |found| *found == Ok(true)).await;
    assert!(store.load("t").await.unwrap().is_some());
}

#[tokio::test]
async fn without_retries_a_transient_failure_is_the_calls() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "no_retries");
    let store = PostgresCheckpointStore::from_client_with(
        sandbox.client().await,
        PostgresStoreOptions::new()
            .with_retry(StoreRetry::none())
            .with_fault(fault_at("save:before-commit", 1)),
    )
    .await
    .unwrap();
    let retries = RetryCounter::new();

    let refused = store
        .save(waiting("t", "2026-09-16T00:00:00Z"))
        .await
        .unwrap_err();

    assert_eq!(refused.0, "injected fault at save:before-commit");
    assert_eq!(log_length(&store, "t").await, 0);
    assert_eq!(retries.count(), 0);
}

#[tokio::test]
async fn a_store_built_from_a_client_cannot_reconnect() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "from_client");
    let name = unique_name("drop");
    let admin = sandbox.client().await;
    let store = PostgresCheckpointStore::from_client(sandbox.named_client(&name).await)
        .await
        .unwrap();
    store
        .save(waiting("t", "2026-09-16T00:00:00Z"))
        .await
        .unwrap();

    assert!(terminate(&admin, &name).await > 0);

    // It has only the caller's client: every later call fails, and once the
    // drop has been noticed, says why.
    let cannot = "the connection is closed, and a store built from a client can't reconnect";
    eventually(&store, |found| {
        assert!(found.is_err(), "a call on a dropped connection failed");
        found.as_ref().is_err_and(|refused| refused == cannot)
    })
    .await;
    assert_eq!(store.load("t").await.unwrap_err().0, cannot);
}

// --- Postgres: a save that may have happened ---

#[tokio::test]
async fn a_save_whose_commit_may_have_failed_is_not_written_twice() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "ambiguous");
    let store = PostgresCheckpointStore::from_client_with(
        sandbox.client().await,
        PostgresStoreOptions::new().with_fault(fault_at("save:after-commit", 3)),
    )
    .await
    .unwrap();
    let first = waiting("t", "2026-09-16T00:00:00Z");
    let second = waiting("t", "2026-09-17T00:00:00Z");

    // The commit happened and the answer was lost: the store looks, finds the
    // checkpoint, and says so.
    store.save(first).await.unwrap();
    assert_eq!(log_length(&store, "t").await, 1);

    // A conditional save keeps its answer.
    assert!(store.save_if_latest(second, Some(0)).await.unwrap());
    assert_eq!(log_length(&store, "t").await, 2);

    // So does one under a lease, and the lease is still its owner's.
    let lease = acquire(&store, "t", "worker-1").await;
    let third = waiting("t", "2026-09-18T00:00:00Z");
    assert_eq!(
        store
            .save_under_lease(third.clone(), &lease, Some(1))
            .await
            .unwrap(),
        LeasedSave::Saved
    );
    assert_eq!(log_length(&store, "t").await, 3);
    assert_eq!(
        store.get_lease("t").await.unwrap().map(|held| held.token),
        Some(lease.token)
    );
    assert_eq!(store.load("t").await.unwrap(), Some(third));
}

#[tokio::test]
async fn a_postgres_save_in_doubt_is_made_again_if_it_was_not_written_and_lost_if_another_took_its_place()
 {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = Arc::new(PgSandbox::new(&url, "in_doubt"));

    // The commit was sent and never happened. The store looks, finds nothing,
    // and saves again: once.
    let store = PostgresCheckpointStore::from_client_with(
        sandbox.client().await,
        PostgresStoreOptions::new().with_fault(fault_at("save:commit", 1)),
    )
    .await
    .unwrap();
    let retries = RetryCounter::new();
    store
        .save(waiting("t", "2026-09-16T00:00:00Z"))
        .await
        .unwrap();
    assert_eq!(log_length(&store, "t").await, 1);
    assert_eq!(retries.count(), 1);

    // The same for a conditional save, except that before the store looks,
    // another writer takes its place: it lost the race, and says so.
    let commit = fault_at("save:commit", 1);
    let store = PostgresCheckpointStore::from_client_with(
        sandbox.client().await,
        PostgresStoreOptions::new().with_fault({
            let sandbox = sandbox.clone();
            move |point| {
                if point == "save:recheck" {
                    sandbox.admin(|client| {
                        client
                            .execute(
                                "INSERT INTO checkpoints
                                    (thread_id, sequence, schema_version, status, suspended_node, next_node,
                                     frozen_state, metadata, created_at)
                                 SELECT thread_id, 1, schema_version, status, suspended_node, next_node,
                                        frozen_state, metadata, '2026-09-18T00:00:00Z'
                                 FROM checkpoints WHERE thread_id = 't' AND sequence = 0",
                                &[],
                            )
                            .unwrap();
                    });
                }
                commit(point)
            }
        }),
    )
    .await
    .unwrap();

    let mine = waiting("t", "2026-09-17T00:00:00Z");
    assert!(!store.save_if_latest(mine, Some(0)).await.unwrap());

    let log = store.get_log("t", None, 100).await.unwrap().entries;
    assert_eq!(
        log.iter()
            .map(|entry| entry.checkpoint.created_at.as_str())
            .collect::<Vec<_>>(),
        ["2026-09-16T00:00:00Z", "2026-09-18T00:00:00Z"]
    );
}

#[tokio::test]
async fn a_save_that_failed_before_its_commit_is_repeated_and_writes_once() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "before_commit");
    let store = PostgresCheckpointStore::from_client_with(
        sandbox.client().await,
        PostgresStoreOptions::new().with_fault(fault_at("save:before-commit", 2)),
    )
    .await
    .unwrap();
    let retries = RetryCounter::new();

    store
        .save(waiting("t", "2026-09-16T00:00:00Z"))
        .await
        .unwrap();

    assert_eq!(log_length(&store, "t").await, 1);
    assert_eq!(retries.count(), 2);
    assert_eq!(
        retries.last(),
        Some(("postgres".to_string(), "save".to_string()))
    );
}

#[tokio::test]
async fn an_acquisition_or_an_append_that_may_have_happened_is_not_repeated() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "not_repeated");
    let store = PostgresCheckpointStore::from_client_with(
        sandbox.client().await,
        PostgresStoreOptions::new().with_fault(fault_at("lease:after-commit", 1)),
    )
    .await
    .unwrap();
    let history = PostgresHistoryStore::from_client_with(
        sandbox.client().await,
        PostgresStoreOptions::new().with_fault(fault_at("append:after-commit", 1)),
    )
    .await
    .unwrap();

    // The store can't tell its own lease from another's with the same name:
    // the failure is the call's, and the lease that was taken runs out by
    // itself.
    assert!(
        store
            .try_acquire_lease("t", "worker-1", LASTING)
            .await
            .is_err()
    );
    let held = store.get_lease("t").await.unwrap().unwrap();
    assert_eq!((held.owner.as_str(), held.token), ("worker-1", 1));

    // A message can't be told from another like it either.
    let message = ChatMessage {
        role: MessageRole::User,
        content: Arc::from("hello"),
        timestamp: Utc.with_ymd_and_hms(2026, 9, 16, 0, 0, 0).unwrap(),
    };
    assert!(history.append("s", message).await.is_err());
    assert_eq!(history.get_history("s", 10).await.unwrap().len(), 1);
}

// --- Migrations ---

fn postgres_versions(sandbox: &PgSandbox) -> Value {
    sandbox.admin(|client| {
        let rows = client
            .query(
                "SELECT component, version FROM strata_schema_version ORDER BY component",
                &[],
            )
            .unwrap();
        Value::Object(
            rows.iter()
                .map(|row| (row.get::<_, String>(0), json!(row.get::<_, i64>(1))))
                .collect(),
        )
    })
}

/// `information_schema` view of a table in the sandbox's schema, in the shape
/// of `checkpoint-log/postgres-columns.json`.
fn postgres_columns(sandbox: &PgSandbox, table: &str) -> Value {
    sandbox.admin(|client| {
        let rows = client
            .query(
                "SELECT c.column_name::text, c.data_type::text, c.is_nullable::text = 'NO',
                        COALESCE(k.ordinal_position, 0)::int
                 FROM information_schema.columns c
                 LEFT JOIN (
                     SELECT kcu.column_name, kcu.ordinal_position
                     FROM information_schema.table_constraints tc
                     JOIN information_schema.key_column_usage kcu
                       ON kcu.constraint_name = tc.constraint_name
                      AND kcu.table_schema = tc.table_schema
                      AND kcu.table_name = tc.table_name
                     WHERE tc.constraint_type = 'PRIMARY KEY'
                       AND tc.table_schema = current_schema() AND tc.table_name = $1
                 ) k ON k.column_name = c.column_name
                 WHERE c.table_schema = current_schema() AND c.table_name = $1
                 ORDER BY c.ordinal_position",
                &[&table],
            )
            .unwrap();
        Value::Array(
            rows.iter()
                .map(|row| {
                    json!({
                        "name": row.get::<_, String>(0),
                        "type": row.get::<_, String>(1),
                        "notnull": row.get::<_, bool>(2),
                        "pk": row.get::<_, i32>(3),
                    })
                })
                .collect(),
        )
    })
}

#[tokio::test]
async fn a_fresh_postgres_database_is_at_the_shared_versions() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "versions");
    drop(
        PostgresCheckpointStore::from_client(sandbox.client().await)
            .await
            .unwrap(),
    );
    drop(
        PostgresHistoryStore::from_client(sandbox.client().await)
            .await
            .unwrap(),
    );

    let shared = fixture("checkpoint-log/schema-versions.json");
    assert_eq!(shared["table"], schema_versions::TABLE);
    assert_eq!(postgres_versions(&sandbox), shared["components"]);
    assert_eq!(
        shared["components"],
        json!({
            schema_versions::CHECKPOINTS: schema_versions::CHECKPOINTS_VERSION,
            schema_versions::HISTORY: schema_versions::HISTORY_VERSION,
        })
    );
    assert_eq!(
        postgres_columns(&sandbox, schema_versions::TABLE),
        fixture("checkpoint-log/postgres-schema-versions-columns.json")
    );
}

#[tokio::test]
async fn a_postgres_database_from_before_the_version_table_is_brought_to_the_current_version() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "baseline");
    let old = PostgresCheckpointStore::from_client(sandbox.client().await)
        .await
        .unwrap();
    old.save(waiting("t-old", "2026-09-17T00:00:00Z"))
        .await
        .unwrap();
    drop(old);

    // As a runtime from before leases and the version table left it.
    sandbox.admin(|client| {
        client
            .batch_execute("DROP TABLE checkpoint_leases, strata_schema_version")
            .unwrap()
    });

    let store = PostgresCheckpointStore::from_client(sandbox.client().await)
        .await
        .unwrap();

    assert_eq!(
        postgres_versions(&sandbox)["checkpoints"],
        schema_versions::CHECKPOINTS_VERSION
    );
    assert!(store.load("t-old").await.unwrap().is_some());
    assert_eq!(acquire(&store, "t-old", "worker-1").await.token, 1);
}

#[tokio::test]
async fn a_newer_postgres_database_is_refused_and_left_alone() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "newer");
    let current = PostgresCheckpointStore::from_client(sandbox.client().await)
        .await
        .unwrap();
    sandbox.admin(|client| {
        client
            .batch_execute(
                "UPDATE strata_schema_version SET version = 3 WHERE component = 'checkpoints'",
            )
            .unwrap()
    });

    let refused = PostgresCheckpointStore::from_client(sandbox.client().await)
        .await
        .err()
        .expect("a newer database is refused");

    assert_eq!(
        refused.0,
        "database is at checkpoints schema version 3, and this build knows 2: upgrade the runtime"
    );
    // Asking for a migration doesn't take it back either.
    assert_eq!(current.migrate().await.unwrap_err(), refused);
    assert_eq!(postgres_versions(&sandbox)["checkpoints"], 3);
}

#[tokio::test]
async fn a_store_that_does_not_migrate_refuses_a_database_that_is_behind() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "manual");
    let manual = || PostgresStoreOptions::new().with_auto_migrate(false);

    let refused = PostgresCheckpointStore::from_client_with(sandbox.client().await, manual())
        .await
        .err()
        .expect("a database that is behind is refused");
    assert_eq!(
        refused.0,
        "database is at checkpoints schema version 0, and this build needs 2: run its migration"
    );

    // It changed nothing: not even the version table is there.
    let untouched: bool = sandbox.admin(|client| {
        client
            .query_one(
                "SELECT to_regclass('strata_schema_version') IS NULL AND to_regclass('checkpoints') IS NULL",
                &[],
            )
            .unwrap()
            .get(0)
    });
    assert!(untouched);

    // A deploy step migrates, and the store then opens and works.
    PostgresCheckpointStore::from_client(sandbox.client().await)
        .await
        .unwrap()
        .migrate()
        .await
        .unwrap();
    let store = PostgresCheckpointStore::from_client_with(sandbox.client().await, manual())
        .await
        .unwrap();
    assert!(store.load("t").await.unwrap().is_none());
}

/// Two steps after this build's own, as a later build would add them.
fn later_steps(integer: &str) -> Vec<SchemaStep> {
    vec![
        SchemaStep::new(
            3,
            "probe",
            [
                format!("CREATE TABLE strata_probe (step {integer} NOT NULL)"),
                "INSERT INTO strata_probe VALUES (3)".to_string(),
            ],
        ),
        SchemaStep::new(4, "probe again", ["INSERT INTO strata_probe VALUES (4)"]),
    ]
}

#[tokio::test]
async fn later_steps_run_once_in_order_in_postgres() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "steps");
    let old = PostgresCheckpointStore::from_client(sandbox.client().await)
        .await
        .unwrap();
    old.save(waiting("t", "2026-09-17T00:00:00Z"))
        .await
        .unwrap();
    drop(old);

    // A later build knows two more steps; opening twice runs each once.
    for _ in 0..2 {
        let store = PostgresCheckpointStore::from_client_with(
            sandbox.client().await,
            PostgresStoreOptions::new().with_later_steps(later_steps("BIGINT")),
        )
        .await
        .unwrap();
        assert!(store.load("t").await.unwrap().is_some());
    }

    assert_eq!(postgres_versions(&sandbox)["checkpoints"], 4);
    let ran: String = sandbox.admin(|client| {
        client
            .query_one(
                "SELECT string_agg(step::text, ',' ORDER BY step) FROM strata_probe",
                &[],
            )
            .unwrap()
            .get(0)
    });
    assert_eq!(ran, "3,4");

    // And this build, which knows neither, refuses what the later one left.
    assert!(
        PostgresCheckpointStore::from_client(sandbox.client().await)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn a_sqlite_file_has_the_version_table_and_runs_later_steps_once() {
    let path = std::env::temp_dir().join(format!("{}.sqlite", unique_name("versions")));
    let scalar = |sql: &str| -> Value {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.query_row(sql, [], |row| row.get::<_, rusqlite::types::Value>(0))
            .map(|value| match value {
                rusqlite::types::Value::Integer(i) => json!(i),
                rusqlite::types::Value::Text(text) => json!(text),
                other => panic!("unexpected {other:?}"),
            })
            .unwrap()
    };

    let store = SqliteCheckpointStore::open(&path).unwrap();
    store
        .save(waiting("t", "2026-09-17T00:00:00Z"))
        .await
        .unwrap();
    drop(store);

    assert_eq!(
        scalar("SELECT version FROM strata_schema_version WHERE component = 'checkpoints'"),
        schema_versions::CHECKPOINTS_VERSION
    );
    let columns: Vec<Value> = {
        let conn = rusqlite::Connection::open(&path).unwrap();
        let mut info = conn
            .prepare("PRAGMA table_info(strata_schema_version)")
            .unwrap();
        info.query_map([], |row| {
            Ok(json!({
                "name": row.get::<_, String>(1)?,
                "type": row.get::<_, String>(2)?,
                "notnull": row.get::<_, i64>(3)? != 0,
                "pk": row.get::<_, i64>(5)?,
            }))
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
    };
    assert_eq!(
        Value::Array(columns),
        fixture("checkpoint-log/schema-versions-columns.json")
    );

    for _ in 0..2 {
        let store = SqliteCheckpointStore::open_with_steps(&path, &later_steps("INTEGER")).unwrap();
        assert!(store.load("t").await.unwrap().is_some());
    }
    assert_eq!(
        scalar("SELECT version FROM strata_schema_version WHERE component = 'checkpoints'"),
        4
    );
    assert_eq!(
        scalar("SELECT group_concat(step, ',') FROM (SELECT step FROM strata_probe ORDER BY step)"),
        "3,4"
    );

    // This build knows neither step, and refuses the file.
    let refused = SqliteCheckpointStore::open(&path)
        .err()
        .expect("a newer file is refused");
    assert_eq!(
        refused.0,
        "database is at checkpoints schema version 4, and this build knows 2: upgrade the runtime"
    );
    let _ = std::fs::remove_file(&path);
}

// --- Retention ---

/// Retention against `store` (`design/34-Storage-Hardening.md`, "Retention"):
/// a purge takes whole threads by their latest checkpoint's state and age,
/// oldest first, up to its limit, and leaves newer ones, other states, and a
/// thread a live lease holds. The xUnit twin is
/// `StoreContracts.PurgeContractAsync`.
async fn run_purge_contract<T>(store: &T)
where
    T: CheckpointStore + CheckpointQuery + CheckpointRetention,
{
    use CheckpointStatus::{Completed, Ended, Failed, Interrupted};
    let cutoff = Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap();
    let finished = PurgeStates::finished();
    for (thread_id, status, created_at) in [
        ("old-completed", Completed, "2026-01-01T00:00:00Z"),
        ("old-ended", Interrupted, "2026-01-01T00:00:00Z"),
        ("old-ended", Ended, "2026-01-02T00:00:00Z"),
        ("new-completed", Completed, "2026-06-01T00:00:00Z"),
        ("at-the-cutoff", Completed, "2026-03-01T00:00:00Z"),
        ("old-failed", Failed, "2026-01-01T00:00:00Z"),
        ("old-waiting", Interrupted, "2026-01-01T00:00:00Z"),
        ("held-completed", Completed, "2026-01-01T00:00:00Z"),
    ] {
        store
            .save(checkpoint(thread_id, status, created_at))
            .await
            .unwrap();
    }
    let LeaseAttempt::Acquired(held) = store
        .try_acquire_lease("held-completed", "worker-1", Duration::from_secs(3600))
        .await
        .unwrap()
    else {
        panic!("nobody holds the thread yet");
    };

    // Finished threads older than the cutoff go, whole. A newer one, one
    // written at the cutoff, a failed one, a waiting one and one a live lease
    // holds stay.
    assert_eq!(
        store.purge(cutoff, &finished, MAX_PAGE_SIZE).await.unwrap(),
        2
    );
    assert!(store.load("old-completed").await.unwrap().is_none());
    assert!(store.load("old-ended").await.unwrap().is_none());
    assert_eq!(log_length(store, "old-ended").await, 0);
    assert_eq!(
        threads(store).await,
        [
            "at-the-cutoff",
            "held-completed",
            "new-completed",
            "old-failed",
            "old-waiting"
        ]
    );
    assert_eq!(
        store.purge(cutoff, &finished, MAX_PAGE_SIZE).await.unwrap(),
        0
    );

    // Named states are honoured, and only they.
    assert_eq!(
        store
            .purge(
                cutoff,
                &PurgeStates::of([ThreadState::Failed]),
                MAX_PAGE_SIZE
            )
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        threads(store).await,
        [
            "at-the-cutoff",
            "held-completed",
            "new-completed",
            "old-waiting"
        ]
    );
    assert_eq!(
        store
            .purge(cutoff, &PurgeStates::of([]), MAX_PAGE_SIZE)
            .await
            .unwrap(),
        0
    );

    // The limit takes the oldest first.
    for (thread_id, created_at) in [
        ("c1", "2026-01-03T00:00:00Z"),
        ("c2", "2026-01-04T00:00:00Z"),
        ("c3", "2026-01-05T00:00:00Z"),
    ] {
        store
            .save(checkpoint(thread_id, Completed, created_at))
            .await
            .unwrap();
    }
    assert_eq!(store.purge(cutoff, &finished, 2).await.unwrap(), 2);
    assert_eq!(
        threads(store).await,
        [
            "at-the-cutoff",
            "c3",
            "held-completed",
            "new-completed",
            "old-waiting"
        ]
    );
    assert_eq!(store.purge(cutoff, &finished, 2).await.unwrap(), 1);
    assert!(store.purge(cutoff, &finished, 0).await.is_err());
    assert!(
        store
            .purge(cutoff, &finished, MAX_PAGE_SIZE + 1)
            .await
            .is_err()
    );

    // Once its lease is let go, the held thread goes too, with its lease
    // record: the id starts over.
    assert!(store.release_lease(&held).await.unwrap());
    assert_eq!(
        store.purge(cutoff, &finished, MAX_PAGE_SIZE).await.unwrap(),
        1
    );
    assert_eq!(
        threads(store).await,
        ["at-the-cutoff", "new-completed", "old-waiting"]
    );
    assert_eq!(acquire(store, "held-completed", "worker-2").await.token, 1);

    // A purged id can be used again, from sequence 0.
    store
        .save(waiting("old-completed", "2026-07-01T00:00:00Z"))
        .await
        .unwrap();
    assert_eq!(
        store
            .load_latest("old-completed")
            .await
            .unwrap()
            .and_then(|latest| latest.sequence),
        Some(0)
    );
}

#[tokio::test]
async fn the_in_memory_store_purges_finished_threads() {
    run_purge_contract(&InMemoryCheckpointStore::new()).await;
}

#[tokio::test]
async fn the_sqlite_store_purges_finished_threads() {
    run_purge_contract(&SqliteCheckpointStore::open_in_memory().unwrap()).await;
}

#[tokio::test]
async fn the_postgres_store_purges_finished_threads() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "purge");
    let store = PostgresCheckpointStore::from_client(sandbox.client().await)
        .await
        .unwrap();
    run_purge_contract(&store).await;

    // Nothing of a purged thread is left: no row, no head, no lease.
    let left: i64 = sandbox.admin(|client| {
        client
            .query_one(
                "SELECT (SELECT count(*) FROM checkpoints WHERE thread_id = 'old-ended')
                      + (SELECT count(*) FROM checkpoint_threads WHERE thread_id = 'old-ended')
                      + (SELECT count(*) FROM checkpoint_leases WHERE thread_id = 'c1')",
                &[],
            )
            .unwrap()
            .get(0)
    });
    assert_eq!(left, 0);
}

#[tokio::test]
async fn the_redis_store_purges_finished_threads() {
    let Some(url) = backend_url(common::REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "purge");
    let store = RedisCheckpointStore::connect_with_prefix(&url, &sandbox.prefix)
        .await
        .unwrap();
    run_purge_contract(&store).await;

    // Nothing of the purged threads is left under the prefix: no list, no
    // token counter, no index entry.
    let mut conn = sandbox.connection();
    let key = |suffix: &str| format!("{}:{suffix}", sandbox.prefix);
    let score = |conn: &mut redis::Connection, set: &str| -> Option<f64> {
        redis::cmd("ZSCORE")
            .arg(key(set))
            .arg("old-ended")
            .query(conn)
            .unwrap()
    };
    assert_eq!(score(&mut conn, "threads"), None);
    assert_eq!(score(&mut conn, "threads:ended"), None);
    let exists: bool = redis::cmd("EXISTS")
        .arg(key("checkpoints:old-ended"))
        .query(&mut conn)
        .unwrap();
    assert!(!exists);
}

// --- Redis ---

/// A key's remaining time to live in milliseconds: -1 for none, -2 for no key.
fn pttl(conn: &mut redis::Connection, key: &str) -> i64 {
    redis::cmd("PTTL").arg(key).query(conn).unwrap()
}

#[tokio::test]
async fn a_finished_redis_thread_expires_and_one_that_runs_again_does_not() {
    let Some(url) = backend_url(common::REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "ttl");
    let hour = Duration::from_secs(3600);
    let store = RedisCheckpointStore::connect_with(
        &url,
        RedisStoreOptions::new()
            .with_prefix(&sandbox.prefix)
            .with_finished_thread_ttl(hour),
    )
    .await
    .unwrap();
    let mut conn = sandbox.connection();
    let list = format!("{}:checkpoints:t", sandbox.prefix);
    let tokens = format!("{}:lease-tokens:t", sandbox.prefix);

    store
        .save(waiting("t", "2026-09-16T00:00:00Z"))
        .await
        .unwrap();
    assert_eq!(pttl(&mut conn, &list), -1);

    // Completed: it has an hour.
    let completed = checkpoint("t", CheckpointStatus::Completed, "2026-09-16T00:01:00Z");
    assert!(store.save_if_latest(completed, Some(0)).await.unwrap());
    let left = pttl(&mut conn, &list);
    assert!((59 * 60_000..=60 * 60_000).contains(&left), "{left}");

    // The id runs again: it doesn't expire under the run.
    store
        .save(waiting("t", "2026-09-16T00:02:00Z"))
        .await
        .unwrap();
    assert_eq!(pttl(&mut conn, &list), -1);

    // Ended, under a lease: the token counter goes with the list.
    let lease = acquire(&store, "t", "worker-1").await;
    let ended = checkpoint("t", CheckpointStatus::Ended, "2026-09-16T00:03:00Z");
    assert_eq!(
        store.save_under_lease(ended, &lease, None).await.unwrap(),
        LeasedSave::Saved
    );
    assert!(pttl(&mut conn, &list) > 0);
    assert!(pttl(&mut conn, &tokens) > 0);

    // A store without the option never sets one.
    let plain = RedisCheckpointStore::connect_with_prefix(&url, &sandbox.prefix)
        .await
        .unwrap();
    plain
        .save(checkpoint(
            "u",
            CheckpointStatus::Completed,
            "2026-09-16T00:00:00Z",
        ))
        .await
        .unwrap();
    assert_eq!(
        pttl(&mut conn, &format!("{}:checkpoints:u", sandbox.prefix)),
        -1
    );

    // A ttl below a millisecond is refused.
    assert!(
        RedisCheckpointStore::connect_with(
            &url,
            RedisStoreOptions::new()
                .with_prefix(&sandbox.prefix)
                .with_finished_thread_ttl(Duration::from_micros(10)),
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn a_listing_or_a_rebuild_drops_a_thread_whose_list_is_gone() {
    let Some(url) = backend_url(common::REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "expired");
    let store = RedisCheckpointStore::connect_with_prefix(&url, &sandbox.prefix)
        .await
        .unwrap();
    let mut conn = sandbox.connection();
    let key = |suffix: &str| format!("{}:{suffix}", sandbox.prefix);
    let score = |conn: &mut redis::Connection, set: &str, thread_id: &str| -> Option<f64> {
        redis::cmd("ZSCORE")
            .arg(key(set))
            .arg(thread_id)
            .query(conn)
            .unwrap()
    };
    for gone in ["gone", "gone-too"] {
        store
            .save(checkpoint(
                gone,
                CheckpointStatus::Completed,
                "2026-09-16T00:00:00Z",
            ))
            .await
            .unwrap();
    }
    store
        .save(waiting("here", "2026-09-17T00:00:00Z"))
        .await
        .unwrap();

    // As an expiry leaves it: the list is gone, its index entries are not.
    let _: i64 = redis::cmd("DEL")
        .arg(key("checkpoints:gone"))
        .query(&mut conn)
        .unwrap();
    assert!(score(&mut conn, "threads:completed", "gone").is_some());

    let listed = store
        .list_threads(&ThreadQuery::default().with_states([ThreadState::Completed]))
        .await
        .unwrap();
    assert_eq!(
        listed
            .threads
            .iter()
            .map(|t| t.thread_id.as_str())
            .collect::<Vec<_>>(),
        ["gone-too"]
    );
    assert_eq!(score(&mut conn, "threads", "gone"), None);
    assert_eq!(score(&mut conn, "threads:completed", "gone"), None);

    // A rebuild finds the ones no listing has come across.
    let _: i64 = redis::cmd("DEL")
        .arg(key("checkpoints:gone-too"))
        .query(&mut conn)
        .unwrap();
    assert_eq!(store.rebuild_index().await.unwrap(), 1);
    assert_eq!(score(&mut conn, "threads", "gone-too"), None);
    assert_eq!(score(&mut conn, "threads:completed", "gone-too"), None);
    assert_eq!(threads(&store).await, ["here"]);
}

#[tokio::test]
async fn a_redis_save_that_may_have_run_is_not_written_twice() {
    let Some(url) = backend_url(common::REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "ambiguous");
    let store = RedisCheckpointStore::connect_with(
        &url,
        RedisStoreOptions::new()
            .with_prefix(&sandbox.prefix)
            .with_fault(fault_at("save:after", 3)),
    )
    .await
    .unwrap();
    let retries = RetryCounter::new();

    store
        .save(waiting("t", "2026-09-16T00:00:00Z"))
        .await
        .unwrap();
    assert!(
        store
            .save_if_latest(waiting("t", "2026-09-17T00:00:00Z"), Some(0))
            .await
            .unwrap()
    );
    let lease = acquire(&store, "t", "worker-1").await;
    assert_eq!(
        store
            .save_under_lease(waiting("t", "2026-09-18T00:00:00Z"), &lease, Some(1))
            .await
            .unwrap(),
        LeasedSave::Saved
    );

    assert_eq!(log_length(&store, "t").await, 3);
    assert_eq!(retries.count(), 3);
    assert_eq!(
        retries.last(),
        Some(("redis".to_string(), "save".to_string()))
    );

    // A conditional save that really lost its race still says so.
    assert!(
        !store
            .save_if_latest(waiting("t", "2026-09-19T00:00:00Z"), Some(0))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn a_redis_save_in_doubt_is_made_again_if_it_did_not_run_and_lost_if_another_took_its_place()
{
    let Some(url) = backend_url(common::REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "in_doubt");

    // The script was sent and never ran. The store looks, finds nothing, and
    // saves again: once.
    let store = RedisCheckpointStore::connect_with(
        &url,
        RedisStoreOptions::new()
            .with_prefix(&sandbox.prefix)
            .with_fault(fault_at("save:before", 1)),
    )
    .await
    .unwrap();
    let retries = RetryCounter::new();
    let first = waiting("t", "2026-09-16T00:00:00Z");
    store.save(first.clone()).await.unwrap();
    assert_eq!(log_length(&store, "t").await, 1);
    assert_eq!(retries.count(), 1);

    // The same for a conditional save, except that before the store looks,
    // another writer takes its place: it lost the race, and says so.
    let before = fault_at("save:before", 1);
    let theirs = serde_json::to_string(&waiting("t", "2026-09-18T00:00:00Z")).unwrap();
    let store = RedisCheckpointStore::connect_with(
        &url,
        RedisStoreOptions::new()
            .with_prefix(&sandbox.prefix)
            .with_fault({
                let (url, list) = (url.clone(), format!("{}:checkpoints:t", sandbox.prefix));
                move |point| {
                    if point == "save:recheck" {
                        // Off the runtime's thread: the sync client blocks.
                        std::thread::scope(|scope| {
                            scope.spawn(|| {
                                let mut conn = redis::Client::open(url.as_str())
                                    .unwrap()
                                    .get_connection()
                                    .unwrap();
                                let _: i64 = redis::cmd("RPUSH")
                                    .arg(&list)
                                    .arg(&theirs)
                                    .query(&mut conn)
                                    .unwrap();
                            });
                        });
                    }
                    before(point)
                }
            }),
    )
    .await
    .unwrap();

    let mine = waiting("t", "2026-09-17T00:00:00Z");
    assert!(!store.save_if_latest(mine, Some(0)).await.unwrap());

    let log = store.get_log("t", None, 100).await.unwrap().entries;
    assert_eq!(
        log.iter()
            .map(|entry| entry.checkpoint.created_at.as_str())
            .collect::<Vec<_>>(),
        ["2026-09-16T00:00:00Z", "2026-09-18T00:00:00Z"]
    );
}

/// `url` with its database number replaced by `db`.
fn redis_database(url: &str, db: u8) -> String {
    let (scheme, rest) = url.split_once("://").expect("a redis:// URL");
    let authority = rest.split('/').next().unwrap_or(rest);
    format!("{scheme}://{authority}/{db}")
}

#[tokio::test]
async fn a_dropped_redis_connection_does_not_fail_a_run() {
    let Some(url) = backend_url(common::REDIS_VAR) else {
        return;
    };
    // A database of this test's own, so the store's connection can be told
    // from every other test's and killed.
    const DB: u8 = 13;
    let store_url = redis_database(&url, DB);
    let sandbox = RedisSandbox::new(&store_url, "dropped");
    let store = Arc::new(
        RedisCheckpointStore::connect_with(
            &store_url,
            RedisStoreOptions::new()
                .with_prefix(&sandbox.prefix)
                // Long enough for the client to reconnect on a slow machine.
                .with_retry(
                    StoreRetry::new()
                        .with_max_attempts(10)
                        .with_initial_delay(Duration::from_millis(100)),
                ),
        )
        .await
        .unwrap(),
    );
    let killed = Arc::new(AtomicI64::new(0));
    let graph = draft_then_send({
        let (url, killed) = (url.clone(), killed.clone());
        Arc::new(move || {
            let (url, killed) = (url.clone(), killed.clone());
            Box::pin(async move {
                let mut killer = redis_connection(&url).await;
                let clients: String = redis::cmd("CLIENT")
                    .arg("LIST")
                    .query_async(&mut killer)
                    .await
                    .unwrap();
                for client in clients
                    .lines()
                    .filter(|line| line.contains(&format!(" db={DB} ")))
                {
                    let id = client
                        .split(' ')
                        .find_map(|field| field.strip_prefix("id="))
                        .unwrap();
                    let dropped: i64 = redis::cmd("CLIENT")
                        .arg("KILL")
                        .arg("ID")
                        .arg(id)
                        .query_async(&mut killer)
                        .await
                        .unwrap_or(0);
                    killed.fetch_add(dropped, Ordering::SeqCst);
                }
            })
        })
    });

    let result = graph.run("t", initial_state(), store.clone()).await;

    assert!(
        killed.load(Ordering::SeqCst) > 0,
        "the store's connection was killed"
    );
    let GraphExecutionResult::Completed(state) = result else {
        panic!("the run did not complete");
    };
    assert!(state.is_completed);
    assert_eq!(
        statuses(&*store, "t").await,
        [
            CheckpointStatus::Running,
            CheckpointStatus::Running,
            CheckpointStatus::Completed
        ]
    );
}
