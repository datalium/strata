//! Thread leases in every store (`design/32-Thread-Leases.md`): the rules
//! each adapter implements (acquire, renew, release, a held thread refused, an
//! expired lease taken over with a higher token, the stale owner's save
//! fenced), a leased run end to end, and where the leases are kept, checked
//! against the fixtures `Datalium.Strata.Storage`'s tests read, so a lease
//! one runtime takes is honoured by the other. The xUnit twins are
//! `StoreContracts.LeaseContractAsync` and `CheckpointLeaseTableTests`.
//!
//! The in-memory and SQLite stores expire leases on the clock they are given,
//! so their contract runs without waiting; Postgres and Redis expire them on
//! the server's, where it really waits. The Postgres and Redis tests need
//! `STRATA_TEST_POSTGRES_URL` / `STRATA_TEST_REDIS_URL` (see `common`).

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use common::{PgSandbox, RedisSandbox, backend_url, fixture};
use datalium_strata::StrataError;
use datalium_strata::checkpoint::{
    CheckpointStore, GraphExecutionResult, HumanSignal, LeaseAttempt, LeaseOptions, ThreadLease,
};
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{CompiledGraph, StateGraph};
use datalium_strata::schema::{Checkpoint, CheckpointStatus, SCHEMA_VERSION};
use datalium_strata::store::LeasedSave;
use datalium_strata_memory::Clock;
use datalium_strata_memory::checkpoint::{
    CHECKPOINT_LOG_DDL, CHECKPOINT_THREADS_DDL, InMemoryCheckpointStore, SqliteCheckpointStore,
};
use datalium_strata_memory::postgres_store::PostgresCheckpointStore;
use datalium_strata_memory::redis_store::RedisCheckpointStore;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const LASTING: Duration = Duration::from_secs(300);

/// A clock a test moves by hand.
#[derive(Clone)]
struct TestClock(Arc<Mutex<DateTime<Utc>>>);

impl TestClock {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Utc::now())))
    }

    fn clock(&self) -> Clock {
        let now = self.0.clone();
        Arc::new(move || *now.lock().unwrap())
    }

    fn advance(&self, by: Duration) {
        *self.0.lock().unwrap() += chrono::Duration::from_std(by).unwrap();
    }
}

/// How a contract lets a ttl go by on the store's clock.
enum Time {
    Fake(TestClock),
    Real,
}

impl Time {
    /// A ttl short enough to wait out. On a server's clock it also has to
    /// outlast the few calls made while it is held, on a slow machine too.
    fn brief(&self) -> Duration {
        match self {
            Time::Fake(_) => Duration::from_millis(400),
            Time::Real => Duration::from_millis(1500),
        }
    }

    async fn pass(&self, ttl: Duration) {
        match self {
            Time::Fake(clock) => clock.advance(ttl + Duration::from_millis(1)),
            Time::Real => tokio::time::sleep(ttl + Duration::from_millis(300)).await,
        }
    }
}

fn checkpoint(thread_id: &str, status: CheckpointStatus, marker: &str) -> Checkpoint {
    Checkpoint {
        schema_version: SCHEMA_VERSION.to_string(),
        thread_id: thread_id.to_string(),
        status,
        suspended_node: Some("PrepareWireTransfer".to_string()),
        next_node: Some("ExecuteWireTransfer".to_string()),
        frozen_state: json!({ "marker": marker }),
        metadata: None,
        created_at: "2026-09-16T00:00:00Z".to_string(),
        resumed_at: None,
        nested: None,
        decision: None,
        wait: None,
        graph: None,
    }
}

async fn acquire(
    store: &dyn CheckpointStore,
    thread_id: &str,
    owner: &str,
    ttl: Duration,
) -> ThreadLease {
    match store
        .try_acquire_lease(thread_id, owner, ttl)
        .await
        .unwrap()
    {
        LeaseAttempt::Acquired(lease) => lease,
        LeaseAttempt::Held(holder) => panic!("{thread_id} is held by {}", holder.owner),
    }
}

async fn held(store: &dyn CheckpointStore, thread_id: &str) -> Option<(String, i64)> {
    store
        .get_lease(thread_id)
        .await
        .unwrap()
        .map(|lease| (lease.owner, lease.token))
}

/// The store's rules (`design/32-Thread-Leases.md`, "The store's rules").
async fn run_lease_contract(store: &dyn CheckpointStore, time: &Time) {
    const THREAD: &str = "leased";
    let brief = time.brief();
    let sequence = || async {
        store
            .load_latest(THREAD)
            .await
            .unwrap()
            .and_then(|latest| latest.sequence)
    };

    // Nobody holds a thread that never had a lease, and its first lease has token 1.
    assert_eq!(store.get_lease(THREAD).await.unwrap(), None);
    let alice = acquire(store, THREAD, "alice", LASTING).await;
    assert_eq!(
        (alice.thread_id.as_str(), alice.owner.as_str(), alice.token),
        (THREAD, "alice", 1)
    );
    assert_eq!(held(store, THREAD).await, Some(("alice".to_string(), 1)));

    // A held thread is refused to anyone, the same owner name included, and
    // nothing changes.
    for owner in ["bob", "alice"] {
        let refused = store
            .try_acquire_lease(THREAD, owner, LASTING)
            .await
            .unwrap();
        let holder = refused.held_by().expect("the thread is held");
        assert_eq!((holder.owner.as_str(), holder.token), ("alice", 1));
        assert_eq!(refused.acquired(), None);
    }

    // Saves under the lease: unconditional, and conditional on the sequence
    // like `save_if_latest`.
    let one = checkpoint(THREAD, CheckpointStatus::Interrupted, "one");
    let two = checkpoint(THREAD, CheckpointStatus::Interrupted, "two");
    assert_eq!(
        store.save_under_lease(one, &alice, None).await.unwrap(),
        LeasedSave::Saved
    );
    assert_eq!(
        store
            .save_under_lease(two.clone(), &alice, Some(5))
            .await
            .unwrap(),
        LeasedSave::NotLatest
    );
    assert_eq!(sequence().await, Some(0));
    assert_eq!(
        store.save_under_lease(two, &alice, Some(0)).await.unwrap(),
        LeasedSave::Saved
    );
    assert_eq!(sequence().await, Some(1));

    // A renewal extends it.
    let renewed = store
        .renew_lease(&alice, LASTING + LASTING)
        .await
        .unwrap()
        .expect("alice still holds the thread");
    assert_eq!((renewed.owner.as_str(), renewed.token), ("alice", 1));
    assert!(renewed.expires_at > alice.expires_at);

    // A release frees the thread at once, and the next lease gets a higher token.
    assert!(store.release_lease(&alice).await.unwrap());
    assert_eq!(store.get_lease(THREAD).await.unwrap(), None);
    let mut bob = acquire(store, THREAD, "bob", brief).await;
    assert_eq!((bob.owner.as_str(), bob.token), ("bob", 2));

    // Alice is fenced: her save writes nothing, her renewal is refused, her
    // release changes nothing.
    let stale = checkpoint(THREAD, CheckpointStatus::Failed, "stale");
    for expected in [None, Some(1)] {
        assert_eq!(
            store
                .save_under_lease(stale.clone(), &alice, expected)
                .await
                .unwrap(),
            LeasedSave::LeaseLost
        );
    }
    assert_eq!(sequence().await, Some(1));
    assert_eq!(store.renew_lease(&alice, LASTING).await.unwrap(), None);
    assert!(!store.release_lease(&alice).await.unwrap());
    assert_eq!(held(store, THREAD).await, Some(("bob".to_string(), 2)));

    // An expired lease nobody took: the thread isn't held, but its owner's
    // token is still the latest, so it can still save and renew.
    time.pass(brief).await;
    assert_eq!(store.get_lease(THREAD).await.unwrap(), None);
    let three = checkpoint(THREAD, CheckpointStatus::Interrupted, "three");
    assert_eq!(
        store.save_under_lease(three, &bob, None).await.unwrap(),
        LeasedSave::Saved
    );
    assert_eq!(sequence().await, Some(2));
    bob = store
        .renew_lease(&bob, brief)
        .await
        .unwrap()
        .expect("nobody took it");
    assert_eq!(held(store, THREAD).await, Some(("bob".to_string(), 2)));

    // Expired again, and this time taken over: the token goes up, and bob is
    // fenced.
    time.pass(brief).await;
    let carol = acquire(store, THREAD, "carol", LASTING).await;
    assert_eq!((carol.owner.as_str(), carol.token), ("carol", 3));
    assert_eq!(
        store.save_under_lease(stale, &bob, None).await.unwrap(),
        LeasedSave::LeaseLost
    );
    assert_eq!(store.renew_lease(&bob, LASTING).await.unwrap(), None);
    assert_eq!(sequence().await, Some(2));

    // Saves with no lease are as they were, held thread or not.
    store
        .save(checkpoint(
            THREAD,
            CheckpointStatus::Interrupted,
            "unleased",
        ))
        .await
        .unwrap();
    let unleased = checkpoint(THREAD, CheckpointStatus::Interrupted, "unleased too");
    assert!(store.save_if_latest(unleased, Some(3)).await.unwrap());
    let last = checkpoint(THREAD, CheckpointStatus::Completed, "last");
    assert_eq!(
        store
            .save_under_lease(last.clone(), &carol, Some(4))
            .await
            .unwrap(),
        LeasedSave::Saved
    );
    assert_eq!(store.load(THREAD).await.unwrap(), Some(last.clone()));

    // Leases are per thread.
    let other = acquire(store, "leased-other", "carol", LASTING).await;
    assert_eq!(other.token, 1);
    assert_eq!(held(store, THREAD).await, Some(("carol".to_string(), 3)));

    // A lease for another thread, a blank owner and a ttl under a millisecond
    // are the caller's bugs.
    assert!(store.save_under_lease(last, &other, None).await.is_err());
    assert!(store.try_acquire_lease(THREAD, " ", LASTING).await.is_err());
    assert!(
        store
            .try_acquire_lease(THREAD, "dave", Duration::ZERO)
            .await
            .is_err()
    );
    assert!(store.renew_lease(&carol, Duration::ZERO).await.is_err());
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct WireTransferState {
    invoice_generated: bool,
    is_completed: bool,
}

fn wire_transfer_graph() -> CompiledGraph<WireTransferState> {
    StateGraph::new()
        .add_node("PrepareWireTransfer", |mut s: WireTransferState| {
            s.invoice_generated = true;
            s
        })
        .add_interrupt_boundary("PrepareWireTransfer")
        .add_node("ExecuteWireTransfer", |mut s: WireTransferState| {
            s.is_completed = true;
            s
        })
        .add_edge("PrepareWireTransfer", "ExecuteWireTransfer")
        .compile()
        .unwrap()
}

/// A leased run, end to end (`design/32-Thread-Leases.md`, "What a run does"):
/// the run freezes and lets go, a held thread refuses a resume and stays
/// resumable, and the resume then takes the next token and lets go too.
async fn run_leased_run(store: Arc<dyn CheckpointStore>) {
    const THREAD: &str = "wire-leased";
    let graph = wire_transfer_graph();
    let leased =
        || RunConfig::new().with_lease(LeaseOptions::new("worker-1", Duration::from_secs(60)));
    let initial = WireTransferState {
        invoice_generated: false,
        is_completed: false,
    };
    assert!(matches!(
        graph
            .run_with(THREAD, initial, store.clone(), leased())
            .await,
        GraphExecutionResult::Interrupted { .. }
    ));
    assert_eq!(store.get_lease(THREAD).await.unwrap(), None);

    let other = acquire(&*store, THREAD, "worker-2", Duration::from_secs(60)).await;
    assert_eq!(other.token, 2);
    match graph
        .resume_with(
            THREAD,
            HumanSignal::approve("alice"),
            store.clone(),
            leased(),
        )
        .await
    {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => assert!(
            message.starts_with("thread 'wire-leased' is held by 'worker-2' until "),
            "{message}"
        ),
        _ => panic!("a held thread refuses a resume"),
    }
    assert_eq!(store.load(THREAD).await.unwrap().unwrap().resumed_at, None);

    assert!(store.release_lease(&other).await.unwrap());
    assert!(matches!(
        graph
            .resume_with(
                THREAD,
                HumanSignal::approve("alice"),
                store.clone(),
                leased()
            )
            .await,
        GraphExecutionResult::Completed(_)
    ));
    assert!(
        store
            .load(THREAD)
            .await
            .unwrap()
            .unwrap()
            .resumed_at
            .is_some()
    );
    assert_eq!(store.get_lease(THREAD).await.unwrap(), None);
    assert_eq!(
        acquire(&*store, THREAD, "worker-3", Duration::from_secs(60))
            .await
            .token,
        4
    );
}

/// What `store` makes of `leases.rows.json`'s rows: each thread's lease, and
/// the token the next acquisition gets.
async fn assert_reads_the_rows(store: &dyn CheckpointStore, rows: &Value) {
    for (thread_id, expected) in rows["leases"].as_object().unwrap() {
        let lease = store.get_lease(thread_id).await.unwrap().map(|lease| {
            json!({
                "owner": lease.owner,
                "token": lease.token,
                "expires_at": lease.expires_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            })
        });
        assert_eq!(&lease.unwrap_or(Value::Null), expected, "{thread_id}");
    }
    for (thread_id, expected) in rows["next_tokens"].as_object().unwrap() {
        let attempt = store
            .try_acquire_lease(thread_id, "worker-9", LASTING)
            .await
            .unwrap();
        if expected.is_null() {
            assert_eq!(
                attempt.held_by().map(|holder| holder.owner.as_str()),
                rows["leases"][thread_id]["owner"].as_str(),
                "{thread_id}"
            );
        }
        assert_eq!(
            attempt.acquired().map(|lease| lease.token),
            expected.as_i64(),
            "{thread_id}"
        );
    }
}

// --- In-memory ---

#[tokio::test]
async fn in_memory_store_keeps_leases() {
    let clock = TestClock::new();
    let store = InMemoryCheckpointStore::new().with_clock(clock.clock());
    run_lease_contract(&store, &Time::Fake(clock)).await;
}

#[tokio::test]
async fn in_memory_store_runs_under_a_lease() {
    run_leased_run(Arc::new(InMemoryCheckpointStore::new())).await;
}

// --- SQLite ---

fn temp_db(name: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("strata-leases-{}-{name}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    path
}

#[tokio::test]
async fn sqlite_store_keeps_leases() {
    let clock = TestClock::new();
    let store = SqliteCheckpointStore::open_in_memory()
        .unwrap()
        .with_clock(clock.clock());
    run_lease_contract(&store, &Time::Fake(clock)).await;
}

#[tokio::test]
async fn sqlite_store_runs_under_a_lease() {
    run_leased_run(Arc::new(SqliteCheckpointStore::open_in_memory().unwrap())).await;
}

#[tokio::test]
async fn sqlite_lease_table_matches_the_shared_column_fixture() {
    let path = temp_db("layout");
    drop(SqliteCheckpointStore::open(&path).unwrap());
    let conn = rusqlite::Connection::open(&path).unwrap();
    let mut stmt = conn
        .prepare("PRAGMA table_info(checkpoint_leases)")
        .unwrap();
    let columns: Vec<Value> = stmt
        .query_map([], |row| {
            Ok(json!({
                "name": row.get::<_, String>(1)?,
                "type": row.get::<_, String>(2)?,
                "notnull": row.get::<_, i64>(3)? != 0,
                "pk": row.get::<_, i64>(5)?,
            }))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(
        Value::Array(columns),
        fixture("checkpoint-log/leases-columns.json")
    );
}

#[tokio::test]
async fn sqlite_reads_leases_another_writer_left() {
    let path = temp_db("rows");
    let store = SqliteCheckpointStore::open(&path).unwrap();
    let rows = fixture("checkpoint-log/leases.rows.json");
    let conn = rusqlite::Connection::open(&path).unwrap();
    for row in rows["rows"].as_array().unwrap() {
        conn.execute(
            "INSERT INTO checkpoint_leases (thread_id, owner, token, expires_at) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                row["thread_id"].as_str(),
                row["owner"].as_str(),
                row["token"].as_i64(),
                row["expires_at"].as_i64(),
            ],
        )
        .unwrap();
    }

    assert_reads_the_rows(&store, &rows).await;
}

#[tokio::test]
async fn sqlite_writes_a_lease_row_in_the_shared_shape() {
    let path = temp_db("shape");
    let store = SqliteCheckpointStore::open(&path).unwrap();
    let lease = acquire(&store, "t1", "worker-1", LASTING).await;
    let conn = rusqlite::Connection::open(&path).unwrap();
    let row = || {
        conn.query_row(
            "SELECT owner, token, typeof(expires_at), expires_at FROM checkpoint_leases WHERE thread_id = 't1'",
            [],
            |row| {
                Ok(json!({
                    "owner": row.get::<_, Option<String>>(0)?,
                    "token": row.get::<_, i64>(1)?,
                    "expires_at_type": row.get::<_, String>(2)?,
                    "expires_at": row.get::<_, Option<i64>>(3)?,
                }))
            },
        )
        .unwrap()
    };

    assert_eq!(
        row(),
        json!({
            "owner": "worker-1",
            "token": 1,
            "expires_at_type": "integer",
            "expires_at": lease.expires_at.timestamp_millis(),
        })
    );

    // A release keeps the row and its token, and clears who held it.
    store.release_lease(&lease).await.unwrap();
    assert_eq!(
        row(),
        json!({ "owner": null, "token": 1, "expires_at_type": "null", "expires_at": null })
    );
}

#[tokio::test]
async fn opening_a_sqlite_log_from_before_leases_creates_the_table() {
    let path = temp_db("migration");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(CHECKPOINT_LOG_DDL).unwrap();
        conn.execute_batch(CHECKPOINT_THREADS_DDL).unwrap();
    }

    let store = SqliteCheckpointStore::open(&path).unwrap();
    store
        .save(checkpoint("t-old", CheckpointStatus::Interrupted, "old"))
        .await
        .unwrap();

    assert_eq!(acquire(&store, "t-old", "worker-1", LASTING).await.token, 1);
}

// --- Postgres ---

async fn postgres_store(label: &str) -> Option<(PgSandbox, PostgresCheckpointStore)> {
    let url = backend_url(common::POSTGRES_VAR)?;
    let sandbox = PgSandbox::new(&url, label);
    let store = PostgresCheckpointStore::from_client(sandbox.client().await)
        .await
        .unwrap();
    Some((sandbox, store))
}

#[tokio::test]
async fn postgres_store_keeps_leases() {
    let Some((_sandbox, store)) = postgres_store("leases").await else {
        return;
    };
    run_lease_contract(&store, &Time::Real).await;
}

#[tokio::test]
async fn postgres_store_runs_under_a_lease() {
    let Some((_sandbox, store)) = postgres_store("leased_run").await else {
        return;
    };
    run_leased_run(Arc::new(store)).await;
}

#[tokio::test]
async fn postgres_lease_table_matches_the_shared_column_fixture() {
    let Some((sandbox, _store)) = postgres_store("lease_layout").await else {
        return;
    };
    let columns = sandbox.admin(|client| {
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
                       AND tc.table_schema = current_schema() AND tc.table_name = 'checkpoint_leases'
                 ) k ON k.column_name = c.column_name
                 WHERE c.table_schema = current_schema() AND c.table_name = 'checkpoint_leases'
                 ORDER BY c.ordinal_position",
                &[],
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
    });

    assert_eq!(
        columns,
        fixture("checkpoint-log/postgres-leases-columns.json")
    );
}

#[tokio::test]
async fn postgres_reads_leases_another_writer_left() {
    let Some((sandbox, store)) = postgres_store("lease_rows").await else {
        return;
    };
    let rows = fixture("checkpoint-log/leases.rows.json");
    sandbox.admin(|client| {
        for row in rows["rows"].as_array().unwrap() {
            client
                .execute(
                    "INSERT INTO checkpoint_leases (thread_id, owner, token, expires_at)
                     VALUES ($1, $2, $3, to_timestamp($4::bigint / 1000.0))",
                    &[
                        &row["thread_id"].as_str(),
                        &row["owner"].as_str(),
                        &row["token"].as_i64(),
                        &row["expires_at"].as_i64(),
                    ],
                )
                .unwrap();
        }
    });

    assert_reads_the_rows(&store, &rows).await;
}

#[tokio::test]
async fn opening_a_postgres_log_from_before_leases_creates_the_table() {
    let Some((sandbox, old)) = postgres_store("lease_migration").await else {
        return;
    };
    old.save(checkpoint("t-old", CheckpointStatus::Interrupted, "old"))
        .await
        .unwrap();
    drop(old);
    // As a runtime from before leases left it: no lease table, and no record
    // of the version its tables are at (`design/34-Storage-Hardening.md`).
    sandbox.admin(|client| {
        client
            .batch_execute("DROP TABLE checkpoint_leases, strata_schema_version")
            .unwrap()
    });

    let store = PostgresCheckpointStore::from_client(sandbox.client().await)
        .await
        .unwrap();

    assert_eq!(acquire(&store, "t-old", "worker-1", LASTING).await.token, 1);
}

// --- Redis ---

async fn redis_store(label: &str) -> Option<(RedisSandbox, RedisCheckpointStore)> {
    let url = backend_url(common::REDIS_VAR)?;
    let sandbox = RedisSandbox::new(&url, label);
    let store = RedisCheckpointStore::connect_with_prefix(&url, &sandbox.prefix)
        .await
        .unwrap();
    Some((sandbox, store))
}

#[tokio::test]
async fn redis_store_keeps_leases() {
    let Some((_sandbox, store)) = redis_store("leases").await else {
        return;
    };
    run_lease_contract(&store, &Time::Real).await;
}

#[tokio::test]
async fn redis_store_runs_under_a_lease() {
    let Some((_sandbox, store)) = redis_store("leased_run").await else {
        return;
    };
    run_leased_run(Arc::new(store)).await;
}

/// The lease keys of thread `t1` under the sandbox's prefix, in the shape of
/// `redis-leases.json`'s steps.
fn lease_keys(sandbox: &RedisSandbox) -> Value {
    let mut conn = sandbox.connection();
    let hash: HashMap<String, String> = redis::cmd("HGETALL")
        .arg(format!("{}:leases:t1", sandbox.prefix))
        .query(&mut conn)
        .unwrap();
    let token: Option<String> = redis::cmd("GET")
        .arg(format!("{}:lease-tokens:t1", sandbox.prefix))
        .query(&mut conn)
        .unwrap();
    json!({
        "leases:t1": if hash.is_empty() { Value::Null } else { json!(hash) },
        "lease-tokens:t1": token,
    })
}

#[tokio::test]
async fn a_lease_leaves_the_shared_redis_keys() {
    let Some((sandbox, store)) = redis_store("lease_keys").await else {
        return;
    };
    let steps = fixture("checkpoint-log/redis-leases.json");
    let steps = steps["steps"].as_array().unwrap();
    let ttl = Duration::from_secs(60);

    let first = acquire(&store, "t1", "worker-1", ttl).await;
    assert_eq!(lease_keys(&sandbox), steps[0]["keys"]);

    // The hash expires with the lease; the counter never does.
    let mut conn = sandbox.connection();
    let lease_ttl: i64 = redis::cmd("PTTL")
        .arg(format!("{}:leases:t1", sandbox.prefix))
        .query(&mut conn)
        .unwrap();
    assert!((30_000..=60_000).contains(&lease_ttl), "{lease_ttl}");
    let token_ttl: i64 = redis::cmd("PTTL")
        .arg(format!("{}:lease-tokens:t1", sandbox.prefix))
        .query(&mut conn)
        .unwrap();
    assert_eq!(token_ttl, -1);

    store.release_lease(&first).await.unwrap();
    assert_eq!(lease_keys(&sandbox), steps[1]["keys"]);

    acquire(&store, "t1", "worker-2", ttl).await;
    assert_eq!(lease_keys(&sandbox), steps[2]["keys"]);
}

#[tokio::test]
async fn redis_reads_a_lease_another_writer_left() {
    let Some((sandbox, store)) = redis_store("lease_raw").await else {
        return;
    };
    let steps = fixture("checkpoint-log/redis-leases.json");
    let keys = &steps["steps"][2]["keys"];
    let lease_key = format!("{}:leases:t1", sandbox.prefix);
    let mut conn = sandbox.connection();
    for (field, value) in keys["leases:t1"].as_object().unwrap() {
        let _: i64 = redis::cmd("HSET")
            .arg(&lease_key)
            .arg(field)
            .arg(value.as_str().unwrap())
            .query(&mut conn)
            .unwrap();
    }
    let _: i64 = redis::cmd("PEXPIRE")
        .arg(&lease_key)
        .arg(60_000)
        .query(&mut conn)
        .unwrap();
    let _: String = redis::cmd("SET")
        .arg(format!("{}:lease-tokens:t1", sandbox.prefix))
        .arg(keys["lease-tokens:t1"].as_str().unwrap())
        .query(&mut conn)
        .unwrap();

    assert_eq!(held(&store, "t1").await, Some(("worker-2".to_string(), 2)));
    let refused = store
        .try_acquire_lease("t1", "worker-9", LASTING)
        .await
        .unwrap();
    assert_eq!(
        refused.held_by().map(|holder| holder.owner.as_str()),
        Some("worker-2")
    );

    // Once it is gone, the next lease continues the count.
    let _: i64 = redis::cmd("DEL").arg(&lease_key).query(&mut conn).unwrap();
    assert_eq!(acquire(&store, "t1", "worker-9", LASTING).await.token, 3);
}
