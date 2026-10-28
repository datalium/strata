//! Delta checkpoints (`design/41-Checkpoint-Efficiency.md`, SG-15 part 2):
//! the shared rows fixture, chains with a whole row every
//! [`Deltas::with_full_every`], a base that moved, a cold cache, nested
//! levels, a save in doubt, group commit and a durable run, against SQLite,
//! and Postgres and Redis when `STRATA_TEST_POSTGRES_URL` /
//! `STRATA_TEST_REDIS_URL` are set (see `common`). The codec on its own is
//! tested in `src/deltas.rs`; the store contracts with deltas on run in
//! `checkpoint_store_contract.rs` and `durable_execution.rs`. The xUnit twin
//! is `DeltaCheckpointTests`.

mod common;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use common::{PgSandbox, RedisSandbox, backend_url, fixture, unique_name};
use datalium_strata::checkpoint::{
    CheckpointQuery, CheckpointStore, GraphExecutionResult, LeaseAttempt, ThreadQuery,
};
use datalium_strata::graph::{CheckpointPolicy, CompileOptions, StateGraph};
use datalium_strata::schema::{Checkpoint, CheckpointStatus};
use datalium_strata::store::LeasedSave;
use datalium_strata_memory::checkpoint::{InMemoryCheckpointStore, SqliteCheckpointStore};
use datalium_strata_memory::group_commit::{CHECKPOINT_SIZE_METRIC, ENCODING_LABEL};
use datalium_strata_memory::postgres_store::PostgresCheckpointStore;
use datalium_strata_memory::redis_store::RedisCheckpointStore;
use datalium_strata_memory::{
    Deltas, GroupCommit, PostgresStoreOptions, RedisStoreOptions, SqliteStoreOptions,
};
use metrics_util::debugging::DebuggingRecorder;
use redis::Commands;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const WHOLE: &str = "1.7.0";
const DELTA: &str = "1.8.0";

/// How long a test waits for something that should happen at once.
const PATIENCE: Duration = Duration::from_secs(30);

/// A running checkpoint at `step`, with a payload that never changes.
fn running(thread_id: &str, step: u32, nested: Option<Checkpoint>) -> Checkpoint {
    Checkpoint {
        schema_version: WHOLE.to_string(),
        thread_id: thread_id.to_string(),
        status: CheckpointStatus::Running,
        suspended_node: Some("A".to_string()),
        next_node: Some(if nested.is_some() { "Work" } else { "B" }.to_string()),
        frozen_state: json!({ "step": step, "payload": "e".repeat(2048), "flags": [true, null] }),
        metadata: Some(HashMap::from([(
            "strata.hops".to_string(),
            step.to_string(),
        )])),
        created_at: format!("2026-10-15T00:{:02}:{:02}Z", step / 60, step % 60),
        resumed_at: None,
        nested: nested.map(Box::new),
        decision: None,
        wait: None,
        graph: None,
    }
}

fn child(name: &str, state: Value) -> Checkpoint {
    Checkpoint {
        schema_version: WHOLE.to_string(),
        thread_id: name.to_string(),
        status: CheckpointStatus::Running,
        suspended_node: None,
        next_node: Some("Scan".to_string()),
        frozen_state: state,
        metadata: None,
        created_at: "2026-10-15T00:00:00Z".to_string(),
        resumed_at: None,
        nested: None,
        decision: None,
        wait: None,
        graph: None,
    }
}

/// A store, as one trait object.
trait Store: CheckpointStore + CheckpointQuery {}

impl<T: CheckpointStore + CheckpointQuery> Store for T {}

type Fault = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// One database (or key prefix) a test opens stores on, as many as it needs,
/// and reads as stored: each row of a thread as `{schema_version,
/// frozen_state, nested}`.
enum Backend {
    Sqlite(PathBuf),
    Postgres(PgSandbox),
    Redis(RedisSandbox),
}

impl Drop for Backend {
    fn drop(&mut self) {
        if let Self::Sqlite(path) = self {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn sqlite() -> Option<Backend> {
    Some(Backend::Sqlite(
        std::env::temp_dir().join(format!("{}.db", unique_name("deltas"))),
    ))
}

fn postgres(label: &str) -> Option<Backend> {
    let url = backend_url(common::POSTGRES_VAR)?;
    Some(Backend::Postgres(PgSandbox::new(&url, label)))
}

fn redis_backend(label: &str) -> Option<Backend> {
    let url = backend_url(common::REDIS_VAR)?;
    Some(Backend::Redis(RedisSandbox::new(&url, label)))
}

impl Backend {
    fn name(&self) -> &'static str {
        match self {
            Self::Sqlite(_) => "sqlite",
            Self::Postgres(_) => "postgres",
            Self::Redis(_) => "redis",
        }
    }

    async fn open(
        &self,
        deltas: Option<Deltas>,
        group: Option<GroupCommit>,
        fault: Option<Fault>,
    ) -> Arc<dyn Store> {
        match self {
            Self::Sqlite(path) => {
                let mut options = SqliteStoreOptions::new();
                if let Some(deltas) = deltas {
                    options = options.with_deltas(deltas);
                }
                if let Some(group) = group {
                    options = options.with_group_commit(group);
                }
                if let Some(fault) = fault {
                    options = options.with_fault(move |point| fault(point));
                }
                Arc::new(SqliteCheckpointStore::open_with(path, options).unwrap())
            }
            Self::Postgres(sandbox) => {
                let mut options = PostgresStoreOptions::new();
                if let Some(deltas) = deltas {
                    options = options.with_deltas(deltas);
                }
                if let Some(group) = group {
                    options = options.with_group_commit(group);
                }
                if let Some(fault) = fault {
                    options = options.with_fault(move |point| fault(point));
                }
                Arc::new(
                    PostgresCheckpointStore::connect_with(&sandbox.url(None), options)
                        .await
                        .unwrap(),
                )
            }
            Self::Redis(sandbox) => {
                let mut options = RedisStoreOptions::new().with_prefix(&sandbox.prefix);
                if let Some(deltas) = deltas {
                    options = options.with_deltas(deltas);
                }
                if let Some(fault) = fault {
                    options = options.with_fault(move |point| fault(point));
                }
                Arc::new(
                    RedisCheckpointStore::connect_with(&sandbox.url, options)
                        .await
                        .unwrap(),
                )
            }
        }
    }

    /// The rows of `thread_id` as stored, oldest first.
    fn raw(&self, thread_id: &str) -> Vec<Value> {
        fn row(version: String, frozen_state: &str, nested: Option<String>) -> Value {
            json!({
                "schema_version": version,
                "frozen_state": serde_json::from_str::<Value>(frozen_state).unwrap(),
                "nested": nested.map(|nested| serde_json::from_str::<Value>(&nested).unwrap()),
            })
        }
        match self {
            Self::Sqlite(path) => {
                let conn = rusqlite::Connection::open(path).unwrap();
                let mut stmt = conn
                    .prepare(
                        "SELECT schema_version, frozen_state, nested FROM checkpoints
                         WHERE thread_id = ?1 ORDER BY sequence",
                    )
                    .unwrap();
                stmt.query_map([thread_id], |found| {
                    Ok(row(
                        found.get(0)?,
                        &found.get::<_, String>(1)?,
                        found.get(2)?,
                    ))
                })
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap()
            }
            Self::Postgres(sandbox) => sandbox.admin(|client| {
                client
                    .query(
                        "SELECT schema_version, frozen_state::text, nested::text FROM checkpoints
                         WHERE thread_id = $1 ORDER BY sequence",
                        &[&thread_id],
                    )
                    .unwrap()
                    .iter()
                    .map(|found| row(found.get(0), found.get::<_, &str>(1), found.get(2)))
                    .collect()
            }),
            Self::Redis(sandbox) => {
                let elements: Vec<String> = sandbox
                    .connection()
                    .lrange(format!("{}:checkpoints:{thread_id}", sandbox.prefix), 0, -1)
                    .unwrap();
                elements
                    .iter()
                    .map(|element| serde_json::from_str(element).unwrap())
                    .collect()
            }
        }
    }

    fn versions(&self, thread_id: &str) -> Vec<String> {
        self.raw(thread_id)
            .iter()
            .map(|row| row["schema_version"].as_str().unwrap().to_string())
            .collect()
    }

    /// Stores `stored`, a checkpoint as a store would write it, at `sequence`,
    /// as any other writer would.
    fn insert_raw(&self, thread_id: &str, sequence: i64, stored: &Value) {
        let text = |key: &str| stored[key].as_str().map(str::to_string);
        let json = |key: &str| (!stored[key].is_null()).then(|| stored[key].to_string());
        match self {
            Self::Sqlite(path) => {
                rusqlite::Connection::open(path)
                    .unwrap()
                    .execute(
                        "INSERT INTO checkpoints
                            (thread_id, sequence, schema_version, status, suspended_node, next_node,
                             frozen_state, metadata, created_at, resumed_at, nested)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                        rusqlite::params![
                            thread_id,
                            sequence,
                            text("schema_version"),
                            text("status"),
                            text("suspended_node"),
                            text("next_node"),
                            json("frozen_state"),
                            json("metadata"),
                            text("created_at"),
                            text("resumed_at"),
                            json("nested"),
                        ],
                    )
                    .unwrap();
            }
            Self::Postgres(sandbox) => sandbox.admin(|client| {
                client
                    .execute(
                        "INSERT INTO checkpoints
                            (thread_id, sequence, schema_version, status, suspended_node, next_node,
                             frozen_state, metadata, created_at, resumed_at, nested)
                         VALUES ($1, $2, $3, $4, $5, $6, $7::text::jsonb, $8::text::jsonb, $9, $10,
                                 $11::text::jsonb)",
                        &[
                            &thread_id,
                            &sequence,
                            &text("schema_version"),
                            &text("status"),
                            &text("suspended_node"),
                            &text("next_node"),
                            &json("frozen_state"),
                            &json("metadata"),
                            &text("created_at"),
                            &text("resumed_at"),
                            &json("nested"),
                        ],
                    )
                    .unwrap();
            }),
            Self::Redis(sandbox) => {
                let _: i64 = sandbox
                    .connection()
                    .rpush(
                        format!("{}:checkpoints:{thread_id}", sandbox.prefix),
                        stored.to_string(),
                    )
                    .unwrap();
            }
        }
    }
}

async fn log(store: &dyn Store, thread_id: &str) -> Vec<Checkpoint> {
    store
        .get_log(thread_id, None, 1000)
        .await
        .unwrap()
        .entries
        .into_iter()
        .map(|entry| entry.checkpoint)
        .collect()
}

fn steps(thread_id: &str, steps: std::ops::Range<u32>) -> Vec<Checkpoint> {
    steps.map(|step| running(thread_id, step, None)).collect()
}

// --- The shared rows fixture ---

/// `checkpoint-log/delta.rows.json`, stored as it is a row at a time: a load
/// after each row gives its checkpoint, the log gives every row's, a page that
/// starts at a delta reads the rows it needs, and a listing gives the version
/// the latest was saved with. The store reading them has deltas off.
async fn shared_rows(backend: Backend) {
    let fixture = fixture("checkpoint-log/delta.rows.json");
    let thread_id = fixture["thread_id"].as_str().unwrap();
    let rows = fixture["rows"].as_array().unwrap();
    // A store opened before the rows are there, so the SQL tables exist.
    let store = backend.open(None, None, None).await;
    let decoded: Vec<Checkpoint> = rows
        .iter()
        .map(|row| serde_json::from_value(row["decoded"].clone()).unwrap())
        .collect();
    for row in rows {
        let sequence = row["sequence"].as_i64().unwrap();
        backend.insert_raw(thread_id, sequence, &row["stored"]);
        assert_eq!(
            store.load(thread_id).await.unwrap().as_ref(),
            Some(&decoded[sequence as usize])
        );
        let latest = store.load_latest(thread_id).await.unwrap().unwrap();
        assert_eq!(latest.sequence, Some(sequence));
        assert_eq!(latest.checkpoint, decoded[sequence as usize]);
    }

    assert_eq!(backend.versions(thread_id), [WHOLE, DELTA, DELTA]);
    assert_eq!(log(&*store, thread_id).await, decoded);
    let page = store.get_log(thread_id, Some(1), 1).await.unwrap().entries;
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].checkpoint, decoded[2]);

    if let Backend::Redis(sandbox) = &backend {
        RedisCheckpointStore::connect_with_prefix(&sandbox.url, &sandbox.prefix)
            .await
            .unwrap()
            .rebuild_index()
            .await
            .unwrap();
    }
    let threads = store
        .list_threads(&ThreadQuery::default())
        .await
        .unwrap()
        .threads;
    assert_eq!(threads.len(), 1);
    assert_eq!(
        (
            threads[0].thread_id.as_str(),
            threads[0].sequence,
            threads[0].schema_version.as_str()
        ),
        (thread_id, 2, WHOLE)
    );
}

#[tokio::test]
async fn sqlite_reads_the_shared_delta_rows() {
    shared_rows(sqlite().unwrap()).await;
}

#[tokio::test]
async fn postgres_reads_the_shared_delta_rows() {
    if let Some(backend) = postgres("delta_rows") {
        shared_rows(backend).await;
    }
}

#[tokio::test]
async fn redis_reads_the_shared_delta_rows() {
    if let Some(backend) = redis_backend("delta_rows") {
        shared_rows(backend).await;
    }
}

// --- Chains ---

/// Ten running saves, an interrupted one, two more running: a delta carries
/// only what changed, every fourth row and every non-running one is whole,
/// the log decodes to what was saved, and a store with deltas off reads it the
/// same. A Postgres or Redis store records the deltas' sizes (a SQLite one
/// records them on the blocking pool, out of the test's recorder's reach).
async fn chain(backend: Backend) {
    let recorder: &'static DebuggingRecorder = Box::leak(Box::new(DebuggingRecorder::new()));
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(recorder);

    let store = backend
        .open(Some(Deltas::new().with_full_every(4)), None, None)
        .await;
    let mut saved = Vec::new();
    for step in 0..13 {
        let mut checkpoint = running("t", step, None);
        if step == 10 {
            checkpoint.status = CheckpointStatus::Interrupted;
            checkpoint.schema_version = "1.0.0".to_string();
        }
        store.save(checkpoint.clone()).await.unwrap();
        assert_eq!(store.load("t").await.unwrap().as_ref(), Some(&checkpoint));
        saved.push(checkpoint);
    }

    assert_eq!(
        backend.versions("t"),
        [
            WHOLE, DELTA, DELTA, DELTA, WHOLE, DELTA, DELTA, DELTA, WHOLE, DELTA, "1.0.0", DELTA,
            DELTA
        ]
    );
    assert_eq!(
        backend.raw("t")[1]["frozen_state"],
        json!({ "checkpoint_version": "1.7.0", "delta_of": 0, "depth": 1, "set": { "step": 1 }, "unset": [] })
    );
    assert_eq!(log(&*store, "t").await, saved);
    assert_eq!(
        log(&*backend.open(None, None, None).await, "t").await,
        saved
    );

    if !matches!(backend, Backend::Sqlite(_)) {
        let deltas = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, _, _, _)| {
                key.key().name() == CHECKPOINT_SIZE_METRIC
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == ENCODING_LABEL && label.value() == "delta")
            })
            .count();
        assert!(deltas > 0, "no {} delta was recorded", backend.name());
    }
}

#[tokio::test]
async fn sqlite_stores_running_checkpoints_as_deltas_with_every_full_every_th_row_whole() {
    chain(sqlite().unwrap()).await;
}

#[tokio::test]
async fn postgres_stores_running_checkpoints_as_deltas_with_every_full_every_th_row_whole() {
    if let Some(backend) = postgres("delta_chain") {
        chain(backend).await;
    }
}

#[tokio::test]
async fn redis_stores_running_checkpoints_as_deltas_with_every_full_every_th_row_whole() {
    if let Some(backend) = redis_backend("delta_chain") {
        chain(backend).await;
    }
}

// --- A base that moved, a cold cache ---

/// A store's cache says row 1 is the thread's latest; another writer appends
/// row 2. The next save, plain, conditional or fenced, is written whole, and
/// the one after is a delta of it.
async fn base_moved(backend: Backend) {
    let store = backend.open(Some(Deltas::new()), None, None).await;
    let other = backend.open(None, None, None).await;
    let mut saved = Vec::new();
    for (by, step) in [
        (&store, 0),
        (&store, 1),
        (&other, 2),
        (&store, 3),
        (&store, 4),
    ] {
        by.save(running("t", step, None)).await.unwrap();
        saved.push(running("t", step, None));
    }
    assert!(
        store
            .save_if_latest(running("t", 5, None), Some(4))
            .await
            .unwrap()
    );
    other.save(running("t", 6, None)).await.unwrap();
    assert!(
        store
            .save_if_latest(running("t", 7, None), Some(6))
            .await
            .unwrap()
    );
    let LeaseAttempt::Acquired(lease) = store
        .try_acquire_lease("t", "w", Duration::from_secs(300))
        .await
        .unwrap()
    else {
        panic!("the lease is held");
    };
    assert_eq!(
        store
            .save_under_lease(running("t", 8, None), &lease, None)
            .await
            .unwrap(),
        LeasedSave::Saved
    );
    other.save(running("t", 9, None)).await.unwrap();
    assert_eq!(
        store
            .save_under_lease(running("t", 10, None), &lease, None)
            .await
            .unwrap(),
        LeasedSave::Saved
    );
    saved.extend((5..11).map(|step| running("t", step, None)));

    assert_eq!(
        backend.versions("t"),
        [
            WHOLE, DELTA, WHOLE, WHOLE, DELTA, DELTA, WHOLE, WHOLE, DELTA, WHOLE, WHOLE
        ]
    );
    assert_eq!(log(&*store, "t").await, saved);
}

#[tokio::test]
async fn sqlite_writes_whole_when_another_writer_moved_the_base() {
    base_moved(sqlite().unwrap()).await;
}

#[tokio::test]
async fn postgres_writes_whole_when_another_writer_moved_the_base() {
    if let Some(backend) = postgres("delta_moved") {
        base_moved(backend).await;
    }
}

#[tokio::test]
async fn redis_writes_whole_when_another_writer_moved_the_base() {
    if let Some(backend) = redis_backend("delta_moved") {
        base_moved(backend).await;
    }
}

/// A new store doesn't know the thread and saves it whole; one that has
/// loaded the thread's latest, a delta; one that remembers a single thread
/// forgets the first when it saves a second.
async fn cold_cache(backend: Backend) {
    let on = || Some(Deltas::new());
    backend
        .open(on(), None, None)
        .await
        .save(running("t", 0, None))
        .await
        .unwrap();
    let first = backend.open(on(), None, None).await;
    first.save(running("t", 1, None)).await.unwrap();
    let loaded = backend.open(on(), None, None).await;
    assert!(loaded.load_latest("t").await.unwrap().is_some());
    loaded.save(running("t", 2, None)).await.unwrap();

    let small = backend
        .open(Some(Deltas::new().with_cached_threads(1)), None, None)
        .await;
    small.save(running("t", 3, None)).await.unwrap();
    small.save(running("u", 0, None)).await.unwrap();
    small.save(running("t", 4, None)).await.unwrap();
    small.save(running("t", 5, None)).await.unwrap();

    assert_eq!(
        backend.versions("t"),
        [WHOLE, WHOLE, DELTA, WHOLE, WHOLE, DELTA]
    );
    assert_eq!(log(&*first, "t").await, steps("t", 0..6));
}

#[tokio::test]
async fn sqlite_writes_whole_from_a_cold_cache_and_deltas_after_a_load() {
    cold_cache(sqlite().unwrap()).await;
}

#[tokio::test]
async fn postgres_writes_whole_from_a_cold_cache_and_deltas_after_a_load() {
    if let Some(backend) = postgres("delta_cold") {
        cold_cache(backend).await;
    }
}

#[tokio::test]
async fn redis_writes_whole_from_a_cold_cache_and_deltas_after_a_load() {
    if let Some(backend) = redis_backend("delta_cold") {
        cold_cache(backend).await;
    }
}

// --- Nested levels ---

/// A level that appears is whole beside a delta, then a delta itself; a level
/// that goes leaves no trace.
async fn nested(backend: Backend) {
    let store = backend.open(Some(Deltas::new()), None, None).await;
    let saved = vec![
        running("t", 0, None),
        running("t", 1, Some(child("Work", json!({ "n": 0, "draft": "x" })))),
        running("t", 2, Some(child("Work", json!({ "n": 1 })))),
        running("t", 3, None),
    ];
    for checkpoint in &saved {
        store.save(checkpoint.clone()).await.unwrap();
    }

    let raw = backend.raw("t");
    assert_eq!(backend.versions("t"), [WHOLE, DELTA, DELTA, DELTA]);
    assert!(raw[1]["frozen_state"].get("set").is_some());
    assert_eq!(
        raw[1]["nested"]["frozen_state"],
        json!({ "state": { "n": 0, "draft": "x" } })
    );
    assert_eq!(
        raw[2]["nested"]["frozen_state"],
        json!({ "set": { "n": 1 }, "unset": ["draft"] })
    );
    assert!(raw[3]["nested"].is_null());
    assert_eq!(log(&*store, "t").await, saved);
}

#[tokio::test]
async fn sqlite_encodes_each_level_on_its_own() {
    nested(sqlite().unwrap()).await;
}

#[tokio::test]
async fn postgres_encodes_each_level_on_its_own() {
    if let Some(backend) = postgres("delta_nested") {
        nested(backend).await;
    }
}

#[tokio::test]
async fn redis_encodes_each_level_on_its_own() {
    if let Some(backend) = redis_backend("delta_nested") {
        nested(backend).await;
    }
}

// --- A save in doubt ---

/// A fault that fails the call once at `point`, as a lost connection would,
/// after it is armed.
fn armed(point: &'static str) -> (Arc<AtomicBool>, Fault) {
    let armed = Arc::new(AtomicBool::new(false));
    let trigger = armed.clone();
    (
        armed,
        Arc::new(move |reached| reached == point && trigger.swap(false, Ordering::SeqCst)),
    )
}

/// A delta save whose answer is lost after it was written is looked up, as
/// what it decodes to, and found: written once. The store then no longer
/// knows the thread's latest, so the next save is whole.
async fn in_doubt(backend: Backend, point: &'static str) {
    let (arm, fault) = armed(point);
    let store = backend.open(Some(Deltas::new()), None, Some(fault)).await;
    store.save(running("t", 0, None)).await.unwrap();
    arm.store(true, Ordering::SeqCst);
    store.save(running("t", 1, None)).await.unwrap();
    assert!(
        store
            .save_if_latest(running("t", 2, None), Some(1))
            .await
            .unwrap()
    );
    store.save(running("t", 3, None)).await.unwrap();

    assert_eq!(backend.versions("t"), [WHOLE, DELTA, WHOLE, DELTA]);
    assert_eq!(log(&*store, "t").await, steps("t", 0..4));
}

#[tokio::test]
async fn postgres_looks_up_a_delta_save_whose_commit_may_have_happened() {
    if let Some(backend) = postgres("delta_doubt") {
        in_doubt(backend, "save:after-commit").await;
    }
}

#[tokio::test]
async fn redis_looks_up_a_delta_save_that_may_have_run() {
    if let Some(backend) = redis_backend("delta_doubt") {
        in_doubt(backend, "save:after").await;
    }
}

// --- Group commit ---

/// A fault hook that holds the first flush reaching `point` after it is armed
/// until it is released, blocking the thread the flush runs on (through
/// `block_in_place` in a runtime worker).
#[derive(Clone)]
struct Hold {
    point: &'static str,
    armed: Arc<AtomicBool>,
    state: Arc<(Mutex<(bool, bool)>, Condvar)>,
}

impl Hold {
    fn new(point: &'static str) -> Self {
        Self {
            point,
            armed: Arc::new(AtomicBool::new(false)),
            state: Arc::new((Mutex::new((false, false)), Condvar::new())),
        }
    }

    fn fault(&self) -> Fault {
        let hold = self.clone();
        Arc::new(move |reached| {
            if reached == hold.point && hold.armed.swap(false, Ordering::SeqCst) {
                let wait = || {
                    let (lock, changed) = &*hold.state;
                    let mut state = lock.lock().unwrap();
                    state.0 = true;
                    changed.notify_all();
                    let (state, _) = changed
                        .wait_timeout_while(state, PATIENCE, |state| !state.1)
                        .unwrap();
                    assert!(state.1, "the held flush was never released");
                };
                match tokio::runtime::Handle::try_current() {
                    Ok(_) => tokio::task::block_in_place(wait),
                    Err(_) => wait(),
                }
            }
            false
        })
    }

    async fn held(&self) {
        let started = std::time::Instant::now();
        while !self.state.0.lock().unwrap().0 {
            assert!(started.elapsed() < PATIENCE, "no flush was held");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    fn release(&self) {
        let (lock, changed) = &*self.state;
        lock.lock().unwrap().1 = true;
        changed.notify_all();
    }
}

/// With group commit: a delta whose base another writer moved is written
/// whole in its flush, two saves of one thread in one flush are both whole,
/// and the save after them is a delta of the second.
async fn group_commit(backend: Backend, hold_at: &'static str) {
    let hold = Hold::new(hold_at);
    let store = backend
        .open(
            Some(Deltas::new()),
            Some(GroupCommit::new().with_max_concurrent_flushes(1)),
            Some(hold.fault()),
        )
        .await;
    let other = backend.open(None, None, None).await;
    store.save(running("t", 0, None)).await.unwrap();
    store.save(running("t", 1, None)).await.unwrap();
    other.save(running("t", 2, None)).await.unwrap();
    store.save(running("t", 3, None)).await.unwrap();
    store.save(running("t", 4, None)).await.unwrap();

    hold.armed.store(true, Ordering::SeqCst);
    let blocker = tokio::spawn({
        let store = store.clone();
        async move { store.save(running("blocker", 0, None)).await }
    });
    hold.held().await;
    let fifth = tokio::spawn({
        let store = store.clone();
        async move { store.save(running("t", 5, None)).await }
    });
    // Queued after the fifth, so the two go in one flush, in order.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let sixth = tokio::spawn({
        let store = store.clone();
        async move { store.save(running("t", 6, None)).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    hold.release();
    for save in [blocker, fifth, sixth] {
        tokio::time::timeout(PATIENCE, save)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    store.save(running("t", 7, None)).await.unwrap();

    assert_eq!(
        backend.versions("t"),
        [WHOLE, DELTA, WHOLE, WHOLE, DELTA, WHOLE, WHOLE, DELTA]
    );
    assert_eq!(log(&*store, "t").await, steps("t", 0..8));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_writes_whole_in_a_flush_what_can_not_be_a_delta() {
    group_commit(sqlite().unwrap(), "flush:start").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_writes_whole_in_a_flush_what_can_not_be_a_delta() {
    if let Some(backend) = postgres("delta_flush") {
        group_commit(backend, "save:before-commit").await;
    }
}

// --- A durable run ---

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Counter {
    step: u32,
    payload: String,
}

/// A graph that checkpoints every node, run against a store with deltas:
/// every running row after the first is a delta but every fourth, the
/// completed row is whole, and the log decodes to what the same run leaves in
/// the in-memory store.
async fn durable_run(backend: Backend) {
    let mut graph = StateGraph::new();
    for node in 1..=8 {
        graph = graph.add_node(format!("N{node}"), |mut s: Counter| {
            s.step += 1;
            s
        });
        if node > 1 {
            graph = graph.add_edge(format!("N{}", node - 1), format!("N{node}"));
        }
    }
    let graph = graph
        .compile_with(CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode))
        .unwrap();
    let initial = Counter {
        step: 0,
        payload: "p".repeat(4096),
    };
    let store = backend
        .open(Some(Deltas::new().with_full_every(4)), None, None)
        .await;
    let as_store: Arc<dyn CheckpointStore> = store.clone();
    assert!(matches!(
        graph.run("run", initial.clone(), as_store).await,
        GraphExecutionResult::Completed(_)
    ));
    let memory = Arc::new(InMemoryCheckpointStore::new());
    assert!(matches!(
        graph.run("run", initial, memory.clone()).await,
        GraphExecutionResult::Completed(_)
    ));

    let versions = backend.versions("run");
    assert_eq!(versions.len(), 9);
    assert_eq!(
        versions[..8],
        [WHOLE, DELTA, DELTA, DELTA, WHOLE, DELTA, DELTA, DELTA]
    );
    assert_ne!(versions[8], DELTA);
    let expected = log(&*memory, "run").await;
    let actual = log(&*store, "run").await;
    assert_eq!(expected.len(), actual.len());
    for (expected, actual) in expected.iter().zip(&actual) {
        assert_eq!(
            (
                &expected.status,
                &expected.next_node,
                &expected.frozen_state
            ),
            (&actual.status, &actual.next_node, &actual.frozen_state)
        );
    }
}

#[tokio::test]
async fn sqlite_keeps_a_durable_run_as_deltas() {
    durable_run(sqlite().unwrap()).await;
}

#[tokio::test]
async fn postgres_keeps_a_durable_run_as_deltas() {
    if let Some(backend) = postgres("delta_run") {
        durable_run(backend).await;
    }
}

#[tokio::test]
async fn redis_keeps_a_durable_run_as_deltas() {
    if let Some(backend) = redis_backend("delta_run") {
        durable_run(backend).await;
    }
}

#[tokio::test]
async fn invalid_options_are_refused_by_every_constructor() {
    let refused = SqliteCheckpointStore::open_in_memory_with(
        SqliteStoreOptions::new().with_deltas(Deltas::new().with_full_every(1)),
    );
    assert!(refused.is_err());
    if let Some(Backend::Postgres(ref sandbox)) = postgres("delta_options") {
        assert!(
            PostgresCheckpointStore::connect_with(
                &sandbox.url(None),
                PostgresStoreOptions::new().with_deltas(Deltas::new().with_cached_threads(0)),
            )
            .await
            .is_err()
        );
    }
    if let Some(Backend::Redis(ref sandbox)) = redis_backend("delta_options") {
        assert!(
            RedisCheckpointStore::connect_with(
                &sandbox.url,
                RedisStoreOptions::new()
                    .with_prefix(&sandbox.prefix)
                    .with_deltas(Deltas::new().with_full_every(0)),
            )
            .await
            .is_err()
        );
    }
}
