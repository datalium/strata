//! Value offload (`design/41-Checkpoint-Efficiency.md`, SG-15 part 3): the
//! shared rows fixture, values offloaded once and inherited, each blob store
//! (the store's own database, in memory, the filesystem), a missing and a
//! tampered blob, purge and the Redis ttl, schema version 2, a base row
//! another process replaced, group commit and a durable run, against SQLite,
//! and Postgres and Redis when `STRATA_TEST_POSTGRES_URL` /
//! `STRATA_TEST_REDIS_URL` are set (see `common`). The codec on its own is
//! tested in `src/deltas.rs`, the blob stores in `src/offload.rs`. The xUnit
//! twin is `OffloadTests`.

mod common;

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use common::{PgSandbox, RedisSandbox, backend_url, fixture, unique_name};
use datalium_strata::checkpoint::{
    CheckpointQuery, CheckpointRetention, CheckpointStore, GraphExecutionResult, PurgeStates,
    ThreadQuery,
};
use datalium_strata::graph::{CheckpointPolicy, CompileOptions, StateGraph};
use datalium_strata::schema::{Checkpoint, CheckpointStatus};
use datalium_strata_memory::checkpoint::{InMemoryCheckpointStore, SqliteCheckpointStore};
use datalium_strata_memory::group_commit::{
    BLOB_SIZE_METRIC, BLOBS_METRIC, CHECKPOINT_SIZE_METRIC, ENCODING_LABEL,
};
use datalium_strata_memory::postgres_store::PostgresCheckpointStore;
use datalium_strata_memory::redis_store::RedisCheckpointStore;
use datalium_strata_memory::schema_versions;
use datalium_strata_memory::{
    BlobStore, Deltas, FileSystemBlobStore, GroupCommit, InMemoryBlobStore, Offload,
    PostgresStoreOptions, RedisStoreOptions, SqliteStoreOptions,
};
use metrics_util::debugging::DebuggingRecorder;
use redis::Commands;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const WHOLE: &str = "1.7.0";
const DELTA: &str = "1.8.0";
const OFFLOADED: &str = "1.9.0";

/// The threshold the tests offload from: the payload is over it, the rest
/// under.
const THRESHOLD: usize = 1024;

/// A running checkpoint at `step`, whose `evidence` (2 KiB) is offloaded and
/// never changes unless `evidence` says.
fn running(thread_id: &str, step: u32, evidence: &str) -> Checkpoint {
    Checkpoint {
        schema_version: WHOLE.to_string(),
        thread_id: thread_id.to_string(),
        status: CheckpointStatus::Running,
        suspended_node: Some("A".to_string()),
        next_node: Some("B".to_string()),
        frozen_state: json!({ "step": step, "evidence": { "log": evidence.repeat(2048 / evidence.len().max(1)) } }),
        metadata: Some(HashMap::from([(
            "strata.hops".to_string(),
            step.to_string(),
        )])),
        created_at: format!("2026-10-15T00:{:02}:{:02}Z", step / 60, step % 60),
        resumed_at: None,
        nested: None,
        decision: None,
        wait: None,
        graph: None,
    }
}

fn finished(thread_id: &str, status: CheckpointStatus, created_at: &str) -> Checkpoint {
    Checkpoint {
        status,
        schema_version: "1.4.0".to_string(),
        created_at: created_at.to_string(),
        next_node: None,
        ..running(thread_id, 99, "e")
    }
}

fn offload() -> Offload {
    Offload::new().with_threshold(THRESHOLD)
}

/// A store, as one trait object.
trait Store: CheckpointStore + CheckpointQuery + CheckpointRetention {}

impl<T: CheckpointStore + CheckpointQuery + CheckpointRetention> Store for T {}

/// Where a test's blobs go: the store's own database, or a blob store of its
/// own.
#[derive(Clone)]
enum Blobs {
    Own,
    Memory(Arc<InMemoryBlobStore>),
    Files(Arc<FileSystemBlobStore>),
}

impl Blobs {
    fn memory() -> Self {
        Self::Memory(Arc::new(InMemoryBlobStore::new()))
    }

    fn files() -> Self {
        Self::Files(Arc::new(FileSystemBlobStore::new(
            std::env::temp_dir().join(unique_name("blobs")),
        )))
    }

    fn store(&self) -> Option<Arc<dyn BlobStore>> {
        match self {
            Self::Own => None,
            Self::Memory(store) => Some(store.clone()),
            Self::Files(store) => Some(store.clone()),
        }
    }

    fn offload(&self) -> Offload {
        match self.store() {
            Some(store) => offload().with_blobs(store),
            None => offload(),
        }
    }
}

impl Drop for Blobs {
    fn drop(&mut self) {
        if let Self::Files(store) = self
            && Arc::strong_count(store) == 1
        {
            let _ = std::fs::remove_dir_all(store.root());
        }
    }
}

/// One database (or key prefix) a test opens stores on, and reads and writes
/// as stored.
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
        std::env::temp_dir().join(format!("{}.db", unique_name("offload"))),
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

/// What a store is opened with.
#[derive(Default, Clone)]
struct Options {
    deltas: Option<Deltas>,
    offload: Option<Offload>,
    group: Option<GroupCommit>,
    ttl: Option<Duration>,
}

impl Backend {
    async fn open(&self, options: Options) -> Arc<dyn Store> {
        match self {
            Self::Sqlite(path) => {
                let mut opened = SqliteStoreOptions::new();
                if let Some(deltas) = options.deltas {
                    opened = opened.with_deltas(deltas);
                }
                if let Some(offload) = options.offload {
                    opened = opened.with_offload(offload);
                }
                if let Some(group) = options.group {
                    opened = opened.with_group_commit(group);
                }
                Arc::new(SqliteCheckpointStore::open_with(path, opened).unwrap())
            }
            Self::Postgres(sandbox) => {
                let mut opened = PostgresStoreOptions::new();
                if let Some(deltas) = options.deltas {
                    opened = opened.with_deltas(deltas);
                }
                if let Some(offload) = options.offload {
                    opened = opened.with_offload(offload);
                }
                if let Some(group) = options.group {
                    opened = opened.with_group_commit(group);
                }
                Arc::new(
                    PostgresCheckpointStore::connect_with(&sandbox.url(None), opened)
                        .await
                        .unwrap(),
                )
            }
            Self::Redis(sandbox) => {
                let mut opened = RedisStoreOptions::new().with_prefix(&sandbox.prefix);
                if let Some(deltas) = options.deltas {
                    opened = opened.with_deltas(deltas);
                }
                if let Some(offload) = options.offload {
                    opened = opened.with_offload(offload);
                }
                if let Some(ttl) = options.ttl {
                    opened = opened.with_finished_thread_ttl(ttl);
                }
                Arc::new(
                    RedisCheckpointStore::connect_with(&sandbox.url, opened)
                        .await
                        .unwrap(),
                )
            }
        }
    }

    /// The rows of `thread_id` as stored, oldest first, each as
    /// `{schema_version, frozen_state, nested}`.
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

    /// The thread's blobs in the store's own database, hash to content.
    fn own_blobs(&self, thread_id: &str) -> BTreeMap<String, Vec<u8>> {
        match self {
            Self::Sqlite(path) => {
                let conn = rusqlite::Connection::open(path).unwrap();
                let mut stmt = conn
                    .prepare("SELECT hash, content FROM checkpoint_blobs WHERE thread_id = ?1")
                    .unwrap();
                stmt.query_map([thread_id], |row| Ok((row.get(0)?, row.get(1)?)))
                    .unwrap()
                    .collect::<Result<_, _>>()
                    .unwrap()
            }
            Self::Postgres(sandbox) => sandbox.admin(|client| {
                client
                    .query(
                        "SELECT hash, content FROM checkpoint_blobs WHERE thread_id = $1",
                        &[&thread_id],
                    )
                    .unwrap()
                    .iter()
                    .map(|row| (row.get(0), row.get(1)))
                    .collect()
            }),
            Self::Redis(sandbox) => sandbox
                .connection()
                .hgetall(format!("{}:blobs:{thread_id}", sandbox.prefix))
                .unwrap(),
        }
    }

    /// Stores (or overwrites) a blob in the store's own database, as any other
    /// writer would.
    fn put_own_blob(&self, thread_id: &str, hash: &str, content: &[u8]) {
        match self {
            Self::Sqlite(path) => {
                rusqlite::Connection::open(path)
                    .unwrap()
                    .execute(
                        "INSERT OR REPLACE INTO checkpoint_blobs (thread_id, hash, content) VALUES (?1, ?2, ?3)",
                        rusqlite::params![thread_id, hash, content],
                    )
                    .unwrap();
            }
            Self::Postgres(sandbox) => sandbox.admin(|client| {
                client
                    .execute(
                        "INSERT INTO checkpoint_blobs (thread_id, hash, content) VALUES ($1, $2, $3)
                         ON CONFLICT (thread_id, hash) DO UPDATE SET content = EXCLUDED.content",
                        &[&thread_id, &hash, &content],
                    )
                    .unwrap();
            }),
            Self::Redis(sandbox) => {
                let _: i64 = sandbox
                    .connection()
                    .hset(
                        format!("{}:blobs:{thread_id}", sandbox.prefix),
                        hash,
                        content,
                    )
                    .unwrap();
            }
        }
    }

    /// Deletes a thread's rows and blobs as another process's purge would,
    /// behind every store's back.
    fn delete_thread(&self, thread_id: &str) {
        match self {
            Self::Sqlite(path) => {
                let conn = rusqlite::Connection::open(path).unwrap();
                for table in ["checkpoints", "checkpoint_threads", "checkpoint_blobs"] {
                    conn.execute(
                        &format!("DELETE FROM {table} WHERE thread_id = ?1"),
                        [thread_id],
                    )
                    .unwrap();
                }
            }
            Self::Postgres(sandbox) => sandbox.admin(|client| {
                for table in ["checkpoints", "checkpoint_threads", "checkpoint_blobs"] {
                    client
                        .execute(
                            &format!("DELETE FROM {table} WHERE thread_id = $1"),
                            &[&thread_id],
                        )
                        .unwrap();
                }
            }),
            Self::Redis(sandbox) => {
                let _: i64 = sandbox
                    .connection()
                    .del(&[
                        format!("{}:checkpoints:{thread_id}", sandbox.prefix),
                        format!("{}:blobs:{thread_id}", sandbox.prefix),
                    ])
                    .unwrap();
            }
        }
    }
}

/// The thread's blobs wherever `blobs` keeps them, hash to content.
async fn blobs_of(backend: &Backend, blobs: &Blobs, thread_id: &str) -> BTreeMap<String, Vec<u8>> {
    match blobs {
        Blobs::Own => backend.own_blobs(thread_id),
        Blobs::Memory(store) => {
            let mut found = BTreeMap::new();
            for hash in hashes(backend, thread_id) {
                if let Some(content) = store.get(thread_id, &hash).await.unwrap() {
                    found.insert(hash, content);
                }
            }
            assert_eq!(store.count(thread_id), found.len());
            found
        }
        Blobs::Files(store) => {
            let directory = store.thread_directory(thread_id);
            let Ok(entries) = std::fs::read_dir(&directory) else {
                return BTreeMap::new();
            };
            entries
                .map(|entry| {
                    let path = entry.unwrap().path();
                    (
                        format!("sha256:{}", path.file_name().unwrap().to_string_lossy()),
                        std::fs::read(path).unwrap(),
                    )
                })
                .collect()
        }
    }
}

/// Every hash the thread's rows refer to.
fn hashes(backend: &Backend, thread_id: &str) -> Vec<String> {
    fn collect(value: &Value, found: &mut Vec<String>) {
        match value {
            Value::Object(map) => {
                if let Some(Value::Object(blobs)) = map.get("blobs") {
                    for reference in blobs.values() {
                        found.push(reference["hash"].as_str().unwrap().to_string());
                    }
                }
                for (key, child) in map {
                    if key != "blobs" {
                        collect(child, found);
                    }
                }
            }
            Value::Array(items) => items.iter().for_each(|item| collect(item, found)),
            _ => {}
        }
    }
    let mut found = Vec::new();
    for row in backend.raw(thread_id) {
        collect(&row, &mut found);
    }
    found.sort();
    found.dedup();
    found
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

// --- The shared rows fixture ---

/// `checkpoint-log/offloaded.rows.json`, stored as it is a row at a time with
/// its blobs where `blobs` keeps them: a load after each row gives its
/// checkpoint, the log gives every row's, a page that starts at a delta reads
/// the rows it needs, and a listing gives the version the latest was saved
/// with. The store reading them has offload off (but knows where its blobs
/// are).
async fn shared_rows(backend: Backend, blobs: Blobs) {
    let fixture = fixture("checkpoint-log/offloaded.rows.json");
    let thread_id = fixture["thread_id"].as_str().unwrap();
    let rows = fixture["rows"].as_array().unwrap();
    for (hash, content) in fixture["blobs"].as_object().unwrap() {
        let content = content.as_str().unwrap().as_bytes();
        assert_eq!(
            hash,
            &format!("sha256:{:x}", Sha256::digest(content)),
            "the fixture's own hash"
        );
        match &blobs {
            Blobs::Own => {
                // A store opened first, so the SQL tables exist.
                drop(backend.open(Options::default()).await);
                backend.put_own_blob(thread_id, hash, content);
            }
            other => other
                .store()
                .unwrap()
                .put(thread_id, hash, content.to_vec())
                .await
                .unwrap(),
        }
    }
    let store = backend
        .open(Options {
            offload: blobs
                .store()
                .map(|store| Offload::new().with_threshold(usize::MAX).with_blobs(store)),
            ..Options::default()
        })
        .await;
    let decoded: Vec<Checkpoint> = rows
        .iter()
        .map(|row| serde_json::from_value(row["decoded"].clone()).unwrap())
        .collect();
    for row in rows {
        let sequence = row["sequence"].as_i64().unwrap();
        backend.insert_raw(thread_id, sequence, &row["stored"]);
        let latest = store.load_latest(thread_id).await.unwrap().unwrap();
        assert_eq!(latest.sequence, Some(sequence));
        assert_eq!(latest.checkpoint, decoded[sequence as usize]);
    }

    assert_eq!(
        backend.versions(thread_id),
        [OFFLOADED, OFFLOADED, OFFLOADED, OFFLOADED]
    );
    assert_eq!(log(&*store, thread_id).await, decoded);
    let page = store.get_log(thread_id, Some(1), 1).await.unwrap().entries;
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
    assert_eq!(
        (threads[0].sequence, threads[0].schema_version.as_str()),
        (3, "1.0.0")
    );
}

#[tokio::test]
async fn sqlite_reads_the_shared_offloaded_rows() {
    shared_rows(sqlite().unwrap(), Blobs::Own).await;
}

#[tokio::test]
async fn sqlite_reads_the_shared_offloaded_rows_from_a_filesystem_blob_store() {
    shared_rows(sqlite().unwrap(), Blobs::files()).await;
}

#[tokio::test]
async fn postgres_reads_the_shared_offloaded_rows() {
    if let Some(backend) = postgres("offload_rows") {
        shared_rows(backend, Blobs::Own).await;
    }
}

#[tokio::test]
async fn redis_reads_the_shared_offloaded_rows() {
    if let Some(backend) = redis_backend("offload_rows") {
        shared_rows(backend, Blobs::Own).await;
    }
}

// --- Offloaded once, inherited ---

/// A thread whose 2 KiB evidence never changes, saved six times with deltas
/// and offload on: the evidence is written once, the deltas inherit it, the
/// interrupted row refers to it without writing it again, a changed value is
/// a second blob, and both this store and one with offload off read what was
/// saved. A Postgres or Redis store, and a SQLite one's single saves, record
/// the offloaded rows and the blobs.
async fn offloaded_once(backend: Backend, blobs: Blobs) {
    let recorder: &'static DebuggingRecorder = Box::leak(Box::new(DebuggingRecorder::new()));
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(recorder);

    let options = Options {
        deltas: Some(Deltas::new()),
        offload: Some(blobs.offload()),
        ..Options::default()
    };
    let store = backend.open(options).await;
    let mut saved = Vec::new();
    for step in 0..6 {
        let mut checkpoint = running("t", step, if step < 4 { "e" } else { "f" });
        if step == 3 {
            checkpoint.status = CheckpointStatus::Interrupted;
            checkpoint.schema_version = "1.0.0".to_string();
        }
        store.save(checkpoint.clone()).await.unwrap();
        assert_eq!(store.load("t").await.unwrap().as_ref(), Some(&checkpoint));
        saved.push(checkpoint);
    }

    assert_eq!(
        backend.versions("t"),
        [OFFLOADED, DELTA, DELTA, OFFLOADED, OFFLOADED, DELTA]
    );
    let raw = backend.raw("t");
    assert_eq!(raw[0]["frozen_state"]["state"], json!({ "step": 0 }));
    assert!(raw[0]["frozen_state"]["blobs"]["evidence"]["hash"].is_string());
    assert_eq!(raw[1]["frozen_state"]["set"], json!({ "step": 1 }));
    assert_eq!(
        raw[3]["frozen_state"]["blobs"],
        raw[0]["frozen_state"]["blobs"]
    );
    let stored = blobs_of(&backend, &blobs, "t").await;
    assert_eq!(stored.len(), 2);
    for (hash, content) in &stored {
        assert_eq!(hash, &format!("sha256:{:x}", Sha256::digest(content)));
    }
    assert_eq!(log(&*store, "t").await, saved);
    let reader = backend
        .open(Options {
            offload: blobs
                .store()
                .map(|store| Offload::new().with_threshold(usize::MAX).with_blobs(store)),
            ..Options::default()
        })
        .await;
    assert_eq!(log(&*reader, "t").await, saved);

    let snapshot = snapshotter.snapshot().into_vec();
    let count = |name: &str, label: Option<&str>| {
        snapshot
            .iter()
            .filter(|(key, _, _, _)| {
                key.key().name() == name
                    && label.is_none_or(|label| {
                        key.key()
                            .labels()
                            .any(|l| l.key() == ENCODING_LABEL && l.value() == label)
                    })
            })
            .count()
    };
    assert!(count(BLOBS_METRIC, None) > 0, "no blob was counted");
    assert!(count(BLOB_SIZE_METRIC, None) > 0);
    assert!(count(CHECKPOINT_SIZE_METRIC, Some("offloaded")) > 0);
}

#[tokio::test]
async fn sqlite_offloads_a_value_once_into_its_own_file() {
    offloaded_once(sqlite().unwrap(), Blobs::Own).await;
}

#[tokio::test]
async fn sqlite_offloads_a_value_once_into_a_blob_store_in_memory() {
    offloaded_once(sqlite().unwrap(), Blobs::memory()).await;
}

#[tokio::test]
async fn sqlite_offloads_a_value_once_into_a_filesystem_blob_store() {
    offloaded_once(sqlite().unwrap(), Blobs::files()).await;
}

#[tokio::test]
async fn postgres_offloads_a_value_once_into_its_own_database() {
    if let Some(backend) = postgres("offload_once") {
        offloaded_once(backend, Blobs::Own).await;
    }
}

#[tokio::test]
async fn postgres_offloads_a_value_once_into_a_filesystem_blob_store() {
    if let Some(backend) = postgres("offload_files") {
        offloaded_once(backend, Blobs::files()).await;
    }
}

#[tokio::test]
async fn redis_offloads_a_value_once_into_its_own_hash() {
    if let Some(backend) = redis_backend("offload_once") {
        offloaded_once(backend, Blobs::Own).await;
    }
}

#[tokio::test]
async fn redis_offloads_a_value_once_into_a_blob_store_in_memory() {
    if let Some(backend) = redis_backend("offload_memory") {
        offloaded_once(backend, Blobs::memory()).await;
    }
}

// --- A missing and a tampered blob ---

/// A row whose blob is gone, or whose blob no longer matches its hash, fails
/// the load with a message that names the row, the blob and the key.
async fn missing_or_tampered(backend: Backend) {
    let store = backend
        .open(Options {
            offload: Some(offload()),
            ..Options::default()
        })
        .await;
    store.save(running("t", 0, "e")).await.unwrap();
    let hash = hashes(&backend, "t").pop().unwrap();

    backend.put_own_blob("t", &hash, br#"{"log":"tampered"}"#);
    let mismatch = store.load("t").await.unwrap_err();
    assert_eq!(
        mismatch.0,
        format!(
            "checkpoint 't' #0 refers to blob {hash} at 'evidence', whose content doesn't match"
        )
    );

    match &backend {
        Backend::Sqlite(path) => {
            rusqlite::Connection::open(path)
                .unwrap()
                .execute("DELETE FROM checkpoint_blobs", [])
                .unwrap();
        }
        Backend::Postgres(sandbox) => {
            sandbox.admin(|client| client.execute("DELETE FROM checkpoint_blobs", &[]).unwrap());
        }
        Backend::Redis(sandbox) => {
            let _: i64 = sandbox
                .connection()
                .del(format!("{}:blobs:t", sandbox.prefix))
                .unwrap();
        }
    }
    let missing = store.load("t").await.unwrap_err();
    assert_eq!(
        missing.0,
        format!(
            "checkpoint 't' #0 refers to blob {hash} at 'evidence', which the blob store doesn't have"
        )
    );
    assert!(store.get_log("t", None, 10).await.is_err());
}

#[tokio::test]
async fn sqlite_refuses_a_missing_or_tampered_blob() {
    missing_or_tampered(sqlite().unwrap()).await;
}

#[tokio::test]
async fn postgres_refuses_a_missing_or_tampered_blob() {
    if let Some(backend) = postgres("offload_tampered") {
        missing_or_tampered(backend).await;
    }
}

#[tokio::test]
async fn redis_refuses_a_missing_or_tampered_blob() {
    if let Some(backend) = redis_backend("offload_tampered") {
        missing_or_tampered(backend).await;
    }
}

#[tokio::test]
async fn a_blob_missing_from_a_filesystem_blob_store_fails_the_load() {
    let backend = sqlite().unwrap();
    let blobs = Blobs::files();
    let store = backend
        .open(Options {
            offload: Some(blobs.offload()),
            ..Options::default()
        })
        .await;
    store.save(running("t", 0, "e")).await.unwrap();
    let Blobs::Files(files) = &blobs else {
        unreachable!()
    };
    std::fs::remove_dir_all(files.thread_directory("t")).unwrap();
    let missing = store.load("t").await.unwrap_err();
    assert!(
        missing.0.ends_with("which the blob store doesn't have"),
        "{missing:?}"
    );
}

// --- Retention ---

/// A purge deletes a finished thread's blobs with its rows, wherever they
/// are, and leaves another thread's.
async fn purged(backend: Backend, blobs: Blobs) {
    let store = backend
        .open(Options {
            offload: Some(blobs.offload()),
            ..Options::default()
        })
        .await;
    store.save(running("done", 0, "e")).await.unwrap();
    store
        .save(Checkpoint {
            frozen_state: json!({ "evidence": { "log": "e".repeat(2048) } }),
            ..finished("done", CheckpointStatus::Completed, "2026-09-01T00:00:00Z")
        })
        .await
        .unwrap();
    store.save(running("kept", 0, "k")).await.unwrap();
    assert_eq!(blobs_of(&backend, &blobs, "done").await.len(), 1);

    let purged = store
        .purge(Utc::now(), &PurgeStates::finished(), 10)
        .await
        .unwrap();

    assert_eq!(purged, 1);
    assert!(blobs_of(&backend, &blobs, "done").await.is_empty());
    assert_eq!(blobs_of(&backend, &blobs, "kept").await.len(), 1);
    assert!(store.load("kept").await.unwrap().is_some());
}

#[tokio::test]
async fn sqlite_purges_a_threads_blobs() {
    purged(sqlite().unwrap(), Blobs::Own).await;
}

#[tokio::test]
async fn sqlite_purges_a_threads_blobs_from_a_filesystem_blob_store() {
    purged(sqlite().unwrap(), Blobs::files()).await;
}

#[tokio::test]
async fn postgres_purges_a_threads_blobs() {
    if let Some(backend) = postgres("offload_purge") {
        purged(backend, Blobs::Own).await;
    }
}

#[tokio::test]
async fn postgres_purges_a_threads_blobs_from_a_blob_store_in_memory() {
    if let Some(backend) = postgres("offload_purge_memory") {
        purged(backend, Blobs::memory()).await;
    }
}

#[tokio::test]
async fn redis_purges_a_threads_blobs() {
    if let Some(backend) = redis_backend("offload_purge") {
        purged(backend, Blobs::Own).await;
    }
}

#[tokio::test]
async fn redis_expires_a_finished_threads_blob_hash_with_its_list() {
    let Some(backend) = redis_backend("offload_ttl") else {
        return;
    };
    let Backend::Redis(sandbox) = &backend else {
        unreachable!()
    };
    let store = backend
        .open(Options {
            offload: Some(offload()),
            ttl: Some(Duration::from_secs(600)),
            ..Options::default()
        })
        .await;
    let ttl = |key: &str| -> i64 {
        sandbox
            .connection()
            .pttl(format!("{}:{key}", sandbox.prefix))
            .unwrap()
    };
    store.save(running("t", 0, "e")).await.unwrap();
    assert_eq!(ttl("blobs:t"), -1, "a running thread's blobs don't expire");
    store
        .save(finished(
            "t",
            CheckpointStatus::Completed,
            "2026-10-15T01:00:00Z",
        ))
        .await
        .unwrap();
    assert!(ttl("blobs:t") > 0 && ttl("checkpoints:t") > 0);
    store.save(running("t", 2, "e")).await.unwrap();
    assert_eq!(ttl("blobs:t"), -1, "saved again, they stop expiring");
}

// --- Schema version 2 ---

#[tokio::test]
async fn a_version_one_sqlite_file_gains_the_blob_table() {
    let backend = sqlite().unwrap();
    let Backend::Sqlite(path) = &backend else {
        unreachable!()
    };
    drop(SqliteCheckpointStore::open(path).unwrap());
    // As a build from before offload left it.
    rusqlite::Connection::open(path)
        .unwrap()
        .execute_batch(
            "DROP TABLE checkpoint_blobs;
             UPDATE strata_schema_version SET version = 1 WHERE component = 'checkpoints'",
        )
        .unwrap();

    drop(SqliteCheckpointStore::open(path).unwrap());

    let conn = rusqlite::Connection::open(path).unwrap();
    let version: i64 = conn
        .query_row(
            "SELECT version FROM strata_schema_version WHERE component = 'checkpoints'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, schema_versions::CHECKPOINTS_VERSION);
    let columns: Vec<Value> = conn
        .prepare("PRAGMA table_info(checkpoint_blobs)")
        .unwrap()
        .query_map([], |row| {
            Ok(json!({
                "name": row.get::<_, String>(1)?,
                "type": row.get::<_, String>(2)?,
                "notnull": row.get::<_, i64>(3)? != 0,
                "pk": row.get::<_, i64>(5)?,
            }))
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        Value::Array(columns),
        fixture("checkpoint-log/blobs-columns.json")
    );
}

#[tokio::test]
async fn a_version_one_postgres_database_gains_the_blob_table() {
    let Some(backend) = postgres("offload_v2") else {
        return;
    };
    let Backend::Postgres(sandbox) = &backend else {
        unreachable!()
    };
    drop(
        PostgresCheckpointStore::from_client(sandbox.client().await)
            .await
            .unwrap(),
    );
    sandbox.admin(|client| {
        client
            .batch_execute(
                "DROP TABLE checkpoint_blobs;
                 UPDATE strata_schema_version SET version = 1 WHERE component = 'checkpoints'",
            )
            .unwrap()
    });

    drop(
        PostgresCheckpointStore::from_client(sandbox.client().await)
            .await
            .unwrap(),
    );

    let (version, columns) = sandbox.admin(|client| {
        let version: i64 = client
            .query_one(
                "SELECT version FROM strata_schema_version WHERE component = 'checkpoints'",
                &[],
            )
            .unwrap()
            .get(0);
        let columns: Vec<Value> = client
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
                       AND tc.table_schema = current_schema() AND tc.table_name = 'checkpoint_blobs'
                 ) k ON k.column_name = c.column_name
                 WHERE c.table_schema = current_schema() AND c.table_name = 'checkpoint_blobs'
                 ORDER BY c.ordinal_position",
                &[],
            )
            .unwrap()
            .iter()
            .map(|row| {
                json!({
                    "name": row.get::<_, String>(0),
                    "type": row.get::<_, String>(1),
                    "notnull": row.get::<_, bool>(2),
                    "pk": row.get::<_, i32>(3),
                })
            })
            .collect();
        (version, columns)
    });
    assert_eq!(version, schema_versions::CHECKPOINTS_VERSION);
    assert_eq!(
        Value::Array(columns),
        fixture("checkpoint-log/postgres-blobs-columns.json")
    );
}

// --- A base row another process replaced ---

/// A store remembers the thread's latest row (a delta's base, and the blobs
/// it needn't write again). Behind its back the thread is purged and run again
/// by another process up to the same sequence. Its next save must not take
/// that row for the one it remembers: it is written whole, with its blob
/// written again, and the thread reads back as saved
/// (`design/41-Checkpoint-Efficiency.md`, PR 3: the base row's token).
async fn base_replaced(backend: Backend) {
    let store = backend
        .open(Options {
            deltas: Some(Deltas::new()),
            offload: Some(offload()),
            ..Options::default()
        })
        .await;
    store.save(running("t", 0, "e")).await.unwrap();
    store.save(running("t", 1, "e")).await.unwrap();

    backend.delete_thread("t");
    let other = backend.open(Options::default()).await;
    other.save(running("t", 40, "x")).await.unwrap();
    other.save(running("t", 41, "x")).await.unwrap();

    store.save(running("t", 2, "e")).await.unwrap();

    let versions = backend.versions("t");
    assert_eq!(versions[..2], [WHOLE, WHOLE]);
    assert_eq!(
        versions[2], OFFLOADED,
        "written whole, its blob written again"
    );
    assert!(backend.raw("t")[2]["frozen_state"]["delta_of"].is_null());
    assert_eq!(
        log(&*store, "t").await,
        [
            running("t", 40, "x"),
            running("t", 41, "x"),
            running("t", 2, "e")
        ]
    );

    // Offload alone: a save that isn't a delta but leaves out a blob it
    // remembers relies on the row before it just the same.
    let offloading = backend
        .open(Options {
            offload: Some(offload()),
            ..Options::default()
        })
        .await;
    offloading.save(running("u", 0, "e")).await.unwrap();
    backend.delete_thread("u");
    other.save(running("u", 40, "x")).await.unwrap();
    offloading.save(running("u", 1, "e")).await.unwrap();
    assert_eq!(backend.versions("u"), [WHOLE, OFFLOADED]);
    assert_eq!(
        log(&*offloading, "u").await,
        [running("u", 40, "x"), running("u", 1, "e")]
    );
}

#[tokio::test]
async fn sqlite_does_not_rely_on_a_row_another_process_replaced() {
    base_replaced(sqlite().unwrap()).await;
}

#[tokio::test]
async fn postgres_does_not_rely_on_a_row_another_process_replaced() {
    if let Some(backend) = postgres("offload_replaced") {
        base_replaced(backend).await;
    }
}

#[tokio::test]
async fn redis_does_not_rely_on_a_row_another_process_replaced() {
    if let Some(backend) = redis_backend("offload_replaced") {
        base_replaced(backend).await;
    }
}

// --- Group commit ---

/// Concurrent saves of several threads through group commit, with offload:
/// each thread's evidence is written once, and every log reads back.
async fn group_commit(backend: Backend, blobs: Blobs) {
    let store = backend
        .open(Options {
            deltas: Some(Deltas::new()),
            offload: Some(blobs.offload()),
            group: Some(GroupCommit::new()),
            ..Options::default()
        })
        .await;
    let threads: Vec<String> = (0..8).map(|n| format!("g{n}")).collect();
    for step in 0..4 {
        let saves: Vec<_> = threads
            .iter()
            .map(|thread| {
                let (store, checkpoint) = (store.clone(), running(thread, step, "e"));
                tokio::spawn(async move { store.save(checkpoint).await })
            })
            .collect();
        for save in saves {
            save.await.unwrap().unwrap();
        }
    }
    for thread in &threads {
        assert_eq!(blobs_of(&backend, &blobs, thread).await.len(), 1);
        assert_eq!(
            log(&*store, thread).await,
            (0..4)
                .map(|step| running(thread, step, "e"))
                .collect::<Vec<_>>()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_offloads_through_group_commit() {
    group_commit(sqlite().unwrap(), Blobs::Own).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_offloads_through_group_commit_into_a_filesystem_blob_store() {
    group_commit(sqlite().unwrap(), Blobs::files()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_offloads_through_group_commit() {
    if let Some(backend) = postgres("offload_flush") {
        group_commit(backend, Blobs::Own).await;
    }
}

// --- A durable run ---

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Counter {
    step: u32,
    payload: String,
}

/// A graph that checkpoints every node, run against a store with deltas and
/// offload: the payload is written once, and the log decodes to what the same
/// run leaves in the in-memory store.
async fn durable_run(backend: Backend) {
    let mut graph = StateGraph::new();
    for node in 1..=6 {
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
        .open(Options {
            deltas: Some(Deltas::new()),
            offload: Some(offload()),
            ..Options::default()
        })
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

    assert_eq!(backend.own_blobs("run").len(), 1);
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
async fn sqlite_keeps_a_durable_run_offloaded() {
    durable_run(sqlite().unwrap()).await;
}

#[tokio::test]
async fn postgres_keeps_a_durable_run_offloaded() {
    if let Some(backend) = postgres("offload_run") {
        durable_run(backend).await;
    }
}

#[tokio::test]
async fn redis_keeps_a_durable_run_offloaded() {
    if let Some(backend) = redis_backend("offload_run") {
        durable_run(backend).await;
    }
}

#[tokio::test]
async fn invalid_options_are_refused_by_every_constructor() {
    let zero = || Offload::new().with_threshold(0);
    assert!(
        SqliteCheckpointStore::open_in_memory_with(SqliteStoreOptions::new().with_offload(zero()))
            .is_err()
    );
    if let Some(Backend::Postgres(ref sandbox)) = postgres("offload_options") {
        assert!(
            PostgresCheckpointStore::connect_with(
                &sandbox.url(None),
                PostgresStoreOptions::new().with_offload(zero()),
            )
            .await
            .is_err()
        );
    }
    if let Some(Backend::Redis(ref sandbox)) = redis_backend("offload_options") {
        assert!(
            RedisCheckpointStore::connect_with(
                &sandbox.url,
                RedisStoreOptions::new()
                    .with_prefix(&sandbox.prefix)
                    .with_offload(zero()),
            )
            .await
            .is_err()
        );
    }
}
