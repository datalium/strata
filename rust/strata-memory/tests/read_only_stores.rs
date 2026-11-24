//! Opening a checkpoint store read-only (`design/51-Read-Only-Store-Open.md`,
//! FR-MP-12): `SqliteCheckpointStore::open_read_only`,
//! `PostgresCheckpointStore::connect_read_only` and
//! `RedisCheckpointStore::connect_read_only`. Opening creates nothing and
//! never migrates, reads never write, and every write is refused, by the
//! store and, where the database can, by the database too.
//!
//! The central test is per store kind: seed a store with
//! `checkpoint-query/cases.json`, an offloaded log and a held lease, snapshot
//! it, open it read-only, run every read, and compare. The messages come from
//! `schema/fixtures/checkpoint-log/read-only-errors.json`, shared with the
//! .NET engine and the CLI. Postgres and Redis need `STRATA_TEST_POSTGRES_URL`
//! / `STRATA_TEST_REDIS_URL` (see `common`); each test has its own schema or
//! key prefix. The Postgres reads run as `strata_reader_rust`, a role granted
//! only `CONNECT` and `SELECT`, created here if it isn't there.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use chrono::Utc;
use common::{
    POSTGRES_VAR, PgSandbox, REDIS_VAR, RedisSandbox, backend_url, fixture, off_runtime,
    query_seed, redis_connection, run_query_cases, save_query_seed, summary_json, thread_query,
    unique_name,
};
use datalium_strata::checkpoint::{
    CheckpointLogEntry, CheckpointQuery, CheckpointRetention, CheckpointStore,
    CheckpointStoreError, LatestCheckpoint, PurgeStates, ThreadLease, ThreadQuery,
};
use datalium_strata::schema::{Checkpoint, CheckpointStatus};
use datalium_strata_memory::checkpoint::SqliteCheckpointStore;
use datalium_strata_memory::postgres_store::{
    POSTGRES_CHECKPOINT_LOG_DDL, PostgresCheckpointStore,
};
use datalium_strata_memory::redis_store::RedisCheckpointStore;
use datalium_strata_memory::{
    FileSystemBlobStore, Offload, PostgresStoreOptions, RedisStoreOptions, SqliteStoreOptions,
    schema_versions,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// A store, as one trait object.
trait Store: CheckpointStore + CheckpointQuery + CheckpointRetention {}

impl<T: CheckpointStore + CheckpointQuery + CheckpointRetention> Store for T {}

// --- The shared messages ---

fn errors() -> Value {
    fixture("checkpoint-log/read-only-errors.json")
}

/// The open error `case` of the fixture, its `{name}`s replaced by `values`.
fn open_error(case: &str, values: &[(&str, &str)]) -> String {
    let mut message = errors()["open"][case]
        .as_str()
        .unwrap_or_else(|| panic!("read-only-errors.json has no open.{case}"))
        .to_string();
    for (name, value) in values {
        message = message.replace(&format!("{{{name}}}"), value);
    }
    assert!(
        !message.contains('{'),
        "open.{case} has a name the test didn't fill: {message}"
    );
    message
}

/// What a read-only store refuses `operation` with.
fn write_refused(operation: &str) -> String {
    errors()["write"]["template"]
        .as_str()
        .unwrap()
        .replace("{operation}", operation)
}

fn known() -> String {
    schema_versions::CHECKPOINTS_VERSION.to_string()
}

fn refusal<T>(result: Result<T, CheckpointStoreError>) -> String {
    match result {
        Ok(_) => panic!("a read-only store must refuse this"),
        Err(err) => err.0,
    }
}

// --- The seed ---

/// The thread whose log has values offloaded.
const OFFLOADED: &str = "offloaded-evidence";

/// The threshold the offloaded log is written with: its evidence is over it.
const THRESHOLD: usize = 1024;

/// The thread `worker-2` holds.
const HELD: &str = "approve-acme-1";

/// A log whose `evidence` (2 KiB) is offloaded: three running checkpoints,
/// then a completed one.
fn offloaded_log() -> Vec<Checkpoint> {
    (0..4)
        .map(|step| Checkpoint {
            schema_version: datalium_strata::schema::SCHEMA_VERSION.to_string(),
            thread_id: OFFLOADED.to_string(),
            status: if step == 3 {
                CheckpointStatus::Completed
            } else {
                CheckpointStatus::Running
            },
            suspended_node: Some(format!("N{step}")),
            next_node: (step < 3).then(|| format!("N{}", step + 1)),
            frozen_state: json!({ "step": step, "evidence": "e".repeat(2048) }),
            metadata: None,
            created_at: format!("2026-10-02T00:00:0{step}Z"),
            resumed_at: None,
            nested: None,
            decision: None,
            wait: None,
            graph: None,
        })
        .collect()
}

fn offload(blobs: Option<Arc<FileSystemBlobStore>>) -> Offload {
    let offload = Offload::new().with_threshold(THRESHOLD);
    match blobs {
        Some(blobs) => offload.with_blobs(blobs),
        None => offload,
    }
}

/// Every thread the seed has.
fn seeded_threads() -> Vec<String> {
    let mut ids: BTreeSet<String> = query_seed().into_iter().map(|c| c.thread_id).collect();
    ids.insert(OFFLOADED.to_string());
    ids.into_iter().collect()
}

/// Seeds `plain` with the query seed and a held lease, and `offloading` (a
/// store on the same database that offloads) with the offloaded log.
async fn seed(plain: &dyn Store, offloading: &dyn Store) {
    save_query_seed(plain).await;
    for checkpoint in offloaded_log() {
        offloading.save(checkpoint).await.unwrap();
    }
    let held = plain
        .try_acquire_lease(HELD, "worker-2", Duration::from_secs(3600))
        .await
        .unwrap();
    assert!(held.acquired().is_some(), "the seed's lease was taken");
}

/// Everything a store says about one thread.
#[derive(Debug, Clone, PartialEq)]
struct ThreadReads {
    load: Option<Checkpoint>,
    latest: Option<LatestCheckpoint>,
    log: Vec<CheckpointLogEntry>,
    lease: Option<(String, String, i64)>,
}

/// Everything a store says: every listing of `checkpoint-query/cases.json`
/// and of every thread, followed to the end, and every read of every thread
/// in `thread_ids`.
#[derive(Debug, Clone, PartialEq)]
struct Reads {
    listings: Vec<Value>,
    threads: BTreeMap<String, ThreadReads>,
}

async fn reads(store: &dyn Store, thread_ids: &[String]) -> Reads {
    let mut queries: Vec<(String, ThreadQuery)> = fixture("checkpoint-query/cases.json")["list"]
        .as_array()
        .unwrap()
        .iter()
        .map(|case| {
            (
                case["name"].as_str().unwrap().to_string(),
                thread_query(&case["query"]),
            )
        })
        .collect();
    queries.push((
        "every thread".to_string(),
        ThreadQuery::default().with_limit(1000),
    ));
    let mut listings = Vec::new();
    for (name, mut query) in queries {
        let mut listed = Vec::new();
        loop {
            let page = store.list_threads(&query).await.unwrap();
            listed.extend(page.threads.iter().map(summary_json));
            match page.next_cursor {
                Some(cursor) => query = query.with_cursor(cursor),
                None => break,
            }
        }
        listings.push(json!({ "query": name, "listed": listed }));
    }
    Reads {
        listings,
        threads: thread_reads(store, thread_ids).await,
    }
}

async fn thread_reads(store: &dyn Store, thread_ids: &[String]) -> BTreeMap<String, ThreadReads> {
    let mut all = BTreeMap::new();
    for thread_id in thread_ids {
        let lease = store.get_lease(thread_id).await.unwrap();
        if let Some(lease) = &lease {
            // On the store's clock, which Redis reads as now plus what is
            // left: a millisecond either way.
            let left = lease.expires_at - Utc::now();
            assert!(left > chrono::Duration::minutes(50), "{thread_id}: {left}");
        }
        all.insert(
            thread_id.clone(),
            ThreadReads {
                load: store.load(thread_id).await.unwrap(),
                latest: store.load_latest(thread_id).await.unwrap(),
                log: store.get_log(thread_id, None, 1000).await.unwrap().entries,
                lease: lease.map(|lease: ThreadLease| (lease.thread_id, lease.owner, lease.token)),
            },
        );
    }
    all
}

/// The writes only one kind of store has: `PostgresCheckpointStore::migrate`
/// and `RedisCheckpointStore::rebuild_index`.
const STORE_OWN_WRITES: [&str; 2] = ["migrate", "rebuild index"];

/// Every write a store has, each refused with the shared message: the
/// store traits' writes, then `own`, the refusals of the store's own write
/// methods by operation. The operations named are the fixture's, but for the
/// other stores' own writes.
async fn assert_every_write_refused(store: &dyn Store, own: Vec<(&str, String)>) {
    let checkpoint = query_seed().remove(0);
    let thread_id = checkpoint.thread_id.clone();
    let lease = ThreadLease {
        thread_id: thread_id.clone(),
        owner: "worker-1".to_string(),
        token: 1,
        expires_at: Utc::now() + chrono::Duration::hours(1),
    };
    let ttl = Duration::from_secs(60);
    let attempts = [
        ("save", refusal(store.save(checkpoint.clone()).await)),
        (
            "save",
            refusal(store.save_if_latest(checkpoint.clone(), Some(0)).await),
        ),
        (
            "save",
            refusal(store.save_if_latest(checkpoint.clone(), None).await),
        ),
        (
            "save",
            refusal(
                store
                    .save_under_lease(checkpoint.clone(), &lease, None)
                    .await,
            ),
        ),
        (
            "save",
            refusal(
                store
                    .save_under_lease(checkpoint.clone(), &lease, Some(0))
                    .await,
            ),
        ),
        (
            "acquire lease",
            refusal(store.try_acquire_lease(&thread_id, "worker-1", ttl).await),
        ),
        ("renew lease", refusal(store.renew_lease(&lease, ttl).await)),
        ("release lease", refusal(store.release_lease(&lease).await)),
        (
            "purge",
            refusal(store.purge(Utc::now(), &PurgeStates::finished(), 10).await),
        ),
    ];
    let mut named = BTreeSet::new();
    for (operation, message) in attempts.into_iter().chain(own) {
        assert_eq!(message, write_refused(operation));
        named.insert(operation.to_string());
    }
    let shared: BTreeSet<String> =
        serde_json::from_value(errors()["write"]["operations"].clone()).unwrap();
    assert!(
        named.is_subset(&shared),
        "an operation the fixture doesn't name: {named:?}"
    );
    let missing: Vec<&String> = shared.difference(&named).collect();
    assert!(
        missing
            .iter()
            .all(|operation| STORE_OWN_WRITES.contains(&operation.as_str())),
        "the fixture's operations this store wasn't tried with: {missing:?}"
    );
}

// --- SQLite ---

/// Every file in `dir` and below, with its SHA-256: a file created, removed or
/// changed shows.
fn files(dir: &Path) -> BTreeMap<String, String> {
    fn walk(root: &Path, dir: &Path, found: &mut BTreeMap<String, String>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            let name = path
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            if path.is_dir() {
                found.insert(format!("{name}/"), String::new());
                walk(root, &path, found);
            } else {
                found.insert(name, sha256(&std::fs::read(&path).unwrap()));
            }
        }
    }
    let mut found = BTreeMap::new();
    walk(dir, dir, &mut found);
    found
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A directory of its own, removed when the test ends.
struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let dir = std::env::temp_dir().join(unique_name(label));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        #[cfg(unix)]
        restore_permissions(&self.0);
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Makes `dir` and every file in it read-only.
#[cfg(unix)]
fn lock_down(dir: &Path) {
    use std::os::unix::fs::PermissionsExt;
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444)).unwrap();
    }
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o555)).unwrap();
}

#[cfg(unix)]
fn restore_permissions(dir: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o755));
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let _ = std::fs::set_permissions(entry.path(), std::fs::Permissions::from_mode(0o644));
        }
    }
}

/// A seeded SQLite file in a directory of its own, and what the writable
/// store read from it.
async fn seeded_sqlite(label: &str) -> (TempDir, PathBuf, Reads) {
    let dir = TempDir::new(label);
    let path = dir.path().join("checkpoints.sqlite");
    let plain = SqliteCheckpointStore::open(&path).unwrap();
    let offloading = SqliteCheckpointStore::open_with(
        &path,
        SqliteStoreOptions::new().with_offload(offload(None)),
    )
    .unwrap();
    seed(&plain, &offloading).await;
    let expected = reads(&plain, &seeded_threads()).await;
    assert!(
        expected.threads[OFFLOADED].log.len() == 4,
        "the offloaded log was written"
    );
    (dir, path, expected)
}

/// What SQLite itself refuses on a read-only store's connection: a write to
/// the file (its read-only flag) and a temporary table, which only
/// `query_only` stops.
async fn assert_sqlite_refuses_writes_itself(store: &SqliteCheckpointStore) {
    for sql in [
        "INSERT INTO strata_schema_version (component, version) VALUES ('probe', 1)",
        "CREATE TABLE probe (x)",
        "CREATE TEMP TABLE probe (x)",
    ] {
        let refused = store.execute_unguarded(sql).await;
        assert!(
            refused
                .as_ref()
                .is_err_and(|err| err.0.contains("readonly")),
            "{sql}: {refused:?}"
        );
    }
}

/// The central SQLite test, with the file and its directory read-only or not.
async fn sqlite_changes_nothing(locked: bool) {
    let (dir, path, expected) = seeded_sqlite("ro_sqlite").await;
    #[cfg(unix)]
    if locked {
        lock_down(dir.path());
    }
    assert!(
        !locked || cfg!(unix),
        "only Unix can lock the directory down"
    );
    let before = files(dir.path());

    let store = SqliteCheckpointStore::open_read_only(&path, SqliteStoreOptions::new()).unwrap();
    assert!(store.is_read_only());
    assert_eq!(reads(&store, &seeded_threads()).await, expected);
    assert_every_write_refused(&store, Vec::new()).await;
    assert_sqlite_refuses_writes_itself(&store).await;
    assert_eq!(files(dir.path()), before, "while the store is open");
    drop(store);

    let after = files(dir.path());
    assert_eq!(after, before);
    assert!(
        after.keys().all(|name| name == "checkpoints.sqlite"),
        "no -journal, -wal or -shm: {after:?}"
    );
}

#[tokio::test]
async fn a_read_only_sqlite_store_reads_what_the_writable_one_does_and_changes_nothing() {
    sqlite_changes_nothing(false).await;
}

#[cfg(unix)]
#[tokio::test]
async fn a_read_only_sqlite_store_reads_a_file_and_directory_made_read_only() {
    sqlite_changes_nothing(true).await;
}

#[tokio::test]
async fn a_writable_sqlite_store_is_not_read_only() {
    assert!(
        !SqliteCheckpointStore::open_in_memory()
            .unwrap()
            .is_read_only()
    );
}

#[tokio::test]
async fn every_write_on_a_read_only_sqlite_store_is_refused() {
    let dir = TempDir::new("ro_sqlite_writes");
    let path = dir.path().join("checkpoints.sqlite");
    save_query_seed(&SqliteCheckpointStore::open(&path).unwrap()).await;
    let before = files(dir.path());
    let store = SqliteCheckpointStore::open_read_only(&path, SqliteStoreOptions::new()).unwrap();
    assert_every_write_refused(&store, Vec::new()).await;
    drop(store);
    assert_eq!(files(dir.path()), before);
}

#[tokio::test]
async fn sqlite_itself_refuses_a_write_on_a_read_only_stores_connection() {
    let dir = TempDir::new("ro_sqlite_enforced");
    let path = dir.path().join("checkpoints.sqlite");
    drop(SqliteCheckpointStore::open(&path).unwrap());
    let before = files(dir.path());
    let store = SqliteCheckpointStore::open_read_only(&path, SqliteStoreOptions::new()).unwrap();
    assert_sqlite_refuses_writes_itself(&store).await;
    drop(store);
    assert_eq!(files(dir.path()), before);
}

#[tokio::test]
async fn a_missing_sqlite_file_is_refused_and_not_created() {
    let dir = TempDir::new("ro_sqlite_missing");
    let path = dir.path().join("missing.db");
    let refused = refusal(SqliteCheckpointStore::open_read_only(
        &path,
        SqliteStoreOptions::new(),
    ));
    assert_eq!(
        refused,
        open_error(
            "missing_sqlite_file",
            &[("path", &path.display().to_string())]
        )
    );
    assert!(files(dir.path()).is_empty(), "nothing was created");
}

/// Opens `path` read-only, expecting `message`, and checks the directory is
/// as it was.
fn assert_sqlite_refused(dir: &Path, path: &Path, options: SqliteStoreOptions, message: String) {
    let before = files(dir);
    let refused = refusal(SqliteCheckpointStore::open_read_only(path, options));
    assert_eq!(refused, message);
    assert_eq!(files(dir), before, "the refusal changed nothing");
}

#[tokio::test]
async fn a_sqlite_file_with_no_strata_log_is_refused() {
    let dir = TempDir::new("ro_sqlite_no_log");
    let path = dir.path().join("other.db");
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("CREATE TABLE invoices (id INTEGER PRIMARY KEY)")
        .unwrap();
    let shown = path.display().to_string();
    assert_sqlite_refused(
        dir.path(),
        &path,
        SqliteStoreOptions::new(),
        open_error("no_strata_log", &[("store", &shown)]),
    );
}

#[tokio::test]
async fn a_sqlite_log_at_version_0_is_refused_and_not_migrated() {
    let dir = TempDir::new("ro_sqlite_v0");
    let path = dir.path().join("old.db");
    // A log from before the version table: `checkpoints` alone.
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TABLE checkpoints (
                thread_id TEXT NOT NULL, sequence INTEGER NOT NULL,
                schema_version TEXT NOT NULL, status TEXT NOT NULL,
                suspended_node TEXT, next_node TEXT, frozen_state TEXT NOT NULL,
                metadata TEXT, created_at TEXT NOT NULL, resumed_at TEXT,
                PRIMARY KEY (thread_id, sequence)
            );
            INSERT INTO checkpoints VALUES
                ('old_thread', 0, '1.0.0', 'interrupted', 'CallLLM', 'Verify', '{}', NULL,
                 '2026-09-16T00:00:00Z', NULL);",
        )
        .unwrap();
    assert_sqlite_refused(
        dir.path(),
        &path,
        SqliteStoreOptions::new(),
        open_error("version_zero", &[("known", &known())]),
    );
    let tables: i64 = rusqlite::Connection::open(&path)
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'strata_schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(tables, 0, "no version table was created");
}

#[tokio::test]
async fn a_sqlite_log_newer_than_the_build_is_refused_and_left_alone() {
    let dir = TempDir::new("ro_sqlite_newer");
    let path = dir.path().join("newer.db");
    drop(SqliteCheckpointStore::open(&path).unwrap());
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(
            "UPDATE strata_schema_version SET version = 7 WHERE component = 'checkpoints'",
        )
        .unwrap();
    assert_sqlite_refused(
        dir.path(),
        &path,
        SqliteStoreOptions::new(),
        open_error("newer_version", &[("found", "7"), ("known", &known())]),
    );
}

#[tokio::test]
async fn a_missing_blob_directory_is_refused_and_not_created() {
    let dir = TempDir::new("ro_sqlite_blobs");
    let path = dir.path().join("checkpoints.sqlite");
    drop(SqliteCheckpointStore::open(&path).unwrap());
    let blobs = dir.path().join("blobs");
    assert_sqlite_refused(
        dir.path(),
        &path,
        SqliteStoreOptions::new()
            .with_offload(offload(Some(Arc::new(FileSystemBlobStore::new(&blobs))))),
        open_error(
            "missing_blob_directory",
            &[("dir", &blobs.display().to_string())],
        ),
    );
    assert!(!blobs.exists());
}

#[tokio::test]
async fn a_sqlite_file_with_an_unfinished_write_is_refused_and_left_alone() {
    let dir = TempDir::new("ro_sqlite_hot");
    let written = dir.path().join("written");
    std::fs::create_dir_all(&written).unwrap();
    let path = written.join("checkpoints.sqlite");
    save_query_seed(&SqliteCheckpointStore::open(&path).unwrap()).await;
    // A writer in the middle of a transaction big enough to spill to the
    // file; its file and journal copied as a crash would leave them.
    let writer = rusqlite::Connection::open(&path).unwrap();
    writer
        .execute_batch("PRAGMA cache_size = 1; CREATE TABLE pad (x BLOB); BEGIN IMMEDIATE;")
        .unwrap();
    for _ in 0..200 {
        writer
            .execute("INSERT INTO pad VALUES (randomblob(4000))", [])
            .unwrap();
    }
    let crashed = dir.path().join("crashed");
    std::fs::create_dir_all(&crashed).unwrap();
    for name in ["checkpoints.sqlite", "checkpoints.sqlite-journal"] {
        std::fs::copy(written.join(name), crashed.join(name)).unwrap();
    }
    writer.execute_batch("ROLLBACK").unwrap();
    drop(writer);

    let path = crashed.join("checkpoints.sqlite");
    let shown = path.display().to_string();
    assert_sqlite_refused(
        &crashed,
        &path,
        SqliteStoreOptions::new(),
        open_error("hot_journal", &[("path", &shown)]),
    );
}

/// The older database of the cross-engine test (`write_older` in
/// `cross_engine.rs`): every lifecycle fixture, then the layout of the
/// checkpoints component's version 1, with no `checkpoint_blobs`.
fn lifecycle() -> Vec<(String, Value)> {
    let older = &fixture("cross-engine/scenario.json")["older"];
    let fixtures = older["fixtures"].as_str().unwrap();
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/fixtures")
        .join(fixtures);
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.ends_with(".json"))
        .collect();
    names.sort();
    let prefix = older["thread_prefix"].as_str().unwrap();
    names
        .into_iter()
        .map(|name| {
            let thread_id = format!("{prefix}{}", name.trim_end_matches(".json"));
            let mut checkpoint = fixture(&format!("{fixtures}/{name}"));
            checkpoint["thread_id"] = json!(thread_id);
            (thread_id, checkpoint)
        })
        .collect()
}

const VERSION_1: &str = "DROP TABLE checkpoint_blobs;
     UPDATE strata_schema_version SET version = 1 WHERE component = 'checkpoints'";

async fn save_lifecycle(store: &dyn Store) {
    for (_, checkpoint) in lifecycle() {
        store
            .save(serde_json::from_value(checkpoint).unwrap())
            .await
            .unwrap();
    }
}

/// Every lifecycle fixture read back as it was written, every thread listed,
/// one entry per log.
async fn assert_reads_version_1(store: &dyn Store) {
    let fixtures = lifecycle();
    for (thread_id, expected) in &fixtures {
        let loaded = store.load(thread_id).await.unwrap().unwrap();
        assert_eq!(
            &serde_json::to_value(&loaded).unwrap(),
            expected,
            "{thread_id}"
        );
        let log = store.get_log(thread_id, None, 100).await.unwrap().entries;
        assert_eq!(log.len(), 1, "{thread_id}");
        assert_eq!(log[0].checkpoint, loaded, "{thread_id}");
    }
    let listed: BTreeSet<String> = store
        .list_threads(&ThreadQuery::default().with_limit(1000))
        .await
        .unwrap()
        .threads
        .into_iter()
        .map(|thread| thread.thread_id)
        .collect();
    assert_eq!(
        listed,
        fixtures
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<BTreeSet<_>>()
    );
}

#[tokio::test]
async fn a_sqlite_log_at_version_1_is_read_and_not_migrated() {
    let dir = TempDir::new("ro_sqlite_v1");
    let path = dir.path().join("older.sqlite");
    save_lifecycle(&SqliteCheckpointStore::open(&path).unwrap()).await;
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(VERSION_1)
        .unwrap();
    let before = files(dir.path());

    let store = SqliteCheckpointStore::open_read_only(&path, SqliteStoreOptions::new()).unwrap();
    assert_reads_version_1(&store).await;
    drop(store);

    assert_eq!(files(dir.path()), before);
    let conn = rusqlite::Connection::open(&path).unwrap();
    let version: i64 = conn
        .query_row(
            "SELECT version FROM strata_schema_version WHERE component = 'checkpoints'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, 1);
}

#[tokio::test]
async fn a_sqlite_log_offloaded_to_a_blob_directory_is_read_from_it() {
    let dir = TempDir::new("ro_sqlite_blob_dir");
    let path = dir.path().join("checkpoints.sqlite");
    let blobs = Arc::new(FileSystemBlobStore::new(dir.path().join("blobs")));
    let writable = SqliteCheckpointStore::open_with(
        &path,
        SqliteStoreOptions::new().with_offload(offload(Some(blobs.clone()))),
    )
    .unwrap();
    for checkpoint in offloaded_log() {
        writable.save(checkpoint).await.unwrap();
    }
    let threads = [OFFLOADED.to_string()];
    let expected = reads(&writable, &threads).await;
    drop(writable);
    assert!(
        files(dir.path())
            .keys()
            .any(|name| name.starts_with("blobs/")),
        "the values went to the directory"
    );
    let before = files(dir.path());

    let store = SqliteCheckpointStore::open_read_only(
        &path,
        SqliteStoreOptions::new().with_offload(Offload::new().with_blobs(blobs)),
    )
    .unwrap();
    assert_eq!(reads(&store, &threads).await, expected);
    // Without the directory the values aren't in the file.
    let without = SqliteCheckpointStore::open_read_only(&path, SqliteStoreOptions::new()).unwrap();
    assert!(refusal(without.load(OFFLOADED).await).contains("which the blob store doesn't have"));
    drop((store, without));
    assert_eq!(files(dir.path()), before);
}

/// Review decision 8: a file an application switched to WAL, read while its
/// writer has it open. SQLite reads the `-wal` file the writer keeps; the
/// database file and the `-wal` are left as they were (the `-shm`, the
/// shared index every reader takes a read mark in, is written by design).
#[tokio::test]
async fn a_wal_file_whose_writer_is_open_is_read_with_its_latest_writes() {
    let dir = TempDir::new("ro_sqlite_wal_open");
    let path = dir.path().join("app.db");
    drop(SqliteCheckpointStore::open(&path).unwrap());
    let writer = rusqlite::Connection::open(&path).unwrap();
    let mode: String = writer
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .unwrap();
    assert_eq!(mode, "wal");
    let application = SqliteCheckpointStore::open(&path).unwrap();
    save_query_seed(&application).await;
    let before = files(dir.path());
    assert!(before.contains_key("app.db-wal") && before.contains_key("app.db-shm"));

    let store = SqliteCheckpointStore::open_read_only(&path, SqliteStoreOptions::new()).unwrap();
    run_query_cases(&store, true).await;
    drop(store);

    let after = files(dir.path());
    assert_eq!(
        after.keys().collect::<Vec<_>>(),
        before.keys().collect::<Vec<_>>()
    );
    for name in ["app.db", "app.db-wal"] {
        assert_eq!(after[name], before[name], "{name}");
    }
    drop((application, writer));
}

/// Review decision 8, what SQLite does: a WAL file whose writer has closed it
/// has no `-wal` or `-shm`, and a read-only connection creates both next to
/// it (in a directory it can write to) and leaves them when it closes. The
/// database file itself is unchanged. The store doesn't work around it.
#[tokio::test]
async fn a_wal_file_read_without_its_wal_and_shm_gets_them_created_by_sqlite() {
    let dir = TempDir::new("ro_sqlite_wal_closed");
    let path = dir.path().join("app.db");
    {
        let application = SqliteCheckpointStore::open(&path).unwrap();
        let writer = rusqlite::Connection::open(&path).unwrap();
        let _: String = writer
            .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
            .unwrap();
        save_query_seed(&application).await;
    }
    let before = files(dir.path());
    assert_eq!(before.keys().collect::<Vec<_>>(), ["app.db"]);

    let store = SqliteCheckpointStore::open_read_only(&path, SqliteStoreOptions::new()).unwrap();
    run_query_cases(&store, true).await;
    drop(store);

    let after = files(dir.path());
    assert_eq!(
        after["app.db"], before["app.db"],
        "the database is unchanged"
    );
    assert_eq!(
        after.keys().collect::<Vec<_>>(),
        ["app.db", "app.db-shm", "app.db-wal"],
        "SQLite made the WAL's files"
    );
    assert_eq!(after["app.db-wal"], sha256(b""), "an empty -wal");
}

// --- Postgres ---

const READER: &str = "strata_reader_rust";

/// The DDL advisory lock every store takes to migrate (`postgres_store`'s
/// `DDL_LOCK_KEY`, the same in both engines).
const DDL_LOCK_KEY: i64 = 0x5354_5241_5441;

/// The role read-only stores connect as, made once per test process:
/// `CONNECT` and, per sandbox, `USAGE` and `SELECT`. It may already exist,
/// from an earlier run.
fn reader_role(url: &str) {
    static MADE: OnceLock<()> = OnceLock::new();
    MADE.get_or_init(|| {
        off_runtime(|| {
            let mut admin = postgres::Client::connect(url, postgres::NoTls).unwrap();
            admin
                .batch_execute(&format!(
                    "DO $$ BEGIN
                         CREATE ROLE {READER} LOGIN PASSWORD '{READER}';
                     EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL;
                     END $$"
                ))
                .unwrap();
            // Another session granting on the database at the same moment
            // fails this with "tuple concurrently updated": try again.
            for attempt in 1.. {
                match admin.batch_execute(&format!(
                    "DO $$ BEGIN EXECUTE format('GRANT CONNECT ON DATABASE %I TO {READER}', current_database()); END $$"
                )) {
                    Ok(()) => break,
                    Err(_) if attempt < 5 => std::thread::sleep(Duration::from_millis(100)),
                    Err(err) => panic!("granting CONNECT: {err}"),
                }
            }
        })
    });
}

/// `url` (the admin's) as [`READER`], scoped to `schema`.
fn reader_url(url: &str, schema: &str) -> String {
    let (scheme, rest) = url.split_once("://").unwrap();
    let host = rest.rsplit_once('@').map_or(rest, |(_, host)| host);
    let base = format!("{scheme}://{READER}:{READER}@{host}");
    let separator = if base.contains('?') { '&' } else { '?' };
    format!("{base}{separator}options=-c%20search_path%3D{schema}")
}

/// Lets [`READER`] read every table in `sandbox`'s schema, as it is now.
fn grant_reader(sandbox: &PgSandbox) {
    let schema = sandbox.schema.clone();
    sandbox.admin(|client| {
        client
            .batch_execute(&format!(
                "GRANT USAGE ON SCHEMA {schema} TO {READER};
                 GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {READER}"
            ))
            .unwrap()
    });
}

/// Everything in `sandbox`'s schema: each relation (table, index, sequence),
/// trigger and function by name, and every row of every table.
fn pg_snapshot(sandbox: &PgSandbox) -> Value {
    sandbox.admin(|client| {
        let relations: Vec<(String, String)> = client
            .query(
                "SELECT c.relname::text, c.relkind::text FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = current_schema() ORDER BY 1",
                &[],
            )
            .unwrap()
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        let triggers: Vec<String> = client
            .query(
                "SELECT t.tgname::text FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = current_schema() AND NOT t.tgisinternal ORDER BY 1",
                &[],
            )
            .unwrap()
            .iter()
            .map(|row| row.get(0))
            .collect();
        let functions: Vec<String> = client
            .query(
                "SELECT p.proname::text FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                 WHERE n.nspname = current_schema() ORDER BY 1",
                &[],
            )
            .unwrap()
            .iter()
            .map(|row| row.get(0))
            .collect();
        let mut rows = BTreeMap::new();
        for (name, kind) in &relations {
            if kind == "r" {
                let table: Vec<String> = client
                    .query(
                        &format!("SELECT to_jsonb(t)::text FROM {name} t ORDER BY 1"),
                        &[],
                    )
                    .unwrap()
                    .iter()
                    .map(|row| row.get(0))
                    .collect();
                rows.insert(name.clone(), table);
            }
        }
        json!({ "relations": relations, "triggers": triggers, "functions": functions, "rows": rows })
    })
}

fn current_database(sandbox: &PgSandbox) -> String {
    sandbox.admin(|client| {
        client
            .query_one("SELECT current_database()::text", &[])
            .unwrap()
            .get(0)
    })
}

/// A sandbox and the reader role, or `None` (after saying so) without
/// Postgres.
fn pg_sandbox(label: &str) -> Option<(String, PgSandbox)> {
    let url = backend_url(POSTGRES_VAR)?;
    reader_role(&url);
    let sandbox = PgSandbox::new(&url, label);
    Some((url, sandbox))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_postgres_store_reads_what_the_writable_one_does_and_changes_nothing() {
    let Some((url, sandbox)) = pg_sandbox("ro_pg") else {
        return;
    };
    let plain = PostgresCheckpointStore::connect(&sandbox.url(None))
        .await
        .unwrap();
    let offloading = PostgresCheckpointStore::connect_with(
        &sandbox.url(None),
        PostgresStoreOptions::new().with_offload(offload(None)),
    )
    .await
    .unwrap();
    assert!(!plain.is_read_only());
    seed(&plain, &offloading).await;
    let expected = reads(&plain, &seeded_threads()).await;
    assert_eq!(expected.threads[OFFLOADED].log.len(), 4);
    drop((plain, offloading));
    grant_reader(&sandbox);
    let before = pg_snapshot(&sandbox);

    // A migration in progress elsewhere holds the DDL lock: a read-only
    // store takes no lock, so it doesn't wait for it.
    let migrating = sandbox.client().await;
    migrating
        .execute("SELECT pg_advisory_lock($1)", &[&DDL_LOCK_KEY])
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        let store = PostgresCheckpointStore::connect_read_only(
            &reader_url(&url, &sandbox.schema),
            PostgresStoreOptions::new(),
        )
        .await
        .unwrap();
        assert!(store.is_read_only());
        assert_eq!(reads(&store, &seeded_threads()).await, expected);
        assert_every_write_refused(&store, vec![("migrate", refusal(store.migrate().await))]).await;
        assert!(
            store
                .execute_unguarded("UPDATE strata_schema_version SET version = version")
                .await
                .is_err()
        );
    })
    .await
    .expect("a read-only store waited for the DDL lock");
    migrating
        .execute("SELECT pg_advisory_unlock($1)", &[&DDL_LOCK_KEY])
        .await
        .unwrap();

    assert_eq!(pg_snapshot(&sandbox), before);
}

#[tokio::test(flavor = "multi_thread")]
async fn every_write_on_a_read_only_postgres_store_is_refused() {
    let Some((_url, sandbox)) = pg_sandbox("ro_pg_writes") else {
        return;
    };
    save_query_seed(
        &PostgresCheckpointStore::connect(&sandbox.url(None))
            .await
            .unwrap(),
    )
    .await;
    let before = pg_snapshot(&sandbox);
    let store =
        PostgresCheckpointStore::connect_read_only(&sandbox.url(None), PostgresStoreOptions::new())
            .await
            .unwrap();
    assert_every_write_refused(&store, vec![("migrate", refusal(store.migrate().await))]).await;
    assert_eq!(pg_snapshot(&sandbox), before);
}

/// As the owner of every table, so only the session's
/// `default_transaction_read_only` stops the write.
#[tokio::test(flavor = "multi_thread")]
async fn postgres_itself_refuses_a_write_on_a_read_only_stores_session() {
    let Some((_url, sandbox)) = pg_sandbox("ro_pg_enforced") else {
        return;
    };
    save_query_seed(
        &PostgresCheckpointStore::connect(&sandbox.url(None))
            .await
            .unwrap(),
    )
    .await;
    let before = pg_snapshot(&sandbox);
    let store =
        PostgresCheckpointStore::connect_read_only(&sandbox.url(None), PostgresStoreOptions::new())
            .await
            .unwrap();
    for sql in [
        "UPDATE strata_schema_version SET version = version",
        "DELETE FROM checkpoints",
        "CREATE TABLE probe (x int)",
        "CREATE TEMP TABLE probe (x int)",
    ] {
        let refused = store.execute_unguarded(sql).await;
        assert!(
            refused
                .as_ref()
                .is_err_and(|err| err.0.contains("read-only transaction")),
            "{sql}: {refused:?}"
        );
    }
    assert_eq!(pg_snapshot(&sandbox), before);
}

/// Opens `sandbox`'s schema read-only as the reader, expecting `message`, and
/// checks the schema is as it was.
async fn assert_pg_refused(
    url: &str,
    sandbox: &PgSandbox,
    options: PostgresStoreOptions,
    message: String,
) {
    grant_reader(sandbox);
    let before = pg_snapshot(sandbox);
    let refused = refusal(
        PostgresCheckpointStore::connect_read_only(&reader_url(url, &sandbox.schema), options)
            .await,
    );
    assert_eq!(refused, message);
    assert_eq!(pg_snapshot(sandbox), before, "the refusal changed nothing");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_postgres_database_with_no_strata_log_is_refused() {
    let Some((url, sandbox)) = pg_sandbox("ro_pg_no_log") else {
        return;
    };
    let database = current_database(&sandbox);
    assert_pg_refused(
        &url,
        &sandbox,
        PostgresStoreOptions::new(),
        open_error("no_strata_log", &[("store", &database)]),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_postgres_log_at_version_0_is_refused_and_not_migrated() {
    let Some((url, sandbox)) = pg_sandbox("ro_pg_v0") else {
        return;
    };
    sandbox.admin(|client| client.batch_execute(POSTGRES_CHECKPOINT_LOG_DDL).unwrap());
    assert_pg_refused(
        &url,
        &sandbox,
        PostgresStoreOptions::new(),
        open_error("version_zero", &[("known", &known())]),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_postgres_log_newer_than_the_build_is_refused_and_left_alone() {
    let Some((url, sandbox)) = pg_sandbox("ro_pg_newer") else {
        return;
    };
    drop(
        PostgresCheckpointStore::connect(&sandbox.url(None))
            .await
            .unwrap(),
    );
    sandbox.admin(|client| {
        client
            .batch_execute(
                "UPDATE strata_schema_version SET version = 7 WHERE component = 'checkpoints'",
            )
            .unwrap()
    });
    assert_pg_refused(
        &url,
        &sandbox,
        PostgresStoreOptions::new(),
        open_error("newer_version", &[("found", "7"), ("known", &known())]),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_blob_directory_is_refused_by_a_read_only_postgres_store() {
    let Some((url, sandbox)) = pg_sandbox("ro_pg_blobs") else {
        return;
    };
    drop(
        PostgresCheckpointStore::connect(&sandbox.url(None))
            .await
            .unwrap(),
    );
    let blobs = std::env::temp_dir().join(unique_name("missing_blobs"));
    assert_pg_refused(
        &url,
        &sandbox,
        PostgresStoreOptions::new()
            .with_offload(offload(Some(Arc::new(FileSystemBlobStore::new(&blobs))))),
        open_error(
            "missing_blob_directory",
            &[("dir", &blobs.display().to_string())],
        ),
    )
    .await;
    assert!(!blobs.exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_postgres_log_at_version_1_is_read_and_not_migrated() {
    let Some((url, sandbox)) = pg_sandbox("ro_pg_v1") else {
        return;
    };
    save_lifecycle(
        &PostgresCheckpointStore::connect(&sandbox.url(None))
            .await
            .unwrap(),
    )
    .await;
    sandbox.admin(|client| client.batch_execute(VERSION_1).unwrap());
    grant_reader(&sandbox);
    let before = pg_snapshot(&sandbox);

    let store = PostgresCheckpointStore::connect_read_only(
        &reader_url(&url, &sandbox.schema),
        PostgresStoreOptions::new(),
    )
    .await
    .unwrap();
    assert_reads_version_1(&store).await;
    drop(store);

    assert_eq!(pg_snapshot(&sandbox), before);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_postgres_log_offloaded_to_a_blob_directory_is_read_from_it() {
    let Some((url, sandbox)) = pg_sandbox("ro_pg_blob_dir") else {
        return;
    };
    let dir = TempDir::new("ro_pg_blob_dir");
    let blobs = Arc::new(FileSystemBlobStore::new(dir.path().join("blobs")));
    let writable = PostgresCheckpointStore::connect_with(
        &sandbox.url(None),
        PostgresStoreOptions::new().with_offload(offload(Some(blobs.clone()))),
    )
    .await
    .unwrap();
    for checkpoint in offloaded_log() {
        writable.save(checkpoint).await.unwrap();
    }
    let threads = [OFFLOADED.to_string()];
    let expected = reads(&writable, &threads).await;
    drop(writable);
    grant_reader(&sandbox);
    let (before, blobs_before) = (pg_snapshot(&sandbox), files(dir.path()));
    assert!(!blobs_before.is_empty());

    let store = PostgresCheckpointStore::connect_read_only(
        &reader_url(&url, &sandbox.schema),
        PostgresStoreOptions::new().with_offload(Offload::new().with_blobs(blobs)),
    )
    .await
    .unwrap();
    assert_eq!(reads(&store, &threads).await, expected);
    drop(store);

    assert_eq!(pg_snapshot(&sandbox), before);
    assert_eq!(files(dir.path()), blobs_before);
}

// --- Redis ---

/// Every key under `prefix`, with its type, its value (`DUMP`) and when it
/// expires (`PEXPIRETIME`, -1 for never): a key added, removed, changed or
/// given another expiry shows.
async fn redis_snapshot(url: &str, prefix: &str) -> BTreeMap<String, (String, Vec<u8>, i64)> {
    let mut conn = redis_connection(url).await;
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{prefix}:*"))
        .query_async(&mut conn)
        .await
        .unwrap();
    let mut all = BTreeMap::new();
    for key in keys {
        let kind: String = redis::cmd("TYPE")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .unwrap();
        let value: Vec<u8> = redis::cmd("DUMP")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .unwrap();
        let expires: i64 = redis::cmd("PEXPIRETIME")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .unwrap();
        all.insert(key, (kind, value, expires));
    }
    all
}

/// The thread whose list is deleted behind the index's back.
const GONE: &str = "gone-thread";

fn gone() -> Checkpoint {
    Checkpoint {
        thread_id: GONE.to_string(),
        // Newer than every seeded thread: listed first.
        created_at: "2026-12-01T00:00:00Z".to_string(),
        ..query_seed().remove(0)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_redis_store_reads_what_the_writable_one_does_and_changes_nothing() {
    let Some(url) = backend_url(REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "ro_redis");
    let options = || RedisStoreOptions::new().with_prefix(&sandbox.prefix);
    let plain = RedisCheckpointStore::connect_with(&url, options())
        .await
        .unwrap();
    let offloading =
        RedisCheckpointStore::connect_with(&url, options().with_offload(offload(None)))
            .await
            .unwrap();
    assert!(!plain.is_read_only());
    seed(&plain, &offloading).await;
    let expected = reads(&plain, &seeded_threads()).await;
    assert_eq!(expected.threads[OFFLOADED].log.len(), 4);
    // A thread whose list expired: still in the index, which a writable
    // store's listing would take it out of.
    plain.save(gone()).await.unwrap();
    let mut conn = redis_connection(&url).await;
    let _: i64 = redis::cmd("DEL")
        .arg(format!("{}:checkpoints:{GONE}", sandbox.prefix))
        .query_async(&mut conn)
        .await
        .unwrap();
    drop((plain, offloading));
    let before = redis_snapshot(&url, &sandbox.prefix).await;

    let store = RedisCheckpointStore::connect_read_only(&url, options())
        .await
        .unwrap();
    assert!(store.is_read_only());
    assert_eq!(reads(&store, &seeded_threads()).await, expected);
    let listed = store
        .list_threads(&ThreadQuery::default().with_limit(1000))
        .await
        .unwrap();
    assert!(
        !listed.threads.iter().any(|thread| thread.thread_id == GONE),
        "a thread whose list is gone is skipped"
    );
    assert_every_write_refused(
        &store,
        vec![("rebuild index", refusal(store.rebuild_index().await))],
    )
    .await;
    drop(store);

    assert_eq!(redis_snapshot(&url, &sandbox.prefix).await, before);
}

#[tokio::test(flavor = "multi_thread")]
async fn every_write_on_a_read_only_redis_store_is_refused() {
    let Some(url) = backend_url(REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "ro_redis_writes");
    let options = || RedisStoreOptions::new().with_prefix(&sandbox.prefix);
    save_query_seed(
        &RedisCheckpointStore::connect_with(&url, options())
            .await
            .unwrap(),
    )
    .await;
    let before = redis_snapshot(&url, &sandbox.prefix).await;
    let store = RedisCheckpointStore::connect_read_only(&url, options())
        .await
        .unwrap();
    assert_every_write_refused(
        &store,
        vec![("rebuild index", refusal(store.rebuild_index().await))],
    )
    .await;
    assert_eq!(redis_snapshot(&url, &sandbox.prefix).await, before);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_redis_prefix_no_runtime_has_indexed_is_refused_and_not_indexed() {
    let Some(url) = backend_url(REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "ro_redis_unindexed");
    // What a runtime from before the index wrote: a list, and no marker.
    let mut conn = redis_connection(&url).await;
    let _: i64 = redis::cmd("RPUSH")
        .arg(format!("{}:checkpoints:old-thread", sandbox.prefix))
        .arg(serde_json::to_string(&query_seed()[0]).unwrap())
        .query_async(&mut conn)
        .await
        .unwrap();
    let before = redis_snapshot(&url, &sandbox.prefix).await;

    let refused = refusal(
        RedisCheckpointStore::connect_read_only(
            &url,
            RedisStoreOptions::new().with_prefix(&sandbox.prefix),
        )
        .await,
    );
    assert_eq!(
        refused,
        open_error("redis_prefix_not_indexed", &[("prefix", &sandbox.prefix)])
    );
    assert_eq!(redis_snapshot(&url, &sandbox.prefix).await, before);

    // An empty prefix, a mistyped one say, is refused the same way.
    let typo = format!("{}-typo", sandbox.prefix);
    let refused = refusal(
        RedisCheckpointStore::connect_read_only(&url, RedisStoreOptions::new().with_prefix(&typo))
            .await,
    );
    assert_eq!(
        refused,
        open_error("redis_prefix_not_indexed", &[("prefix", &typo)])
    );
    assert!(redis_snapshot(&url, &typo).await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_blob_directory_is_refused_by_a_read_only_redis_store() {
    let Some(url) = backend_url(REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "ro_redis_blobs");
    let options = || RedisStoreOptions::new().with_prefix(&sandbox.prefix);
    drop(
        RedisCheckpointStore::connect_with(&url, options())
            .await
            .unwrap(),
    );
    let blobs = std::env::temp_dir().join(unique_name("missing_blobs"));
    let before = redis_snapshot(&url, &sandbox.prefix).await;
    let refused = refusal(
        RedisCheckpointStore::connect_read_only(
            &url,
            options().with_offload(offload(Some(Arc::new(FileSystemBlobStore::new(&blobs))))),
        )
        .await,
    );
    assert_eq!(
        refused,
        open_error(
            "missing_blob_directory",
            &[("dir", &blobs.display().to_string())]
        )
    );
    assert!(!blobs.exists());
    assert_eq!(redis_snapshot(&url, &sandbox.prefix).await, before);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_redis_thread_whose_list_is_gone_is_skipped_and_left_in_the_index() {
    let Some(url) = backend_url(REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "ro_redis_gone");
    let options = || RedisStoreOptions::new().with_prefix(&sandbox.prefix);
    let writable = RedisCheckpointStore::connect_with(&url, options())
        .await
        .unwrap();
    save_query_seed(&writable).await;
    writable.save(gone()).await.unwrap();
    let mut conn = redis_connection(&url).await;
    let _: i64 = redis::cmd("DEL")
        .arg(format!("{}:checkpoints:{GONE}", sandbox.prefix))
        .query_async(&mut conn)
        .await
        .unwrap();
    let threads_key = format!("{}:threads", sandbox.prefix);
    let indexed = |conn: &mut redis::aio::MultiplexedConnection| {
        let mut conn = conn.clone();
        let key = threads_key.clone();
        async move {
            let score: Option<f64> = redis::cmd("ZSCORE")
                .arg(key)
                .arg(GONE)
                .query_async(&mut conn)
                .await
                .unwrap();
            score.is_some()
        }
    };
    assert!(indexed(&mut conn).await);
    let before = redis_snapshot(&url, &sandbox.prefix).await;

    let store = RedisCheckpointStore::connect_read_only(&url, options())
        .await
        .unwrap();
    let listed: Vec<String> = store
        .list_threads(&ThreadQuery::default().with_limit(1000))
        .await
        .unwrap()
        .threads
        .into_iter()
        .map(|thread| thread.thread_id)
        .collect();
    let mut expected: Vec<String> = query_seed().into_iter().map(|c| c.thread_id).collect();
    expected.sort();
    expected.dedup();
    let mut sorted = listed.clone();
    sorted.sort();
    assert_eq!(
        sorted, expected,
        "every thread but the one whose list is gone"
    );
    assert!(indexed(&mut conn).await, "still indexed");
    assert_eq!(redis_snapshot(&url, &sandbox.prefix).await, before);

    // The writable store takes it out.
    writable
        .list_threads(&ThreadQuery::default().with_limit(1000))
        .await
        .unwrap();
    assert!(!indexed(&mut conn).await);
}

/// Review decision 9: on Redis 7 and later, a read-only store runs its read
/// scripts with `EVALSHA_RO` / `EVAL_RO`, so a Redis user that can't run
/// `EVAL` (or `INFO`) can read through it. The user may use read commands,
/// `MULTI`/`EXEC` (the store reads a list's last element and its length
/// together), `TIME` (the lease script asks the server's clock) and the
/// read-only script commands.
#[tokio::test(flavor = "multi_thread")]
async fn a_redis_user_allowed_only_reads_can_use_a_read_only_store() {
    let Some(url) = backend_url(REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "ro_redis_acl");
    let options = || RedisStoreOptions::new().with_prefix(&sandbox.prefix);
    let writable = RedisCheckpointStore::connect_with(&url, options())
        .await
        .unwrap();
    save_query_seed(&writable).await;
    writable
        .try_acquire_lease(HELD, "worker-2", Duration::from_secs(3600))
        .await
        .unwrap();
    let expected = reads(&writable, &seeded_threads()).await;
    drop(writable);

    let user = AclUser::new(&url, &sandbox.prefix);
    let as_user = user.url();

    let before = redis_snapshot(&url, &sandbox.prefix).await;
    let store = RedisCheckpointStore::connect_read_only(&as_user, options())
        .await
        .unwrap();
    run_query_cases(&store, false).await;
    assert_eq!(reads(&store, &seeded_threads()).await, expected);
    assert_eq!(redis_snapshot(&url, &sandbox.prefix).await, before);

    // The user can't run a script that could write, so a writable store
    // can't save as it.
    let mut conn = redis_connection(&as_user).await;
    let eval: Result<i64, _> = redis::cmd("EVAL")
        .arg("return 1")
        .arg(0)
        .query_async(&mut conn)
        .await;
    assert!(eval.is_err(), "{eval:?}");
    let writable = RedisCheckpointStore::connect_with(&as_user, options())
        .await
        .unwrap();
    assert!(writable.save(query_seed().remove(0)).await.is_err());
    assert_eq!(redis_snapshot(&url, &sandbox.prefix).await, before);
}

/// A Redis user allowed read commands under `prefix` only, deleted on drop.
struct AclUser {
    url: String,
    name: String,
}

impl AclUser {
    fn new(url: &str, prefix: &str) -> Self {
        let name = unique_name("reader");
        let mut admin = redis::Client::open(url).unwrap().get_connection().unwrap();
        let () = redis::cmd("ACL")
            .arg("SETUSER")
            .arg(&name)
            .arg("reset")
            .arg("on")
            .arg(format!(">{name}"))
            .arg(format!("~{prefix}:*"))
            .arg("+@read")
            .arg("+@transaction")
            .arg("+time")
            .arg("+eval_ro")
            .arg("+evalsha_ro")
            .query(&mut admin)
            .unwrap();
        Self {
            url: url.to_string(),
            name,
        }
    }

    /// The server's URL, as this user.
    fn url(&self) -> String {
        let (scheme, host) = self.url.split_once("://").unwrap();
        let host = host.rsplit_once('@').map_or(host, |(_, host)| host);
        format!("{scheme}://{0}:{0}@{host}", self.name)
    }
}

impl Drop for AclUser {
    fn drop(&mut self) {
        if let Ok(mut admin) =
            redis::Client::open(self.url.as_str()).and_then(|c| c.get_connection())
        {
            let _: Result<i64, _> = redis::cmd("ACL")
                .arg("DELUSER")
                .arg(&self.name)
                .query(&mut admin);
        }
    }
}

// --- The shared query cases: each read-only store answers them as its
// writable store does (`checkpoint_store_contract.rs`), Redis held to the
// threads and their order, not the page boundaries.

#[tokio::test]
async fn a_read_only_sqlite_store_answers_the_shared_query_cases() {
    let dir = TempDir::new("ro_sqlite_cases");
    let path = dir.path().join("checkpoints.sqlite");
    save_query_seed(&SqliteCheckpointStore::open(&path).unwrap()).await;
    let store = SqliteCheckpointStore::open_read_only(&path, SqliteStoreOptions::new()).unwrap();
    run_query_cases(&store, true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_postgres_store_answers_the_shared_query_cases() {
    let Some((url, sandbox)) = pg_sandbox("ro_pg_cases") else {
        return;
    };
    save_query_seed(
        &PostgresCheckpointStore::connect(&sandbox.url(None))
            .await
            .unwrap(),
    )
    .await;
    grant_reader(&sandbox);
    let store = PostgresCheckpointStore::connect_read_only(
        &reader_url(&url, &sandbox.schema),
        PostgresStoreOptions::new(),
    )
    .await
    .unwrap();
    run_query_cases(&store, true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_redis_store_answers_the_shared_query_cases() {
    let Some(url) = backend_url(REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "ro_redis_cases");
    let options = || RedisStoreOptions::new().with_prefix(&sandbox.prefix);
    save_query_seed(
        &RedisCheckpointStore::connect_with(&url, options())
            .await
            .unwrap(),
    )
    .await;
    let store = RedisCheckpointStore::connect_read_only(&url, options())
        .await
        .unwrap();
    run_query_cases(&store, false).await;
}

#[test]
fn the_shared_messages_name_every_case() {
    let errors = errors();
    let cases: BTreeSet<&str> = errors["open"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        cases,
        BTreeSet::from([
            "missing_sqlite_file",
            "no_strata_log",
            "newer_version",
            "version_zero",
            "hot_journal",
            "redis_prefix_not_indexed",
            "missing_blob_directory",
        ])
    );
    // Every write the fixture names is one some store has.
    let operations: BTreeSet<String> =
        serde_json::from_value(errors["write"]["operations"].clone()).unwrap();
    let known: BTreeSet<String> = [
        "save",
        "acquire lease",
        "renew lease",
        "release lease",
        "purge",
    ]
    .into_iter()
    .chain(STORE_OWN_WRITES)
    .map(str::to_string)
    .collect();
    assert_eq!(operations, known);
    // The newer-version refusal is every store's, read-only or not.
    assert_eq!(
        open_error("newer_version", &[("found", "7"), ("known", "2")]),
        schema_versions::newer(schema_versions::CHECKPOINTS, 7, 2)
    );
}
