//! The cross-engine migration test (`design/Arch-Specs/STRATA-SPEC-005.md`,
//! "Conformance"; `schema/fixtures/cross-engine/scenario.json`): one engine
//! writes a SQLite file and a Postgres schema at the checkpoints component's
//! schema version 1, holding a checkpoint at every engine-visible version, and
//! another at the current version written with deltas and offload; the other
//! engine opens them with its current stores, migrates the older one, reads
//! every checkpoint and resumes threads to the end. The xUnit twin is
//! `CrossEngineTests`; keep the two in step.
//!
//! `STRATA_INTEROP_DIR` names the directory the engines share and
//! `STRATA_INTEROP_STEP` what this run does there: `write` this engine's
//! databases (`rust-*`), or `read` the .NET engine's (`dotnet-*`).
//! `.github/workflows/cross-engine-ci.yml` runs the four steps. Without them,
//! this engine writes into a directory of its own and reads what it wrote, so
//! the test runs in every build. Postgres needs `STRATA_TEST_POSTGRES_URL`
//! (see `common`); a cross-engine step requires it.

mod common;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use common::{POSTGRES_VAR, backend_url, fixture, off_runtime, unique_name};
use datalium_strata::BoxFuture;
use datalium_strata::checkpoint::{
    CheckpointQuery, CheckpointStore, CheckpointStoreError, GraphExecutionResult, HumanSignal,
    LatestCheckpoint, ThreadQuery,
};
use datalium_strata::context::NodeContext;
use datalium_strata::graph::{CheckpointPolicy, CompileOptions, CompiledGraph, StateGraph};
use datalium_strata::schema::{Checkpoint, CheckpointStatus};
use datalium_strata_memory::checkpoint::SqliteCheckpointStore;
use datalium_strata_memory::postgres_store::PostgresCheckpointStore;
use datalium_strata_memory::schema_versions;
use datalium_strata_memory::{Deltas, Offload, PostgresStoreOptions, SqliteStoreOptions};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const ENGINE: &str = "rust";
const OTHER: &str = "dotnet";
const OLDER: &str = "older";
const CURRENT: &str = "current";

/// What a store call returns once the writer's process has died.
const DIED: &str = "the process died";

fn scenario() -> Value {
    fixture("cross-engine/scenario.json")
}

// --- Where the databases are ---

/// One of the two databases an engine writes, in one backend.
#[derive(Clone)]
enum Database {
    Sqlite(PathBuf),
    /// The server's URL and the schema the database's tables are in.
    Postgres {
        url: String,
        schema: String,
    },
}

impl Database {
    fn sqlite(dir: &Path, engine: &str, database: &str) -> Self {
        Self::Sqlite(dir.join(format!("{engine}-{database}.sqlite")))
    }

    fn postgres(url: &str, prefix: &str, engine: &str, database: &str) -> Self {
        let schema = format!("{prefix}_{engine}_{database}");
        // Postgres truncates a longer identifier, so two databases' schemas
        // could become one.
        assert!(
            schema.len() <= 63,
            "schema name '{schema}' is longer than Postgres's 63 characters"
        );
        Self::Postgres {
            url: url.to_string(),
            schema,
        }
    }

    fn label(&self) -> String {
        match self {
            Self::Sqlite(path) => path.display().to_string(),
            Self::Postgres { schema, .. } => format!("postgres schema {schema}"),
        }
    }

    /// Removes whatever an earlier run left, and makes the Postgres schema.
    fn create(&self) {
        match self {
            Self::Sqlite(path) => {
                let _ = std::fs::remove_file(path);
            }
            Self::Postgres { url, schema } => admin(url, None, |client| {
                client
                    .batch_execute(&format!(
                        "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}"
                    ))
                    .unwrap()
            }),
        }
    }

    fn remove(&self) {
        match self {
            Self::Sqlite(path) => {
                let _ = std::fs::remove_file(path);
            }
            Self::Postgres { url, schema } => admin(url, None, |client| {
                client
                    .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                    .unwrap()
            }),
        }
    }

    /// A current store on this database, with deltas and offload on.
    async fn open(&self) -> Arc<dyn Store> {
        let scenario = scenario();
        let threshold = scenario["current"]["offload_threshold"].as_u64().unwrap() as usize;
        let offload = Offload::new().with_threshold(threshold);
        match self {
            Self::Sqlite(path) => Arc::new(
                SqliteCheckpointStore::open_with(
                    path,
                    SqliteStoreOptions::new()
                        .with_deltas(Deltas::new())
                        .with_offload(offload),
                )
                .unwrap(),
            ),
            Self::Postgres { url, schema } => Arc::new(
                PostgresCheckpointStore::connect_with(
                    &in_schema(url, schema),
                    PostgresStoreOptions::new()
                        .with_deltas(Deltas::new())
                        .with_offload(offload),
                )
                .await
                .unwrap(),
            ),
        }
    }

    /// Runs `sql` as any other writer would, behind every store's back.
    fn execute(&self, sql: &str) {
        match self {
            Self::Sqlite(path) => rusqlite::Connection::open(path)
                .unwrap()
                .execute_batch(sql)
                .unwrap(),
            Self::Postgres { url, schema } => admin(url, Some(schema), |client| {
                client.batch_execute(sql).unwrap()
            }),
        }
    }

    /// The `checkpoints` component's recorded version, and whether the blob
    /// table is there.
    fn layout(&self) -> (i64, bool) {
        const VERSION: &str =
            "SELECT version FROM strata_schema_version WHERE component = 'checkpoints'";
        match self {
            Self::Sqlite(path) => {
                let conn = rusqlite::Connection::open(path).unwrap();
                let version = conn.query_row(VERSION, [], |row| row.get(0)).unwrap();
                let blobs: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'checkpoint_blobs'",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap();
                (version, blobs == 1)
            }
            Self::Postgres { url, schema } => admin(url, Some(schema), |client| {
                let version = client.query_one(VERSION, &[]).unwrap().get(0);
                let blobs: bool = client
                    .query_one("SELECT to_regclass('checkpoint_blobs') IS NOT NULL", &[])
                    .unwrap()
                    .get(0);
                (version, blobs)
            }),
        }
    }

    /// The `schema_version` column of each of `thread_id`'s rows, oldest first.
    fn versions(&self, thread_id: &str) -> Vec<String> {
        const ROWS: &str =
            "SELECT schema_version FROM checkpoints WHERE thread_id = $1 ORDER BY sequence";
        match self {
            Self::Sqlite(path) => {
                let conn = rusqlite::Connection::open(path).unwrap();
                let mut statement = conn.prepare(&ROWS.replace("$1", "?1")).unwrap();
                statement
                    .query_map([thread_id], |row| row.get(0))
                    .unwrap()
                    .collect::<Result<_, _>>()
                    .unwrap()
            }
            Self::Postgres { url, schema } => admin(url, Some(schema), |client| {
                client
                    .query(ROWS, &[&thread_id])
                    .unwrap()
                    .iter()
                    .map(|row| row.get(0))
                    .collect()
            }),
        }
    }
}

/// `url` with its connections' `search_path` set to `schema`.
fn in_schema(url: &str, schema: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}options=-c%20search_path%3D{schema}")
}

/// Runs `work` with a sync connection, scoped to `schema` if given.
fn admin<R: Send>(
    url: &str,
    schema: Option<&str>,
    work: impl FnOnce(&mut postgres::Client) -> R + Send,
) -> R {
    off_runtime(|| {
        let mut config: postgres::Config = url.parse().unwrap();
        if let Some(schema) = schema {
            config.options(&format!("-c search_path={schema}"));
        }
        let mut client = config.connect(postgres::NoTls).unwrap();
        work(&mut client)
    })
}

/// A store, as one trait object.
trait Store: CheckpointStore + CheckpointQuery {}

impl<T: CheckpointStore + CheckpointQuery> Store for T {}

/// A store as the writer's process sees it: once it has died, nothing it does
/// reaches the database.
struct Process {
    inner: Arc<dyn Store>,
    dead: AtomicBool,
}

impl Process {
    fn alive<'a, T: Send + 'a>(
        &self,
        call: impl FnOnce() -> BoxFuture<'a, Result<T, CheckpointStoreError>>,
    ) -> BoxFuture<'a, Result<T, CheckpointStoreError>> {
        if self.dead.load(Ordering::SeqCst) {
            Box::pin(std::future::ready(Err(CheckpointStoreError(
                DIED.to_string(),
            ))))
        } else {
            call()
        }
    }
}

impl CheckpointStore for Process {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        self.alive(|| self.inner.save(checkpoint))
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        self.alive(|| self.inner.load(thread_id))
    }

    fn load_latest<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<LatestCheckpoint>, CheckpointStoreError>> {
        self.alive(|| self.inner.load_latest(thread_id))
    }

    fn save_if_latest(
        &self,
        checkpoint: Checkpoint,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'_, Result<bool, CheckpointStoreError>> {
        self.alive(|| self.inner.save_if_latest(checkpoint, expected_sequence))
    }
}

// --- The graphs ---

fn durable() -> CompileOptions {
    CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode)
}

/// `frozen.json`'s and `identified.json`'s state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct WireTransfer {
    invoice_generated: bool,
    amount: f64,
    currency: String,
    approved_by: Option<String>,
    is_completed: bool,
}

/// `PrepareWireTransfer` (a boundary) -> `ExecuteWireTransfer`.
fn wire_transfer(options: CompileOptions) -> CompiledGraph<WireTransfer> {
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

/// `running.json`'s and `claimed.json`'s state, and the current database's:
/// every node adds its name to `trail`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Job {
    trail: String,
    #[serde(default)]
    approved: bool,
    #[serde(default)]
    limit: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    evidence: String,
}

/// One run of a node: which, under which key, whether it may have run before,
/// and the decision it saw.
#[derive(Debug, Clone, PartialEq)]
struct Visit {
    node: String,
    key: String,
    recovery: bool,
    reviewer: Option<String>,
}

/// Every node a graph ran, and what to do when one is reached.
#[derive(Default)]
struct Journal {
    visits: Mutex<Vec<Visit>>,
    dies_at: Mutex<Option<(String, Arc<Process>)>>,
}

impl Journal {
    fn visit(&self, mut job: Job, context: &NodeContext) -> Job {
        self.visits.lock().unwrap().push(Visit {
            node: context.node().to_string(),
            key: context.idempotency_key().to_string(),
            recovery: context.is_recovery(),
            reviewer: context.decision().map(|d| d.reviewer.clone()),
        });
        if let Some((node, process)) = &*self.dies_at.lock().unwrap()
            && node == context.node()
        {
            process.dead.store(true, Ordering::SeqCst);
        }
        job.trail.push_str(context.node());
        job
    }

    fn visits(&self) -> Vec<Visit> {
        self.visits.lock().unwrap().clone()
    }
}

/// The nodes in a line, each adding its name to the trail.
fn chain(journal: &Arc<Journal>, nodes: &[&str]) -> StateGraph<Job> {
    let mut graph = StateGraph::<Job>::new();
    for (index, name) in nodes.iter().enumerate() {
        let journal = journal.clone();
        graph = graph.add_async_node(*name, move |job: Job, context: NodeContext| {
            let journal = journal.clone();
            async move { journal.visit(job, &context) }
        });
        if index > 0 {
            graph = graph.add_edge(nodes[index - 1], *name);
        }
    }
    graph
}

fn line(journal: &Arc<Journal>, nodes: &[&str]) -> CompiledGraph<Job> {
    chain(journal, nodes).compile_with(durable()).unwrap()
}

/// `A` (a boundary, rejected to `Rejected`) -> `B` -> `C`.
fn approval(journal: &Arc<Journal>) -> CompiledGraph<Job> {
    let graph =
        chain(journal, &["A", "B", "C"]).add_interrupt_boundary_with_reject("A", "Rejected");
    let journal = journal.clone();
    graph
        .add_async_node("Rejected", move |job: Job, context: NodeContext| {
            let journal = journal.clone();
            async move { journal.visit(job, &context) }
        })
        .compile_with(durable())
        .unwrap()
}

fn current_nodes() -> Vec<String> {
    scenario()["current"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|node| node.as_str().unwrap().to_string())
        .collect()
}

fn completed<S>(label: &str, result: GraphExecutionResult<S>) -> S {
    match result {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(error) => {
            panic!("{label}: expected a completed run, got {error:?}")
        }
        GraphExecutionResult::Interrupted { .. } => {
            panic!("{label}: expected a completed run, got a freeze")
        }
        GraphExecutionResult::Cancelled { reason, .. } => {
            panic!("{label}: expected a completed run, got one cancelled as {reason:?}")
        }
    }
}

// --- The lifecycle fixtures ---

/// Every `checkpoint-lifecycle` fixture as `(thread id, checkpoint)`, the
/// thread id the writer saves it under, sorted by file name.
fn lifecycle() -> Vec<(String, Value)> {
    let scenario = scenario();
    let older = &scenario["older"];
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/fixtures")
        .join(older["fixtures"].as_str().unwrap());
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
            let mut checkpoint =
                fixture(&format!("{}/{name}", older["fixtures"].as_str().unwrap()));
            checkpoint["thread_id"] = json!(thread_id);
            (thread_id, checkpoint)
        })
        .collect()
}

async fn thread_ids(store: &dyn Store) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    let mut query = ThreadQuery::default();
    loop {
        let page = store.list_threads(&query).await.unwrap();
        ids.extend(page.threads.into_iter().map(|thread| thread.thread_id));
        match page.next_cursor {
            Some(cursor) => query = ThreadQuery::default().with_cursor(cursor),
            None => return ids,
        }
    }
}

// --- Writing ---

/// The older database: every lifecycle fixture, then the layout of the
/// checkpoints component's version 1.
async fn write_older(database: &Database) {
    database.create();
    let store = database.open().await;
    for (thread_id, checkpoint) in lifecycle() {
        let checkpoint: Checkpoint = serde_json::from_value(checkpoint).unwrap();
        store.save(checkpoint).await.unwrap();
        assert!(
            store.load(&thread_id).await.unwrap().is_some(),
            "{thread_id} was saved"
        );
    }
    drop(store);

    let version = scenario()["older"]["written_checkpoints_version"]
        .as_i64()
        .unwrap();
    database.execute(&format!(
        "DROP TABLE checkpoint_blobs;
         UPDATE strata_schema_version SET version = {version} WHERE component = 'checkpoints'"
    ));
    assert_eq!(database.layout(), (version, false), "{}", database.label());
}

/// The current database: a run with deltas and offload whose process dies.
async fn write_current(database: &Database) {
    let scenario = scenario();
    let current = &scenario["current"];
    let thread_id = current["thread_id"].as_str().unwrap();
    database.create();
    let store = database.open().await;
    let process = Arc::new(Process {
        inner: store,
        dead: AtomicBool::new(false),
    });
    let journal = Arc::new(Journal::default());
    *journal.dies_at.lock().unwrap() = Some((
        current["dies_at"].as_str().unwrap().to_string(),
        process.clone(),
    ));
    let nodes = current_nodes();
    let graph = line(
        &journal,
        &nodes.iter().map(String::as_str).collect::<Vec<_>>(),
    );
    let evidence = "e".repeat(current["evidence_bytes"].as_u64().unwrap() as usize);
    let result = graph
        .run(
            thread_id,
            Job {
                trail: String::new(),
                approved: false,
                limit: 0,
                evidence,
            },
            process.clone(),
        )
        .await;
    assert!(
        matches!(result, GraphExecutionResult::Failed(_)),
        "the writer's process died"
    );
    drop(process);

    let written = current["written"].as_array().unwrap();
    assert_eq!(
        database.versions(thread_id),
        written
            .iter()
            .map(|row| row["schema_version"].as_str().unwrap().to_string())
            .collect::<Vec<_>>(),
        "{}",
        database.label()
    );
}

// --- Reading ---

/// The other engine's older database: migrated on open, every checkpoint as
/// its fixture, and the threads `resumes` names finished.
async fn read_older(database: &Database) {
    let scenario = scenario();
    let older = &scenario["older"];
    let label = database.label();
    let written = older["written_checkpoints_version"].as_i64().unwrap();
    assert_eq!(database.layout(), (written, false), "{label} as written");

    let store = database.open().await;
    assert_eq!(
        database.layout(),
        (schema_versions::CHECKPOINTS_VERSION, true),
        "{label} once opened"
    );

    let fixtures = lifecycle();
    for (thread_id, expected) in &fixtures {
        let loaded = store
            .load(thread_id)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{label}: {thread_id} is missing"));
        assert_eq!(
            &serde_json::to_value(&loaded).unwrap(),
            expected,
            "{label}: {thread_id}"
        );
    }
    assert_eq!(
        thread_ids(&*store).await,
        fixtures.iter().map(|(id, _)| id.clone()).collect(),
        "{label}"
    );

    let as_store: Arc<dyn CheckpointStore> = store.clone();
    for resume in older["resumes"].as_array().unwrap() {
        let thread_id = resume["thread_id"].as_str().unwrap();
        let what = format!("{label}: {thread_id}");
        match thread_id {
            "lifecycle-frozen" | "lifecycle-identified" => {
                let options = if thread_id == "lifecycle-identified" {
                    durable()
                        .with_graph_id("wire-transfer")
                        .with_graph_version("2")
                } else {
                    durable()
                };
                let signal = &resume["signal"];
                let mut human = HumanSignal::approve(signal["approve"].as_str().unwrap());
                for (key, value) in signal["overrides"].as_object().unwrap() {
                    human = human.with_override(key.clone(), value.clone());
                }
                let state = completed(
                    &what,
                    wire_transfer(options)
                        .resume(thread_id, human, as_store.clone())
                        .await,
                );
                assert_eq!(
                    serde_json::to_value(state).unwrap(),
                    resume["state"],
                    "{what}"
                );
            }
            "lifecycle-running" | "lifecycle-claimed" => {
                let journal = Arc::new(Journal::default());
                let graph = if thread_id == "lifecycle-running" {
                    line(&journal, &["A", "B", "C"])
                } else {
                    approval(&journal)
                };
                let state = completed(&what, graph.recover(thread_id, as_store.clone()).await);
                assert_eq!(
                    serde_json::to_value(state).unwrap(),
                    resume["state"],
                    "{what}"
                );
                let recovered = &resume["recovered"];
                assert_eq!(
                    journal.visits()[0],
                    Visit {
                        node: recovered["node"].as_str().unwrap().to_string(),
                        key: recovered["key"].as_str().unwrap().to_string(),
                        recovery: true,
                        reviewer: recovered["reviewer"].as_str().map(str::to_string),
                    },
                    "{what}"
                );
            }
            other => panic!("the scenario resumes {other}, which this test doesn't know"),
        }
        let latest = store.load(thread_id).await.unwrap().unwrap();
        assert_eq!(latest.status, CheckpointStatus::Completed, "{what}");
    }
}

/// The other engine's current database: its rows decoded and its run
/// recovered to the end.
async fn read_current(database: &Database) {
    let scenario = scenario();
    let current = &scenario["current"];
    let thread_id = current["thread_id"].as_str().unwrap();
    let label = database.label();
    let written = current["written"].as_array().unwrap();
    assert_eq!(
        database.versions(thread_id),
        written
            .iter()
            .map(|row| row["schema_version"].as_str().unwrap().to_string())
            .collect::<Vec<_>>(),
        "{label} as written"
    );

    let store = database.open().await;
    let evidence = "e".repeat(current["evidence_bytes"].as_u64().unwrap() as usize);
    let log = store.get_log(thread_id, None, 100).await.unwrap().entries;
    assert_eq!(log.len(), written.len(), "{label}");
    for (entry, row) in log.iter().zip(written) {
        let checkpoint = &entry.checkpoint;
        let state: Job = serde_json::from_value(checkpoint.frozen_state.clone()).unwrap();
        assert_eq!(
            (
                serde_json::to_value(checkpoint.status).unwrap(),
                checkpoint.next_node.clone(),
                state.trail.as_str(),
                state.evidence == evidence,
            ),
            (
                row["status"].clone(),
                row["next_node"].as_str().map(str::to_string),
                row["trail"].as_str().unwrap(),
                true,
            ),
            "{label}: row {}",
            entry.sequence
        );
        // The engine never sees a store's encodings.
        assert!(
            !["1.8.0", "1.9.0"].contains(&checkpoint.schema_version.as_str()),
            "{label}: row {} decoded to {}",
            entry.sequence,
            checkpoint.schema_version
        );
    }

    let journal = Arc::new(Journal::default());
    let nodes = current_nodes();
    let graph = line(
        &journal,
        &nodes.iter().map(String::as_str).collect::<Vec<_>>(),
    );
    let as_store: Arc<dyn CheckpointStore> = store.clone();
    let state = completed(&label, graph.recover(thread_id, as_store).await);
    assert_eq!(
        state.trail,
        current["state"]["trail"].as_str().unwrap(),
        "{label}"
    );
    assert_eq!(state.evidence, evidence, "{label}");
    let visits = journal.visits();
    assert_eq!(
        visits.iter().map(|v| v.node.as_str()).collect::<Vec<_>>(),
        current["visits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect::<Vec<_>>(),
        "{label}"
    );
    let recovered = &current["recovered"];
    assert_eq!(
        (visits[0].key.as_str(), visits[0].recovery),
        (recovered["key"].as_str().unwrap(), true),
        "{label}"
    );
    let latest = store.load(thread_id).await.unwrap().unwrap();
    assert_eq!(latest.status, CheckpointStatus::Completed, "{label}");
}

// --- The steps ---

/// The databases `engine` writes: SQLite in `dir`, and Postgres when there is
/// one.
fn databases(
    dir: &Path,
    postgres: Option<&(String, String)>,
    engine: &str,
) -> Vec<(&'static str, Database)> {
    let mut all = vec![
        (OLDER, Database::sqlite(dir, engine, OLDER)),
        (CURRENT, Database::sqlite(dir, engine, CURRENT)),
    ];
    if let Some((url, prefix)) = postgres {
        all.push((OLDER, Database::postgres(url, prefix, engine, OLDER)));
        all.push((CURRENT, Database::postgres(url, prefix, engine, CURRENT)));
    }
    all
}

async fn write(dir: &Path, postgres: Option<&(String, String)>) {
    for (kind, database) in databases(dir, postgres, ENGINE) {
        match kind {
            OLDER => write_older(&database).await,
            _ => write_current(&database).await,
        }
    }
}

async fn read(dir: &Path, postgres: Option<&(String, String)>, engine: &str) {
    for (kind, database) in databases(dir, postgres, engine) {
        match kind {
            OLDER => read_older(&database).await,
            _ => read_current(&database).await,
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_engine_reads_migrates_and_resumes_what_the_other_wrote() {
    let step = std::env::var("STRATA_INTEROP_STEP").unwrap_or_default();
    match std::env::var("STRATA_INTEROP_DIR") {
        Ok(dir) if !dir.is_empty() => {
            // A step of the cross-engine job: Postgres is part of it.
            let url = std::env::var(POSTGRES_VAR)
                .unwrap_or_else(|_| panic!("a cross-engine step needs {POSTGRES_VAR}"));
            let postgres = (url, "strata_interop".to_string());
            let dir = PathBuf::from(dir);
            std::fs::create_dir_all(&dir).unwrap();
            match step.as_str() {
                "write" => write(&dir, Some(&postgres)).await,
                "read" => read(&dir, Some(&postgres), OTHER).await,
                other => panic!("STRATA_INTEROP_STEP is {other:?}: expected write or read"),
            }
        }
        _ => {
            // On its own: this engine reads what it wrote.
            let dir = std::env::temp_dir().join(unique_name("cross_engine"));
            std::fs::create_dir_all(&dir).unwrap();
            let postgres = backend_url(POSTGRES_VAR).map(|url| (url, unique_name("interop")));
            write(&dir, postgres.as_ref()).await;
            read(&dir, postgres.as_ref(), ENGINE).await;
            for (_, database) in databases(&dir, postgres.as_ref(), ENGINE) {
                database.remove();
            }
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}
