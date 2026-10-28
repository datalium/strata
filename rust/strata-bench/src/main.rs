//! The profile (`design/37-Benchmarks.md`, SG-27): what the microbenchmarks'
//! means don't say. How the engine's latency is distributed (P50 to P99.9),
//! how many transitions a second it makes under concurrency, what a suspended
//! thread holds in memory, and what each store does: saves and loads a second,
//! and a run that checkpoints every node. The .NET twin is
//! `dotnet/benchmarks/Datalium.Strata.Benchmarks/Profile.cs` — keep the two in
//! step: same scenarios, same variants, same metric names.
//!
//!   cargo run --release -p strata-bench                        # everything
//!   cargo run --release -p strata-bench -- --only throughput   # one scenario
//!   cargo run --release -p strata-bench -- --out rust.json     # also as JSON
//!   cargo run --release -p strata-bench -- --quick             # small counts: proves it runs, measures nothing
//!
//! The Postgres and Redis rows need `STRATA_TEST_POSTGRES_URL` /
//! `STRATA_TEST_REDIS_URL` (the repo's `docker-compose.yml`), the variables
//! the tests read; without one, that store is left out and the profile says so.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use datalium_strata::checkpoint::CheckpointStore;
use datalium_strata::graph::{CompileOptions, CompiledGraph};
use datalium_strata::schema::{Checkpoint, CheckpointStatus, RUNNING_CHECKPOINT_SCHEMA_VERSION};
use datalium_strata_memory::checkpoint::{InMemoryCheckpointStore, SqliteCheckpointStore};
use datalium_strata_memory::group_commit::{BLOB_SIZE_METRIC, CHECKPOINT_SIZE_METRIC};
use datalium_strata_memory::postgres_store::PostgresCheckpointStore;
use datalium_strata_memory::redis_store::RedisCheckpointStore;
use datalium_strata_memory::{
    Deltas, GroupCommit, Offload, PostgresStoreOptions, RedisStoreOptions, SqliteStoreOptions,
};
use serde_json::{Map, Value, json};
use strata_bench::stats::Distribution;
use strata_bench::{
    Gaps, Handler, PAYLOAD_BYTES, RUN_NODES, Work, approval, completed, durable, interrupted,
    linear, sample_checkpoint, timed, work,
};

const POSTGRES_VAR: &str = "STRATA_TEST_POSTGRES_URL";
const REDIS_VAR: &str = "STRATA_TEST_REDIS_URL";
const SCENARIOS: [&str; 6] = [
    "transition_latency",
    "run_latency",
    "throughput",
    "suspended_memory",
    "stores",
    "large_state",
];

/// Counts the bytes allocated and freed while [`COUNTING`] is on, so the
/// memory scenario can say what a suspended thread keeps alive. Off, it costs
/// a load of a flag nobody writes, which is why the other scenarios can share
/// the binary.
struct CountingAllocator;

static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCATED: AtomicU64 = AtomicU64::new(0);
static FREED: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCATED.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if COUNTING.load(Ordering::Relaxed) {
            FREED.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            FREED.fetch_add(layout.size() as u64, Ordering::Relaxed);
            ALLOCATED.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// The bytes still allocated of those allocated since counting was turned on.
fn live_bytes() -> i64 {
    ALLOCATED.load(Ordering::Relaxed) as i64 - FREED.load(Ordering::Relaxed) as i64
}

struct Options {
    quick: bool,
    out: Option<PathBuf>,
    only: Vec<String>,
}

impl Options {
    fn parse() -> Result<Self, String> {
        let mut options = Options {
            quick: false,
            out: None,
            only: Vec::new(),
        };
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--quick" => options.quick = true,
                "--out" => {
                    options.out = Some(PathBuf::from(args.next().ok_or("--out needs a path")?))
                }
                "--only" => {
                    for name in args.next().ok_or("--only needs a scenario")?.split(',') {
                        if !SCENARIOS.contains(&name) {
                            return Err(format!(
                                "unknown scenario '{name}': one of {}",
                                SCENARIOS.join(", ")
                            ));
                        }
                        options.only.push(name.to_string());
                    }
                }
                other => return Err(format!("unknown argument '{other}'")),
            }
        }
        Ok(options)
    }

    fn runs(&self, scenario: &str) -> bool {
        self.only.is_empty() || self.only.iter().any(|name| name == scenario)
    }

    /// `full`, or `quick` under `--quick`.
    fn count(&self, full: usize, quick: usize) -> usize {
        if self.quick { quick } else { full }
    }
}

/// One line of the report: a scenario, the variant of it, and what was
/// measured. A metric's name ends with its unit (`_ns`, `_per_sec`, `_bytes`).
struct Row {
    scenario: &'static str,
    variant: String,
    metrics: Vec<(&'static str, f64)>,
}

#[derive(Default)]
struct Report {
    rows: Vec<Row>,
    notes: Vec<String>,
}

impl Report {
    fn add(
        &mut self,
        scenario: &'static str,
        variant: impl Into<String>,
        metrics: Vec<(&'static str, f64)>,
    ) {
        let row = Row {
            scenario,
            variant: variant.into(),
            metrics,
        };
        eprintln!(
            "  {} {}: {}",
            row.scenario,
            row.variant,
            shown(&row.metrics)
        );
        self.rows.push(row);
    }

    fn note(&mut self, note: impl Into<String>) {
        let note = note.into();
        eprintln!("  note: {note}");
        self.notes.push(note);
    }
}

/// A latency distribution as metrics in nanoseconds.
fn latency(distribution: &Distribution) -> Vec<(&'static str, f64)> {
    vec![
        ("samples", distribution.samples as f64),
        ("mean_ns", distribution.mean),
        ("p50_ns", distribution.p50 as f64),
        ("p90_ns", distribution.p90 as f64),
        ("p99_ns", distribution.p99 as f64),
        ("p999_ns", distribution.p999 as f64),
        ("max_ns", distribution.max as f64),
    ]
}

/// A duration for a reader: `812 ns`, `4.31 µs`, `1.20 ms`.
fn duration(nanos: f64) -> String {
    if nanos < 1_000.0 {
        format!("{nanos:.0} ns")
    } else if nanos < 1_000_000.0 {
        format!("{:.2} µs", nanos / 1_000.0)
    } else {
        format!("{:.2} ms", nanos / 1_000_000.0)
    }
}

/// A count with thousands separators.
fn grouped(value: f64) -> String {
    let digits = format!("{:.0}", value.abs());
    let mut out = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            out.push(',');
        }
        out.push(digit);
    }
    if value < 0.0 {
        out.insert(0, '-');
    }
    out
}

fn shown(metrics: &[(&'static str, f64)]) -> String {
    metrics
        .iter()
        .map(|(name, value)| match name.strip_suffix("_ns") {
            Some(stem) => format!("{stem} {}", duration(*value)),
            None => format!("{name} {}", grouped(*value)),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn elapsed_nanos(since: Instant) -> u64 {
    since.elapsed().as_nanos() as u64
}

/// Runs `graph` for a second before anything is measured (a tenth of one under
/// `--quick`), so that no sample is among the code's first executions. The
/// .NET profile needs it for its JIT, which compiles a method again once it
/// has run for a while; this one does the same so the two stay alike.
async fn warm(graph: &CompiledGraph<Work>, store: &Arc<dyn CheckpointStore>, options: &Options) {
    let until = Instant::now() + Duration::from_millis(options.count(1_000, 100) as u64);
    while Instant::now() < until {
        completed(graph.run("warm", work(0), store.clone()).await);
    }
}

/// The gap between one handler returning and the next starting, over runs of
/// a hundred-node line: the engine's scheduling overhead per transition.
async fn transition_latency(options: &Options, report: &mut Report) {
    const NODES: usize = 100;
    let runs = options.count(2_000, 50);
    let store: Arc<dyn CheckpointStore> = strata_bench::LatestOnlyStore::new();
    for handler in Handler::ALL {
        let gaps = Gaps::new();
        let graph = timed(NODES, handler, &gaps);
        warm(&graph, &store, options).await;
        gaps.take();
        for _ in 0..runs {
            completed(graph.run("bench", work(0), store.clone()).await);
        }
        report.add(
            "transition_latency",
            handler.name(),
            latency(&Distribution::of(gaps.take())),
        );
    }
}

/// A whole run of the ten-node line, one after another.
async fn run_latency(options: &Options, report: &mut Report) {
    let store: Arc<dyn CheckpointStore> = strata_bench::LatestOnlyStore::new();
    for handler in Handler::ALL {
        let runs = match handler {
            Handler::Async | Handler::Inline => options.count(100_000, 2_000),
            Handler::Sync => options.count(20_000, 500),
        };
        let graph = linear(RUN_NODES, handler, CompileOptions::new());
        warm(&graph, &store, options).await;
        let mut samples = Vec::with_capacity(runs);
        for _ in 0..runs {
            let started = Instant::now();
            completed(graph.run("bench", work(0), store.clone()).await);
            samples.push(elapsed_nanos(started));
        }
        report.add(
            "run_latency",
            format!("{}/{RUN_NODES}_nodes", handler.name()),
            latency(&Distribution::of(samples)),
        );
    }
}

/// Transitions a second with 1, 4, 16 and 64 ten-node runs going at once.
async fn throughput(options: &Options, report: &mut Report) {
    let window = Duration::from_millis(options.count(2_000, 200) as u64);
    let store: Arc<dyn CheckpointStore> = strata_bench::LatestOnlyStore::new();
    for handler in Handler::ALL {
        let graph = Arc::new(linear(RUN_NODES, handler, CompileOptions::new()));
        for concurrency in [1usize, 4, 16, 64] {
            let runs = Arc::new(AtomicU64::new(0));
            let started = Instant::now();
            let deadline = started + window;
            let tasks: Vec<_> = (0..concurrency)
                .map(|_| {
                    let (graph, store, runs) = (graph.clone(), store.clone(), runs.clone());
                    tokio::spawn(async move {
                        while Instant::now() < deadline {
                            completed(graph.run("bench", work(0), store.clone()).await);
                            runs.fetch_add(1, Ordering::Relaxed);
                        }
                    })
                })
                .collect();
            for task in tasks {
                task.await.expect("a run's task");
            }
            let seconds = started.elapsed().as_secs_f64();
            let runs = runs.load(Ordering::Relaxed) as f64;
            report.add(
                "throughput",
                format!("{}/{concurrency}_concurrent", handler.name()),
                vec![
                    ("transitions_per_sec", runs * RUN_NODES as f64 / seconds),
                    ("runs_per_sec", runs / seconds),
                ],
            );
        }
    }
}

/// A folder of the profile's own under the system's temporary one.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("strata-bench-{}", std::process::id()));
        std::fs::create_dir_all(&path).expect("a scratch folder");
        Self(path)
    }

    fn file(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The size of a SQLite database: its file and its write-ahead log, if any.
fn database_bytes(path: &std::path::Path) -> u64 {
    let size = |path: &std::path::Path| std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    let mut wal = path.as_os_str().to_owned();
    wal.push("-wal");
    size(path) + size(std::path::Path::new(&wal))
}

/// Threads frozen at an interrupt boundary with a 1 KiB state: the bytes each
/// keeps allocated in the process, and on disk for SQLite. The in-memory
/// store's bytes are the checkpoint itself; with a real store the process
/// keeps nothing.
async fn suspended_memory(options: &Options, scratch: &Scratch, report: &mut Report) {
    let threads = options.count(10_000, 200);
    let graph = approval();
    let checkpoint_bytes = serde_json::to_string(&sample_checkpoint().await)
        .expect("a checkpoint is JSON")
        .len() as f64;

    let path = scratch.file("suspended.db");
    let stores: [(&str, Arc<dyn CheckpointStore>); 2] = [
        ("memory", Arc::new(InMemoryCheckpointStore::new())),
        (
            "sqlite",
            Arc::new(SqliteCheckpointStore::open(&path).expect("a SQLite file")),
        ),
    ];
    for (name, store) in stores {
        // Whatever the first runs set up once is not a thread's.
        for index in 0..8 {
            interrupted(
                graph
                    .run(format!("warm-{index}"), work(PAYLOAD_BYTES), store.clone())
                    .await,
            );
        }
        let disk_before = database_bytes(&path);
        COUNTING.store(true, Ordering::SeqCst);
        let before = live_bytes();
        for index in 0..threads {
            interrupted(
                graph
                    .run(format!("t-{index}"), work(PAYLOAD_BYTES), store.clone())
                    .await,
            );
        }
        let held = live_bytes() - before;
        COUNTING.store(false, Ordering::SeqCst);

        let mut metrics = vec![
            ("threads", threads as f64),
            ("heap_bytes_per_thread", held as f64 / threads as f64),
            ("checkpoint_json_bytes", checkpoint_bytes),
        ];
        if name == "sqlite" {
            let disk = database_bytes(&path) - disk_before;
            metrics.push(("disk_bytes_per_thread", disk as f64 / threads as f64));
        }
        report.add("suspended_memory", name, metrics);
    }
    report.note(
        "suspended_memory: heap bytes are what Rust's allocator handed out and didn't get back; SQLite's own page cache is not counted",
    );
}

/// A store the profile measures, how much to ask of it, and how to clean up.
struct Target {
    name: &'static str,
    store: Arc<dyn CheckpointStore>,
    operations: usize,
    runs: usize,
    /// The same store with group commit on (`design/41-Checkpoint-Efficiency.md`),
    /// where it has it.
    batched: Option<Arc<dyn CheckpointStore>>,
}

/// The writer counts `store_save` measures at once.
const WRITER_COUNTS: [usize; 3] = [16, 64, 256];

/// The concurrent runs `durable_throughput` measures.
const DURABLE_WORKERS: usize = 64;

/// A Postgres schema of the profile's own, dropped when the profile is done.
struct PostgresSandbox {
    url: String,
    schema: String,
}

impl PostgresSandbox {
    async fn create(url: &str) -> Result<Self, String> {
        Self::create_named(url, "").await
    }

    /// A schema of its own for a scenario: `strata_bench_{pid}_{name}`.
    async fn create_named(url: &str, name: &str) -> Result<Self, String> {
        let schema = if name.is_empty() {
            format!("strata_bench_{}", std::process::id())
        } else {
            format!("strata_bench_{}_{name}", std::process::id())
        };
        let admin = datalium_strata_memory::postgres_store::connect(url).await?;
        admin
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .map_err(|e| e.to_string())?;
        Ok(Self {
            url: url.to_string(),
            schema,
        })
    }

    /// A URL whose connections work in the sandbox's schema.
    fn scoped_url(&self) -> String {
        let separator = if self.url.contains('?') { '&' } else { '?' };
        format!(
            "{}{separator}options=-c%20search_path%3D{}",
            self.url, self.schema
        )
    }

    async fn drop_schema(&self) {
        if let Ok(admin) = datalium_strata_memory::postgres_store::connect(&self.url).await {
            let _ = admin
                .batch_execute(&format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema))
                .await;
        }
    }
}

/// Deletes every key under `prefix`.
async fn sweep_redis(url: &str, prefix: &str) {
    let Ok(client) = redis::Client::open(url) else {
        return;
    };
    let Ok(mut connection) = client.get_multiplexed_async_connection().await else {
        return;
    };
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{prefix}:*"))
        .query_async(&mut connection)
        .await
        .unwrap_or_default();
    for chunk in keys.chunks(500) {
        let _: Result<i64, _> = redis::cmd("DEL")
            .arg(chunk)
            .query_async(&mut connection)
            .await;
    }
}

fn backend_url(var: &str, report: &mut Report) -> Option<String> {
    match std::env::var(var) {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            report.note(format!(
                "stores: {var} is not set, so that store was not measured"
            ));
            None
        }
    }
}

/// Sums `strata.store.checkpoint.size`, the JSON each save stored, while
/// [`MEASURING_STORED`] is on: the profile's global recorder. Off, every
/// metric is a no-op handle, as with no recorder, so the other scenarios pay
/// for a flag nobody writes.
struct StoredBytes;

static MEASURING_STORED: AtomicBool = AtomicBool::new(false);
static STORED_BYTES: AtomicU64 = AtomicU64::new(0);
static STORED_SAVES: AtomicU64 = AtomicU64::new(0);

struct StoredHistogram;

impl metrics::HistogramFn for StoredHistogram {
    fn record(&self, value: f64) {
        STORED_BYTES.fetch_add(value as u64, Ordering::Relaxed);
        STORED_SAVES.fetch_add(1, Ordering::Relaxed);
    }
}

/// An offloaded value's bytes, counted with the save that wrote it.
struct BlobHistogram;

impl metrics::HistogramFn for BlobHistogram {
    fn record(&self, value: f64) {
        STORED_BYTES.fetch_add(value as u64, Ordering::Relaxed);
    }
}

impl metrics::Recorder for StoredBytes {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn register_counter(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        metrics::Counter::noop()
    }

    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }

    fn register_histogram(
        &self,
        key: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        match key.name() {
            _ if !MEASURING_STORED.load(Ordering::Relaxed) => metrics::Histogram::noop(),
            CHECKPOINT_SIZE_METRIC => metrics::Histogram::from_arc(Arc::new(StoredHistogram)),
            BLOB_SIZE_METRIC => metrics::Histogram::from_arc(Arc::new(BlobHistogram)),
            _ => metrics::Histogram::noop(),
        }
    }
}

/// The mean bytes a save stored since the last call, the blobs it offloaded
/// included.
fn take_stored_mean() -> f64 {
    let bytes = STORED_BYTES.swap(0, Ordering::Relaxed);
    let saves = STORED_SAVES.swap(0, Ordering::Relaxed);
    if saves == 0 {
        0.0
    } else {
        bytes as f64 / saves as f64
    }
}

/// The payload `store_save_large` and `store_load_chain` carry: 64 KiB that
/// never changes.
const LARGE_PAYLOAD_BYTES: usize = 64 * 1024;

/// The rows of a thread `store_save_large` writes, as a run of that many
/// nodes would.
const LARGE_CHAIN: usize = 20;

/// The rows `store_load_chain`'s thread has: with deltas, its latest is 15
/// deltas from a whole row.
const CHAIN_ROWS: usize = 16;

/// What a durable run of the `Work` state with `payload` writes after node
/// `step`.
fn large_running(thread_id: String, step: usize, payload: &str) -> Checkpoint {
    Checkpoint {
        schema_version: RUNNING_CHECKPOINT_SCHEMA_VERSION.to_string(),
        thread_id,
        status: CheckpointStatus::Running,
        suspended_node: Some(format!("N{step}")),
        next_node: Some(format!("N{}", step + 1)),
        frozen_state: json!({ "step": step, "payload": payload }),
        metadata: Some(HashMap::from([(
            "strata.hops".to_string(),
            step.to_string(),
        )])),
        created_at: "2026-10-15T00:00:00Z".to_string(),
        resumed_at: None,
        nested: None,
        decision: None,
        wait: None,
        graph: None,
    }
}

/// How `large_state` stores the state: whole, as deltas, with the payload
/// offloaded (to the store's own database), and both.
const LARGE_VARIANTS: [(&str, &str); 4] = [
    ("whole", "whole"),
    ("delta", "15_deltas"),
    ("offload", "offloaded"),
    ("delta_offload", "15_deltas_offloaded"),
];

/// What `large_state` turns on for `variant`: deltas, offload, or both.
fn large_options(variant: &str) -> (Option<Deltas>, Option<Offload>) {
    (
        variant.contains("delta").then(Deltas::new),
        variant.contains("offload").then(Offload::new),
    )
}

/// A store `large_state` measures, once per [`LARGE_VARIANTS`].
struct LargeTarget {
    name: &'static str,
    stores: Vec<Arc<dyn CheckpointStore>>,
    saves: usize,
    loads: usize,
}

/// A state whose 64 KiB payload never changes and whose step does
/// (`design/41-Checkpoint-Efficiency.md`, "Measuring"): `store_save_large`
/// saves it as a run would, a running checkpoint per node, stored whole, as
/// deltas, with the payload offloaded, and as deltas with it offloaded;
/// `store_load_chain` loads a thread whose latest row is whole, one whose
/// latest is 15 deltas from a whole one, and each with the payload offloaded.
async fn large_state(options: &Options, scratch: &Scratch, report: &mut Report) {
    let (saves, loads) = (options.count(400, LARGE_CHAIN), options.count(500, 10));
    let (remote_saves, remote_loads) = (options.count(200, LARGE_CHAIN), options.count(200, 10));
    let mut sqlite = Vec::new();
    for (variant, _) in LARGE_VARIANTS {
        let (deltas, offload) = large_options(variant);
        let mut opened = SqliteStoreOptions::new();
        if let Some(deltas) = deltas {
            opened = opened.with_deltas(deltas);
        }
        if let Some(offload) = offload {
            opened = opened.with_offload(offload);
        }
        let store: Arc<dyn CheckpointStore> = Arc::new(
            SqliteCheckpointStore::open_with(scratch.file(&format!("large-{variant}.db")), opened)
                .expect("a SQLite file"),
        );
        sqlite.push(store);
    }
    let mut targets = vec![LargeTarget {
        name: "sqlite",
        stores: sqlite,
        saves,
        loads,
    }];
    let mut postgres = None;
    if let Some(url) = backend_url(POSTGRES_VAR, report) {
        let sandbox = PostgresSandbox::create_named(&url, "large")
            .await
            .expect("a schema");
        let mut stores: Vec<Arc<dyn CheckpointStore>> = Vec::new();
        for (variant, _) in LARGE_VARIANTS {
            let (deltas, offload) = large_options(variant);
            let mut opened = PostgresStoreOptions::new();
            if let Some(deltas) = deltas {
                opened = opened.with_deltas(deltas);
            }
            if let Some(offload) = offload {
                opened = opened.with_offload(offload);
            }
            stores.push(Arc::new(
                PostgresCheckpointStore::connect_with(&sandbox.scoped_url(), opened)
                    .await
                    .expect("a Postgres store"),
            ));
        }
        targets.push(LargeTarget {
            name: "postgres",
            stores,
            saves: remote_saves,
            loads: remote_loads,
        });
        postgres = Some(sandbox);
    }
    let mut redis = None;
    if let Some(url) = backend_url(REDIS_VAR, report) {
        let prefix = format!("strata_bench_{}_large", std::process::id());
        let mut stores: Vec<Arc<dyn CheckpointStore>> = Vec::new();
        for (variant, _) in LARGE_VARIANTS {
            let (deltas, offload) = large_options(variant);
            let mut opened = RedisStoreOptions::new().with_prefix(&prefix);
            if let Some(deltas) = deltas {
                opened = opened.with_deltas(deltas);
            }
            if let Some(offload) = offload {
                opened = opened.with_offload(offload);
            }
            stores.push(Arc::new(
                RedisCheckpointStore::connect_with(&url, opened)
                    .await
                    .expect("a Redis store"),
            ));
        }
        targets.push(LargeTarget {
            name: "redis",
            stores,
            saves: remote_saves,
            loads: remote_loads,
        });
        redis = Some((url, prefix));
    }

    let payload = "x".repeat(LARGE_PAYLOAD_BYTES);
    MEASURING_STORED.store(true, Ordering::Relaxed);
    for target in &targets {
        for ((variant, shape), store) in LARGE_VARIANTS.iter().zip(&target.stores) {
            for step in 0..LARGE_CHAIN {
                store
                    .save(large_running(
                        format!("large-warm-{variant}"),
                        step,
                        &payload,
                    ))
                    .await
                    .expect("a save");
            }

            take_stored_mean();
            let started = Instant::now();
            for index in 0..target.saves {
                store
                    .save(large_running(
                        format!("large-{variant}-{}", index / LARGE_CHAIN),
                        index % LARGE_CHAIN,
                        &payload,
                    ))
                    .await
                    .expect("a save");
            }
            let seconds = started.elapsed().as_secs_f64();
            report.add(
                "store_save_large",
                format!("{}/{variant}", target.name),
                vec![
                    ("ops_per_sec", target.saves as f64 / seconds),
                    ("stored_bytes", take_stored_mean()),
                ],
            );

            let thread_id = format!("chain-{variant}");
            for step in 0..CHAIN_ROWS {
                store
                    .save(large_running(thread_id.clone(), step, &payload))
                    .await
                    .expect("a save");
            }
            let mut samples = Vec::with_capacity(target.loads);
            for _ in 0..target.loads {
                let started = Instant::now();
                let latest = store.load_latest(&thread_id).await.expect("a load");
                samples.push(elapsed_nanos(started));
                assert_eq!(
                    latest.and_then(|latest| latest.sequence),
                    Some(CHAIN_ROWS as i64 - 1),
                    "{thread_id} has {CHAIN_ROWS} rows"
                );
            }
            let total: u64 = samples.iter().sum();
            let distribution = Distribution::of(samples);
            report.add(
                "store_load_chain",
                format!("{}/{shape}", target.name),
                vec![
                    ("ops_per_sec", target.loads as f64 * 1e9 / total as f64),
                    ("p50_ns", distribution.p50 as f64),
                    ("p99_ns", distribution.p99 as f64),
                ],
            );
        }
    }
    MEASURING_STORED.store(false, Ordering::Relaxed);

    drop(targets);
    if let Some(sandbox) = postgres {
        sandbox.drop_schema().await;
    }
    if let Some((url, prefix)) = redis {
        sweep_redis(&url, &prefix).await;
    }
}

/// `checkpoint` under another thread id.
fn renamed(checkpoint: &Checkpoint, thread_id: String) -> Checkpoint {
    Checkpoint {
        thread_id,
        ..checkpoint.clone()
    }
}

/// Each store's saves and loads of a 1 KiB checkpoint, and a ten-node run that
/// checkpoints after every node (eleven saves).
async fn stores(options: &Options, scratch: &Scratch, report: &mut Report) {
    const WARM_UP: usize = 50;
    let (local, remote) = (options.count(20_000, 200), options.count(2_000, 50));
    let (local_runs, remote_runs) = (options.count(2_000, 20), options.count(200, 5));

    let mut targets = vec![
        Target {
            name: "memory",
            store: Arc::new(InMemoryCheckpointStore::new()),
            operations: local,
            runs: local_runs,
            batched: None,
        },
        Target {
            name: "sqlite",
            store: Arc::new(
                SqliteCheckpointStore::open(scratch.file("stores.db")).expect("a SQLite file"),
            ),
            operations: remote,
            runs: remote_runs,
            batched: Some(Arc::new(
                SqliteCheckpointStore::open_with(
                    scratch.file("stores-batched.db"),
                    SqliteStoreOptions::new().with_group_commit(GroupCommit::new()),
                )
                .expect("a SQLite file"),
            )),
        },
    ];
    let mut postgres = None;
    if let Some(url) = backend_url(POSTGRES_VAR, report) {
        let sandbox = PostgresSandbox::create(&url).await.expect("a schema");
        let store = PostgresCheckpointStore::connect(&sandbox.scoped_url())
            .await
            .expect("a Postgres store");
        let batched = PostgresCheckpointStore::connect_with(
            &sandbox.scoped_url(),
            PostgresStoreOptions::new().with_group_commit(GroupCommit::new()),
        )
        .await
        .expect("a Postgres store");
        targets.push(Target {
            name: "postgres",
            store: Arc::new(store),
            operations: remote,
            runs: remote_runs,
            batched: Some(Arc::new(batched)),
        });
        postgres = Some(sandbox);
    }
    let mut redis = None;
    if let Some(url) = backend_url(REDIS_VAR, report) {
        let prefix = format!("strata_bench_{}", std::process::id());
        let store = RedisCheckpointStore::connect_with_prefix(&url, &prefix)
            .await
            .expect("a Redis store");
        targets.push(Target {
            name: "redis",
            store: Arc::new(store),
            operations: remote,
            runs: remote_runs,
            batched: None,
        });
        redis = Some((url, prefix));
    }

    let template = sample_checkpoint().await;
    let graph = Arc::new(durable());
    for target in &targets {
        let store = &target.store;

        // The store's first calls are not measured: they open, prepare and cache.
        for index in 0..WARM_UP {
            let thread_id = format!("warm-{index}");
            store
                .save(renamed(&template, thread_id.clone()))
                .await
                .expect("a save");
            store.load_latest(&thread_id).await.expect("a load");
        }
        for index in 0..3 {
            completed(
                graph
                    .run(
                        format!("warm-durable-{index}"),
                        work(PAYLOAD_BYTES),
                        store.clone(),
                    )
                    .await,
            );
        }

        // One writer, a thread per save.
        let checkpoints: Vec<Checkpoint> = (0..target.operations)
            .map(|index| renamed(&template, format!("one-{index}")))
            .collect();
        let mut samples = Vec::with_capacity(target.operations);
        for checkpoint in checkpoints {
            let started = Instant::now();
            store.save(checkpoint).await.expect("a save");
            samples.push(elapsed_nanos(started));
        }
        let total: u64 = samples.iter().sum();
        let distribution = Distribution::of(samples);
        report.add(
            "store_save",
            format!("{}/1_writer", target.name),
            vec![
                ("ops_per_sec", target.operations as f64 * 1e9 / total as f64),
                ("p50_ns", distribution.p50 as f64),
                ("p99_ns", distribution.p99 as f64),
            ],
        );

        // Sixteen writers at once, each with threads of its own; more come
        // after the durable run, so what they leave in the store doesn't weigh
        // on it.
        save_concurrently(
            store,
            &template,
            target,
            WRITER_COUNTS[0],
            "many",
            "",
            options,
            report,
        )
        .await;

        // One reader, the latest checkpoint of each thread saved above.
        let mut samples = Vec::with_capacity(target.operations);
        for index in 0..target.operations {
            let thread_id = format!("one-{index}");
            let started = Instant::now();
            let latest = store.load_latest(&thread_id).await.expect("a load");
            samples.push(elapsed_nanos(started));
            assert!(latest.is_some(), "{thread_id} was saved");
        }
        let total: u64 = samples.iter().sum();
        let distribution = Distribution::of(samples);
        report.add(
            "store_load",
            format!("{}/1_reader", target.name),
            vec![
                ("ops_per_sec", target.operations as f64 * 1e9 / total as f64),
                ("p50_ns", distribution.p50 as f64),
                ("p99_ns", distribution.p99 as f64),
            ],
        );

        // A run that checkpoints after every node.
        let mut samples = Vec::with_capacity(target.runs);
        for index in 0..target.runs {
            let started = Instant::now();
            completed(
                graph
                    .run(
                        format!("durable-{index}"),
                        work(PAYLOAD_BYTES),
                        store.clone(),
                    )
                    .await,
            );
            samples.push(elapsed_nanos(started));
        }
        let distribution = Distribution::of(samples);
        report.add(
            "durable_run",
            format!("{}/{RUN_NODES}_nodes", target.name),
            vec![
                ("mean_ns", distribution.mean),
                ("p50_ns", distribution.p50 as f64),
                ("p99_ns", distribution.p99 as f64),
                ("checkpoints_per_run", RUN_NODES as f64 + 1.0),
            ],
        );
        for writers in &WRITER_COUNTS[1..] {
            save_concurrently(
                store, &template, target, *writers, "many", "", options, report,
            )
            .await;
        }
        durable_throughput(store, &graph, target, "durable", "", report).await;

        // The same store with group commit on: its saves and runs, measured
        // the same way.
        if let Some(batched) = &target.batched {
            for index in 0..WARM_UP {
                batched
                    .save(renamed(&template, format!("batched-warm-{index}")))
                    .await
                    .expect("a save");
            }
            save_concurrently(
                batched,
                &template,
                target,
                1,
                "batched-one",
                "_batched",
                options,
                report,
            )
            .await;
            for writers in WRITER_COUNTS {
                save_concurrently(
                    batched,
                    &template,
                    target,
                    writers,
                    "batched-many",
                    "_batched",
                    options,
                    report,
                )
                .await;
            }
            durable_throughput(
                batched,
                &graph,
                target,
                "batched-durable",
                "_batched",
                report,
            )
            .await;
        }
    }

    drop(targets);
    if let Some(sandbox) = postgres {
        sandbox.drop_schema().await;
    }
    if let Some((url, prefix)) = redis {
        sweep_redis(&url, &prefix).await;
    }
}

/// `writers` writers saving at once, each a thread per save: `store_save`'s
/// `{store}/{writers}_writers{suffix}`. Each writer saves at least 32
/// checkpoints (one in a quick run), so the many-writer cases last long
/// enough to measure.
#[allow(clippy::too_many_arguments)]
async fn save_concurrently(
    store: &Arc<dyn CheckpointStore>,
    template: &Checkpoint,
    target: &Target,
    writers: usize,
    prefix: &str,
    suffix: &str,
    options: &Options,
    report: &mut Report,
) {
    let per_writer = (target.operations / writers).max(options.count(32, 1));
    let batches: Vec<Vec<Checkpoint>> = (0..writers)
        .map(|writer| {
            (0..per_writer)
                .map(|index| renamed(template, format!("{prefix}-{writers}-{writer}-{index}")))
                .collect()
        })
        .collect();
    let started = Instant::now();
    let tasks: Vec<_> = batches
        .into_iter()
        .map(|batch| {
            let store = store.clone();
            tokio::spawn(async move {
                for checkpoint in batch {
                    store.save(checkpoint).await.expect("a save");
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.expect("a writer's task");
    }
    let variant = if writers == 1 {
        format!("{}/1_writer{suffix}", target.name)
    } else {
        format!("{}/{writers}_writers{suffix}", target.name)
    };
    report.add(
        "store_save",
        variant,
        vec![(
            "ops_per_sec",
            (per_writer * writers) as f64 / started.elapsed().as_secs_f64(),
        )],
    );
}

/// [`DURABLE_WORKERS`] workers each running durable ten-node runs one after
/// another: `durable_throughput`'s `{store}/64_concurrent{suffix}`, in runs and
/// saves a second.
async fn durable_throughput(
    store: &Arc<dyn CheckpointStore>,
    graph: &Arc<CompiledGraph<Work>>,
    target: &Target,
    prefix: &str,
    suffix: &str,
    report: &mut Report,
) {
    let per_worker = (target.runs / 16).max(1);
    let started = Instant::now();
    let tasks: Vec<_> = (0..DURABLE_WORKERS)
        .map(|worker| {
            let (store, graph, prefix) = (store.clone(), graph.clone(), prefix.to_string());
            tokio::spawn(async move {
                for index in 0..per_worker {
                    completed(
                        graph
                            .run(
                                format!("{prefix}-{worker}-{index}"),
                                work(PAYLOAD_BYTES),
                                store.clone(),
                            )
                            .await,
                    );
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.expect("a worker's task");
    }
    let seconds = started.elapsed().as_secs_f64();
    let runs = (per_worker * DURABLE_WORKERS) as f64;
    report.add(
        "durable_throughput",
        format!("{}/{DURABLE_WORKERS}_concurrent{suffix}", target.name),
        vec![
            ("runs_per_sec", runs / seconds),
            ("saves_per_sec", runs * (RUN_NODES as f64 + 1.0) / seconds),
        ],
    );
}

fn markdown(report: &Report, options: &Options) -> String {
    let mut out = format!(
        "### Rust profile, {}{}\n\n{} {}, {} logical cores\n\n| Scenario | Variant | Measured |\n|---|---|---|\n",
        env!("CARGO_PKG_VERSION"),
        if options.quick {
            " (quick: not a measurement)"
        } else {
            ""
        },
        std::env::consts::OS,
        std::env::consts::ARCH,
        cores(),
    );
    for row in &report.rows {
        out.push_str(&format!(
            "| `{}` | `{}` | {} |\n",
            row.scenario,
            row.variant,
            shown(&row.metrics)
        ));
    }
    for note in &report.notes {
        out.push_str(&format!("\n- {note}"));
    }
    out
}

fn cores() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

fn document(report: &Report, options: &Options, started_at: &str) -> Value {
    let results: Vec<Value> = report
        .rows
        .iter()
        .map(|row| {
            let metrics: Map<String, Value> = row
                .metrics
                .iter()
                .map(|(name, value)| (name.to_string(), json!(value)))
                .collect();
            json!({ "scenario": row.scenario, "variant": row.variant, "metrics": metrics })
        })
        .collect();
    json!({
        "engine": "rust",
        "version": env!("CARGO_PKG_VERSION"),
        "quick": options.quick,
        "started_at": started_at,
        "machine": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "logical_cores": cores(),
        },
        "results": results,
        "notes": report.notes,
    })
}

#[tokio::main]
async fn main() -> ExitCode {
    let options = match Options::parse() {
        Ok(options) => options,
        Err(message) => {
            eprintln!("strata-bench: {message}");
            return ExitCode::from(2);
        }
    };
    if cfg!(debug_assertions) {
        eprintln!("strata-bench: this is a debug build; measure with --release");
    }

    // Nothing is recorded until a scenario asks for it.
    let _ = metrics::set_global_recorder(StoredBytes);
    let started_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let scratch = Scratch::new();
    let mut report = Report::default();
    if options.runs("transition_latency") {
        eprintln!("transition_latency");
        transition_latency(&options, &mut report).await;
    }
    if options.runs("run_latency") {
        eprintln!("run_latency");
        run_latency(&options, &mut report).await;
    }
    if options.runs("throughput") {
        eprintln!("throughput");
        throughput(&options, &mut report).await;
    }
    if options.runs("suspended_memory") {
        eprintln!("suspended_memory");
        suspended_memory(&options, &scratch, &mut report).await;
    }
    if options.runs("stores") {
        eprintln!("stores");
        stores(&options, &scratch, &mut report).await;
    }
    if options.runs("large_state") {
        eprintln!("large_state");
        large_state(&options, &scratch, &mut report).await;
    }

    println!("{}", markdown(&report, &options));
    if let Some(path) = &options.out {
        let json = serde_json::to_string_pretty(&document(&report, &options, &started_at))
            .expect("a report is JSON");
        if let Err(error) = std::fs::write(path, json + "\n") {
            eprintln!("strata-bench: {}: {error}", path.display());
            return ExitCode::FAILURE;
        }
    }
    ExitCode::SUCCESS
}
