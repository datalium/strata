//! Samples for <https://stratamesh.dev/guides/production/hosting/>.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use datalium_strata::CancellationToken;
use datalium_strata::checkpoint::{GraphExecutionResult, HumanSignal};
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{CheckpointPolicy, CompileOptions, CompiledGraph, StateGraph};
use datalium_strata_host::{
    DriverStatus, Host, StoreHealth, StrataSettings, WakeDriver, check_store,
};
use datalium_strata_memory::checkpoint::SqliteCheckpointStore;
use serde::{Deserialize, Serialize};

const STRATA_JSON: &str = include_str!("../../../../files/guides/production/hosting/strata.json");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Refund {
    customer: String,
    amount: f64,
    status: String,
}

/// Set when a test runs again in a process of its own: the name of that test.
const OWN_PROCESS: &str = "STRATA_DOCS_OWN_PROCESS";

/// Whether this process runs `test` alone, started by [`run_in_own_process`].
pub(super) fn in_own_process(test: &str) -> bool {
    std::env::var(OWN_PROCESS).is_ok_and(|name| name == test)
}

/// Runs the test `test` (its path in this crate) again, alone, in a process of its own whose working
/// directory is `dir`, and returns what it printed. For samples that set what a process has only one of:
/// the working directory, a global `tracing` subscriber.
pub(super) fn run_in_own_process(test: &str, dir: &Path) -> String {
    let output = Command::new(std::env::current_exe().expect("the test binary's path"))
        .args([test, "--exact", "--nocapture", "--test-threads=1"])
        .env(OWN_PROCESS, test)
        .current_dir(dir)
        .output()
        .expect("the test binary starts");
    let printed = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.status.success(),
        "{test} failed in its own process:\n{printed}"
    );
    printed
}

/// A directory of its own under the system's temp directory, deleted when dropped.
pub(super) struct TempDir(pub(super) PathBuf);

impl TempDir {
    pub(super) fn new(name: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is after 1970")
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("strata-docs-{name}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("the temp directory is created");
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// What `main` below reports to the test: the host's status, and the token that stands in for Ctrl+C.
static STATUS: Mutex<Option<DriverStatus>> = Mutex::new(None);
static STOP: LazyLock<CancellationToken> = LazyLock::new(CancellationToken::new);

// docs:begin guides/production/hosting/register
#[tokio::main]
async fn main() {
    let text = std::fs::read_to_string("strata.json").expect("strata.json is readable");
    let settings: StrataSettings = serde_json::from_str(&text).expect("the settings parse");

    let store = Arc::new(settings.sqlite.open().expect("the store opens"));
    let graph = Arc::new(build());
    let config = settings.runs.config(); // the lease, when runs.lease.ttl is set

    let host = Host::new()
        .wake(
            graph.clone(),
            store.clone(),
            settings.wake.clone().with_config(config.clone()),
        )
        .recover(
            graph.clone(),
            store.clone(),
            settings.recovery.clone().with_config(config.clone()),
        )
        .retain(store.clone(), settings.retention.clone());
    let status = host.status(); // take it before run() consumes the host
    // docs:hide
    *STATUS.lock().unwrap() = Some(status.clone());
    // docs:show

    let shutdown = CancellationToken::new();
    let running = tokio::spawn(host.run(shutdown.clone()));

    // Start your own server or consumer here, with graph, store and config.

    // docs:hide
    tokio::select! {
        _ = STOP.cancelled() => {}
        _ = async {
            // docs:show
            tokio::signal::ctrl_c().await.expect("listens for Ctrl+C");
            // docs:hide
        } => {}
    }
    // docs:show
    shutdown.cancel();
    match tokio::time::timeout(Duration::from_secs(30), running).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(refused))) => eprintln!("the host didn't start: {refused}"),
        Ok(Err(panicked)) => eprintln!("the host task panicked: {panicked}"),
        Err(_) => eprintln!("a run outlasted the shutdown timeout"),
    }
}

fn build() -> CompiledGraph<Refund> {
    StateGraph::<Refund>::new()
        .add_node("Prepare", |r: Refund| Refund {
            status: "awaiting approval".into(),
            ..r
        })
        .add_interrupt_boundary("Prepare")
        .add_node("Pay", |r: Refund| Refund {
            status: "paid".into(),
            ..r
        })
        .add_edge("Prepare", "Pay")
        .compile_with(CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode))
        .expect("the graph is valid")
}
// docs:end guides/production/hosting/register

#[test]
fn register() {
    const TEST: &str = "guides::production::hosting::register";
    if in_own_process(TEST) {
        // Here the working directory holds strata.json. Stop the host, as Ctrl+C would, once every driver
        // has finished a round.
        let watcher = std::thread::spawn(|| {
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let status = STATUS.lock().unwrap().clone();
                let drivers = status.map(|s| s.drivers()).unwrap_or_default();
                if drivers.len() == 3 && drivers.iter().all(|d| d.rounds > 0) {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "the drivers didn't run a round: {drivers:?}"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
            STOP.cancel();
        });
        main();
        watcher.join().expect("every driver ran a round");

        let status = STATUS
            .lock()
            .unwrap()
            .clone()
            .expect("main took the status");
        let names: Vec<String> = status.drivers().into_iter().map(|d| d.name).collect();
        assert_eq!(names, ["recovery:Refund", "retention", "wake:Refund"]);
        assert!(status.failing().is_empty());
        println!("main returned");
        return;
    }

    let dir = TempDir::new("hosting-register");
    std::fs::write(dir.0.join("strata.json"), STRATA_JSON).expect("strata.json is written");
    let printed = run_in_own_process(TEST, &dir.0);
    assert!(printed.contains("main returned"), "{printed}");
    assert!(!printed.contains("the host didn't start"), "{printed}");
    assert!(!printed.contains("outlasted"), "{printed}");
    // The store is the file sqlite.path names, relative to the working directory.
    assert!(dir.0.join("strata.db").exists());
}

#[test]
fn settings() {
    // The page's strata.json: every value it sets, and a default for what it leaves out.
    let settings: StrataSettings = serde_json::from_str(STRATA_JSON).expect("the settings parse");
    assert_eq!(settings.sqlite.path.as_deref(), Some("strata.db"));
    assert_eq!(settings.runs.lease.ttl, Some(Duration::from_secs(30)));
    assert!(settings.runs.config().has_lease());
    assert_eq!(settings.wake.interval, Duration::from_secs(5));
    assert_eq!(settings.recovery.interval, Duration::from_secs(30));
    assert_eq!(settings.recovery.max_concurrency, 8);
    assert_eq!(settings.retention.interval, Duration::from_secs(3600));
    assert_eq!(
        settings.retention.older_than,
        Some(Duration::from_secs(30 * 24 * 3600))
    );
    assert!(settings.postgres.url.is_none());
}

// docs:begin guides/production/hosting/use-graph
#[derive(Clone)]
pub struct Refunds {
    graph: Arc<CompiledGraph<Refund>>,
    store: Arc<SqliteCheckpointStore>,
    config: RunConfig,
}

impl Refunds {
    pub async fn start(&self, id: &str, refund: Refund) -> GraphExecutionResult<Refund> {
        self.graph
            .run_with(id, refund, self.store.clone(), self.config.clone())
            .await
    }

    pub async fn approve(&self, id: &str, reviewer: &str) -> GraphExecutionResult<Refund> {
        self.graph
            .resume_with(
                id,
                HumanSignal::approve(reviewer),
                self.store.clone(),
                self.config.clone(),
            )
            .await
    }
}
// docs:end guides/production/hosting/use-graph

#[tokio::test]
async fn handler() {
    let settings: StrataSettings = serde_json::from_str(STRATA_JSON).expect("the settings parse");
    let refunds = Refunds {
        graph: Arc::new(build()),
        store: Arc::new(SqliteCheckpointStore::open_in_memory().expect("the store opens")),
        config: settings.runs.config(),
    };

    // docs:begin guides/production/hosting/handler
    let started = refunds
        .start(
            "refund-1042",
            Refund {
                customer: "acme".into(),
                amount: 120.0,
                status: "new".into(),
            },
        )
        .await;
    // Interrupted: the thread waits for approval.

    let approved = refunds.approve("refund-1042", "jdoe").await;
    // Completed(Refund { status: "paid", .. })
    // docs:end guides/production/hosting/handler

    let GraphExecutionResult::Interrupted { suspended_node, .. } = started else {
        panic!("expected the refund to wait for approval");
    };
    assert_eq!(suspended_node, "Prepare");
    let GraphExecutionResult::Completed(refund) = approved else {
        panic!("expected the approval to complete the refund");
    };
    assert_eq!(refund.status, "paid");
    assert_eq!(refund.customer, "acme");
    assert_eq!(refund.amount, 120.0);

    // A completed thread can't be resumed.
    let again = refunds.approve("refund-1042", "jdoe").await;
    assert!(matches!(again, GraphExecutionResult::Failed(_)));
}

#[tokio::test]
async fn health() {
    let store = Arc::new(SqliteCheckpointStore::open_in_memory().expect("the store opens"));
    let host = Host::new().wake(Arc::new(build()), store.clone(), WakeDriver::new());
    let status = host.status();
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(host.run(shutdown.clone()));
    let deadline = Instant::now() + Duration::from_secs(10);
    while status.drivers().iter().all(|d| d.rounds == 0) {
        assert!(
            Instant::now() < deadline,
            "the wake driver didn't run a round"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // docs:begin guides/production/hosting/health
    let ready = match check_store(&*store, Duration::from_secs(5)).await {
        StoreHealth::Healthy { .. } => status.failing().is_empty(),
        StoreHealth::Unhealthy { .. } => false,
    };
    // docs:end guides/production/hosting/health

    assert!(ready);
    shutdown.cancel();
    running
        .await
        .expect("the host task doesn't panic")
        .expect("the host starts");
}
