//! Samples for <https://stratamesh.dev/guides/persistence/strata-cloud/>.
//!
//! The store, run and decision worker samples talk to a live Strata Cloud:
//! like `strata-cloud`'s `tests/live.rs`, they need `STRATA_CLOUD_URL` and
//! `STRATA_CLOUD_KEY`, are skipped without them, and so are compiled but not
//! run in CI. The hosting sample runs offline: it starts the host and stops it.

use std::sync::Arc;
use std::time::Duration;

use datalium_strata::graph::{CheckpointPolicy, CompileOptions, CompiledGraph, StateGraph};
use datalium_strata_cloud::{StrataCloudCheckpointStore, StrataCloudOptions};
use serde::{Deserialize, Serialize};

const URL_VAR: &str = "STRATA_CLOUD_URL";
const KEY_VAR: &str = "STRATA_CLOUD_KEY";

/// Whether the live variables are set; says so when they aren't.
fn live() -> bool {
    let missing: Vec<_> = [URL_VAR, KEY_VAR]
        .into_iter()
        .filter(|var| std::env::var(var).map_or(true, |v| v.is_empty()))
        .collect();
    if !missing.is_empty() {
        eprintln!(
            "SKIPPED: {} not set (a live Strata Cloud)",
            missing.join(", ")
        );
    }
    missing.is_empty()
}

fn live_options() -> StrataCloudOptions {
    StrataCloudOptions::new(
        std::env::var(URL_VAR).unwrap(),
        std::env::var(KEY_VAR).unwrap(),
    )
    .with_request_timeout(Duration::from_secs(10))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Refund {
    customer: String,
    amount: f64,
    status: String,
}

/// The refund graph: `Prepare` waits for approval, then `Pay` runs.
fn refunds() -> Arc<CompiledGraph<Refund>> {
    let graph = StateGraph::<Refund>::new()
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
        .expect("the graph is valid");
    Arc::new(graph)
}

#[tokio::test]
async fn create() {
    use datalium_strata::checkpoint::CheckpointStore;

    if !live() {
        return;
    }

    // docs:begin guides/persistence/strata-cloud/create
    use datalium_strata_cloud::{StrataCloudCheckpointStore, StrataCloudOptions};

    let cloud = StrataCloudOptions::new(
        std::env::var("STRATA_CLOUD_URL").expect("STRATA_CLOUD_URL is set"), // <your beta endpoint>
        std::env::var("STRATA_CLOUD_KEY").expect("STRATA_CLOUD_KEY is set"),
    )
    .with_request_timeout(Duration::from_secs(10));

    let store =
        Arc::new(StrataCloudCheckpointStore::new(cloud.clone()).expect("the options are valid"));
    // docs:end guides/persistence/strata-cloud/create

    let missing = format!("docs-cloud-create-{}", std::process::id());
    assert!(store.load(&missing).await.unwrap().is_none());
    assert!(
        datalium_strata_cloud::Decisions::new(cloud).is_ok(),
        "the decisions client takes the same options"
    );
}

#[tokio::test]
async fn run() {
    use datalium_strata::checkpoint::{CheckpointStore, GraphExecutionResult};
    use datalium_strata::schema::CheckpointStatus;

    if !live() {
        return;
    }
    let store = Arc::new(StrataCloudCheckpointStore::new(live_options()).unwrap());
    let graph = refunds();
    let initial = Refund {
        customer: "ada".into(),
        amount: 120.0,
        status: "new".into(),
    };
    let mut entitled = true;

    // docs:begin guides/persistence/strata-cloud/run
    use datalium_strata_cloud::is_entitlement_exceeded;

    match graph.run("refund-42", initial, store.clone()).await {
        GraphExecutionResult::Interrupted { .. } => println!("waiting for a decision"),
        GraphExecutionResult::Failed(error) if is_entitlement_exceeded(&error) => {
            // Your plan's limit refused a new thread. Threads already running carry on.
            // docs:hide
            entitled = false;
            // docs:show
        }
        GraphExecutionResult::Failed(error) => println!("failed: {error:?}"),
        _ => {}
    }
    // docs:end guides/persistence/strata-cloud/run

    if entitled {
        let latest = store
            .load("refund-42")
            .await
            .unwrap()
            .expect("a checkpoint");
        assert_eq!(latest.status, CheckpointStatus::Interrupted);
    }
}

/// The threads `decision_worker`'s callback saw: the callback is `'static`,
/// and the sample's closure captures nothing.
static HANDLED: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

#[tokio::test]
async fn decision_worker() -> Result<(), Box<dyn std::error::Error>> {
    use datalium_strata::CancellationToken;

    if !live() {
        return Ok(());
    }
    let cloud = live_options();
    let store = Arc::new(StrataCloudCheckpointStore::new(cloud.clone())?);
    let graph = refunds();
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(3)).await;
        stop.cancel();
    });

    // docs:begin guides/persistence/strata-cloud/decision-worker
    use datalium_strata_cloud::{DecisionWorker, Decisions};

    let decisions = Decisions::new(cloud.clone()).expect("the options are valid");
    let mut worker = DecisionWorker::new(graph.clone(), store.clone(), decisions)
        .with_thread_id_prefix("refund-")
        .with_on_handled(|handled| {
            println!(
                "{}: {:?} (decided by {:?})",
                handled.decision.thread_id, handled.outcome, handled.decision.decided_by
            );
            // docs:hide
            HANDLED
                .lock()
                .unwrap()
                .push(handled.decision.thread_id.clone());
            // docs:show
        });

    worker.run(shutdown).await?; // polls and resumes until the token is cancelled
    // docs:end guides/persistence/strata-cloud/decision-worker

    // It handled only the refund threads.
    assert!(
        HANDLED
            .lock()
            .unwrap()
            .iter()
            .all(|id| id.starts_with("refund-"))
    );
    Ok(())
}

#[tokio::test]
async fn host() {
    use datalium_strata::CancellationToken;
    use datalium_strata_cloud::{DecisionWorker, Decisions};
    use datalium_strata_host::{Host, WakeDriver};

    // A port nothing listens on: the drivers fail their calls, wait, and try
    // again until the host stops.
    let cloud = StrataCloudOptions::new("http://127.0.0.1:9", "sk-docs")
        .with_connect_timeout(Duration::from_millis(200));
    let store = Arc::new(StrataCloudCheckpointStore::new(cloud.clone()).unwrap());
    let graph = refunds();

    // docs:begin guides/persistence/strata-cloud/host
    let host = Host::new()
        .wake(graph.clone(), store.clone(), WakeDriver::new())
        .task("decisions", move |shutdown| {
            let (graph, store, cloud) = (graph.clone(), store.clone(), cloud.clone());
            async move {
                let decisions = Decisions::new(cloud).map_err(|e| e.to_string())?;
                DecisionWorker::new(graph, store, decisions)
                    .with_thread_id_prefix("refund-")
                    .run(shutdown)
                    .await
                    .map_err(|e| e.to_string())
            }
        });

    let shutdown = CancellationToken::new();
    let running = tokio::spawn(host.run(shutdown.clone()));
    // docs:end guides/persistence/strata-cloud/host

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!running.is_finished(), "the host runs until it is stopped");
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("the host stops")
        .expect("the host's task doesn't panic")
        .expect("the host stops cleanly");
}
