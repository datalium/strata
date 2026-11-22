//! Samples for <https://stratamesh.dev/reference/configuration/>: the full `strata.json`, a whole-file sample
//! in `files/reference/configuration/`. The test parses it into `StrataSettings`, checks the values, and
//! opens what it describes: the SQLite stores, the scheduler, a memory store, and a host with every driver.
//! The Postgres and Redis stores open when `STRATA_TEST_POSTGRES_URL` / `STRATA_TEST_REDIS_URL` are set.

use std::sync::Arc;
use std::time::Duration;

use datalium_strata::CancellationToken;
use datalium_strata::graph::{CheckpointPolicy, CompileOptions, StateGraph};
use datalium_strata_host::{Host, StrataSettings};
use serde::{Deserialize, Serialize};

use crate::child::TempDir;

const STRATA_JSON: &str = include_str!("../../../files/reference/configuration/strata.json");

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Tick {
    done: bool,
}

/// The URL in `var`, or `None` to skip, unless `STRATA_REQUIRE_BACKENDS=1` says the test must run.
fn backend_url(var: &str) -> Option<String> {
    match std::env::var(var) {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            assert!(
                std::env::var("STRATA_REQUIRE_BACKENDS").as_deref() != Ok("1"),
                "{var} is not set, but STRATA_REQUIRE_BACKENDS=1 says these tests must run"
            );
            eprintln!("SKIPPED: {var} is not set (start `docker compose up -d --wait`)");
            None
        }
    }
}

#[test]
fn strata_json_parses() {
    let settings: StrataSettings =
        serde_json::from_str(STRATA_JSON).expect("strata.json fits StrataSettings");

    assert_eq!(settings.sqlite.path.as_deref(), Some("strata.db"));
    assert!(settings.sqlite.deltas.enabled);
    assert_eq!(settings.sqlite.deltas.full_every, 16);
    assert!(!settings.postgres.auto_migrate);
    assert_eq!(settings.postgres.pool_size, 16);
    assert_eq!(
        settings.postgres.retry.initial_delay,
        Duration::from_millis(50)
    );
    assert_eq!(settings.postgres.retry.max_delay, Duration::from_secs(2));
    assert_eq!(settings.redis.prefix, "orders");
    assert_eq!(
        settings.redis.finished_thread_ttl,
        Some(Duration::from_secs(7 * 86_400))
    );
    assert_eq!(settings.runs.lease.owner, "orders-1");
    assert_eq!(settings.runs.lease.ttl, Some(Duration::from_secs(30)));
    let scheduler = settings
        .scheduler
        .as_ref()
        .expect("the file has a scheduler");
    assert_eq!(scheduler.max_concurrent, 16);
    assert_eq!(scheduler.max_queue_time, Some(Duration::from_secs(10)));
    assert_eq!(scheduler.tenant_limits.get("acme"), Some(&8));
    assert_eq!(settings.wake.interval, Duration::from_secs(5));
    assert!(settings.wake.match_graph);
    assert_eq!(
        settings.recovery.stale_after,
        Some(Duration::from_secs(300))
    );
    assert_eq!(settings.recovery.priority, Some(10));
    assert_eq!(
        settings.retention.older_than,
        Some(Duration::from_secs(30 * 86_400))
    );
    assert_eq!(settings.retention.states, ["completed", "ended", "failed"]);
    assert_eq!(settings.memory.dimensions, Some(1536));
    assert_eq!(settings.memory.index.kind, "hnsw");
    assert_eq!(
        settings.memory.retention.namespace.as_deref(),
        Some("incidents")
    );
}

#[tokio::test]
async fn strata_json_opens_the_stores_and_the_drivers() {
    let mut settings: StrataSettings =
        serde_json::from_str(STRATA_JSON).expect("strata.json fits StrataSettings");

    // The file's relative paths, in a directory of the test's own.
    let dir = TempDir::new("configuration");
    settings.sqlite.path = Some(dir.path().join("strata.db").display().to_string());
    settings.sqlite.offload.path = Some(dir.path().join("blobs").display().to_string());
    let store = Arc::new(settings.sqlite.open().expect("the SQLite store opens"));
    settings
        .sqlite
        .open_history()
        .expect("the SQLite history store opens");

    let scheduler = settings
        .scheduler
        .as_ref()
        .expect("the file has a scheduler")
        .open()
        .expect("the scheduler's settings are valid");
    let _config = settings.runs.config_with(&scheduler);
    let memory = Arc::new(
        settings
            .memory
            .open_in_memory()
            .expect("the memory settings are valid"),
    );

    let graph = Arc::new(
        StateGraph::<Tick>::new()
            .add_node("Tick", |_: Tick| Tick { done: true })
            .compile_with(CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode))
            .expect("graph is structurally valid"),
    );
    let host = Host::new()
        .wake(graph.clone(), store.clone(), settings.wake.clone())
        .recover(graph, store.clone(), settings.recovery.clone())
        .retain(store, settings.retention.clone())
        .retain_memory(memory, settings.memory.retention.clone())
        .close_on_shutdown(scheduler);
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(host.run(shutdown.clone()));
    shutdown.cancel();
    running
        .await
        .expect("the host doesn't panic")
        .expect("the host accepts every driver's settings");

    if let Some(url) = backend_url("STRATA_TEST_POSTGRES_URL") {
        // The file turns auto_migrate off, so bring the tables up to date first, as a deployment step would.
        let mut migrate = settings.postgres.clone();
        migrate.url = Some(url.clone());
        migrate.auto_migrate = true;
        migrate.open().await.expect("the Postgres store migrates");

        settings.postgres.url = Some(url);
        settings
            .postgres
            .open()
            .await
            .expect("the Postgres store opens");
    }

    if let Some(url) = backend_url("STRATA_TEST_REDIS_URL") {
        settings.redis.url = Some(url);
        settings.redis.prefix = format!("docs-reference-configuration-{}", std::process::id());
        settings.redis.open().await.expect("the Redis store opens");
    }
}
