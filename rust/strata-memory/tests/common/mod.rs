//! Helpers for the tests that need a live Postgres or Redis, and the runner for
//! the shared query cases.
//!
//! Those tests read `STRATA_TEST_POSTGRES_URL` / `STRATA_TEST_REDIS_URL` (see
//! the repo's `docker-compose.yml`). When a variable is unset the test prints a
//! `SKIPPED` line and returns — Rust can't mark a test skipped at runtime — so
//! a machine without the services still gets a green `cargo test`. CI sets
//! `STRATA_REQUIRE_BACKENDS=1`, which turns a missing variable into a panic so
//! a misconfigured job can't quietly stop testing the adapters.
//!
//! Every test gets its own sandbox — a fresh Postgres schema, or a fresh Redis
//! key prefix — and removes it afterwards, so tests are independent, can run in
//! parallel, and can be re-run against a database that already has data in it.
//! Each `tests/*.rs` file uses a different subset of this module.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use datalium_strata::checkpoint::{CheckpointQuery, CheckpointStore, ThreadQuery, ThreadState};
use datalium_strata::schema::Checkpoint;
use datalium_strata::store::parse_timestamp;
use datalium_strata_memory::postgres_store::config_from_url;
use serde_json::{Value, json};

pub const POSTGRES_VAR: &str = "STRATA_TEST_POSTGRES_URL";
pub const REDIS_VAR: &str = "STRATA_TEST_REDIS_URL";

/// The URL in `var`, or `None` (after saying so) if the test should skip.
pub fn backend_url(var: &str) -> Option<String> {
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

/// A name unique to this process and call, safe as a Postgres identifier and a
/// Redis key segment.
pub fn unique_name(label: &str) -> String {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    format!(
        "strata_test_{}_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed),
        label
    )
}

/// An async connection for a test's own Redis commands, with the store's
/// timeouts. The `redis` crate's defaults (1 s to connect, 500 ms for a reply)
/// are too short on a busy CI runner.
pub async fn redis_connection(url: &str) -> redis::aio::MultiplexedConnection {
    let timeout = Some(std::time::Duration::from_secs(5));
    let config = redis::AsyncConnectionConfig::new()
        .set_connection_timeout(timeout)
        .set_response_timeout(timeout);
    redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection_with_config(&config)
        .await
        .unwrap()
}

/// A file under `schema/fixtures`.
pub fn fixture(path: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/fixtures")
        .join(path);
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {}: {}", path.display(), e));
    serde_json::from_str(&raw).unwrap()
}

/// A throwaway Postgres schema. Stores created through [`PgSandbox::client`]
/// put their tables in it (`search_path`), so nothing touches `public`.
pub struct PgSandbox {
    url: String,
    pub schema: String,
}

/// Runs `work` on a thread of its own, so the sync `postgres` client never runs
/// inside a tokio runtime (where it panics).
fn off_runtime<R: Send>(work: impl FnOnce() -> R + Send) -> R {
    std::thread::scope(|scope| scope.spawn(work).join().unwrap())
}

impl PgSandbox {
    pub fn new(url: &str, label: &str) -> Self {
        let schema = unique_name(label);
        off_runtime(|| {
            let mut admin = postgres::Client::connect(url, postgres::NoTls).unwrap();
            admin
                .batch_execute(&format!("CREATE SCHEMA {schema}"))
                .unwrap();
        });
        Self {
            url: url.to_string(),
            schema,
        }
    }

    /// A new async connection scoped to this sandbox's schema, for a store.
    pub async fn client(&self) -> tokio_postgres::Client {
        self.connect(None).await
    }

    /// [`Self::client`] whose backend is named `application_name`, so a test
    /// can tell it from every other connection and terminate it.
    pub async fn named_client(&self, application_name: &str) -> tokio_postgres::Client {
        self.connect(Some(application_name)).await
    }

    async fn connect(&self, application_name: Option<&str>) -> tokio_postgres::Client {
        let mut config = config_from_url(&self.url).unwrap();
        config.options(format!("-c search_path={}", self.schema));
        if let Some(name) = application_name {
            config.application_name(name);
        }
        let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }

    /// A URL whose connections are scoped to this sandbox's schema, for a
    /// store that connects, and reconnects, by itself. With an
    /// `application_name` its backends can be told from every other
    /// connection's.
    pub fn url(&self, application_name: Option<&str>) -> String {
        let separator = if self.url.contains('?') { '&' } else { '?' };
        let mut url = format!(
            "{}{separator}options=-c%20search_path%3D{}",
            self.url, self.schema
        );
        if let Some(name) = application_name {
            url.push_str(&format!("&application_name={name}"));
        }
        url
    }

    /// [`Self::url`] whose connections also search `also` after the sandbox's
    /// schema, where an extension's types live (pgvector's `vector` in
    /// `public`). The sandbox's schema stays the one tables are created in.
    pub fn url_also_searching(&self, also: &str) -> String {
        format!("{}%2C{also}", self.url(None))
    }

    /// Runs `work` with a sync connection scoped to this sandbox's schema, for
    /// setting up and inspecting tables.
    pub fn admin<R: Send>(&self, work: impl FnOnce(&mut postgres::Client) -> R + Send) -> R {
        off_runtime(|| {
            let mut config: postgres::Config = self.url.parse().unwrap();
            config.options(&format!("-c search_path={}", self.schema));
            let mut client = config.connect(postgres::NoTls).unwrap();
            work(&mut client)
        })
    }
}

impl Drop for PgSandbox {
    fn drop(&mut self) {
        let (url, schema) = (&self.url, &self.schema);
        off_runtime(|| {
            if let Ok(mut admin) = postgres::Client::connect(url, postgres::NoTls) {
                let _ = admin.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"));
            }
        });
    }
}

/// A unique Redis key prefix; every key under it is deleted on drop.
pub struct RedisSandbox {
    pub url: String,
    pub prefix: String,
}

impl RedisSandbox {
    pub fn new(url: &str, label: &str) -> Self {
        Self {
            url: url.to_string(),
            prefix: unique_name(label),
        }
    }

    /// A sync connection, for inspecting keys.
    pub fn connection(&self) -> redis::Connection {
        redis::Client::open(self.url.as_str())
            .unwrap()
            .get_connection()
            .unwrap()
    }
}

impl Drop for RedisSandbox {
    fn drop(&mut self) {
        let mut conn = self.connection();
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(format!("{}:*", self.prefix))
            .query(&mut conn)
            .unwrap_or_default();
        if !keys.is_empty() {
            let _: Result<i64, _> = redis::cmd("DEL").arg(keys).query(&mut conn);
        }
    }
}

/// `checkpoint-query/cases.json`'s seed, as checkpoints in save order.
pub fn query_seed() -> Vec<Checkpoint> {
    serde_json::from_value(fixture("checkpoint-query/cases.json")["seed"].clone()).unwrap()
}

/// Saves the seed of `checkpoint-query/cases.json` to `store`.
pub async fn save_query_seed(store: &dyn CheckpointStore) {
    for checkpoint in query_seed() {
        store.save(checkpoint).await.unwrap();
    }
}

fn thread_query(json: &Value) -> ThreadQuery {
    let mut query = ThreadQuery::default();
    if let Some(states) = json["states"].as_array() {
        query = query.with_states(
            states
                .iter()
                .map(|state| ThreadState::parse(state.as_str().unwrap()).unwrap()),
        );
    }
    if let Some(metadata) = json["metadata"].as_object() {
        for (key, value) in metadata {
            query = query.with_metadata(key, value.as_str().unwrap());
        }
    }
    if let Some(after) = json["updated_after"].as_str() {
        query = query.updated_after(parse_timestamp(after).unwrap());
    }
    if let Some(before) = json["updated_before"].as_str() {
        query = query.updated_before(parse_timestamp(before).unwrap());
    }
    if let Some(due) = json["due_by"].as_str() {
        query = query.due_by(parse_timestamp(due).unwrap());
    }
    if let Some(id) = json["graph_id"].as_str() {
        query = query.with_graph(id);
    }
    if let Some(version) = json["graph_version"].as_str() {
        query = query.with_graph_version(version);
    }
    if let Some(limit) = json["limit"].as_u64() {
        query = query.with_limit(limit as usize);
    }
    query
}

/// Runs every case of `checkpoint-query/cases.json` against `store`, which
/// must already hold the seed. An `exact` store must page exactly as the
/// fixture says; any store must return the same threads in the same order.
pub async fn run_query_cases(store: &dyn CheckpointQuery, exact: bool) {
    let cases = fixture("checkpoint-query/cases.json");
    let summaries = &cases["summaries"];

    for case in cases["list"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let mut query = thread_query(&case["query"]);
        let mut pages: Vec<Vec<String>> = Vec::new();
        loop {
            let page = store.list_threads(&query).await.unwrap();
            assert!(page.threads.len() <= query.limit, "{name}: page too long");
            for summary in &page.threads {
                assert_eq!(
                    summary_json(summary),
                    summaries[&summary.thread_id],
                    "{name}: summary of {}",
                    summary.thread_id
                );
            }
            pages.push(page.threads.iter().map(|t| t.thread_id.clone()).collect());
            match page.next_cursor {
                Some(cursor) => query = query.with_cursor(cursor),
                None => break,
            }
            assert!(pages.len() < 50, "{name}: the cursor never ended");
        }
        let expected: Vec<Vec<String>> = serde_json::from_value(case["pages"].clone()).unwrap();
        if exact {
            assert_eq!(pages, expected, "{name}");
        } else {
            assert_eq!(pages.concat(), expected.concat(), "{name}");
        }
    }

    let seed = query_seed();
    for case in cases["log"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let thread_id = case["thread_id"].as_str().unwrap();
        let page = store
            .get_log(
                thread_id,
                case["after_sequence"].as_i64(),
                case["limit"].as_u64().unwrap() as usize,
            )
            .await
            .unwrap();
        let sequences: Vec<i64> = page.entries.iter().map(|e| e.sequence).collect();
        assert_eq!(json!(sequences), case["sequences"], "{name}");
        assert_eq!(
            json!(page.next_after_sequence),
            case["next_after_sequence"],
            "{name}"
        );
        let log: Vec<&Checkpoint> = seed.iter().filter(|c| c.thread_id == thread_id).collect();
        for entry in &page.entries {
            assert_eq!(
                &entry.checkpoint, log[entry.sequence as usize],
                "{name}: entry {}",
                entry.sequence
            );
        }
    }

    // Out-of-range page sizes and a malformed cursor are refused.
    for limit in [0, 1001] {
        assert!(
            store
                .list_threads(&ThreadQuery::default().with_limit(limit))
                .await
                .is_err()
        );
        assert!(store.get_log("failed-acme", None, limit).await.is_err());
    }
    assert!(
        store
            .list_threads(&ThreadQuery::default().with_cursor("not a cursor"))
            .await
            .is_err()
    );
}

/// A summary in the shape of `cases.json`'s `summaries`.
pub fn summary_json(summary: &datalium_strata::checkpoint::ThreadSummary) -> Value {
    let metadata: BTreeMap<_, _> = summary.metadata.iter().collect();
    json!({
        "sequence": summary.sequence,
        "state": summary.state.as_str(),
        "status": serde_json::to_value(summary.status).unwrap(),
        "suspended_node": summary.suspended_node,
        "next_node": summary.next_node,
        "waiting_at": summary.waiting_at,
        "metadata": metadata,
        "updated_at": datalium_strata::store::timestamp_text(summary.updated_at),
        "schema_version": summary.schema_version,
        "wait": summary.wait,
        "graph": summary.graph,
    })
}
