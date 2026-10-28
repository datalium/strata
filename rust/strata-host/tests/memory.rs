//! Long-term memory in the host (`design/45-Long-Term-Memory.md`, "Hosting"):
//! the `memory` settings read through serde, `open_memory` on each store's
//! settings and `open_in_memory`, their refusals, and the memory retention
//! driver purging by age on the host's clock. The xUnit twin is the hosting
//! memory tests.
//!
//! The Postgres and Redis tests need `STRATA_TEST_POSTGRES_URL` /
//! `STRATA_TEST_REDIS_URL`; unset, they print `SKIPPED` and pass, unless
//! `STRATA_REQUIRE_BACKENDS=1`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use datalium_strata::CancellationToken;
use datalium_strata::memory::{
    Embedding, InMemoryMemoryStore, MemoryKey, MemoryQuery, MemoryRecord, MemoryStore,
};
use datalium_strata_host::{
    Host, HostError, MemoryIndexSettings, MemoryRetentionDriver, MemorySettings, PostgresSettings,
    RedisSettings, SqliteSettings, StrataSettings,
};
use datalium_strata_memory::{Clock, MemoryIndex};
use serde_json::json;

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

fn unique_name(label: &str) -> String {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    format!(
        "strata_host_memory_{}_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed),
        label
    )
}

/// An async connection for a test's own Redis commands, with the store's
/// timeouts. The `redis` crate's defaults (1 s to connect, 500 ms for a reply)
/// are too short on a busy CI runner.
async fn redis_connection(url: &str) -> redis::aio::MultiplexedConnection {
    let timeout = Some(Duration::from_secs(5));
    let config = redis::AsyncConnectionConfig::new()
        .set_connection_timeout(timeout)
        .set_response_timeout(timeout);
    redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection_with_config(&config)
        .await
        .unwrap()
}

fn record(namespace: &str, tenant: &str, id: &str) -> MemoryRecord {
    MemoryRecord::new(
        namespace,
        tenant,
        id,
        Embedding::new("test/embed", vec![1.0, 0.0, 0.0]),
    )
}

fn memory(dimensions: usize) -> MemorySettings {
    MemorySettings {
        dimensions: Some(dimensions),
        ..MemorySettings::default()
    }
}

/// A clock the test sets.
#[derive(Clone)]
struct TestClock(Arc<Mutex<DateTime<Utc>>>);

impl TestClock {
    fn at(now: DateTime<Utc>) -> Self {
        Self(Arc::new(Mutex::new(now)))
    }

    fn set(&self, now: DateTime<Utc>) {
        *self.0.lock().unwrap() = now;
    }

    fn clock(&self) -> Clock {
        let now = self.0.clone();
        Arc::new(move || *now.lock().unwrap())
    }
}

fn noon() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 6, 12, 0, 0).unwrap()
}

/// Waits, in tokio's paused time, for `condition`.
async fn until(what: &str, mut condition: impl FnMut() -> bool) {
    for _ in 0..2000 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    panic!("{what}");
}

async fn ids(store: &InMemoryMemoryStore, namespace: &str, tenant: &str) -> Vec<String> {
    store
        .list(&MemoryQuery::new(namespace).tenant(tenant))
        .await
        .unwrap()
        .records
        .into_iter()
        .map(|record| record.id)
        .collect()
}

// --- Settings ----------------------------------------------------------------

#[test]
fn the_memory_section_comes_from_any_serde_source() {
    let settings: StrataSettings = serde_json::from_value(json!({
        "memory": {
            "dimensions": 768,
            "index": { "kind": "hnsw", "m": 32 },
            "table": "ops.memories",
            "prefix": "ops-memory",
            "retention": {
                "interval": "10m",
                "batch_size": 250,
                "older_than": "365d",
                "namespace": "incidents",
                "tenant": "acme"
            }
        }
    }))
    .unwrap();

    assert_eq!(
        settings.memory,
        MemorySettings {
            dimensions: Some(768),
            index: MemoryIndexSettings {
                kind: "hnsw".to_string(),
                m: 32,
                ef_construction: 64,
            },
            table: Some("ops.memories".to_string()),
            prefix: Some("ops-memory".to_string()),
            retention: MemoryRetentionDriver::new()
                .with_interval(Duration::from_secs(600))
                .with_batch_size(250)
                .with_older_than(Duration::from_secs(365 * 86_400))
                .with_namespace("incidents")
                .with_tenant("acme"),
        }
    );
    assert_eq!(
        settings.memory.index.index().unwrap(),
        MemoryIndex::Hnsw {
            m: 32,
            ef_construction: 64
        }
    );
}

#[test]
fn without_a_source_memory_has_no_dimensions_an_exact_index_and_an_hourly_driver() {
    let settings: StrataSettings = serde_json::from_value(json!({})).unwrap();

    assert_eq!(settings.memory, MemorySettings::default());
    assert_eq!(settings.memory.dimensions, None);
    assert_eq!(settings.memory.index.index().unwrap(), MemoryIndex::None);
    assert_eq!(
        (settings.memory.table, settings.memory.prefix),
        (None, None)
    );
    let retention = settings.memory.retention;
    assert_eq!(
        (
            retention.interval,
            retention.batch_size,
            retention.older_than,
            retention.namespace,
            retention.tenant
        ),
        (Duration::from_secs(3600), 1000, None, None, None)
    );
}

#[test]
fn the_index_kind_is_read_in_any_case() {
    for kind in ["hnsw", "HNSW", "Hnsw"] {
        let settings = MemoryIndexSettings {
            kind: kind.to_string(),
            ..MemoryIndexSettings::default()
        };
        assert_eq!(settings.index().unwrap(), MemoryIndex::hnsw(), "{kind}");
    }
    let none = MemoryIndexSettings {
        kind: "None".to_string(),
        ..MemoryIndexSettings::default()
    };
    assert_eq!(none.index().unwrap(), MemoryIndex::None);
}

#[test]
fn a_blank_memory_prefix_is_redis_s_and_the_options_carry_the_settings() {
    let redis = RedisSettings {
        prefix: "ops".to_string(),
        ..RedisSettings::default()
    };
    for prefix in [None, Some(""), Some("  ")] {
        let mut settings = memory(3);
        settings.prefix = prefix.map(str::to_string);
        assert_eq!(redis.memory_options(&settings).unwrap().prefix(), "ops");
    }
    let mut own = memory(3);
    own.prefix = Some("ops-memory".to_string());
    own.index.kind = "hnsw".to_string();
    let options = redis.memory_options(&own).unwrap();
    assert_eq!(
        (options.prefix(), options.dimensions(), options.index()),
        ("ops-memory", 3, MemoryIndex::hnsw())
    );

    let mut table = memory(768);
    table.table = Some("ops.memories".to_string());
    let options = PostgresSettings::default().memory_options(&table).unwrap();
    assert_eq!(
        (options.table(), options.dimensions(), options.index()),
        ("ops.memories", 768, MemoryIndex::None)
    );
}

#[tokio::test]
async fn a_memory_store_without_dimensions_or_with_an_unknown_index_is_refused() {
    let settings = StrataSettings::default();
    let no_dimensions = "Strata's memory store has no dimensions: set memory.dimensions";

    assert_eq!(
        settings.memory.open_in_memory().err().unwrap().0,
        no_dimensions
    );
    assert_eq!(memory(0).open_in_memory().err().unwrap().0, no_dimensions);
    assert_eq!(
        settings
            .postgres
            .open_memory(&settings.memory)
            .await
            .err()
            .unwrap()
            .0,
        no_dimensions
    );
    assert_eq!(
        settings
            .redis
            .open_memory(&settings.memory)
            .await
            .err()
            .unwrap()
            .0,
        no_dimensions
    );
    assert_eq!(
        settings
            .sqlite
            .open_memory(&settings.memory)
            .err()
            .unwrap()
            .0,
        no_dimensions
    );

    // With dimensions, the missing connection is what is said.
    assert_eq!(
        settings
            .postgres
            .open_memory(&memory(3))
            .await
            .err()
            .unwrap()
            .0,
        "Strata's Postgres store has no url: set postgres.url"
    );
    assert_eq!(
        settings
            .redis
            .open_memory(&memory(3))
            .await
            .err()
            .unwrap()
            .0,
        "Strata's Redis store has no url: set redis.url"
    );
    assert_eq!(
        settings.sqlite.open_memory(&memory(3)).err().unwrap().0,
        "Strata's SQLite store has no file: set sqlite.path"
    );

    let mut flat = memory(3);
    flat.index.kind = "flat".to_string();
    assert_eq!(
        flat.open_in_memory().err().unwrap().0,
        "Strata's memory index kind 'flat' is unknown: use none or hnsw"
    );
    assert_eq!(
        memory(16_001).open_in_memory().err().unwrap().0,
        "a memory store has 1 to 16000 dimensions, not 16001"
    );
}

#[tokio::test]
async fn open_in_memory_makes_a_store_of_the_dimensions() {
    let store = memory(3).open_in_memory().unwrap();
    assert_eq!(store.dimensions(), 3);
    store
        .upsert(record("incidents", "", "inc-1"))
        .await
        .unwrap();
    assert!(
        store
            .get(&MemoryKey::new("incidents", "", "inc-1"))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn the_sqlite_memory_store_shares_the_checkpoint_store_s_file_and_keeps_its_dimensions() {
    let path = std::env::temp_dir().join(format!("{}.sqlite", unique_name("sqlite")));
    let sqlite = SqliteSettings {
        path: Some(path.to_string_lossy().into_owned()),
        ..SqliteSettings::default()
    };
    {
        let checkpoints = sqlite.open().unwrap();
        let mut settings = memory(3);
        settings.table = Some("memories".to_string());
        let store = sqlite.open_memory(&settings).unwrap();
        store
            .upsert(record("incidents", "", "inc-1"))
            .await
            .unwrap();
        drop((store, checkpoints));

        let again = sqlite.open_memory(&settings).unwrap();
        assert!(
            again
                .get(&MemoryKey::new("incidents", "", "inc-1"))
                .await
                .unwrap()
                .is_some()
        );
        drop(again);

        // The table was made with 3 dimensions.
        settings.dimensions = Some(4);
        let refused = sqlite.open_memory(&settings).err().unwrap().0;
        assert!(refused.contains("3 dimensions"), "{refused}");
    }
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn postgres_settings_open_a_memory_store_that_is_migrated_or_only_checked() {
    let Some(url) = backend_url("STRATA_TEST_POSTGRES_URL") else {
        return;
    };
    // A schema of this test's own, through the URL every connection the
    // store makes will use.
    let schema = unique_name("pg");
    let (admin, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let separator = if url.contains('?') { '&' } else { '?' };
    let settings = |auto_migrate: bool| PostgresSettings {
        url: Some(format!(
            "{url}{separator}options=-c%20search_path%3D{schema}"
        )),
        auto_migrate,
        ..PostgresSettings::default()
    };
    let mut memory = memory(3);
    memory.table = Some(format!("{schema}.memories"));

    // A store that doesn't migrate refuses a table that isn't there.
    assert!(settings(false).open_memory(&memory).await.is_err());

    let store = settings(true).open_memory(&memory).await.unwrap();
    store
        .upsert(record("incidents", "", "inc-1"))
        .await
        .unwrap();
    let checked = settings(false).open_memory(&memory).await.unwrap();
    assert!(
        checked
            .get(&MemoryKey::new("incidents", "", "inc-1"))
            .await
            .unwrap()
            .is_some()
    );

    drop((store, checked));
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
}

#[tokio::test]
async fn redis_settings_open_a_memory_store_under_the_memory_prefix_or_redis_s() {
    let Some(url) = backend_url("STRATA_TEST_REDIS_URL") else {
        return;
    };
    let (own, shared) = (unique_name("own"), unique_name("shared"));
    let redis = RedisSettings {
        url: Some(url.clone()),
        prefix: shared.clone(),
        ..RedisSettings::default()
    };
    let mut conn = redis_connection(&url).await;

    let mut settings = memory(3);
    let store = redis.open_memory(&settings).await.unwrap();
    store
        .upsert(record("incidents", "", "inc-1"))
        .await
        .unwrap();
    settings.prefix = Some(own.clone());
    let other = redis.open_memory(&settings).await.unwrap();
    other
        .upsert(record("incidents", "", "inc-2"))
        .await
        .unwrap();

    for (prefix, id) in [(&shared, "inc-1"), (&own, "inc-2")] {
        let exists: i64 = redis::cmd("EXISTS")
            .arg(format!("{prefix}:memory:records:incidents::{id}"))
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(exists, 1, "{prefix}");
    }

    for prefix in [&own, &shared] {
        let _: () = redis::cmd("FT.DROPINDEX")
            .arg(format!("{prefix}:memory:idx"))
            .query_async(&mut conn)
            .await
            .unwrap_or(());
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(format!("{prefix}:*"))
            .query_async(&mut conn)
            .await
            .unwrap();
        if !keys.is_empty() {
            let _: i64 = redis::cmd("DEL")
                .arg(keys)
                .query_async(&mut conn)
                .await
                .unwrap();
        }
    }
}

// --- The memory retention driver ---------------------------------------------

#[tokio::test(start_paused = true)]
async fn the_memory_retention_driver_purges_old_memories_batch_after_batch() {
    let written = TestClock::at(noon() - chrono::Duration::days(60));
    let store = Arc::new(InMemoryMemoryStore::new(3).with_clock(written.clock()));
    for old in ["a", "b", "c", "d", "e"] {
        store.upsert(record("incidents", "", old)).await.unwrap();
    }
    store
        .upsert(record("runbooks", "", "old-runbook"))
        .await
        .unwrap();
    written.set(noon() - chrono::Duration::days(1));
    store
        .upsert(record("incidents", "", "recent"))
        .await
        .unwrap();

    let host = Host::new()
        .with_clock(TestClock::at(noon()).clock())
        .retain_memory(
            store.clone(),
            MemoryRetentionDriver::new()
                .with_older_than(Duration::from_secs(30 * 86_400))
                .with_namespace("incidents")
                .with_batch_size(2),
        );
    let status = host.status();
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(host.run(shutdown.clone()));

    // One round, three purges of two: every old incident is gone before the
    // driver waits; another namespace's is kept.
    until("the first round finished", || {
        status
            .drivers()
            .iter()
            .any(|d| d.name == "memory-retention" && d.rounds == 1)
    })
    .await;
    assert_eq!(ids(&store, "incidents", "").await, ["recent"]);
    assert_eq!(ids(&store, "runbooks", "").await, ["old-runbook"]);
    shutdown.cancel();
    assert_eq!(running.await.unwrap(), Ok(()));
}

#[tokio::test(start_paused = true)]
async fn the_memory_retention_driver_purges_one_tenant_when_told() {
    let written = TestClock::at(noon() - chrono::Duration::days(60));
    let store = Arc::new(InMemoryMemoryStore::new(3).with_clock(written.clock()));
    store
        .upsert(record("incidents", "acme", "a"))
        .await
        .unwrap();
    store
        .upsert(record("incidents", "globex", "g"))
        .await
        .unwrap();
    let store_dyn: Arc<dyn datalium_strata::memory::MemoryRetention> = store.clone();

    let host = Host::new()
        .with_clock(TestClock::at(noon()).clock())
        .retain_memory(
            store_dyn,
            MemoryRetentionDriver::new()
                .with_older_than(Duration::from_secs(30 * 86_400))
                .with_tenant("acme"),
        );
    let status = host.status();
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(host.run(shutdown.clone()));
    until("the first round finished", || {
        status
            .drivers()
            .iter()
            .any(|d| d.name == "memory-retention" && d.rounds == 1)
    })
    .await;

    assert!(ids(&store, "incidents", "acme").await.is_empty());
    assert_eq!(ids(&store, "incidents", "globex").await, ["g"]);
    shutdown.cancel();
    assert_eq!(running.await.unwrap(), Ok(()));
}

#[tokio::test(start_paused = true)]
async fn a_memory_retention_driver_without_what_it_needs_stops_the_host_from_starting() {
    let store = Arc::new(InMemoryMemoryStore::new(3));
    let refusal = async |host: Host| -> String {
        let HostError(refusal) = host.run(CancellationToken::new()).await.unwrap_err();
        refusal
    };
    let year = Duration::from_secs(365 * 86_400);

    assert_eq!(
        refusal(Host::new().retain_memory(store.clone(), MemoryRetentionDriver::new())).await,
        "the memory retention driver needs an older_than: how long a memory is kept after it was last written"
    );
    assert_eq!(
        refusal(Host::new().retain_memory(
            store.clone(),
            MemoryRetentionDriver::new().with_older_than(Duration::ZERO)
        ))
        .await,
        "the memory retention driver needs an older_than: how long a memory is kept after it was last written"
    );
    assert_eq!(
        refusal(
            Host::new().retain_memory(
                store.clone(),
                MemoryRetentionDriver::new()
                    .with_older_than(year)
                    .with_interval(Duration::ZERO)
            )
        )
        .await,
        "the memory retention driver's interval must be more than zero"
    );
    // A purge the store would refuse stops the host, not every round.
    assert_eq!(
        refusal(
            Host::new().retain_memory(
                store.clone(),
                MemoryRetentionDriver::new()
                    .with_older_than(year)
                    .with_namespace("")
            )
        )
        .await,
        "the namespace is empty"
    );
    assert_eq!(
        refusal(
            Host::new().retain_memory(
                store.clone(),
                MemoryRetentionDriver::new()
                    .with_older_than(year)
                    .with_tenant("t".repeat(257))
            )
        )
        .await,
        "the tenant is longer than 256 bytes"
    );
    for batch_size in [0, 1001] {
        assert_eq!(
            refusal(
                Host::new().retain_memory(
                    store.clone(),
                    MemoryRetentionDriver::new()
                        .with_older_than(year)
                        .with_batch_size(batch_size)
                )
            )
            .await,
            "the memory retention driver's batch_size must be between 1 and 1000"
        );
    }
}
