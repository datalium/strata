//! The host's settings (`design/35-Hosting-Integration.md`, "Configuration"):
//! every option read through serde, the defaults, durations in each accepted
//! form, and `open()` on each store's. The xUnit twin is `HostingTests`'
//! settings tests.
//!
//! The Postgres and Redis tests need `STRATA_TEST_POSTGRES_URL` /
//! `STRATA_TEST_REDIS_URL`; unset, they print `SKIPPED` and pass, unless
//! `STRATA_REQUIRE_BACKENDS=1`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use datalium_strata::checkpoint::{CheckpointQuery, CheckpointStore, ThreadQuery};
use datalium_strata::history::{ChatMessage, HistoryStore, MessageRole};
use datalium_strata::schema::{Checkpoint, CheckpointStatus, SCHEMA_VERSION};
use datalium_strata_host::{
    DeltaSettings, GroupCommitSettings, LeaseSettings, OffloadSettings, PostgresSettings,
    RedisSettings, RetrySettings, SqliteSettings, StrataSettings,
};
use datalium_strata_memory::{Deltas, GroupCommit, StoreRetry};
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
        "strata_host_test_{}_{}_{}",
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

fn finished(thread_id: &str) -> Checkpoint {
    Checkpoint {
        schema_version: SCHEMA_VERSION.to_string(),
        thread_id: thread_id.to_string(),
        status: CheckpointStatus::Completed,
        suspended_node: Some("Send".to_string()),
        next_node: None,
        frozen_state: json!({ "done": true }),
        metadata: None,
        created_at: "2026-09-16T00:00:00Z".to_string(),
        resumed_at: None,
        nested: None,
        decision: None,
        wait: None,
        graph: None,
    }
}

fn hello() -> ChatMessage {
    ChatMessage {
        role: MessageRole::User,
        content: "hello".into(),
        timestamp: chrono::Utc::now(),
    }
}

#[test]
fn settings_come_from_any_serde_source() {
    let settings: StrataSettings = serde_json::from_value(json!({
        "postgres": {
            "url": "postgres://db/strata",
            "auto_migrate": false,
            "retry": { "max_attempts": 6, "max_delay": "5s" }
        },
        "redis": {
            "url": "redis://cache",
            "prefix": "ops",
            "finished_thread_ttl": "30d",
            "retry": { "initial_delay": "100ms" }
        },
        "sqlite": { "path": "strata.sqlite" },
        "runs": { "lease": { "owner": "worker-7", "ttl": 30 } },
        "wake": { "interval": "2s", "batch_size": 10, "metadata": { "graph": "investigation" }, "match_graph": false },
        "recovery": { "interval": 15, "stale_after": "10m", "max_recoveries": 3 },
        "retention": { "interval": "10m", "older_than": "30d", "states": ["completed", "failed"] }
    }))
    .unwrap();

    assert_eq!(
        settings.postgres,
        PostgresSettings {
            url: Some("postgres://db/strata".to_string()),
            auto_migrate: false,
            retry: RetrySettings {
                max_attempts: 6,
                initial_delay: Duration::from_millis(50),
                max_delay: Duration::from_secs(5),
            },
            ..PostgresSettings::default()
        }
    );
    assert_eq!(
        settings.postgres.retry.policy(),
        StoreRetry::new()
            .with_max_attempts(6)
            .with_max_delay(Duration::from_secs(5))
    );
    assert_eq!(
        settings.redis,
        RedisSettings {
            url: Some("redis://cache".to_string()),
            prefix: "ops".to_string(),
            finished_thread_ttl: Some(Duration::from_secs(30 * 86_400)),
            retry: RetrySettings {
                max_attempts: 4,
                initial_delay: Duration::from_millis(100),
                max_delay: Duration::from_secs(2),
            },
            deltas: DeltaSettings::default(),
            offload: OffloadSettings::default(),
        }
    );
    assert_eq!(settings.sqlite.path.as_deref(), Some("strata.sqlite"));
    assert_eq!(
        settings.runs.lease,
        LeaseSettings {
            owner: "worker-7".to_string(),
            ttl: Some(Duration::from_secs(30)),
        }
    );
    assert!(settings.runs.config().has_lease());

    assert_eq!(settings.wake.interval, Duration::from_secs(2));
    assert_eq!(settings.wake.batch_size, 10);
    assert_eq!(settings.wake.metadata["graph"], "investigation");
    assert!(!settings.wake.match_graph);

    assert_eq!(settings.recovery.interval, Duration::from_secs(15));
    assert_eq!(
        settings.recovery.stale_after,
        Some(Duration::from_secs(600))
    );
    assert_eq!(settings.recovery.max_recoveries, Some(3));

    assert_eq!(settings.retention.interval, Duration::from_secs(600));
    assert_eq!(
        settings.retention.older_than,
        Some(Duration::from_secs(30 * 86_400))
    );
    assert_eq!(settings.retention.states, ["completed", "failed"]);
}

#[test]
fn without_a_source_every_setting_has_its_default() {
    let settings: StrataSettings = serde_json::from_value(json!({})).unwrap();

    assert_eq!(settings.postgres, PostgresSettings::default());
    assert!(settings.postgres.auto_migrate && settings.postgres.url.is_none());
    assert_eq!(settings.postgres.retry.policy(), StoreRetry::new());
    assert_eq!(settings.redis.prefix, "strata");
    assert_eq!(settings.redis.finished_thread_ttl, None);
    assert_eq!(settings.sqlite, SqliteSettings::default());

    // No lease unless a ttl says so; the owner names the process.
    assert_eq!(settings.runs.lease.ttl, None);
    assert!(
        settings
            .runs
            .lease
            .owner
            .ends_with(&format!("/{}", std::process::id()))
    );
    assert!(!settings.runs.config().has_lease());

    assert_eq!(
        (
            settings.wake.interval,
            settings.wake.batch_size,
            settings.wake.match_graph
        ),
        (Duration::from_secs(5), 100, true)
    );
    assert_eq!(
        (
            settings.recovery.interval,
            settings.recovery.batch_size,
            settings.recovery.stale_after
        ),
        (Duration::from_secs(30), 100, None)
    );
    assert_eq!(
        (
            settings.retention.interval,
            settings.retention.batch_size,
            settings.retention.older_than
        ),
        (Duration::from_secs(3600), 1000, None)
    );
    assert!(settings.retention.states.is_empty());

    // An explicit null is "not set" too.
    let nulls: StrataSettings = serde_json::from_value(json!({
        "runs": { "lease": { "ttl": null } },
        "recovery": { "stale_after": null }
    }))
    .unwrap();
    assert_eq!(nulls.runs.lease.ttl, None);
    assert_eq!(nulls.recovery.stale_after, None);
}

#[test]
fn a_duration_that_makes_no_sense_is_refused_with_what_to_write() {
    for bad in [json!("soon"), json!(-5), json!("5 weeks"), json!(true)] {
        let refused =
            serde_json::from_value::<StrataSettings>(json!({ "wake": { "interval": bad } }))
                .err()
                .unwrap_or_else(|| panic!("{bad} was accepted"));
        assert!(!refused.to_string().is_empty());
    }
    let refused =
        serde_json::from_value::<StrataSettings>(json!({ "wake": { "interval": "soon" } }))
            .unwrap_err()
            .to_string();
    assert!(
        refused.starts_with(
            "'soon' is not a duration: use a number of seconds, or a number with ms, s, m, h or d"
        ),
        "{refused}"
    );
    let fractional: StrataSettings =
        serde_json::from_value(json!({ "wake": { "interval": 0.25 } })).unwrap();
    assert_eq!(fractional.wake.interval, Duration::from_millis(250));
}

#[tokio::test]
async fn a_store_with_nowhere_to_connect_says_which_setting_is_missing() {
    let settings = StrataSettings::default();

    assert_eq!(
        settings.postgres.open().await.err().unwrap().0,
        "Strata's Postgres store has no url: set postgres.url"
    );
    assert_eq!(
        settings.postgres.open_history().await.err().unwrap().0,
        "Strata's Postgres store has no url: set postgres.url"
    );
    assert_eq!(
        settings.redis.open().await.err().unwrap().0,
        "Strata's Redis store has no url: set redis.url"
    );
    assert_eq!(
        settings.sqlite.open().err().unwrap().0,
        "Strata's SQLite store has no file: set sqlite.path"
    );
}

#[tokio::test]
async fn group_commit_is_off_unless_enabled_and_its_settings_reach_the_stores() {
    let settings: StrataSettings = serde_json::from_value(json!({
        "postgres": {
            "pool_size": 32,
            "group_commit": { "enabled": true, "max_batch": 64, "max_concurrent_flushes": 2 }
        },
        "sqlite": { "group_commit": { "enabled": true } }
    }))
    .unwrap();

    let postgres = settings.postgres.options();
    assert_eq!(postgres.pool_size(), 32);
    assert_eq!(
        postgres.group_commit(),
        Some(
            &GroupCommit::new()
                .with_max_batch(64)
                .with_max_concurrent_flushes(2)
        )
    );
    assert_eq!(
        settings.sqlite.group_commit,
        GroupCommitSettings {
            enabled: true,
            ..GroupCommitSettings::default()
        }
    );
    let defaults = StrataSettings::default();
    assert_eq!(defaults.postgres.options().group_commit(), None);
    assert_eq!(defaults.postgres.options().pool_size(), 16);

    let path = std::env::temp_dir().join(format!("{}.sqlite", unique_name("batched")));
    let sqlite = SqliteSettings {
        path: Some(path.to_string_lossy().into_owned()),
        ..settings.sqlite.clone()
    };
    {
        let store = sqlite.open().unwrap();
        assert!(store.group_commits());
        store.save(finished("t")).await.unwrap();
        assert!(store.load("t").await.unwrap().is_some());
    }
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn deltas_are_off_unless_enabled_and_their_settings_reach_the_stores() {
    let settings: StrataSettings = serde_json::from_value(json!({
        "postgres": { "deltas": { "enabled": true, "full_every": 8 } },
        "redis": { "deltas": { "enabled": true, "cached_threads": 500 } },
        "sqlite": { "deltas": { "enabled": true } }
    }))
    .unwrap();

    assert_eq!(
        settings.postgres.options().deltas(),
        Some(&Deltas::new().with_full_every(8))
    );
    assert_eq!(
        settings.redis.options().deltas(),
        Some(&Deltas::new().with_cached_threads(500))
    );
    let defaults = StrataSettings::default();
    assert_eq!(defaults.postgres.options().deltas(), None);
    assert_eq!(defaults.redis.options().deltas(), None);
    assert_eq!(defaults.sqlite.deltas, DeltaSettings::default());

    let path = std::env::temp_dir().join(format!("{}.sqlite", unique_name("deltas")));
    let sqlite = SqliteSettings {
        path: Some(path.to_string_lossy().into_owned()),
        ..settings.sqlite.clone()
    };
    {
        let store = sqlite.open().unwrap();
        assert!(store.writes_deltas());
        store.save(finished("t")).await.unwrap();
        assert!(store.load("t").await.unwrap().is_some());
    }
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn offload_is_off_unless_enabled_and_its_settings_reach_the_stores() {
    let blobs = std::env::temp_dir().join(unique_name("host-blobs"));
    let settings: StrataSettings = serde_json::from_value(json!({
        "postgres": { "offload": { "enabled": true, "threshold": 4096 } },
        "redis": { "offload": { "enabled": true, "path": "/var/lib/strata/blobs" } },
        "sqlite": { "offload": { "enabled": true, "threshold": 1, "path": blobs } }
    }))
    .unwrap();

    let postgres = settings.postgres.options();
    let offload = postgres.offload().unwrap();
    assert_eq!(
        (offload.threshold(), offload.blobs().is_some()),
        (4096, false)
    );
    let redis = settings.redis.options();
    let offload = redis.offload().unwrap();
    assert_eq!(
        (offload.threshold(), offload.blobs().is_some()),
        (65536, true)
    );
    let defaults = StrataSettings::default();
    assert!(defaults.postgres.options().offload().is_none());
    assert!(defaults.redis.options().offload().is_none());
    assert_eq!(defaults.sqlite.offload, OffloadSettings::default());

    let path = std::env::temp_dir().join(format!("{}.sqlite", unique_name("offload")));
    let sqlite = SqliteSettings {
        path: Some(path.to_string_lossy().into_owned()),
        ..settings.sqlite.clone()
    };
    {
        let store = sqlite.open().unwrap();
        assert_eq!(store.offloads(), (true, true));
        store.save(finished("t")).await.unwrap();
        assert!(store.load("t").await.unwrap().is_some());
        assert!(std::fs::read_dir(&blobs).unwrap().next().is_some());
    }
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir_all(&blobs);
}

#[tokio::test]
async fn the_sqlite_stores_share_one_file() {
    let path = std::env::temp_dir().join(format!("{}.sqlite", unique_name("sqlite")));
    let settings = SqliteSettings {
        path: Some(path.to_string_lossy().into_owned()),
        ..SqliteSettings::default()
    };
    {
        let store = settings.open().unwrap();
        let history = settings.open_history().unwrap();
        store.save(finished("t")).await.unwrap();
        history.append("s", hello()).await.unwrap();

        let listed = store.list_threads(&ThreadQuery::default()).await.unwrap();
        assert_eq!(listed.threads.len(), 1);
        assert_eq!(history.get_history("s", 10).await.unwrap().len(), 1);
    }
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn postgres_settings_open_stores_that_are_migrated_or_only_checked() {
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

    // A store that doesn't migrate refuses a database that is behind.
    assert_eq!(
        settings(false).open().await.err().unwrap().0,
        "database is at checkpoints schema version 0, and this build needs 2: run its migration"
    );

    let store = settings(true).open().await.unwrap();
    store.save(finished("t")).await.unwrap();
    assert!(store.load("t").await.unwrap().is_some());
    let history = settings(true).open_history().await.unwrap();
    history.append("s", hello()).await.unwrap();
    assert_eq!(history.get_history("s", 10).await.unwrap().len(), 1);

    // And now the store that only checks is satisfied.
    assert!(
        settings(false)
            .open()
            .await
            .unwrap()
            .load("t")
            .await
            .unwrap()
            .is_some()
    );

    drop((store, history));
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
}

#[tokio::test]
async fn redis_settings_open_stores_with_the_prefix_and_the_ttl() {
    let Some(url) = backend_url("STRATA_TEST_REDIS_URL") else {
        return;
    };
    let prefix = unique_name("redis");
    let settings = RedisSettings {
        url: Some(url.clone()),
        prefix: prefix.clone(),
        finished_thread_ttl: Some(Duration::from_secs(3600)),
        ..RedisSettings::default()
    };
    let mut conn = redis_connection(&url).await;

    let store = settings.open().await.unwrap();
    store.save(finished("t")).await.unwrap();
    let ttl: i64 = redis::cmd("PTTL")
        .arg(format!("{prefix}:checkpoints:t"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(ttl > 0, "{ttl}");

    let history = settings.open_history().await.unwrap();
    history.append("s", hello()).await.unwrap();
    let length: i64 = redis::cmd("LLEN")
        .arg(format!("{prefix}:history:s"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(length, 1);

    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{prefix}:*"))
        .query_async(&mut conn)
        .await
        .unwrap();
    let _: i64 = redis::cmd("DEL")
        .arg(keys)
        .query_async(&mut conn)
        .await
        .unwrap();
}

/// `design/40-Run-Scheduler.md`, "Hosting": the scheduler section is read
/// like the others, `open()` makes the scheduler, a source without one has
/// none, and `config_with` gives every run its slots.
#[test]
fn the_scheduler_section_opens_a_scheduler() {
    use datalium_strata_host::{ReservationSettings, SchedulerSettings};

    let settings: StrataSettings = serde_json::from_value(json!({
        "scheduler": {
            "name": "investigations",
            "max_concurrent": 32,
            "max_queued": 2000,
            "max_queue_time": "2m",
            "reservations": [ { "min_priority": 3, "slots": 4 } ],
            "tenant_limit": 8,
            "tenant_limits": { "acme": 16 },
            "tenant_max_queued": 100
        }
    }))
    .unwrap();
    let scheduler = settings.scheduler.as_ref().expect("a scheduler section");
    assert_eq!(scheduler.max_queue_time, Some(Duration::from_secs(120)));
    assert_eq!(
        scheduler.reservations,
        [ReservationSettings {
            min_priority: 3,
            slots: 4
        }]
    );
    let opened = scheduler.open().unwrap();
    let snapshot = opened.snapshot();
    assert_eq!(
        (
            snapshot.name.as_str(),
            snapshot.max_concurrent,
            snapshot.max_queued
        ),
        ("investigations", 32, 2000)
    );
    assert_eq!(snapshot.tenants["acme"].limit, Some(16));
    assert_eq!(opened.options().tenant_key(), "tenant");
    let config = settings.runs.config_with(&opened);
    assert!(config.scheduler().is_some());

    // Absent: no scheduler. Empty: the scheduler's defaults.
    let without: StrataSettings = serde_json::from_value(json!({})).unwrap();
    assert!(without.scheduler.is_none());
    let defaults: StrataSettings = serde_json::from_value(json!({ "scheduler": {} })).unwrap();
    assert_eq!(defaults.scheduler, Some(SchedulerSettings::default()));
    assert_eq!(
        defaults
            .scheduler
            .unwrap()
            .open()
            .unwrap()
            .snapshot()
            .max_concurrent,
        16
    );

    // Invalid: open() refuses it.
    let invalid: StrataSettings =
        serde_json::from_value(json!({ "scheduler": { "max_concurrent": 0 } })).unwrap();
    assert!(invalid.scheduler.unwrap().open().is_err());
}
