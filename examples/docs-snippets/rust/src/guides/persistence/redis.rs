//! Samples for <https://stratamesh.dev/guides/persistence/redis/>.
//!
//! The tests need the Redis at `STRATA_TEST_REDIS_URL`, which CI sets to
//! `redis://localhost:6379`, the URL the samples show. Each test writes under a
//! prefix of its own, which a hidden line puts over the sample's `acme`, and
//! deletes its keys afterwards.

use std::sync::LazyLock;
use std::sync::atomic::{AtomicUsize, Ordering};

const REDIS_VAR: &str = "STRATA_TEST_REDIS_URL";

/// The Redis URL, or `None` (after saying so) when the test should skip. With
/// `STRATA_REQUIRE_BACKENDS=1`, as in CI, a missing variable fails the test.
fn redis_url() -> Option<String> {
    match std::env::var(REDIS_VAR) {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            assert!(
                std::env::var("STRATA_REQUIRE_BACKENDS").as_deref() != Ok("1"),
                "{REDIS_VAR} is not set, but STRATA_REQUIRE_BACKENDS=1 says these tests must run"
            );
            eprintln!("SKIPPED: {REDIS_VAR} is not set (start `docker compose up -d --wait`)");
            None
        }
    }
}

/// A key prefix unique to one test; every key under it is deleted on drop.
struct Sandbox {
    url: String,
    prefix: String,
}

impl Sandbox {
    fn new(url: &str, label: &str) -> Self {
        Self {
            url: url.to_string(),
            prefix: unique_prefix(label),
        }
    }

    fn connection(&self) -> redis::Connection {
        redis::Client::open(self.url.as_str())
            .unwrap()
            .get_connection()
            .unwrap()
    }

    /// The keys under the prefix, sorted, with the prefix written as `shown_as`.
    fn keys(&self, shown_as: &str) -> Vec<String> {
        let mut keys: Vec<String> = redis::cmd("KEYS")
            .arg(format!("{}:*", self.prefix))
            .query::<Vec<String>>(&mut self.connection())
            .unwrap()
            .into_iter()
            .map(|key| format!("{shown_as}{}", &key[self.prefix.len()..]))
            .collect();
        keys.sort();
        keys
    }

    fn ttl(&self, key: &str) -> i64 {
        redis::cmd("TTL")
            .arg(format!("{}:{key}", self.prefix))
            .query(&mut self.connection())
            .unwrap()
    }
}

impl Drop for Sandbox {
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

fn unique_prefix(label: &str) -> String {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    format!(
        "docs_redis_{}_{}_{label}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

#[tokio::test]
async fn create() -> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = redis_url() else {
        return Ok(());
    };
    let sandbox = Sandbox::new(&url, "create");

    // docs:begin guides/persistence/redis/create
    use std::sync::Arc;
    use std::time::Duration;

    use datalium_strata_memory::RedisStoreOptions;
    use datalium_strata_memory::redis_store::RedisCheckpointStore;

    let options = RedisStoreOptions::new()
        .with_prefix("acme")
        .with_finished_thread_ttl(Duration::from_secs(30 * 24 * 60 * 60));
    // docs:hide
    let options = options.with_prefix(&sandbox.prefix);
    // docs:show
    let store =
        Arc::new(RedisCheckpointStore::connect_with("redis://localhost:6379", options).await?);
    // docs:end guides/persistence/redis/create

    // `connect_with` indexed the prefix before it returned.
    assert_eq!(sandbox.keys("acme"), ["acme:threads:indexed"]);
    use datalium_strata::checkpoint::CheckpointStore;
    assert!(store.load("redis-create-nothing").await?.is_none());
    Ok(())
}

/// The prefix `run_thread`'s program writes under: its `main` is an item, so
/// it can't see the test's variables.
static RUN_THREAD_PREFIX: LazyLock<String> = LazyLock::new(|| unique_prefix("run"));

#[test]
fn run_thread() {
    let Some(url) = redis_url() else {
        return;
    };
    let sandbox = Sandbox {
        url,
        prefix: RUN_THREAD_PREFIX.clone(),
    };

    // docs:begin guides/persistence/redis/run-thread
    use std::sync::Arc;
    use std::time::Duration;

    use datalium_strata::checkpoint::{GraphExecutionResult, HumanSignal};
    use datalium_strata::graph::{CheckpointPolicy, CompileOptions, StateGraph};
    use datalium_strata_memory::RedisStoreOptions;
    use datalium_strata_memory::redis_store::RedisCheckpointStore;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Refund {
        customer: String,
        amount: f64,
        status: String,
    }

    fn outcome(result: &GraphExecutionResult<Refund>) -> &'static str {
        match result {
            GraphExecutionResult::Completed(_) => "Completed",
            GraphExecutionResult::Interrupted { .. } => "Interrupted",
            _ => "Failed or cancelled",
        }
    }

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
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

        let options = RedisStoreOptions::new()
            .with_prefix("acme")
            .with_finished_thread_ttl(Duration::from_secs(30 * 24 * 60 * 60));
        // docs:hide
        let options = options.with_prefix(RUN_THREAD_PREFIX.as_str());
        // docs:show
        let store =
            Arc::new(RedisCheckpointStore::connect_with("redis://localhost:6379", options).await?);

        let ada = Refund {
            customer: "Ada".into(),
            amount: 42.0,
            status: "new".into(),
        };
        let paused = graph.run("refund-1", ada, store.clone()).await;
        println!("refund-1: {}", outcome(&paused));

        let done = graph
            .resume("refund-1", HumanSignal::approve("lead"), store.clone())
            .await;
        println!("refund-1: {}", outcome(&done));

        let grace = Refund {
            customer: "Grace".into(),
            amount: 7.0,
            status: "new".into(),
        };
        let waiting = graph.run("refund-2", grace, store.clone()).await;
        println!("refund-2: {}", outcome(&waiting));
        // docs:hide
        // What the page says the program prints.
        assert_eq!(
            [outcome(&paused), outcome(&done), outcome(&waiting)],
            ["Interrupted", "Completed", "Interrupted"]
        );
        // docs:show
        Ok(())
    }
    // docs:end guides/persistence/redis/run-thread

    main().expect("the program runs");

    // The keys the page lists, under the test's prefix instead of `acme`.
    assert_eq!(
        sandbox.keys("acme"),
        [
            "acme:checkpoints:refund-1",
            "acme:checkpoints:refund-2",
            "acme:threads",
            "acme:threads:completed",
            "acme:threads:indexed",
            "acme:threads:interrupted",
        ]
    );

    // "Let finished threads expire": the completed thread has the 30-day ttl,
    // the waiting one none.
    let ttl = sandbox.ttl("checkpoints:refund-1");
    assert!(
        (2_592_000 - 60..=2_592_000).contains(&ttl),
        "refund-1's ttl is {ttl}"
    );
    assert_eq!(sandbox.ttl("checkpoints:refund-2"), -1);
}

#[tokio::test]
async fn retry() -> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = redis_url() else {
        return Ok(());
    };
    let sandbox = Sandbox::new(&url, "retry");
    use std::time::Duration;

    use datalium_strata::checkpoint::CheckpointStore;
    use datalium_strata_memory::redis_store::RedisCheckpointStore;

    // docs:begin guides/persistence/redis/retry
    use datalium_strata_memory::{RedisStoreOptions, StoreRetry};

    let options = RedisStoreOptions::new().with_prefix("acme").with_retry(
        StoreRetry::new()
            .with_max_attempts(6)
            .with_max_delay(Duration::from_secs(5)),
    );
    // docs:hide
    let options = options.with_prefix(&sandbox.prefix);
    // docs:show
    let store = RedisCheckpointStore::connect_with("redis://localhost:6379", options).await?;
    // docs:end guides/persistence/redis/retry

    assert!(store.load("redis-retry-nothing").await?.is_none());

    // "`StoreRetry::none()` turns retries off": one attempt.
    assert_eq!(StoreRetry::none().max_attempts(), 1);
    assert_eq!(
        StoreRetry::new()
            .with_max_attempts(6)
            .with_max_delay(Duration::from_secs(5))
            .max_attempts(),
        6
    );
    Ok(())
}

#[tokio::test]
async fn host() -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::Arc;

    use datalium_strata::checkpoint::CheckpointStore;
    use datalium_strata::history::HistoryStore;
    use datalium_strata_host::StrataSettings;

    let Some(url) = redis_url() else {
        return Ok(());
    };
    let sandbox = Sandbox::new(&url, "host");

    // The file as the page shows it.
    let mut settings: StrataSettings = serde_json::from_str(include_str!(
        "../../../../files/guides/persistence/redis/strata.json"
    ))?;
    assert_eq!(
        settings.redis.url.as_deref(),
        Some("redis://localhost:6379")
    );
    assert_eq!(settings.redis.prefix, "acme");
    assert_eq!(
        settings.redis.finished_thread_ttl,
        Some(std::time::Duration::from_secs(30 * 24 * 60 * 60))
    );
    settings.redis.url = Some(url);
    settings.redis.prefix = sandbox.prefix.clone();

    // docs:begin guides/persistence/redis/host
    let store = Arc::new(settings.redis.open().await?);
    let history = settings.redis.open_history().await?;
    // docs:end guides/persistence/redis/host

    assert!(store.load("redis-host-nothing").await?.is_none());
    assert!(
        history
            .get_history("redis-host-nothing", 10)
            .await?
            .is_empty()
    );
    Ok(())
}
