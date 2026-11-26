//! Samples for <https://stratamesh.dev/guides/persistence/choose-a-store/>.
//!
//! The Postgres tests need `STRATA_TEST_POSTGRES_URL` and the Redis test
//! `STRATA_TEST_REDIS_URL`: without them they print `SKIPPED` and pass, unless
//! `STRATA_REQUIRE_BACKENDS=1`, which makes them fail.

use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use datalium_strata::checkpoint::{CheckpointStore, GraphExecutionResult, HumanSignal};
use datalium_strata::graph::{CheckpointPolicy, CompileOptions, StateGraph};
use datalium_strata::history::{ChatMessage, HistoryStore, MessageRole};
use serde::{Deserialize, Serialize};

use super::postgres::{PgSandbox, backend_url};

type TestResult = Result<(), Box<dyn Error>>;

#[tokio::test]
async fn in_memory() -> TestResult {
    // docs:begin guides/persistence/choose-a-store/in-memory
    use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
    use datalium_strata_memory::history::InMemoryHistoryStore;

    let checkpoints = InMemoryCheckpointStore::new();
    let history = InMemoryHistoryStore::new();
    // docs:end guides/persistence/choose-a-store/in-memory

    pause_and_resume(
        Arc::new(checkpoints),
        &history,
        "guides-persistence-choose-a-store-in-memory",
    )
    .await
}

#[tokio::test]
async fn sqlite() -> TestResult {
    let file = TempFile::new("choose-a-store");
    // docs:begin guides/persistence/choose-a-store/sqlite
    use datalium_strata_memory::checkpoint::SqliteCheckpointStore;
    use datalium_strata_memory::history::SqliteHistoryStore;

    let path = "strata.sqlite";
    // docs:hide
    let path = file.instead_of(path);
    // docs:show
    let checkpoints = SqliteCheckpointStore::open(path)?;
    let history = SqliteHistoryStore::open(path)?;
    // docs:end guides/persistence/choose-a-store/sqlite

    // A missing file is created, and the two stores share it.
    assert!(path.exists());
    pause_and_resume(
        Arc::new(checkpoints),
        &history,
        "guides-persistence-choose-a-store-sqlite",
    )
    .await?;

    // For tests, a SQLite database that lives in memory.
    let in_memory = SqliteCheckpointStore::open_in_memory()?;
    assert!(
        in_memory
            .load("guides-persistence-choose-a-store-sqlite")
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn postgres() -> TestResult {
    let Some(sandbox) = PgSandbox::new("choose_a_store_postgres").await else {
        return Ok(());
    };
    // docs:begin guides/persistence/choose-a-store/postgres
    use datalium_strata_memory::postgres_store::{PostgresCheckpointStore, PostgresHistoryStore};

    let url = "postgres://strata:strata@localhost:5432/strata";
    // docs:hide
    let url = sandbox.instead_of(url);
    // docs:show
    let checkpoints = PostgresCheckpointStore::connect(url).await?;
    let history = PostgresHistoryStore::connect(url).await?;
    // docs:end guides/persistence/choose-a-store/postgres

    pause_and_resume(
        Arc::new(checkpoints),
        &history,
        "guides-persistence-choose-a-store-postgres",
    )
    .await?;
    drop(history);
    sandbox.remove().await;
    Ok(())
}

#[tokio::test]
async fn redis() -> TestResult {
    let Some(redis_url) = backend_url("STRATA_TEST_REDIS_URL") else {
        return Ok(());
    };
    // docs:begin guides/persistence/choose-a-store/redis
    use datalium_strata_memory::RedisStoreOptions;
    use datalium_strata_memory::redis_store::{RedisCheckpointStore, RedisHistoryStore};

    let url = "redis://localhost:6379";
    // docs:hide
    assert!(!url.is_empty());
    let url = redis_url.as_str();
    // docs:show
    let checkpoints =
        RedisCheckpointStore::connect_with(url, RedisStoreOptions::new().with_prefix("acme"))
            .await?;
    let history =
        RedisHistoryStore::connect_with(url, RedisStoreOptions::new().with_prefix("acme")).await?;
    // docs:end guides/persistence/choose-a-store/redis

    // Other tests share the "acme" prefix, so this one only reads, from a
    // thread and a session no one writes.
    let unique = format!(
        "guides-persistence-choose-a-store-redis-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    assert!(checkpoints.load(&unique).await?.is_none());
    assert!(history.get_history(&unique, 10).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn store_options() -> TestResult {
    let Some(sandbox) = PgSandbox::new("choose_a_store_options").await else {
        return Ok(());
    };
    let url = sandbox.instead_of("postgres://strata:strata@localhost:5432/strata");
    // docs:begin guides/persistence/choose-a-store/store-options
    use datalium_strata_memory::postgres_store::PostgresCheckpointStore;
    use datalium_strata_memory::{Deltas, GroupCommit, Offload, PostgresStoreOptions};

    let options = PostgresStoreOptions::new()
        .with_group_commit(GroupCommit::new())
        .with_deltas(Deltas::new())
        .with_offload(Offload::new());
    let checkpoints = PostgresCheckpointStore::connect_with(url, options).await?;
    // docs:end guides/persistence/choose-a-store/store-options

    assert!(checkpoints.group_commits());
    let history =
        datalium_strata_memory::postgres_store::PostgresHistoryStore::connect(url).await?;
    pause_and_resume(
        Arc::new(checkpoints),
        &history,
        "guides-persistence-choose-a-store-options",
    )
    .await?;
    drop(history);
    sandbox.remove().await;
    Ok(())
}

#[tokio::test]
async fn read_only() -> TestResult {
    let file = TempFile::new("choose-a-store-read-only");
    pause(&file.0, "refund-1042").await;
    let before = std::fs::read(&file.0)?;

    // docs:begin guides/persistence/choose-a-store/read-only
    use datalium_strata::checkpoint::{CheckpointQuery, CheckpointStore, ThreadQuery, ThreadState};
    use datalium_strata_memory::SqliteStoreOptions;
    use datalium_strata_memory::checkpoint::SqliteCheckpointStore;

    let path = "strata.sqlite";
    // docs:hide
    let path = file.instead_of(path);
    // docs:show
    let store = SqliteCheckpointStore::open_read_only(path, SqliteStoreOptions::new())?;

    let query = ThreadQuery::default().with_states([ThreadState::Interrupted]);
    let waiting = store.list_threads(&query).await?;
    for thread in &waiting.threads {
        println!(
            "{} waits at {}",
            thread.thread_id,
            thread.suspended_node.as_deref().unwrap_or("-")
        );
    }

    let latest = store
        .load("refund-1042")
        .await?
        .expect("the thread is there");
    if let Err(refused) = store.save(latest.clone()).await {
        println!("{refused}");
    }
    // docs:end guides/persistence/choose-a-store/read-only

    // It reads what the writable store wrote, and refuses the write with the
    // message the page shows.
    assert!(store.is_read_only());
    assert_eq!(waiting.threads.len(), 1);
    assert_eq!(waiting.threads[0].thread_id, "refund-1042");
    assert_eq!(
        waiting.threads[0].suspended_node.as_deref(),
        Some("Prepare")
    );
    let refused = store
        .save(latest)
        .await
        .expect_err("a read-only store refuses a save");
    assert_eq!(refused.0, "the store was opened read-only: save is refused");
    drop(store);

    // Opening, listing, loading and the refused save left the file as it was.
    assert_eq!(std::fs::read(&file.0)?, before);

    // A missing file is refused, and not created.
    let missing = file.0.with_extension("missing");
    let not_there = SqliteCheckpointStore::open_read_only(&missing, SqliteStoreOptions::new())
        .err()
        .expect("a missing file is refused");
    assert_eq!(
        not_there.0,
        format!(
            "no SQLite store at '{}': the file doesn't exist",
            missing.display()
        )
    );
    assert!(!missing.exists());
    Ok(())
}

#[derive(Clone, Serialize, Deserialize)]
struct Refund {
    customer: String,
    amount: f64,
    status: String,
}

/// Runs a refund graph on `checkpoints` until it waits, resumes it from the
/// store, and keeps a message in `history`: what the page says every store
/// does with the same graph code.
async fn pause_and_resume(
    checkpoints: Arc<dyn CheckpointStore>,
    history: &dyn HistoryStore,
    thread_id: &str,
) -> TestResult {
    let graph = refund_graph();

    let initial = Refund {
        customer: "acme".into(),
        amount: 120.0,
        status: "new".into(),
    };
    match graph.run(thread_id, initial, checkpoints.clone()).await {
        GraphExecutionResult::Interrupted { suspended_node, .. } => {
            assert_eq!(suspended_node, "Prepare")
        }
        _ => panic!("the run didn't wait at the boundary"),
    }
    history
        .append(
            thread_id,
            ChatMessage {
                role: MessageRole::User,
                content: Arc::from("Refund order 1042"),
                timestamp: Utc::now(),
            },
        )
        .await?;

    match graph
        .resume(thread_id, HumanSignal::approve("jdoe"), checkpoints)
        .await
    {
        GraphExecutionResult::Completed(refund) => assert_eq!(refund.status, "paid"),
        _ => panic!("the resumed run didn't complete"),
    }
    let messages = history.get_history(thread_id, 10).await?;
    assert_eq!(messages.len(), 1);
    assert_eq!(&*messages[0].content, "Refund order 1042");
    Ok(())
}

/// The refund graph: `Prepare`, an interrupt boundary, then `Pay`.
fn refund_graph() -> datalium_strata::graph::CompiledGraph<Refund> {
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

/// Runs the refund graph on a writable SQLite store at `path` until it waits
/// after `Prepare`: the thread a read-only store then reads.
async fn pause(path: &std::path::Path, thread_id: &str) {
    use datalium_strata_memory::checkpoint::SqliteCheckpointStore;

    let writable = Arc::new(SqliteCheckpointStore::open(path).expect("the file opens"));
    let initial = Refund {
        customer: "acme".into(),
        amount: 120.0,
        status: "new".into(),
    };
    assert!(matches!(
        refund_graph().run(thread_id, initial, writable).await,
        GraphExecutionResult::Interrupted { .. }
    ));
}

/// A SQLite file in the temp directory, deleted on drop.
struct TempFile(PathBuf);

impl TempFile {
    fn new(label: &str) -> Self {
        Self(std::env::temp_dir().join(format!(
            "docs-{label}-{}-{}.sqlite",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        )))
    }

    /// The file's path, for a sample that shows `shown`, a path relative to the
    /// working directory.
    fn instead_of(&self, shown: &str) -> &std::path::Path {
        assert!(!shown.is_empty());
        &self.0
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
