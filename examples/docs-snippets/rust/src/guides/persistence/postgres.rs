//! Samples for <https://stratamesh.dev/guides/persistence/postgres/>.
//!
//! Every test opens its stores in a Postgres schema of its own ([`PgSandbox`]),
//! so the thread IDs the page shows can't collide with another test's. The
//! tests need `STRATA_TEST_POSTGRES_URL`: without it they print `SKIPPED` and
//! pass, unless `STRATA_REQUIRE_BACKENDS=1`, which makes them fail.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datalium_strata::checkpoint::{CheckpointStore, GraphExecutionResult};
use datalium_strata::graph::{CheckpointPolicy, CompileOptions, CompiledGraph, StateGraph};
use datalium_strata::history::{ChatMessage, HistoryStore, MessageRole};
use datalium_strata_host::StrataSettings;
use datalium_strata_memory::PostgresStoreOptions;
use datalium_strata_memory::postgres_store::{PostgresCheckpointStore, PostgresHistoryStore};
use serde::{Deserialize, Serialize};

type TestResult = Result<(), Box<dyn Error>>;

/// The compose file's database, which the page's samples name.
const COMPOSE_URL: &str = "postgres://strata:strata@localhost:5432/strata";

#[tokio::test]
async fn open() -> TestResult {
    let Some(sandbox) = PgSandbox::new("postgres_open").await else {
        return Ok(());
    };
    // docs:begin guides/persistence/postgres/open
    use std::sync::Arc;

    use chrono::Utc;
    use datalium_strata::checkpoint::GraphExecutionResult;
    use datalium_strata::history::{ChatMessage, HistoryStore, MessageRole};
    use datalium_strata_memory::postgres_store::{PostgresCheckpointStore, PostgresHistoryStore};

    let url = "postgres://strata:strata@localhost:5432/strata";
    // docs:hide
    let url = sandbox.instead_of(url);
    // docs:show
    let checkpoints = Arc::new(
        PostgresCheckpointStore::connect(url)
            .await
            .map_err(|e| e.0)?,
    );
    let history = PostgresHistoryStore::connect(url).await.map_err(|e| e.0)?;

    let initial = Refund {
        customer: "acme".into(),
        amount: 120.0,
        status: "new".into(),
    };
    let result = build().run("refund-1042", initial, checkpoints).await;
    history
        .append(
            "refund-1042",
            ChatMessage {
                role: MessageRole::User,
                content: Arc::from("Refund order 1042"),
                timestamp: Utc::now(),
            },
        )
        .await
        .map_err(|e| e.0)?;

    match result {
        GraphExecutionResult::Interrupted { suspended_node, .. } => {
            println!("refund-1042 waits after {suspended_node}")
        }
        _ => println!("unexpected result"),
    }
    // docs:end guides/persistence/postgres/open

    let store = PostgresCheckpointStore::connect(url).await?;
    let latest = store
        .load("refund-1042")
        .await?
        .expect("the run saved the thread");
    assert_eq!(latest.suspended_node.as_deref(), Some("Prepare"));
    let messages = history.get_history("refund-1042", 10).await?;
    assert_eq!(messages.len(), 1);
    assert_eq!(&*messages[0].content, "Refund order 1042");
    drop((store, history));
    sandbox.remove().await;
    Ok(())
}

#[tokio::test]
async fn tls() -> TestResult {
    let Some(sandbox) = PgSandbox::new("postgres_tls").await else {
        return Ok(());
    };
    // docs:begin guides/persistence/postgres/tls
    let config = "host=db.internal user=strata password=... dbname=strata sslmode=require";
    // docs:hide
    // The test's server has no TLS and no db.internal: the same calls, on the sandbox, with TLS if the server offers it.
    let config = &*format!("{}&sslmode=prefer", sandbox.instead_of(config));
    // docs:show
    let connector = postgres_native_tls::MakeTlsConnector::new(native_tls::TlsConnector::new()?);
    let (client, connection) = tokio_postgres::connect(config, connector).await?;
    tokio::spawn(connection);
    let checkpoints = PostgresCheckpointStore::from_client(client)
        .await
        .map_err(|e| e.0)?;
    // docs:end guides/persistence/postgres/tls

    let checkpoints = Arc::new(checkpoints);
    let initial = Refund {
        customer: "acme".into(),
        amount: 120.0,
        status: "new".into(),
    };
    let result = build()
        .run("refund-1042", initial, checkpoints.clone())
        .await;
    assert!(matches!(result, GraphExecutionResult::Interrupted { .. }));
    assert!(checkpoints.load("refund-1042").await?.is_some());
    drop(checkpoints);
    sandbox.remove().await;
    Ok(())
}

#[tokio::test]
async fn auto_migrate() -> TestResult {
    let Some(sandbox) = PgSandbox::new("postgres_migrate").await else {
        return Ok(());
    };
    let url = sandbox.instead_of(COMPOSE_URL);

    // Before the deploy step has run, the application's store refuses the database.
    let Err(refused) = PostgresCheckpointStore::connect_with(
        url,
        PostgresStoreOptions::new().with_auto_migrate(false),
    )
    .await
    else {
        panic!("a database that is behind is refused");
    };
    assert_eq!(
        refused.0,
        "database is at checkpoints schema version 0, and this build needs 2: run its migration"
    );

    // docs:begin guides/persistence/postgres/auto-migrate
    // The deploy step:
    PostgresCheckpointStore::connect(url)
        .await
        .map_err(|e| e.0)?;
    PostgresHistoryStore::connect(url).await.map_err(|e| e.0)?;

    // The application:
    let options = PostgresStoreOptions::new().with_auto_migrate(false);
    let checkpoints = PostgresCheckpointStore::connect_with(url, options)
        .await
        .map_err(|e| e.0)?;
    // docs:end guides/persistence/postgres/auto-migrate

    assert!(checkpoints.load("refund-1042").await?.is_none());
    // `migrate()` migrates whatever the store's options say.
    checkpoints.migrate().await?;
    drop(checkpoints);
    sandbox.remove().await;
    Ok(())
}

#[tokio::test]
async fn retries() -> TestResult {
    let Some(sandbox) = PgSandbox::new("postgres_retries").await else {
        return Ok(());
    };
    let url = sandbox.instead_of(COMPOSE_URL);
    // docs:begin guides/persistence/postgres/retries
    use std::time::Duration;

    use datalium_strata_memory::{GroupCommit, PostgresStoreOptions, StoreRetry};

    let options = PostgresStoreOptions::new()
        .with_retry(
            StoreRetry::new()
                .with_max_attempts(6)
                .with_max_delay(Duration::from_secs(5)),
        )
        .with_pool_size(32)
        .with_group_commit(GroupCommit::new());
    let checkpoints = PostgresCheckpointStore::connect_with(url, options)
        .await
        .map_err(|e| e.0)?;
    // docs:end guides/persistence/postgres/retries

    assert!(checkpoints.group_commits());
    assert_eq!(StoreRetry::none().max_attempts(), 1);
    let checkpoints = Arc::new(checkpoints);
    let graph = Arc::new(build());
    let mut runs = Vec::new();
    for i in 1..=8 {
        let (graph, store) = (graph.clone(), checkpoints.clone());
        runs.push(tokio::spawn(async move {
            let refund = Refund {
                customer: format!("customer-{i}"),
                amount: 10.0 * i as f64,
                status: "new".into(),
            };
            graph.run(format!("refund-{i}"), refund, store).await
        }));
    }
    for run in runs {
        assert!(matches!(
            run.await?,
            GraphExecutionResult::Interrupted { .. }
        ));
    }
    drop(checkpoints);
    sandbox.remove().await;
    Ok(())
}

/// `files/guides/persistence/postgres/strata.json`, the settings the page shows.
#[tokio::test]
async fn host_settings() -> TestResult {
    let text = include_str!("../../../../files/guides/persistence/postgres/strata.json");
    let mut settings: StrataSettings = serde_json::from_str(text)?;
    assert_eq!(settings.postgres.url.as_deref(), Some(COMPOSE_URL));
    assert_eq!(settings.postgres.pool_size, 16);
    assert_eq!(settings.postgres.retry.max_attempts, 6);
    assert!(settings.postgres.group_commit.enabled);
    assert!(settings.postgres.auto_migrate);

    let Some(sandbox) = PgSandbox::new("postgres_host").await else {
        return Ok(());
    };
    settings.postgres.url = Some(sandbox.instead_of(COMPOSE_URL).to_string());
    let checkpoints = settings.postgres.open().await?;
    let history = settings.postgres.open_history().await?;
    assert!(checkpoints.group_commits());
    assert!(checkpoints.load("refund-1042").await?.is_none());
    assert!(history.get_history("refund-1042", 10).await?.is_empty());
    drop((checkpoints, history));
    sandbox.remove().await;
    Ok(())
}

#[tokio::test]
async fn resume() -> TestResult {
    let Some(sandbox) = PgSandbox::new("postgres_resume").await else {
        return Ok(());
    };
    // docs:begin guides/persistence/postgres/resume
    use datalium_strata::checkpoint::HumanSignal;

    // docs:hide
    let url = sandbox.instead_of(COMPOSE_URL);
    let checkpoints = Arc::new(PostgresCheckpointStore::connect(url).await?);
    let history = PostgresHistoryStore::connect(url).await?;

    // What the first sample on the page did, in this language or the other.
    let initial = Refund {
        customer: "acme".into(),
        amount: 120.0,
        status: "new".into(),
    };
    let paused = build()
        .run("refund-1042", initial, checkpoints.clone())
        .await;
    assert!(matches!(paused, GraphExecutionResult::Interrupted { .. }));
    history
        .append(
            "refund-1042",
            ChatMessage {
                role: MessageRole::User,
                content: Arc::from("Refund order 1042"),
                timestamp: chrono::Utc::now(),
            },
        )
        .await?;
    let reader = checkpoints.clone();

    // docs:show
    match build()
        .resume("refund-1042", HumanSignal::approve("jdoe"), checkpoints)
        .await
    {
        GraphExecutionResult::Completed(refund) => println!("refund-1042 is {}", refund.status),
        _ => println!("unexpected result"),
    }
    for message in history
        .get_history("refund-1042", 10)
        .await
        .map_err(|e| e.0)?
    {
        println!("{:?}: {}", message.role, message.content);
    }

    // docs:hide
    let latest = reader
        .load("refund-1042")
        .await?
        .expect("the thread is saved");
    assert_eq!(latest.frozen_state["status"], "paid");
    let messages = history.get_history("refund-1042", 10).await?;
    let lines: Vec<String> = messages
        .iter()
        .map(|message| format!("{:?}: {}", message.role, message.content))
        .collect();
    assert_eq!(lines, ["User: Refund order 1042"]);
    drop((reader, history));
    sandbox.remove().await;
    Ok(())
}

// docs:show
#[derive(Clone, Serialize, Deserialize)]
struct Refund {
    customer: String,
    amount: f64,
    status: String,
}
// docs:end guides/persistence/postgres/resume

#[tokio::test]
async fn read_only() -> TestResult {
    let Some(sandbox) = PgSandbox::new("postgres_read_only").await else {
        return Ok(());
    };
    {
        let writable = Arc::new(
            PostgresCheckpointStore::connect(sandbox.instead_of("postgres://"))
                .await
                .map_err(|e| e.0)?,
        );
        let initial = Refund {
            customer: "acme".into(),
            amount: 120.0,
            status: "new".into(),
        };
        build().run("refund-1042", initial, writable).await;
    }

    // docs:begin guides/persistence/postgres/read-only
    use datalium_strata_memory::PostgresStoreOptions;
    use datalium_strata_memory::postgres_store::PostgresCheckpointStore;

    let url = "postgres://reader:secret@replica.internal:5432/strata";
    // docs:hide
    let url = sandbox.instead_of(url);
    // docs:show
    let store = PostgresCheckpointStore::connect_read_only(url, PostgresStoreOptions::new())
        .await
        .map_err(|e| e.0)?;
    // docs:end guides/persistence/postgres/read-only

    assert!(store.is_read_only());
    let latest = store
        .load("refund-1042")
        .await
        .map_err(|e| e.0)?
        .expect("the thread is there");
    assert_eq!(
        store.save(latest).await.expect_err("a save is refused").0,
        "the store was opened read-only: save is refused"
    );
    assert_eq!(
        store.migrate().await.expect_err("a migration is refused").0,
        "the store was opened read-only: migrate is refused"
    );
    drop(store);

    // A schema with no Strata tables is refused, by the database's name, and
    // left empty.
    let Some(empty) = PgSandbox::new("postgres_read_only_empty").await else {
        return Ok(());
    };
    let base = backend_url("STRATA_TEST_POSTGRES_URL").expect("the sandbox has a URL");
    let config: tokio_postgres::Config = base.parse()?;
    let database = config
        .get_dbname()
        .expect("the URL names a database")
        .to_string();
    let refused = PostgresCheckpointStore::connect_read_only(
        empty.instead_of("postgres://"),
        PostgresStoreOptions::new(),
    )
    .await
    .err()
    .expect("a schema with no Strata log is refused");
    assert_eq!(
        refused.0,
        format!("'{database}' has no Strata checkpoint log")
    );
    empty.remove().await;
    sandbox.remove().await;
    Ok(())
}

/// The refund graph from `/guides/production/hosting/`: `Prepare`, an
/// interrupt boundary, then `Pay`.
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

/// A throwaway Postgres schema in the database `STRATA_TEST_POSTGRES_URL`
/// names, as the strata tests' `PgSandbox` makes one: a store on
/// [`Self::instead_of`]'s URL puts its tables in it. [`Self::remove`] drops it.
pub(super) struct PgSandbox {
    base: String,
    schema: String,
    url: String,
}

impl PgSandbox {
    /// A new schema, or `None` (after saying so) when the test should skip.
    pub(super) async fn new(label: &str) -> Option<Self> {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let base = backend_url("STRATA_TEST_POSTGRES_URL")?;
        let schema = format!(
            "docs_{label}_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let separator = if base.contains('?') { '&' } else { '?' };
        let url = format!("{base}{separator}options=-c%20search_path%3D{schema}");
        let sandbox = Self { base, schema, url };
        sandbox
            .execute(&format!("CREATE SCHEMA {}", sandbox.schema))
            .await;
        Some(sandbox)
    }

    /// The sandbox's URL, for a sample that shows `shown`, the compose file's
    /// database or a server the test doesn't have.
    pub(super) fn instead_of(&self, shown: &str) -> &str {
        assert!(!shown.is_empty());
        &self.url
    }

    /// Drops the schema, with every table in it.
    pub(super) async fn remove(self) {
        self.execute(&format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema))
            .await;
    }

    async fn execute(&self, sql: &str) {
        let (client, connection) = tokio_postgres::connect(&self.base, tokio_postgres::NoTls)
            .await
            .expect("the test Postgres is reachable");
        tokio::spawn(connection);
        client
            .batch_execute(sql)
            .await
            .expect("the sandbox statement runs");
    }
}

/// The URL in `var`, or `None` (after saying so) if the test should skip, as
/// the strata tests' `backend_url` reads it: `STRATA_REQUIRE_BACKENDS=1` turns
/// a missing variable into a failure.
pub(super) fn backend_url(var: &str) -> Option<String> {
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
