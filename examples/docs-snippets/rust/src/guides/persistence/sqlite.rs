//! Samples for <https://stratamesh.dev/guides/persistence/sqlite/>.
//!
//! Every test uses a SQLite file of its own in the temp directory.

use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datalium_strata::checkpoint::{CheckpointQuery, ThreadQuery, ThreadState};
use datalium_strata::graph::{CheckpointPolicy, CompileOptions, CompiledGraph, StateGraph};
use datalium_strata_host::StrataSettings;
use datalium_strata_memory::GroupCommit;
use datalium_strata_memory::SqliteStoreOptions;
use datalium_strata_memory::checkpoint::SqliteCheckpointStore;
use serde::{Deserialize, Serialize};

type TestResult = Result<(), Box<dyn Error>>;

#[tokio::test]
async fn open() -> TestResult {
    let file = TempFile::new();
    // docs:begin guides/persistence/sqlite/open
    use std::sync::Arc;

    use datalium_strata_memory::checkpoint::SqliteCheckpointStore;
    use datalium_strata_memory::history::SqliteHistoryStore;
    use datalium_strata_memory::{GroupCommit, SqliteStoreOptions};

    let path = "strata.db";
    // docs:hide
    let path = file.instead_of(path);
    // docs:show
    let checkpoints = Arc::new(SqliteCheckpointStore::open_with(
        path,
        SqliteStoreOptions::new().with_group_commit(GroupCommit::new()),
    )?);
    let history = SqliteHistoryStore::open(path)?;
    // docs:end guides/persistence/sqlite/open

    assert!(checkpoints.group_commits());
    // The missing file was created, with the tables the page lists, at
    // checkpoints schema version 2.
    {
        let connection = rusqlite::Connection::open(path)?;
        let mut statement =
            connection.prepare("SELECT name FROM sqlite_master WHERE type = 'table'")?;
        let tables = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        for table in [
            "checkpoints",
            "checkpoint_threads",
            "checkpoint_leases",
            "checkpoint_blobs",
            "strata_schema_version",
            "history_records",
        ] {
            assert!(tables.iter().any(|t| t == table), "no table {table}");
        }
        let version: i64 = connection.query_row(
            "SELECT version FROM strata_schema_version WHERE component = 'checkpoints'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(version, 2);

        // A file that a newer version of Strata wrote is refused, and left as it is.
        connection.execute(
            "UPDATE strata_schema_version SET version = 3 WHERE component = 'checkpoints'",
            [],
        )?;
    }
    drop((checkpoints, history));
    let Err(refused) = SqliteCheckpointStore::open(path) else {
        panic!("a file a newer version wrote is refused");
    };
    assert_eq!(
        refused.0,
        "database is at checkpoints schema version 3, and this build knows 2: upgrade the runtime"
    );
    Ok(())
}

#[tokio::test]
async fn many_threads() -> TestResult {
    let file = TempFile::new();
    let checkpoints = Arc::new(SqliteCheckpointStore::open_with(
        file.path(),
        SqliteStoreOptions::new().with_group_commit(GroupCommit::new()),
    )?);
    let graph = Arc::new(build());

    // docs:begin guides/persistence/sqlite/many-threads
    let mut runs = Vec::new();
    for i in 1..=64 {
        let (graph, store) = (graph.clone(), checkpoints.clone());
        runs.push(tokio::spawn(async move {
            let refund = Refund {
                customer: format!("customer-{i}"),
                amount: 10.0 * i as f64,
                status: "new".into(),
            };
            graph.run(&format!("refund-{i}"), refund, store).await
        }));
    }
    for run in runs {
        run.await?;
    }

    let query = ThreadQuery::default().with_states([ThreadState::Interrupted]);
    let page = checkpoints.list_threads(&query).await?;
    println!("{} refunds wait for approval", page.threads.len());
    // docs:end guides/persistence/sqlite/many-threads

    assert_eq!(page.threads.len(), 64);
    assert!(
        page.threads
            .iter()
            .all(|thread| thread.suspended_node.as_deref() == Some("Prepare"))
    );
    Ok(())
}

/// `files/guides/persistence/sqlite/strata.json`, the settings the page shows.
#[tokio::test]
async fn host_settings() -> TestResult {
    let file = TempFile::new();
    let settings_file = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../files/guides/persistence/sqlite/strata.json")
        .to_string_lossy()
        .into_owned();
    // docs:begin guides/persistence/sqlite/host
    let path = "strata.json";
    // docs:hide
    assert_eq!(path, "strata.json");
    let path = settings_file.as_str();
    // docs:show
    let text = std::fs::read_to_string(path)?;
    let settings: StrataSettings = serde_json::from_str(&text)?;
    // docs:hide
    // The file's "strata.db" is relative to the working directory: the test uses a temp file instead.
    assert_eq!(settings.sqlite.path.as_deref(), Some("strata.db"));
    let mut settings = settings;
    settings.sqlite.path = Some(file.path().to_string_lossy().into_owned());
    // docs:show
    let checkpoints = settings.sqlite.open()?;
    let history = settings.sqlite.open_history()?;
    // docs:end guides/persistence/sqlite/host

    assert!(checkpoints.group_commits());
    assert!(file.path().exists());
    drop((checkpoints, history));

    let Err(refused) = StrataSettings::default().sqlite.open() else {
        panic!("a store with no file is refused");
    };
    assert_eq!(
        refused.0,
        "Strata's SQLite store has no file: set sqlite.path"
    );
    Ok(())
}

#[derive(Clone, Serialize, Deserialize)]
struct Refund {
    customer: String,
    amount: f64,
    status: String,
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

/// A SQLite file in the temp directory, deleted on drop.
struct TempFile(PathBuf);

impl TempFile {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        Self(std::env::temp_dir().join(format!(
            "docs-sqlite-{}-{}.db",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }

    fn path(&self) -> &Path {
        &self.0
    }

    /// The file's path, for a sample that shows `shown`, a path relative to the
    /// working directory.
    fn instead_of(&self, shown: &str) -> &Path {
        assert!(!shown.is_empty());
        &self.0
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
