//! Samples for <https://stratamesh.dev/guides/persistence/retention-and-storage-size/>.
//!
//! Some samples name a SQLite file or a blob folder relative to the working
//! folder, which `cargo test` sets to this crate's folder: those tests delete
//! them before and after.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use datalium_strata::checkpoint::{CheckpointStore, GraphExecutionResult};
use datalium_strata::graph::{CheckpointPolicy, CompileOptions, CompiledGraph, StateGraph};
use serde::{Deserialize, Serialize};

/// The hash of the 100 KB of evidence, the blob the report graph offloads.
const EVIDENCE_HASH: &str = "55bb3c98333f4f20aa23ec49458f604541b9b5413cdf3b90ad17ceae5653b036";

/// A whole-file sample in `files/guides/persistence/retention-and-storage-size/`.
fn sample(name: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../files/guides/persistence/retention-and-storage-size")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// A SQLite file in the temp folder, unique to this call.
fn temp_path(label: &str) -> PathBuf {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    std::env::temp_dir().join(format!(
        "docs_retention_{}_{}_{label}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

/// Deletes a SQLite file with its journal files, and a blob folder.
fn delete(sqlite: &Path, blobs: Option<&Path>) {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let mut path = sqlite.as_os_str().to_owned();
        path.push(suffix);
        let _ = std::fs::remove_file(path);
    }
    if let Some(blobs) = blobs {
        let _ = std::fs::remove_dir_all(blobs);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Report {
    evidence: String,
    step: u32,
}

/// The report graph of "Turn on deltas and offload".
fn report_graph() -> CompiledGraph<Report> {
    StateGraph::<Report>::new()
        .add_node("Gather", |r: Report| Report {
            evidence: "x".repeat(100_000),
            ..r
        })
        .add_node("Analyse", |r: Report| Report {
            step: r.step + 1,
            ..r
        })
        .add_node("Draft", |r: Report| Report {
            step: r.step + 1,
            ..r
        })
        .add_node("Review", |r: Report| Report {
            step: r.step + 1,
            ..r
        })
        .add_edge("Gather", "Analyse")
        .add_edge("Analyse", "Draft")
        .add_edge("Draft", "Review")
        .compile_with(CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode))
        .expect("the graph is valid")
}

fn empty_report() -> Report {
    Report {
        evidence: String::new(),
        step: 0,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    item: String,
    shipped: bool,
}

#[tokio::test]
async fn purge() {
    use datalium_strata::checkpoint::HumanSignal;
    use datalium_strata_memory::checkpoint::SqliteCheckpointStore;

    let path = temp_path("purge.sqlite");
    let store = Arc::new(SqliteCheckpointStore::open(&path).expect("the file opens"));

    // Four orders wait at Check; three are approved and complete, and order-4
    // still waits.
    let graph = StateGraph::<Order>::new()
        .add_node("Check", |order: Order| order)
        .add_interrupt_boundary("Check")
        .add_node("Ship", |order: Order| Order {
            shipped: true,
            ..order
        })
        .add_edge("Check", "Ship")
        .compile_with(CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode))
        .expect("the graph is valid");
    for id in ["order-1", "order-2", "order-3", "order-4"] {
        let order = Order {
            item: id.to_string(),
            shipped: false,
        };
        let result = graph.run(id, order, store.clone()).await;
        assert!(matches!(result, GraphExecutionResult::Interrupted { .. }));
    }
    for id in ["order-1", "order-2", "order-3"] {
        let result = graph
            .resume(id, HumanSignal::approve("lead"), store.clone())
            .await;
        assert!(matches!(result, GraphExecutionResult::Completed(_)));
    }

    // docs:begin guides/persistence/retention-and-storage-size/purge
    use chrono::{DateTime, Duration, Utc};
    use datalium_strata::checkpoint::{CheckpointRetention, CheckpointStore, PurgeStates};

    async fn purge_all(store: &impl CheckpointRetention, older_than: DateTime<Utc>) -> usize {
        const BATCH: usize = 100;
        let mut total = 0;
        loop {
            let purged = store
                .purge(older_than, &PurgeStates::finished(), BATCH)
                .await
                .expect("the purge succeeds");
            total += purged;
            if purged < BATCH {
                return total;
            }
        }
    }

    // In main, with `store` an Arc<SqliteCheckpointStore>:
    let older_than = Utc::now() - Duration::days(30);
    println!(
        "Older than 30 days: {} threads purged",
        purge_all(store.as_ref(), older_than).await
    );

    // A cutoff just ahead of now, so the threads this example just finished count as old.
    let cutoff = Utc::now() + Duration::seconds(1);
    println!(
        "Older than now: {} threads purged",
        purge_all(store.as_ref(), cutoff).await
    );

    for id in ["order-1", "order-4"] {
        match store.load(id).await.expect("the load succeeds") {
            Some(latest) => println!("{id}: kept ({:?})", latest.status),
            None => println!("{id}: gone"),
        }
    }
    // docs:end guides/persistence/retention-and-storage-size/purge

    // What the page says it prints: the three completed orders went, and the
    // waiting one stayed.
    for id in ["order-1", "order-2", "order-3"] {
        assert!(store.load(id).await.unwrap().is_none(), "{id} is purged");
    }
    let kept = store
        .load("order-4")
        .await
        .unwrap()
        .expect("order-4 is kept");
    assert_eq!(format!("{:?}", kept.status), "Interrupted");
    assert_eq!(purge_all(store.as_ref(), cutoff).await, 0);

    drop(store);
    delete(&path, None);
}

#[tokio::test]
async fn schedule() -> Result<(), Box<dyn std::error::Error>> {
    use datalium_strata_host::HostError;

    let path = temp_path("schedule.sqlite");

    // docs:begin guides/persistence/retention-and-storage-size/schedule
    use std::sync::Arc;

    use datalium_strata::CancellationToken;
    use datalium_strata_host::{Host, StrataSettings};

    // docs:hide
    let read = || -> Result<StrataSettings, Box<dyn std::error::Error>> {
        // docs:show
        let settings: StrataSettings =
            serde_json::from_str(&std::fs::read_to_string("strata.json")?)?;
        // docs:hide
        Ok(settings)
    };
    // The tests run in this crate's folder, which has no strata.json: the
    // page's file is in files/, and its SQLite file goes to the temp folder.
    let missing = read().expect_err("there is no strata.json here");
    assert!(missing.is::<std::io::Error>(), "{missing}");
    let mut settings: StrataSettings = serde_json::from_str(&sample("schedule-strata.json"))?;
    assert_eq!(settings.sqlite.path.as_deref(), Some("strata.db"));
    assert_eq!(
        settings.retention.older_than,
        Some(std::time::Duration::from_secs(30 * 24 * 60 * 60))
    );
    assert_eq!(
        settings.retention.interval,
        std::time::Duration::from_secs(60 * 60)
    );
    settings.sqlite.path = Some(path.display().to_string());
    // docs:show
    let store = Arc::new(settings.sqlite.open()?);
    let host = Host::new().retain(store.clone(), settings.retention.clone());
    // docs:hide
    // The host runs until it is cancelled: half a second shows that it started.
    let ran = tokio::time::timeout(std::time::Duration::from_millis(500), async {
        // docs:show
        host.run(CancellationToken::new()).await?;
        // docs:hide
        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .await;
    assert!(ran.is_err(), "the host stopped: {ran:?}");
    // docs:show
    // docs:end guides/persistence/retention-and-storage-size/schedule

    // "Without `older_than`, `run` returns an error before any driver starts".
    settings.retention.older_than = None;
    let refused = Host::new()
        .retain(store.clone(), settings.retention.clone())
        .run(CancellationToken::new())
        .await;
    assert_eq!(
        format!("{:?}", refused.expect_err("the host refuses to start")),
        format!(
            "{:?}",
            HostError(
                "the retention driver needs an older_than: how long a thread is kept after its latest checkpoint"
                    .to_string()
            )
        )
    );

    drop(store);
    delete(&path, None);
    Ok(())
}

#[test]
fn deltas_and_offload() {
    delete(Path::new("reports.sqlite"), None);

    // docs:begin guides/persistence/retention-and-storage-size/deltas-and-offload
    use std::sync::Arc;

    use datalium_strata::checkpoint::CheckpointStore;
    use datalium_strata::graph::{CheckpointPolicy, CompileOptions, StateGraph};
    use datalium_strata_memory::checkpoint::SqliteCheckpointStore;
    use datalium_strata_memory::{Deltas, Offload, SqliteStoreOptions};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Report {
        evidence: String,
        step: u32,
    }

    #[tokio::main]
    async fn main() {
        let options = SqliteStoreOptions::new()
            .with_deltas(Deltas::new()) // a whole row every 16
            .with_offload(Offload::new()); // values of 64 KiB or more, into this file
        let store = Arc::new(
            SqliteCheckpointStore::open_with("reports.sqlite", options).expect("the file opens"),
        );

        let graph = StateGraph::<Report>::new()
            .add_node("Gather", |r: Report| Report {
                evidence: "x".repeat(100_000),
                ..r
            })
            .add_node("Analyse", |r: Report| Report {
                step: r.step + 1,
                ..r
            })
            .add_node("Draft", |r: Report| Report {
                step: r.step + 1,
                ..r
            })
            .add_node("Review", |r: Report| Report {
                step: r.step + 1,
                ..r
            })
            .add_edge("Gather", "Analyse")
            .add_edge("Analyse", "Draft")
            .add_edge("Draft", "Review")
            .compile_with(CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode))
            .expect("the graph is valid");

        let initial = Report {
            evidence: String::new(),
            step: 0,
        };
        graph.run("report-1", initial, store.clone()).await;

        let latest = store
            .load("report-1")
            .await
            .expect("the load succeeds")
            .expect("a checkpoint");
        println!(
            "Loaded {:?}: {} bytes of state",
            latest.status,
            latest.frozen_state.to_string().len()
        );
        // docs:hide
        // What the page says it prints.
        assert_eq!(format!("{:?}", latest.status), "Completed");
        assert_eq!(latest.frozen_state.to_string().len(), 100_024);
        // docs:show
    }
    // docs:end guides/persistence/retention-and-storage-size/deltas-and-offload

    main();

    // The rows the page says both languages write.
    let file = rusqlite::Connection::open("reports.sqlite").unwrap();
    let rows: Vec<String> = file
        .prepare(
            "SELECT sequence, status, schema_version, length(frozen_state) FROM checkpoints \
             WHERE thread_id = 'report-1' ORDER BY sequence",
        )
        .unwrap()
        .query_map([], |row| {
            Ok(format!(
                "{} {} {} {}",
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?
            ))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        rows,
        [
            "0 running 1.7.0 24",
            "1 running 1.9.0 192",
            "2 running 1.8.0 81",
            "3 running 1.8.0 81",
            "4 completed 1.9.0 194",
        ]
    );
    let blobs: Vec<(String, i64)> = file
        .prepare("SELECT hash, length(content) FROM checkpoint_blobs")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(blobs, [(format!("sha256:{EVIDENCE_HASH}"), 100_002)]);
    drop(file);

    delete(Path::new("reports.sqlite"), None);
}

#[tokio::test]
async fn filesystem_blobs() {
    use datalium_strata_memory::checkpoint::SqliteCheckpointStore;
    use datalium_strata_memory::{Deltas, Offload, SqliteStoreOptions};

    let path = temp_path("blobs.sqlite");
    delete(&path, Some(Path::new("blobs")));

    // docs:begin guides/persistence/retention-and-storage-size/filesystem-blobs
    use datalium_strata_memory::FileSystemBlobStore;

    let options = SqliteStoreOptions::new()
        .with_deltas(Deltas::new())
        .with_offload(Offload::new().with_blobs(Arc::new(FileSystemBlobStore::new("blobs"))));
    // docs:end guides/persistence/retention-and-storage-size/filesystem-blobs

    let store = Arc::new(SqliteCheckpointStore::open_with(&path, options).expect("the file opens"));
    let result = report_graph()
        .run("report-1", empty_report(), store.clone())
        .await;
    assert!(matches!(result, GraphExecutionResult::Completed(_)));

    // The one file the page says the run writes.
    let thread = Path::new("blobs").join("7265706f72742d31");
    let files: Vec<PathBuf> = std::fs::read_dir(&thread)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(files, [thread.join(EVIDENCE_HASH)]);
    assert_eq!(std::fs::read_dir("blobs").unwrap().count(), 1);
    let latest = store.load("report-1").await.unwrap().unwrap();
    assert_eq!(latest.frozen_state.to_string().len(), 100_024);

    drop(store);
    delete(&path, Some(Path::new("blobs")));
}

#[tokio::test]
async fn host_settings() {
    use datalium_strata_host::StrataSettings;

    let mut settings: StrataSettings =
        serde_json::from_str(&sample("host-strata.json")).expect("the settings parse");
    assert_eq!(settings.sqlite.path.as_deref(), Some("strata.db"));
    assert!(settings.sqlite.deltas.enabled);
    assert!(settings.sqlite.offload.enabled);
    assert_eq!(settings.sqlite.offload.path.as_deref(), Some("blobs"));
    assert_eq!(
        settings.retention.older_than,
        Some(std::time::Duration::from_secs(30 * 24 * 60 * 60))
    );

    // `open()` applies them: the evidence goes to the folder.
    let path = temp_path("host.sqlite");
    let blobs = temp_path("host-blobs");
    settings.sqlite.path = Some(path.display().to_string());
    settings.sqlite.offload.path = Some(blobs.display().to_string());
    let store = Arc::new(settings.sqlite.open().expect("the file opens"));
    let result = report_graph()
        .run("report-1", empty_report(), store.clone())
        .await;
    assert!(matches!(result, GraphExecutionResult::Completed(_)));
    assert!(blobs.join("7265706f72742d31").join(EVIDENCE_HASH).is_file());

    drop(store);
    delete(&path, Some(&blobs));
}
