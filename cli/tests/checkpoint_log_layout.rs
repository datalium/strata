//! The CLI opens and seeds SQLite checkpoint logs through `strata-memory`'s
//! `SqliteCheckpointStore` (`design/19-Async-Queryable-Store.md`), so a file it
//! creates must have the shared layout (`schema/README.md`, "SQLite checkpoint
//! log") — the `checkpoints` table and its `checkpoint_threads` head table —
//! and rows it writes must match the shared row fixtures, exactly as
//! `Datalium.Strata.Storage`'s `EfCoreCheckpointStore` is held to them.

use datalium_strata::schema::Checkpoint;

fn fixture(dir: &str, name: &str) -> String {
    let path = format!(
        "{}/../schema/fixtures/{dir}/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"))
}

fn table_info(conn: &rusqlite::Connection, table: &str) -> serde_json::Value {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .unwrap();
    let columns: Vec<serde_json::Value> = stmt
        .query_map([], |row| {
            Ok(serde_json::json!({
                "name": row.get::<_, String>(1)?,
                "type": row.get::<_, String>(2)?,
                "notnull": row.get::<_, i64>(3)? != 0,
                "pk": row.get::<_, i64>(5)?,
            }))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    serde_json::json!(columns)
}

#[tokio::test]
async fn a_log_the_cli_creates_matches_the_shared_column_fixtures() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.sqlite");
    drop(cli::store::open(path.to_str().unwrap()).await.unwrap());

    let conn = rusqlite::Connection::open(&path).unwrap();
    let expected: serde_json::Value =
        serde_json::from_str(&fixture("checkpoint-log", "columns.json")).unwrap();
    assert_eq!(table_info(&conn, "checkpoints"), expected);
    let expected: serde_json::Value =
        serde_json::from_str(&fixture("checkpoint-log", "threads-columns.json")).unwrap();
    assert_eq!(table_info(&conn, "checkpoint_threads"), expected);
    // Thread leases (design/32-Thread-Leases.md).
    let expected: serde_json::Value =
        serde_json::from_str(&fixture("checkpoint-log", "leases-columns.json")).unwrap();
    assert_eq!(table_info(&conn, "checkpoint_leases"), expected);
    // Offloaded values (design/41-Checkpoint-Efficiency.md, "Offload").
    let expected: serde_json::Value =
        serde_json::from_str(&fixture("checkpoint-log", "blobs-columns.json")).unwrap();
    assert_eq!(table_info(&conn, "checkpoint_blobs"), expected);
    // The version its tables are at (design/34-Storage-Hardening.md).
    let expected: serde_json::Value =
        serde_json::from_str(&fixture("checkpoint-log", "schema-versions-columns.json")).unwrap();
    assert_eq!(table_info(&conn, "strata_schema_version"), expected);
    let shared: serde_json::Value =
        serde_json::from_str(&fixture("checkpoint-log", "schema-versions.json")).unwrap();
    let version: i64 = conn
        .query_row(
            "SELECT version FROM strata_schema_version WHERE component = 'checkpoints'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(shared["components"]["checkpoints"], version);
}

#[tokio::test]
async fn a_log_a_newer_runtime_has_moved_on_is_refused_and_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.sqlite");
    drop(cli::store::open(path.to_str().unwrap()).await.unwrap());
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute(
        "UPDATE strata_schema_version SET version = 7 WHERE component = 'checkpoints'",
        [],
    )
    .unwrap();

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_strata"))
        .args(["thread", "list", "--storage"])
        .arg(&path)
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(
            "database is at checkpoints schema version 7, and this build knows 2: upgrade the runtime"
        ),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let version: i64 = conn
        .query_row(
            "SELECT version FROM strata_schema_version WHERE component = 'checkpoints'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, 7);
}

/// The raw `checkpoints` row for `checkpoint`, laid out like the `*.row.json` fixtures.
async fn row_after_insert(checkpoint: &Checkpoint) -> serde_json::Value {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.sqlite");
    let storage = cli::store::open(path.to_str().unwrap()).await.unwrap();
    storage.insert(checkpoint).await.unwrap();
    drop(storage);

    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.query_row(
        "SELECT thread_id, sequence, schema_version, status, suspended_node, next_node,
                frozen_state, metadata, created_at, resumed_at, nested, decision, wait, wake_at,
                graph_id, graph_version
         FROM checkpoints WHERE thread_id = ?1 AND sequence = 0",
        [&checkpoint.thread_id],
        |row| {
            let json = |text: Option<String>| {
                text.map(|text| serde_json::from_str::<serde_json::Value>(&text).unwrap())
            };
            Ok(serde_json::json!({
                "thread_id": row.get::<_, String>(0)?,
                "sequence": row.get::<_, i64>(1)?,
                "schema_version": row.get::<_, String>(2)?,
                "status": row.get::<_, String>(3)?,
                "suspended_node": row.get::<_, Option<String>>(4)?,
                "next_node": row.get::<_, Option<String>>(5)?,
                "frozen_state": json(Some(row.get(6)?)),
                "metadata": json(row.get(7)?),
                "created_at": row.get::<_, String>(8)?,
                "resumed_at": row.get::<_, Option<String>>(9)?,
                "nested": json(row.get(10)?),
                "decision": json(row.get(11)?),
                "wait": json(row.get(12)?),
                "wake_at": row.get::<_, Option<String>>(13)?,
                "graph_id": row.get::<_, Option<String>>(14)?,
                "graph_version": row.get::<_, Option<String>>(15)?,
            }))
        },
    )
    .unwrap()
}

#[tokio::test]
async fn cli_row_for_the_shared_fixture_matches_the_shared_row_fixture() {
    let checkpoint: Checkpoint =
        serde_json::from_str(&fixture("checkpoint-lifecycle", "frozen.json")).unwrap();

    let actual = row_after_insert(&checkpoint).await;

    let expected: serde_json::Value =
        serde_json::from_str(&fixture("checkpoint-log", "frozen.row.json")).unwrap();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn cli_row_for_a_cancelled_checkpoint_matches_the_shared_row_fixture() {
    let checkpoint: Checkpoint =
        serde_json::from_str(&fixture("checkpoint-lifecycle", "cancelled.json")).unwrap();

    let actual = row_after_insert(&checkpoint).await;

    let expected: serde_json::Value =
        serde_json::from_str(&fixture("checkpoint-log", "cancelled.row.json")).unwrap();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn cli_row_for_an_ended_checkpoint_matches_the_shared_row_fixture() {
    let checkpoint: Checkpoint =
        serde_json::from_str(&fixture("checkpoint-lifecycle", "ended.json")).unwrap();

    let actual = row_after_insert(&checkpoint).await;

    let expected: serde_json::Value =
        serde_json::from_str(&fixture("checkpoint-log", "ended.row.json")).unwrap();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn cli_row_for_a_running_checkpoint_matches_the_shared_row_fixture() {
    // design/33-Durable-Execution.md: a status, and nothing new in the layout.
    let checkpoint: Checkpoint =
        serde_json::from_str(&fixture("checkpoint-lifecycle", "running.json")).unwrap();

    let actual = row_after_insert(&checkpoint).await;

    let expected: serde_json::Value =
        serde_json::from_str(&fixture("checkpoint-log", "running.row.json")).unwrap();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn cli_row_for_an_identified_checkpoint_matches_the_shared_row_fixture() {
    // design/31-Graph-Identity.md: the outermost level's graph, in two columns.
    let checkpoint: Checkpoint =
        serde_json::from_str(&fixture("checkpoint-lifecycle", "identified.json")).unwrap();

    let actual = row_after_insert(&checkpoint).await;

    let expected: serde_json::Value =
        serde_json::from_str(&fixture("checkpoint-log", "identified.row.json")).unwrap();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn cli_row_for_a_waiting_checkpoint_matches_the_shared_row_fixture() {
    // design/29-Event-Waits-And-Timers.md: the wait, and the wake time the head table indexes.
    let checkpoint: Checkpoint =
        serde_json::from_str(&fixture("checkpoint-lifecycle", "waiting-event.json")).unwrap();

    let actual = row_after_insert(&checkpoint).await;

    let expected: serde_json::Value =
        serde_json::from_str(&fixture("checkpoint-log", "waiting.row.json")).unwrap();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn cli_row_for_a_nested_checkpoint_holds_the_child_as_json_in_the_nested_column() {
    let checkpoint: Checkpoint =
        serde_json::from_str(&fixture("checkpoint-lifecycle", "nested.json")).unwrap();

    let actual = row_after_insert(&checkpoint).await;

    let expected: serde_json::Value =
        serde_json::from_str(&fixture("checkpoint-log", "nested.row.json")).unwrap();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn cli_row_for_a_checkpoint_with_a_decision_holds_it_as_json_in_the_decision_column() {
    let checkpoint: Checkpoint =
        serde_json::from_str(&fixture("checkpoint-lifecycle", "decision-failed.json")).unwrap();

    let actual = row_after_insert(&checkpoint).await;

    let expected: serde_json::Value =
        serde_json::from_str(&fixture("checkpoint-log", "decision.row.json")).unwrap();
    assert_eq!(actual, expected);
}

/// `checkpoint-log/delta.rows.json` (`design/41-Checkpoint-Efficiency.md`,
/// "Deltas"): rows a store with deltas on wrote, which the CLI decodes
/// through `strata-memory` like any other reader.
#[tokio::test]
async fn the_cli_reads_a_log_stored_as_deltas() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.sqlite");
    drop(cli::store::open(path.to_str().unwrap()).await.unwrap());
    let shared: serde_json::Value =
        serde_json::from_str(&fixture("checkpoint-log", "delta.rows.json")).unwrap();
    let thread_id = shared["thread_id"].as_str().unwrap();
    let rows = shared["rows"].as_array().unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    for row in rows {
        let stored = &row["stored"];
        let text = |key: &str| stored[key].as_str().map(str::to_string);
        let json = |key: &str| (!stored[key].is_null()).then(|| stored[key].to_string());
        conn.execute(
            "INSERT INTO checkpoints
                (thread_id, sequence, schema_version, status, suspended_node, next_node,
                 frozen_state, metadata, created_at, resumed_at, nested)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            rusqlite::params![
                thread_id,
                row["sequence"].as_i64(),
                text("schema_version"),
                text("status"),
                text("suspended_node"),
                text("next_node"),
                json("frozen_state"),
                json("metadata"),
                text("created_at"),
                text("resumed_at"),
                json("nested"),
            ],
        )
        .unwrap();
    }
    drop(conn);

    let strata = |args: &[&str]| {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_strata"))
            .args(args)
            .arg("--json")
            .arg("--storage")
            .arg(&path)
            .output()
            .unwrap();
        assert!(output.status.success(), "{args:?}: {output:?}");
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()
    };
    for row in rows {
        let sequence = row["sequence"].to_string();
        let replayed = strata(&["checkpoint", "replay", thread_id, "--sequence", &sequence]);
        assert_eq!(replayed["state"], row["decoded"]["frozen_state"]);
    }
    let diff = strata(&[
        "checkpoint",
        "diff",
        thread_id,
        "--seq-a",
        "0",
        "--seq-b",
        "2",
    ]);
    let mut changed: Vec<&str> = diff["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|change| change["key"].as_str().unwrap())
        .collect();
    changed.sort_unstable();
    assert_eq!(changed, ["draft", "findings", "step"]);
    let log = strata(&["checkpoint", "log", thread_id]);
    assert_eq!(log.as_array().unwrap().len(), 3);
    assert_eq!(log[2]["status"], "running");
}

/// Inserts a shared fixture's stored rows into the SQLite log at `path`, as a
/// writer would.
fn insert_rows(path: &std::path::Path, thread_id: &str, rows: &[serde_json::Value]) {
    let conn = rusqlite::Connection::open(path).unwrap();
    for row in rows {
        let stored = &row["stored"];
        let text = |key: &str| stored[key].as_str().map(str::to_string);
        let json = |key: &str| (!stored[key].is_null()).then(|| stored[key].to_string());
        conn.execute(
            "INSERT INTO checkpoints
                (thread_id, sequence, schema_version, status, suspended_node, next_node,
                 frozen_state, metadata, created_at, resumed_at, nested)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            rusqlite::params![
                thread_id,
                row["sequence"].as_i64(),
                text("schema_version"),
                text("status"),
                text("suspended_node"),
                text("next_node"),
                json("frozen_state"),
                json("metadata"),
                text("created_at"),
                text("resumed_at"),
                json("nested"),
            ],
        )
        .unwrap();
    }
}

/// `checkpoint-log/offloaded.rows.json` (`design/41-Checkpoint-Efficiency.md`,
/// "Offload"): rows that refer to blobs, read by the CLI with the blobs in the
/// file's own `checkpoint_blobs`, and with them in a filesystem blob store
/// given with `--blobs`; without `--blobs` the second log can't be read.
#[tokio::test]
async fn the_cli_reads_a_log_with_offloaded_values() {
    let shared: serde_json::Value =
        serde_json::from_str(&fixture("checkpoint-log", "offloaded.rows.json")).unwrap();
    let thread_id = shared["thread_id"].as_str().unwrap();
    let rows = shared["rows"].as_array().unwrap();
    let blobs = shared["blobs"].as_object().unwrap();
    let dir = tempfile::tempdir().unwrap();

    let own = dir.path().join("own.sqlite");
    drop(cli::store::open(own.to_str().unwrap()).await.unwrap());
    insert_rows(&own, thread_id, rows);
    let conn = rusqlite::Connection::open(&own).unwrap();
    for (hash, content) in blobs {
        conn.execute(
            "INSERT INTO checkpoint_blobs (thread_id, hash, content) VALUES (?1, ?2, ?3)",
            rusqlite::params![thread_id, hash, content.as_str().unwrap().as_bytes()],
        )
        .unwrap();
    }
    drop(conn);

    let elsewhere = dir.path().join("elsewhere.sqlite");
    drop(cli::store::open(elsewhere.to_str().unwrap()).await.unwrap());
    insert_rows(&elsewhere, thread_id, rows);
    // The filesystem layout: <root>/<thread id as hex>/<hash's hex digest>.
    let thread_dir = dir.path().join("blobs").join(
        thread_id
            .bytes()
            .map(|b| format!("{b:02x}"))
            .collect::<String>(),
    );
    std::fs::create_dir_all(&thread_dir).unwrap();
    for (hash, content) in blobs {
        std::fs::write(
            thread_dir.join(hash.strip_prefix("sha256:").unwrap()),
            content.as_str().unwrap(),
        )
        .unwrap();
    }

    let strata = |path: &std::path::Path, blobs: Option<&std::path::Path>, args: &[&str]| {
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_strata"));
        command.args(args).arg("--json").arg("--storage").arg(path);
        if let Some(blobs) = blobs {
            command.arg("--blobs").arg(blobs);
        }
        command.output().unwrap()
    };
    let read = |path: &std::path::Path, blobs: Option<&std::path::Path>, args: &[&str]| {
        let output = strata(path, blobs, args);
        assert!(output.status.success(), "{args:?}: {output:?}");
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()
    };
    let blob_dir = dir.path().join("blobs");
    for (path, blobs) in [(&own, None), (&elsewhere, Some(blob_dir.as_path()))] {
        for row in rows {
            let sequence = row["sequence"].to_string();
            let replayed = read(
                path,
                blobs,
                &["checkpoint", "replay", thread_id, "--sequence", &sequence],
            );
            assert_eq!(replayed["state"], row["decoded"]["frozen_state"]);
        }
        assert_eq!(
            read(path, blobs, &["checkpoint", "log", thread_id])
                .as_array()
                .unwrap()
                .len(),
            4
        );
        let inspected = read(path, blobs, &["thread", "inspect", thread_id]);
        assert_eq!(inspected["status"], "interrupted");
    }
    let refused = strata(
        &elsewhere,
        None,
        &["checkpoint", "replay", thread_id, "--sequence", "0"],
    );
    assert!(!refused.status.success());
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("which the blob store doesn't have"),
        "{refused:?}"
    );
}
