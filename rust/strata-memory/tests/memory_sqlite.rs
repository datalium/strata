//! `SqliteMemoryStore` (`design/45-Long-Term-Memory.md`, "SQLite and
//! sqlite-vec"): the shared cases (`schema/fixtures/memory/search-cases.json`)
//! on a file and in memory, the tables both runtimes create
//! (`schema/fixtures/memory/sqlite-columns.json`,
//! `sqlite-settings-columns.json`), the dimensions fixed per table, the
//! `memory` component in `strata_schema_version`, and a file shared with the
//! checkpoint store. Needs no server.

mod common;
#[path = "../../strata-core/tests/memory_runner/mod.rs"]
mod memory_runner;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::{fixture, unique_name};
use datalium_strata::memory::{
    Embedding, MemoryKey, MemoryRecord, MemorySearch, MemoryStore, MemoryStoreError,
};
use datalium_strata_memory::checkpoint::SqliteCheckpointStore;
use datalium_strata_memory::sqlite_memory::{SqliteMemoryOptions, SqliteMemoryStore};
use memory_runner::{Opened, run_cases};
use serde_json::{Value, json};

/// A file in the temporary directory, deleted (with SQLite's side files) when
/// dropped.
struct TempFile(PathBuf);

impl TempFile {
    fn new(label: &str) -> Self {
        Self(std::env::temp_dir().join(format!("{}.db", unique_name(label))))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        for suffix in ["", "-journal", "-wal", "-shm"] {
            let mut path = self.0.clone().into_os_string();
            path.push(suffix);
            let _ = std::fs::remove_file(path);
        }
    }
}

fn raw(file: &TempFile) -> rusqlite::Connection {
    rusqlite::Connection::open(file.path()).unwrap()
}

/// `PRAGMA table_info` in the shape of `memory/sqlite-columns.json`.
fn columns(conn: &rusqlite::Connection, table: &str) -> Value {
    let mut statement = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .unwrap();
    let rows = statement
        .query_map([], |row| {
            Ok(json!({
                "name": row.get::<_, String>(1)?,
                "type": row.get::<_, String>(2)?,
                "notnull": row.get::<_, i64>(3)? != 0,
                "pk": row.get::<_, i64>(5)?,
            }))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    Value::Array(rows)
}

fn version(conn: &rusqlite::Connection, component: &str) -> Option<i64> {
    conn.query_row(
        "SELECT version FROM strata_schema_version WHERE component = ?1",
        [component],
        |row| row.get(0),
    )
    .ok()
}

fn indexes(conn: &rusqlite::Connection, table: &str) -> Vec<String> {
    let mut statement = conn
        .prepare(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ?1
             AND name NOT LIKE 'sqlite_autoindex%' ORDER BY name",
        )
        .unwrap();
    statement
        .query_map([table], |row| row.get(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn open(
    file: &TempFile,
    options: SqliteMemoryOptions,
) -> Result<SqliteMemoryStore, MemoryStoreError> {
    SqliteMemoryStore::open(file.path(), options)
}

fn record(id: &str, vector: Vec<f32>) -> MemoryRecord {
    MemoryRecord::new("n", "", id, Embedding::new("m", vector))
}

#[tokio::test]
async fn the_shared_cases_pass_against_a_sqlite_file() {
    run_cases(|dimensions, clock| async move {
        let file = TempFile::new("memory_case");
        let store = Arc::new(
            open(
                &file,
                SqliteMemoryOptions::new(dimensions).with_clock(clock),
            )
            .unwrap(),
        );
        Opened {
            store: store.clone(),
            retention: store,
            guard: Some(Box::new(file)),
        }
    })
    .await;
}

#[tokio::test]
async fn the_shared_cases_pass_against_sqlite_in_memory() {
    run_cases(|dimensions, clock| async move {
        let store = Arc::new(
            SqliteMemoryStore::open_in_memory(
                SqliteMemoryOptions::new(dimensions).with_clock(clock),
            )
            .unwrap(),
        );
        Opened {
            store: store.clone(),
            retention: store,
            guard: None,
        }
    })
    .await;
}

#[tokio::test]
async fn the_tables_are_the_ones_both_runtimes_create() {
    let file = TempFile::new("memory_layout");
    open(&file, SqliteMemoryOptions::new(4)).unwrap();

    let conn = raw(&file);
    assert_eq!(
        columns(&conn, "memory_records"),
        fixture("memory/sqlite-columns.json")
    );
    assert_eq!(
        columns(&conn, "memory_settings"),
        fixture("memory/sqlite-settings-columns.json")
    );
    assert_eq!(
        indexes(&conn, "memory_records"),
        ["memory_records_scope", "memory_records_updated"]
    );
    let dimensions: i64 = conn
        .query_row(
            "SELECT dimensions FROM memory_settings WHERE table_name = 'memory_records'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(dimensions, 4);
}

#[tokio::test]
async fn a_row_holds_the_vector_as_float32_bytes_and_the_rest_as_text() {
    let file = TempFile::new("memory_row");
    let sample = fixture("memory/record.sample.json");
    let mut stamped = sample.clone();
    for field in ["schema_version", "created_at", "updated_at"] {
        stamped.as_object_mut().unwrap().remove(field);
    }
    let at = chrono::DateTime::parse_from_rfc3339(sample["created_at"].as_str().unwrap())
        .unwrap()
        .with_timezone(&chrono::Utc);
    let store = open(
        &file,
        SqliteMemoryOptions::new(4).with_clock(Arc::new(move || at)),
    )
    .unwrap();
    store.upsert(memory_runner::record(&stamped)).await.unwrap();

    let conn = raw(&file);
    let (embedding, content, metadata, created_at): (Vec<u8>, String, String, String) = conn
        .query_row(
            "SELECT embedding, content, metadata, created_at FROM memory_records",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    let hex: String = embedding.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(hex, "0000003e000000bf0000803e0000403f");
    assert_eq!(
        serde_json::from_str::<Value>(&content).unwrap(),
        sample["content"]
    );
    assert_eq!(
        metadata,
        r#"{"scenario":"db-saturation","service":"checkout-api"}"#
    );
    assert_eq!(created_at, "2026-10-21T14:02:11Z");

    let found = store
        .get(&MemoryKey::new(
            "incidents",
            "acme",
            sample["id"].as_str().unwrap(),
        ))
        .await
        .unwrap()
        .unwrap();
    let mut expected = sample.clone();
    expected.as_object_mut().unwrap().remove("schema_version");
    expected["updated_at"] = json!("2026-10-21T14:02:11Z");
    assert_eq!(memory_runner::record_json(&found), expected);
}

#[tokio::test]
async fn the_memory_component_is_recorded_at_version_1() {
    let file = TempFile::new("memory_version");
    open(&file, SqliteMemoryOptions::new(3)).unwrap();
    assert_eq!(version(&raw(&file), "memory"), Some(1));
    // Opening it again changes nothing.
    open(&file, SqliteMemoryOptions::new(3)).unwrap();
    assert_eq!(version(&raw(&file), "memory"), Some(1));
}

#[tokio::test]
async fn a_table_made_with_other_dimensions_is_refused() {
    let file = TempFile::new("memory_dimensions");
    let store = open(&file, SqliteMemoryOptions::new(768)).unwrap();
    drop(store);
    let err = open(&file, SqliteMemoryOptions::new(1536))
        .err()
        .expect("other dimensions");
    assert_eq!(
        err.0,
        "memory was created with 768 dimensions, and this store was opened with 1536"
    );
}

#[tokio::test]
async fn another_table_holds_other_dimensions_in_the_same_file() {
    let file = TempFile::new("memory_tables");
    let small = open(&file, SqliteMemoryOptions::new(3)).unwrap();
    let large = open(&file, SqliteMemoryOptions::new(5).with_table("memory_5")).unwrap();

    small
        .upsert(record("a", vec![1.0, 0.0, 0.0]))
        .await
        .unwrap();
    large
        .upsert(record("a", vec![0.0, 0.0, 0.0, 0.0, 1.0]))
        .await
        .unwrap();
    let found = large
        .get(&MemoryKey::new("n", "", "a"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.embedding.vector, vec![0.0, 0.0, 0.0, 0.0, 1.0]);

    let conn = raw(&file);
    assert_eq!(
        indexes(&conn, "memory_5"),
        ["memory_5_scope", "memory_5_updated"]
    );
    let settings: Vec<(String, i64)> = conn
        .prepare("SELECT table_name, dimensions FROM memory_settings ORDER BY table_name")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        settings,
        [
            ("memory_5".to_string(), 5),
            ("memory_records".to_string(), 3)
        ]
    );
}

#[tokio::test]
async fn a_newer_file_is_refused_unchanged() {
    let file = TempFile::new("memory_newer");
    open(&file, SqliteMemoryOptions::new(3)).unwrap();
    raw(&file)
        .execute(
            "UPDATE strata_schema_version SET version = 2 WHERE component = 'memory'",
            [],
        )
        .unwrap();

    let err = open(&file, SqliteMemoryOptions::new(3))
        .err()
        .expect("a newer file");
    assert_eq!(
        err.0,
        "database is at memory schema version 2, and this build knows 1: upgrade the runtime"
    );
    assert_eq!(version(&raw(&file), "memory"), Some(2));
}

#[tokio::test]
async fn a_refused_open_leaves_nothing_behind() {
    let file = TempFile::new("memory_refused");
    open(&file, SqliteMemoryOptions::new(3)).unwrap();
    // A newer file is refused in the transaction that would make the table,
    // so a store on another table makes nothing.
    raw(&file)
        .execute(
            "UPDATE strata_schema_version SET version = 2 WHERE component = 'memory'",
            [],
        )
        .unwrap();
    assert!(
        open(
            &file,
            SqliteMemoryOptions::new(3).with_table("memory_other")
        )
        .is_err()
    );
    let conn = raw(&file);
    let made: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'memory_other'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(made, 0);
}

#[tokio::test]
async fn a_file_is_shared_with_the_checkpoint_store() {
    let file = TempFile::new("memory_shared");
    let checkpoints = SqliteCheckpointStore::open(file.path()).unwrap();
    let memory = open(&file, SqliteMemoryOptions::new(3)).unwrap();
    // And the other way round: a checkpoint store opens a file memory made.
    drop(checkpoints);
    SqliteCheckpointStore::open(file.path()).unwrap();

    let conn = raw(&file);
    assert_eq!(version(&conn, "memory"), Some(1));
    assert_eq!(
        version(&conn, "checkpoints"),
        Some(datalium_strata_memory::schema_versions::CHECKPOINTS_VERSION)
    );
    memory
        .upsert(record("a", vec![1.0, 0.0, 0.0]))
        .await
        .unwrap();
    let matches = memory
        .search(&MemorySearch::new(
            "n",
            Embedding::new("m", vec![1.0, 0.0, 0.0]),
        ))
        .await
        .unwrap();
    assert_eq!(matches.len(), 1);
    assert!((matches[0].similarity - 1.0).abs() < 1e-6);
}

#[tokio::test]
async fn a_metadata_key_with_quotes_and_dots_is_matched_exactly() {
    let store = SqliteMemoryStore::open_in_memory(SqliteMemoryOptions::new(2)).unwrap();
    store
        .upsert(record("a", vec![1.0, 0.0]).with_metadata("a.b\"c", "x"))
        .await
        .unwrap();
    store
        .upsert(record("b", vec![1.0, 0.0]).with_metadata("a", "x"))
        .await
        .unwrap();
    let ids = |matches: Vec<datalium_strata::memory::MemoryMatch>| -> Vec<String> {
        matches.into_iter().map(|m| m.record.id).collect()
    };
    let search = MemorySearch::new("n", Embedding::new("m", vec![1.0, 0.0]));
    assert_eq!(
        ids(store
            .search(&search.clone().with_metadata("a.b\"c", "x"))
            .await
            .unwrap()),
        ["a"]
    );
    assert_eq!(
        ids(store.search(&search.with_metadata("a", "x")).await.unwrap()),
        ["b"]
    );
}

#[tokio::test]
async fn bad_options_are_refused_before_the_file_is_made() {
    let file = TempFile::new("memory_options");
    let err = open(&file, SqliteMemoryOptions::new(3).with_table("Memory"))
        .err()
        .expect("a bad table");
    assert_eq!(
        err.0,
        "the memory table 'Memory' isn't a lowercase SQL name"
    );
    assert!(!file.path().exists());
}
