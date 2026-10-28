//! `PostgresMemoryStore` (`design/45-Long-Term-Memory.md`): the shared cases
//! (`schema/fixtures/memory/search-cases.json`), exact and through an HNSW
//! index, the table both runtimes create
//! (`schema/fixtures/memory/postgres-columns.json`), the dimensions fixed per
//! table, and the `memory` component in `strata_schema_version`. Needs
//! `STRATA_TEST_POSTGRES_URL` on a server with pgvector (CI's
//! `pgvector/pgvector:pg16`); without it each test says `SKIPPED` and passes.
//!
//! pgvector is installed in `public`, once, before any store opens; each
//! store's connections search its sandbox's schema and then `public`, so its
//! table goes in the sandbox and `vector` is found.

mod common;
#[path = "../../strata-core/tests/memory_runner/mod.rs"]
mod memory_runner;

use std::sync::{Arc, Once};

use common::{PgSandbox, backend_url, fixture};
use datalium_strata::memory::{
    Embedding, MemoryKey, MemoryRecord, MemorySearch, MemoryStore, MemoryStoreError,
};
use datalium_strata_memory::memory::MemoryIndex;
use datalium_strata_memory::postgres_memory::{PostgresMemoryOptions, PostgresMemoryStore};
use memory_runner::{Opened, run_cases};
use serde_json::{Value, json};

/// The server's URL, with pgvector installed in `public`, or `None` to skip.
fn server() -> Option<String> {
    let url = backend_url(common::POSTGRES_VAR)?;
    static PGVECTOR: Once = Once::new();
    PGVECTOR.call_once(|| {
        let sandbox = PgSandbox::new(&url, "memory_pgvector");
        sandbox.admin(|client| {
            client
                .batch_execute("CREATE EXTENSION IF NOT EXISTS vector SCHEMA public")
                .unwrap()
        });
    });
    Some(url)
}

async fn open(
    sandbox: &PgSandbox,
    options: PostgresMemoryOptions,
) -> Result<PostgresMemoryStore, MemoryStoreError> {
    PostgresMemoryStore::connect(&sandbox.url_also_searching("public"), options).await
}

/// `information_schema`'s view of a table in the sandbox's schema, in the
/// shape of `memory/postgres-columns.json`: a pgvector column's `data_type` is
/// `USER-DEFINED`, so its type is its `udt_name`.
fn memory_columns(client: &mut postgres::Client, table: &str) -> Value {
    let rows = client
        .query(
            "SELECT c.column_name::text,
                    CASE WHEN c.data_type = 'USER-DEFINED' THEN c.udt_name::text ELSE c.data_type::text END,
                    c.is_nullable::text = 'NO',
                    COALESCE(k.ordinal_position, 0)::int
             FROM information_schema.columns c
             LEFT JOIN (
                 SELECT kcu.column_name, kcu.ordinal_position
                 FROM information_schema.table_constraints tc
                 JOIN information_schema.key_column_usage kcu
                   ON kcu.constraint_name = tc.constraint_name
                  AND kcu.table_schema = tc.table_schema
                  AND kcu.table_name = tc.table_name
                 WHERE tc.constraint_type = 'PRIMARY KEY'
                   AND tc.table_schema = current_schema() AND tc.table_name = $1
             ) k ON k.column_name = c.column_name
             WHERE c.table_schema = current_schema() AND c.table_name = $1
             ORDER BY c.ordinal_position",
            &[&table],
        )
        .unwrap();
    Value::Array(
        rows.iter()
            .map(|row| {
                json!({
                    "name": row.get::<_, String>(0),
                    "type": row.get::<_, String>(1),
                    "notnull": row.get::<_, bool>(2),
                    "pk": row.get::<_, i32>(3),
                })
            })
            .collect(),
    )
}

/// The definitions of the indexes on `table` in the sandbox's schema, by name.
fn indexes(sandbox: &PgSandbox, table: &str) -> Vec<(String, String)> {
    let table = table.to_string();
    sandbox.admin(move |client| {
        client
            .query(
                "SELECT indexname::text, indexdef FROM pg_indexes
                 WHERE schemaname = current_schema() AND tablename = $1 ORDER BY indexname",
                &[&table],
            )
            .unwrap()
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect()
    })
}

fn memory_version(sandbox: &PgSandbox) -> Option<i64> {
    // A refused store leaves no `strata_schema_version` behind: its creation
    // rolls back with the rest.
    if !table_exists(sandbox, "strata_schema_version") {
        return None;
    }
    sandbox.admin(|client| {
        client
            .query_opt(
                "SELECT version FROM strata_schema_version WHERE component = 'memory'",
                &[],
            )
            .unwrap()
            .map(|row| row.get(0))
    })
}

fn table_exists(sandbox: &PgSandbox, table: &str) -> bool {
    let table = table.to_string();
    sandbox.admin(move |client| {
        client
            .query_one("SELECT to_regclass($1) IS NOT NULL", &[&table])
            .unwrap()
            .get(0)
    })
}

async fn the_shared_cases_pass(index: MemoryIndex) {
    let Some(url) = server() else {
        return;
    };
    run_cases(|dimensions, clock| {
        let url = url.clone();
        async move {
            let sandbox = PgSandbox::new(&url, "memory_case");
            let store = Arc::new(
                open(
                    &sandbox,
                    PostgresMemoryOptions::new(dimensions)
                        .with_clock(clock)
                        .with_index(index),
                )
                .await
                .unwrap(),
            );
            Opened {
                store: store.clone(),
                retention: store,
                guard: Some(Box::new(sandbox)),
            }
        }
    })
    .await;
}

#[tokio::test]
async fn the_shared_cases_pass_against_postgres() {
    the_shared_cases_pass(MemoryIndex::None).await;
}

#[tokio::test]
async fn the_shared_cases_pass_against_postgres_with_an_hnsw_index() {
    the_shared_cases_pass(MemoryIndex::hnsw()).await;
}

#[tokio::test]
async fn the_table_is_the_one_both_runtimes_create() {
    let Some(url) = server() else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "memory_layout");
    open(&sandbox, PostgresMemoryOptions::new(4)).await.unwrap();

    let columns = sandbox.admin(|client| memory_columns(client, "memory_records"));
    assert_eq!(columns, fixture("memory/postgres-columns.json"));
    let names: Vec<String> = indexes(&sandbox, "memory_records")
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    assert_eq!(
        names,
        [
            "memory_records_pkey",
            "memory_records_scope",
            "memory_records_updated"
        ]
    );
}

#[tokio::test]
async fn the_memory_component_is_recorded_at_version_1() {
    let Some(url) = server() else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "memory_version");
    open(&sandbox, PostgresMemoryOptions::new(3)).await.unwrap();
    assert_eq!(memory_version(&sandbox), Some(1));
    // Opening it again changes nothing.
    open(&sandbox, PostgresMemoryOptions::new(3)).await.unwrap();
    assert_eq!(memory_version(&sandbox), Some(1));
}

#[tokio::test]
async fn a_table_made_with_other_dimensions_is_refused() {
    let Some(url) = server() else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "memory_dimensions");
    open(&sandbox, PostgresMemoryOptions::new(768))
        .await
        .unwrap();
    let err = open(&sandbox, PostgresMemoryOptions::new(1536))
        .await
        .err()
        .expect("other dimensions");
    assert_eq!(
        err.0,
        "memory was created with 768 dimensions, and this store was opened with 1536"
    );
}

#[tokio::test]
async fn another_table_holds_other_dimensions_in_the_same_database() {
    let Some(url) = server() else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "memory_tables");
    let small = open(&sandbox, PostgresMemoryOptions::new(3)).await.unwrap();
    // The database is already at version 1: the second table is made anyway.
    let large = open(
        &sandbox,
        PostgresMemoryOptions::new(5).with_table("memory_5"),
    )
    .await
    .unwrap();
    assert!(table_exists(&sandbox, "memory_5"));

    small
        .upsert(MemoryRecord::new(
            "n",
            "",
            "a",
            Embedding::new("m", vec![1.0, 0.0, 0.0]),
        ))
        .await
        .unwrap();
    large
        .upsert(MemoryRecord::new(
            "n",
            "",
            "a",
            Embedding::new("m", vec![0.0, 0.0, 0.0, 0.0, 1.0]),
        ))
        .await
        .unwrap();
    let found = large
        .get(&MemoryKey::new("n", "", "a"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.embedding.vector, vec![0.0, 0.0, 0.0, 0.0, 1.0]);
    let names: Vec<String> = indexes(&sandbox, "memory_5")
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    assert_eq!(
        names,
        ["memory_5_pkey", "memory_5_scope", "memory_5_updated"]
    );
}

#[tokio::test]
async fn a_newer_database_is_refused() {
    let Some(url) = server() else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "memory_newer");
    open(&sandbox, PostgresMemoryOptions::new(3)).await.unwrap();
    sandbox.admin(|client| {
        client
            .execute(
                "UPDATE strata_schema_version SET version = 2 WHERE component = 'memory'",
                &[],
            )
            .unwrap()
    });

    let err = open(&sandbox, PostgresMemoryOptions::new(3))
        .await
        .err()
        .expect("a newer database");
    assert_eq!(
        err.0,
        "database is at memory schema version 2, and this build knows 1: upgrade the runtime"
    );
    // Refused unchanged.
    assert_eq!(memory_version(&sandbox), Some(2));
}

#[tokio::test]
async fn a_store_that_does_not_migrate_refuses_a_database_that_is_behind() {
    let Some(url) = server() else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "memory_behind");
    let checking = || PostgresMemoryOptions::new(3).with_auto_migrate(false);

    let err = open(&sandbox, checking()).await.err().expect("behind");
    assert_eq!(
        err.0,
        "database is at memory schema version 0, and this build needs 1: run its migration"
    );
    // It changed nothing.
    assert!(!table_exists(&sandbox, "memory_records"));
    assert_eq!(memory_version(&sandbox), None);

    // A store that migrates brings it up to date; then one that doesn't opens.
    let migrating = open(&sandbox, PostgresMemoryOptions::new(3)).await.unwrap();
    migrating.migrate().await.unwrap();
    let store = open(&sandbox, checking()).await.unwrap();
    store
        .upsert(MemoryRecord::new(
            "n",
            "",
            "a",
            Embedding::new("m", vec![1.0, 0.0, 0.0]),
        ))
        .await
        .unwrap();

    // A table it doesn't have is refused too, though the component is current.
    let err = open(&sandbox, checking().with_table("memory_other"))
        .await
        .err()
        .expect("no table");
    assert_eq!(
        err.0,
        "the memory table 'memory_other' doesn't exist: run its migration"
    );
    assert!(!table_exists(&sandbox, "memory_other"));
}

#[tokio::test]
async fn an_hnsw_index_is_created_when_asked() {
    let Some(url) = server() else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "memory_hnsw");
    open(&sandbox, PostgresMemoryOptions::new(3)).await.unwrap();
    assert!(
        indexes(&sandbox, "memory_records")
            .iter()
            .all(|(_, definition)| !definition.contains("USING hnsw"))
    );

    // Asked for on a table that already exists: created when the store opens.
    let store = open(
        &sandbox,
        PostgresMemoryOptions::new(3).with_index(MemoryIndex::Hnsw {
            m: 8,
            ef_construction: 32,
        }),
    )
    .await
    .unwrap();
    let hnsw: Vec<(String, String)> = indexes(&sandbox, "memory_records")
        .into_iter()
        .filter(|(_, definition)| definition.contains("USING hnsw"))
        .collect();
    assert_eq!(hnsw.len(), 1, "{hnsw:?}");
    let (name, definition) = &hnsw[0];
    assert_eq!(name, "memory_records_embedding_hnsw");
    assert!(definition.contains("vector_cosine_ops"), "{definition}");
    assert!(definition.contains("m='8'"), "{definition}");
    assert!(definition.contains("ef_construction='32'"), "{definition}");

    // A search through it runs (in its transaction with an iterative scan).
    store
        .upsert(MemoryRecord::new(
            "n",
            "",
            "a",
            Embedding::new("m", vec![1.0, 0.0, 0.0]),
        ))
        .await
        .unwrap();
    let matches = store
        .search(&MemorySearch::new(
            "n",
            Embedding::new("m", vec![1.0, 0.0, 0.0]),
        ))
        .await
        .unwrap();
    assert_eq!(matches.len(), 1);
    assert!((matches[0].similarity - 1.0).abs() < 1e-5);
}

#[tokio::test]
async fn a_store_over_a_client_works_like_one_that_connects() {
    let Some(url) = server() else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "memory_client");
    let mut config = datalium_strata_memory::postgres_store::config_from_url(&url).unwrap();
    config.options(format!("-c search_path={},public", sandbox.schema));
    let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let store = PostgresMemoryStore::from_client(client, PostgresMemoryOptions::new(3))
        .await
        .unwrap();
    let record = MemoryRecord::new("n", "acme", "a", Embedding::new("m", vec![0.5, -0.25, 0.1]))
        .with_text("Alert: errors on checkout-api.")
        .with_metadata("service", "checkout-api")
        .with_content(
            json!({ "recovered": true, "steps": [1, 2] })
                .as_object()
                .unwrap()
                .clone(),
        );
    store.upsert(record.clone()).await.unwrap();

    let found = store.get(&record.key()).await.unwrap().unwrap();
    assert_eq!(
        found.embedding, record.embedding,
        "float32 round-trips exactly"
    );
    assert_eq!(found.content, record.content);
    assert_eq!(found.text, record.text);
    assert_eq!(found.metadata, record.metadata);
    assert!(store.delete(&record.key()).await.unwrap());
}
