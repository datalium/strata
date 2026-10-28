//! Postgres-specific behaviour of `PostgresCheckpointStore` /
//! `PostgresHistoryStore` — everything the shared contract suites
//! (`checkpoint_store_contract.rs`, `history_store_contract.rs`) can't say
//! because it isn't true of every backend: the on-disk layout the two runtimes
//! share (the log and its trigger-maintained head table), migrations, and
//! safety under concurrent writers. Needs `STRATA_TEST_POSTGRES_URL` (see
//! `tests/common`) except for the unreachable-server test.

mod common;

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use common::{PgSandbox, backend_url, fixture};
use datalium_strata::checkpoint::{CheckpointQuery, CheckpointStore, ThreadQuery, ThreadState};
use datalium_strata::history::{ChatMessage, HistoryStore, MessageRole};
use datalium_strata::schema::Checkpoint;
use datalium_strata_memory::postgres_store::{PostgresCheckpointStore, PostgresHistoryStore};
use serde_json::{Value, json};

fn frozen_fixture() -> Checkpoint {
    serde_json::from_value(fixture("checkpoint-lifecycle/frozen.json")).unwrap()
}

/// `information_schema` view of a table in the sandbox's schema, in the shape
/// of `checkpoint-log/postgres-columns.json`.
fn table_columns(client: &mut postgres::Client, table: &str) -> Value {
    let rows = client
        .query(
            "SELECT c.column_name::text, c.data_type::text, c.is_nullable::text = 'NO',
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

fn raw_checkpoint_row(client: &mut postgres::Client, thread_id: &str, sequence: i64) -> Value {
    let row = client
        .query_one(
            "SELECT thread_id, sequence, schema_version, status, suspended_node, next_node,
                    frozen_state, metadata, created_at, resumed_at, nested, decision, wait, wake_at,
                    graph_id, graph_version
             FROM checkpoints WHERE thread_id = $1 AND sequence = $2",
            &[&thread_id, &sequence],
        )
        .unwrap();
    json!({
        "thread_id": row.get::<_, String>(0),
        "sequence": row.get::<_, i64>(1),
        "schema_version": row.get::<_, String>(2),
        "status": row.get::<_, String>(3),
        "suspended_node": row.get::<_, Option<String>>(4),
        "next_node": row.get::<_, Option<String>>(5),
        "frozen_state": row.get::<_, Value>(6),
        "metadata": row.get::<_, Option<Value>>(7),
        "created_at": row.get::<_, String>(8),
        "resumed_at": row.get::<_, Option<String>>(9),
        "nested": row.get::<_, Option<Value>>(10),
        "decision": row.get::<_, Option<Value>>(11),
        "wait": row.get::<_, Option<Value>>(12),
        "wake_at": row.get::<_, Option<String>>(13),
        "graph_id": row.get::<_, Option<String>>(14),
        "graph_version": row.get::<_, Option<String>>(15),
    })
}

/// Inserts a row in the shape of `checkpoint-log/*.row.json` as raw SQL — what
/// any other writer of the shared log does.
fn insert_raw_row(client: &mut postgres::Client, row: &Value) {
    client
        .execute(
            "INSERT INTO checkpoints
                (thread_id, sequence, schema_version, status, suspended_node, next_node,
                 frozen_state, metadata, created_at, resumed_at, nested, decision, wait, wake_at,
                 graph_id, graph_version)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)",
            &[
                &row["thread_id"].as_str(),
                &row["sequence"].as_i64(),
                &row["schema_version"].as_str(),
                &row["status"].as_str(),
                &row["suspended_node"].as_str(),
                &row["next_node"].as_str(),
                &row["frozen_state"],
                &(!row["metadata"].is_null()).then(|| row["metadata"].clone()),
                &row["created_at"].as_str(),
                &row["resumed_at"].as_str(),
                &(!row["nested"].is_null()).then(|| row["nested"].clone()),
                &(!row["decision"].is_null()).then(|| row["decision"].clone()),
                &(!row["wait"].is_null()).then(|| row["wait"].clone()),
                &row["wake_at"].as_str(),
                &row["graph_id"].as_str(),
                &row["graph_version"].as_str(),
            ],
        )
        .unwrap();
}

/// `checkpoint_threads`, in the shape of `checkpoint-log/threads.rows.json`.
fn head_rows(client: &mut postgres::Client) -> Value {
    let rows = client
        .query(
            "SELECT thread_id, sequence, state, updated_at, metadata, wake_at, graph_id, graph_version
             FROM checkpoint_threads ORDER BY thread_id",
            &[],
        )
        .unwrap();
    Value::Array(
        rows.iter()
            .map(|row| {
                json!({
                    "thread_id": row.get::<_, String>(0),
                    "sequence": row.get::<_, i64>(1),
                    "state": row.get::<_, String>(2),
                    "updated_at": row.get::<_, String>(3),
                    "metadata": row.get::<_, Option<Value>>(4),
                    "wake_at": row.get::<_, Option<String>>(5),
                    "graph_id": row.get::<_, Option<String>>(6),
                    "graph_version": row.get::<_, Option<String>>(7),
                })
            })
            .collect(),
    )
}

fn message(content: String, seconds_offset: i64) -> ChatMessage {
    ChatMessage {
        role: MessageRole::User,
        content: Arc::from(content.as_str()),
        timestamp: Utc
            .timestamp_opt(1_700_000_000 + seconds_offset, 0)
            .unwrap(),
    }
}

async fn store(sandbox: &PgSandbox) -> PostgresCheckpointStore {
    PostgresCheckpointStore::from_client(sandbox.client().await)
        .await
        .unwrap()
}

#[tokio::test]
async fn an_unreachable_server_is_an_error_not_a_hang() {
    // Port 1 on loopback refuses immediately; the point is the error mapping
    // (and that a bad URL doesn't panic), so this needs no server.
    let started = std::time::Instant::now();
    let err = PostgresCheckpointStore::connect("postgres://strata:strata@127.0.0.1:1/strata")
        .await
        .err()
        .expect("nothing listens on port 1");
    assert!(!err.0.is_empty());
    assert!(started.elapsed().as_secs() < 30);

    assert!(PostgresHistoryStore::connect("not a url").await.is_err());
}

#[test]
fn the_store_works_on_a_current_thread_runtime() {
    // Under design/17 the sync `postgres` crate panicked if called on a runtime
    // thread, so the engine hopped to the blocking pool. The async store is
    // called right on the executor, here a single-threaded one.
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "current_thread");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let store = store(&sandbox).await;
            store.save(frozen_fixture()).await.unwrap();
            assert_eq!(
                store.load(&frozen_fixture().thread_id).await.unwrap(),
                Some(frozen_fixture())
            );
        });
}

#[tokio::test]
async fn the_tables_match_the_shared_column_fixtures() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "layout");
    drop(store(&sandbox).await);

    sandbox.admin(|client| {
        assert_eq!(
            table_columns(client, "checkpoints"),
            fixture("checkpoint-log/postgres-columns.json")
        );
        assert_eq!(
            table_columns(client, "checkpoint_threads"),
            fixture("checkpoint-log/postgres-threads-columns.json")
        );
    });
}

#[tokio::test]
async fn the_trigger_keeps_the_head_table_current_whoever_writes_the_log() {
    // Raw inserts, not `save`: the head row must come from the database itself,
    // so an older runtime or any other writer keeps it right.
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "trigger");
    let store = store(&sandbox).await;
    let rows = fixture("checkpoint-log/threads.rows.json");

    sandbox.admin(|client| {
        for row in rows["inserts"].as_array().unwrap() {
            insert_raw_row(client, row);
        }
        assert_eq!(head_rows(client), rows["threads"]);
    });

    // And a listing reads what the trigger wrote.
    let page = store
        .list_threads(&ThreadQuery::default().with_states([ThreadState::Resumed]))
        .await
        .unwrap();
    let ids: Vec<&str> = page.threads.iter().map(|t| t.thread_id.as_str()).collect();
    assert_eq!(ids, ["t-woken", "t-resumed"]);

    // The trigger copied wake_at, and only the waiting one is due
    // (design/29-Event-Waits-And-Timers.md).
    let due = store
        .list_threads(&ThreadQuery::default().due_by("2026-09-16T08:00:00Z".parse().unwrap()))
        .await
        .unwrap();
    let ids: Vec<&str> = due.threads.iter().map(|t| t.thread_id.as_str()).collect();
    assert_eq!(ids, ["t-waiting"]);
}

#[tokio::test]
async fn a_log_from_before_the_head_table_is_backfilled_once_when_opened() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "backfill");
    let rows = fixture("checkpoint-log/threads.rows.json");
    // A log as the previous version left it: no head table, no trigger.
    sandbox.admin(|client| {
        client
            .batch_execute(datalium_strata_memory::postgres_store::POSTGRES_CHECKPOINT_LOG_DDL)
            .unwrap();
        for row in rows["inserts"].as_array().unwrap() {
            insert_raw_row(client, row);
        }
    });

    // Several stores open it at once, as several workers starting would.
    let clients = futures_all((0..8).map(|_| sandbox.client()).collect::<Vec<_>>()).await;
    let opens = clients
        .into_iter()
        .map(|client| tokio::spawn(PostgresCheckpointStore::from_client(client)))
        .collect::<Vec<_>>();
    let mut stores = Vec::new();
    for open in opens {
        stores.push(open.await.unwrap().unwrap());
    }

    sandbox.admin(|client| assert_eq!(head_rows(client), rows["threads"]));

    // From now on the trigger keeps it current.
    let mut next = frozen_fixture();
    next.thread_id = "t-late".to_string();
    next.created_at = "2026-09-17T00:00:00Z".to_string();
    stores[0].save(next).await.unwrap();
    sandbox.admin(|client| {
        let late: (i64, String) = {
            let row = client
                .query_one(
                    "SELECT sequence, state FROM checkpoint_threads WHERE thread_id = 't-late'",
                    &[],
                )
                .unwrap();
            (row.get(0), row.get(1))
        };
        assert_eq!(late, (2, "interrupted".to_string()));
    });
}

/// Awaits every future in order.
async fn futures_all<F: std::future::Future>(futures: Vec<F>) -> Vec<F::Output> {
    let mut outputs = Vec::with_capacity(futures.len());
    for future in futures {
        outputs.push(future.await);
    }
    outputs
}

#[tokio::test]
async fn a_checkpoints_table_from_before_nested_checkpoints_is_upgraded_in_place() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "pre_nested");
    // The table as it was before nested checkpoints: no `nested` column.
    sandbox.admin(|client| {
        client
            .batch_execute(
                "CREATE TABLE checkpoints (
                    thread_id      TEXT NOT NULL,
                    sequence       BIGINT NOT NULL,
                    schema_version TEXT NOT NULL,
                    status         TEXT NOT NULL,
                    suspended_node TEXT,
                    next_node      TEXT,
                    frozen_state   JSONB NOT NULL,
                    metadata       JSONB,
                    created_at     TEXT NOT NULL,
                    resumed_at     TEXT,
                    PRIMARY KEY (thread_id, sequence)
                );
                INSERT INTO checkpoints VALUES
                    ('old', 0, '1.0.0', 'interrupted', 'A', 'B', '{\"n\": 1}', NULL,
                     '2026-09-16T00:00:00Z', NULL)",
            )
            .unwrap();
    });

    let store = store(&sandbox).await;

    // The old row reads back as it was, with no child.
    let old = store.load("old").await.unwrap().unwrap();
    assert_eq!(old.frozen_state, json!({ "n": 1 }));
    assert_eq!(old.nested, None);

    // A checkpoint with a child saves and loads on the upgraded table.
    let mut nested = old.clone();
    nested.thread_id = "new".to_string();
    nested.schema_version = "1.1.0".to_string();
    nested.nested = Some(Box::new(old));
    store.save(nested.clone()).await.unwrap();
    assert_eq!(store.load("new").await.unwrap(), Some(nested));

    // The upgrade left exactly the shared layout, and opening again is a no-op.
    drop(self::store(&sandbox).await);
    sandbox.admin(|client| {
        assert_eq!(
            table_columns(client, "checkpoints"),
            fixture("checkpoint-log/postgres-columns.json")
        );
    });
}

#[tokio::test]
async fn the_history_table_matches_the_shared_column_fixture() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "history_layout");
    drop(
        PostgresHistoryStore::from_client(sandbox.client().await)
            .await
            .unwrap(),
    );

    sandbox.admin(|client| {
        assert_eq!(
            table_columns(client, "history_records"),
            fixture("history-log/postgres-columns.json")
        );
    });
}

#[tokio::test]
async fn the_row_for_the_shared_fixture_matches_the_shared_row_fixture() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "row");
    let store = store(&sandbox).await;
    let frozen = frozen_fixture();
    store.save(frozen.clone()).await.unwrap();

    // The same row fixture the SQLite stores and the CLI are held to.
    sandbox.admin(|client| {
        assert_eq!(
            raw_checkpoint_row(client, &frozen.thread_id, 0),
            fixture("checkpoint-log/frozen.row.json")
        );
    });
}

#[tokio::test]
async fn the_row_for_a_cancelled_checkpoint_matches_the_shared_row_fixture() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "cancelled_row");
    let store = store(&sandbox).await;
    let cancelled: Checkpoint =
        serde_json::from_value(fixture("checkpoint-lifecycle/cancelled.json")).unwrap();
    store.save(cancelled.clone()).await.unwrap();

    sandbox.admin(|client| {
        assert_eq!(
            raw_checkpoint_row(client, &cancelled.thread_id, 0),
            fixture("checkpoint-log/cancelled.row.json")
        );
    });
    assert_eq!(
        store.load(&cancelled.thread_id).await.unwrap(),
        Some(cancelled)
    );
}

#[tokio::test]
async fn the_row_for_a_running_checkpoint_matches_the_shared_row_fixture() {
    // design/33-Durable-Execution.md: a status, and nothing new in the layout.
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "running_row");
    let store = store(&sandbox).await;
    let running: Checkpoint =
        serde_json::from_value(fixture("checkpoint-lifecycle/running.json")).unwrap();
    store.save(running.clone()).await.unwrap();

    sandbox.admin(|client| {
        assert_eq!(
            raw_checkpoint_row(client, &running.thread_id, 0),
            fixture("checkpoint-log/running.row.json")
        );
    });
    assert_eq!(store.load(&running.thread_id).await.unwrap(), Some(running));
}

#[tokio::test]
async fn the_row_for_an_ended_checkpoint_matches_the_shared_row_fixture() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "ended_row");
    let store = store(&sandbox).await;
    let ended: Checkpoint =
        serde_json::from_value(fixture("checkpoint-lifecycle/ended.json")).unwrap();
    store.save(ended.clone()).await.unwrap();

    sandbox.admin(|client| {
        assert_eq!(
            raw_checkpoint_row(client, &ended.thread_id, 0),
            fixture("checkpoint-log/ended.row.json")
        );
    });
    assert_eq!(store.load(&ended.thread_id).await.unwrap(), Some(ended));
}

#[tokio::test]
async fn the_row_for_a_nested_checkpoint_holds_the_child_as_jsonb_in_the_nested_column() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "nested_row");
    let store = store(&sandbox).await;
    let nested: Checkpoint =
        serde_json::from_value(fixture("checkpoint-lifecycle/nested.json")).unwrap();
    store.save(nested.clone()).await.unwrap();

    sandbox.admin(|client| {
        assert_eq!(
            raw_checkpoint_row(client, &nested.thread_id, 0),
            fixture("checkpoint-log/nested.row.json")
        );
    });
    assert_eq!(store.load(&nested.thread_id).await.unwrap(), Some(nested));
}

#[tokio::test]
async fn the_row_for_a_checkpoint_with_a_decision_holds_it_as_jsonb_in_the_decision_column() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "decision_row");
    let store = store(&sandbox).await;
    let decided: Checkpoint =
        serde_json::from_value(fixture("checkpoint-lifecycle/decision-failed.json")).unwrap();
    store.save(decided.clone()).await.unwrap();

    sandbox.admin(|client| {
        assert_eq!(
            raw_checkpoint_row(client, &decided.thread_id, 0),
            fixture("checkpoint-log/decision.row.json")
        );
    });
    assert_eq!(store.load(&decided.thread_id).await.unwrap(), Some(decided));
}

#[tokio::test]
async fn the_row_for_a_waiting_checkpoint_matches_the_shared_row_fixture() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "waiting_row");
    let store = store(&sandbox).await;
    let waiting: Checkpoint =
        serde_json::from_value(fixture("checkpoint-lifecycle/waiting-event.json")).unwrap();
    store.save(waiting.clone()).await.unwrap();
    let nested: Checkpoint =
        serde_json::from_value(fixture("checkpoint-lifecycle/nested-timer.json")).unwrap();
    store.save(nested.clone()).await.unwrap();

    sandbox.admin(|client| {
        assert_eq!(
            raw_checkpoint_row(client, &waiting.thread_id, 0),
            fixture("checkpoint-log/waiting.row.json")
        );
        // A nested wait's derived column is the innermost level's.
        let row = raw_checkpoint_row(client, &nested.thread_id, 0);
        assert_eq!(row["wait"], Value::Null);
        assert_eq!(row["wake_at"], "2026-10-06T12:05:00Z");
    });
    assert_eq!(store.load(&waiting.thread_id).await.unwrap(), Some(waiting));
    assert_eq!(store.load(&nested.thread_id).await.unwrap(), Some(nested));
}

#[tokio::test]
async fn a_head_table_from_before_waits_is_upgraded_in_place() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "pre_wait");
    // The tables and trigger as they were before waits: no `wait` or `wake_at`.
    sandbox.admin(|client| {
        client
            .batch_execute(
                r#"CREATE TABLE checkpoints (
                    thread_id TEXT NOT NULL, sequence BIGINT NOT NULL, schema_version TEXT NOT NULL,
                    status TEXT NOT NULL, suspended_node TEXT, next_node TEXT,
                    frozen_state JSONB NOT NULL, metadata JSONB, created_at TEXT NOT NULL,
                    resumed_at TEXT, nested JSONB, decision JSONB,
                    PRIMARY KEY (thread_id, sequence)
                );
                CREATE TABLE checkpoint_threads (
                    thread_id TEXT COLLATE "C" NOT NULL PRIMARY KEY, sequence BIGINT NOT NULL,
                    state TEXT NOT NULL, updated_at TEXT COLLATE "C" NOT NULL, metadata JSONB
                );
                CREATE OR REPLACE FUNCTION strata_checkpoints_head() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    INSERT INTO checkpoint_threads AS h (thread_id, sequence, state, updated_at, metadata)
                    VALUES (NEW.thread_id, NEW.sequence,
                            CASE WHEN NEW.resumed_at IS NOT NULL THEN 'resumed' ELSE NEW.status END,
                            COALESCE(NEW.resumed_at, NEW.created_at), NEW.metadata)
                    ON CONFLICT (thread_id) DO UPDATE SET
                        sequence = EXCLUDED.sequence, state = EXCLUDED.state,
                        updated_at = EXCLUDED.updated_at, metadata = EXCLUDED.metadata
                    WHERE EXCLUDED.sequence > h.sequence;
                    RETURN NULL;
                END;
                $$;
                CREATE TRIGGER checkpoints_head AFTER INSERT ON checkpoints
                    FOR EACH ROW EXECUTE FUNCTION strata_checkpoints_head();
                INSERT INTO checkpoints (thread_id, sequence, schema_version, status, suspended_node,
                    next_node, frozen_state, created_at)
                VALUES ('old', 0, '1.0.0', 'interrupted', 'Approve', 'Execute', '{}', '2026-09-16T00:00:00Z');"#,
            )
            .unwrap();
    });

    let store = store(&sandbox).await;
    drop(self::store(&sandbox).await);
    sandbox.admin(|client| {
        assert_eq!(
            table_columns(client, "checkpoints"),
            fixture("checkpoint-log/postgres-columns.json")
        );
        assert_eq!(
            table_columns(client, "checkpoint_threads"),
            fixture("checkpoint-log/postgres-threads-columns.json")
        );
    });

    // The new trigger copies wake_at: a waiting thread is due, the old one is still listed.
    let nested: Checkpoint =
        serde_json::from_value(fixture("checkpoint-lifecycle/nested-timer.json")).unwrap();
    store.save(nested).await.unwrap();
    let due = store
        .list_threads(&ThreadQuery::default().due_by("2026-10-06T12:05:00Z".parse().unwrap()))
        .await
        .unwrap();
    let ids: Vec<&str> = due.threads.iter().map(|t| t.thread_id.as_str()).collect();
    assert_eq!(ids, ["thread_verify_demo"]);
    assert_eq!(
        store
            .list_threads(&ThreadQuery::default())
            .await
            .unwrap()
            .threads
            .len(),
        2
    );
}

#[tokio::test]
async fn the_row_for_an_identified_checkpoint_matches_the_shared_row_fixture() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "identified_row");
    let store = store(&sandbox).await;
    let identified: Checkpoint =
        serde_json::from_value(fixture("checkpoint-lifecycle/identified.json")).unwrap();
    store.save(identified.clone()).await.unwrap();
    let nested: Checkpoint =
        serde_json::from_value(fixture("checkpoint-lifecycle/nested-identified.json")).unwrap();
    store.save(nested.clone()).await.unwrap();

    sandbox.admin(|client| {
        assert_eq!(
            raw_checkpoint_row(client, &identified.thread_id, 0),
            fixture("checkpoint-log/identified.row.json")
        );
        // The columns hold the outermost level's identity
        // (design/31-Graph-Identity.md).
        assert_eq!(
            raw_checkpoint_row(client, &nested.thread_id, 0)["graph_id"],
            "incident"
        );
    });
    assert_eq!(
        store.load(&identified.thread_id).await.unwrap(),
        Some(identified.clone())
    );
    assert_eq!(store.load(&nested.thread_id).await.unwrap(), Some(nested));
    let listed = store
        .list_threads(
            &ThreadQuery::default()
                .with_graph("wire-transfer")
                .with_graph_version("2"),
        )
        .await
        .unwrap();
    let ids: Vec<&str> = listed
        .threads
        .iter()
        .map(|t| t.thread_id.as_str())
        .collect();
    assert_eq!(ids, [identified.thread_id.as_str()]);
}

#[tokio::test]
async fn a_head_table_from_before_graph_identities_is_upgraded_in_place() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "pre_identity");
    // The tables and trigger as they were before graph identities: no
    // `graph_id` or `graph_version`.
    sandbox.admin(|client| {
        client
            .batch_execute(
                r#"CREATE TABLE checkpoints (
                    thread_id TEXT NOT NULL, sequence BIGINT NOT NULL, schema_version TEXT NOT NULL,
                    status TEXT NOT NULL, suspended_node TEXT, next_node TEXT,
                    frozen_state JSONB NOT NULL, metadata JSONB, created_at TEXT NOT NULL,
                    resumed_at TEXT, nested JSONB, decision JSONB, wait JSONB, wake_at TEXT,
                    PRIMARY KEY (thread_id, sequence)
                );
                CREATE TABLE checkpoint_threads (
                    thread_id TEXT COLLATE "C" NOT NULL PRIMARY KEY, sequence BIGINT NOT NULL,
                    state TEXT NOT NULL, updated_at TEXT COLLATE "C" NOT NULL, metadata JSONB,
                    wake_at TEXT COLLATE "C"
                );
                CREATE OR REPLACE FUNCTION strata_checkpoints_head() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    INSERT INTO checkpoint_threads AS h (thread_id, sequence, state, updated_at, metadata, wake_at)
                    VALUES (NEW.thread_id, NEW.sequence,
                            CASE WHEN NEW.resumed_at IS NOT NULL THEN 'resumed' ELSE NEW.status END,
                            COALESCE(NEW.resumed_at, NEW.created_at), NEW.metadata, NEW.wake_at)
                    ON CONFLICT (thread_id) DO UPDATE SET
                        sequence = EXCLUDED.sequence, state = EXCLUDED.state,
                        updated_at = EXCLUDED.updated_at, metadata = EXCLUDED.metadata,
                        wake_at = EXCLUDED.wake_at
                    WHERE EXCLUDED.sequence > h.sequence;
                    RETURN NULL;
                END;
                $$;
                CREATE TRIGGER checkpoints_head AFTER INSERT ON checkpoints
                    FOR EACH ROW EXECUTE FUNCTION strata_checkpoints_head();
                INSERT INTO checkpoints (thread_id, sequence, schema_version, status, suspended_node,
                    next_node, frozen_state, created_at)
                VALUES ('old', 0, '1.0.0', 'interrupted', 'Approve', 'Execute', '{}', '2026-09-16T00:00:00Z');"#,
            )
            .unwrap();
    });

    let store = store(&sandbox).await;
    drop(self::store(&sandbox).await);
    sandbox.admin(|client| {
        assert_eq!(
            table_columns(client, "checkpoints"),
            fixture("checkpoint-log/postgres-columns.json")
        );
        assert_eq!(
            table_columns(client, "checkpoint_threads"),
            fixture("checkpoint-log/postgres-threads-columns.json")
        );
    });

    // The new trigger copies the identity: the identified thread is listed by
    // its graph, the old one is still listed.
    let identified: Checkpoint =
        serde_json::from_value(fixture("checkpoint-lifecycle/identified.json")).unwrap();
    store.save(identified).await.unwrap();
    let by_graph = store
        .list_threads(&ThreadQuery::default().with_graph("wire-transfer"))
        .await
        .unwrap();
    let ids: Vec<&str> = by_graph
        .threads
        .iter()
        .map(|t| t.thread_id.as_str())
        .collect();
    assert_eq!(ids, ["thread_hitl_demo"]);
    assert_eq!(
        store
            .list_threads(&ThreadQuery::default())
            .await
            .unwrap()
            .threads
            .len(),
        2
    );
}

#[tokio::test]
async fn a_row_written_as_raw_sql_is_loadable() {
    // The reader side of the contract: a row laid out exactly as the fixture
    // says — which is what the .NET store writes — is readable through `load`.
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "reader");
    let store = store(&sandbox).await;
    let row = fixture("checkpoint-log/frozen.row.json");
    sandbox.admin(|client| insert_raw_row(client, &row));

    let frozen = frozen_fixture();
    assert_eq!(store.load(&frozen.thread_id).await.unwrap(), Some(frozen));
}

fn frozen_fixture_for(thread_id: &str) -> Checkpoint {
    Checkpoint {
        thread_id: thread_id.to_string(),
        ..frozen_fixture()
    }
}

#[tokio::test]
async fn save_appends_to_the_log_and_load_returns_the_latest() {
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "log");
    let store = store(&sandbox).await;
    let first = frozen_fixture();
    let mut resumed = first.clone();
    resumed.resumed_at = Some("2026-09-16T01:00:00Z".to_string());

    store.save(first.clone()).await.unwrap();
    store.save(resumed.clone()).await.unwrap();
    store
        .save(frozen_fixture_for("other-thread"))
        .await
        .unwrap();

    assert_eq!(store.load(&first.thread_id).await.unwrap(), Some(resumed));
    sandbox.admin(|client| {
        let sequences: Vec<i64> = client
            .query(
                "SELECT sequence FROM checkpoints WHERE thread_id = $1 ORDER BY sequence",
                &[&first.thread_id],
            )
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        assert_eq!(sequences, [0, 1], "the earlier checkpoint stays as history");
        // Sequences are per thread.
        assert_eq!(
            raw_checkpoint_row(client, "other-thread", 0)["sequence"],
            json!(0)
        );
    });
}

#[tokio::test]
async fn a_checkpoint_survives_independent_store_instances() {
    // The point of a database over `InMemoryCheckpointStore`: what one
    // connection saved, a brand-new one (a restarted process) reads back.
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "persist");
    let frozen = frozen_fixture();
    {
        let store = store(&sandbox).await;
        store.save(frozen.clone()).await.unwrap();
    }

    let reopened = store(&sandbox).await;
    assert_eq!(
        reopened.load(&frozen.thread_id).await.unwrap(),
        Some(frozen)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_writers_to_one_thread_never_collide_or_lose_a_checkpoint() {
    // The claim SQLite couldn't make ("single writer assumed"): eight
    // connections, each its own store, hammer the *same* thread. Without the
    // per-thread advisory lock two of them pick the same `sequence` and one
    // fails on the primary key.
    const WRITERS: usize = 8;
    const SAVES_EACH: usize = 25;
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "concurrent_checkpoints");
    let mut stores = Vec::new();
    for _ in 0..WRITERS {
        stores.push(Arc::new(store(&sandbox).await));
    }
    let barrier = Arc::new(tokio::sync::Barrier::new(WRITERS));

    let writers: Vec<_> = stores
        .iter()
        .enumerate()
        .map(|(writer, store)| {
            let (store, barrier) = (store.clone(), barrier.clone());
            tokio::spawn(async move {
                barrier.wait().await;
                for save in 0..SAVES_EACH {
                    let mut checkpoint = frozen_fixture_for("contended");
                    checkpoint.created_at = format!("writer-{writer}-save-{save}");
                    store.save(checkpoint).await.unwrap();
                }
            })
        })
        .collect();
    for writer in writers {
        writer.await.unwrap();
    }

    sandbox.admin(|client| {
        let sequences: Vec<i64> = client
            .query(
                "SELECT sequence FROM checkpoints WHERE thread_id = 'contended' ORDER BY sequence",
                &[],
            )
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        let expected: Vec<i64> = (0..(WRITERS * SAVES_EACH) as i64).collect();
        assert_eq!(
            sequences, expected,
            "every save landed, at a unique sequence"
        );

        // Each writer's own saves are still in the order it made them.
        for writer in 0..WRITERS {
            let saves: Vec<usize> = client
                .query(
                    "SELECT created_at FROM checkpoints WHERE thread_id = 'contended'
                     AND created_at LIKE $1 ORDER BY sequence",
                    &[&format!("writer-{writer}-save-%")],
                )
                .unwrap()
                .iter()
                .map(|r| {
                    let created_at: String = r.get(0);
                    created_at.rsplit('-').next().unwrap().parse().unwrap()
                })
                .collect();
            assert_eq!(saves, (0..SAVES_EACH).collect::<Vec<_>>());
        }

        // The head row followed the last of them.
        let head: i64 = client
            .query_one(
                "SELECT sequence FROM checkpoint_threads WHERE thread_id = 'contended'",
                &[],
            )
            .unwrap()
            .get(0);
        assert_eq!(head, (WRITERS * SAVES_EACH) as i64 - 1);
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_appenders_to_one_session_keep_a_contiguous_history() {
    const WRITERS: usize = 8;
    const APPENDS_EACH: usize = 25;
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "concurrent_history");
    let mut stores = Vec::new();
    for _ in 0..WRITERS {
        stores.push(Arc::new(
            PostgresHistoryStore::from_client(sandbox.client().await)
                .await
                .unwrap(),
        ));
    }
    let barrier = Arc::new(tokio::sync::Barrier::new(WRITERS));

    let writers: Vec<_> = stores
        .iter()
        .enumerate()
        .map(|(writer, store)| {
            let (store, barrier) = (store.clone(), barrier.clone());
            tokio::spawn(async move {
                barrier.wait().await;
                for n in 0..APPENDS_EACH {
                    store
                        .append("contended", message(format!("{writer}:{n}"), 0))
                        .await
                        .unwrap();
                }
            })
        })
        .collect();
    for writer in writers {
        writer.await.unwrap();
    }

    let history = stores[0]
        .get_history("contended", WRITERS * APPENDS_EACH + 10)
        .await
        .unwrap();
    assert_eq!(history.len(), WRITERS * APPENDS_EACH, "no append was lost");
    for writer in 0..WRITERS {
        let mine: Vec<usize> = history
            .iter()
            .filter_map(|m| m.content.strip_prefix(&format!("{writer}:")))
            .map(|n| n.parse().unwrap())
            .collect();
        assert_eq!(mine, (0..APPENDS_EACH).collect::<Vec<_>>());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn opening_stores_at_the_same_moment_on_a_fresh_database_does_not_race() {
    // `CREATE TABLE IF NOT EXISTS` from two sessions at once can fail on
    // `pg_type`'s unique index in Postgres; the DDL advisory lock prevents it.
    // Two runtimes starting against a shared, empty database is exactly this.
    const OPENERS: usize = 8;
    let Some(url) = backend_url(common::POSTGRES_VAR) else {
        return;
    };
    let sandbox = PgSandbox::new(&url, "ddl_race");
    let mut clients = Vec::new();
    for _ in 0..OPENERS {
        clients.push(sandbox.client().await);
    }
    let barrier = Arc::new(tokio::sync::Barrier::new(OPENERS));

    let opens: Vec<_> = clients
        .into_iter()
        .map(|client| {
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                PostgresCheckpointStore::from_client(client).await.unwrap();
            })
        })
        .collect();
    for open in opens {
        open.await.unwrap();
    }
}
