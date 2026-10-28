//! Redis-specific behaviour of `RedisCheckpointStore` / `RedisHistoryStore`:
//! the key layout and JSON the two runtimes share, the thread index
//! (`design/19-Async-Queryable-Store.md`), key-prefix isolation, and
//! concurrent writers. Needs `STRATA_TEST_REDIS_URL` (see `tests/common`)
//! except for the unreachable-server test.

mod common;

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use common::{RedisSandbox, backend_url, fixture, query_seed, save_query_seed};
use datalium_strata::checkpoint::{CheckpointQuery, CheckpointStore, ThreadQuery};
use datalium_strata::history::{ChatMessage, HistoryStore, MessageRole};
use datalium_strata::schema::Checkpoint;
use datalium_strata_memory::redis_store::{RedisCheckpointStore, RedisHistoryStore};
use redis::Commands;
use serde_json::{Value, json};

fn frozen_json() -> Value {
    fixture("checkpoint-lifecycle/frozen.json")
}

fn frozen() -> Checkpoint {
    serde_json::from_value(frozen_json()).unwrap()
}

fn message(content: String) -> ChatMessage {
    ChatMessage {
        role: MessageRole::Assistant,
        content: Arc::from(content.as_str()),
        timestamp: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
    }
}

async fn store(sandbox: &RedisSandbox) -> RedisCheckpointStore {
    RedisCheckpointStore::connect_with_prefix(&sandbox.url, &sandbox.prefix)
        .await
        .unwrap()
}

/// The index sets under `prefix`, in the shape of `checkpoint-log/redis-index.json`.
fn index_sets(sandbox: &RedisSandbox) -> Value {
    let mut conn = sandbox.connection();
    let mut sets = serde_json::Map::new();
    for set in [
        "threads",
        "threads:waking",
        "threads:interrupted",
        "threads:failed",
        "threads:cancelled",
        "threads:resumed",
        "threads:completed",
        "threads:ended",
        "threads:running",
    ] {
        let members: Vec<(String, f64)> = redis::cmd("ZRANGE")
            .arg(format!("{}:{set}", sandbox.prefix))
            .arg(0)
            .arg(-1)
            .arg("REV")
            .arg("WITHSCORES")
            .query(&mut conn)
            .unwrap();
        sets.insert(
            set.to_string(),
            json!(
                members
                    .into_iter()
                    .map(|(member, score)| json!([member, score as u64]))
                    .collect::<Vec<_>>()
            ),
        );
    }
    Value::Object(sets)
}

#[tokio::test]
async fn an_unreachable_server_is_an_error_not_a_hang() {
    let started = std::time::Instant::now();
    let err = RedisCheckpointStore::connect("redis://127.0.0.1:1")
        .await
        .err()
        .expect("nothing listens on port 1");
    assert!(!err.0.is_empty());
    assert!(RedisHistoryStore::connect("not a url").await.is_err());
    assert!(started.elapsed().as_secs() < 30);
}

#[tokio::test]
async fn a_nested_checkpoint_is_one_list_element_holding_the_child_inside_it() {
    let Some(url) = backend_url(common::REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "nested");
    let store = store(&sandbox).await;
    let wire = fixture("checkpoint-lifecycle/nested.json");
    let checkpoint: Checkpoint = serde_json::from_value(wire.clone()).unwrap();
    store.save(checkpoint.clone()).await.unwrap();

    // Still one element, and it is the nested wire shape verbatim.
    let mut conn = sandbox.connection();
    let key = format!("{}:checkpoints:{}", sandbox.prefix, checkpoint.thread_id);
    let elements: Vec<String> = conn.lrange(&key, 0, -1).unwrap();
    assert_eq!(elements.len(), 1);
    assert_eq!(serde_json::from_str::<Value>(&elements[0]).unwrap(), wire);
    assert_eq!(
        store.load(&checkpoint.thread_id).await.unwrap(),
        Some(checkpoint)
    );
}

#[tokio::test]
async fn a_checkpoint_is_one_list_element_holding_the_wire_json() {
    let Some(url) = backend_url(common::REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "layout");
    let store = store(&sandbox).await;
    let checkpoint = frozen();
    store.save(checkpoint.clone()).await.unwrap();

    let mut conn = sandbox.connection();
    let key = format!("{}:checkpoints:{}", sandbox.prefix, checkpoint.thread_id);
    let elements: Vec<String> = conn.lrange(&key, 0, -1).unwrap();
    assert_eq!(elements.len(), 1);
    // The element is the shared checkpoint wire shape, verbatim: the same file
    // `checkpoint.schema.json` describes and the .NET store must also write.
    assert_eq!(
        serde_json::from_str::<Value>(&elements[0]).unwrap(),
        frozen_json()
    );

    // Saving again appends — the list is the log — and `load` reads the tail.
    let mut resumed = checkpoint.clone();
    resumed.resumed_at = Some("2026-09-16T01:00:00Z".to_string());
    store.save(resumed.clone()).await.unwrap();
    assert_eq!(conn.llen::<_, i64>(&key).unwrap(), 2);
    assert_eq!(
        store.load(&checkpoint.thread_id).await.unwrap(),
        Some(resumed)
    );
}

#[tokio::test]
async fn a_checkpoint_written_as_raw_redis_commands_is_loadable() {
    // The reader side of the contract: the wire JSON pushed by anything else
    // (the .NET store, redis-cli) is readable through `load`.
    let Some(url) = backend_url(common::REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "reader");
    let store = store(&sandbox).await;
    let checkpoint = frozen();

    let _: i64 = sandbox
        .connection()
        .rpush(
            format!("{}:checkpoints:{}", sandbox.prefix, checkpoint.thread_id),
            frozen_json().to_string(),
        )
        .unwrap();

    assert_eq!(
        store.load(&checkpoint.thread_id).await.unwrap(),
        Some(checkpoint)
    );
}

#[tokio::test]
async fn saving_the_query_seed_leaves_the_shared_index() {
    let Some(url) = backend_url(common::REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "index");
    let store = store(&sandbox).await;
    save_query_seed(&store).await;

    assert_eq!(
        index_sets(&sandbox),
        fixture("checkpoint-log/redis-index.json")["sets"]
    );
}

#[tokio::test]
async fn rebuilding_the_index_from_the_lists_alone_gives_the_same_sets() {
    let Some(url) = backend_url(common::REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "rebuild");
    let store = store(&sandbox).await;
    save_query_seed(&store).await;

    // Drop every index set, keeping the lists, then rebuild: the Lua that
    // reads each list's tail must agree with what the writer computed.
    let mut conn = sandbox.connection();
    let index_keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{}:threads*", sandbox.prefix))
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("DEL").arg(index_keys).query(&mut conn).unwrap();

    assert_eq!(store.rebuild_index().await.unwrap(), 15);
    assert_eq!(
        index_sets(&sandbox),
        fixture("checkpoint-log/redis-index.json")["sets"]
    );
}

#[tokio::test]
async fn a_prefix_written_before_the_index_is_indexed_on_first_connect() {
    let Some(url) = backend_url(common::REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "first_connect");
    // Lists only, as a runtime from before SG-11 leaves them.
    let mut conn = sandbox.connection();
    for checkpoint in query_seed() {
        let _: i64 = conn
            .rpush(
                format!("{}:checkpoints:{}", sandbox.prefix, checkpoint.thread_id),
                serde_json::to_string(&checkpoint).unwrap(),
            )
            .unwrap();
    }

    let store = store(&sandbox).await;
    assert_eq!(
        index_sets(&sandbox),
        fixture("checkpoint-log/redis-index.json")["sets"]
    );
    let page = store.list_threads(&ThreadQuery::default()).await.unwrap();
    assert_eq!(page.threads.len(), 15);
}

#[tokio::test]
async fn a_thread_pushed_without_the_index_is_listed_after_a_rebuild() {
    // What an older runtime writing to an indexed prefix does: it appends, and
    // the index doesn't hear about it until `rebuild_index`.
    let Some(url) = backend_url(common::REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "old_writer");
    let store = store(&sandbox).await;
    let _: i64 = sandbox
        .connection()
        .rpush(
            format!("{}:checkpoints:{}", sandbox.prefix, frozen().thread_id),
            frozen_json().to_string(),
        )
        .unwrap();

    let listed = |page: datalium_strata::checkpoint::ThreadPage| page.threads.len();
    assert_eq!(
        listed(store.list_threads(&ThreadQuery::default()).await.unwrap()),
        0
    );
    store.rebuild_index().await.unwrap();
    assert_eq!(
        listed(store.list_threads(&ThreadQuery::default()).await.unwrap()),
        1
    );
}

#[tokio::test]
async fn a_message_is_one_list_element_with_role_content_and_timestamp() {
    let Some(url) = backend_url(common::REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "history_layout");
    let store = RedisHistoryStore::connect_with_prefix(&url, &sandbox.prefix)
        .await
        .unwrap();
    store
        .append("s1", message("hello".to_string()))
        .await
        .unwrap();

    let elements: Vec<String> = sandbox
        .connection()
        .lrange(format!("{}:history:s1", sandbox.prefix), 0, -1)
        .unwrap();
    assert_eq!(elements.len(), 1);
    assert_eq!(
        serde_json::from_str::<Value>(&elements[0]).unwrap(),
        json!({
            "role": "assistant",
            "content": "hello",
            "timestamp": "2023-11-14T22:13:20Z",
        })
    );
}

#[tokio::test]
async fn different_prefixes_are_different_namespaces_on_one_redis() {
    let Some(url) = backend_url(common::REDIS_VAR) else {
        return;
    };
    let a = RedisSandbox::new(&url, "ns_a");
    let b = RedisSandbox::new(&url, "ns_b");
    let store_a = store(&a).await;
    let store_b = store(&b).await;
    let history_a = RedisHistoryStore::connect_with_prefix(&url, &a.prefix)
        .await
        .unwrap();
    let history_b = RedisHistoryStore::connect_with_prefix(&url, &b.prefix)
        .await
        .unwrap();

    store_a.save(frozen()).await.unwrap();
    history_a
        .append("s1", message("only in a".to_string()))
        .await
        .unwrap();

    assert_eq!(store_b.load(&frozen().thread_id).await.unwrap(), None);
    assert_eq!(history_b.get_history("s1", 10).await.unwrap(), Vec::new());
    assert_eq!(
        store_b
            .list_threads(&ThreadQuery::default())
            .await
            .unwrap()
            .threads
            .len(),
        0
    );
    assert_eq!(
        store_a.load(&frozen().thread_id).await.unwrap(),
        Some(frozen())
    );
    assert_eq!(history_a.get_history("s1", 10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_checkpoint_survives_independent_store_instances() {
    let Some(url) = backend_url(common::REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "persist");
    {
        let store = store(&sandbox).await;
        store.save(frozen()).await.unwrap();
    }

    let reopened = store(&sandbox).await;
    assert_eq!(
        reopened.load(&frozen().thread_id).await.unwrap(),
        Some(frozen())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_writers_never_lose_a_checkpoint_or_a_message() {
    // The save script is atomic, so — unlike the SQL stores — there is no
    // read-then-write to protect; this proves that stays true through the
    // adapter with many connections at once, index included.
    const WRITERS: usize = 8;
    const EACH: usize = 50;
    let Some(url) = backend_url(common::REDIS_VAR) else {
        return;
    };
    let sandbox = RedisSandbox::new(&url, "concurrent");
    let barrier = Arc::new(tokio::sync::Barrier::new(WRITERS));

    let writers: Vec<_> = (0..WRITERS)
        .map(|writer| {
            let (barrier, url, prefix) = (barrier.clone(), url.clone(), sandbox.prefix.clone());
            tokio::spawn(async move {
                let checkpoints = RedisCheckpointStore::connect_with_prefix(&url, &prefix)
                    .await
                    .unwrap();
                let history = RedisHistoryStore::connect_with_prefix(&url, &prefix)
                    .await
                    .unwrap();
                barrier.wait().await;
                for n in 0..EACH {
                    let mut checkpoint = frozen();
                    checkpoint.created_at = format!("2026-09-16T00:{writer:02}:{n:02}Z");
                    checkpoints.save(checkpoint).await.unwrap();
                    history
                        .append("contended", message(format!("{writer}:{n}")))
                        .await
                        .unwrap();
                }
            })
        })
        .collect();
    for writer in writers {
        writer.await.unwrap();
    }

    let mut conn = sandbox.connection();
    let checkpoints: i64 = conn
        .llen(format!(
            "{}:checkpoints:{}",
            sandbox.prefix,
            frozen().thread_id
        ))
        .unwrap();
    assert_eq!(checkpoints as usize, WRITERS * EACH);

    let history = RedisHistoryStore::connect_with_prefix(&url, &sandbox.prefix)
        .await
        .unwrap()
        .get_history("contended", WRITERS * EACH + 10)
        .await
        .unwrap();
    assert_eq!(history.len(), WRITERS * EACH);
    for writer in 0..WRITERS {
        let mine: Vec<usize> = history
            .iter()
            .filter_map(|m| m.content.strip_prefix(&format!("{writer}:")))
            .map(|n| n.parse().unwrap())
            .collect();
        assert_eq!(mine, (0..EACH).collect::<Vec<_>>());
    }

    // The thread is in exactly one state set, with its latest checkpoint's score.
    let latest = store(&sandbox)
        .await
        .load(&frozen().thread_id)
        .await
        .unwrap();
    let page = store(&sandbox)
        .await
        .list_threads(&ThreadQuery::default())
        .await
        .unwrap();
    assert_eq!(page.threads.len(), 1);
    assert_eq!(
        datalium_strata::store::timestamp_text(page.threads[0].updated_at),
        latest.unwrap().created_at
    );
}
