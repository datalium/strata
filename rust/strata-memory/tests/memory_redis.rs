//! `RedisMemoryStore` (`design/45-Long-Term-Memory.md`, "Redis"): the shared
//! cases (`schema/fixtures/memory/search-cases.json`), with a `FLAT` and an
//! `HNSW` index, the keys both runtimes write
//! (`schema/fixtures/memory/redis-record.json`), the dimensions fixed per
//! prefix, the `memory` component's version, and a search right after an
//! upsert finding what it wrote (50 fresh prefixes per index). Needs
//! `STRATA_TEST_REDIS_URL` on a server with the Query Engine (CI's Redis 8);
//! without it each test says `SKIPPED` and passes. The key escaping, the tags,
//! the `FT.CREATE` arguments, the replies and the refusal of a server without
//! the Query Engine are unit tests in `src/redis_memory.rs`, which need no
//! server.

mod common;
#[path = "../../strata-core/tests/memory_runner/mod.rs"]
mod memory_runner;

use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use common::{REDIS_VAR, RedisSandbox, backend_url, fixture};
use datalium_strata::memory::{MemoryStore, MemoryStoreError};
use datalium_strata_memory::memory::MemoryIndex;
use datalium_strata_memory::redis_memory::{RedisMemoryOptions, RedisMemoryStore};
use memory_runner::{Opened, run_cases};
use serde_json::{Value, json};

/// A prefix of its own, whose index is dropped and keys deleted afterwards.
struct MemorySandbox(RedisSandbox);

impl MemorySandbox {
    fn new(url: &str, label: &str) -> Self {
        Self(RedisSandbox::new(url, label))
    }

    fn prefix(&self) -> &str {
        &self.0.prefix
    }

    fn connection(&self) -> redis::Connection {
        self.0.connection()
    }
}

impl Drop for MemorySandbox {
    fn drop(&mut self) {
        // The keys go with the sandbox, after this.
        let _: Result<(), _> = redis::cmd("FT.DROPINDEX")
            .arg(format!("{}:memory:idx", self.0.prefix))
            .query(&mut self.0.connection());
    }
}

async fn open(
    sandbox: &MemorySandbox,
    options: RedisMemoryOptions,
) -> Result<RedisMemoryStore, MemoryStoreError> {
    RedisMemoryStore::connect(&sandbox.0.url, options.with_prefix(sandbox.prefix())).await
}

fn time(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .unwrap()
        .with_timezone(&Utc)
}

async fn the_shared_cases_pass(index: MemoryIndex) {
    let Some(url) = backend_url(REDIS_VAR) else {
        return;
    };
    run_cases(|dimensions, clock| {
        let url = url.clone();
        async move {
            let sandbox = MemorySandbox::new(&url, "memory_case");
            let store = Arc::new(
                open(
                    &sandbox,
                    RedisMemoryOptions::new(dimensions)
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
async fn the_shared_cases_pass_against_redis() {
    the_shared_cases_pass(MemoryIndex::None).await;
}

#[tokio::test]
async fn the_shared_cases_pass_against_redis_with_an_hnsw_index() {
    the_shared_cases_pass(MemoryIndex::hnsw()).await;
}

/// A sorted set's members and scores, in the shape of the fixture's `sets`.
fn sorted_set(conn: &mut redis::Connection, key: &str) -> Value {
    let flat: Vec<String> = redis::cmd("ZRANGE")
        .arg(key)
        .arg(0)
        .arg(-1)
        .arg("WITHSCORES")
        .query(conn)
        .unwrap();
    Value::Array(
        flat.as_chunks::<2>()
            .0
            .iter()
            .map(|[member, score]| json!([member, score.parse::<u64>().unwrap()]))
            .collect(),
    )
}

#[tokio::test]
async fn the_keys_are_the_ones_both_runtimes_write() {
    let Some(url) = backend_url(REDIS_VAR) else {
        return;
    };
    let layout = fixture("memory/redis-record.json");
    let sandbox = MemorySandbox::new(&url, "memory_layout");
    let prefix = sandbox.prefix().to_string();
    let now = Arc::new(Mutex::new(time(layout["created_at"].as_str().unwrap())));
    let clock = {
        let now = now.clone();
        Arc::new(move || *now.lock().unwrap())
    };
    let store = open(
        &sandbox,
        RedisMemoryOptions::new(layout["dimensions"].as_u64().unwrap() as usize).with_clock(clock),
    )
    .await
    .unwrap();
    let record = memory_runner::record(&layout["record"]);
    store.upsert(record.clone()).await.unwrap();
    *now.lock().unwrap() = time(layout["updated_at"].as_str().unwrap());
    store.upsert(record).await.unwrap();

    let mut conn = sandbox.connection();
    let hash: std::collections::HashMap<String, Vec<u8>> = redis::cmd("HGETALL")
        .arg(format!(
            "{prefix}:{}",
            layout["hash"]["key"].as_str().unwrap()
        ))
        .query(&mut conn)
        .unwrap();
    let expected = layout["hash"]["fields"].as_object().unwrap();
    let mut names: Vec<&String> = hash.keys().collect();
    names.sort();
    let mut wanted: Vec<&String> = expected.keys().collect();
    wanted.sort();
    assert_eq!(names, wanted, "the hash's fields");
    for (name, value) in &hash {
        let want = expected[name].as_str().unwrap();
        match name.as_str() {
            "embedding" => {
                let hex: String = value.iter().map(|b| format!("{b:02x}")).collect();
                assert_eq!(hex, want, "embedding");
            }
            "content" | "metadata" => assert_eq!(
                serde_json::from_slice::<Value>(value).unwrap(),
                serde_json::from_str::<Value>(want).unwrap(),
                "{name}"
            ),
            _ => assert_eq!(std::str::from_utf8(value).unwrap(), want, "{name}"),
        }
    }

    for (key, members) in layout["sets"].as_object().unwrap() {
        let members: Vec<Value> = members
            .as_array()
            .unwrap()
            .iter()
            .map(|pair| {
                let member = pair[0].as_str().unwrap();
                let member = match member.strip_prefix("strata:") {
                    Some(rest) => format!("{prefix}:{rest}"),
                    None => member.to_string(),
                };
                json!([member, pair[1]])
            })
            .collect();
        assert_eq!(
            sorted_set(&mut conn, &format!("{prefix}:{key}")),
            Value::Array(members),
            "{key}"
        );
    }

    let schema: std::collections::HashMap<String, String> = redis::cmd("HGETALL")
        .arg(format!(
            "{prefix}:{}",
            layout["schema"]["key"].as_str().unwrap()
        ))
        .query(&mut conn)
        .unwrap();
    let expected: std::collections::HashMap<String, String> =
        serde_json::from_value(layout["schema"]["fields"].clone()).unwrap();
    assert_eq!(schema, expected);
}

#[tokio::test]
async fn a_prefix_made_with_other_dimensions_is_refused() {
    let Some(url) = backend_url(REDIS_VAR) else {
        return;
    };
    let sandbox = MemorySandbox::new(&url, "memory_dimensions");
    open(&sandbox, RedisMemoryOptions::new(768)).await.unwrap();
    let err = open(&sandbox, RedisMemoryOptions::new(1536))
        .await
        .err()
        .expect("other dimensions");
    assert_eq!(
        err.0,
        "memory was created with 768 dimensions, and this store was opened with 1536"
    );
}

#[tokio::test]
async fn a_newer_prefix_is_refused_unchanged() {
    let Some(url) = backend_url(REDIS_VAR) else {
        return;
    };
    let sandbox = MemorySandbox::new(&url, "memory_newer");
    open(&sandbox, RedisMemoryOptions::new(3)).await.unwrap();
    let schema_key = format!("{}:memory:schema", sandbox.prefix());
    let _: () = redis::cmd("HSET")
        .arg(&schema_key)
        .arg("version")
        .arg(2)
        .query(&mut sandbox.connection())
        .unwrap();

    let err = open(&sandbox, RedisMemoryOptions::new(3))
        .await
        .err()
        .expect("a newer prefix");
    assert_eq!(
        err.0,
        "database is at memory schema version 2, and this build knows 1: upgrade the runtime"
    );
    let version: String = redis::cmd("HGET")
        .arg(&schema_key)
        .arg("version")
        .query(&mut sandbox.connection())
        .unwrap();
    assert_eq!(version, "2");
}

#[tokio::test]
async fn opening_a_prefix_again_keeps_its_index_and_records() {
    let Some(url) = backend_url(REDIS_VAR) else {
        return;
    };
    let sandbox = MemorySandbox::new(&url, "memory_reopen");
    let store = open(&sandbox, RedisMemoryOptions::new(3)).await.unwrap();
    store
        .upsert(datalium_strata::memory::MemoryRecord::new(
            "n:1",
            "a%b",
            "id:x",
            datalium_strata::memory::Embedding::new("m", vec![1.0, 0.0, 0.0]),
        ))
        .await
        .unwrap();
    // The index exists: a second open accepts it.
    let again = open(&sandbox, RedisMemoryOptions::new(3)).await.unwrap();
    let matches = again
        .search(
            &datalium_strata::memory::MemorySearch::new(
                "n:1",
                datalium_strata::memory::Embedding::new("m", vec![1.0, 0.0, 0.0]),
            )
            .tenant("a%b"),
        )
        .await
        .unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].record.id, "id:x");
    assert!((matches[0].similarity - 1.0).abs() < 1e-9);
}

#[tokio::test]
async fn a_store_that_loses_the_dimensions_makes_no_index() {
    let Some(url) = backend_url(REDIS_VAR) else {
        return;
    };
    // Another store claimed the empty prefix first, with other dimensions,
    // and hasn't made the index yet.
    let sandbox = MemorySandbox::new(&url, "memory_race");
    let _: () = redis::cmd("HSET")
        .arg(format!("{}:memory:schema", sandbox.prefix()))
        .arg("version")
        .arg(1)
        .arg("dimensions")
        .arg(1536)
        .query(&mut sandbox.connection())
        .unwrap();

    let err = open(&sandbox, RedisMemoryOptions::new(768))
        .await
        .err()
        .expect("other dimensions");
    assert_eq!(
        err.0,
        "memory was created with 1536 dimensions, and this store was opened with 768"
    );
    // The loser was refused before FT.CREATE: the winner makes the index.
    let info: Result<redis::Value, _> = redis::cmd("FT.INFO")
        .arg(format!("{}:memory:idx", sandbox.prefix()))
        .query(&mut sandbox.connection());
    assert!(info.is_err(), "no index was made: {info:?}");
    open(&sandbox, RedisMemoryOptions::new(1536)).await.unwrap();
}

/// How many fresh prefixes each read-your-writes test writes and searches.
const ROUNDS: usize = 50;

/// The first shared case's six records.
fn six_records() -> Vec<datalium_strata::memory::MemoryRecord> {
    let cases = memory_runner::cases();
    cases["cases"][0]["steps"][0]["upsert_many"]
        .as_array()
        .unwrap()
        .iter()
        .map(memory_runner::record)
        .collect()
}

/// The ids a search of `n` nearest `vector` finds, in order.
async fn ranked(store: &RedisMemoryStore, vector: Vec<f32>) -> Vec<String> {
    store
        .search(&datalium_strata::memory::MemorySearch::new(
            "n",
            datalium_strata::memory::Embedding::new("m", vector),
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|found| found.record.id)
        .collect()
}

/// A search right after an upsert finds every record it wrote, as written,
/// however the Query Engine's workers are scheduled.
async fn a_search_right_after_an_upsert_finds_it(index: MemoryIndex, one_by_one: bool) {
    let Some(url) = backend_url(REDIS_VAR) else {
        return;
    };
    for round in 0..ROUNDS {
        let sandbox = MemorySandbox::new(&url, "memory_ryw");
        let store = open(&sandbox, RedisMemoryOptions::new(3).with_index(index))
            .await
            .unwrap();
        if one_by_one {
            for record in six_records() {
                store.upsert(record).await.unwrap();
            }
        } else {
            store.upsert_many(six_records()).await.unwrap();
        }
        assert_eq!(
            ranked(&store, vec![1.0, 0.0, 0.0]).await,
            ["a", "e", "b", "f", "c", "d"],
            "round {round}"
        );
        // An update that moves `d` next to the query is ranked as updated.
        let mut moved = six_records().pop().unwrap();
        moved.id = "d".to_string();
        store.upsert(moved).await.unwrap();
        assert_eq!(
            ranked(&store, vec![1.0, 0.0, 0.0]).await,
            ["a", "d", "e", "b", "f", "c"],
            "round {round}, after the update"
        );
    }
}

#[tokio::test]
async fn a_search_right_after_upsert_many_finds_every_record() {
    a_search_right_after_an_upsert_finds_it(MemoryIndex::None, false).await;
}

#[tokio::test]
async fn a_search_right_after_upsert_many_finds_every_record_with_an_hnsw_index() {
    a_search_right_after_an_upsert_finds_it(MemoryIndex::hnsw(), false).await;
}

#[tokio::test]
async fn a_search_right_after_each_upsert_finds_every_record() {
    a_search_right_after_an_upsert_finds_it(MemoryIndex::None, true).await;
}

#[tokio::test]
async fn a_search_right_after_each_upsert_finds_every_record_with_an_hnsw_index() {
    a_search_right_after_an_upsert_finds_it(MemoryIndex::hnsw(), true).await;
}

/// A key written twice in one `upsert_many` is confirmed at its last write:
/// the call returns (confirming the first write would time out after 5 s),
/// and a search right after sees the last vector.
async fn a_key_written_twice_in_one_upsert_many_is_its_last_write(index: MemoryIndex) {
    let Some(url) = backend_url(REDIS_VAR) else {
        return;
    };
    let sandbox = MemorySandbox::new(&url, "memory_twice");
    let store = open(&sandbox, RedisMemoryOptions::new(3).with_index(index))
        .await
        .unwrap();
    let mut records = six_records();
    let mut moved = records.last().unwrap().clone();
    moved.id = "d".to_string();
    records.push(moved);
    store.upsert_many(records).await.unwrap();
    assert_eq!(
        ranked(&store, vec![1.0, 0.0, 0.0]).await,
        ["a", "d", "e", "b", "f", "c"]
    );
    let d = store
        .get(&datalium_strata::memory::MemoryKey::new("n", "", "d"))
        .await
        .unwrap()
        .expect("d");
    assert_eq!(d.embedding.vector, [1.0, 0.0, 0.0]);
}

#[tokio::test]
async fn a_key_written_twice_in_one_upsert_many_is_its_last_write_against_redis() {
    a_key_written_twice_in_one_upsert_many_is_its_last_write(MemoryIndex::None).await;
}

#[tokio::test]
async fn a_key_written_twice_in_one_upsert_many_is_its_last_write_with_an_hnsw_index() {
    a_key_written_twice_in_one_upsert_many_is_its_last_write(MemoryIndex::hnsw()).await;
}
