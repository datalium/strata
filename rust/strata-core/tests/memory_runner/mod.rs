//! The runner of the memory store's shared cases
//! (`schema/fixtures/memory/search-cases.json`; its `description` explains the
//! format), replayed against every memory store: `tests/memory_cases.rs` runs
//! them on `InMemoryMemoryStore`, and `strata-memory`'s
//! `tests/memory_postgres.rs` includes this file by path to run them on
//! Postgres. Each case opens a fresh store, whose clock a step's `at` sets.
#![allow(dead_code)]

use std::any::Any;
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use datalium_strata::memory::{
    Embedding, MemoryClock, MemoryKey, MemoryPurge, MemoryQuery, MemoryRecord, MemoryRetention,
    MemorySearch, MemoryStore, MemoryStoreError,
};
use datalium_strata::store::timestamp_text;
use serde_json::{Map, Value};

/// A fresh store for one case, and whatever must live as long as it (a
/// database sandbox).
pub struct Opened {
    pub store: Arc<dyn MemoryStore>,
    pub retention: Arc<dyn MemoryRetention>,
    pub guard: Option<Box<dyn Any + Send>>,
}

/// The cases file.
pub fn cases() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/fixtures/memory/search-cases.json");
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    serde_json::from_str(&raw).unwrap()
}

/// Runs every case on a store `open` makes for it from its dimensions and a
/// clock the steps set.
pub async fn run_cases<F, Fut>(open: F)
where
    F: Fn(usize, MemoryClock) -> Fut,
    Fut: Future<Output = Opened>,
{
    let cases = cases();
    let cases = cases["cases"].as_array().expect("cases");
    assert!(!cases.is_empty());
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let dimensions = case["dimensions"].as_u64().unwrap() as usize;
        let now = Arc::new(Mutex::new(Utc::now()));
        let clock: MemoryClock = {
            let now = now.clone();
            Arc::new(move || *now.lock().unwrap())
        };
        let opened = open(dimensions, clock).await;
        for (index, step) in case["steps"].as_array().unwrap().iter().enumerate() {
            let at = format!("case '{name}', step {index}");
            if let Some(time) = step.get("at") {
                *now.lock().unwrap() = time_of(time);
            }
            run_step(&opened, step, &at).await;
        }
        drop(opened);
    }
}

fn time_of(value: &Value) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value.as_str().unwrap())
        .unwrap()
        .with_timezone(&Utc)
}

fn text(value: &Value, key: &str, default: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or(default)
        .to_string()
}

fn vector(value: &Value) -> Vec<f32> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_f64().unwrap() as f32)
        .collect()
}

fn pairs(value: Option<&Value>) -> Vec<(String, String)> {
    value
        .and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string()))
                .collect()
        })
        .unwrap_or_default()
}

/// A record in its JSON form, without the timestamps.
pub fn record(value: &Value) -> MemoryRecord {
    let embedding = &value["embedding"];
    let mut record = MemoryRecord::new(
        text(value, "namespace", ""),
        text(value, "tenant", ""),
        text(value, "id", ""),
        Embedding::new(text(embedding, "model", ""), vector(&embedding["vector"])),
    );
    if let Some(Value::Object(content)) = value.get("content") {
        record = record.with_content(content.clone());
    }
    if let Some(Value::String(body)) = value.get("text") {
        record = record.with_text(body.clone());
    }
    for (key, value) in pairs(value.get("metadata")) {
        record = record.with_metadata(key, value);
    }
    record
}

fn key(value: &Value) -> MemoryKey {
    MemoryKey::new(
        text(value, "namespace", ""),
        text(value, "tenant", ""),
        text(value, "id", ""),
    )
}

fn search(value: &Value) -> MemorySearch {
    let mut search = MemorySearch::new(
        text(value, "namespace", ""),
        Embedding::new(text(value, "model", ""), vector(&value["vector"])),
    )
    .tenant(text(value, "tenant", ""));
    if let Some(limit) = value.get("limit") {
        search = search.limit(limit.as_u64().unwrap() as usize);
    }
    if let Some(min) = value.get("min_similarity") {
        search = search.min_similarity(min.as_f64().unwrap());
    }
    for id in value
        .get("exclude")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        search = search.exclude(id.as_str().unwrap());
    }
    for (key, value) in pairs(value.get("metadata")) {
        search = search.with_metadata(key, value);
    }
    search
}

fn query(value: &Value) -> MemoryQuery {
    let mut query =
        MemoryQuery::new(text(value, "namespace", "")).tenant(text(value, "tenant", ""));
    if let Some(limit) = value.get("limit") {
        query = query.limit(limit.as_u64().unwrap() as usize);
    }
    for (key, value) in pairs(value.get("metadata")) {
        query = query.with_metadata(key, value);
    }
    query
}

fn purge(value: &Value) -> MemoryPurge {
    let mut purge = MemoryPurge::new(time_of(&value["older_than"]));
    if let Some(namespace) = value.get("namespace").and_then(Value::as_str) {
        purge = purge.namespace(namespace);
    }
    if let Some(tenant) = value.get("tenant").and_then(Value::as_str) {
        purge = purge.tenant(tenant);
    }
    if let Some(limit) = value.get("limit") {
        purge = purge.limit(limit.as_u64().unwrap() as usize);
    }
    purge
}

/// A step's outcome: what a successful call gives back, or its refusal.
enum Outcome {
    Done(Value),
    Pages(Vec<Vec<String>>),
}

async fn run_step(opened: &Opened, step: &Value, at: &str) {
    let store = &*opened.store;
    let result: Result<Outcome, MemoryStoreError> = if let Some(value) = step.get("upsert") {
        store
            .upsert(record(value))
            .await
            .map(|()| Outcome::Done(Value::Null))
    } else if let Some(value) = step.get("upsert_many") {
        let records = value.as_array().unwrap().iter().map(record).collect();
        store
            .upsert_many(records)
            .await
            .map(|()| Outcome::Done(Value::Null))
    } else if let Some(value) = step.get("get") {
        store
            .get(&key(value))
            .await
            .map(|found| Outcome::Done(found.as_ref().map_or(Value::Null, record_json)))
    } else if let Some(value) = step.get("delete") {
        store
            .delete(&key(value))
            .await
            .map(|deleted| Outcome::Done(Value::Bool(deleted)))
    } else if let Some(value) = step.get("search") {
        store.search(&search(value)).await.map(|matches| {
            Outcome::Done(Value::Array(
                matches
                    .iter()
                    .map(|found| serde_json::json!({ "id": found.record.id, "similarity": found.similarity }))
                    .collect(),
            ))
        })
    } else if let Some(value) = step.get("list") {
        list_all(store, query(value)).await.map(Outcome::Pages)
    } else if let Some(value) = step.get("purge") {
        opened
            .retention
            .purge(&purge(value))
            .await
            .map(|count| Outcome::Done(Value::from(count)))
    } else {
        panic!("{at}: a step without an operation: {step}");
    };

    if let Some(error) = step.get("error") {
        match result {
            Err(err) => assert_eq!(err.0, error.as_str().unwrap(), "{at}: the refusal"),
            Ok(_) => panic!("{at}: expected the refusal '{error}', and the call succeeded"),
        }
        return;
    }
    let outcome = result.unwrap_or_else(|err| panic!("{at}: {err}"));
    if step.get("search").is_some() {
        let Outcome::Done(Value::Array(actual)) = outcome else {
            unreachable!()
        };
        let expected = step["expect"].as_array().unwrap();
        let ids = |list: &[Value]| -> Vec<String> {
            list.iter()
                .map(|m| m["id"].as_str().unwrap().to_string())
                .collect()
        };
        assert_eq!(ids(&actual), ids(expected), "{at}: the matches in order");
        for (actual, expected) in actual.iter().zip(expected) {
            let (a, e) = (
                actual["similarity"].as_f64().unwrap(),
                expected["similarity"].as_f64().unwrap(),
            );
            assert!(
                (a - e).abs() <= 1e-5,
                "{at}: '{}' has similarity {a}, not {e}",
                actual["id"]
            );
        }
    } else if let Some(pages) = step.get("expect_pages") {
        let Outcome::Pages(actual) = outcome else {
            unreachable!()
        };
        let expected: Vec<Vec<String>> = serde_json::from_value(pages.clone()).unwrap();
        assert_eq!(actual, expected, "{at}: the pages");
    } else if let Some(expected) = step.get("expect") {
        let Outcome::Done(actual) = outcome else {
            unreachable!()
        };
        if step.get("get").is_some() && !expected.is_null() {
            assert_record(&actual, expected, at);
        } else {
            assert_eq!(&actual, expected, "{at}");
        }
    }
}

/// Every page of `query`, following the cursor until there is none.
async fn list_all(
    store: &dyn MemoryStore,
    mut query: MemoryQuery,
) -> Result<Vec<Vec<String>>, MemoryStoreError> {
    let mut pages = Vec::new();
    loop {
        let page = store.list(&query).await?;
        pages.push(page.records.iter().map(|r| r.id.clone()).collect());
        match page.next_cursor {
            Some(cursor) => query = query.cursor(cursor),
            None => return Ok(pages),
        }
        assert!(pages.len() < 100, "a listing that never ends");
    }
}

/// A record's JSON form, without `schema_version`.
pub fn record_json(record: &MemoryRecord) -> Value {
    serde_json::json!({
        "namespace": record.namespace,
        "tenant": record.tenant,
        "id": record.id,
        "embedding": {
            "model": record.embedding.model,
            "vector": record.embedding.vector.iter().map(|x| f64::from(*x)).collect::<Vec<_>>(),
        },
        "content": record.content.clone().map(Value::Object),
        "text": record.text,
        "metadata": record.metadata.iter().map(|(k, v)| (k.clone(), Value::from(v.clone()))).collect::<Map<_, _>>(),
        "created_at": record.created_at.map(timestamp_text),
        "updated_at": record.updated_at.map(timestamp_text),
    })
}

/// Compares a record with its expected JSON form: the vector by value (JSON
/// `1` is `1.0`), everything else exactly.
fn assert_record(actual: &Value, expected: &Value, at: &str) {
    let mut actual = actual.clone();
    let mut expected = expected.clone();
    let numbers = |value: &mut Value| -> Vec<f64> {
        value["embedding"]["vector"]
            .take()
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_f64().unwrap())
            .collect()
    };
    assert_eq!(
        numbers(&mut actual),
        numbers(&mut expected),
        "{at}: the vector"
    );
    assert_eq!(actual, expected, "{at}: the record");
}
