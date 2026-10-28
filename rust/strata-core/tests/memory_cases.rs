//! The memory store's shared cases (`schema/fixtures/memory/search-cases.json`)
//! against `InMemoryMemoryStore`, and what only the in-memory store can say
//! (`design/45-Long-Term-Memory.md`).

mod memory_runner;

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use datalium_strata::memory::{
    Embedding, InMemoryMemoryStore, MemoryKey, MemoryRecord, MemorySearch, MemoryStore,
};
use memory_runner::{Opened, run_cases};

#[tokio::test]
async fn the_shared_cases_pass_against_the_in_memory_store() {
    run_cases(|dimensions, clock| async move {
        let store = Arc::new(InMemoryMemoryStore::new(dimensions).with_clock(clock));
        Opened {
            store: store.clone(),
            retention: store,
            guard: None,
        }
    })
    .await;
}

#[tokio::test]
async fn the_store_stamps_records_at_whole_seconds_and_ignores_a_callers_timestamps() {
    let at = Utc.timestamp_opt(1_790_000_000, 750_000_000).unwrap();
    let store = InMemoryMemoryStore::new(2).with_clock(Arc::new(move || at));
    let mut record = MemoryRecord::new("n", "", "a", Embedding::new("m", vec![1.0, 0.0]));
    record.created_at = Some(Utc.timestamp_opt(0, 0).unwrap());
    store.upsert(record).await.unwrap();

    let stored = store
        .get(&MemoryKey::new("n", "", "a"))
        .await
        .unwrap()
        .unwrap();
    let whole = Utc.timestamp_opt(1_790_000_000, 0).unwrap();
    assert_eq!(stored.created_at, Some(whole));
    assert_eq!(stored.updated_at, Some(whole));
    assert_eq!(store.dimensions(), 2);
}

#[tokio::test]
async fn a_match_carries_the_whole_record() {
    let store = InMemoryMemoryStore::new(2);
    let record = MemoryRecord::new(
        "incidents",
        "acme",
        "i-1",
        Embedding::new("m", vec![1.0, 1.0]),
    )
    .with_text("Alert: errors on checkout-api.")
    .with_metadata("service", "checkout-api")
    .with_content(
        serde_json::json!({ "recovered": true })
            .as_object()
            .unwrap()
            .clone(),
    );
    store.upsert(record.clone()).await.unwrap();

    let matches = store
        .search(&MemorySearch::new("incidents", Embedding::new("m", vec![2.0, 2.0])).tenant("acme"))
        .await
        .unwrap();
    assert_eq!(matches.len(), 1);
    let found = &matches[0];
    assert!((found.similarity - 1.0).abs() < 1e-12);
    assert_eq!(found.record.text, record.text);
    assert_eq!(found.record.content, record.content);
    assert_eq!(found.record.metadata, record.metadata);
}

#[test]
#[should_panic(expected = "a memory store has 1 to 16000 dimensions, not 0")]
fn a_store_of_no_dimensions_is_refused() {
    let _ = InMemoryMemoryStore::new(0);
}

#[test]
#[should_panic(expected = "a memory store has 1 to 16000 dimensions, not 16001")]
fn a_store_of_more_dimensions_than_pgvector_takes_is_refused() {
    let _ = InMemoryMemoryStore::new(16_001);
}
