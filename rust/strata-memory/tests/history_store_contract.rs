//! Shared `HistoryStore` contract test suite (Phase 6 exit criterion): the
//! exact same assertions run against every backend — `InMemoryHistoryStore`,
//! `SqliteHistoryStore`, and (when `STRATA_TEST_POSTGRES_URL` /
//! `STRATA_TEST_REDIS_URL` are set; see `tests/common`) `PostgresHistoryStore`
//! and `RedisHistoryStore` — proving they behave identically from a caller's
//! perspective regardless of backend.

mod common;

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use common::{PgSandbox, RedisSandbox, backend_url};
use datalium_strata::history::{
    BlockingHistoryStore, ChatMessage, HistoryStore, HistoryStoreError, MessageRole,
    SyncHistoryStore,
};
use datalium_strata_memory::history::{InMemoryHistoryStore, SqliteHistoryStore};
use datalium_strata_memory::postgres_store::PostgresHistoryStore;
use datalium_strata_memory::redis_store::RedisHistoryStore;

fn message(role: MessageRole, content: &str, seconds_offset: i64) -> ChatMessage {
    ChatMessage {
        role,
        content: Arc::from(content),
        timestamp: Utc
            .timestamp_opt(1_700_000_000 + seconds_offset, 0)
            .unwrap(),
    }
}

fn contents(messages: &[ChatMessage]) -> Vec<&str> {
    messages.iter().map(|m| m.content.as_ref()).collect()
}

async fn run_contract(store: &dyn HistoryStore) {
    // Unknown session -> empty history, not an error.
    assert_eq!(store.get_history("missing", 10).await.unwrap(), Vec::new());

    // Append then retrieve, in chronological (insertion) order.
    store
        .append("s1", message(MessageRole::User, "hi", 0))
        .await
        .unwrap();
    store
        .append("s1", message(MessageRole::Assistant, "hello!", 1))
        .await
        .unwrap();
    assert_eq!(
        contents(&store.get_history("s1", 10).await.unwrap()),
        ["hi", "hello!"]
    );

    // `limit` returns only the most recent N, still chronological.
    store
        .append("s1", message(MessageRole::User, "third", 2))
        .await
        .unwrap();
    assert_eq!(
        contents(&store.get_history("s1", 2).await.unwrap()),
        ["hello!", "third"]
    );

    // A limit larger than the history is not an error; a limit of zero is an
    // empty history (in Redis, `LRANGE key -0 -1` would be *everything*).
    assert_eq!(store.get_history("s1", 1_000).await.unwrap().len(), 3);
    assert_eq!(store.get_history("s1", 0).await.unwrap(), Vec::new());

    // Sessions are isolated from each other.
    store
        .append("s2", message(MessageRole::System, "other session", 0))
        .await
        .unwrap();
    assert_eq!(store.get_history("s1", 10).await.unwrap().len(), 3);
    assert_eq!(
        contents(&store.get_history("s2", 10).await.unwrap()),
        ["other session"]
    );
}

/// Every role, a sub-second timestamp, and content that would break a naive
/// encoding (quotes, backslashes, newlines, non-ASCII, empty) come back intact.
async fn run_round_trip_contract(store: &dyn HistoryStore) {
    let awkward = [
        "",
        "say \"hi\" \\ then\nnewline\ttab",
        "naïve — 日本語 🚀",
        "{\"looks\":\"like json\"}",
    ];
    let roles = [
        MessageRole::System,
        MessageRole::User,
        MessageRole::Assistant,
        MessageRole::Tool,
    ];
    let timestamp = Utc.timestamp_opt(1_700_000_000, 123_456_000).unwrap();

    let mut expected = Vec::new();
    for (i, content) in awkward.iter().enumerate() {
        let sent = ChatMessage {
            role: roles[i],
            content: Arc::from(*content),
            timestamp,
        };
        store.append("round-trip", sent.clone()).await.unwrap();
        expected.push(sent);
    }

    assert_eq!(store.get_history("round-trip", 10).await.unwrap(), expected);
}

async fn postgres_store() -> Option<(PgSandbox, PostgresHistoryStore)> {
    let url = backend_url(common::POSTGRES_VAR)?;
    let sandbox = PgSandbox::new(&url, "history");
    let store = PostgresHistoryStore::from_client(sandbox.client().await)
        .await
        .unwrap();
    Some((sandbox, store))
}

async fn redis_store() -> Option<(RedisSandbox, RedisHistoryStore)> {
    let url = backend_url(common::REDIS_VAR)?;
    let sandbox = RedisSandbox::new(&url, "history");
    let store = RedisHistoryStore::connect_with_prefix(&url, &sandbox.prefix)
        .await
        .unwrap();
    Some((sandbox, store))
}

#[tokio::test]
async fn in_memory_store_satisfies_the_history_store_contract() {
    run_contract(&InMemoryHistoryStore::new()).await;
    run_round_trip_contract(&InMemoryHistoryStore::new()).await;
}

#[tokio::test]
async fn sqlite_store_satisfies_the_history_store_contract() {
    run_contract(&SqliteHistoryStore::open_in_memory().unwrap()).await;
    run_round_trip_contract(&SqliteHistoryStore::open_in_memory().unwrap()).await;
}

#[tokio::test]
async fn postgres_store_satisfies_the_history_store_contract() {
    let Some((_sandbox, store)) = postgres_store().await else {
        return;
    };
    run_contract(&store).await;
}

#[tokio::test]
async fn postgres_store_round_trips_every_role_and_awkward_content() {
    let Some((_sandbox, store)) = postgres_store().await else {
        return;
    };
    run_round_trip_contract(&store).await;
}

#[tokio::test]
async fn redis_store_satisfies_the_history_store_contract() {
    let Some((_sandbox, store)) = redis_store().await else {
        return;
    };
    run_contract(&store).await;
}

#[tokio::test]
async fn redis_store_round_trips_every_role_and_awkward_content() {
    let Some((_sandbox, store)) = redis_store().await else {
        return;
    };
    run_round_trip_contract(&store).await;
}

/// A history store with a blocking API, wrapped for async use.
#[derive(Default)]
struct LegacyHistory(std::sync::Mutex<Vec<ChatMessage>>);

impl SyncHistoryStore for LegacyHistory {
    fn append(&self, _session_id: &str, message: ChatMessage) -> Result<(), HistoryStoreError> {
        self.0.lock().unwrap().push(message);
        Ok(())
    }

    fn get_history(
        &self,
        _session_id: &str,
        limit: usize,
    ) -> Result<Vec<ChatMessage>, HistoryStoreError> {
        let messages = self.0.lock().unwrap();
        Ok(messages[messages.len().saturating_sub(limit)..].to_vec())
    }
}

#[tokio::test]
async fn a_blocking_history_store_works_through_the_blocking_adapter() {
    let store = BlockingHistoryStore::new(LegacyHistory::default());
    store
        .append("s1", message(MessageRole::User, "hi", 0))
        .await
        .unwrap();
    assert_eq!(
        contents(&store.get_history("s1", 10).await.unwrap()),
        ["hi"]
    );
}
