//! `HistoryStore` adapters (`design/03-Architecture.md` / Phase 6): an
//! in-memory adapter first, then SQLite, per the same ring-buffer-first
//! progression `checkpoint.rs` followed for `CheckpointStore`.
//!
//! The Postgres and Redis adapters (built as a follow-on, after initial
//! parity) live in [`crate::postgres_store`] and
//! [`crate::redis_store`]. Nothing about [`HistoryStore`]'s shape assumes a
//! particular backend: adding them touched neither the trait nor either
//! adapter in this module.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use datalium_strata::BoxFuture;
use datalium_strata::history::{ChatMessage, HistoryStore, HistoryStoreError, MessageRole};

use crate::support::blocking;

/// A plain `Vec` per session behind a `Mutex` — the "in-memory ring buffer"
/// tier from `design/03-Architecture.md`'s memory blueprint. `get_history`
/// clones the returned `ChatMessage`s, but `ChatMessage::content` is an
/// `Arc<str>`, so that clone is a refcount bump, not a string copy (see
/// `tests::get_history_shares_the_same_arc_allocation_not_a_fresh_copy`
/// below).
#[derive(Default)]
pub struct InMemoryHistoryStore {
    sessions: Mutex<HashMap<String, Vec<ChatMessage>>>,
}

impl InMemoryHistoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

fn lock_poisoned(store_name: &str) -> HistoryStoreError {
    HistoryStoreError(format!("{store_name} lock poisoned"))
}

impl InMemoryHistoryStore {
    fn append_now(&self, session_id: &str, message: ChatMessage) -> Result<(), HistoryStoreError> {
        let mut guard = self
            .sessions
            .lock()
            .map_err(|_| lock_poisoned("in-memory history store"))?;
        guard
            .entry(session_id.to_string())
            .or_default()
            .push(message);
        Ok(())
    }

    fn history_now(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<ChatMessage>, HistoryStoreError> {
        let guard = self
            .sessions
            .lock()
            .map_err(|_| lock_poisoned("in-memory history store"))?;
        let Some(messages) = guard.get(session_id) else {
            return Ok(Vec::new());
        };
        let start = messages.len().saturating_sub(limit);
        Ok(messages[start..].to_vec())
    }
}

impl HistoryStore for InMemoryHistoryStore {
    fn append<'a>(
        &'a self,
        session_id: &'a str,
        message: ChatMessage,
    ) -> BoxFuture<'a, Result<(), HistoryStoreError>> {
        Box::pin(std::future::ready(self.append_now(session_id, message)))
    }

    fn get_history<'a>(
        &'a self,
        session_id: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<ChatMessage>, HistoryStoreError>> {
        Box::pin(std::future::ready(self.history_now(session_id, limit)))
    }
}

/// The "embedded" tier from `design/03-Architecture.md`'s memory blueprint,
/// backed by `rusqlite` (`bundled` feature — no system SQLite install
/// required). Each call runs on tokio's blocking pool, since rusqlite has no
/// async API (`design/19-Async-Queryable-Store.md`).
pub struct SqliteHistoryStore {
    conn: Arc<Mutex<rusqlite::Connection>>,
}

impl SqliteHistoryStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, HistoryStoreError> {
        let conn = rusqlite::Connection::open(path).map_err(sqlite_err)?;
        Self::from_connection(conn)
    }

    pub fn open_in_memory() -> Result<Self, HistoryStoreError> {
        let conn = rusqlite::Connection::open_in_memory().map_err(sqlite_err)?;
        Self::from_connection(conn)
    }

    fn from_connection(conn: rusqlite::Connection) -> Result<Self, HistoryStoreError> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS history_records (
                session_id TEXT NOT NULL,
                sequence   INTEGER NOT NULL,
                role       TEXT NOT NULL,
                content    TEXT NOT NULL,
                timestamp  TEXT NOT NULL,
                PRIMARY KEY (session_id, sequence)
             )",
        )
        .map_err(sqlite_err)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Runs `work` against the connection on the blocking pool.
    fn with_conn<R: Send + 'static>(
        &self,
        work: impl FnOnce(&rusqlite::Connection) -> Result<R, HistoryStoreError> + Send + 'static,
    ) -> BoxFuture<'static, Result<R, HistoryStoreError>> {
        let conn = self.conn.clone();
        Box::pin(blocking(
            move || {
                work(
                    &*conn
                        .lock()
                        .map_err(|_| lock_poisoned("sqlite history store"))?,
                )
            },
            HistoryStoreError,
        ))
    }
}

fn sqlite_err(err: rusqlite::Error) -> HistoryStoreError {
    HistoryStoreError(err.to_string())
}

fn role_to_str(role: MessageRole) -> &'static str {
    match role {
        MessageRole::System => "system",
        MessageRole::User => "user",
        MessageRole::Assistant => "assistant",
        MessageRole::Tool => "tool",
    }
}

fn role_from_str(role: &str) -> Result<MessageRole, HistoryStoreError> {
    match role {
        "system" => Ok(MessageRole::System),
        "user" => Ok(MessageRole::User),
        "assistant" => Ok(MessageRole::Assistant),
        "tool" => Ok(MessageRole::Tool),
        other => Err(HistoryStoreError(format!(
            "unknown role '{other}' in history store"
        ))),
    }
}

fn append_row(
    conn: &rusqlite::Connection,
    session_id: &str,
    message: &ChatMessage,
) -> Result<(), HistoryStoreError> {
    let next_sequence: i64 = conn
        .query_row(
            "SELECT COALESCE(MAX(sequence), -1) + 1 FROM history_records WHERE session_id = ?1",
            [session_id],
            |row| row.get(0),
        )
        .map_err(sqlite_err)?;

    conn.execute(
        "INSERT INTO history_records (session_id, sequence, role, content, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![
            session_id,
            next_sequence,
            role_to_str(message.role),
            message.content.as_ref(),
            message.timestamp.to_rfc3339(),
        ],
    )
    .map_err(sqlite_err)?;
    Ok(())
}

fn history_rows(
    conn: &rusqlite::Connection,
    session_id: &str,
    limit: usize,
) -> Result<Vec<ChatMessage>, HistoryStoreError> {
    let mut stmt = conn
        .prepare(
            "SELECT role, content, timestamp FROM history_records
                 WHERE session_id = ?1 ORDER BY sequence DESC LIMIT ?2",
        )
        .map_err(sqlite_err)?;

    let mut rows = stmt
        .query(rusqlite::params![session_id, limit as i64])
        .map_err(sqlite_err)?;

    let mut messages = Vec::new();
    while let Some(row) = rows.next().map_err(sqlite_err)? {
        let role_str: String = row.get(0).map_err(sqlite_err)?;
        let content: String = row.get(1).map_err(sqlite_err)?;
        let timestamp_str: String = row.get(2).map_err(sqlite_err)?;
        let timestamp: DateTime<Utc> = DateTime::parse_from_rfc3339(&timestamp_str)
            .map_err(|e| HistoryStoreError(e.to_string()))?
            .with_timezone(&Utc);
        messages.push(ChatMessage {
            role: role_from_str(&role_str)?,
            content: Arc::from(content.as_str()),
            timestamp,
        });
    }
    // Queried DESC (to LIMIT to the most recent rows); callers want
    // chronological order back.
    messages.reverse();
    Ok(messages)
}

impl HistoryStore for SqliteHistoryStore {
    fn append<'a>(
        &'a self,
        session_id: &'a str,
        message: ChatMessage,
    ) -> BoxFuture<'a, Result<(), HistoryStoreError>> {
        let session_id = session_id.to_string();
        self.with_conn(move |conn| append_row(conn, &session_id, &message))
    }

    fn get_history<'a>(
        &'a self,
        session_id: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<ChatMessage>, HistoryStoreError>> {
        let session_id = session_id.to_string();
        self.with_conn(move |conn| history_rows(conn, &session_id, limit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_history_shares_the_same_arc_allocation_not_a_fresh_copy() {
        let store = InMemoryHistoryStore::new();
        let content: Arc<str> = Arc::from("hello world");
        store
            .append(
                "s1",
                ChatMessage {
                    role: MessageRole::User,
                    content: Arc::clone(&content),
                    timestamp: Utc::now(),
                },
            )
            .await
            .unwrap();

        let history = store.get_history("s1", 10).await.unwrap();
        assert_eq!(history.len(), 1);
        assert!(
            Arc::ptr_eq(&history[0].content, &content),
            "get_history must hand back the same Arc<str> allocation, not a copy"
        );
    }

    #[tokio::test]
    async fn sqlite_store_round_trips_role_and_timestamp() {
        let store = SqliteHistoryStore::open_in_memory().unwrap();
        let timestamp = Utc::now();
        store
            .append(
                "s1",
                ChatMessage {
                    role: MessageRole::Tool,
                    content: Arc::from("42"),
                    timestamp,
                },
            )
            .await
            .unwrap();

        let history = store.get_history("s1", 10).await.unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].role, MessageRole::Tool);
        assert_eq!(history[0].content.as_ref(), "42");
        // SQLite round-trips through an RFC3339 string column; assert
        // second-level equality rather than exact struct equality, since
        // `DateTime<Utc>`'s sub-second precision isn't guaranteed to survive
        // the RFC3339 text round trip bit-for-bit.
        assert_eq!(history[0].timestamp.timestamp(), timestamp.timestamp());
    }
}
