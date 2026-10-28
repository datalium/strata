//! Pluggable chat/session history persistence (`design/03-Architecture.md`,
//! `CLAUDE.md` item 6 — Memory & Persistence). [`HistoryStore`] is the port;
//! concrete adapters (starting with an in-memory store, then SQLite, per the
//! same ring-buffer-first progression `checkpoint.rs` documents for
//! [`crate::checkpoint::CheckpointStore`]) live in `strata-memory`, which
//! depends on this crate for the trait — not the other way around.
//!
//! `ChatMessage::content` is `Arc<str>`, not `String`: per design/03's
//! "Zero-Copy Advantage", a store handing back messages it already holds
//! only needs to bump a refcount, not perform a fresh heap allocation per
//! message. See `strata-memory`'s `history::tests` for a test that proves
//! this (not just asserts it by type signature) via `Arc::ptr_eq`.
//!
//! This is deliberately a distinct type from `strata-memory`'s
//! `schema::HistoryRecord` (the Phase 1 interop wire shape) — the same
//! core-local-type/interop-type split `checkpoint.rs`'s module doc and the
//! Phase 4 evaluation document for `Checkpoint`. `ChatMessage` is never
//! itself serialized to JSON; only a store adapter that needs an on-disk
//! representation (e.g. the SQLite adapter) converts to and from columns.

use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::store::{BoxFuture, on_blocking_pool};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageRole {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChatMessage {
    pub role: MessageRole,
    pub content: Arc<str>,
    pub timestamp: DateTime<Utc>,
}

/// The persistence port for chat/session history.
///
/// `get_history`'s `limit` returns at most the `limit` *most recent*
/// messages for `session_id`, but always in chronological (oldest-first)
/// order — the shape a caller needs to feed straight into an LLM context
/// window, matching `design/03-Architecture.md`'s `ctx.history().get_history(...)`
/// usage.
///
/// Async like [`crate::checkpoint::CheckpointStore`]
/// (`design/19-Async-Queryable-Store.md`): an async handler awaits it directly.
/// A store built on a blocking client implements [`SyncHistoryStore`] and is
/// wrapped in a [`BlockingHistoryStore`].
pub trait HistoryStore: Send + Sync {
    fn append<'a>(
        &'a self,
        session_id: &'a str,
        message: ChatMessage,
    ) -> BoxFuture<'a, Result<(), HistoryStoreError>>;

    fn get_history<'a>(
        &'a self,
        session_id: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<ChatMessage>, HistoryStoreError>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryStoreError(pub String);

impl std::fmt::Display for HistoryStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for HistoryStoreError {}

/// A history store with a blocking API: the shape [`HistoryStore`] had before
/// SG-11. Wrap one in [`BlockingHistoryStore`] to use it as a [`HistoryStore`].
pub trait SyncHistoryStore: Send + Sync + 'static {
    fn append(&self, session_id: &str, message: ChatMessage) -> Result<(), HistoryStoreError>;
    fn get_history(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<ChatMessage>, HistoryStoreError>;
}

/// Adapts a [`SyncHistoryStore`] to [`HistoryStore`] by running each call on
/// tokio's blocking pool.
pub struct BlockingHistoryStore<T>(Arc<T>);

impl<T: SyncHistoryStore> BlockingHistoryStore<T> {
    pub fn new(store: T) -> Self {
        Self(Arc::new(store))
    }

    /// Wraps a store the caller keeps a handle to.
    pub fn from_arc(store: Arc<T>) -> Self {
        Self(store)
    }

    /// The wrapped store.
    pub fn inner(&self) -> &Arc<T> {
        &self.0
    }
}

fn runtime_shut_down() -> HistoryStoreError {
    HistoryStoreError("the runtime shut down during a history store call".to_string())
}

impl<T: SyncHistoryStore> HistoryStore for BlockingHistoryStore<T> {
    fn append<'a>(
        &'a self,
        session_id: &'a str,
        message: ChatMessage,
    ) -> BoxFuture<'a, Result<(), HistoryStoreError>> {
        let (store, session_id) = (self.0.clone(), session_id.to_string());
        Box::pin(on_blocking_pool(
            move || store.append(&session_id, message),
            runtime_shut_down,
        ))
    }

    fn get_history<'a>(
        &'a self,
        session_id: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<ChatMessage>, HistoryStoreError>> {
        let (store, session_id) = (self.0.clone(), session_id.to_string());
        Box::pin(on_blocking_pool(
            move || store.get_history(&session_id, limit),
            runtime_shut_down,
        ))
    }
}
