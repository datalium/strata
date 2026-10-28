//! Plumbing shared by the adapters: the blocking-pool hop the rusqlite stores
//! use, the stored status names, and the in-code thread listing the in-memory
//! and Redis stores share (`design/19-Async-Queryable-Store.md`).

use std::sync::Arc;

use chrono::{DateTime, Utc};
use datalium_strata::checkpoint::{
    CheckpointStoreError, ThreadCursor, ThreadPage, ThreadQuery, ThreadSummary,
};
use datalium_strata::schema::{Checkpoint, CheckpointStatus};
use datalium_strata::store::updated_at_text;

/// Runs `work` on tokio's blocking pool and re-raises a panic from it here.
/// rusqlite has no async API, and a local file is the one place the blocking
/// pool is the right tool.
pub(crate) async fn blocking<R, E>(
    work: impl FnOnce() -> Result<R, E> + Send + 'static,
    error: impl FnOnce(String) -> E,
) -> Result<R, E>
where
    R: Send + 'static,
    E: Send + 'static,
{
    match tokio::task::spawn_blocking(work).await {
        Ok(result) => result,
        Err(join) if join.is_panic() => std::panic::resume_unwind(join.into_panic()),
        Err(_) => Err(error(
            "the runtime shut down during a store call".to_string(),
        )),
    }
}

/// The clock thread leases expire on in the stores that have none of their
/// own, the in-memory and SQLite ones (`design/32-Thread-Leases.md`, "The
/// clock"). The system clock unless a store is given another with its
/// `with_clock`, as a test does.
pub type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

pub(crate) fn system_clock() -> Clock {
    Arc::new(Utc::now)
}

/// The time `millis` Unix milliseconds are, as a store reads a lease's expiry.
pub(crate) fn from_millis(millis: i64) -> Result<DateTime<Utc>, CheckpointStoreError> {
    DateTime::from_timestamp_millis(millis)
        .ok_or_else(|| CheckpointStoreError(format!("bad lease expiry '{millis}' in the store")))
}

/// How many times an acquisition looks again at a lease that changed while it
/// was being read.
pub(crate) const ACQUIRE_ATTEMPTS: u32 = 3;

pub(crate) fn lease_kept_changing(thread_id: &str) -> CheckpointStoreError {
    CheckpointStoreError(format!(
        "the lease of thread '{thread_id}' kept changing while it was being acquired"
    ))
}

pub(crate) fn json_err(err: serde_json::Error) -> CheckpointStoreError {
    CheckpointStoreError(err.to_string())
}

pub(crate) fn poisoned() -> CheckpointStoreError {
    CheckpointStoreError("checkpoint store lock poisoned".to_string())
}

pub(crate) fn status_str(status: CheckpointStatus) -> &'static str {
    match status {
        CheckpointStatus::Interrupted => "interrupted",
        CheckpointStatus::Completed => "completed",
        CheckpointStatus::Failed => "failed",
        CheckpointStatus::Cancelled => "cancelled",
        CheckpointStatus::Ended => "ended",
        CheckpointStatus::Running => "running",
    }
}

pub(crate) fn parse_status(status: &str) -> Result<CheckpointStatus, CheckpointStoreError> {
    match status {
        "interrupted" => Ok(CheckpointStatus::Interrupted),
        "completed" => Ok(CheckpointStatus::Completed),
        "failed" => Ok(CheckpointStatus::Failed),
        "cancelled" => Ok(CheckpointStatus::Cancelled),
        "ended" => Ok(CheckpointStatus::Ended),
        "running" => Ok(CheckpointStatus::Running),
        other => Err(CheckpointStoreError(format!(
            "unknown checkpoint status '{other}' in the checkpoint log"
        ))),
    }
}

/// One page of `query` over every thread's latest checkpoint (with its
/// sequence), for stores that list in code. Exact: the cursor is `None` only
/// when nothing more matches.
pub(crate) fn page_from_latest(
    latest: impl IntoIterator<Item = (i64, Checkpoint)>,
    query: &ThreadQuery,
) -> Result<ThreadPage, CheckpointStoreError> {
    let cursor = query.validate()?;
    let mut matching: Vec<(i64, Checkpoint)> = latest
        .into_iter()
        .filter(|(_, checkpoint)| query.matches(checkpoint))
        .filter(|(_, checkpoint)| {
            cursor.as_ref().is_none_or(|cursor| {
                cursor.admits(updated_at_text(checkpoint), &checkpoint.thread_id)
            })
        })
        .collect();
    matching.sort_by(|(_, a), (_, b)| {
        (updated_at_text(b), &b.thread_id).cmp(&(updated_at_text(a), &a.thread_id))
    });
    let more = matching.len() > query.limit;
    matching.truncate(query.limit);
    let next_cursor = if more {
        matching.last().map(|(_, checkpoint)| {
            ThreadCursor::new(updated_at_text(checkpoint), &checkpoint.thread_id).encode()
        })
    } else {
        None
    };
    let threads = matching
        .iter()
        .map(|(sequence, checkpoint)| ThreadSummary::of(*sequence, checkpoint))
        .collect::<Result<_, _>>()?;
    Ok(ThreadPage {
        threads,
        next_cursor,
    })
}
