//! How the CLI reaches a checkpoint log: through `datalium-strata-memory`'s
//! stores and their query port (`design/19-Async-Queryable-Store.md`), the
//! same code the Rust runtime writes with — so the CLI has no copy of the log
//! layout, the listing rules or the status names to keep in step.
//!
//! `--storage` is a SQLite file path (created if missing, and brought up to
//! date like any store opening it), a `postgres://` URL, or a `redis://` URL.
//! A Redis key prefix other than `strata` is given as `?prefix=<name>`.
//! Either runtime's logs read the same, since both write the shared layout.
//! Offloaded values (`design/41-Checkpoint-Efficiency.md`, "Offload") are
//! read from the store's own database, or from a filesystem blob store given
//! with `--blobs <dir>`.

use std::sync::Arc;

use datalium_strata::checkpoint::{
    CheckpointLogEntry, CheckpointQuery, CheckpointStore, MAX_PAGE_SIZE, ThreadLease, ThreadQuery,
    ThreadSummary,
};
use datalium_strata::schema::{
    Checkpoint, CheckpointStatus, CheckpointWait, GraphIdentity, WaitKind,
};
use datalium_strata_memory::checkpoint::SqliteCheckpointStore;
use datalium_strata_memory::postgres_store::PostgresCheckpointStore;
use datalium_strata_memory::redis_store::{DEFAULT_PREFIX, RedisCheckpointStore};
use datalium_strata_memory::{
    FileSystemBlobStore, Offload, PostgresStoreOptions, RedisStoreOptions, SqliteStoreOptions,
};

/// An open checkpoint log, whichever backend holds it.
pub struct Storage {
    store: Arc<dyn CheckpointStore>,
    query: Arc<dyn CheckpointQuery>,
}

/// Opens the log `storage` names: a path, `postgres://…` or `redis://…`.
pub async fn open(storage: &str) -> Result<Storage, String> {
    open_with(storage, None).await
}

/// [`open`], reading offloaded values from the filesystem blob store at
/// `blobs` instead of the store's own database. The CLI writes nothing, so no
/// value is offloaded by it whatever the threshold.
pub async fn open_with(storage: &str, blobs: Option<&str>) -> Result<Storage, String> {
    fn both<S: CheckpointStore + CheckpointQuery + 'static>(store: S) -> Storage {
        let store = Arc::new(store);
        Storage {
            store: store.clone(),
            query: store,
        }
    }

    let offload = blobs.map(|directory| {
        Offload::new()
            .with_threshold(usize::MAX)
            .with_blobs(Arc::new(FileSystemBlobStore::new(directory)))
    });
    let lower = storage.to_ascii_lowercase();
    if lower.starts_with("postgres://") || lower.starts_with("postgresql://") {
        let mut options = PostgresStoreOptions::new();
        if let Some(offload) = offload {
            options = options.with_offload(offload);
        }
        let store = PostgresCheckpointStore::connect_with(storage, options)
            .await
            .map_err(|e| e.0)?;
        Ok(both(store))
    } else if lower.starts_with("redis://") {
        let (url, prefix) = redis_prefix(storage);
        let mut options = RedisStoreOptions::new().with_prefix(prefix);
        if let Some(offload) = offload {
            options = options.with_offload(offload);
        }
        let store = RedisCheckpointStore::connect_with(&url, options)
            .await
            .map_err(|e| e.0)?;
        Ok(both(store))
    } else {
        let mut options = SqliteStoreOptions::new();
        if let Some(offload) = offload {
            options = options.with_offload(offload);
        }
        let store = SqliteCheckpointStore::open_with(storage, options).map_err(|e| e.0)?;
        Ok(both(store))
    }
}

/// Splits a `prefix` query parameter off a Redis URL.
fn redis_prefix(url: &str) -> (String, String) {
    let Some((base, query)) = url.split_once('?') else {
        return (url.to_string(), DEFAULT_PREFIX.to_string());
    };
    let mut prefix = DEFAULT_PREFIX.to_string();
    let rest: Vec<&str> = query
        .split('&')
        .filter(|pair| match pair.strip_prefix("prefix=") {
            Some(value) => {
                prefix = value.to_string();
                false
            }
            None => !pair.is_empty(),
        })
        .collect();
    let url = if rest.is_empty() {
        base.to_string()
    } else {
        format!("{base}?{}", rest.join("&"))
    };
    (url, prefix)
}

impl Storage {
    /// Every thread `query` matches, following its cursor to the end.
    pub async fn threads(&self, mut query: ThreadQuery) -> Result<Vec<ThreadSummary>, String> {
        let mut threads = Vec::new();
        loop {
            let page = self.query.list_threads(&query).await.map_err(|e| e.0)?;
            threads.extend(page.threads);
            match page.next_cursor {
                Some(cursor) => query = query.with_cursor(cursor),
                None => return Ok(threads),
            }
        }
    }

    /// Every checkpoint of `thread_id`, oldest first.
    pub async fn log(&self, thread_id: &str) -> Result<Vec<CheckpointLogEntry>, String> {
        let mut entries = Vec::new();
        let mut after = None;
        loop {
            let page = self
                .query
                .get_log(thread_id, after, MAX_PAGE_SIZE)
                .await
                .map_err(|e| e.0)?;
            entries.extend(page.entries);
            match page.next_after_sequence {
                Some(next) => after = Some(next),
                None => return Ok(entries),
            }
        }
    }

    /// The checkpoint at `sequence` in `thread_id`'s log.
    pub async fn checkpoint(
        &self,
        thread_id: &str,
        sequence: i64,
    ) -> Result<Option<Checkpoint>, String> {
        let page = self
            .query
            .get_log(thread_id, Some(sequence - 1), 1)
            .await
            .map_err(|e| e.0)?;
        Ok(page
            .entries
            .into_iter()
            .find(|entry| entry.sequence == sequence)
            .map(|entry| entry.checkpoint))
    }

    /// The lease `thread_id` is held under now (`design/32-Thread-Leases.md`),
    /// or `None` when nobody holds it.
    pub async fn lease(&self, thread_id: &str) -> Result<Option<ThreadLease>, String> {
        self.store.get_lease(thread_id).await.map_err(|e| e.0)
    }

    /// `owner` takes `thread_id`'s lease for `ttl`, if it is free. For seeding
    /// a held thread in tests: the CLI itself never writes.
    pub async fn hold(
        &self,
        thread_id: &str,
        owner: &str,
        ttl: std::time::Duration,
    ) -> Result<Option<ThreadLease>, String> {
        let attempt = self
            .store
            .try_acquire_lease(thread_id, owner, ttl)
            .await
            .map_err(|e| e.0)?;
        Ok(attempt.acquired())
    }

    /// Appends `checkpoint` to its thread's log and returns its sequence.
    /// For seeding a log in tests: the CLI itself never writes.
    pub async fn insert(&self, checkpoint: &Checkpoint) -> Result<i64, String> {
        self.store.save(checkpoint.clone()).await.map_err(|e| e.0)?;
        let log = self.log(&checkpoint.thread_id).await?;
        Ok(log.last().map_or(0, |entry| entry.sequence))
    }
}

/// The stored name of `status`.
pub fn status_name(status: CheckpointStatus) -> &'static str {
    match status {
        CheckpointStatus::Interrupted => "interrupted",
        CheckpointStatus::Completed => "completed",
        CheckpointStatus::Failed => "failed",
        CheckpointStatus::Cancelled => "cancelled",
        CheckpointStatus::Ended => "ended",
        CheckpointStatus::Running => "running",
    }
}

/// Where a thread frozen (or failed, or cancelled) inside a subgraph is, one
/// node per level from the outermost graph in — `Work/Approve` for a boundary
/// at `Approve` inside the subgraph node `Work`. `None` for a checkpoint with
/// no nested child.
pub fn waiting_path(checkpoint: &Checkpoint) -> Option<String> {
    checkpoint.nested.as_ref()?;
    Some(checkpoint.waiting_at().join("/"))
}

/// How to say the thread stopped where [`waiting_path`] says: a failed,
/// cancelled or ended thread isn't waiting for anyone, and neither is a
/// running one.
pub fn stopped_verb(status: CheckpointStatus) -> &'static str {
    match status {
        CheckpointStatus::Failed => "Failed",
        CheckpointStatus::Cancelled => "Cancelled",
        CheckpointStatus::Ended => "Ended",
        CheckpointStatus::Running => "Running",
        _ => "Waiting",
    }
}

/// Where a `running` thread is (`design/33-Durable-Execution.md`), as
/// `inspect` says it: `after 'Plan', next 'Execute'`, with the subgraph nodes
/// it is inside first (`in 'Work', after 'X', next 'Y'`), and `next 'Plan'`
/// alone before its first node. The next node is the one in flight if the
/// thread's process died. `None` for a thread that isn't running.
pub fn running_text(checkpoint: &Checkpoint) -> Option<String> {
    if checkpoint.status != CheckpointStatus::Running {
        return None;
    }
    let mut parts = Vec::new();
    let mut level = checkpoint;
    while let Some(child) = level.nested.as_deref() {
        parts.push(format!(
            "in '{}'",
            level.next_node.as_deref().unwrap_or("-")
        ));
        level = child;
    }
    if let Some(node) = &level.suspended_node {
        parts.push(format!("after '{node}'"));
    }
    parts.push(format!(
        "next '{}'",
        level.next_node.as_deref().unwrap_or("-")
    ));
    Some(parts.join(", "))
}

/// How many times the thread was recovered from the checkpoint it stands at
/// (`strata.recoveries`, `design/33-Durable-Execution.md`): recoveries since
/// a node last committed. `None` when it wasn't.
pub fn recoveries(checkpoint: &Checkpoint) -> Option<String> {
    checkpoint
        .metadata
        .as_ref()?
        .get("strata.recoveries")
        .cloned()
}

/// A lease as `inspect` says it: `held by 'worker-2' until
/// 2026-10-09T12:00:05Z (token 7)`, the expiry at whole seconds.
pub fn lease_text(lease: &ThreadLease) -> String {
    format!(
        "held by '{}' until {} (token {})",
        lease.owner,
        datalium_strata::store::timestamp_text(lease.expires_at),
        lease.token
    )
}

/// A lease in `--json`: `owner`, `token` and `expires_at`.
pub fn lease_json(lease: &ThreadLease) -> serde_json::Value {
    serde_json::json!({
        "owner": lease.owner,
        "token": lease.token,
        "expires_at": datalium_strata::store::timestamp_text(lease.expires_at),
    })
}

/// A graph's identity (`design/31-Graph-Identity.md`) as `incident version
/// 1`, or `incident unversioned`.
pub fn identity_text(identity: &GraphIdentity) -> String {
    match &identity.version {
        Some(version) => format!("{} version {version}", identity.id),
        None => format!("{} unversioned", identity.id),
    }
}

/// Which graph wrote each level that says (`design/31-Graph-Identity.md`),
/// from the outermost graph in: `(None, identity)` for the outermost level,
/// `(Some("Work"), identity)` for the level inside the subgraph node `Work`.
/// Empty for a thread whose levels have no identity.
pub fn graphs(checkpoint: &Checkpoint) -> Vec<(Option<String>, String)> {
    let mut lines = Vec::new();
    let mut prefix: Vec<String> = Vec::new();
    let mut level = Some(checkpoint);
    while let Some(current) = level {
        if let Some(identity) = &current.graph {
            let at = (!prefix.is_empty()).then(|| prefix.join("/"));
            lines.push((at, identity_text(identity)));
        }
        prefix.extend(current.suspended_node.iter().cloned());
        level = current.nested.as_deref();
    }
    lines
}

/// Every level's decision (`design/21-Typed-Human-Signals.md`), from the
/// outermost graph in, each rendered as `approved by alice at Work/Approve,
/// <decided_at>`: the boundary is named under the subgraph nodes leading to its
/// level. Empty when no level has one.
pub fn decisions(checkpoint: &Checkpoint) -> Vec<String> {
    let mut lines = Vec::new();
    let mut prefix: Vec<String> = Vec::new();
    let mut level = Some(checkpoint);
    while let Some(current) = level {
        if let Some(decision) = &current.decision {
            let mut path = prefix.clone();
            path.push(decision.boundary.clone());
            lines.push(format!(
                "{} by {} at {}, {}",
                if decision.is_approved {
                    "approved"
                } else {
                    "rejected"
                },
                decision.reviewer,
                path.join("/"),
                decision.decided_at
            ));
        }
        prefix.extend(current.suspended_node.iter().cloned());
        level = current.nested.as_deref();
    }
    lines
}

/// Why a cancelled thread was cancelled (`requested`, `deadline`, or
/// `timeout`; `design/17-Async-Execution.md`). `None` for any other.
pub fn cancel_reason(checkpoint: &Checkpoint) -> Option<String> {
    checkpoint.metadata.as_ref()?.get("cancel_reason").cloned()
}

/// What a waiting thread waits for (`design/29-Event-Waits-And-Timers.md`):
/// `event 'deployment.finished'`, `a timer`, `a human`.
pub fn wait_subject(wait: &CheckpointWait) -> String {
    match wait.kind {
        WaitKind::Event => format!("event '{}'", wait.event_type.as_deref().unwrap_or_default()),
        WaitKind::Timer => "a timer".to_string(),
        WaitKind::Human => "a human".to_string(),
    }
}

/// When a wait wakes (a timer) or times out (a human or an event), as an
/// `inspect` line; `None` for an event wait with no timeout.
pub fn wake_line(wait: &CheckpointWait) -> Option<String> {
    let at = wait.wake_at.as_deref()?;
    Some(match wait.kind {
        WaitKind::Timer => format!("Wakes at:        {at}"),
        _ => format!("Times out at:    {at}"),
    })
}

/// A wait in one cell of `thread list`: `timer until <t>`, `event x until <t>`,
/// `human until <t>`, or without `until` when it never times out.
pub fn describe_wait(wait: &CheckpointWait) -> String {
    let subject = match wait.kind {
        WaitKind::Event => format!("event {}", wait.event_type.as_deref().unwrap_or_default()),
        WaitKind::Timer => "timer".to_string(),
        WaitKind::Human => "human".to_string(),
    };
    match &wait.wake_at {
        Some(at) => format!("{subject} until {at}"),
        None => subject,
    }
}

/// Why an ended thread was ended (`strata.end_reason`: `requested`, or
/// `timed_out` for a wait that timed out with no route;
/// `design/28-Ending-A-Thread.md`, `design/29-Event-Waits-And-Timers.md`).
/// `None` for any other.
pub fn end_reason(checkpoint: &Checkpoint) -> Option<String> {
    checkpoint
        .metadata
        .as_ref()?
        .get("strata.end_reason")
        .cloned()
}

/// Who ended an ended thread (`strata.ended_by`), when the end said.
pub fn ended_by(checkpoint: &Checkpoint) -> Option<String> {
    checkpoint
        .metadata
        .as_ref()?
        .get("strata.ended_by")
        .cloned()
}

/// How many nodes the thread's outermost graph had started before the
/// checkpoint's next node (`strata.hops`, `design/24-Loop-Circuit-Breaker.md`).
/// `None` when the graph had no hop limit.
pub fn hops(checkpoint: &Checkpoint) -> Option<String> {
    checkpoint.metadata.as_ref()?.get("strata.hops").cloned()
}

/// The caller's metadata (`design/18-Run-Observability.md`), sorted by key:
/// every `metadata` entry except the engine's own keys (`error`, `error_kind`,
/// `attempts`, `cancel_reason`, and anything under `strata.`).
pub fn caller_metadata(checkpoint: &Checkpoint) -> Vec<(String, String)> {
    let mut entries: Vec<(String, String)> = checkpoint
        .metadata
        .iter()
        .flatten()
        .filter(|(key, _)| !is_engine_key(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    entries.sort();
    entries
}

/// The checkpoint `metadata` keys the engine writes or reserves, which caller
/// metadata can't use (`design/18-Run-Observability.md`, "Reserved keys").
fn is_engine_key(key: &str) -> bool {
    matches!(key, "error" | "error_kind" | "attempts" | "cancel_reason")
        || key.starts_with("strata.")
}

#[cfg(test)]
mod tests {
    use super::redis_prefix;

    #[test]
    fn a_redis_prefix_is_split_off_the_url() {
        assert_eq!(
            redis_prefix("redis://localhost:6379"),
            ("redis://localhost:6379".to_string(), "strata".to_string())
        );
        assert_eq!(
            redis_prefix("redis://localhost:6379/0?prefix=acme"),
            ("redis://localhost:6379/0".to_string(), "acme".to_string())
        );
        assert_eq!(
            redis_prefix("redis://localhost?protocol=resp3&prefix=acme"),
            (
                "redis://localhost?protocol=resp3".to_string(),
                "acme".to_string()
            )
        );
    }
}
