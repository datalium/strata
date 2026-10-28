//! In-memory and SQLite checkpoint stores: the first two tiers of
//! `design/03-Architecture.md`'s "in-memory ring buffer → SQLite/RocksDB →
//! Redis/PostgreSQL" progression. Both implement the async
//! [`CheckpointStore`], the [`CheckpointQuery`] port
//! (`design/19-Async-Queryable-Store.md`) and the [`CheckpointRetention`] port
//! (`design/34-Storage-Hardening.md`). Neither retries: they have no network.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use datalium_strata::BoxFuture;
use datalium_strata::checkpoint::{
    CheckpointLogEntry, CheckpointLogPage, CheckpointQuery, CheckpointRetention, CheckpointStore,
    CheckpointStoreError, LatestCheckpoint, LeaseAttempt, PurgeStates, ThreadCursor, ThreadLease,
    ThreadPage, ThreadQuery, ThreadSummary,
};
use datalium_strata::schema::{Checkpoint, GraphIdentity};
use datalium_strata::store::{
    LeasedSave, check_lease, check_lease_thread, check_lease_ttl, check_limit, lease_millis,
    timestamp_text, updated_at_text,
};
use rusqlite::{OptionalExtension, TransactionBehavior};

use crate::deltas::{
    self, CheckpointWriter, Decoded, Deltas, ENCODED_VERSIONS_SQL, Encoding, depth_of,
    listed_version,
};
use crate::group_commit::{Flush, GroupCommit, Queue, Row as FlushRow, Verdict, record_saved};
use crate::offload::{self, BlobStore, Offload};
use crate::retry::{Failure, FaultHook, fault_at};
use crate::schema_versions::{self, SchemaStep};
use crate::support::{
    ACQUIRE_ATTEMPTS, Clock, blocking, from_millis, json_err, lease_kept_changing,
    page_from_latest, parse_status, poisoned, status_str, system_clock,
};

/// Keeps every thread's whole checkpoint log in memory, like the persistent
/// stores keep it on disk, so it answers [`CheckpointQuery::get_log`] like
/// they do. It grows with every save; it is for tests and development.
///
/// Thread leases (`design/32-Thread-Leases.md`) are kept beside the logs and
/// expire on the system clock, or on the one [`Self::with_clock`] gives it.
pub struct InMemoryCheckpointStore {
    logs: Mutex<HashMap<String, Vec<Checkpoint>>>,
    // Every lease method, and every save under a lease, runs under this one
    // lock, so a save's token check and its append can't be split by an
    // acquisition.
    leases: Mutex<HashMap<String, LeaseRow>>,
    clock: Clock,
}

/// A thread's lease: its latest token, and who holds it until when (`None`
/// once released).
struct LeaseRow {
    token: i64,
    holder: Option<(String, DateTime<Utc>)>,
}

impl LeaseRow {
    /// The lease the thread is held under at `now`, if it is.
    fn held_at(&self, thread_id: &str, now: DateTime<Utc>) -> Option<ThreadLease> {
        let (owner, expires_at) = self.holder.as_ref()?;
        (*expires_at > now).then(|| ThreadLease {
            thread_id: thread_id.to_string(),
            owner: owner.clone(),
            token: self.token,
            expires_at: *expires_at,
        })
    }
}

/// `ttl` from `now`, as a lease's expiry.
fn expiry(now: DateTime<Utc>, ttl: Duration) -> DateTime<Utc> {
    now + chrono::Duration::milliseconds(lease_millis(ttl))
}

impl Default for InMemoryCheckpointStore {
    fn default() -> Self {
        Self {
            logs: Mutex::default(),
            leases: Mutex::default(),
            clock: system_clock(),
        }
    }
}

impl InMemoryCheckpointStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Expires leases on `clock` instead of the system clock: how a test makes
    /// time pass.
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    fn try_acquire_lease_now(
        &self,
        thread_id: &str,
        owner: &str,
        ttl: Duration,
    ) -> Result<LeaseAttempt, CheckpointStoreError> {
        check_lease(owner, ttl)?;
        let mut leases = self.leases.lock().map_err(|_| poisoned())?;
        let now = (self.clock)();
        let row = leases.get(thread_id);
        if let Some(held) = row.and_then(|row| row.held_at(thread_id, now)) {
            return Ok(LeaseAttempt::Held(held));
        }
        let lease = ThreadLease {
            thread_id: thread_id.to_string(),
            owner: owner.to_string(),
            token: row.map_or(0, |row| row.token) + 1,
            expires_at: expiry(now, ttl),
        };
        leases.insert(
            thread_id.to_string(),
            LeaseRow {
                token: lease.token,
                holder: Some((lease.owner.clone(), lease.expires_at)),
            },
        );
        Ok(LeaseAttempt::Acquired(lease))
    }

    fn renew_lease_now(
        &self,
        lease: &ThreadLease,
        ttl: Duration,
    ) -> Result<Option<ThreadLease>, CheckpointStoreError> {
        check_lease_ttl(ttl)?;
        let mut leases = self.leases.lock().map_err(|_| poisoned())?;
        let Some(row) = leases
            .get_mut(&lease.thread_id)
            .filter(|row| row.token == lease.token)
        else {
            return Ok(None);
        };
        let renewed = ThreadLease {
            expires_at: expiry((self.clock)(), ttl),
            ..lease.clone()
        };
        row.holder = Some((renewed.owner.clone(), renewed.expires_at));
        Ok(Some(renewed))
    }

    fn release_lease_now(&self, lease: &ThreadLease) -> Result<bool, CheckpointStoreError> {
        let mut leases = self.leases.lock().map_err(|_| poisoned())?;
        let Some(row) = leases
            .get_mut(&lease.thread_id)
            .filter(|row| row.token == lease.token)
        else {
            return Ok(false);
        };
        row.holder = None;
        Ok(true)
    }

    fn get_lease_now(&self, thread_id: &str) -> Result<Option<ThreadLease>, CheckpointStoreError> {
        let leases = self.leases.lock().map_err(|_| poisoned())?;
        Ok(leases
            .get(thread_id)
            .and_then(|row| row.held_at(thread_id, (self.clock)())))
    }

    /// Checks the token and appends under the lease lock, so no acquisition
    /// can come between them.
    fn save_under_lease_now(
        &self,
        checkpoint: Checkpoint,
        lease: &ThreadLease,
        expected: Option<i64>,
    ) -> Result<LeasedSave, CheckpointStoreError> {
        check_lease_thread(&checkpoint, lease)?;
        let leases = self.leases.lock().map_err(|_| poisoned())?;
        if leases.get(&lease.thread_id).map(|row| row.token) != Some(lease.token) {
            return Ok(LeasedSave::LeaseLost);
        }
        Ok(if self.save_if_latest_now(checkpoint, expected)? {
            LeasedSave::Saved
        } else {
            LeasedSave::NotLatest
        })
    }

    fn save_now(&self, checkpoint: Checkpoint) -> Result<(), CheckpointStoreError> {
        let mut logs = self.logs.lock().map_err(|_| poisoned())?;
        logs.entry(checkpoint.thread_id.clone())
            .or_default()
            .push(checkpoint);
        Ok(())
    }

    fn load_now(&self, thread_id: &str) -> Result<Option<Checkpoint>, CheckpointStoreError> {
        let logs = self.logs.lock().map_err(|_| poisoned())?;
        Ok(logs.get(thread_id).and_then(|log| log.last().cloned()))
    }

    fn load_latest_now(
        &self,
        thread_id: &str,
    ) -> Result<Option<LatestCheckpoint>, CheckpointStoreError> {
        let logs = self.logs.lock().map_err(|_| poisoned())?;
        Ok(logs.get(thread_id).and_then(|log| {
            log.last().map(|checkpoint| LatestCheckpoint {
                checkpoint: checkpoint.clone(),
                sequence: Some(log.len() as i64 - 1),
            })
        }))
    }

    /// Appends under the lock if the log's last sequence is still `expected`.
    fn save_if_latest_now(
        &self,
        checkpoint: Checkpoint,
        expected: Option<i64>,
    ) -> Result<bool, CheckpointStoreError> {
        let mut logs = self.logs.lock().map_err(|_| poisoned())?;
        let log = logs.entry(checkpoint.thread_id.clone()).or_default();
        if expected.is_some_and(|expected| log.len() as i64 - 1 != expected) {
            return Ok(false);
        }
        log.push(checkpoint);
        Ok(true)
    }

    /// Removes the logs of threads that are done with
    /// (`design/34-Storage-Hardening.md`, "Retention"), oldest first, with
    /// their lease rows, under the lease lock and then the logs': the order a
    /// save under a lease takes them, so nothing changes in between.
    fn purge_now(
        &self,
        older_than: DateTime<Utc>,
        states: &PurgeStates,
        limit: usize,
    ) -> Result<usize, CheckpointStoreError> {
        check_limit(limit)?;
        let before = timestamp_text(older_than);
        let now = (self.clock)();
        let mut leases = self.leases.lock().map_err(|_| poisoned())?;
        let mut logs = self.logs.lock().map_err(|_| poisoned())?;
        let mut picked: Vec<(String, String)> = logs
            .iter()
            .filter_map(|(thread_id, log)| {
                let latest = log.last()?;
                // A held thread isn't a candidate, so it takes no place in
                // the limit.
                let held = leases
                    .get(thread_id)
                    .is_some_and(|lease| lease.held_at(thread_id, now).is_some());
                (!held && states.takes(latest, &before))
                    .then(|| (updated_at_text(latest).to_string(), thread_id.clone()))
            })
            .collect();
        picked.sort();
        picked.truncate(limit);
        for (_, thread_id) in &picked {
            logs.remove(thread_id);
            leases.remove(thread_id);
        }
        Ok(picked.len())
    }
}

impl CheckpointStore for InMemoryCheckpointStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        Box::pin(std::future::ready(self.save_now(checkpoint)))
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        Box::pin(std::future::ready(self.load_now(thread_id)))
    }

    fn load_latest<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<LatestCheckpoint>, CheckpointStoreError>> {
        Box::pin(std::future::ready(self.load_latest_now(thread_id)))
    }

    fn save_if_latest(
        &self,
        checkpoint: Checkpoint,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'_, Result<bool, CheckpointStoreError>> {
        Box::pin(std::future::ready(
            self.save_if_latest_now(checkpoint, expected_sequence),
        ))
    }

    fn try_acquire_lease<'a>(
        &'a self,
        thread_id: &'a str,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<LeaseAttempt, CheckpointStoreError>> {
        Box::pin(std::future::ready(
            self.try_acquire_lease_now(thread_id, owner, ttl),
        ))
    }

    fn renew_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        Box::pin(std::future::ready(self.renew_lease_now(lease, ttl)))
    }

    fn release_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
    ) -> BoxFuture<'a, Result<bool, CheckpointStoreError>> {
        Box::pin(std::future::ready(self.release_lease_now(lease)))
    }

    fn get_lease<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        Box::pin(std::future::ready(self.get_lease_now(thread_id)))
    }

    fn save_under_lease<'a>(
        &'a self,
        checkpoint: Checkpoint,
        lease: &'a ThreadLease,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'a, Result<LeasedSave, CheckpointStoreError>> {
        Box::pin(std::future::ready(self.save_under_lease_now(
            checkpoint,
            lease,
            expected_sequence,
        )))
    }
}

impl CheckpointQuery for InMemoryCheckpointStore {
    fn list_threads<'a>(
        &'a self,
        query: &'a ThreadQuery,
    ) -> BoxFuture<'a, Result<ThreadPage, CheckpointStoreError>> {
        let page = self.logs.lock().map_err(|_| poisoned()).and_then(|logs| {
            let latest = logs.values().filter_map(|log| {
                let checkpoint = log.last()?.clone();
                Some((log.len() as i64 - 1, checkpoint))
            });
            page_from_latest(latest.collect::<Vec<_>>(), query)
        });
        Box::pin(std::future::ready(page))
    }

    fn get_log<'a>(
        &'a self,
        thread_id: &'a str,
        after_sequence: Option<i64>,
        limit: usize,
    ) -> BoxFuture<'a, Result<CheckpointLogPage, CheckpointStoreError>> {
        let page = check_limit(limit).and_then(|()| {
            let logs = self.logs.lock().map_err(|_| poisoned())?;
            let start = after_sequence.map_or(0, |after| (after + 1).max(0) as usize);
            let entries = logs
                .get(thread_id)
                .map(|log| {
                    log.iter()
                        .enumerate()
                        .skip(start)
                        .take(limit)
                        .map(|(sequence, checkpoint)| CheckpointLogEntry {
                            sequence: sequence as i64,
                            checkpoint: checkpoint.clone(),
                        })
                        .collect()
                })
                .unwrap_or_default();
            Ok(CheckpointLogPage::new(entries, limit))
        });
        Box::pin(std::future::ready(page))
    }
}

impl CheckpointRetention for InMemoryCheckpointStore {
    fn purge<'a>(
        &'a self,
        older_than: DateTime<Utc>,
        states: &'a PurgeStates,
        limit: usize,
    ) -> BoxFuture<'a, Result<usize, CheckpointStoreError>> {
        Box::pin(std::future::ready(
            self.purge_now(older_than, states, limit),
        ))
    }
}

/// The canonical `checkpoints` table (see `schema/README.md`, "SQLite
/// checkpoint log"). This exact layout is what `Datalium.Strata.Storage`'s
/// `EfCoreCheckpointStore` creates too, so a database file written by either
/// runtime can be read by the other and by the `strata` CLI (which reads
/// through this store). Keep the two in lockstep — each language's tests
/// compare the created table against `schema/fixtures/checkpoint-log/columns.json`.
pub const CHECKPOINT_LOG_DDL: &str = "CREATE TABLE IF NOT EXISTS checkpoints (
    thread_id      TEXT NOT NULL,
    sequence       INTEGER NOT NULL,
    schema_version TEXT NOT NULL,
    status         TEXT NOT NULL,
    suspended_node TEXT,
    next_node      TEXT,
    frozen_state   TEXT NOT NULL,
    metadata       TEXT,
    created_at     TEXT NOT NULL,
    resumed_at     TEXT,
    nested         TEXT,
    decision       TEXT,
    wait           TEXT,
    wake_at        TEXT,
    graph_id       TEXT,
    graph_version  TEXT,
    PRIMARY KEY (thread_id, sequence)
)";

/// The head table that makes listing threads cheap
/// (`design/19-Async-Queryable-Store.md`, "SQL: a trigger-maintained head
/// table"): one row per thread, pointing at its latest checkpoint, kept current
/// by a trigger on `checkpoints` — so every writer keeps it right, whichever
/// runtime, version or tool it is. Identical to `EfCoreCheckpointStore.CheckpointThreadsDdl`;
/// pinned by `schema/fixtures/checkpoint-log/threads-columns.json` and
/// `threads.rows.json`.
pub const CHECKPOINT_THREADS_DDL: &str = "CREATE TABLE IF NOT EXISTS checkpoint_threads (
    thread_id  TEXT NOT NULL PRIMARY KEY,
    sequence   INTEGER NOT NULL,
    state      TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    metadata      TEXT,
    wake_at       TEXT,
    graph_id      TEXT,
    graph_version TEXT
);
CREATE INDEX IF NOT EXISTS checkpoint_threads_by_state
    ON checkpoint_threads (state, updated_at, thread_id);
CREATE INDEX IF NOT EXISTS checkpoint_threads_by_updated
    ON checkpoint_threads (updated_at, thread_id);
CREATE INDEX IF NOT EXISTS checkpoint_threads_by_wake
    ON checkpoint_threads (wake_at);
CREATE INDEX IF NOT EXISTS checkpoint_threads_by_graph
    ON checkpoint_threads (graph_id, updated_at, thread_id);
CREATE TRIGGER IF NOT EXISTS checkpoints_head AFTER INSERT ON checkpoints
BEGIN
    INSERT INTO checkpoint_threads (thread_id, sequence, state, updated_at, metadata, wake_at, graph_id, graph_version)
    VALUES (NEW.thread_id, NEW.sequence,
            CASE WHEN NEW.resumed_at IS NOT NULL THEN 'resumed' ELSE NEW.status END,
            COALESCE(NEW.resumed_at, NEW.created_at), NEW.metadata, NEW.wake_at, NEW.graph_id, NEW.graph_version)
    ON CONFLICT (thread_id) DO UPDATE SET
        sequence = excluded.sequence,
        state = excluded.state,
        updated_at = excluded.updated_at,
        metadata = excluded.metadata,
        wake_at = excluded.wake_at,
        graph_id = excluded.graph_id,
        graph_version = excluded.graph_version
    WHERE excluded.sequence > checkpoint_threads.sequence;
END;";

/// Fills a just-created `checkpoint_threads` from the log it was added to.
/// Identical to `EfCoreCheckpointStore.CheckpointThreadsBackfill`.
pub const CHECKPOINT_THREADS_BACKFILL: &str =
    "INSERT INTO checkpoint_threads (thread_id, sequence, state, updated_at, metadata, wake_at, graph_id, graph_version)
SELECT c.thread_id, c.sequence,
       CASE WHEN c.resumed_at IS NOT NULL THEN 'resumed' ELSE c.status END,
       COALESCE(c.resumed_at, c.created_at), c.metadata, c.wake_at, c.graph_id, c.graph_version
FROM checkpoints c
JOIN (SELECT thread_id, MAX(sequence) AS latest FROM checkpoints GROUP BY thread_id) l
  ON c.thread_id = l.thread_id AND c.sequence = l.latest";

/// The thread leases (`design/32-Thread-Leases.md`): one row per thread that
/// ever had one, kept until the thread is purged, so `token` only grows. A
/// thread is held while
/// `expires_at` (Unix milliseconds on the store's clock) is set and in the
/// future; a release clears it and `owner`. Identical to
/// `EfCoreCheckpointStore.CheckpointLeasesDdl`; pinned by
/// `schema/fixtures/checkpoint-log/leases-columns.json`.
pub const CHECKPOINT_LEASES_DDL: &str = "CREATE TABLE IF NOT EXISTS checkpoint_leases (
    thread_id  TEXT NOT NULL PRIMARY KEY,
    owner      TEXT,
    token      INTEGER NOT NULL,
    expires_at INTEGER
)";

/// SQLite-backed [`CheckpointStore`] — the "SQLite/RocksDB" tier from
/// `design/03-Architecture.md`'s memory blueprint, one step up from
/// [`InMemoryCheckpointStore`]. Backed by `rusqlite` (`bundled` feature, no
/// system SQLite install required); each call runs on tokio's blocking pool,
/// since rusqlite has no async API.
///
/// The database is a language-agnostic **append-only checkpoint log**, one
/// row per `save` at the next `sequence` for its `thread_id`, with each
/// `Checkpoint` field in its own column (`frozen_state` and `metadata` as
/// JSON text), plus the `checkpoint_threads` head table a trigger keeps
/// current. [`CheckpointStore::load`] returns the highest-sequence row; the
/// earlier ones stay on disk, which is what [`CheckpointQuery::get_log`] and
/// `strata checkpoint log`/`diff`/`replay` read, and what lets the .NET
/// runtime's `EfCoreCheckpointStore` read a file this store wrote (and vice
/// versa).
///
/// Assumes a single writer at a time: `save` computes the next sequence in
/// the same statement that inserts it, so two processes racing on one thread
/// would collide on the primary key rather than corrupt anything, but no
/// retry is attempted.
///
/// Thread leases (`design/32-Thread-Leases.md`) are a `checkpoint_leases`
/// table in the same file, expiring on the system clock, or on the one
/// [`Self::with_clock`] gives it: SQLite has no clock of its own that every
/// process sharing the file would agree on better than this.
///
/// What the file holds is recorded in its `strata_schema_version` table
/// ([`crate::schema_versions`], `design/34-Storage-Hardening.md`): opening a
/// file brings it up to date, and a file a newer build has moved on is
/// refused.
pub struct SqliteCheckpointStore {
    conn: Arc<Mutex<rusqlite::Connection>>,
    clock: Clock,
    group: Option<Queue>,
    writer: Option<Arc<CheckpointWriter>>,
    /// Where offloaded values go when not in this file.
    blobs: Option<Arc<dyn BlobStore>>,
}

/// Options of [`SqliteCheckpointStore`]. Mirrors the .NET `SqliteStoreOptions`.
#[derive(Clone, Default)]
pub struct SqliteStoreOptions {
    group_commit: Option<GroupCommit>,
    deltas: Option<Deltas>,
    offload: Option<Offload>,
    fault: Option<FaultHook>,
}

impl std::fmt::Debug for SqliteStoreOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteStoreOptions")
            .field("group_commit", &self.group_commit)
            .field("deltas", &self.deltas)
            .field("offload", &self.offload)
            .finish_non_exhaustive()
    }
}

impl SqliteStoreOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Flushes saves that arrive while another flush runs together, in one
    /// transaction and one sync to disk (`design/41-Checkpoint-Efficiency.md`).
    /// Off by default. A SQLite store runs one flush at a time, whatever
    /// [`GroupCommit::with_max_concurrent_flushes`] says.
    pub fn with_group_commit(mut self, group_commit: GroupCommit) -> Self {
        self.group_commit = Some(group_commit);
        self
    }

    /// The group commit the store runs, if any.
    pub fn group_commit(&self) -> Option<&GroupCommit> {
        self.group_commit.as_ref()
    }

    /// Stores a `running` checkpoint as the changes to the thread's previous
    /// row, with a whole row every [`Deltas::with_full_every`]
    /// (`design/41-Checkpoint-Efficiency.md`, "Deltas"). Off by default. A
    /// store reads rows stored this way whatever its own options say.
    pub fn with_deltas(mut self, deltas: Deltas) -> Self {
        self.deltas = Some(deltas);
        self
    }

    /// The deltas the store writes, if any.
    pub fn deltas(&self) -> Option<&Deltas> {
        self.deltas.as_ref()
    }

    /// Stores each top-level value of a level's state whose JSON is at least
    /// [`Offload::with_threshold`] bytes once, in a blob store, and refers to
    /// it (`design/41-Checkpoint-Efficiency.md`, "Offload"). Off by default.
    /// The blobs are read from where these options say, whatever the
    /// threshold: this file's `checkpoint_blobs` unless
    /// [`Offload::with_blobs`] names another store.
    pub fn with_offload(mut self, offload: Offload) -> Self {
        self.offload = Some(offload);
        self
    }

    /// The offload the store does, if any.
    pub fn offload(&self) -> Option<&Offload> {
        self.offload.as_ref()
    }

    /// For tests: `fault` is called as a group-commit flush starts
    /// (`"flush:start"`), where it may wait, or return `true` to fail the
    /// flush as a lost connection would.
    #[doc(hidden)]
    pub fn with_fault(mut self, fault: impl Fn(&str) -> bool + Send + Sync + 'static) -> Self {
        self.fault = Some(Arc::new(fault));
        self
    }
}

impl SqliteCheckpointStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CheckpointStoreError> {
        let conn = rusqlite::Connection::open(path).map_err(sqlite_err)?;
        Self::from_connection(conn)
    }

    pub fn open_in_memory() -> Result<Self, CheckpointStoreError> {
        let conn = rusqlite::Connection::open_in_memory().map_err(sqlite_err)?;
        Self::from_connection(conn)
    }

    /// [`open`](Self::open) with `options`.
    pub fn open_with(
        path: impl AsRef<Path>,
        options: SqliteStoreOptions,
    ) -> Result<Self, CheckpointStoreError> {
        let conn = rusqlite::Connection::open(path).map_err(sqlite_err)?;
        Self::from_connection_with(conn, &[])?.with_options(options)
    }

    /// [`open_in_memory`](Self::open_in_memory) with `options`.
    pub fn open_in_memory_with(options: SqliteStoreOptions) -> Result<Self, CheckpointStoreError> {
        Self::open_in_memory()?.with_options(options)
    }

    fn with_options(mut self, options: SqliteStoreOptions) -> Result<Self, CheckpointStoreError> {
        self.writer =
            CheckpointWriter::new(options.deltas.as_ref(), options.offload.as_ref())?.map(Arc::new);
        self.blobs = options
            .offload
            .as_ref()
            .and_then(|offload| offload.blobs().cloned());
        if let Some(group_commit) = &options.group_commit {
            group_commit.check()?;
            let (conn, fault, writer, blobs) = (
                self.conn.clone(),
                options.fault.clone(),
                self.writer.clone(),
                self.blobs.clone(),
            );
            let flush: Flush = Arc::new(move |rows| {
                let (conn, fault, writer, blobs) =
                    (conn.clone(), fault.clone(), writer.clone(), blobs.clone());
                Box::pin(async move {
                    // A thread with two saves in one flush has both written
                    // whole, as in the Postgres store. Blobs elsewhere are all
                    // written before the transaction, so a row written whole in
                    // it needs none that isn't there.
                    let encodings: Vec<Encoding> = {
                        let alone =
                            deltas::alone(rows.iter().map(|row| row.checkpoint.thread_id.as_str()));
                        rows.iter()
                            .map(|row| {
                                writer.as_ref().map_or_else(Encoding::plain, |writer| {
                                    writer.prepare(
                                        &row.checkpoint,
                                        alone.contains(row.checkpoint.thread_id.as_str()),
                                        blobs.is_some(),
                                    )
                                })
                            })
                            .collect()
                    };
                    if let Some(blobs) = &blobs {
                        for (row, encoding) in rows.iter().zip(&encodings) {
                            offload::put_all(&**blobs, &row.checkpoint.thread_id, &encoding.blobs)
                                .await?;
                        }
                    }
                    let own = blobs.is_none();
                    blocking(
                        move || flush_rows(&conn, &fault, &rows, encodings, writer.as_deref(), own),
                        Failure::fatal,
                    )
                    .await
                })
            });
            // One connection, one writer: one flush at a time.
            self.group = Some(Queue::new(group_commit, "sqlite", flush, Some(1)));
        }
        Ok(self)
    }

    /// Whether saves go through group commit.
    #[doc(hidden)]
    pub fn group_commits(&self) -> bool {
        self.group.is_some()
    }

    /// Whether running checkpoints are stored as deltas.
    #[doc(hidden)]
    pub fn writes_deltas(&self) -> bool {
        self.writer
            .as_ref()
            .is_some_and(|writer| writer.writes_deltas())
    }

    /// Whether large values are offloaded, and whether to another blob store
    /// than this file.
    #[doc(hidden)]
    pub fn offloads(&self) -> (bool, bool) {
        (
            self.writer.as_ref().is_some_and(|writer| writer.offloads()),
            self.blobs.is_some(),
        )
    }

    /// `row` through group commit's queue, as a fenced save's outcome.
    async fn queued(
        &self,
        group: &Queue,
        row: FlushRow,
    ) -> Result<LeasedSave, CheckpointStoreError> {
        Ok(match group.save(row).await? {
            Verdict::Saved => LeasedSave::Saved,
            Verdict::Lost => LeasedSave::NotLatest,
            Verdict::LeaseLost => LeasedSave::LeaseLost,
        })
    }

    /// For tests: opens `path` as a build that also knows `steps` would.
    #[doc(hidden)]
    pub fn open_with_steps(
        path: impl AsRef<Path>,
        steps: &[SchemaStep],
    ) -> Result<Self, CheckpointStoreError> {
        let conn = rusqlite::Connection::open(path).map_err(sqlite_err)?;
        Self::from_connection_with(conn, steps)
    }

    fn from_connection(conn: rusqlite::Connection) -> Result<Self, CheckpointStoreError> {
        Self::from_connection_with(conn, &[])
    }

    fn from_connection_with(
        mut conn: rusqlite::Connection,
        steps: &[SchemaStep],
    ) -> Result<Self, CheckpointStoreError> {
        migrate(&mut conn, steps)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            clock: system_clock(),
            group: None,
            writer: None,
            blobs: None,
        })
    }

    /// Expires leases on `clock` instead of the system clock: how a test makes
    /// time pass.
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// Now on the store's clock, in the Unix milliseconds `expires_at` holds.
    fn now_millis(&self) -> i64 {
        (self.clock)().timestamp_millis()
    }

    /// Runs `work` against the connection on the blocking pool.
    fn with_conn<R: Send + 'static>(
        &self,
        work: impl FnOnce(&rusqlite::Connection) -> Result<R, CheckpointStoreError> + Send + 'static,
    ) -> BoxFuture<'static, Result<R, CheckpointStoreError>> {
        let conn = self.conn.clone();
        Box::pin(blocking(
            move || work(&*conn.lock().map_err(|_| poisoned())?),
            CheckpointStoreError,
        ))
    }

    /// Appends `checkpoint` (see [`insert_sized`]), stored as a delta of the
    /// thread's latest row, or with its large values offloaded, when the store
    /// can, and whole, with every blob written again, when the row it relied
    /// on is no longer the latest. Blobs in this file are written in the
    /// row's transaction; blobs elsewhere, before it. Records what it wrote;
    /// says whether it did.
    async fn insert(
        &self,
        checkpoint: Checkpoint,
        expected: Option<i64>,
        token: Option<i64>,
    ) -> Result<bool, CheckpointStoreError> {
        let mut encoding = Arc::new(self.writer.as_ref().map_or_else(Encoding::plain, |writer| {
            writer.prepare(&checkpoint, true, false)
        }));
        let checkpoint = Arc::new(checkpoint);
        let own = self.blobs.is_none();
        let append = |encoding: Arc<Encoding>| {
            let checkpoint = checkpoint.clone();
            async move {
                if let Some(blobs) = &self.blobs {
                    offload::put_all(&**blobs, &checkpoint.thread_id, &encoding.blobs).await?;
                }
                self.with_conn(move |conn| {
                    append_row(conn, &checkpoint, &encoding, expected, token, own)
                })
                .await
            }
        };
        let mut inserted = append(encoding.clone()).await?;
        if inserted.is_none() && encoding.is_based() {
            encoding = Arc::new(encoding.whole(&checkpoint));
            inserted = append(encoding.clone()).await?;
        }
        match inserted {
            Some((sequence, bytes)) => {
                record_saved("sqlite", &encoding, bytes);
                if let Some(writer) = &self.writer {
                    writer.saved(&checkpoint, &encoding, sequence, &checkpoint.created_at);
                }
                Ok(true)
            }
            None => {
                if let Some(writer) = &self.writer {
                    writer.forget(&checkpoint.thread_id);
                }
                Ok(false)
            }
        }
    }

    /// `thread_id`'s latest row, decoded, its offloaded values read, with its
    /// sequence; the store then knows it as the thread's latest.
    async fn latest(
        &self,
        thread_id: &str,
    ) -> Result<Option<(i64, Checkpoint)>, CheckpointStoreError> {
        let (owned, own) = (thread_id.to_string(), self.blobs.is_none());
        let Some((sequence, decoded, depth)) = self
            .with_conn(move |conn| latest_row(conn, &owned, own))
            .await?
        else {
            return Ok(None);
        };
        let Some(decoded) = self.resolve(thread_id, vec![decoded]).await?.pop() else {
            return Ok(None);
        };
        if let Some(writer) = &self.writer {
            writer.loaded(sequence, depth, &decoded, &decoded.checkpoint.created_at);
        }
        Ok(Some((sequence, decoded.checkpoint)))
    }

    /// Reads the offloaded values `decoded` refers to from the blob store the
    /// options name; those in this file were read with the rows.
    async fn resolve(
        &self,
        thread_id: &str,
        decoded: Vec<Decoded>,
    ) -> Result<Vec<Decoded>, CheckpointStoreError> {
        match &self.blobs {
            Some(blobs) => {
                let wanted = deltas::wanted(&decoded);
                if wanted.is_empty() {
                    return Ok(decoded);
                }
                let found = offload::read(&**blobs, thread_id, &wanted).await?;
                deltas::resolve(decoded, &found)
            }
            None => Ok(decoded),
        }
    }
}

/// Brings the checkpoint component up to date in one immediate transaction,
/// so concurrent first opens can't interleave
/// (`design/34-Storage-Hardening.md`, "Migrations"): reads its version from
/// `strata_schema_version` (no row: 0), refuses a file newer than this build,
/// at 0 runs the baseline ([`prepare_log`]), then each later step the file
/// hasn't had, and records the version reached.
fn migrate(
    conn: &mut rusqlite::Connection,
    later: &[SchemaStep],
) -> Result<(), CheckpointStoreError> {
    let steps = schema_versions::sqlite_checkpoint_steps(later);
    let known = schema_versions::known(schema_versions::BASELINE_VERSION, &steps);
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_err)?;
    tx.execute_batch(schema_versions::SCHEMA_VERSION_DDL)
        .map_err(sqlite_err)?;
    let mut found: i64 = tx
        .query_row(
            "SELECT version FROM strata_schema_version WHERE component = ?1",
            [schema_versions::CHECKPOINTS],
            |row| row.get(0),
        )
        .optional()
        .map_err(sqlite_err)?
        .unwrap_or(0);
    if found > known {
        return Err(CheckpointStoreError(schema_versions::newer(
            schema_versions::CHECKPOINTS,
            found,
            known,
        )));
    }
    if found == known {
        return tx.commit().map_err(sqlite_err);
    }
    if found == 0 {
        prepare_log(&tx)?;
        found = schema_versions::BASELINE_VERSION;
    }
    for step in schema_versions::pending(&steps, found) {
        for statement in &step.statements {
            tx.execute_batch(statement).map_err(sqlite_err)?;
        }
    }
    tx.execute(
        "INSERT INTO strata_schema_version (component, version) VALUES (?1, ?2)
         ON CONFLICT (component) DO UPDATE SET version = excluded.version",
        rusqlite::params![schema_versions::CHECKPOINTS, known],
    )
    .map_err(sqlite_err)?;
    tx.commit().map_err(sqlite_err)
}

/// Version 1 of the checkpoint component, inside [`migrate`]'s transaction:
/// creates the log and its head table, or brings an older file up to date. It
/// adds the `nested` column a log from before nested checkpoints lacks, the
/// `decision` column one from before recorded decisions lacks
/// (`design/21-Typed-Human-Signals.md`), the `wait` and `wake_at` columns
/// one from before waits lacks (`design/29-Event-Waits-And-Timers.md`), and the
/// `graph_id` and `graph_version` columns one from before graph identities
/// lacks (`design/31-Graph-Identity.md`); creates and backfills
/// `checkpoint_threads` on a log that predates it
/// (`design/19-Async-Queryable-Store.md`, "Migration"), or adds the columns it
/// lacks and replaces its trigger on one from before waits or identities; and
/// creates `checkpoint_leases` on one from before leases
/// (`design/32-Thread-Leases.md`). A no-op on a current file.
fn prepare_log(tx: &rusqlite::Connection) -> Result<(), CheckpointStoreError> {
    tx.execute_batch(CHECKPOINT_LOG_DDL).map_err(sqlite_err)?;
    for column in [
        "nested",
        "decision",
        "wait",
        "wake_at",
        "graph_id",
        "graph_version",
    ] {
        add_missing_column(tx, "checkpoints", column)?;
    }
    let has_head: bool = tx
        .query_row(
            "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type = 'table' AND name = 'checkpoint_threads'",
            [],
            |row| row.get(0),
        )
        .map_err(sqlite_err)?;
    if !has_head {
        tx.execute_batch(CHECKPOINT_THREADS_DDL)
            .map_err(sqlite_err)?;
        tx.execute(CHECKPOINT_THREADS_BACKFILL, [])
            .map_err(sqlite_err)?;
    } else {
        let mut added = false;
        for column in ["wake_at", "graph_id", "graph_version"] {
            added |= add_missing_column(tx, "checkpoint_threads", column)?;
        }
        if added {
            // A head table from before waits or identities: its trigger
            // doesn't copy the new columns. Nothing in it waits with a time or
            // has an identity, so every row's NULL is right.
            tx.execute_batch("DROP TRIGGER IF EXISTS checkpoints_head")
                .map_err(sqlite_err)?;
            tx.execute_batch(CHECKPOINT_THREADS_DDL)
                .map_err(sqlite_err)?;
        }
    }
    tx.execute_batch(CHECKPOINT_LEASES_DDL).map_err(sqlite_err)
}

/// Upgrades a table created before nested checkpoints
/// (`design/16-Nested-Interrupts.md`), recorded decisions
/// (`design/21-Typed-Human-Signals.md`), waits
/// (`design/29-Event-Waits-And-Timers.md`) or graph identities
/// (`design/31-Graph-Identity.md`), which lacks `column`, by adding it as
/// `TEXT`: `CREATE TABLE IF NOT EXISTS` leaves such a table as it is. Every
/// existing row keeps `NULL`, which is right — none of them has a child, a
/// decision, a wait or an identity. Says whether it added the column.
fn add_missing_column(
    conn: &rusqlite::Connection,
    table: &str,
    column: &str,
) -> Result<bool, CheckpointStoreError> {
    let present: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info(?1) WHERE name = ?2",
            [table, column],
            |row| row.get(0),
        )
        .map_err(sqlite_err)?;
    if present == 0 {
        conn.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column} TEXT"))
            .map_err(sqlite_err)?;
    }
    Ok(present == 0)
}

/// The blobs of value offload (`design/41-Checkpoint-Efficiency.md`,
/// "Offload"): each value a store offloaded to its own database, by thread and
/// `sha256:` hash, as its JSON text. The checkpoint component's schema version
/// 2. Identical to `EfCoreCheckpointStore.CheckpointBlobsDdl`; pinned by
/// `schema/fixtures/checkpoint-log/blobs-columns.json`.
pub const CHECKPOINT_BLOBS_DDL: &str = "CREATE TABLE IF NOT EXISTS checkpoint_blobs (
    thread_id TEXT NOT NULL,
    hash      TEXT NOT NULL,
    content   BLOB NOT NULL,
    PRIMARY KEY (thread_id, hash)
)";

fn sqlite_err(err: rusqlite::Error) -> CheckpointStoreError {
    CheckpointStoreError(err.to_string())
}

/// The columns every read selects, in this order.
const CHECKPOINT_COLUMNS: &str = "c.thread_id, c.sequence, c.schema_version, c.status, \
     c.suspended_node, c.next_node, c.frozen_state, c.metadata, c.created_at, c.resumed_at, c.nested, \
     c.decision, c.wait, c.graph_id, c.graph_version";

/// A row of [`CHECKPOINT_COLUMNS`] as `(sequence, checkpoint)`.
fn read_checkpoint(row: &rusqlite::Row<'_>) -> rusqlite::Result<(i64, RawCheckpoint)> {
    Ok((
        row.get(1)?,
        RawCheckpoint {
            thread_id: row.get(0)?,
            schema_version: row.get(2)?,
            status: row.get(3)?,
            suspended_node: row.get(4)?,
            next_node: row.get(5)?,
            frozen_state: row.get(6)?,
            metadata: row.get(7)?,
            created_at: row.get(8)?,
            resumed_at: row.get(9)?,
            nested: row.get(10)?,
            decision: row.get(11)?,
            wait: row.get(12)?,
            graph_id: row.get(13)?,
            graph_version: row.get(14)?,
        },
    ))
}

/// A checkpoint row as stored, before its JSON columns are parsed.
struct RawCheckpoint {
    thread_id: String,
    schema_version: String,
    status: String,
    suspended_node: Option<String>,
    next_node: Option<String>,
    frozen_state: String,
    metadata: Option<String>,
    created_at: String,
    resumed_at: Option<String>,
    nested: Option<String>,
    decision: Option<String>,
    wait: Option<String>,
    graph_id: Option<String>,
    graph_version: Option<String>,
}

impl RawCheckpoint {
    fn parse(self) -> Result<Checkpoint, CheckpointStoreError> {
        Ok(Checkpoint {
            schema_version: self.schema_version,
            thread_id: self.thread_id,
            status: parse_status(&self.status)?,
            suspended_node: self.suspended_node,
            next_node: self.next_node,
            frozen_state: serde_json::from_str(&self.frozen_state).map_err(json_err)?,
            metadata: self
                .metadata
                .map(|json| serde_json::from_str(&json))
                .transpose()
                .map_err(json_err)?,
            created_at: self.created_at,
            resumed_at: self.resumed_at,
            nested: self
                .nested
                .map(|json| serde_json::from_str(&json))
                .transpose()
                .map_err(json_err)?,
            decision: self
                .decision
                .map(|json| serde_json::from_str(&json))
                .transpose()
                .map_err(json_err)?,
            wait: self
                .wait
                .map(|json| serde_json::from_str(&json))
                .transpose()
                .map_err(json_err)?,
            graph: self
                .graph_id
                .map(|id| GraphIdentity::new(id, self.graph_version)),
        })
    }
}

/// The rows a load of one row reads: it (`l`, `?1`'s row picked by
/// `target`) and the rows before it that it is a delta of
/// (`design/41-Checkpoint-Efficiency.md`, "Deltas"), oldest first, in one
/// read. A row's depth says how many rows before it that is.
fn chain_sql(target: &str) -> String {
    format!(
        "WITH target AS (
             SELECT l.sequence,
                    CASE WHEN l.schema_version IN ({ENCODED_VERSIONS_SQL})
                         THEN CAST(json_extract(l.frozen_state, '$.depth') AS INTEGER) ELSE 0 END AS depth
             FROM checkpoints l WHERE l.thread_id = ?1 {target})
         SELECT {CHECKPOINT_COLUMNS} FROM checkpoints c, target
         WHERE c.thread_id = ?1 AND c.sequence BETWEEN target.sequence - target.depth AND target.sequence
         ORDER BY c.sequence"
    )
}

/// The rows of `thread_id` from `from` to `to`, oldest first, as stored.
fn range_rows(
    conn: &rusqlite::Connection,
    thread_id: &str,
    from: i64,
    to: i64,
) -> Result<Vec<(i64, Checkpoint)>, CheckpointStoreError> {
    let mut stmt = conn
        .prepare_cached(&format!(
            "SELECT {CHECKPOINT_COLUMNS} FROM checkpoints c
             WHERE c.thread_id = ?1 AND c.sequence BETWEEN ?2 AND ?3 ORDER BY c.sequence"
        ))
        .map_err(sqlite_err)?;
    stmt.query_map(rusqlite::params![thread_id, from, to], read_checkpoint)
        .map_err(sqlite_err)?
        .map(|row| {
            let (sequence, raw) = row.map_err(sqlite_err)?;
            Ok((sequence, raw.parse()?))
        })
        .collect()
}

/// `thread_id`'s latest row, decoded, with its sequence and its depth; with
/// `own_blobs`, its offloaded values read from this file.
fn latest_row(
    conn: &rusqlite::Connection,
    thread_id: &str,
    own_blobs: bool,
) -> Result<Option<(i64, Decoded, u32)>, CheckpointStoreError> {
    let mut stmt = conn
        .prepare_cached(&chain_sql("ORDER BY l.sequence DESC LIMIT 1"))
        .map_err(sqlite_err)?;
    let rows = stmt
        .query_map([thread_id], read_checkpoint)
        .map_err(sqlite_err)?
        .map(|row| {
            let (sequence, raw) = row.map_err(sqlite_err)?;
            Ok((sequence, raw.parse()?))
        })
        .collect::<Result<Vec<(i64, Checkpoint)>, CheckpointStoreError>>()?;
    let Some((_, last)) = rows.last() else {
        return Ok(None);
    };
    let depth = depth_of(last);
    let from = rows.len() - 1;
    let mut decoded = deltas::decode(rows, from)?;
    if own_blobs {
        decoded = resolve_here(conn, thread_id, decoded)?;
    }
    Ok(decoded.pop().map(|row| (row.sequence, row, depth)))
}

/// Reads the offloaded values `decoded` refers to from this file's
/// `checkpoint_blobs`, in one query, and puts them in place.
fn resolve_here(
    conn: &rusqlite::Connection,
    thread_id: &str,
    decoded: Vec<Decoded>,
) -> Result<Vec<Decoded>, CheckpointStoreError> {
    let wanted = deltas::wanted(&decoded);
    if wanted.is_empty() {
        return Ok(decoded);
    }
    let marks = vec!["?"; wanted.len()].join(", ");
    let mut stmt = conn
        .prepare(&format!(
            "SELECT hash, content FROM checkpoint_blobs WHERE thread_id = ? AND hash IN ({marks})"
        ))
        .map_err(sqlite_err)?;
    let params = std::iter::once(thread_id).chain(wanted.iter().map(String::as_str));
    let found = stmt
        .query_map(rusqlite::params_from_iter(params), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
        })
        .map_err(sqlite_err)?
        .collect::<rusqlite::Result<HashMap<_, _>>>()
        .map_err(sqlite_err)?;
    deltas::resolve(decoded, &found)
}

/// One append of `encoding` (see [`insert_sized`]): with `own_blobs`, in one
/// immediate transaction with the blobs it writes, which go in only if the row
/// does. Returns the sequence it was appended at and the bytes of its JSON.
fn append_row(
    conn: &rusqlite::Connection,
    checkpoint: &Checkpoint,
    encoding: &Encoding,
    expected: Option<i64>,
    token: Option<i64>,
    own_blobs: bool,
) -> Result<Option<(i64, usize)>, CheckpointStoreError> {
    if !own_blobs || encoding.blobs.is_empty() {
        return insert_sized(conn, checkpoint, encoding, expected, token);
    }
    let tx = rusqlite::Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
        .map_err(sqlite_err)?;
    let inserted = insert_sized(&tx, checkpoint, encoding, expected, token)?;
    if inserted.is_some() {
        insert_blobs(&tx, &checkpoint.thread_id, encoding)?;
    }
    tx.commit().map_err(sqlite_err)?;
    Ok(inserted)
}

/// The blobs `encoding` writes, into `checkpoint_blobs`; one the thread has
/// already is left as it is.
fn insert_blobs(
    conn: &rusqlite::Connection,
    thread_id: &str,
    encoding: &Encoding,
) -> Result<(), CheckpointStoreError> {
    let mut stmt = conn
        .prepare_cached(
            "INSERT OR IGNORE INTO checkpoint_blobs (thread_id, hash, content) VALUES (?1, ?2, ?3)",
        )
        .map_err(sqlite_err)?;
    for blob in &encoding.blobs {
        stmt.execute(rusqlite::params![thread_id, blob.hash, &blob.content[..]])
            .map_err(sqlite_err)?;
    }
    Ok(())
}

/// Group commit's flush (`design/41-Checkpoint-Efficiency.md`): every row in
/// one immediate transaction, so one commit, and one sync to disk, for all of
/// them, each stored as `encodings` says. Each row reads its thread's latest
/// sequence and lease token and gets the verdict a single save would (a lost
/// lease first, then a moved sequence); the immediate transaction keeps every
/// other writer out until the commit. With `own_blobs`, a saved row's blobs go
/// in with it.
fn flush_rows(
    conn: &Mutex<rusqlite::Connection>,
    fault: &Option<FaultHook>,
    rows: &[FlushRow],
    encodings: Vec<Encoding>,
    writer: Option<&CheckpointWriter>,
    own_blobs: bool,
) -> Result<Vec<Verdict>, Failure> {
    fault_at(fault, "flush:start")?;
    let mut conn = conn.lock().map_err(|_| Failure::from(poisoned()))?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_err)?;
    let mut verdicts = Vec::with_capacity(rows.len());
    let mut stored = Vec::new();
    for (row, encoding) in rows.iter().zip(encodings) {
        let (latest, token, latest_created): (i64, Option<i64>, Option<String>) = tx
            .query_row(
                "SELECT (SELECT COALESCE(MAX(sequence), -1) FROM checkpoints WHERE thread_id = ?1),
                        (SELECT token FROM checkpoint_leases WHERE thread_id = ?1),
                        (SELECT created_at FROM checkpoints WHERE thread_id = ?1
                         ORDER BY sequence DESC LIMIT 1)",
                [&row.checkpoint.thread_id],
                |found| Ok((found.get(0)?, found.get(1)?, found.get(2)?)),
            )
            .map_err(sqlite_err)?;
        let verdict = if row.token.is_some() && token != row.token {
            Verdict::LeaseLost
        } else if row.expected.is_some_and(|expected| expected != latest) {
            Verdict::Lost
        } else {
            // Under the immediate transaction nothing else can append: the
            // insert lands at `latest + 1`, and a row relying on anything but
            // the latest is written whole.
            let written = match &encoding.base {
                Some(base)
                    if base.sequence != latest
                        || latest_created.as_deref() != Some(&*base.token) =>
                {
                    encoding.whole(&row.checkpoint)
                }
                _ => encoding,
            };
            let (sequence, bytes) = insert_sized(&tx, &row.checkpoint, &written, None, None)?
                .unwrap_or((latest + 1, 0));
            if own_blobs {
                insert_blobs(&tx, &row.checkpoint.thread_id, &written)?;
            }
            stored.push((row, written, sequence, bytes));
            Verdict::Saved
        };
        if verdict == Verdict::Lost
            && let Some(writer) = writer
        {
            writer.forget(&row.checkpoint.thread_id);
        }
        verdicts.push(verdict);
    }
    tx.commit().map_err(sqlite_err)?;
    for (row, written, sequence, bytes) in stored {
        record_saved("sqlite", &written, bytes);
        if let Some(writer) = writer {
            writer.saved(
                &row.checkpoint,
                &written,
                sequence,
                &row.checkpoint.created_at,
            );
        }
    }
    Ok(verdicts)
}

/// Appends `checkpoint` as `encoding` stores it at the next sequence for its
/// thread, or, with an `expected` sequence, at `expected + 1` only if that is
/// still the thread's latest (`design/28-Ending-A-Thread.md`, "Races"). One
/// statement either way, and another process writing the same row first
/// counts as losing. With a lease `token`, also only if that is still the
/// thread's latest token (`design/32-Thread-Leases.md`, "Fencing"): the same
/// statement, so no acquisition can come between the check and the insert. A
/// row that relies on the one before it only if that row is still the latest,
/// by its sequence and its `created_at`. Returns the sequence it was appended
/// at and the bytes of the JSON the row holds, what
/// `strata.store.checkpoint.size` records, when it inserted one.
fn insert_sized(
    conn: &rusqlite::Connection,
    checkpoint: &Checkpoint,
    encoding: &Encoding,
    expected: Option<i64>,
    token: Option<i64>,
) -> Result<Option<(i64, usize)>, CheckpointStoreError> {
    let checkpoint = encoding.stored(checkpoint);
    let frozen_state = checkpoint.frozen_state.to_string();
    let metadata = checkpoint
        .metadata
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(json_err)?;
    // The child's whole checkpoint, as the JSON of `checkpoint.schema.json`.
    let nested = checkpoint
        .nested
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(json_err)?;
    // The decision in effect at the checkpoint's level, as its JSON.
    let decision = checkpoint
        .decision
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(json_err)?;
    // The level's own wait, and the innermost level's `wake_at` for the head
    // table (design/29-Event-Waits-And-Timers.md).
    let wait = checkpoint
        .wait
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(json_err)?;
    let wake_at = checkpoint
        .innermost_wait()
        .and_then(|wait| wait.wake_at.clone());
    let bytes = frozen_state.len()
        + [&metadata, &nested, &decision, &wait]
            .into_iter()
            .flatten()
            .map(String::len)
            .sum::<usize>();
    let inserted = conn.query_row(
        "INSERT INTO checkpoints
            (thread_id, sequence, schema_version, status, suspended_node, next_node,
             frozen_state, metadata, created_at, resumed_at, nested, decision, wait, wake_at,
             graph_id, graph_version)
         SELECT ?1, latest.sequence + 1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?13, ?14, ?15, ?16
         FROM (SELECT COALESCE(MAX(sequence), -1) AS sequence FROM checkpoints WHERE thread_id = ?1) latest
         WHERE (?12 IS NULL OR latest.sequence = ?12)
           AND (?17 IS NULL OR (SELECT token FROM checkpoint_leases WHERE thread_id = ?1) = ?17)
           AND (?18 IS NULL OR (latest.sequence = ?18 AND EXISTS (
               SELECT 1 FROM checkpoints b WHERE b.thread_id = ?1 AND b.sequence = ?18 AND b.created_at = ?19)))
         RETURNING sequence",
        rusqlite::params![
            checkpoint.thread_id,
            checkpoint.schema_version,
            status_str(checkpoint.status),
            checkpoint.suspended_node,
            checkpoint.next_node,
            frozen_state,
            metadata,
            checkpoint.created_at,
            checkpoint.resumed_at,
            nested,
            decision,
            expected,
            wait,
            wake_at,
            checkpoint.graph.as_ref().map(|graph| graph.id.as_str()),
            checkpoint
                .graph
                .as_ref()
                .and_then(|graph| graph.version.as_deref()),
            token,
            encoding.base_sequence(),
            encoding.base_token(),
        ],
        |row| row.get::<_, i64>(0),
    );
    match inserted {
        Ok(sequence) => Ok(Some((sequence, bytes))),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(rusqlite::Error::SqliteFailure(error, _))
            if expected.is_some() && error.code == rusqlite::ErrorCode::ConstraintViolation =>
        {
            Ok(None)
        }
        Err(err) => Err(sqlite_err(err)),
    }
}

/// What a fenced save that inserted nothing ran into: a token never goes
/// back, so one that still matches means the sequence didn't.
fn refused_under_lease(
    conn: &rusqlite::Connection,
    lease: &ThreadLease,
    expected: Option<i64>,
) -> Result<LeasedSave, CheckpointStoreError> {
    let current: Option<i64> = conn
        .query_row(
            "SELECT token FROM checkpoint_leases WHERE thread_id = ?1",
            [&lease.thread_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(sqlite_err)?;
    Ok(if current == Some(lease.token) && expected.is_some() {
        LeasedSave::NotLatest
    } else {
        LeasedSave::LeaseLost
    })
}

/// The lease `thread_id` is held under at `now`, if it is.
fn held_lease_row(
    conn: &rusqlite::Connection,
    thread_id: &str,
    now: i64,
) -> Result<Option<ThreadLease>, CheckpointStoreError> {
    conn.query_row(
        "SELECT owner, token, expires_at FROM checkpoint_leases
         WHERE thread_id = ?1 AND expires_at > ?2",
        rusqlite::params![thread_id, now],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        },
    )
    .optional()
    .map_err(sqlite_err)?
    .map(|(owner, token, expires_at)| {
        Ok(ThreadLease {
            thread_id: thread_id.to_string(),
            owner,
            token,
            expires_at: from_millis(expires_at)?,
        })
    })
    .transpose()
}

/// One upsert: the thread's first lease at token 1, or its next token when
/// the lease it has isn't held; no row back means it is held, and a second
/// statement reads by whom (`design/32-Thread-Leases.md`).
fn acquire_lease_row(
    conn: &rusqlite::Connection,
    thread_id: &str,
    owner: &str,
    ttl: Duration,
    now: i64,
) -> Result<LeaseAttempt, CheckpointStoreError> {
    check_lease(owner, ttl)?;
    let expires_at = now + lease_millis(ttl);
    for _ in 0..ACQUIRE_ATTEMPTS {
        let token: Option<i64> = conn
            .query_row(
                "INSERT INTO checkpoint_leases (thread_id, owner, token, expires_at) VALUES (?1, ?2, 1, ?3)
                 ON CONFLICT (thread_id) DO UPDATE
                     SET owner = excluded.owner, token = checkpoint_leases.token + 1, expires_at = excluded.expires_at
                     WHERE checkpoint_leases.expires_at IS NULL OR checkpoint_leases.expires_at <= ?4
                 RETURNING token",
                rusqlite::params![thread_id, owner, expires_at, now],
                |row| row.get(0),
            )
            .optional()
            .map_err(sqlite_err)?;
        if let Some(token) = token {
            return Ok(LeaseAttempt::Acquired(ThreadLease {
                thread_id: thread_id.to_string(),
                owner: owner.to_string(),
                token,
                expires_at: from_millis(expires_at)?,
            }));
        }
        if let Some(held) = held_lease_row(conn, thread_id, now)? {
            return Ok(LeaseAttempt::Held(held));
        }
        // Released or expired between the two statements: look again.
    }
    Err(lease_kept_changing(thread_id))
}

fn renew_lease_row(
    conn: &rusqlite::Connection,
    lease: &ThreadLease,
    ttl: Duration,
    now: i64,
) -> Result<Option<ThreadLease>, CheckpointStoreError> {
    check_lease_ttl(ttl)?;
    let expires_at = now + lease_millis(ttl);
    let renewed = conn
        .execute(
            "UPDATE checkpoint_leases SET owner = ?1, expires_at = ?2 WHERE thread_id = ?3 AND token = ?4",
            rusqlite::params![lease.owner, expires_at, lease.thread_id, lease.token],
        )
        .map_err(sqlite_err)?;
    if renewed == 0 {
        return Ok(None);
    }
    Ok(Some(ThreadLease {
        expires_at: from_millis(expires_at)?,
        ..lease.clone()
    }))
}

fn release_lease_row(
    conn: &rusqlite::Connection,
    lease: &ThreadLease,
) -> Result<bool, CheckpointStoreError> {
    let released = conn
        .execute(
            "UPDATE checkpoint_leases SET owner = NULL, expires_at = NULL WHERE thread_id = ?1 AND token = ?2",
            rusqlite::params![lease.thread_id, lease.token],
        )
        .map_err(sqlite_err)?;
    Ok(released == 1)
}

/// One page of `query`, from the head table joined to each thread's latest row.
fn list_rows(
    conn: &rusqlite::Connection,
    query: &ThreadQuery,
) -> Result<ThreadPage, CheckpointStoreError> {
    use rusqlite::types::Value;

    let cursor = query.validate()?;
    let mut filters: Vec<String> = Vec::new();
    let mut params: Vec<Value> = Vec::new();
    if !query.states.is_empty() {
        let marks = vec!["?"; query.states.len()].join(", ");
        filters.push(format!("h.state IN ({marks})"));
        params.extend(
            query
                .states
                .iter()
                .map(|state| Value::Text(state.as_str().to_string())),
        );
    }
    for (key, value) in &query.metadata {
        filters.push(
            "EXISTS (SELECT 1 FROM json_each(h.metadata) j WHERE j.key = ? AND j.value = ?)"
                .to_string(),
        );
        params.push(Value::Text(key.clone()));
        params.push(Value::Text(value.clone()));
    }
    if let Some(after) = query.updated_after_text() {
        filters.push("h.updated_at > ?".to_string());
        params.push(Value::Text(after));
    }
    if let Some(before) = query.updated_before_text() {
        filters.push("h.updated_at < ?".to_string());
        params.push(Value::Text(before));
    }
    if let Some(due) = query.due_by_text() {
        filters.push("h.state = 'interrupted' AND h.wake_at <= ?".to_string());
        params.push(Value::Text(due));
    }
    if let Some(id) = &query.graph_id {
        filters.push("h.graph_id = ?".to_string());
        params.push(Value::Text(id.clone()));
    }
    if let Some(version) = &query.graph_version {
        filters.push("h.graph_version = ?".to_string());
        params.push(Value::Text(version.clone()));
    }
    if let Some(cursor) = &cursor {
        filters.push("(h.updated_at, h.thread_id) < (?, ?)".to_string());
        params.push(Value::Text(cursor.updated_at.clone()));
        params.push(Value::Text(cursor.thread_id.clone()));
    }
    let filter = if filters.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", filters.join(" AND "))
    };
    // One more than a page, to know whether another follows.
    params.push(Value::Integer(query.limit as i64 + 1));
    // A summary reads no state: of an encoded row it needs only the version
    // it was saved with.
    let sql = format!(
        "SELECT {CHECKPOINT_COLUMNS}, h.updated_at,
                CASE WHEN c.schema_version IN ({ENCODED_VERSIONS_SQL})
                     THEN json_extract(c.frozen_state, '$.checkpoint_version') END
         FROM checkpoint_threads h
         JOIN checkpoints c ON c.thread_id = h.thread_id AND c.sequence = h.sequence
         {filter}
         ORDER BY h.updated_at DESC, h.thread_id DESC
         LIMIT ?"
    );

    let mut stmt = conn.prepare(&sql).map_err(sqlite_err)?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params), |row| {
            Ok((
                read_checkpoint(row)?,
                row.get::<_, String>(15)?,
                row.get::<_, Option<String>>(16)?,
            ))
        })
        .map_err(sqlite_err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_err)?;

    let more = rows.len() > query.limit;
    let mut threads = Vec::with_capacity(rows.len().min(query.limit));
    let mut next_cursor = None;
    for ((sequence, raw), updated_at, version) in rows.into_iter().take(query.limit) {
        // The summary needs no state: skip parsing it.
        let raw = RawCheckpoint {
            schema_version: listed_version(raw.schema_version, version),
            frozen_state: "null".to_string(),
            ..raw
        };
        let checkpoint = raw.parse()?;
        if more {
            next_cursor = Some(ThreadCursor::new(updated_at, &checkpoint.thread_id).encode());
        }
        threads.push(ThreadSummary::of(sequence, &checkpoint)?);
    }
    Ok(ThreadPage {
        threads,
        next_cursor,
    })
}

/// A page of `thread_id`'s log, decoded; with `own_blobs`, its offloaded
/// values read from this file.
fn log_rows(
    conn: &rusqlite::Connection,
    thread_id: &str,
    after_sequence: Option<i64>,
    limit: usize,
    own_blobs: bool,
) -> Result<Vec<Decoded>, CheckpointStoreError> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {CHECKPOINT_COLUMNS} FROM checkpoints c
             WHERE c.thread_id = ?1 AND c.sequence > ?2
             ORDER BY c.sequence LIMIT ?3"
        ))
        .map_err(sqlite_err)?;
    let rows = stmt
        .query_map(
            rusqlite::params![thread_id, after_sequence.unwrap_or(-1), limit as i64],
            read_checkpoint,
        )
        .map_err(sqlite_err)?
        .map(|row| {
            let (sequence, raw) = row.map_err(sqlite_err)?;
            Ok((sequence, raw.parse()?))
        })
        .collect::<Result<Vec<_>, CheckpointStoreError>>()?;
    let decoded = decode_page(rows, |from, to| range_rows(conn, thread_id, from, to))?;
    if own_blobs {
        resolve_here(conn, thread_id, decoded)
    } else {
        Ok(decoded)
    }
}

/// `rows` (a page of a thread's log, oldest first) decoded, reading through
/// `earlier` the rows before the page that its first row is a delta of; their
/// offloaded values are still to read.
pub(crate) fn decode_page(
    rows: Vec<(i64, Checkpoint)>,
    earlier: impl FnOnce(i64, i64) -> Result<Vec<(i64, Checkpoint)>, CheckpointStoreError>,
) -> Result<Vec<Decoded>, CheckpointStoreError> {
    let depth = rows.first().map_or(0, |(_, first)| depth_of(first)) as i64;
    if depth == 0 {
        return deltas::decode(rows, 0);
    }
    let first = rows[0].0;
    let mut all = earlier(first - depth, first - 1)?;
    let from = all.len();
    all.extend(rows);
    deltas::decode(all, from)
}

impl CheckpointStore for SqliteCheckpointStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        if let Some(group) = &self.group {
            return Box::pin(async move {
                let row = FlushRow {
                    checkpoint,
                    expected: None,
                    token: None,
                };
                self.queued(group, row).await.map(|_| ())
            });
        }
        Box::pin(async move { self.insert(checkpoint, None, None).await.map(|_| ()) })
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        Box::pin(async move {
            Ok(self
                .latest(thread_id)
                .await?
                .map(|(_, checkpoint)| checkpoint))
        })
    }

    fn load_latest<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<LatestCheckpoint>, CheckpointStoreError>> {
        Box::pin(async move {
            Ok(self
                .latest(thread_id)
                .await?
                .map(|(sequence, checkpoint)| LatestCheckpoint {
                    checkpoint,
                    sequence: Some(sequence),
                }))
        })
    }

    fn save_if_latest(
        &self,
        checkpoint: Checkpoint,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'_, Result<bool, CheckpointStoreError>> {
        if let Some(group) = &self.group {
            return Box::pin(async move {
                let row = FlushRow {
                    checkpoint,
                    expected: expected_sequence,
                    token: None,
                };
                Ok(self.queued(group, row).await? == LeasedSave::Saved)
            });
        }
        Box::pin(self.insert(checkpoint, expected_sequence, None))
    }

    fn try_acquire_lease<'a>(
        &'a self,
        thread_id: &'a str,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<LeaseAttempt, CheckpointStoreError>> {
        let (thread_id, owner, now) = (thread_id.to_string(), owner.to_string(), self.now_millis());
        self.with_conn(move |conn| acquire_lease_row(conn, &thread_id, &owner, ttl, now))
    }

    fn renew_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        let (lease, now) = (lease.clone(), self.now_millis());
        self.with_conn(move |conn| renew_lease_row(conn, &lease, ttl, now))
    }

    fn release_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
    ) -> BoxFuture<'a, Result<bool, CheckpointStoreError>> {
        let lease = lease.clone();
        self.with_conn(move |conn| release_lease_row(conn, &lease))
    }

    fn get_lease<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        let (thread_id, now) = (thread_id.to_string(), self.now_millis());
        self.with_conn(move |conn| held_lease_row(conn, &thread_id, now))
    }

    fn save_under_lease<'a>(
        &'a self,
        checkpoint: Checkpoint,
        lease: &'a ThreadLease,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'a, Result<LeasedSave, CheckpointStoreError>> {
        if let Some(group) = &self.group {
            return Box::pin(async move {
                check_lease_thread(&checkpoint, lease)?;
                let row = FlushRow {
                    checkpoint,
                    expected: expected_sequence,
                    token: Some(lease.token),
                };
                self.queued(group, row).await
            });
        }
        Box::pin(async move {
            check_lease_thread(&checkpoint, lease)?;
            if self
                .insert(checkpoint, expected_sequence, Some(lease.token))
                .await?
            {
                return Ok(LeasedSave::Saved);
            }
            let lease = lease.clone();
            self.with_conn(move |conn| refused_under_lease(conn, &lease, expected_sequence))
                .await
        })
    }
}

/// Deletes the logs of threads that are done with
/// (`design/34-Storage-Hardening.md`, "Retention"), oldest first, in one
/// immediate transaction: each thread's checkpoints, its head row, its lease
/// row and its blobs. A thread a live lease holds at `now` is kept. Returns
/// the threads it deleted.
fn purge_rows(
    conn: &rusqlite::Connection,
    before: &str,
    states: &PurgeStates,
    limit: usize,
    now: i64,
) -> Result<Vec<String>, CheckpointStoreError> {
    if states.is_empty() {
        return Ok(Vec::new());
    }
    // The names are the engine's own, not input.
    let named = states
        .iter()
        .map(|state| format!("'{}'", state.as_str()))
        .collect::<Vec<_>>()
        .join(", ");
    let tx = rusqlite::Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
        .map_err(sqlite_err)?;
    let threads: Vec<String> = {
        let mut pick = tx
            .prepare(&format!(
                "SELECT h.thread_id FROM checkpoint_threads h
                 WHERE h.state IN ({named}) AND h.updated_at < ?1
                   AND NOT EXISTS (SELECT 1 FROM checkpoint_leases l WHERE l.thread_id = h.thread_id AND l.expires_at > ?2)
                 ORDER BY h.updated_at, h.thread_id LIMIT ?3"
            ))
            .map_err(sqlite_err)?;
        pick.query_map(rusqlite::params![before, now, limit as i64], |row| {
            row.get(0)
        })
        .map_err(sqlite_err)?
        .collect::<rusqlite::Result<_>>()
        .map_err(sqlite_err)?
    };
    for thread_id in &threads {
        // The table names are the store's own constants.
        for table in [
            "checkpoints",
            "checkpoint_threads",
            "checkpoint_leases",
            "checkpoint_blobs",
        ] {
            tx.execute(
                &format!("DELETE FROM {table} WHERE thread_id = ?1"),
                [thread_id],
            )
            .map_err(sqlite_err)?;
        }
    }
    tx.commit().map_err(sqlite_err)?;
    Ok(threads)
}

impl CheckpointRetention for SqliteCheckpointStore {
    fn purge<'a>(
        &'a self,
        older_than: DateTime<Utc>,
        states: &'a PurgeStates,
        limit: usize,
    ) -> BoxFuture<'a, Result<usize, CheckpointStoreError>> {
        if let Err(refused) = check_limit(limit) {
            return Box::pin(std::future::ready(Err(refused)));
        }
        let before = timestamp_text(older_than);
        let states = states.clone();
        let now = self.now_millis();
        Box::pin(async move {
            let purged = self
                .with_conn(move |conn| purge_rows(conn, &before, &states, limit, now))
                .await?;
            // Blobs in another store go after the rows
            // (`design/41-Checkpoint-Efficiency.md`, "Retention").
            for thread_id in &purged {
                if let Some(writer) = &self.writer {
                    writer.forget(thread_id);
                }
                if let Some(blobs) = &self.blobs {
                    blobs.delete_thread(thread_id).await?;
                }
            }
            Ok(purged.len())
        })
    }
}

impl CheckpointQuery for SqliteCheckpointStore {
    fn list_threads<'a>(
        &'a self,
        query: &'a ThreadQuery,
    ) -> BoxFuture<'a, Result<ThreadPage, CheckpointStoreError>> {
        let query = query.clone();
        self.with_conn(move |conn| list_rows(conn, &query))
    }

    fn get_log<'a>(
        &'a self,
        thread_id: &'a str,
        after_sequence: Option<i64>,
        limit: usize,
    ) -> BoxFuture<'a, Result<CheckpointLogPage, CheckpointStoreError>> {
        Box::pin(async move {
            check_limit(limit)?;
            let (owned, own) = (thread_id.to_string(), self.blobs.is_none());
            let decoded = self
                .with_conn(move |conn| log_rows(conn, &owned, after_sequence, limit, own))
                .await?;
            let entries = self
                .resolve(thread_id, decoded)
                .await?
                .into_iter()
                .map(|row| CheckpointLogEntry {
                    sequence: row.sequence,
                    checkpoint: row.checkpoint,
                })
                .collect();
            Ok(CheckpointLogPage::new(entries, limit))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datalium_strata::checkpoint::{GraphExecutionResult, HumanSignal};
    use datalium_strata::graph::{CompiledGraph, NodeError, RecoveryPolicy, StateGraph};
    use datalium_strata::schema::{CheckpointStatus, NESTED_CHECKPOINT_SCHEMA_VERSION};
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn sample_checkpoint(thread_id: &str) -> Checkpoint {
        Checkpoint {
            schema_version: datalium_strata::schema::SCHEMA_VERSION.to_string(),
            thread_id: thread_id.to_string(),
            status: CheckpointStatus::Interrupted,
            suspended_node: Some("PrepareWireTransfer".to_string()),
            next_node: Some("ExecuteWireTransfer".to_string()),
            frozen_state: json!({ "invoice_generated": true }),
            metadata: None,
            created_at: "2026-09-16T00:00:00Z".to_string(),
            resumed_at: None,
            nested: None,
            decision: None,
            wait: None,
            graph: None,
        }
    }

    #[tokio::test]
    async fn save_then_load_round_trips() {
        let store = InMemoryCheckpointStore::new();
        store.save(sample_checkpoint("thread_1")).await.unwrap();

        let loaded = store.load("thread_1").await.unwrap();
        assert_eq!(loaded, Some(sample_checkpoint("thread_1")));
    }

    #[tokio::test]
    async fn load_missing_thread_returns_none() {
        let store = InMemoryCheckpointStore::new();
        assert_eq!(store.load("does-not-exist").await.unwrap(), None);
    }

    #[tokio::test]
    async fn save_overwrites_existing_checkpoint_for_same_thread() {
        let store = InMemoryCheckpointStore::new();
        store.save(sample_checkpoint("thread_1")).await.unwrap();

        let mut updated = sample_checkpoint("thread_1");
        updated.resumed_at = Some("2026-09-16T01:00:00Z".to_string());
        store.save(updated.clone()).await.unwrap();

        assert_eq!(store.load("thread_1").await.unwrap(), Some(updated));
    }

    // --- Cross-language checkpoint fixture (Phase 4 exit criterion) ---
    //
    // `schema/fixtures/checkpoint-lifecycle/frozen.json` is the shared
    // interop artifact: this suite proves the *real* production store
    // (`InMemoryCheckpointStore`) plus `datalium_strata`'s execution engine
    // can both produce a checkpoint matching that fixture and consume it to
    // resume correctly. `Datalium.Strata.Tests`' equivalent suite proves
    // the same two things independently in C#, against the identical file
    // — that's what "freeze in Rust, resume in C# (or vice versa) ...
    // using the Phase 1 shared schema" means in a test suite that can't
    // literally pipe one live process into another.

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct WireTransferState {
        invoice_generated: bool,
        amount: f64,
        currency: String,
        approved_by: Option<String>,
        is_completed: bool,
    }

    fn wire_transfer_graph() -> CompiledGraph<WireTransferState> {
        StateGraph::<WireTransferState>::new()
            .add_node("PrepareWireTransfer", |s: WireTransferState| {
                WireTransferState {
                    invoice_generated: true,
                    ..s
                }
            })
            .add_interrupt_boundary("PrepareWireTransfer")
            .add_node("ExecuteWireTransfer", |s: WireTransferState| {
                WireTransferState {
                    is_completed: true,
                    ..s
                }
            })
            .add_edge("PrepareWireTransfer", "ExecuteWireTransfer")
            .compile()
            .unwrap()
    }

    fn load_fixture() -> Checkpoint {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../schema/fixtures/checkpoint-lifecycle/frozen.json");
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read fixture {}: {}", path.display(), e));
        serde_json::from_str(&raw).unwrap()
    }

    #[tokio::test]
    async fn freezing_the_wire_transfer_graph_matches_the_shared_fixture_shape() {
        let graph = wire_transfer_graph();
        let store = Arc::new(InMemoryCheckpointStore::new());
        let initial = WireTransferState {
            invoice_generated: false,
            amount: 4200.5,
            currency: "USD".to_string(),
            approved_by: None,
            is_completed: false,
        };

        graph.run("thread_hitl_demo", initial, store.clone()).await;
        let persisted = store.load("thread_hitl_demo").await.unwrap().unwrap();
        let fixture = load_fixture();

        // Every field except the timestamps (the fixture's is a fixed
        // historical value; a live freeze uses the real current time).
        assert_eq!(persisted.schema_version, fixture.schema_version);
        assert_eq!(persisted.thread_id, fixture.thread_id);
        assert_eq!(persisted.status, fixture.status);
        assert_eq!(persisted.suspended_node, fixture.suspended_node);
        assert_eq!(persisted.next_node, fixture.next_node);
        assert_eq!(persisted.frozen_state, fixture.frozen_state);
        assert_eq!(persisted.metadata, fixture.metadata);
        assert!(persisted.resumed_at.is_none());
    }

    #[tokio::test]
    async fn resuming_the_shared_fixture_produces_the_expected_completed_state() {
        let graph = wire_transfer_graph();
        let store = Arc::new(InMemoryCheckpointStore::new());
        let fixture = load_fixture();
        let thread_id = fixture.thread_id.clone();
        store.save(fixture).await.unwrap();

        let mut overrides = HashMap::new();
        overrides.insert("approved_by".to_string(), "admin_01".to_string());
        let signal = HumanSignal {
            is_approved: true,
            reviewer_token: "token-abc".to_string(),
            field_overrides: overrides,
            ..Default::default()
        };

        let result = graph.resume(&thread_id, signal, store.clone()).await;

        match result {
            GraphExecutionResult::Completed(state) => {
                assert_eq!(
                    state,
                    WireTransferState {
                        invoice_generated: true,
                        amount: 4200.5,
                        currency: "USD".to_string(),
                        approved_by: Some("admin_01".to_string()),
                        is_completed: true,
                    }
                );
            }
            _ => panic!("expected Completed"),
        }
    }

    // --- `status: "failed"` fixture (design/04-Error-Recovery.md) ---
    //
    // `schema/fixtures/checkpoint-lifecycle/failed.json` is the shared
    // artifact for a fallible node that exhausted its retries with no
    // fallback: a live failure produces it, and a resume re-enters the failed
    // node from it. `Datalium.Strata.Tests`' `FailedCheckpointFixtureParityTests`
    // proves the same two things in C#, against the identical file.

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct PaymentState {
        prepared: bool,
        charged: bool,
        notes: Vec<String>,
    }

    fn payment_graph() -> CompiledGraph<PaymentState> {
        StateGraph::<PaymentState>::new()
            .add_node("Prepare", |s: PaymentState| PaymentState {
                prepared: true,
                ..s
            })
            .add_fallible_node("Charge", |s: &PaymentState| {
                // Partial work that must never reach the committed state.
                let mut partial = s.clone();
                partial.charged = true;
                let _ = partial;
                Err(NodeError::Transient("gateway timeout".to_string()))
            })
            .add_edge("Prepare", "Charge")
            .set_recovery_policy("Charge", RecoveryPolicy::retry(1))
            .compile()
            .unwrap()
    }

    fn load_failed_fixture() -> Checkpoint {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../schema/fixtures/checkpoint-lifecycle/failed.json");
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read fixture {}: {}", path.display(), e));
        serde_json::from_str(&raw).unwrap()
    }

    #[tokio::test]
    async fn a_failed_node_produces_a_checkpoint_matching_the_shared_fixture_shape() {
        let store = Arc::new(InMemoryCheckpointStore::new());
        let initial = PaymentState {
            prepared: false,
            charged: false,
            notes: Vec::new(),
        };

        let result = payment_graph()
            .run("thread_recovery_demo", initial, store.clone())
            .await;
        assert!(matches!(result, GraphExecutionResult::Failed(_)));

        let persisted = store.load("thread_recovery_demo").await.unwrap().unwrap();
        let fixture = load_failed_fixture();

        // Every field except the timestamp (the fixture's is a fixed
        // historical value; a live failure uses the real current time).
        assert_eq!(persisted.schema_version, fixture.schema_version);
        assert_eq!(persisted.thread_id, fixture.thread_id);
        assert_eq!(persisted.status, CheckpointStatus::Failed);
        assert_eq!(persisted.status, fixture.status);
        assert_eq!(persisted.suspended_node, fixture.suspended_node);
        assert_eq!(persisted.next_node, fixture.next_node);
        assert_eq!(persisted.frozen_state, fixture.frozen_state);
        assert_eq!(persisted.metadata, fixture.metadata);
        assert_eq!(persisted.resumed_at, fixture.resumed_at);
    }

    #[tokio::test]
    async fn resume_re_enters_the_failed_node_of_the_shared_failed_fixture() {
        // The same graph shape, with `Charge` working now.
        let graph = StateGraph::<PaymentState>::new()
            .add_node("Prepare", |mut s: PaymentState| {
                s.notes.push("prepared".to_string());
                s
            })
            .add_fallible_node("Charge", |s: &PaymentState| {
                let mut next = s.clone();
                next.charged = true;
                next.notes.push("charged".to_string());
                Ok(next)
            })
            .add_edge("Prepare", "Charge")
            .compile()
            .unwrap();
        let store = Arc::new(InMemoryCheckpointStore::new());
        let fixture = load_failed_fixture();
        let thread_id = fixture.thread_id.clone();
        store.save(fixture).await.unwrap();

        match graph
            .resume(&thread_id, HumanSignal::default(), store.clone())
            .await
        {
            GraphExecutionResult::Completed(state) => {
                // `Prepare` did not run again: the fixture's state is the one
                // it had when `Charge` failed.
                assert!(state.prepared && state.charged);
                assert_eq!(state.notes, ["charged"]);
            }
            _ => panic!("expected Completed: a failed checkpoint resumes at the failed node"),
        }
    }

    // --- SqliteCheckpointStore: same coverage as InMemoryCheckpointStore,
    // plus a test proving the point of a SQLite-backed adapter in the first
    // place (persistence across independent store instances / a simulated
    // process restart), which InMemoryCheckpointStore structurally can't
    // satisfy.

    #[tokio::test]
    async fn sqlite_save_then_load_round_trips() {
        let store = SqliteCheckpointStore::open_in_memory().unwrap();
        store.save(sample_checkpoint("thread_1")).await.unwrap();

        let loaded = store.load("thread_1").await.unwrap();
        assert_eq!(loaded, Some(sample_checkpoint("thread_1")));
    }

    #[tokio::test]
    async fn sqlite_load_missing_thread_returns_none() {
        let store = SqliteCheckpointStore::open_in_memory().unwrap();
        assert_eq!(store.load("does-not-exist").await.unwrap(), None);
    }

    #[tokio::test]
    async fn sqlite_save_overwrites_existing_checkpoint_for_same_thread() {
        let store = SqliteCheckpointStore::open_in_memory().unwrap();
        store.save(sample_checkpoint("thread_1")).await.unwrap();

        let mut updated = sample_checkpoint("thread_1");
        updated.resumed_at = Some("2026-09-16T01:00:00Z".to_string());
        store.save(updated.clone()).await.unwrap();

        assert_eq!(store.load("thread_1").await.unwrap(), Some(updated));
    }

    #[tokio::test]
    async fn sqlite_freezing_the_wire_transfer_graph_matches_the_shared_fixture_shape() {
        let graph = wire_transfer_graph();
        let store = Arc::new(SqliteCheckpointStore::open_in_memory().unwrap());
        let initial = WireTransferState {
            invoice_generated: false,
            amount: 4200.5,
            currency: "USD".to_string(),
            approved_by: None,
            is_completed: false,
        };

        graph.run("thread_hitl_demo", initial, store.clone()).await;
        let persisted = store.load("thread_hitl_demo").await.unwrap().unwrap();
        let fixture = load_fixture();

        // Every field except the timestamps (the fixture's is a fixed
        // historical value; a live freeze uses the real current time).
        assert_eq!(persisted.schema_version, fixture.schema_version);
        assert_eq!(persisted.thread_id, fixture.thread_id);
        assert_eq!(persisted.status, fixture.status);
        assert_eq!(persisted.suspended_node, fixture.suspended_node);
        assert_eq!(persisted.next_node, fixture.next_node);
        assert_eq!(persisted.frozen_state, fixture.frozen_state);
        assert_eq!(persisted.metadata, fixture.metadata);
        assert!(persisted.resumed_at.is_none());
    }

    #[tokio::test]
    async fn sqlite_resuming_the_shared_fixture_produces_the_expected_completed_state() {
        let graph = wire_transfer_graph();
        let store = Arc::new(SqliteCheckpointStore::open_in_memory().unwrap());
        let fixture = load_fixture();
        let thread_id = fixture.thread_id.clone();
        store.save(fixture).await.unwrap();

        let mut overrides = HashMap::new();
        overrides.insert("approved_by".to_string(), "admin_01".to_string());
        let signal = HumanSignal {
            is_approved: true,
            reviewer_token: "token-abc".to_string(),
            field_overrides: overrides,
            ..Default::default()
        };

        let result = graph.resume(&thread_id, signal, store.clone()).await;

        match result {
            GraphExecutionResult::Completed(state) => {
                assert_eq!(
                    state,
                    WireTransferState {
                        invoice_generated: true,
                        amount: 4200.5,
                        currency: "USD".to_string(),
                        approved_by: Some("admin_01".to_string()),
                        is_completed: true,
                    }
                );
            }
            _ => panic!("expected Completed"),
        }
    }

    #[tokio::test]
    async fn sqlite_checkpoint_persists_across_independent_store_instances_at_the_same_path() {
        // Proves the actual point of `SqliteCheckpointStore` over
        // `InMemoryCheckpointStore`: a checkpoint written by one store
        // instance is readable by a second, independent instance opened
        // against the same file path afterward — i.e. state survives what
        // would be a process restart.
        let path = std::env::temp_dir().join(format!(
            "strata-checkpoint-persistence-test-{}-{}.sqlite",
            std::process::id(),
            "thread_1"
        ));
        // Best-effort cleanup in case a previous run of this test was
        // interrupted before reaching its own cleanup below.
        let _ = std::fs::remove_file(&path);

        {
            let store = SqliteCheckpointStore::open(&path).unwrap();
            store.save(sample_checkpoint("thread_1")).await.unwrap();
            // `store` (and its `rusqlite::Connection`) is dropped here,
            // simulating a process restart before the next instance opens
            // the same file.
        }

        let reopened = SqliteCheckpointStore::open(&path).unwrap();
        let loaded = reopened.load("thread_1").await.unwrap();
        assert_eq!(loaded, Some(sample_checkpoint("thread_1")));

        drop(reopened);
        let _ = std::fs::remove_file(&path);
    }

    // --- The shared SQLite checkpoint log (schema/README.md, "SQLite
    // checkpoint log"). `Datalium.Strata.Storage`'s `EfCoreCheckpointStore`
    // and the `strata` CLI's `store::open` create the same table, so these
    // tests compare against fixtures all three read: `columns.json` (the
    // expected `PRAGMA table_info`) and `frozen.row.json` (the expected raw
    // row after saving `checkpoint-lifecycle/frozen.json`).

    fn log_fixture(name: &str) -> serde_json::Value {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../schema/fixtures/checkpoint-log")
            .join(name);
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read fixture {}: {}", path.display(), e));
        serde_json::from_str(&raw).unwrap()
    }

    fn temp_db(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "strata-checkpoint-log-test-{}-{}.sqlite",
            std::process::id(),
            name
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn raw_row(conn: &rusqlite::Connection, thread_id: &str, sequence: i64) -> serde_json::Value {
        conn.query_row(
            "SELECT thread_id, sequence, schema_version, status, suspended_node, next_node,
                    frozen_state, metadata, created_at, resumed_at, nested, decision, wait, wake_at,
                    graph_id, graph_version
             FROM checkpoints WHERE thread_id = ?1 AND sequence = ?2",
            rusqlite::params![thread_id, sequence],
            |row| {
                let frozen_state: String = row.get(6)?;
                let metadata: Option<String> = row.get(7)?;
                let nested: Option<String> = row.get(10)?;
                let decision: Option<String> = row.get(11)?;
                let wait: Option<String> = row.get(12)?;
                Ok(json!({
                    "thread_id": row.get::<_, String>(0)?,
                    "sequence": row.get::<_, i64>(1)?,
                    "schema_version": row.get::<_, String>(2)?,
                    "status": row.get::<_, String>(3)?,
                    "suspended_node": row.get::<_, Option<String>>(4)?,
                    "next_node": row.get::<_, Option<String>>(5)?,
                    "frozen_state": serde_json::from_str::<serde_json::Value>(&frozen_state).unwrap(),
                    "metadata": metadata.map(|m| serde_json::from_str::<serde_json::Value>(&m).unwrap()),
                    "created_at": row.get::<_, String>(8)?,
                    "resumed_at": row.get::<_, Option<String>>(9)?,
                    "nested": nested.map(|n| serde_json::from_str::<serde_json::Value>(&n).unwrap()),
                    "decision": decision.map(|d| serde_json::from_str::<serde_json::Value>(&d).unwrap()),
                    "wait": wait.map(|w| serde_json::from_str::<serde_json::Value>(&w).unwrap()),
                    "wake_at": row.get::<_, Option<String>>(13)?,
                    "graph_id": row.get::<_, Option<String>>(14)?,
                    "graph_version": row.get::<_, Option<String>>(15)?,
                }))
            },
        )
        .unwrap()
    }

    #[tokio::test]
    async fn sqlite_table_layout_matches_the_shared_column_fixture() {
        let path = temp_db("layout");
        drop(SqliteCheckpointStore::open(&path).unwrap());

        let conn = rusqlite::Connection::open(&path).unwrap();
        let mut stmt = conn.prepare("PRAGMA table_info(checkpoints)").unwrap();
        let columns: Vec<serde_json::Value> = stmt
            .query_map([], |row| {
                Ok(json!({
                    "name": row.get::<_, String>(1)?,
                    "type": row.get::<_, String>(2)?,
                    "notnull": row.get::<_, i64>(3)? != 0,
                    "pk": row.get::<_, i64>(5)?,
                }))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();

        assert_eq!(json!(columns), log_fixture("columns.json"));

        drop(stmt);
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    /// The `checkpoints` table as it was before nested checkpoints
    /// (`design/16-Nested-Interrupts.md`): no `nested` column.
    const PRE_NESTED_DDL: &str = "CREATE TABLE checkpoints (
        thread_id      TEXT NOT NULL,
        sequence       INTEGER NOT NULL,
        schema_version TEXT NOT NULL,
        status         TEXT NOT NULL,
        suspended_node TEXT,
        next_node      TEXT,
        frozen_state   TEXT NOT NULL,
        metadata       TEXT,
        created_at     TEXT NOT NULL,
        resumed_at     TEXT,
        PRIMARY KEY (thread_id, sequence)
    )";

    #[tokio::test]
    async fn opening_a_log_from_before_nested_checkpoints_adds_the_column_and_keeps_its_rows() {
        let path = temp_db("pre-nested");
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(PRE_NESTED_DDL).unwrap();
            conn.execute(
                "INSERT INTO checkpoints VALUES
                    ('old', 0, '1.0.0', 'interrupted', 'A', 'B', '{\"n\":1}', NULL,
                     '2026-09-16T00:00:00Z', NULL)",
                [],
            )
            .unwrap();
        }

        let store = SqliteCheckpointStore::open(&path).unwrap();

        // The old row reads back as it was, with no child.
        let old = store.load("old").await.unwrap().unwrap();
        assert_eq!(old.frozen_state, json!({ "n": 1 }));
        assert_eq!(old.next_node.as_deref(), Some("B"));
        assert_eq!(old.nested, None);

        // A checkpoint with a child saves and loads on the upgraded file.
        let mut nested = sample_checkpoint("new");
        nested.schema_version = NESTED_CHECKPOINT_SCHEMA_VERSION.to_string();
        nested.nested = Some(Box::new(sample_checkpoint("Work")));
        store.save(nested.clone()).await.unwrap();
        assert_eq!(store.load("new").await.unwrap(), Some(nested));

        // So does one with a decision (design/21-Typed-Human-Signals.md): its
        // column was added too.
        let decided: Checkpoint = serde_json::from_str(
            &std::fs::read_to_string(
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../../schema/fixtures/checkpoint-lifecycle/decision-failed.json"),
            )
            .unwrap(),
        )
        .unwrap();
        store.save(decided.clone()).await.unwrap();
        assert_eq!(store.load(&decided.thread_id).await.unwrap(), Some(decided));
        drop(store);

        // The upgrade left exactly the shared layout, and opening again is a no-op.
        drop(SqliteCheckpointStore::open(&path).unwrap());
        let conn = rusqlite::Connection::open(&path).unwrap();
        let names: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('checkpoints')")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let expected: Vec<String> = log_fixture("columns.json")
            .as_array()
            .unwrap()
            .iter()
            .map(|column| column["name"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(names, expected);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn sqlite_row_for_the_shared_fixture_matches_the_shared_row_fixture() {
        let path = temp_db("row");
        let store = SqliteCheckpointStore::open(&path).unwrap();
        let fixture = load_fixture();
        store.save(fixture.clone()).await.unwrap();

        let conn = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            raw_row(&conn, &fixture.thread_id, 0),
            log_fixture("frozen.row.json")
        );

        drop(conn);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn sqlite_row_for_a_cancelled_checkpoint_matches_the_shared_row_fixture() {
        let path = temp_db("cancelled-row");
        let store = SqliteCheckpointStore::open(&path).unwrap();
        let raw = std::fs::read_to_string(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../schema/fixtures/checkpoint-lifecycle/cancelled.json"),
        )
        .unwrap();
        let fixture: Checkpoint = serde_json::from_str(&raw).unwrap();
        store.save(fixture.clone()).await.unwrap();

        let conn = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            raw_row(&conn, &fixture.thread_id, 0),
            log_fixture("cancelled.row.json")
        );
        assert_eq!(store.load(&fixture.thread_id).await.unwrap(), Some(fixture));

        drop(conn);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn sqlite_row_for_a_running_checkpoint_matches_the_shared_row_fixture() {
        // design/33-Durable-Execution.md: a status, and nothing new in the layout.
        let path = temp_db("running-row");
        let store = SqliteCheckpointStore::open(&path).unwrap();
        let raw = std::fs::read_to_string(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../schema/fixtures/checkpoint-lifecycle/running.json"),
        )
        .unwrap();
        let fixture: Checkpoint = serde_json::from_str(&raw).unwrap();
        store.save(fixture.clone()).await.unwrap();

        let conn = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            raw_row(&conn, &fixture.thread_id, 0),
            log_fixture("running.row.json")
        );
        assert_eq!(store.load(&fixture.thread_id).await.unwrap(), Some(fixture));

        drop(conn);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn sqlite_row_for_an_ended_checkpoint_matches_the_shared_row_fixture() {
        let path = temp_db("ended-row");
        let store = SqliteCheckpointStore::open(&path).unwrap();
        let raw = std::fs::read_to_string(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../schema/fixtures/checkpoint-lifecycle/ended.json"),
        )
        .unwrap();
        let fixture: Checkpoint = serde_json::from_str(&raw).unwrap();
        store.save(fixture.clone()).await.unwrap();

        let conn = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            raw_row(&conn, &fixture.thread_id, 0),
            log_fixture("ended.row.json")
        );
        assert_eq!(store.load(&fixture.thread_id).await.unwrap(), Some(fixture));

        drop(conn);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn sqlite_row_for_a_checkpoint_with_a_decision_holds_it_as_json_in_the_decision_column() {
        let path = temp_db("decision-row");
        let store = SqliteCheckpointStore::open(&path).unwrap();
        let raw = std::fs::read_to_string(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../schema/fixtures/checkpoint-lifecycle/decision-failed.json"),
        )
        .unwrap();
        let fixture: Checkpoint = serde_json::from_str(&raw).unwrap();
        store.save(fixture.clone()).await.unwrap();

        let conn = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            raw_row(&conn, &fixture.thread_id, 0),
            log_fixture("decision.row.json")
        );
        // And it reads back whole, decision included.
        assert_eq!(store.load(&fixture.thread_id).await.unwrap(), Some(fixture));

        drop(conn);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn sqlite_row_for_a_nested_checkpoint_holds_the_child_as_json_in_the_nested_column() {
        let path = temp_db("nested-row");
        let store = SqliteCheckpointStore::open(&path).unwrap();
        let raw = std::fs::read_to_string(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../schema/fixtures/checkpoint-lifecycle/nested.json"),
        )
        .unwrap();
        let fixture: Checkpoint = serde_json::from_str(&raw).unwrap();
        store.save(fixture.clone()).await.unwrap();

        let conn = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            raw_row(&conn, &fixture.thread_id, 0),
            log_fixture("nested.row.json")
        );
        // And it reads back whole, child included.
        assert_eq!(store.load(&fixture.thread_id).await.unwrap(), Some(fixture));

        drop(conn);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn sqlite_loads_a_row_inserted_as_raw_sql_from_the_shared_row_fixture() {
        // The reader side of the contract: a row laid out exactly as the
        // fixture says (which is what the .NET store and the CLI write) is
        // readable through `load`.
        let path = temp_db("reader");
        let store = SqliteCheckpointStore::open(&path).unwrap();
        let row = log_fixture("frozen.row.json");

        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO checkpoints
                (thread_id, sequence, schema_version, status, suspended_node, next_node,
                 frozen_state, metadata, created_at, resumed_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8, NULL)",
            rusqlite::params![
                row["thread_id"].as_str().unwrap(),
                row["sequence"].as_i64().unwrap(),
                row["schema_version"].as_str().unwrap(),
                row["status"].as_str().unwrap(),
                row["suspended_node"].as_str().unwrap(),
                row["next_node"].as_str().unwrap(),
                row["frozen_state"].to_string(),
                row["created_at"].as_str().unwrap(),
            ],
        )
        .unwrap();

        let fixture = load_fixture();
        assert_eq!(store.load(&fixture.thread_id).await.unwrap(), Some(fixture));

        drop(conn);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn sqlite_save_appends_to_the_log_and_load_returns_the_latest() {
        let path = temp_db("history");
        let store = SqliteCheckpointStore::open(&path).unwrap();
        let first = load_fixture();
        let mut resumed = first.clone();
        resumed.resumed_at = Some("2026-09-16T01:00:00Z".to_string());
        resumed.metadata = Some(HashMap::from([(
            "approved_by".to_string(),
            "x".to_string(),
        )]));

        store.save(first.clone()).await.unwrap();
        store.save(resumed.clone()).await.unwrap();

        assert_eq!(store.load(&first.thread_id).await.unwrap(), Some(resumed));
        let conn = rusqlite::Connection::open(&path).unwrap();
        let rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM checkpoints WHERE thread_id = ?1",
                [&first.thread_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, 2, "the earlier checkpoint stays on disk as history");
        // Sequences are per thread.
        assert_eq!(
            raw_row(&conn, &first.thread_id, 0)["resumed_at"],
            json!(null)
        );
        assert_eq!(
            raw_row(&conn, &first.thread_id, 1)["metadata"],
            json!({ "approved_by": "x" })
        );

        drop(conn);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    // --- The `checkpoint_threads` head table (design/19-Async-Queryable-Store.md):
    // its layout, the trigger that keeps it current whoever writes the log, and
    // the one-time backfill of a log that predates it.

    fn insert_raw_row(conn: &rusqlite::Connection, row: &serde_json::Value) {
        let json_or_null =
            |value: &serde_json::Value| (!value.is_null()).then(|| value.to_string());
        conn.execute(
            "INSERT INTO checkpoints
                (thread_id, sequence, schema_version, status, suspended_node, next_node,
                 frozen_state, metadata, created_at, resumed_at, nested, decision, wait, wake_at,
                 graph_id, graph_version)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            rusqlite::params![
                row["thread_id"].as_str(),
                row["sequence"].as_i64(),
                row["schema_version"].as_str(),
                row["status"].as_str(),
                row["suspended_node"].as_str(),
                row["next_node"].as_str(),
                row["frozen_state"].to_string(),
                json_or_null(&row["metadata"]),
                row["created_at"].as_str(),
                row["resumed_at"].as_str(),
                json_or_null(&row["nested"]),
                json_or_null(&row["decision"]),
                json_or_null(&row["wait"]),
                row["wake_at"].as_str(),
                row["graph_id"].as_str(),
                row["graph_version"].as_str(),
            ],
        )
        .unwrap();
    }

    fn head_rows(conn: &rusqlite::Connection) -> serde_json::Value {
        let mut stmt = conn
            .prepare(
                "SELECT thread_id, sequence, state, updated_at, metadata, wake_at, graph_id, graph_version
                 FROM checkpoint_threads ORDER BY thread_id",
            )
            .unwrap();
        let rows: Vec<serde_json::Value> = stmt
            .query_map([], |row| {
                let metadata: Option<String> = row.get(4)?;
                Ok(json!({
                    "thread_id": row.get::<_, String>(0)?,
                    "sequence": row.get::<_, i64>(1)?,
                    "state": row.get::<_, String>(2)?,
                    "updated_at": row.get::<_, String>(3)?,
                    "metadata": metadata.map(|m| serde_json::from_str::<serde_json::Value>(&m).unwrap()),
                    "wake_at": row.get::<_, Option<String>>(5)?,
                    "graph_id": row.get::<_, Option<String>>(6)?,
                    "graph_version": row.get::<_, Option<String>>(7)?,
                }))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        json!(rows)
    }

    #[tokio::test]
    async fn sqlite_head_table_layout_matches_the_shared_column_fixture() {
        let path = temp_db("head-layout");
        drop(SqliteCheckpointStore::open(&path).unwrap());

        let conn = rusqlite::Connection::open(&path).unwrap();
        let mut stmt = conn
            .prepare("PRAGMA table_info(checkpoint_threads)")
            .unwrap();
        let columns: Vec<serde_json::Value> = stmt
            .query_map([], |row| {
                Ok(json!({
                    "name": row.get::<_, String>(1)?,
                    "type": row.get::<_, String>(2)?,
                    "notnull": row.get::<_, i64>(3)? != 0,
                    "pk": row.get::<_, i64>(5)?,
                }))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(json!(columns), log_fixture("threads-columns.json"));

        drop(stmt);
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn sqlite_trigger_keeps_the_head_table_current_whoever_writes_the_log() {
        // Raw inserts, not `save`: the head row must come from the database
        // itself, so an older runtime or any other writer keeps it right.
        let path = temp_db("trigger");
        let store = SqliteCheckpointStore::open(&path).unwrap();
        let rows = log_fixture("threads.rows.json");

        let conn = rusqlite::Connection::open(&path).unwrap();
        for row in rows["inserts"].as_array().unwrap() {
            insert_raw_row(&conn, row);
        }
        assert_eq!(head_rows(&conn), rows["threads"]);

        let page = store
            .list_threads(
                &ThreadQuery::default()
                    .with_states([datalium_strata::checkpoint::ThreadState::Resumed]),
            )
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

        drop(conn);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn sqlite_log_from_before_the_head_table_is_backfilled_once_when_opened() {
        let path = temp_db("backfill");
        let rows = log_fixture("threads.rows.json");
        {
            // A log as the previous version left it: no head table, no trigger.
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(CHECKPOINT_LOG_DDL).unwrap();
            for row in rows["inserts"].as_array().unwrap() {
                insert_raw_row(&conn, row);
            }
        }

        let store = SqliteCheckpointStore::open(&path).unwrap();
        let conn = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(head_rows(&conn), rows["threads"]);

        // Opening again doesn't backfill twice, and the trigger has taken over.
        drop(SqliteCheckpointStore::open(&path).unwrap());
        let mut next = load_fixture();
        next.thread_id = "t-late".to_string();
        next.created_at = "2026-09-17T00:00:00Z".to_string();
        store.save(next).await.unwrap();
        let late: (i64, String) = conn
            .query_row(
                "SELECT sequence, state FROM checkpoint_threads WHERE thread_id = 't-late'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(late, (2, "interrupted".to_string()));

        drop(conn);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    fn lifecycle(name: &str) -> Checkpoint {
        serde_json::from_str(
            &std::fs::read_to_string(
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../../schema/fixtures/checkpoint-lifecycle")
                    .join(name),
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn sqlite_row_for_a_waiting_checkpoint_holds_its_wait_and_wake_at() {
        let path = temp_db("waiting-row");
        let store = SqliteCheckpointStore::open(&path).unwrap();
        let fixture = lifecycle("waiting-event.json");
        store.save(fixture.clone()).await.unwrap();

        let conn = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            raw_row(&conn, &fixture.thread_id, 0),
            log_fixture("waiting.row.json")
        );
        assert_eq!(store.load(&fixture.thread_id).await.unwrap(), Some(fixture));

        drop(conn);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn sqlite_row_for_a_nested_wait_derives_the_innermost_wake_at() {
        let path = temp_db("nested-wait-row");
        let store = SqliteCheckpointStore::open(&path).unwrap();
        let fixture = lifecycle("nested-timer.json");
        store.save(fixture.clone()).await.unwrap();

        let conn = rusqlite::Connection::open(&path).unwrap();
        let row = raw_row(&conn, &fixture.thread_id, 0);
        // The outermost level doesn't wait; the derived column is the child's.
        assert_eq!(row["wait"], serde_json::Value::Null);
        assert_eq!(row["wake_at"], "2026-10-06T12:05:00Z");
        assert_eq!(store.load(&fixture.thread_id).await.unwrap(), Some(fixture));

        drop(conn);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    /// The `checkpoints` table and its head table as they were before waits
    /// (`design/29-Event-Waits-And-Timers.md`): no `wait`, no `wake_at`, and a
    /// trigger that doesn't copy it.
    const PRE_WAIT_DDL: &str = "CREATE TABLE checkpoints (
    thread_id TEXT NOT NULL, sequence INTEGER NOT NULL, schema_version TEXT NOT NULL,
    status TEXT NOT NULL, suspended_node TEXT, next_node TEXT, frozen_state TEXT NOT NULL,
    metadata TEXT, created_at TEXT NOT NULL, resumed_at TEXT, nested TEXT, decision TEXT,
    PRIMARY KEY (thread_id, sequence)
);
CREATE TABLE checkpoint_threads (
    thread_id TEXT NOT NULL PRIMARY KEY, sequence INTEGER NOT NULL, state TEXT NOT NULL,
    updated_at TEXT NOT NULL, metadata TEXT
);
CREATE TRIGGER checkpoints_head AFTER INSERT ON checkpoints
BEGIN
    INSERT INTO checkpoint_threads (thread_id, sequence, state, updated_at, metadata)
    VALUES (NEW.thread_id, NEW.sequence,
            CASE WHEN NEW.resumed_at IS NOT NULL THEN 'resumed' ELSE NEW.status END,
            COALESCE(NEW.resumed_at, NEW.created_at), NEW.metadata)
    ON CONFLICT (thread_id) DO UPDATE SET
        sequence = excluded.sequence, state = excluded.state,
        updated_at = excluded.updated_at, metadata = excluded.metadata
    WHERE excluded.sequence > checkpoint_threads.sequence;
END;
INSERT INTO checkpoints (thread_id, sequence, schema_version, status, suspended_node, next_node,
    frozen_state, metadata, created_at, resumed_at)
VALUES ('old', 0, '1.0.0', 'interrupted', 'Approve', 'Execute', '{}', NULL, '2026-09-16T00:00:00Z', NULL);";

    #[tokio::test]
    async fn opening_a_log_from_before_waits_adds_the_columns_and_replaces_the_trigger() {
        let path = temp_db("pre-wait");
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(PRE_WAIT_DDL).unwrap();
        }

        let store = SqliteCheckpointStore::open(&path).unwrap();
        // Opening again is a no-op.
        drop(SqliteCheckpointStore::open(&path).unwrap());
        let conn = rusqlite::Connection::open(&path).unwrap();
        let columns = |table: &str| -> Vec<String> {
            let mut stmt = conn
                .prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))
                .unwrap();
            stmt.query_map([], |row| row.get(0))
                .unwrap()
                .map(Result::unwrap)
                .collect()
        };
        let names = |fixture: &str| -> Vec<String> {
            log_fixture(fixture)
                .as_array()
                .unwrap()
                .iter()
                .map(|column| column["name"].as_str().unwrap().to_string())
                .collect()
        };
        assert_eq!(columns("checkpoints"), names("columns.json"));
        assert_eq!(columns("checkpoint_threads"), names("threads-columns.json"));

        // The old thread is still listed; a waiting one is due through the new trigger.
        store.save(lifecycle("nested-timer.json")).await.unwrap();
        let due =
            store
                .list_threads(&ThreadQuery::default().due_by(
                    datalium_strata::store::parse_timestamp("2026-10-06T12:05:00Z").unwrap(),
                ))
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

        drop(conn);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn sqlite_row_for_an_identified_checkpoint_matches_the_shared_row_fixture() {
        let path = temp_db("identified-row");
        let store = SqliteCheckpointStore::open(&path).unwrap();
        let fixture = lifecycle("identified.json");
        let nested = lifecycle("nested-identified.json");
        store.save(fixture.clone()).await.unwrap();
        store.save(nested.clone()).await.unwrap();

        let conn = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            raw_row(&conn, &fixture.thread_id, 0),
            log_fixture("identified.row.json")
        );
        // The columns hold the outermost level's identity; the child's is
        // inside `nested` (design/31-Graph-Identity.md).
        let row = raw_row(&conn, &nested.thread_id, 0);
        assert_eq!(row["graph_id"], "incident");
        assert_eq!(row["nested"]["graph"]["id"], "approval");
        assert_eq!(
            store.load(&fixture.thread_id).await.unwrap(),
            Some(fixture.clone())
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
        assert_eq!(ids, [fixture.thread_id.as_str()]);

        drop(conn);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    /// The `checkpoints` table and its head table as they were before graph
    /// identities (`design/31-Graph-Identity.md`): no `graph_id` or
    /// `graph_version`, and a trigger that doesn't copy them.
    const PRE_IDENTITY_DDL: &str = "CREATE TABLE checkpoints (
    thread_id TEXT NOT NULL, sequence INTEGER NOT NULL, schema_version TEXT NOT NULL,
    status TEXT NOT NULL, suspended_node TEXT, next_node TEXT, frozen_state TEXT NOT NULL,
    metadata TEXT, created_at TEXT NOT NULL, resumed_at TEXT, nested TEXT, decision TEXT,
    wait TEXT, wake_at TEXT,
    PRIMARY KEY (thread_id, sequence)
);
CREATE TABLE checkpoint_threads (
    thread_id TEXT NOT NULL PRIMARY KEY, sequence INTEGER NOT NULL, state TEXT NOT NULL,
    updated_at TEXT NOT NULL, metadata TEXT, wake_at TEXT
);
CREATE TRIGGER checkpoints_head AFTER INSERT ON checkpoints
BEGIN
    INSERT INTO checkpoint_threads (thread_id, sequence, state, updated_at, metadata, wake_at)
    VALUES (NEW.thread_id, NEW.sequence,
            CASE WHEN NEW.resumed_at IS NOT NULL THEN 'resumed' ELSE NEW.status END,
            COALESCE(NEW.resumed_at, NEW.created_at), NEW.metadata, NEW.wake_at)
    ON CONFLICT (thread_id) DO UPDATE SET
        sequence = excluded.sequence, state = excluded.state,
        updated_at = excluded.updated_at, metadata = excluded.metadata, wake_at = excluded.wake_at
    WHERE excluded.sequence > checkpoint_threads.sequence;
END;
INSERT INTO checkpoints (thread_id, sequence, schema_version, status, suspended_node, next_node,
    frozen_state, metadata, created_at, resumed_at)
VALUES ('old', 0, '1.0.0', 'interrupted', 'Approve', 'Execute', '{}', NULL, '2026-09-16T00:00:00Z', NULL);";

    #[tokio::test]
    async fn opening_a_log_from_before_graph_identities_adds_the_columns_and_replaces_the_trigger()
    {
        let path = temp_db("pre-identity");
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(PRE_IDENTITY_DDL).unwrap();
        }

        let store = SqliteCheckpointStore::open(&path).unwrap();
        // Opening again is a no-op.
        drop(SqliteCheckpointStore::open(&path).unwrap());
        let conn = rusqlite::Connection::open(&path).unwrap();
        let columns = |table: &str| -> Vec<String> {
            let mut stmt = conn
                .prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))
                .unwrap();
            stmt.query_map([], |row| row.get(0))
                .unwrap()
                .map(Result::unwrap)
                .collect()
        };
        let names = |fixture: &str| -> Vec<String> {
            log_fixture(fixture)
                .as_array()
                .unwrap()
                .iter()
                .map(|column| column["name"].as_str().unwrap().to_string())
                .collect()
        };
        assert_eq!(columns("checkpoints"), names("columns.json"));
        assert_eq!(columns("checkpoint_threads"), names("threads-columns.json"));

        // The old thread is still listed and has no identity; an identified one
        // is listed by its graph through the new trigger.
        store.save(lifecycle("identified.json")).await.unwrap();
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
        let all = store
            .list_threads(&ThreadQuery::default())
            .await
            .unwrap()
            .threads;
        assert_eq!(all.len(), 2);
        assert_eq!(
            all.iter().find(|t| t.thread_id == "old").unwrap().graph,
            None
        );

        drop(conn);
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn in_memory_store_keeps_every_checkpoint_of_a_thread() {
        let store = InMemoryCheckpointStore::new();
        let first = sample_checkpoint("thread_1");
        let mut stamp = first.clone();
        stamp.resumed_at = Some("2026-09-16T01:00:00Z".to_string());
        store.save(first.clone()).await.unwrap();
        store.save(stamp.clone()).await.unwrap();

        let log = store.get_log("thread_1", None, 10).await.unwrap();
        let kept: Vec<Checkpoint> = log.entries.into_iter().map(|e| e.checkpoint).collect();
        assert_eq!(kept, [first, stamp]);
    }
}
