//! Postgres `CheckpointStore` and `HistoryStore` adapters — the "Redis/PostgreSQL"
//! tier from `design/03-Architecture.md`'s memory blueprint, and the first
//! backend that more than one process can write to at once.
//!
//! Backed by `tokio-postgres`, so the engine and async handlers await it
//! directly (`design/19-Async-Queryable-Store.md`). A store built with
//! `connect` keeps a pool of connections (`deadpool-postgres`, 16 by default,
//! [`PostgresStoreOptions::with_pool_size`]); one built from a caller's client
//! has that one, behind a `tokio::sync::Mutex`. Connection tasks are spawned on
//! the runtime that opens them, so **create a store inside the runtime that will
//! use it**.
//!
//! **Group commit** (`design/41-Checkpoint-Efficiency.md`):
//! [`PostgresStoreOptions::with_group_commit`] flushes saves that arrive while
//! others are being written together, in one transaction, each keeping its own
//! answer.
//!
//! **Hardening** (`design/34-Storage-Hardening.md`). Every call runs under a
//! [`StoreRetry`]: a transient failure (a dropped connection, a server
//! shutting down, a deadlock) is repeated, and a save that may have been
//! written is looked up before it is made again, so it happens once. A store
//! built with `connect` / `connect_with` replaces a connection that has
//! dropped (the pool's recycling). What a database holds is recorded in `strata_schema_version`
//! ([`crate::schema_versions`]): a store brings an older database up to date
//! when it is created (or only checks it, with
//! [`PostgresStoreOptions::with_auto_migrate`]), and refuses one a newer
//! build has moved on. [`CheckpointRetention::purge`] deletes finished
//! threads' logs.
//!
//! **Layout.** Both tables are the SQLite ones with Postgres types (see
//! `schema/README.md`, "Postgres checkpoint log"): `sequence` is `BIGINT`, and
//! `frozen_state`/`metadata` are `JSONB` rather than JSON text. `JSONB` means
//! key order and insignificant whitespace are not preserved on the way back out
//! — harmless, since state is deserialized by field name, but it is not
//! byte-identical to what was saved. Beside the log, the `checkpoint_threads`
//! head table (kept current by a trigger) is what listing threads reads.
//! `Datalium.Strata.Storage`'s `PostgresCheckpointStore` creates the same
//! tables, so a thread frozen by one runtime can be resumed by the other.
//!
//! **Concurrency.** Unlike the SQLite adapters, more than one writer is
//! supported: `save`/`append` take a per-thread (per-session) transaction-scoped
//! advisory lock before choosing the next `sequence`, so concurrent writers to
//! one thread serialize instead of colliding on the primary key. The lock is
//! taken with `pg_advisory_xact_lock(hashtextextended(..))`, which is computed
//! server-side, so both runtimes contend on the same lock.
//!
//! **Not built:** TLS (`connect` uses `NoTls`; build a `tokio_postgres::Client`
//! with your own connector and pass it to `from_client`, which then can't
//! reconnect or pool). Postgres `TEXT` cannot hold a NUL
//! character, so a history message containing one is rejected with a store
//! error.

use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use datalium_strata::BoxFuture;
use datalium_strata::checkpoint::{
    CheckpointLogEntry, CheckpointLogPage, CheckpointQuery, CheckpointRetention, CheckpointStore,
    CheckpointStoreError, LatestCheckpoint, LeaseAttempt, PurgeStates, ThreadCursor, ThreadLease,
    ThreadPage, ThreadQuery, ThreadSummary,
};
use datalium_strata::history::{ChatMessage, HistoryStore, HistoryStoreError, MessageRole};
use datalium_strata::schema::{Checkpoint, GraphIdentity};
use datalium_strata::store::{
    LeasedSave, check_lease, check_lease_thread, check_lease_ttl, check_limit, lease_millis,
    timestamp_text,
};
use deadpool_postgres::{Manager, ManagerConfig, Object, Pool, PoolError, RecyclingMethod};
use tokio::sync::{Mutex, MutexGuard};
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Config, NoTls, Row, Statement, Transaction};

use crate::deltas::{
    self, CheckpointWriter, Decoded, Deltas, ENCODED_VERSIONS_SQL, Encoding, depth_of,
    listed_version,
};
use crate::group_commit::{Flush, GroupCommit, Queue, Row as FlushRow, Verdict, record_saved};
use crate::offload::{self, Blob, BlobStore, Offload};
use crate::retry::{Failure, FaultHook, Retrier, SaveFate, StoreRetry, fate_of, fault_at};
use crate::schema_versions::{self, SchemaStep};
use crate::support::{
    ACQUIRE_ATTEMPTS, from_millis, json_err, lease_kept_changing, parse_status, status_str,
};

/// The canonical Postgres `checkpoints` table. Identical to
/// `Datalium.Strata.Storage`'s `PostgresCheckpointStore.CheckpointLogDdl`; each
/// language's tests compare the created table against
/// `schema/fixtures/checkpoint-log/postgres-columns.json`.
pub const POSTGRES_CHECKPOINT_LOG_DDL: &str = "CREATE TABLE IF NOT EXISTS checkpoints (
    thread_id      TEXT NOT NULL,
    sequence       BIGINT NOT NULL,
    schema_version TEXT NOT NULL,
    status         TEXT NOT NULL,
    suspended_node TEXT,
    next_node      TEXT,
    frozen_state   JSONB NOT NULL,
    metadata       JSONB,
    created_at     TEXT NOT NULL,
    resumed_at     TEXT,
    nested         JSONB,
    decision       JSONB,
    wait           JSONB,
    wake_at        TEXT,
    graph_id       TEXT,
    graph_version  TEXT,
    PRIMARY KEY (thread_id, sequence)
)";

/// Adds the `nested` column to a `checkpoints` table created before nested
/// checkpoints (`design/16-Nested-Interrupts.md`), the `decision` column to
/// one created before recorded decisions (`design/21-Typed-Human-Signals.md`),
/// the `wait` and `wake_at` columns to one created before waits
/// (`design/29-Event-Waits-And-Timers.md`), and the `graph_id` and
/// `graph_version` columns to one created before graph identities
/// (`design/31-Graph-Identity.md`), which `CREATE TABLE IF NOT EXISTS` leaves
/// as it is. Existing rows keep `NULL`, which is right: none has a child, a
/// decision, a wait or an identity. A no-op on a current table. Identical to
/// .NET's `PostgresCheckpointStore.CheckpointLogMigration`.
pub const POSTGRES_CHECKPOINT_LOG_MIGRATION: &str = "ALTER TABLE checkpoints ADD COLUMN IF NOT EXISTS nested JSONB, ADD COLUMN IF NOT EXISTS decision JSONB, ADD COLUMN IF NOT EXISTS wait JSONB, ADD COLUMN IF NOT EXISTS wake_at TEXT, ADD COLUMN IF NOT EXISTS graph_id TEXT, ADD COLUMN IF NOT EXISTS graph_version TEXT";

/// Adds `wake_at` to a head table created before waits
/// (`design/29-Event-Waits-And-Timers.md`), and `graph_id` and
/// `graph_version` to one created before graph identities
/// (`design/31-Graph-Identity.md`); [`POSTGRES_CHECKPOINT_THREADS_DDL`] then
/// replaces its trigger. Identical to .NET's
/// `PostgresCheckpointStore.CheckpointThreadsMigration`.
pub const POSTGRES_CHECKPOINT_THREADS_MIGRATION: &str = r#"ALTER TABLE checkpoint_threads ADD COLUMN IF NOT EXISTS wake_at TEXT COLLATE "C", ADD COLUMN IF NOT EXISTS graph_id TEXT, ADD COLUMN IF NOT EXISTS graph_version TEXT"#;

/// The head table listing threads reads (`design/19-Async-Queryable-Store.md`,
/// "SQL: a trigger-maintained head table"), its indexes, and the trigger that
/// keeps it current whoever writes the log. `thread_id` and `updated_at` use
/// the `"C"` collation, so threads sort byte-wise as in every other store.
/// Identical to `PostgresCheckpointStore.CheckpointThreadsDdl` in
/// `Datalium.Strata.Storage`; pinned by
/// `schema/fixtures/checkpoint-log/postgres-threads-columns.json`.
pub const POSTGRES_CHECKPOINT_THREADS_DDL: &str = r#"CREATE TABLE IF NOT EXISTS checkpoint_threads (
    thread_id  TEXT COLLATE "C" NOT NULL PRIMARY KEY,
    sequence   BIGINT NOT NULL,
    state      TEXT NOT NULL,
    updated_at TEXT COLLATE "C" NOT NULL,
    metadata      JSONB,
    wake_at       TEXT COLLATE "C",
    graph_id      TEXT,
    graph_version TEXT
);
CREATE INDEX IF NOT EXISTS checkpoint_threads_by_state
    ON checkpoint_threads (state, updated_at, thread_id);
CREATE INDEX IF NOT EXISTS checkpoint_threads_by_updated
    ON checkpoint_threads (updated_at, thread_id);
CREATE INDEX IF NOT EXISTS checkpoint_threads_by_metadata
    ON checkpoint_threads USING GIN (metadata jsonb_path_ops);
CREATE INDEX IF NOT EXISTS checkpoint_threads_by_wake
    ON checkpoint_threads (wake_at);
CREATE INDEX IF NOT EXISTS checkpoint_threads_by_graph
    ON checkpoint_threads (graph_id, updated_at, thread_id);
CREATE OR REPLACE FUNCTION strata_checkpoints_head() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO checkpoint_threads AS h (thread_id, sequence, state, updated_at, metadata, wake_at, graph_id, graph_version)
    VALUES (NEW.thread_id, NEW.sequence,
            CASE WHEN NEW.resumed_at IS NOT NULL THEN 'resumed' ELSE NEW.status END,
            COALESCE(NEW.resumed_at, NEW.created_at), NEW.metadata, NEW.wake_at, NEW.graph_id, NEW.graph_version)
    ON CONFLICT (thread_id) DO UPDATE SET
        sequence = EXCLUDED.sequence,
        state = EXCLUDED.state,
        updated_at = EXCLUDED.updated_at,
        metadata = EXCLUDED.metadata,
        wake_at = EXCLUDED.wake_at,
        graph_id = EXCLUDED.graph_id,
        graph_version = EXCLUDED.graph_version
    WHERE EXCLUDED.sequence > h.sequence;
    RETURN NULL;
END;
$$;
CREATE OR REPLACE TRIGGER checkpoints_head AFTER INSERT ON checkpoints
    FOR EACH ROW EXECUTE FUNCTION strata_checkpoints_head();"#;

/// Fills a just-created `checkpoint_threads` from the log it was added to.
/// Identical to `PostgresCheckpointStore.CheckpointThreadsBackfill`.
pub const POSTGRES_CHECKPOINT_THREADS_BACKFILL: &str =
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
/// `expires_at` is set and after `clock_timestamp()`, the server's clock; a
/// release clears it and `owner`. Identical to
/// `PostgresCheckpointStore.CheckpointLeasesDdl`; pinned by
/// `schema/fixtures/checkpoint-log/postgres-leases-columns.json`.
pub const POSTGRES_CHECKPOINT_LEASES_DDL: &str = "CREATE TABLE IF NOT EXISTS checkpoint_leases (
    thread_id  TEXT NOT NULL PRIMARY KEY,
    owner      TEXT,
    token      BIGINT NOT NULL,
    expires_at TIMESTAMPTZ
)";

/// The blobs of value offload (`design/41-Checkpoint-Efficiency.md`,
/// "Offload"): each value a store offloaded to its own database, by thread and
/// `sha256:` hash, as its JSON text. The checkpoint component's schema version
/// 2. Identical to `PostgresCheckpointStore.CheckpointBlobsDdl`; pinned by
/// `schema/fixtures/checkpoint-log/postgres-blobs-columns.json`.
pub const POSTGRES_CHECKPOINT_BLOBS_DDL: &str = "CREATE TABLE IF NOT EXISTS checkpoint_blobs (
    thread_id TEXT NOT NULL,
    hash      TEXT NOT NULL,
    content   BYTEA NOT NULL,
    PRIMARY KEY (thread_id, hash)
)";

/// A lease as every lease statement returns it: its expiry as Unix
/// milliseconds, so no date type has to cross the wire.
const LEASE_COLUMNS: &str =
    "owner, token, floor(extract(epoch from expires_at) * 1000)::bigint AS expires_at";

/// The Postgres `history_records` table. Identical to
/// `PostgresHistoryStore.HistoryLogDdl` in `Datalium.Strata.Storage`.
pub const POSTGRES_HISTORY_DDL: &str = "CREATE TABLE IF NOT EXISTS history_records (
    session_id TEXT NOT NULL,
    sequence   BIGINT NOT NULL,
    role       TEXT NOT NULL,
    content    TEXT NOT NULL,
    timestamp  TEXT NOT NULL,
    PRIMARY KEY (session_id, sequence)
)";

/// Held while a store creates or migrates its tables. `CREATE TABLE IF NOT
/// EXISTS` is not safe to run from two sessions at once in Postgres (the loser
/// can fail on `pg_type`'s unique index instead of being a no-op), and opening
/// two stores — or one from each runtime — at the same moment is exactly what
/// happens on a shared database.
const DDL_LOCK_KEY: i64 = 0x5354_5241_5441; // "STRATA"

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// The per-thread lock a save, a lease acquisition and a purge take.
const THREAD_LOCK: &str =
    "SELECT pg_advisory_xact_lock(hashtextextended('checkpoints:' || $1::text, 0))";

/// A save's statement, under [`THREAD_LOCK`]: the row at the next sequence,
/// or only if `$12` is still the thread's latest, and, for a save that relies
/// on the row before it, only if that row is still the latest, by its sequence
/// (`$17`) and its `created_at` (`$18`); then, only if the row was appended,
/// its blobs (`$19` hashes, `$20` contents), a blob the thread has already
/// left as it is. Returns the sequence it was appended at. Identical to the
/// .NET `InsertSql`.
const INSERT_SQL: &str = "WITH appended AS (
         INSERT INTO checkpoints
             (thread_id, sequence, schema_version, status, suspended_node, next_node,
              frozen_state, metadata, created_at, resumed_at, nested, decision, wait, wake_at,
              graph_id, graph_version)
         SELECT $1, latest.sequence + 1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $13, $14, $15, $16
         FROM (SELECT COALESCE(MAX(sequence), -1) AS sequence FROM checkpoints WHERE thread_id = $1) latest
         WHERE ($12::bigint IS NULL OR latest.sequence = $12::bigint)
           AND ($17::bigint IS NULL OR (latest.sequence = $17::bigint AND EXISTS (
               SELECT 1 FROM checkpoints b
               WHERE b.thread_id = $1 AND b.sequence = $17::bigint AND b.created_at = $18::text)))
         RETURNING sequence
     ), blobs AS (
         INSERT INTO checkpoint_blobs (thread_id, hash, content)
         SELECT $1, b.hash, b.content FROM unnest($19::text[], $20::bytea[]) AS b(hash, content)
         WHERE EXISTS (SELECT 1 FROM appended)
         ON CONFLICT DO NOTHING
     )
     SELECT sequence FROM appended";

/// The rows a load of one row reads: it (`l`, `$1`'s row picked by the
/// target) and the rows before it that it is a delta of
/// (`design/41-Checkpoint-Efficiency.md`, "Deltas"), oldest first, in one
/// read. A row's depth says how many rows before it that is.
fn chain_sql(target: &str) -> String {
    format!(
        "WITH target AS (
             SELECT l.sequence,
                    CASE WHEN l.schema_version IN ({ENCODED_VERSIONS_SQL})
                         THEN (l.frozen_state->>'depth')::bigint ELSE 0 END AS depth
             FROM checkpoints l WHERE l.thread_id = $1 {target})
         SELECT {CHECKPOINT_COLUMNS} FROM checkpoints c, target
         WHERE c.thread_id = $1 AND c.sequence BETWEEN target.sequence - target.depth AND target.sequence
         ORDER BY c.sequence"
    )
}

/// Takes the advisory lock of every thread of a flush, in the order of their
/// keys, so flushes and single saves never wait on each other in a cycle. The
/// key is [`THREAD_LOCK`]'s. Identical to the .NET `FlushLockSql`.
const FLUSH_LOCK_SQL: &str = "SELECT pg_advisory_xact_lock(k)
     FROM (SELECT DISTINCT hashtextextended('checkpoints' || ':' || t, 0) AS k FROM unnest($1::text[]) AS t) keys
     ORDER BY k";

/// One wave of a flush: rows of distinct threads, whose advisory locks are
/// already held, given as one array per column. Each row reads its thread's
/// latest sequence and lease token, gets the verdict a single save would (a
/// lost lease first, then a moved sequence), and is appended only when it is
/// saved. A row that relies on a base that isn't the latest any more (by
/// sequence and `created_at`) is `rebase`: not appended, and written whole by
/// the flush after. The blobs of the rows saved (`$21` to `$23`, each with its
/// row's position) are written with them. Returns each row's position,
/// verdict and the sequence it got. One statement, so one plan, however many
/// rows; a later wave sees an earlier one's rows. Identical to the .NET
/// `FlushWaveSql`.
const FLUSH_WAVE_SQL: &str = "WITH rows AS (
            SELECT * FROM unnest(
                $1::int[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::jsonb[], $8::jsonb[],
                $9::text[], $10::text[], $11::jsonb[], $12::jsonb[], $13::jsonb[], $14::text[], $15::text[], $16::text[],
                $17::bigint[], $18::bigint[], $19::bigint[], $20::text[])
                AS r(ord, thread_id, schema_version, status, suspended_node, next_node, frozen_state, metadata,
                     created_at, resumed_at, nested, decision, wait, wake_at, graph_id, graph_version, expected, token,
                     base, base_created)
        ), verdict AS (
            SELECT r.*, latest.sequence AS latest,
                   CASE
                       WHEN r.token IS NOT NULL
                            AND (SELECT l.token FROM checkpoint_leases l WHERE l.thread_id = r.thread_id) IS DISTINCT FROM r.token
                           THEN 'lease_lost'
                       WHEN r.expected IS NOT NULL AND latest.sequence <> r.expected THEN 'lost'
                       WHEN r.base IS NOT NULL AND (latest.sequence <> r.base OR NOT EXISTS (
                           SELECT 1 FROM checkpoints b
                           WHERE b.thread_id = r.thread_id AND b.sequence = r.base AND b.created_at = r.base_created))
                           THEN 'rebase'
                       ELSE 'saved'
                   END AS outcome
            FROM rows r
            CROSS JOIN LATERAL (
                SELECT COALESCE(MAX(c.sequence), -1) AS sequence FROM checkpoints c WHERE c.thread_id = r.thread_id
            ) latest
        ), blobs AS (
            INSERT INTO checkpoint_blobs (thread_id, hash, content)
            SELECT v.thread_id, b.hash, b.content
            FROM unnest($21::int[], $22::text[], $23::bytea[]) AS b(ord, hash, content)
            JOIN verdict v ON v.ord = b.ord
            WHERE v.outcome = 'saved'
            ON CONFLICT DO NOTHING
        ), inserted AS (
            INSERT INTO checkpoints
                (thread_id, sequence, schema_version, status, suspended_node, next_node,
                 frozen_state, metadata, created_at, resumed_at, nested, decision, wait, wake_at,
                 graph_id, graph_version)
            SELECT thread_id, latest + 1, schema_version, status, suspended_node, next_node,
                   frozen_state, metadata, created_at, resumed_at, nested, decision, wait, wake_at,
                   graph_id, graph_version
            FROM verdict WHERE outcome = 'saved'
        )
        SELECT ord, outcome, latest + 1 FROM verdict ORDER BY ord";

/// Options of the Postgres stores (`design/34-Storage-Hardening.md`). Mirrors
/// the .NET `PostgresStoreOptions`.
#[derive(Clone)]
pub struct PostgresStoreOptions {
    retry: StoreRetry,
    auto_migrate: bool,
    pool_size: usize,
    group_commit: Option<GroupCommit>,
    deltas: Option<Deltas>,
    offload: Option<Offload>,
    fault: Option<FaultHook>,
    later_steps: Vec<SchemaStep>,
}

/// How many connections a store built with `connect` keeps at most.
pub const DEFAULT_POOL_SIZE: usize = 16;

impl Default for PostgresStoreOptions {
    fn default() -> Self {
        Self {
            retry: StoreRetry::new(),
            auto_migrate: true,
            pool_size: DEFAULT_POOL_SIZE,
            group_commit: None,
            deltas: None,
            offload: None,
            fault: None,
            later_steps: Vec::new(),
        }
    }
}

impl std::fmt::Debug for PostgresStoreOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresStoreOptions")
            .field("retry", &self.retry)
            .field("auto_migrate", &self.auto_migrate)
            .field("pool_size", &self.pool_size)
            .field("group_commit", &self.group_commit)
            .field("deltas", &self.deltas)
            .field("offload", &self.offload)
            .finish_non_exhaustive()
    }
}

impl PostgresStoreOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// How a call that failed for a transient reason is repeated.
    /// [`StoreRetry::none`] turns it off.
    pub fn with_retry(mut self, retry: StoreRetry) -> Self {
        self.retry = retry;
        self
    }

    /// Whether the store brings its tables up to date when it is created (the
    /// default). When `false` it only checks them, and refuses a database
    /// that is behind: a deploy step then calls `migrate` on a store created
    /// with the default.
    pub fn with_auto_migrate(mut self, auto_migrate: bool) -> Self {
        self.auto_migrate = auto_migrate;
        self
    }

    /// How many connections a store built with `connect` keeps at most
    /// ([`DEFAULT_POOL_SIZE`] by default). At least 1. A store built from a
    /// client has that one.
    pub fn with_pool_size(mut self, pool_size: usize) -> Self {
        self.pool_size = pool_size;
        self
    }

    /// Flushes saves that arrive while others are being written together, in
    /// one transaction (`design/41-Checkpoint-Efficiency.md`). Off by default.
    /// The history store ignores it.
    pub fn with_group_commit(mut self, group_commit: GroupCommit) -> Self {
        self.group_commit = Some(group_commit);
        self
    }

    /// The group commit the checkpoint store runs, if any.
    pub fn group_commit(&self) -> Option<&GroupCommit> {
        self.group_commit.as_ref()
    }

    /// The pool size a store built with `connect` uses.
    pub fn pool_size(&self) -> usize {
        self.pool_size
    }

    /// Stores a `running` checkpoint as the changes to the thread's previous
    /// row, with a whole row every [`Deltas::with_full_every`]
    /// (`design/41-Checkpoint-Efficiency.md`, "Deltas"). Off by default. A
    /// store reads rows stored this way whatever its own options say. The
    /// history store ignores it.
    pub fn with_deltas(mut self, deltas: Deltas) -> Self {
        self.deltas = Some(deltas);
        self
    }

    /// The deltas the checkpoint store writes, if any.
    pub fn deltas(&self) -> Option<&Deltas> {
        self.deltas.as_ref()
    }

    /// Stores each top-level value of a level's state whose JSON is at least
    /// [`Offload::with_threshold`] bytes once, in a blob store, and refers to
    /// it (`design/41-Checkpoint-Efficiency.md`, "Offload"). Off by default.
    /// The blobs are read from where these options say, whatever the
    /// threshold: this database's `checkpoint_blobs` unless
    /// [`Offload::with_blobs`] names another store. The history store ignores
    /// it.
    pub fn with_offload(mut self, offload: Offload) -> Self {
        self.offload = Some(offload);
        self
    }

    /// The offload the checkpoint store does, if any.
    pub fn offload(&self) -> Option<&Offload> {
        self.offload.as_ref()
    }

    /// For tests: `fault` is called at named points of a call
    /// (`"save:before-commit"`, `"save:commit"`, `"save:after-commit"`,
    /// `"save:recheck"`, `"lease:after-commit"`, `"append:after-commit"`),
    /// where `true` fails the call as a lost connection would.
    #[doc(hidden)]
    pub fn with_fault(mut self, fault: impl Fn(&str) -> bool + Send + Sync + 'static) -> Self {
        self.fault = Some(Arc::new(fault));
        self
    }

    /// For tests: schema steps after the ones this build has, as a later
    /// build would add them.
    #[doc(hidden)]
    pub fn with_later_steps(mut self, steps: impl IntoIterator<Item = SchemaStep>) -> Self {
        self.later_steps = steps.into_iter().collect();
        self
    }
}

fn parse_config(url: &str) -> Result<Config, tokio_postgres::Error> {
    let mut config: Config = url.parse()?;
    if config.get_connect_timeout().is_none() {
        config.connect_timeout(CONNECT_TIMEOUT);
    }
    Ok(config)
}

/// Parses `url` (`postgres://user:pass@host:port/db`), applying a 5 s connect
/// timeout if the URL didn't set one — an unreachable server would otherwise
/// hang for the OS's TCP timeout.
pub fn config_from_url(url: &str) -> Result<Config, String> {
    parse_config(url).map_err(|e| e.to_string())
}

async fn open(url: &str) -> Result<Client, tokio_postgres::Error> {
    let (client, connection) = parse_config(url)?.connect(NoTls).await?;
    tokio::spawn(async move {
        // The task ends when the client is dropped or the connection breaks;
        // a broken connection surfaces on the client's next call.
        let _ = connection.await;
    });
    Ok(client)
}

/// Connects to `url` and spawns the connection task on the current runtime.
pub async fn connect(url: &str) -> Result<Client, String> {
    open(url).await.map_err(|e| e.to_string())
}

/// Whether `code` is a SQLSTATE a store repeats a call for
/// (`design/34-Storage-Hardening.md`, "Retries"): a connection exception
/// (class `08`), a server shutting down or starting (`57P01` to `57P03`), a
/// serialization failure (`40001`), a deadlock (`40P01`), or too many
/// connections (`53300`). Mirrors the .NET `PostgresFaults`.
pub fn is_transient_sqlstate(code: &str) -> bool {
    code.starts_with("08")
        || matches!(
            code,
            "57P01" | "57P02" | "57P03" | "40001" | "40P01" | "53300"
        )
}

impl From<tokio_postgres::Error> for Failure {
    fn from(err: tokio_postgres::Error) -> Self {
        let (transient, class) = match err.code() {
            Some(state) => (
                is_transient_sqlstate(state.code()),
                state.code().to_string(),
            ),
            None if err.is_closed() => (true, "closed".to_string()),
            None => {
                // A connection that broke under a call, or couldn't be made:
                // an I/O error somewhere beneath (a timeout is one).
                let mut source = std::error::Error::source(&err);
                let mut io = false;
                while let Some(cause) = source {
                    io |= cause.is::<std::io::Error>();
                    source = cause.source();
                }
                (io, if io { "io" } else { "client" }.to_string())
            }
        };
        // The error's own text names its kind only ("db error"); what went
        // wrong is beneath it. Neither holds the connection string.
        let mut message = err.to_string();
        let mut source = std::error::Error::source(&err);
        while let Some(cause) = source {
            message.push_str(&format!(": {cause}"));
            source = cause.source();
        }
        Failure {
            message,
            transient,
            never_sent: false,
            class,
        }
    }
}

/// The component of a database a store keeps, as `strata_schema_version`
/// names it.
#[derive(Debug, Clone, Copy)]
enum Component {
    Checkpoints,
    History,
}

impl Component {
    fn name(self) -> &'static str {
        match self {
            Component::Checkpoints => schema_versions::CHECKPOINTS,
            Component::History => schema_versions::HISTORY,
        }
    }

    /// The version its baseline brings a database to.
    fn baseline_version(self) -> i64 {
        match self {
            Component::Checkpoints => schema_versions::BASELINE_VERSION,
            Component::History => schema_versions::HISTORY_VERSION,
        }
    }
}

/// Brings `component`'s tables up to date under [`DDL_LOCK_KEY`], in one
/// transaction (`design/34-Storage-Hardening.md`, "Migrations"): reads its
/// version from `strata_schema_version` (no row: 0); refuses a database newer
/// than this build; at 0 runs the baseline, which creates the tables or adds
/// what an older layout lacks and is safe on any database; then runs each of
/// `steps` the database hasn't had, in order; and records the version it
/// reached. With `apply` off it changes nothing, and refuses a database that
/// is behind.
async fn migrate_component(
    client: &mut Client,
    component: Component,
    steps: &[SchemaStep],
    apply: bool,
) -> Result<(), Failure> {
    let tx = client.transaction().await?;
    match migrate_in(&tx, component, steps, apply).await {
        Ok(()) => Ok(tx.commit().await?),
        Err(failure) => {
            // Rolled back before the failure is the call's, not by a message
            // left for the connection to send: the transaction holds the DDL
            // lock every store opening this database waits for.
            let _ = tx.rollback().await;
            Err(failure)
        }
    }
}

/// [`migrate_component`] inside its transaction, which the caller commits.
async fn migrate_in(
    tx: &Transaction<'_>,
    component: Component,
    steps: &[SchemaStep],
    apply: bool,
) -> Result<(), Failure> {
    let known = schema_versions::known(component.baseline_version(), steps);
    let mut found = locked_version(tx, component.name(), known, apply).await?;
    if found == known {
        return Ok(());
    }
    if found == 0 {
        match component {
            Component::Checkpoints => prepare_checkpoint_log(tx).await?,
            Component::History => tx.batch_execute(POSTGRES_HISTORY_DDL).await?,
        }
        found = component.baseline_version();
    }
    run_steps(tx, component.name(), steps, found, known).await
}

/// Takes [`DDL_LOCK_KEY`] for the transaction and reads `component`'s version
/// from `strata_schema_version` (no row: 0). Refuses a database newer than
/// `known`, and, with `apply` off, one that is behind it.
pub(crate) async fn locked_version(
    tx: &Transaction<'_>,
    component: &str,
    known: i64,
    apply: bool,
) -> Result<i64, Failure> {
    tx.execute("SELECT pg_advisory_xact_lock($1)", &[&DDL_LOCK_KEY])
        .await?;
    tx.batch_execute(schema_versions::POSTGRES_SCHEMA_VERSION_DDL)
        .await?;
    let found: i64 = tx
        .query_opt(
            "SELECT version FROM strata_schema_version WHERE component = $1",
            &[&component],
        )
        .await?
        .map_or(0, |row| row.get(0));
    if found > known {
        return Err(Failure::fatal(schema_versions::newer(
            component, found, known,
        )));
    }
    if found < known && !apply {
        // Rolled back with the transaction: a store that doesn't migrate
        // changes nothing.
        return Err(Failure::fatal(schema_versions::behind(
            component, found, known,
        )));
    }
    Ok(found)
}

/// Runs each of `steps` a database at `found` (its baseline done) hasn't had,
/// in order, and records `known` as `component`'s version.
pub(crate) async fn run_steps(
    tx: &Transaction<'_>,
    component: &str,
    steps: &[SchemaStep],
    found: i64,
    known: i64,
) -> Result<(), Failure> {
    for step in schema_versions::pending(steps, found) {
        for statement in &step.statements {
            tx.batch_execute(statement).await?;
        }
    }
    tx.execute(
        "INSERT INTO strata_schema_version (component, version) VALUES ($1, $2)
         ON CONFLICT (component) DO UPDATE SET version = EXCLUDED.version",
        &[&component, &known],
    )
    .await?;
    Ok(())
}

/// Version 1 of the checkpoint component, inside [`migrate_component`]'s
/// transaction: creates the log and its head table, or brings an older
/// database up to date. It adds the `nested`, `decision`, `wait`, `wake_at`,
/// `graph_id` and `graph_version` columns, creates and backfills
/// `checkpoint_threads` when it doesn't exist yet
/// (`design/19-Async-Queryable-Store.md`, "Migration"), and adds the columns
/// it lacks and replaces its trigger when it predates waits
/// (`design/29-Event-Waits-And-Timers.md`) or graph identities
/// (`design/31-Graph-Identity.md`). Creating the trigger
/// locks `checkpoints` against inserts until the transaction commits, so no
/// save can land between the backfill and the trigger taking over.
async fn prepare_checkpoint_log(tx: &Transaction<'_>) -> Result<(), tokio_postgres::Error> {
    tx.batch_execute(POSTGRES_CHECKPOINT_LOG_DDL).await?;
    tx.batch_execute(POSTGRES_CHECKPOINT_LOG_MIGRATION).await?;
    let has_head: bool = tx
        .query_one("SELECT to_regclass('checkpoint_threads') IS NOT NULL", &[])
        .await?
        .get(0);
    if !has_head {
        tx.batch_execute(POSTGRES_CHECKPOINT_THREADS_DDL).await?;
        tx.batch_execute(POSTGRES_CHECKPOINT_THREADS_BACKFILL)
            .await?;
    } else {
        let current: bool = tx
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() \
                 AND table_name = 'checkpoint_threads' AND column_name = 'graph_version')",
                &[],
            )
            .await?
            .get(0);
        if !current {
            // A head table from before waits or identities: nothing in it
            // waits with a time or has an identity, so every row's NULL is
            // right.
            tx.batch_execute(POSTGRES_CHECKPOINT_THREADS_MIGRATION)
                .await?;
            tx.batch_execute(POSTGRES_CHECKPOINT_THREADS_DDL).await?;
        }
    }
    // Leases (design/32-Thread-Leases.md): nothing to migrate, a database
    // from before them has none.
    tx.batch_execute(POSTGRES_CHECKPOINT_LEASES_DDL).await
}

/// Where a store's connections come from: a pool it fills from its URL, or
/// the one client it was given.
enum Connections {
    Pool(Pool),
    Single(Mutex<Client>),
}

/// A connection for one attempt of a call: a pooled one, given back when it is
/// dropped, or the store's one client, held until then.
pub(crate) enum ClientGuard<'a> {
    Pooled(Box<Object>),
    Single(MutexGuard<'a, Client>),
}

impl Deref for ClientGuard<'_> {
    type Target = Client;

    fn deref(&self) -> &Client {
        match self {
            Self::Pooled(object) => object,
            Self::Single(client) => client,
        }
    }
}

impl DerefMut for ClientGuard<'_> {
    fn deref_mut(&mut self) -> &mut Client {
        match self {
            Self::Pooled(object) => object,
            Self::Single(client) => client,
        }
    }
}

impl ClientGuard<'_> {
    /// `sql` prepared on this connection: once per pooled connection, the
    /// round trip of preparing it every time saved. A store's one client
    /// prepares it each time, as before.
    pub(crate) async fn statement(&self, sql: &str) -> Result<Statement, tokio_postgres::Error> {
        match self {
            Self::Pooled(object) => object.prepare_cached(sql).await,
            Self::Single(client) => client.prepare(sql).await,
        }
    }
}

/// A pool's failure to give a connection, as a store's: the connection's own
/// error keeps its classification.
fn pool_failure(err: PoolError) -> Failure {
    match err {
        PoolError::Backend(err) => Failure::from(err),
        other => Failure::fatal(format!("the connection pool failed: {other}")),
    }
}

/// A store's connections and how they are kept: a pool that replaces a
/// connection that has dropped (a store built from a caller's client has that
/// client only), and the retry policy every call runs under
/// (`design/34-Storage-Hardening.md`, "Reconnecting";
/// `design/41-Checkpoint-Efficiency.md`, "The Rust Postgres pool").
pub(crate) struct Link {
    connections: Connections,
    pub(crate) retry: Retrier,
    fault: Option<FaultHook>,
    /// The checkpoint store's deltas and offload, if it does either.
    writer: Option<CheckpointWriter>,
    /// Where the checkpoint store's offloaded values go when not in this
    /// database.
    blobs: Option<Arc<dyn BlobStore>>,
}

impl Link {
    async fn connect(url: &str, options: &PostgresStoreOptions) -> Result<Self, Failure> {
        Self::connect_for(url, options, "migrate").await
    }

    /// [`Self::connect`], counting a retry of its first connection under
    /// `operation` (the memory store counts every call as `memory`).
    pub(crate) async fn connect_for(
        url: &str,
        options: &PostgresStoreOptions,
        operation: &'static str,
    ) -> Result<Self, Failure> {
        if options.pool_size == 0 {
            return Err(Failure::fatal(
                "a Postgres store's pool size must be at least 1",
            ));
        }
        let manager = Manager::from_config(
            parse_config(url)?,
            NoTls,
            ManagerConfig {
                // A connection whose socket has closed is dropped and replaced
                // when it is next taken.
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(manager)
            .max_size(options.pool_size)
            .build()
            .map_err(|err| Failure::fatal(format!("the connection pool can't be built: {err}")))?;
        let retry = Retrier::new(options.retry.clone(), "postgres");
        // One connection now, so a wrong URL or a server that is down is the
        // constructor's failure.
        retry
            .run(operation, || async {
                pool.get().await.map(drop).map_err(pool_failure)
            })
            .await?;
        Ok(Self {
            connections: Connections::Pool(pool),
            retry,
            fault: options.fault.clone(),
            writer: None,
            blobs: None,
        })
    }

    /// Over a pool the application keeps, which it may share with its other
    /// stores. Its size is the application's.
    pub(crate) fn pooled(pool: Pool, options: &PostgresStoreOptions) -> Self {
        Self {
            connections: Connections::Pool(pool),
            retry: Retrier::new(options.retry.clone(), "postgres"),
            fault: options.fault.clone(),
            writer: None,
            blobs: None,
        }
    }

    pub(crate) fn over(client: Client, options: &PostgresStoreOptions) -> Self {
        Self {
            connections: Connections::Single(Mutex::new(client)),
            retry: Retrier::new(options.retry.clone(), "postgres"),
            fault: options.fault.clone(),
            writer: None,
            blobs: None,
        }
    }

    /// `thread_id`'s row at `sequence` (the latest when `None`), decoded, its
    /// offloaded values read, with its sequence and how far it is from a whole
    /// row; one attempt.
    async fn row(
        &self,
        thread_id: &str,
        sequence: Option<i64>,
    ) -> Result<Option<(i64, Decoded, u32)>, Failure> {
        let client = self.client().await?;
        let rows = match sequence {
            Some(sequence) => {
                let statement = client.statement(&chain_sql("AND l.sequence = $2")).await?;
                client.query(&statement, &[&thread_id, &sequence]).await?
            }
            None => {
                let statement = client
                    .statement(&chain_sql("ORDER BY l.sequence DESC LIMIT 1"))
                    .await?;
                client.query(&statement, &[&thread_id]).await?
            }
        };
        let rows = rows
            .iter()
            .map(read_checkpoint)
            .collect::<Result<Vec<_>, CheckpointStoreError>>()?;
        // The blobs are read on a connection of their own: a store built from
        // one client has only that.
        drop(client);
        let Some((_, last)) = rows.last() else {
            return Ok(None);
        };
        let depth = depth_of(last);
        let from = rows.len() - 1;
        let decoded = self.resolve(thread_id, deltas::decode(rows, from)?).await?;
        Ok(decoded
            .into_iter()
            .next_back()
            .map(|row| (row.sequence, row, depth)))
    }

    /// Reads the offloaded values `decoded` refers to, from the blob store
    /// the options name or from this database's `checkpoint_blobs` in one
    /// query, and puts them in place.
    async fn resolve(
        &self,
        thread_id: &str,
        decoded: Vec<Decoded>,
    ) -> Result<Vec<Decoded>, Failure> {
        let wanted = deltas::wanted(&decoded);
        if wanted.is_empty() {
            return Ok(decoded);
        }
        let found = match &self.blobs {
            Some(blobs) => offload::read(&**blobs, thread_id, &wanted).await?,
            None => {
                let client = self.client().await?;
                client
                    .query(
                        "SELECT hash, content FROM checkpoint_blobs WHERE thread_id = $1 AND hash = ANY($2)",
                        &[&thread_id, &wanted],
                    )
                    .await?
                    .iter()
                    .map(|row| (row.get::<_, String>(0), row.get::<_, Vec<u8>>(1)))
                    .collect()
            }
        };
        Ok(deltas::resolve(decoded, &found)?)
    }

    /// The rows of `thread_id` from `from` to `to`, oldest first, as stored.
    async fn range(
        &self,
        thread_id: &str,
        from: i64,
        to: i64,
    ) -> Result<Vec<(i64, Checkpoint)>, Failure> {
        let client = self.client().await?;
        let statement = client
            .statement(&format!(
                "SELECT {CHECKPOINT_COLUMNS} FROM checkpoints c
                 WHERE c.thread_id = $1 AND c.sequence BETWEEN $2 AND $3 ORDER BY c.sequence"
            ))
            .await?;
        Ok(client
            .query(&statement, &[&thread_id, &from, &to])
            .await?
            .iter()
            .map(read_checkpoint)
            .collect::<Result<Vec<_>, CheckpointStoreError>>()?)
    }

    /// Forgets which row is `thread_id`'s latest, if the store remembers it.
    fn forget(&self, thread_id: &str) {
        if let Some(writer) = &self.writer {
            writer.forget(thread_id);
        }
    }

    /// A connection for one attempt of a call: from the pool, which replaces
    /// one that has dropped, or the store's one client.
    pub(crate) async fn client(&self) -> Result<ClientGuard<'_>, Failure> {
        match &self.connections {
            Connections::Pool(pool) => pool
                .get()
                .await
                .map(|object| ClientGuard::Pooled(Box::new(object)))
                .map_err(pool_failure),
            Connections::Single(client) => {
                let client = client.lock().await;
                if client.is_closed() {
                    return Err(Failure::fatal(
                        "the connection is closed, and a store built from a client can't reconnect",
                    ));
                }
                Ok(ClientGuard::Single(client))
            }
        }
    }

    fn fault(&self, point: &str) -> Result<(), Failure> {
        fault_at(&self.fault, point)
    }

    /// Group commit's flush (`design/41-Checkpoint-Efficiency.md`, "Group
    /// commit"): every row in one transaction, the threads' locks and the rows
    /// sent together, under the retry policy. An attempt whose commit may have
    /// happened is looked up row by row before the rest are tried again: a row
    /// appended at the sequence it was given was saved; a row whose place
    /// another checkpoint took lost its race if it was conditional, and is
    /// saved again if not.
    async fn flush(&self, rows: Arc<Vec<FlushRow>>) -> Result<Vec<Verdict>, Failure> {
        // A thread with two saves in one flush has both written whole: the row
        // before the second isn't known until the first has its verdict. Blobs
        // elsewhere are all written before the flush, so a row written whole in
        // it needs none that isn't there.
        let alone = deltas::alone(rows.iter().map(|row| row.checkpoint.thread_id.as_str()));
        let encodings: Vec<Encoding> = rows
            .iter()
            .map(|row| {
                self.writer.as_ref().map_or_else(Encoding::plain, |writer| {
                    writer.prepare(
                        &row.checkpoint,
                        alone.contains(row.checkpoint.thread_id.as_str()),
                        self.blobs.is_some(),
                    )
                })
            })
            .collect();
        if let Some(blobs) = &self.blobs {
            for (row, encoding) in rows.iter().zip(&encodings) {
                offload::put_all(&**blobs, &row.checkpoint.thread_id, &encoding.blobs).await?;
            }
        }
        let saves = rows
            .iter()
            .zip(&encodings)
            .map(|(row, encoding)| SaveRow::of(encoding.stored(&row.checkpoint)))
            .collect::<Result<Vec<_>, _>>()?;
        let mut verdicts: Vec<Option<Verdict>> = vec![None; rows.len()];
        // Where each row the flush answered itself was appended, and how.
        let mut written: Vec<Option<Attempted>> = vec![None; rows.len()];
        let mut pending: Vec<usize> = (0..rows.len()).collect();
        let mut in_doubt: Option<Vec<Attempted>> = None;
        let mut number = 1;
        loop {
            let attempt = async {
                if let Some(attempted) = &in_doubt {
                    // Forgotten only once the log has answered: a look that
                    // fails is made again before anything is written again.
                    self.fault("save:recheck")?;
                    pending = self
                        .settle(&rows, &pending, attempted, &mut verdicts)
                        .await?;
                    in_doubt = None;
                    if pending.is_empty() {
                        return Ok(());
                    }
                }
                let attempted = self
                    .flush_once(&rows, &encodings, &saves, &pending, &mut in_doubt)
                    .await?;
                for (&index, attempt) in pending.iter().zip(attempted) {
                    verdicts[index] = Some(attempt.verdict);
                    written[index] = Some(attempt);
                }
                Ok(())
            };
            match attempt.await {
                Ok(()) => break,
                Err(failure) => {
                    if !self.retry.again("save", number, &failure).await {
                        return Err(failure);
                    }
                }
            }
            number += 1;
        }
        Ok(verdicts
            .into_iter()
            .enumerate()
            .map(|(index, verdict)| {
                let verdict = verdict.unwrap_or(Verdict::Saved);
                let thread_id = &rows[index].checkpoint.thread_id;
                match (verdict, &written[index]) {
                    (Verdict::Saved, Some(attempt)) => {
                        let whole;
                        let checkpoint = &rows[index].checkpoint;
                        let encoding = if attempt.whole.is_some() {
                            whole = encodings[index].whole(checkpoint);
                            &whole
                        } else {
                            &encodings[index]
                        };
                        record_saved(
                            "postgres",
                            encoding,
                            attempt.whole.unwrap_or(saves[index].bytes),
                        );
                        match (&self.writer, attempt.sequence) {
                            (Some(writer), Some(sequence)) => {
                                writer.saved(checkpoint, encoding, sequence, &checkpoint.created_at)
                            }
                            _ => self.forget(thread_id),
                        }
                    }
                    // Looked up after a flush in doubt: where, and how, isn't
                    // known.
                    (Verdict::Saved, None) => {
                        record_saved("postgres", &encodings[index], saves[index].bytes);
                        self.forget(thread_id);
                    }
                    (Verdict::Lost, _) => self.forget(thread_id),
                    (Verdict::LeaseLost, _) => {}
                }
                verdict
            })
            .collect())
    }

    /// One attempt of [`Self::flush`] for the rows at `pending`: the
    /// transaction, its locks and the rows, then the commit. The rows go in
    /// waves of distinct threads (the first save of each thread, then the
    /// second, and so on), so one thread's saves keep their order. Returns
    /// each row's verdict and the sequence it got; `in_doubt` is given them
    /// once the commit is sent, when a failure no longer says whether they
    /// were written.
    async fn flush_once(
        &self,
        rows: &[FlushRow],
        encodings: &[Encoding],
        saves: &[SaveRow],
        pending: &[usize],
        in_doubt: &mut Option<Vec<Attempted>>,
    ) -> Result<Vec<Attempted>, Failure> {
        let mut client = self.client().await?;
        let lock = client.statement(FLUSH_LOCK_SQL).await?;
        let wave_statement = client.statement(FLUSH_WAVE_SQL).await?;
        let tx = client.transaction().await?;

        let mut threads: Vec<&str> = pending
            .iter()
            .map(|&index| rows[index].checkpoint.thread_id.as_str())
            .collect();
        threads.sort_unstable();
        threads.dedup();
        let mut waves: Vec<Vec<usize>> = Vec::new();
        let mut seen: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
        for (position, &index) in pending.iter().enumerate() {
            let wave = seen
                .entry(rows[index].checkpoint.thread_id.as_str())
                .or_default();
            if *wave == waves.len() {
                waves.push(Vec::new());
            }
            waves[*wave].push(position);
            *wave += 1;
        }

        let own = self.blobs.is_none();
        let columns: Vec<WaveColumns> = waves
            .iter()
            .map(|wave| {
                WaveColumns::of(
                    wave.iter().map(|&position| {
                        let index = pending[position];
                        (
                            position as i32,
                            &rows[index],
                            encodings[index].stored(&rows[index].checkpoint),
                            &saves[index],
                            &encodings[index],
                        )
                    }),
                    own,
                )
            })
            .collect();
        let lock_params: [&(dyn ToSql + Sync); 1] = [&threads];
        let wave_params: Vec<[&(dyn ToSql + Sync); 23]> =
            columns.iter().map(WaveColumns::params).collect();
        let mut calls: Vec<Query<'_>> = vec![Box::pin(tx.query(&lock, &lock_params))];
        for params in &wave_params {
            calls.push(Box::pin(tx.query(&wave_statement, params)));
        }
        // Sent together: the locks and every wave in one round trip.
        let mut results = JoinAll::new(calls).await.into_iter();
        results.next().transpose()?;

        let mut attempted: Vec<Attempted> = vec![Attempted::default(); pending.len()];
        let mut rebased = Vec::new();
        for result in results {
            for row in result? {
                let position: i32 = row.get(0);
                if row.get::<_, &str>(1) == "rebase" {
                    rebased.push(position as usize);
                } else {
                    attempted[position as usize] = Attempted::read(&row);
                }
            }
        }
        if !rebased.is_empty() {
            // Rows whose base another writer moved past: written whole, in the
            // same transaction and under the same locks. Each is its thread's
            // only row in the flush, so one wave holds them all.
            let encoded: Vec<Encoding> = rebased
                .iter()
                .map(|&position| {
                    let index = pending[position];
                    encodings[index].whole(&rows[index].checkpoint)
                })
                .collect();
            let whole: Vec<SaveRow> = rebased
                .iter()
                .zip(&encoded)
                .map(|(&position, encoding)| {
                    SaveRow::of(encoding.stored(&rows[pending[position]].checkpoint))
                })
                .collect::<Result<_, _>>()?;
            let columns =
                WaveColumns::of(
                    rebased.iter().zip(&whole).zip(&encoded).map(
                        |((&position, save), encoding)| {
                            let row = &rows[pending[position]];
                            (
                                position as i32,
                                row,
                                encoding.stored(&row.checkpoint),
                                save,
                                encoding,
                            )
                        },
                    ),
                    own,
                );
            for row in tx.query(&wave_statement, &columns.params()).await? {
                let position = row.get::<_, i32>(0) as usize;
                let mut attempt = Attempted::read(&row);
                let index = rebased.iter().position(|&p| p == position).unwrap_or(0);
                attempt.whole = Some(whole[index].bytes);
                attempted[position] = attempt;
            }
        }

        self.fault("save:before-commit")?;
        if attempted
            .iter()
            .any(|attempt| attempt.verdict == Verdict::Saved)
        {
            // From here on a failure doesn't say whether the rows are there.
            *in_doubt = Some(attempted.clone());
        }
        self.fault("save:commit")?;
        tx.commit().await?;
        self.fault("save:after-commit")?;
        *in_doubt = None;
        Ok(attempted)
    }

    /// After an attempt whose commit may have happened: answers every pending
    /// row whose fate the log tells, and returns the rows to flush again. A
    /// refused row's verdict stands (a log and a lease token only move
    /// forward); a row that was given a sequence is looked up there, all in one
    /// query.
    async fn settle(
        &self,
        rows: &[FlushRow],
        pending: &[usize],
        attempted: &[Attempted],
        verdicts: &mut [Option<Verdict>],
    ) -> Result<Vec<usize>, Failure> {
        let placed: Vec<(usize, i64)> = pending
            .iter()
            .zip(attempted)
            .filter_map(|(&index, attempt)| attempt.sequence.map(|sequence| (index, sequence)))
            .collect();
        let mut stored = std::collections::HashMap::new();
        if !placed.is_empty() {
            let threads: Vec<&str> = placed
                .iter()
                .map(|&(index, _)| rows[index].checkpoint.thread_id.as_str())
                .collect();
            let sequences: Vec<i64> = placed.iter().map(|&(_, sequence)| sequence).collect();
            let client = self.client().await?;
            for row in client
                .query(
                    &format!(
                        "SELECT {CHECKPOINT_COLUMNS} FROM checkpoints c
                         JOIN unnest($1::text[], $2::bigint[]) AS p(thread_id, sequence)
                           ON c.thread_id = p.thread_id AND c.sequence = p.sequence"
                    ),
                    &[&threads, &sequences],
                )
                .await?
            {
                let (sequence, checkpoint) = read_checkpoint(&row)?;
                stored.insert((checkpoint.thread_id.clone(), sequence), checkpoint);
            }
            // A delta is compared as what it decodes to, read again with the
            // rows it needs, on a connection of its own: a store built from one
            // client has only that.
            drop(client);
            for ((thread_id, sequence), checkpoint) in stored.iter_mut() {
                if deltas::is_encoded(&checkpoint.schema_version)
                    && let Some((_, decoded, _)) = self.row(thread_id, Some(*sequence)).await?
                {
                    *checkpoint = decoded.checkpoint;
                }
            }
        }

        let mut again = Vec::new();
        for (&index, attempt) in pending.iter().zip(attempted) {
            let (verdict, sequence) = (&attempt.verdict, &attempt.sequence);
            let Some(sequence) = sequence else {
                verdicts[index] = Some(*verdict);
                continue;
            };
            let row = &rows[index];
            let next = stored.get(&(row.checkpoint.thread_id.clone(), *sequence));
            match fate_of(&row.checkpoint, next, row.expected.is_some()) {
                SaveFate::Saved => verdicts[index] = Some(Verdict::Saved),
                SaveFate::Lost => verdicts[index] = Some(Verdict::Lost),
                SaveFate::NotWritten => again.push(index),
            }
        }
        Ok(again)
    }

    async fn migrate(
        &self,
        component: Component,
        later: &[SchemaStep],
        apply: bool,
    ) -> Result<(), Failure> {
        let steps = match component {
            Component::Checkpoints => schema_versions::postgres_checkpoint_steps(later),
            Component::History => later.to_vec(),
        };
        let mut client = self.client().await?;
        migrate_component(&mut client, component, &steps, apply).await
    }
}

/// Postgres-backed [`CheckpointStore`]: the same append-only checkpoint log the
/// SQLite stores keep (one row per `save` at the next `sequence` for its
/// `thread_id`; `load` returns the highest), but safe for concurrent writers.
///
/// Every call runs under the store's [`StoreRetry`]
/// (`design/34-Storage-Hardening.md`): a transient failure is repeated, a save
/// that may have been written is looked up first, and a store built with
/// [`connect`](Self::connect) connects again when its connection has dropped.
pub struct PostgresCheckpointStore {
    link: Arc<Link>,
    steps: Vec<SchemaStep>,
    group: Option<Queue>,
}

impl PostgresCheckpointStore {
    pub async fn connect(url: &str) -> Result<Self, CheckpointStoreError> {
        Self::connect_with(url, PostgresStoreOptions::new()).await
    }

    /// [`connect`](Self::connect) with `options`: the retry policy, and
    /// whether the store migrates the database or only checks it.
    pub async fn connect_with(
        url: &str,
        options: PostgresStoreOptions,
    ) -> Result<Self, CheckpointStoreError> {
        let link = Link::connect(url, &options).await?;
        Self::prepared(link, options).await
    }

    /// Wraps an already-connected client — the way to use TLS, a custom
    /// `search_path`, or a connection your application already manages. The
    /// store can't replace a client it didn't make: once its connection
    /// drops, every later call fails.
    pub async fn from_client(client: Client) -> Result<Self, CheckpointStoreError> {
        Self::from_client_with(client, PostgresStoreOptions::new()).await
    }

    /// [`from_client`](Self::from_client) with `options`.
    pub async fn from_client_with(
        client: Client,
        options: PostgresStoreOptions,
    ) -> Result<Self, CheckpointStoreError> {
        Self::prepared(Link::over(client, &options), options).await
    }

    async fn prepared(
        mut link: Link,
        options: PostgresStoreOptions,
    ) -> Result<Self, CheckpointStoreError> {
        link.writer = CheckpointWriter::new(options.deltas.as_ref(), options.offload.as_ref())?;
        link.blobs = options
            .offload
            .as_ref()
            .and_then(|offload| offload.blobs().cloned());
        let link = Arc::new(link);
        let group = match &options.group_commit {
            Some(group_commit) => {
                group_commit.check()?;
                let flusher = link.clone();
                let flush: Flush = Arc::new(move |rows| {
                    let link = flusher.clone();
                    Box::pin(async move { link.flush(rows).await })
                });
                Some(Queue::new(group_commit, "postgres", flush, None))
            }
            None => None,
        };
        let store = Self {
            link,
            steps: options.later_steps,
            group,
        };
        store.prepare(options.auto_migrate).await?;
        Ok(store)
    }

    /// Whether saves go through group commit.
    #[doc(hidden)]
    pub fn group_commits(&self) -> bool {
        self.group.is_some()
    }

    /// Brings the checkpoint tables up to date, whatever the store's options
    /// say (`design/34-Storage-Hardening.md`, "Migrations"): what a deploy
    /// step runs when the application's stores don't migrate.
    pub async fn migrate(&self) -> Result<(), CheckpointStoreError> {
        self.prepare(true).await
    }

    async fn prepare(&self, apply: bool) -> Result<(), CheckpointStoreError> {
        self.link
            .retry
            .run("migrate", || {
                self.link
                    .migrate(Component::Checkpoints, &self.steps, apply)
            })
            .await?;
        Ok(())
    }
}

type Param = Box<dyn ToSql + Sync + Send>;

/// Adds `value` to `params` and returns its placeholder (`$n`).
fn bind(params: &mut Vec<Param>, value: Param) -> String {
    params.push(value);
    format!("${}", params.len())
}

/// The columns every checkpoint read selects, in this order.
const CHECKPOINT_COLUMNS: &str = "c.thread_id, c.sequence, c.schema_version, c.status, \
     c.suspended_node, c.next_node, c.frozen_state, c.metadata, c.created_at, c.resumed_at, c.nested, \
     c.decision, c.wait, c.graph_id, c.graph_version";

/// A row of [`CHECKPOINT_COLUMNS`] as `(sequence, checkpoint)`.
fn read_checkpoint(row: &Row) -> Result<(i64, Checkpoint), CheckpointStoreError> {
    let status: String = row.get("status");
    let metadata: Option<serde_json::Value> = row.get("metadata");
    let nested: Option<serde_json::Value> = row.get("nested");
    let decision: Option<serde_json::Value> = row.get("decision");
    let wait: Option<serde_json::Value> = row.get("wait");
    let graph_id: Option<String> = row.get("graph_id");
    Ok((
        row.get("sequence"),
        Checkpoint {
            schema_version: row.get("schema_version"),
            thread_id: row.get("thread_id"),
            status: parse_status(&status)?,
            suspended_node: row.get("suspended_node"),
            next_node: row.get("next_node"),
            frozen_state: row.get("frozen_state"),
            metadata: metadata
                .map(serde_json::from_value)
                .transpose()
                .map_err(json_err)?,
            created_at: row.get("created_at"),
            resumed_at: row.get("resumed_at"),
            nested: nested
                .map(serde_json::from_value)
                .transpose()
                .map_err(json_err)?,
            decision: decision
                .map(serde_json::from_value)
                .transpose()
                .map_err(json_err)?,
            wait: wait
                .map(serde_json::from_value)
                .transpose()
                .map_err(json_err)?,
            graph: graph_id.map(|id| GraphIdentity::new(id, row.get("graph_version"))),
        },
    ))
}

/// A row of [`LEASE_COLUMNS`] as `thread_id`'s lease.
fn read_lease(thread_id: &str, row: &Row) -> Result<ThreadLease, CheckpointStoreError> {
    Ok(ThreadLease {
        thread_id: thread_id.to_string(),
        owner: row.get("owner"),
        token: row.get("token"),
        expires_at: from_millis(row.get("expires_at"))?,
    })
}

/// A checkpoint's columns that aren't its own fields as they are: the JSON
/// ones, and the ones derived for the head table. Made once per save, however
/// many attempts it takes.
struct SaveRow {
    metadata: Option<serde_json::Value>,
    nested: Option<serde_json::Value>,
    decision: Option<serde_json::Value>,
    wait: Option<serde_json::Value>,
    wake_at: Option<String>,
    graph_id: Option<String>,
    graph_version: Option<String>,
    /// The bytes of the JSON the row holds: what
    /// `strata.store.checkpoint.size` records.
    bytes: usize,
}

/// The bytes of `value` as JSON text.
fn json_len(value: &serde_json::Value) -> usize {
    serde_json::to_string(value).map_or(0, |text| text.len())
}

impl SaveRow {
    fn of(checkpoint: &Checkpoint) -> Result<Self, CheckpointStoreError> {
        Ok(Self {
            metadata: checkpoint
                .metadata
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .map_err(json_err)?,
            nested: checkpoint
                .nested
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .map_err(json_err)?,
            decision: checkpoint
                .decision
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .map_err(json_err)?,
            wait: checkpoint
                .wait
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .map_err(json_err)?,
            wake_at: checkpoint
                .innermost_wait()
                .and_then(|wait| wait.wake_at.clone()),
            graph_id: checkpoint.graph.as_ref().map(|graph| graph.id.clone()),
            graph_version: checkpoint
                .graph
                .as_ref()
                .and_then(|graph| graph.version.clone()),
            bytes: 0,
        })
        .map(|mut row: Self| {
            row.bytes = json_len(&checkpoint.frozen_state)
                + [&row.metadata, &row.nested, &row.decision, &row.wait]
                    .into_iter()
                    .flatten()
                    .map(json_len)
                    .sum::<usize>();
            row
        })
    }
}

/// [`INSERT_SQL`] for `stored`, whose other columns are `row`, under the
/// thread's lock in `tx`, relying on `encoding`'s base, with `blobs`: the
/// sequence it was appended at, if it was.
async fn insert_statement(
    tx: &Transaction<'_>,
    insert: &Statement,
    stored: &Checkpoint,
    row: &SaveRow,
    expected: Option<i64>,
    encoding: &Encoding,
    blobs: &[Blob],
) -> Result<Option<i64>, tokio_postgres::Error> {
    let hashes: Vec<&str> = blobs.iter().map(|blob| blob.hash.as_str()).collect();
    let contents: Vec<&[u8]> = blobs.iter().map(|blob| &blob.content[..]).collect();
    Ok(tx
        .query_opt(
            insert,
            &[
                &stored.thread_id,
                &stored.schema_version,
                &status_str(stored.status),
                &stored.suspended_node,
                &stored.next_node,
                &stored.frozen_state,
                &row.metadata,
                &stored.created_at,
                &stored.resumed_at,
                &row.nested,
                &row.decision,
                &expected,
                &row.wait,
                &row.wake_at,
                &row.graph_id,
                &row.graph_version,
                &encoding.base_sequence(),
                &encoding.base_token(),
                &hashes,
                &contents,
            ],
        )
        .await?
        .map(|inserted| inserted.get(0)))
}

/// A statement of a flush, sent with the others.
type Query<'a> = Pin<Box<dyn Future<Output = Result<Vec<Row>, tokio_postgres::Error>> + Send + 'a>>;

/// A row's verdict in a flush attempt, the sequence it was given when it was
/// appended, and, for a delta written whole because its base had moved, the
/// bytes of the whole row.
#[derive(Debug, Clone, Copy)]
struct Attempted {
    verdict: Verdict,
    sequence: Option<i64>,
    whole: Option<usize>,
}

impl Default for Attempted {
    fn default() -> Self {
        Self {
            verdict: Verdict::Saved,
            sequence: None,
            whole: None,
        }
    }
}

impl Attempted {
    /// A row of [`FLUSH_WAVE_SQL`]'s result that isn't `rebase`.
    fn read(row: &Row) -> Self {
        match row.get::<_, &str>(1) {
            "saved" => Self {
                sequence: Some(row.get::<_, i64>(2)),
                ..Self::default()
            },
            "lost" => Self {
                verdict: Verdict::Lost,
                ..Self::default()
            },
            _ => Self {
                verdict: Verdict::LeaseLost,
                ..Self::default()
            },
        }
    }
}

/// One wave of a flush as [`FLUSH_WAVE_SQL`] binds it: an array per column.
struct WaveColumns {
    ord: Vec<i32>,
    thread_id: Vec<String>,
    schema_version: Vec<String>,
    status: Vec<String>,
    suspended_node: Vec<Option<String>>,
    next_node: Vec<Option<String>>,
    frozen_state: Vec<serde_json::Value>,
    metadata: Vec<Option<serde_json::Value>>,
    created_at: Vec<String>,
    resumed_at: Vec<Option<String>>,
    nested: Vec<Option<serde_json::Value>>,
    decision: Vec<Option<serde_json::Value>>,
    wait: Vec<Option<serde_json::Value>>,
    wake_at: Vec<Option<String>>,
    graph_id: Vec<Option<String>>,
    graph_version: Vec<Option<String>>,
    expected: Vec<Option<i64>>,
    token: Vec<Option<i64>>,
    base: Vec<Option<i64>>,
    base_created: Vec<Option<String>>,
    blob_ord: Vec<i32>,
    blob_hash: Vec<String>,
    blob_content: Vec<Vec<u8>>,
}

impl WaveColumns {
    /// The rows of a wave, each its position, its save, the checkpoint as it
    /// is stored, that checkpoint's columns and how it is encoded (the row it
    /// relies on, the blobs it writes when `own_blobs`).
    fn of<'a>(
        wave: impl Iterator<Item = (i32, &'a FlushRow, &'a Checkpoint, &'a SaveRow, &'a Encoding)>,
        own_blobs: bool,
    ) -> Self {
        let mut columns = Self {
            ord: Vec::new(),
            thread_id: Vec::new(),
            schema_version: Vec::new(),
            status: Vec::new(),
            suspended_node: Vec::new(),
            next_node: Vec::new(),
            frozen_state: Vec::new(),
            metadata: Vec::new(),
            created_at: Vec::new(),
            resumed_at: Vec::new(),
            nested: Vec::new(),
            decision: Vec::new(),
            wait: Vec::new(),
            wake_at: Vec::new(),
            graph_id: Vec::new(),
            graph_version: Vec::new(),
            expected: Vec::new(),
            token: Vec::new(),
            base: Vec::new(),
            base_created: Vec::new(),
            blob_ord: Vec::new(),
            blob_hash: Vec::new(),
            blob_content: Vec::new(),
        };
        for (position, row, checkpoint, save, encoding) in wave {
            columns.ord.push(position);
            columns.thread_id.push(checkpoint.thread_id.clone());
            columns
                .schema_version
                .push(checkpoint.schema_version.clone());
            columns
                .status
                .push(status_str(checkpoint.status).to_string());
            columns
                .suspended_node
                .push(checkpoint.suspended_node.clone());
            columns.next_node.push(checkpoint.next_node.clone());
            columns.frozen_state.push(checkpoint.frozen_state.clone());
            columns.metadata.push(save.metadata.clone());
            columns.created_at.push(checkpoint.created_at.clone());
            columns.resumed_at.push(checkpoint.resumed_at.clone());
            columns.nested.push(save.nested.clone());
            columns.decision.push(save.decision.clone());
            columns.wait.push(save.wait.clone());
            columns.wake_at.push(save.wake_at.clone());
            columns.graph_id.push(save.graph_id.clone());
            columns.graph_version.push(save.graph_version.clone());
            columns.expected.push(row.expected);
            columns.token.push(row.token);
            columns.base.push(encoding.base_sequence());
            columns
                .base_created
                .push(encoding.base_token().map(str::to_string));
            if own_blobs {
                for blob in &encoding.blobs {
                    columns.blob_ord.push(position);
                    columns.blob_hash.push(blob.hash.clone());
                    columns.blob_content.push(blob.content.to_vec());
                }
            }
        }
        columns
    }

    fn params(&self) -> [&(dyn ToSql + Sync); 23] {
        [
            &self.ord,
            &self.thread_id,
            &self.schema_version,
            &self.status,
            &self.suspended_node,
            &self.next_node,
            &self.frozen_state,
            &self.metadata,
            &self.created_at,
            &self.resumed_at,
            &self.nested,
            &self.decision,
            &self.wait,
            &self.wake_at,
            &self.graph_id,
            &self.graph_version,
            &self.expected,
            &self.token,
            &self.base,
            &self.base_created,
            &self.blob_ord,
            &self.blob_hash,
            &self.blob_content,
        ]
    }
}

/// Polls every future at once and gives their outputs in order: how a flush
/// sends its statements together, which `tokio-postgres` pipelines on one
/// connection.
struct JoinAll<'a, T> {
    calls: Vec<Option<Pin<Box<dyn Future<Output = T> + Send + 'a>>>>,
    outputs: Vec<Option<T>>,
}

impl<'a, T> JoinAll<'a, T> {
    fn new(calls: Vec<Pin<Box<dyn Future<Output = T> + Send + 'a>>>) -> Self {
        let outputs = calls.iter().map(|_| None).collect();
        Self {
            calls: calls.into_iter().map(Some).collect(),
            outputs,
        }
    }
}

// The outputs are only moved out, never pinned, and the calls are boxed.
impl<T> Unpin for JoinAll<'_, T> {}

impl<T> Future for JoinAll<'_, T> {
    type Output = Vec<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Vec<T>> {
        let this = self.get_mut();
        let mut done = true;
        for (call, output) in this.calls.iter_mut().zip(this.outputs.iter_mut()) {
            if let Some(future) = call {
                match future.as_mut().poll(cx) {
                    Poll::Ready(value) => {
                        *output = Some(value);
                        *call = None;
                    }
                    Poll::Pending => done = false,
                }
            }
        }
        if done {
            Poll::Ready(this.outputs.iter_mut().filter_map(Option::take).collect())
        } else {
            Poll::Pending
        }
    }
}

impl PostgresCheckpointStore {
    async fn save_row(&self, checkpoint: Checkpoint) -> Result<(), CheckpointStoreError> {
        self.insert_row(checkpoint, None, None).await.map(|_| ())
    }

    /// Acquires under the retry policy. An acquisition whose commit failed
    /// may have taken the lease, and the store can't tell its own from
    /// another process's with the same owner name: that failure is the
    /// call's, and the lease, if it was taken, runs out by itself. A failure
    /// before the commit is repeated (`design/34-Storage-Hardening.md`).
    async fn acquire_lease_row(
        &self,
        thread_id: &str,
        owner: &str,
        ttl: Duration,
    ) -> Result<LeaseAttempt, CheckpointStoreError> {
        check_lease(owner, ttl)?;
        let ttl = lease_millis(ttl);
        let mut number = 1;
        loop {
            let mut committing = false;
            match self
                .acquire_once(thread_id, owner, ttl, &mut committing)
                .await
            {
                Ok(attempt) => return Ok(attempt),
                Err(failure) => {
                    if committing || !self.link.retry.again("lease", number, &failure).await {
                        return Err(failure.into());
                    }
                }
            }
            number += 1;
        }
    }

    /// Under the thread's advisory lock, the one a save takes: the thread's
    /// first lease at token 1, or its next token when the lease it has isn't
    /// held; no row back means it is held, and a second statement reads by
    /// whom (`design/32-Thread-Leases.md`). `committing` is set once the
    /// commit is sent.
    async fn acquire_once(
        &self,
        thread_id: &str,
        owner: &str,
        ttl: i64,
        committing: &mut bool,
    ) -> Result<LeaseAttempt, Failure> {
        let mut client = self.link.client().await?;
        let tx = client.transaction().await?;
        tx.execute(THREAD_LOCK, &[&thread_id]).await?;
        for _ in 0..ACQUIRE_ATTEMPTS {
            let acquired = tx
                .query_opt(
                    &format!(
                        "INSERT INTO checkpoint_leases (thread_id, owner, token, expires_at)
                         VALUES ($1, $2, 1, clock_timestamp() + $3::bigint * interval '1 millisecond')
                         ON CONFLICT (thread_id) DO UPDATE
                             SET owner = EXCLUDED.owner, token = checkpoint_leases.token + 1, expires_at = EXCLUDED.expires_at
                             WHERE checkpoint_leases.expires_at IS NULL OR checkpoint_leases.expires_at <= clock_timestamp()
                         RETURNING {LEASE_COLUMNS}"
                    ),
                    &[&thread_id, &owner, &ttl],
                )
                .await?;
            if let Some(row) = acquired {
                let lease = read_lease(thread_id, &row)?;
                *committing = true;
                tx.commit().await?;
                self.link.fault("lease:after-commit")?;
                return Ok(LeaseAttempt::Acquired(lease));
            }
            let holder = tx
                .query_opt(
                    &format!(
                        "SELECT {LEASE_COLUMNS} FROM checkpoint_leases
                         WHERE thread_id = $1 AND expires_at > clock_timestamp()"
                    ),
                    &[&thread_id],
                )
                .await?;
            if let Some(row) = holder {
                let held = read_lease(thread_id, &row)?;
                tx.commit().await?;
                return Ok(LeaseAttempt::Held(held));
            }
            // Released or expired between the two statements (a release takes
            // no lock): look again.
        }
        Err(lease_kept_changing(thread_id).into())
    }

    /// Renewing twice is renewing once, so a transient failure is repeated.
    async fn renew_lease_row(
        &self,
        lease: &ThreadLease,
        ttl: Duration,
    ) -> Result<Option<ThreadLease>, CheckpointStoreError> {
        check_lease_ttl(ttl)?;
        let ttl = lease_millis(ttl);
        Ok(self
            .link
            .retry
            .run("lease", || self.renew_once(lease, ttl))
            .await?)
    }

    async fn renew_once(
        &self,
        lease: &ThreadLease,
        ttl: i64,
    ) -> Result<Option<ThreadLease>, Failure> {
        let client = self.link.client().await?;
        let row = client
            .query_opt(
                &format!(
                    "UPDATE checkpoint_leases
                     SET owner = $1, expires_at = clock_timestamp() + $2::bigint * interval '1 millisecond'
                     WHERE thread_id = $3 AND token = $4
                     RETURNING {LEASE_COLUMNS}"
                ),
                &[&lease.owner, &ttl, &lease.thread_id, &lease.token],
            )
            .await?;
        Ok(row
            .map(|row| read_lease(&lease.thread_id, &row))
            .transpose()?)
    }

    /// Releasing twice is releasing once: the second finds the lease already
    /// let go, by the same token.
    async fn release_lease_row(&self, lease: &ThreadLease) -> Result<bool, CheckpointStoreError> {
        Ok(self
            .link
            .retry
            .run("lease", || self.release_once(lease))
            .await?)
    }

    async fn release_once(&self, lease: &ThreadLease) -> Result<bool, Failure> {
        let client = self.link.client().await?;
        // A release repeated after one that went through finds the row
        // already cleared and still its own.
        let released = client
            .execute(
                "UPDATE checkpoint_leases SET owner = NULL, expires_at = NULL WHERE thread_id = $1 AND token = $2",
                &[&lease.thread_id, &lease.token],
            )
            .await?;
        Ok(released == 1)
    }

    async fn held_lease_row(
        &self,
        thread_id: &str,
    ) -> Result<Option<ThreadLease>, CheckpointStoreError> {
        Ok(self
            .link
            .retry
            .run("lease", || self.held_once(thread_id))
            .await?)
    }

    async fn held_once(&self, thread_id: &str) -> Result<Option<ThreadLease>, Failure> {
        let client = self.link.client().await?;
        let row = client
            .query_opt(
                &format!(
                    "SELECT {LEASE_COLUMNS} FROM checkpoint_leases
                     WHERE thread_id = $1 AND expires_at > clock_timestamp()"
                ),
                &[&thread_id],
            )
            .await?;
        Ok(row.map(|row| read_lease(thread_id, &row)).transpose()?)
    }

    /// [`Self::insert_row`] under `lease`.
    async fn save_under_lease_row(
        &self,
        checkpoint: Checkpoint,
        lease: &ThreadLease,
        expected: Option<i64>,
    ) -> Result<LeasedSave, CheckpointStoreError> {
        check_lease_thread(&checkpoint, lease)?;
        self.insert_row(checkpoint, expected, Some(lease.token))
            .await
    }

    /// Appends `checkpoint` at the next sequence for its thread, or, with an
    /// `expected` sequence, only if that is still the thread's latest, decided
    /// under the save's per-thread advisory lock
    /// (`design/28-Ending-A-Thread.md`, "Races"). With a lease `token`, only
    /// if that is still the thread's latest token, read under the same lock,
    /// which an acquisition takes too (`design/32-Thread-Leases.md`,
    /// "Fencing").
    ///
    /// An attempt that failed while it committed may have been written. The
    /// next attempt looks before it writes again, so a checkpoint is never
    /// appended twice and a conditional save keeps its answer
    /// (`design/34-Storage-Hardening.md`, "A save that may have happened").
    async fn insert_row(
        &self,
        checkpoint: Checkpoint,
        expected: Option<i64>,
        token: Option<i64>,
    ) -> Result<LeasedSave, CheckpointStoreError> {
        if let Some(group) = &self.group {
            let verdict = group
                .save(FlushRow {
                    checkpoint,
                    expected,
                    token,
                })
                .await?;
            return Ok(match verdict {
                Verdict::Saved => LeasedSave::Saved,
                Verdict::Lost => LeasedSave::NotLatest,
                Verdict::LeaseLost => LeasedSave::LeaseLost,
            });
        }
        let encoding = self
            .link
            .writer
            .as_ref()
            .map_or_else(Encoding::plain, |writer| {
                writer.prepare(&checkpoint, true, false)
            });
        if let Some(blobs) = &self.link.blobs {
            // Blobs in another store go before the row that refers to them.
            offload::put_all(&**blobs, &checkpoint.thread_id, &encoding.blobs).await?;
        }
        let row = SaveRow::of(encoding.stored(&checkpoint))?;
        let mut may_be_written = false;
        let mut number = 1;
        loop {
            match self
                .insert_attempt(
                    &checkpoint,
                    &encoding,
                    &row,
                    expected,
                    token,
                    &mut may_be_written,
                )
                .await
            {
                Ok(saved) => return Ok(saved),
                Err(failure) => {
                    if !self.link.retry.again("save", number, &failure).await {
                        return Err(failure.into());
                    }
                }
            }
            number += 1;
        }
    }

    async fn insert_attempt(
        &self,
        checkpoint: &Checkpoint,
        encoding: &Encoding,
        row: &SaveRow,
        expected: Option<i64>,
        token: Option<i64>,
        may_be_written: &mut bool,
    ) -> Result<LeasedSave, Failure> {
        if *may_be_written {
            self.link.fault("save:recheck")?;
            let fate = self.fate(checkpoint, expected).await?;
            // Whatever the look finds, the store no longer knows which row is
            // the thread's latest.
            self.link.forget(&checkpoint.thread_id);
            match fate {
                SaveFate::Saved => return Ok(LeasedSave::Saved),
                SaveFate::Lost => return Ok(LeasedSave::NotLatest),
                SaveFate::NotWritten => *may_be_written = false,
            }
        }
        self.insert_once(checkpoint, encoding, row, expected, token, may_be_written)
            .await
    }

    /// What a look at the log says about a save that may have happened: for a
    /// conditional one, the entry right after its expected sequence; for any
    /// other, the thread's latest.
    async fn fate(
        &self,
        checkpoint: &Checkpoint,
        expected: Option<i64>,
    ) -> Result<SaveFate, Failure> {
        let next = self
            .link
            .row(&checkpoint.thread_id, expected.map(|expected| expected + 1))
            .await?
            .map(|(_, stored, _)| stored.checkpoint);
        Ok(fate_of(checkpoint, next.as_ref(), expected.is_some()))
    }

    /// One attempt of [`Self::insert_row`]. `may_be_written` is set once the
    /// commit of an inserted row is sent: from there on a failure doesn't say
    /// whether the row is there. A save whose base is no longer the thread's
    /// latest is written whole instead, under the same lock.
    async fn insert_once(
        &self,
        checkpoint: &Checkpoint,
        encoding: &Encoding,
        row: &SaveRow,
        expected: Option<i64>,
        token: Option<i64>,
        may_be_written: &mut bool,
    ) -> Result<LeasedSave, Failure> {
        let mut client = self.link.client().await?;
        let lock = client.statement(THREAD_LOCK).await?;
        let insert = client.statement(INSERT_SQL).await?;
        let tx = client.transaction().await?;
        tx.execute(&lock, &[&checkpoint.thread_id]).await?;
        if let Some(token) = token {
            let current: Option<i64> = tx
                .query_opt(
                    "SELECT token FROM checkpoint_leases WHERE thread_id = $1",
                    &[&checkpoint.thread_id],
                )
                .await?
                .map(|row| row.get(0));
            if current != Some(token) {
                // Lets the thread's lock go now, not when the connection
                // next gets to a queued rollback.
                let _ = tx.rollback().await;
                return Ok(LeasedSave::LeaseLost);
            }
        }
        // The latest sequence is read under the lock, so nothing can be
        // appended between the check and the insert.
        let own = |encoding: &Encoding| -> Vec<Blob> {
            if self.link.blobs.is_none() {
                encoding.blobs.clone()
            } else {
                Vec::new()
            }
        };
        let stored = encoding.stored(checkpoint);
        let mut inserted = insert_statement(
            &tx,
            &insert,
            stored,
            row,
            expected,
            encoding,
            &own(encoding),
        )
        .await?;
        let mut whole = None;
        if inserted.is_none() && encoding.is_based() {
            let written = encoding.whole(checkpoint);
            if let Some(blobs) = &self.link.blobs {
                offload::put_all(&**blobs, &checkpoint.thread_id, &written.blobs).await?;
            }
            let whole_row = SaveRow::of(written.stored(checkpoint))?;
            inserted = insert_statement(
                &tx,
                &insert,
                written.stored(checkpoint),
                &whole_row,
                expected,
                &written,
                &own(&written),
            )
            .await?;
            whole = Some((written, whole_row.bytes));
        }
        self.link.fault("save:before-commit")?;
        if inserted.is_some() {
            *may_be_written = true;
        }
        self.link.fault("save:commit")?;
        tx.commit().await?;
        self.link.fault("save:after-commit")?;
        let Some(sequence) = inserted else {
            self.link.forget(&checkpoint.thread_id);
            return Ok(LeasedSave::NotLatest);
        };
        let (written, bytes) = match &whole {
            Some((whole, bytes)) => (whole, *bytes),
            None => (encoding, row.bytes),
        };
        record_saved("postgres", written, bytes);
        if let Some(writer) = &self.link.writer {
            writer.saved(checkpoint, written, sequence, &checkpoint.created_at);
        }
        Ok(LeasedSave::Saved)
    }

    async fn load_latest_row(
        &self,
        thread_id: &str,
    ) -> Result<Option<LatestCheckpoint>, CheckpointStoreError> {
        let latest = self
            .link
            .retry
            .run("load", || self.latest_once(thread_id))
            .await?;
        Ok(latest.map(|(sequence, checkpoint)| LatestCheckpoint {
            checkpoint,
            sequence: Some(sequence),
        }))
    }

    async fn load_row(&self, thread_id: &str) -> Result<Option<Checkpoint>, CheckpointStoreError> {
        let latest = self
            .link
            .retry
            .run("load", || self.latest_once(thread_id))
            .await?;
        Ok(latest.map(|(_, checkpoint)| checkpoint))
    }

    /// `thread_id`'s latest row, decoded; the store then knows it as the
    /// thread's latest.
    async fn latest_once(&self, thread_id: &str) -> Result<Option<(i64, Checkpoint)>, Failure> {
        let Some((sequence, decoded, depth)) = self.link.row(thread_id, None).await? else {
            return Ok(None);
        };
        if let Some(writer) = &self.link.writer {
            writer.loaded(sequence, depth, &decoded, &decoded.checkpoint.created_at);
        }
        Ok(Some((sequence, decoded.checkpoint)))
    }

    async fn list_rows(&self, query: &ThreadQuery) -> Result<ThreadPage, CheckpointStoreError> {
        let cursor = query.validate()?;
        let mut filters: Vec<String> = Vec::new();
        let mut params: Vec<Param> = Vec::new();
        if !query.states.is_empty() {
            let states: Vec<String> = query
                .states
                .iter()
                .map(|state| state.as_str().to_string())
                .collect();
            let p = bind(&mut params, Box::new(states));
            filters.push(format!("h.state = ANY({p})"));
        }
        if !query.metadata.is_empty() {
            let wanted = serde_json::to_value(&query.metadata).map_err(json_err)?;
            let p = bind(&mut params, Box::new(wanted));
            filters.push(format!("h.metadata @> {p}"));
        }
        if let Some(after) = query.updated_after_text() {
            let p = bind(&mut params, Box::new(after));
            filters.push(format!("h.updated_at > {p}"));
        }
        if let Some(before) = query.updated_before_text() {
            let p = bind(&mut params, Box::new(before));
            filters.push(format!("h.updated_at < {p}"));
        }
        if let Some(due) = query.due_by_text() {
            let p = bind(&mut params, Box::new(due));
            filters.push(format!("h.state = 'interrupted' AND h.wake_at <= {p}"));
        }
        if let Some(id) = &query.graph_id {
            let p = bind(&mut params, Box::new(id.clone()));
            filters.push(format!("h.graph_id = {p}"));
        }
        if let Some(version) = &query.graph_version {
            let p = bind(&mut params, Box::new(version.clone()));
            filters.push(format!("h.graph_version = {p}"));
        }
        if let Some(cursor) = &cursor {
            let u = bind(&mut params, Box::new(cursor.updated_at.clone()));
            let t = bind(&mut params, Box::new(cursor.thread_id.clone()));
            filters.push(format!("(h.updated_at, h.thread_id) < ({u}, {t})"));
        }
        let filter = if filters.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", filters.join(" AND "))
        };
        // One more than a page, to know whether another follows.
        let limit = bind(&mut params, Box::new(query.limit as i64 + 1));
        // A summary reads no state: of an encoded row it needs only the
        // version it was saved with.
        let sql = format!(
            "SELECT c.thread_id, c.sequence, c.schema_version, c.status, c.suspended_node,
                    c.next_node, 'null'::jsonb AS frozen_state, c.metadata, c.created_at,
                    c.resumed_at, c.nested, c.decision, c.wait, c.graph_id, c.graph_version,
                    h.updated_at,
                    CASE WHEN c.schema_version IN ({ENCODED_VERSIONS_SQL})
                         THEN c.frozen_state->>'checkpoint_version' END AS checkpoint_version
             FROM checkpoint_threads h
             JOIN checkpoints c ON c.thread_id = h.thread_id AND c.sequence = h.sequence
             {filter}
             ORDER BY h.updated_at DESC, h.thread_id DESC
             LIMIT {limit}"
        );
        Ok(self
            .link
            .retry
            .run("list", || self.list_once(&sql, &params, query.limit))
            .await?)
    }

    async fn list_once(
        &self,
        sql: &str,
        params: &[Param],
        limit: usize,
    ) -> Result<ThreadPage, Failure> {
        let refs: Vec<&(dyn ToSql + Sync)> = params
            .iter()
            .map(|param| param.as_ref() as &(dyn ToSql + Sync))
            .collect();
        let client = self.link.client().await?;
        let rows = client.query(sql, &refs).await?;
        let more = rows.len() > limit;
        let mut threads = Vec::with_capacity(rows.len().min(limit));
        let mut next_cursor = None;
        for row in rows.iter().take(limit) {
            let (sequence, mut checkpoint) = read_checkpoint(row)?;
            checkpoint.schema_version =
                listed_version(checkpoint.schema_version, row.get("checkpoint_version"));
            if more {
                let updated_at: String = row.get("updated_at");
                next_cursor = Some(ThreadCursor::new(updated_at, &checkpoint.thread_id).encode());
            }
            threads.push(ThreadSummary::of(sequence, &checkpoint)?);
        }
        Ok(ThreadPage {
            threads,
            next_cursor,
        })
    }

    async fn log_rows(
        &self,
        thread_id: &str,
        after_sequence: Option<i64>,
        limit: usize,
    ) -> Result<CheckpointLogPage, CheckpointStoreError> {
        check_limit(limit)?;
        let entries = self
            .link
            .retry
            .run("load", || self.log_once(thread_id, after_sequence, limit))
            .await?;
        Ok(CheckpointLogPage::new(entries, limit))
    }

    async fn log_once(
        &self,
        thread_id: &str,
        after_sequence: Option<i64>,
        limit: usize,
    ) -> Result<Vec<CheckpointLogEntry>, Failure> {
        let client = self.link.client().await?;
        let rows = client
            .query(
                &format!(
                    "SELECT {CHECKPOINT_COLUMNS} FROM checkpoints c
                     WHERE c.thread_id = $1 AND c.sequence > $2
                     ORDER BY c.sequence LIMIT $3"
                ),
                &[&thread_id, &after_sequence.unwrap_or(-1), &(limit as i64)],
            )
            .await?;
        let rows = rows
            .iter()
            .map(read_checkpoint)
            .collect::<Result<Vec<_>, CheckpointStoreError>>()?;
        // A page whose first row is a delta is read with the rows it needs, on
        // a connection of its own: a store built from one client has only that.
        drop(client);
        let depth = rows.first().map_or(0, |(_, first)| depth_of(first)) as i64;
        let decoded = if depth == 0 {
            deltas::decode(rows, 0)?
        } else {
            let first = rows[0].0;
            let mut all = self.link.range(thread_id, first - depth, first - 1).await?;
            let from = all.len();
            all.extend(rows);
            deltas::decode(all, from)?
        };
        Ok(self
            .link
            .resolve(thread_id, decoded)
            .await?
            .into_iter()
            .map(|row| CheckpointLogEntry {
                sequence: row.sequence,
                checkpoint: row.checkpoint,
            })
            .collect())
    }

    /// Deletes the logs of threads that are done with
    /// (`design/34-Storage-Hardening.md`, "Retention"), oldest first. Each
    /// thread is deleted under its advisory lock, and only if its head is
    /// still the one that was picked and no live lease holds it, so a thread
    /// that ran again meanwhile is kept. Its blobs go in the same transaction,
    /// or, in another blob store, after it
    /// (`design/41-Checkpoint-Efficiency.md`, "Retention"), and a failure
    /// there is the purge's.
    async fn purge_rows(
        &self,
        older_than: DateTime<Utc>,
        states: &PurgeStates,
        limit: usize,
    ) -> Result<usize, CheckpointStoreError> {
        check_limit(limit)?;
        if states.is_empty() {
            return Ok(0);
        }
        let names = states.names();
        let before = timestamp_text(older_than);
        let candidates = self
            .link
            .retry
            .run("purge", || self.purge_candidates(&names, &before, limit))
            .await?;
        let mut purged = 0;
        for (thread_id, sequence) in &candidates {
            // Purging twice is purging once: a repeat finds the head gone and
            // deletes nothing.
            if self
                .link
                .retry
                .run("purge", || {
                    self.purge_once(thread_id, *sequence, &names, &before)
                })
                .await?
            {
                purged += 1;
                self.link.forget(thread_id);
                if let Some(blobs) = &self.link.blobs {
                    blobs.delete_thread(thread_id).await?;
                }
            }
        }
        Ok(purged)
    }

    /// The threads a purge takes, oldest first. A held thread isn't a
    /// candidate, so it takes no place in the limit.
    async fn purge_candidates(
        &self,
        names: &[String],
        before: &str,
        limit: usize,
    ) -> Result<Vec<(String, i64)>, Failure> {
        let client = self.link.client().await?;
        let rows = client
            .query(
                "SELECT h.thread_id, h.sequence FROM checkpoint_threads h
                 WHERE h.state = ANY($1) AND h.updated_at < $2
                   AND NOT EXISTS (
                       SELECT 1 FROM checkpoint_leases l WHERE l.thread_id = h.thread_id AND l.expires_at > clock_timestamp())
                 ORDER BY h.updated_at, h.thread_id LIMIT $3",
                &[&names, &before, &(limit as i64)],
            )
            .await?;
        Ok(rows.iter().map(|row| (row.get(0), row.get(1))).collect())
    }

    async fn purge_once(
        &self,
        thread_id: &str,
        sequence: i64,
        names: &[String],
        before: &str,
    ) -> Result<bool, Failure> {
        let mut client = self.link.client().await?;
        let tx = client.transaction().await?;
        tx.execute(THREAD_LOCK, &[&thread_id]).await?;
        let head = tx
            .execute(
                "DELETE FROM checkpoint_threads h
                 WHERE h.thread_id = $1 AND h.sequence = $2 AND h.state = ANY($3) AND h.updated_at < $4
                   AND NOT EXISTS (
                       SELECT 1 FROM checkpoint_leases l WHERE l.thread_id = h.thread_id AND l.expires_at > clock_timestamp())",
                &[&thread_id, &sequence, &names, &before],
            )
            .await?;
        if head == 0 {
            let _ = tx.rollback().await;
            return Ok(false);
        }
        // The names are the store's own constants, not input.
        for table in ["checkpoints", "checkpoint_leases", "checkpoint_blobs"] {
            tx.execute(
                &format!("DELETE FROM {table} WHERE thread_id = $1"),
                &[&thread_id],
            )
            .await?;
        }
        tx.commit().await?;
        Ok(true)
    }
}

impl CheckpointStore for PostgresCheckpointStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        Box::pin(self.save_row(checkpoint))
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        Box::pin(self.load_row(thread_id))
    }

    fn load_latest<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<LatestCheckpoint>, CheckpointStoreError>> {
        Box::pin(self.load_latest_row(thread_id))
    }

    fn save_if_latest(
        &self,
        checkpoint: Checkpoint,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'_, Result<bool, CheckpointStoreError>> {
        Box::pin(async move {
            let saved = self.insert_row(checkpoint, expected_sequence, None).await?;
            Ok(saved == LeasedSave::Saved)
        })
    }

    fn try_acquire_lease<'a>(
        &'a self,
        thread_id: &'a str,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<LeaseAttempt, CheckpointStoreError>> {
        Box::pin(self.acquire_lease_row(thread_id, owner, ttl))
    }

    fn renew_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        Box::pin(self.renew_lease_row(lease, ttl))
    }

    fn release_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
    ) -> BoxFuture<'a, Result<bool, CheckpointStoreError>> {
        Box::pin(self.release_lease_row(lease))
    }

    fn get_lease<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        Box::pin(self.held_lease_row(thread_id))
    }

    fn save_under_lease<'a>(
        &'a self,
        checkpoint: Checkpoint,
        lease: &'a ThreadLease,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'a, Result<LeasedSave, CheckpointStoreError>> {
        Box::pin(self.save_under_lease_row(checkpoint, lease, expected_sequence))
    }
}

impl CheckpointQuery for PostgresCheckpointStore {
    fn list_threads<'a>(
        &'a self,
        query: &'a ThreadQuery,
    ) -> BoxFuture<'a, Result<ThreadPage, CheckpointStoreError>> {
        Box::pin(self.list_rows(query))
    }

    fn get_log<'a>(
        &'a self,
        thread_id: &'a str,
        after_sequence: Option<i64>,
        limit: usize,
    ) -> BoxFuture<'a, Result<CheckpointLogPage, CheckpointStoreError>> {
        Box::pin(self.log_rows(thread_id, after_sequence, limit))
    }
}

impl CheckpointRetention for PostgresCheckpointStore {
    fn purge<'a>(
        &'a self,
        older_than: DateTime<Utc>,
        states: &'a PurgeStates,
        limit: usize,
    ) -> BoxFuture<'a, Result<usize, CheckpointStoreError>> {
        Box::pin(self.purge_rows(older_than, states, limit))
    }
}

/// Postgres-backed [`HistoryStore`]: one `history_records` row per message,
/// ordered by a per-session `sequence`, safe for concurrent appenders.
///
/// A read is repeated after a transient failure
/// (`design/34-Storage-Hardening.md`); an append only when it failed before
/// its commit, since the store can't tell a message that was written from one
/// that wasn't.
pub struct PostgresHistoryStore {
    link: Link,
    steps: Vec<SchemaStep>,
}

impl PostgresHistoryStore {
    pub async fn connect(url: &str) -> Result<Self, HistoryStoreError> {
        Self::connect_with(url, PostgresStoreOptions::new()).await
    }

    /// See [`PostgresCheckpointStore::connect_with`].
    pub async fn connect_with(
        url: &str,
        options: PostgresStoreOptions,
    ) -> Result<Self, HistoryStoreError> {
        let link = Link::connect(url, &options).await?;
        Self::prepared(link, options).await
    }

    /// See [`PostgresCheckpointStore::from_client`].
    pub async fn from_client(client: Client) -> Result<Self, HistoryStoreError> {
        Self::from_client_with(client, PostgresStoreOptions::new()).await
    }

    /// See [`PostgresCheckpointStore::from_client_with`].
    pub async fn from_client_with(
        client: Client,
        options: PostgresStoreOptions,
    ) -> Result<Self, HistoryStoreError> {
        Self::prepared(Link::over(client, &options), options).await
    }

    async fn prepared(
        link: Link,
        options: PostgresStoreOptions,
    ) -> Result<Self, HistoryStoreError> {
        let store = Self {
            link,
            steps: options.later_steps,
        };
        store.prepare(options.auto_migrate).await?;
        Ok(store)
    }

    /// See [`PostgresCheckpointStore::migrate`].
    pub async fn migrate(&self) -> Result<(), HistoryStoreError> {
        self.prepare(true).await
    }

    async fn prepare(&self, apply: bool) -> Result<(), HistoryStoreError> {
        self.link
            .retry
            .run("migrate", || {
                self.link.migrate(Component::History, &self.steps, apply)
            })
            .await?;
        Ok(())
    }

    async fn append_row(
        &self,
        session_id: &str,
        message: ChatMessage,
    ) -> Result<(), HistoryStoreError> {
        let timestamp = message
            .timestamp
            .to_rfc3339_opts(SecondsFormat::AutoSi, true);
        let mut number = 1;
        loop {
            let mut committing = false;
            match self
                .append_once(session_id, &message, &timestamp, &mut committing)
                .await
            {
                Ok(()) => return Ok(()),
                Err(failure) => {
                    if committing || !self.link.retry.again("append", number, &failure).await {
                        return Err(failure.into());
                    }
                }
            }
            number += 1;
        }
    }

    async fn append_once(
        &self,
        session_id: &str,
        message: &ChatMessage,
        timestamp: &str,
        committing: &mut bool,
    ) -> Result<(), Failure> {
        let mut client = self.link.client().await?;
        let tx = client.transaction().await?;
        tx.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended('history:' || $1::text, 0))",
            &[&session_id],
        )
        .await?;
        tx.execute(
            "INSERT INTO history_records (session_id, sequence, role, content, timestamp)
             VALUES (
                $1,
                (SELECT COALESCE(MAX(sequence), -1) + 1 FROM history_records WHERE session_id = $1),
                $2, $3, $4)",
            &[
                &session_id,
                &role_to_str(message.role),
                &message.content.as_ref(),
                &timestamp,
            ],
        )
        .await?;
        *committing = true;
        tx.commit().await?;
        self.link.fault("append:after-commit")?;
        Ok(())
    }

    async fn history_rows(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<ChatMessage>, HistoryStoreError> {
        Ok(self
            .link
            .retry
            .run("load", || self.history_once(session_id, limit))
            .await?)
    }

    async fn history_once(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<ChatMessage>, Failure> {
        let client = self.link.client().await?;
        let rows = client
            .query(
                "SELECT role, content, timestamp FROM history_records
                 WHERE session_id = $1 ORDER BY sequence DESC LIMIT $2",
                &[&session_id, &i64::try_from(limit).unwrap_or(i64::MAX)],
            )
            .await?;

        let mut messages = Vec::with_capacity(rows.len());
        for row in &rows {
            let role: String = row.get("role");
            let content: String = row.get("content");
            let timestamp: String = row.get("timestamp");
            messages.push(ChatMessage {
                role: role_from_str(&role)?,
                content: Arc::from(content.as_str()),
                timestamp: DateTime::parse_from_rfc3339(&timestamp)
                    .map_err(|e| HistoryStoreError(e.to_string()))?
                    .with_timezone(&Utc),
            });
        }
        // Queried DESC (to LIMIT to the most recent rows); callers want
        // chronological order back.
        messages.reverse();
        Ok(messages)
    }
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

impl HistoryStore for PostgresHistoryStore {
    fn append<'a>(
        &'a self,
        session_id: &'a str,
        message: ChatMessage,
    ) -> BoxFuture<'a, Result<(), HistoryStoreError>> {
        Box::pin(self.append_row(session_id, message))
    }

    fn get_history<'a>(
        &'a self,
        session_id: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<ChatMessage>, HistoryStoreError>> {
        Box::pin(self.history_rows(session_id, limit))
    }
}
