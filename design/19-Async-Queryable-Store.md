# Datalium Strata: Async, Queryable Checkpoint Store

The checkpoint store is the last synchronous thing on the executor's path, and the only thing it can do is hand back a thread's latest checkpoint. Two problems follow:

- **Blocking calls.** .NET calls `ICheckpointStore.Save` / `Load` inline, blocking a pool thread for every Postgres round trip. Rust hops to the blocking pool for every call, because the sync `postgres` crate panics on a runtime thread (design/17). Saves are rare today, but SG-10 (a checkpoint after every node) would turn this into the run's main cost.
- **No queries.** Every SQL log and every Redis list already keeps **every** checkpoint, but neither engine can list threads, filter them, or read a thread's history. Only the Rust CLI can, through its own SQLite reader. So a downstream product keeps its own index of threads, fed from run results and events, just to answer "which threads are waiting for a human?" and "what happened to this thread?".

This document specifies both halves of SG-11 as one change. Each half touches the store port and every adapter in both engines, so doing them separately would mean touching every adapter twice:

- **Async half:** async save and load in both engines, implemented natively by every adapter, and awaited directly by the executor.
- **Query half:** a separate, opt-in query port that lists threads, with filters and a stable cursor, and reads a thread's checkpoint log page by page.

The SG numbers refer to the gap log of StrataOps (not public yet). Both engines get every capability in one change (strict parity, StrataOps ADR 0003). The APIs are idiomatic in each language, not identical.

It was written as a blueprint before implementation, reviewed, and is now built. "Review decisions" settles the questions the review was asked, and "Implementation Status" at the end records where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| .NET compatibility | **Source-compatible.** `ICheckpointStore` gains `SaveAsync` / `LoadAsync` as **default interface methods** that call the sync members inline, which is exactly today's behaviour. Built-in adapters override them with native async I/O. The executor only calls the async members. No new store interface, and no new `RunAsync` overloads. |
| Rust trait shape | **Breaking, for store implementors only.** `CheckpointStore`'s methods return a boxed future (`BoxFuture`, the pattern `node.rs` already uses), so `Arc<dyn CheckpointStore>` keeps working and `run` / `resume` keep their signatures. No `async-trait` dependency. |
| Sync stores | **Still supported.** .NET: a store that only implements `Save` / `Load` keeps working unchanged. Rust: the old trait survives as `SyncCheckpointStore`, and `BlockingCheckpointStore::new(store)` adapts one by running it on the blocking pool, exactly as the engine does today. |
| Rust history | **`HistoryStore` becomes async too**, as design/17 promised for SG-11. It gives parity with .NET's `IHistoryStore` (already async) and lets `strata-memory` drop the sync `postgres` crate. |
| Native async per adapter | .NET: Npgsql async, StackExchange.Redis async, EF Core async. Rust: `tokio-postgres`, the `redis` crate's async multiplexed connection, and `spawn_blocking` inside the SQLite adapter (rusqlite has no async API). |
| Cancellation of store calls | **Store calls are never cancelled by the run's token.** That is the current behaviour, and design/17 depends on it: a cancelled run must still write its `cancelled` checkpoint. A hung store is bounded by the store's own timeouts. |
| Query port | **Separate and opt-in:** `ICheckpointQuery` / `CheckpointQuery`, with two operations: `ListThreads` and `GetLog`. All four built-in adapters in each engine implement it. |
| What a listed thread's state means | A derived **`ThreadState`**: `Interrupted`, `Failed` or `Cancelled` when the latest checkpoint has that status and has **not** been resumed; `Resumed` when it has; `Completed` if something wrote a `completed` checkpoint. Nothing in the engine writes one today. |
| List filters | Any set of states. Metadata `key = value` pairs, where every pair must match. Updated after or before. Paged newest first, with an opaque keyset cursor on `(updated_at, thread_id)`. |
| SQL layout | **A new `checkpoint_threads` table**, one row per thread pointing at its latest checkpoint, maintained by an **`AFTER INSERT` trigger** on `checkpoints`. The trigger keeps it correct whoever writes the log: either runtime, an older runtime, the CLI's test seeding, or raw SQL. `checkpoints` itself is unchanged, and the table is backfilled from the log when it is created. |
| Redis layout | **Index sorted sets** (`{prefix}:threads`, and one per state), maintained atomically with the `RPUSH` by a Lua script. Metadata filters are evaluated on the candidates' latest checkpoints, which a listing reads anyway. An existing keyspace is indexed once when a store first connects. |
| Wire format | **No change.** No `schema_version` bump, and `checkpoints` keeps its columns. New fixtures pin the head table, the Redis index and a shared set of query cases that every adapter in both engines, and the CLI, must answer identically. |
| CLI | **Moves onto `strata-memory`'s query port** and drops `sqlx`. It keeps reading logs written by either engine. Because of the move, Postgres and Redis support (SG-31's read side) is a URL in `--storage`, and it is part of this change. |
| What the query can't answer until SG-10 | Whether a thread is **running**, and whether a resumed thread **finished**. A fresh run writes nothing until it interrupts, fails or is cancelled. A resumed thread shows `Resumed` whether it is still running, completed, or died with its process. |
| Not in scope | Per-node checkpoints (SG-10), leases (SG-12), graph identity (SG-13), retention, TTL and deltas (SG-15). |

---

## What the store holds today

This was verified at `bc3aa87`. It shapes several decisions below.

- **Writes happen at four points:** an interrupt boundary, a terminal failure, a cancellation, and the `resumed_at` stamp a resume puts on the checkpoint it resumes. The stamp **re-saves** that checkpoint with only `resumed_at` set, so in an append-only store it is a new row whose `created_at` is the *original* checkpoint's.
- **No engine writes `completed`.** The status exists in the schema and every reader handles it, but a run that completes writes nothing. A thread that was resumed and then completed therefore ends with its resume stamp as its latest row.
- **The two in-memory stores keep only the latest checkpoint.** They hold a dictionary keyed by thread id. Every other adapter keeps the whole log.
- **The SQL logs and the Redis lists already keep everything,** with the same columns and JSON in both engines. The CLI's `thread list` derives "latest per thread" with a `GROUP BY thread_id` join over the whole table.
- **Timestamps are RFC 3339 text at second precision with a `Z` suffix** (`2026-10-02T09:15:00Z`), written the same way by both engines. So text order is time order.
- **Custom stores exist downstream.** `strata-demos-dotnet` demo 12 passes stores around as `ICheckpointStore`. Every Rust test file has its own `TestStore`. The engine has a third one inside it: the subgraph's `CapturingStore`.

---

## Async store (the async half)

### .NET

```csharp
public interface ICheckpointStore
{
    void Save(Checkpoint checkpoint);

    Checkpoint? Load(string threadId);

    /// What the engine calls. The default runs Save inline, which is exactly what the engine did
    /// before this change, so a store that only implements the sync members behaves as it did.
    ValueTask SaveAsync(Checkpoint checkpoint, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Save(checkpoint);
        return ValueTask.CompletedTask;
    }

    ValueTask<Checkpoint?> LoadAsync(string threadId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(Load(threadId));
    }
}
```

**Why default interface methods rather than a new `IAsyncCheckpointStore`:**

- **Overload ambiguity.** A separate interface needs the engine to accept both. Doubling the twelve store-taking `RunAsync` / `ResumeAsync` / `RunStreaming` / `ResumeStreaming` overloads breaks every caller that passes a built-in adapter. The adapters would implement both interfaces, so `RunAsync(id, state, new PostgresCheckpointStore(…))` becomes an ambiguous call (CS0121). The other route is a runtime type check (`store as IAsyncCheckpointStore`) behind the existing `ICheckpointStore` parameter. Then an async-only store couldn't be passed at all unless it also implemented the sync interface, which is the same burden as default methods, with a second interface on top.
- **One port.** Default methods keep one port that every existing store already satisfies. Custom stores, the demo-12 `Stores` record and `ObservedStore` compile unchanged, and a store opts in to real async I/O by overriding two methods.
- **The cost.** A store written async-first must still provide `Save` / `Load`. Since the engine never calls them, such a store can make them thin sync-over-async wrappers, or throw `NotSupportedException` if nothing else calls them. That is documented on the interface.

**Why `ValueTask` (`IHistoryStore` uses `Task`):** the default implementations and the in-memory store complete synchronously, and the executor awaits each call exactly once. `ValueTask` makes those calls allocation-free. That matters once SG-10 saves after every node. Handler shapes already use `ValueTask` (design/17).

**Adapters:**

| Adapter | Async members | Sync members |
|---|---|---|
| `InMemoryCheckpointStore` | Complete synchronously. | Unchanged. |
| `EfCoreCheckpointStore` | `ExecuteSqlAsync` and `FirstOrDefaultAsync`, serialized by a `SemaphoreSlim` because a `DbContext` isn't thread-safe. `EfCoreHistoryStore` already does this. Concurrent runs sharing one store race on the context today, so this also fixes a latent bug. | Wrappers over the async members. |
| `PostgresCheckpointStore` | `OpenConnectionAsync`, `BeginTransactionAsync`, `PostgresSchema.LockAsync` (it already exists, for the history store), `ExecuteNonQueryAsync`, `ExecuteReaderAsync`. | Wrappers over the async members. |
| `RedisCheckpointStore` | `ScriptEvaluateAsync` (see "Redis") and `ListGetByIndexAsync`. | Wrappers over the async members. |

Each adapter has one code path, async. The sync members are the "thin wrapper" the gap doc proposed, and they only serve direct callers, since the engine no longer uses them. Sync-over-async is safe here: there's no `SynchronizationContext` in the core or the adapters, and every await uses `ConfigureAwait(false)`.

### Rust

```rust
/// A boxed, `Send` future: what every store method returns, so the traits stay object-safe and the
/// engine can hold an `Arc<dyn CheckpointStore>`.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait CheckpointStore: Send + Sync {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>>;
    fn load<'a>(&'a self, thread_id: &'a str)
        -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>>;
}

impl CheckpointStore for MyStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        Box::pin(async move { self.client.insert(&checkpoint).await.map_err(to_store_error) })
    }
    // …
}
```

**Why `BoxFuture` rather than `async-trait`:** both compile to the same thing: a boxed `Send` future per call. `BoxFuture` needs no proc-macro dependency, and it is the shape the engine already uses for every node handler (`node::BoxFuture`, which becomes this public alias). The signatures also say what they are in rustdoc, where `#[async_trait]` rewrites them. The cost is `Box::pin(async move { … })` in each implementation. Native `async fn` in traits isn't object-safe, and the engine needs `dyn`.

**Sync stores** keep working through an adapter:

```rust
pub trait SyncCheckpointStore: Send + Sync + 'static {        // the trait as it is today
    fn save(&self, checkpoint: Checkpoint) -> Result<(), CheckpointStoreError>;
    fn load(&self, thread_id: &str) -> Result<Option<Checkpoint>, CheckpointStoreError>;
}

/// Runs a synchronous store on tokio's blocking pool, exactly as the engine did before SG-11.
pub struct BlockingCheckpointStore<T>(Arc<T>);
impl<T: SyncCheckpointStore> BlockingCheckpointStore<T> { pub fn new(store: T) -> Self; }
impl<T: SyncCheckpointStore> CheckpointStore for BlockingCheckpointStore<T> { … }
```

A blanket `impl<T: SyncCheckpointStore> CheckpointStore for T` isn't possible: `spawn_blocking` needs a `'static` closure, so it needs the `Arc` the wrapper holds. A blanket impl would also stop an adapter from implementing both traits.

**`HistoryStore` follows the same pattern:** `append` and `get_history` return `BoxFuture`, there's a `SyncHistoryStore` and a `BlockingHistoryStore`, and async handlers call `ctx.history()…await` instead of going through `spawn_blocking`. design/17 deferred this to SG-11. Without it, `strata-memory` would need both the sync `postgres` crate (for history) and `tokio-postgres` (for checkpoints).

**Adapters:**

| Adapter | How it is async |
|---|---|
| `InMemoryCheckpointStore` | A `std::sync::Mutex` that is never held across an await, so it completes on first poll. |
| `SqliteCheckpointStore` | The connection moves into an `Arc<Mutex<rusqlite::Connection>>`, and each call runs on `spawn_blocking` *inside the adapter*. rusqlite has no async API, and a local file is the one place the blocking pool is the right tool. |
| `PostgresCheckpointStore` | `tokio-postgres`. One connection behind a `tokio::sync::Mutex` (a transaction needs `&mut Client`), as today's one connection behind a `std::sync::Mutex`. `connect` becomes `async fn` and spawns the connection task, so **a store must be created inside the runtime that uses it**. `from_client` takes a `tokio_postgres::Client` instead, for TLS. |
| `RedisCheckpointStore` | The `redis` crate's `MultiplexedConnection` (features `tokio-comp` and `script`). It is cloneable and pipelined, so it needs no mutex. `connect` becomes `async fn`. |

Dependencies: `postgres` is replaced by `tokio-postgres` (with `with-serde_json-1`). `redis` gains `tokio-comp`, `aio` and `script`. `strata-memory` gains `tokio` (`rt`, `sync`) as a normal dependency.

### The executor

- **.NET:** every `store.Save(…)` / `store.Load(…)` becomes `await store.SaveAsync(…, CancellationToken.None).ConfigureAwait(false)` (or `LoadAsync`). `CancelAt`, the failed-checkpoint writer and the boundary freeze become async. `ObservedStore` overrides `SaveAsync` / `LoadAsync` and times the await. The subgraph's `CapturingStore` overrides them to complete synchronously.
- **Rust:** `save_checkpoint` / `load_checkpoint` await `store.save(…)` / `store.load(…)` directly. The `spawn_blocking` hop and its `JoinError` handling are removed. A panic in a store now unwinds through the run's future, where `CatchUnwind` (design/18) ends the run's telemetry before the unwind continues. Before, `resume_unwind` re-raised it from the blocking pool, so the outcome is the same. `CapturingStore` returns ready futures.
- **Cancellation.** The engine never passes the run's token to a store call, and Rust never races one against it. That is today's behaviour, made explicit. A cancelled run must still write its `cancelled` checkpoint, and a resume started with a cancelled token still stamps `resumed_at` before writing one (design/17, "Already cancelled"). Cancelling a save halfway would lose exactly the record cancellation exists to leave. A store that hangs is bounded by its own timeouts: Npgsql's command timeout, StackExchange.Redis's sync and async timeouts, and the `tokio-postgres` / `redis` connect timeouts the adapters already set. The `CancellationToken` parameter on the .NET members serves direct callers.
- **Telemetry.** `strata.checkpoint.latency` and `CheckpointSaved` keep their meaning and call sites. In Rust the latency no longer includes a hop to the blocking pool, except inside the SQLite adapter, where the hop is part of the store's own cost. design/18's metric table is updated to say so.

---

## Query port (the query half)

### Shape

```csharp
public interface ICheckpointQuery
{
    ValueTask<ThreadPage> ListThreadsAsync(ThreadQuery query, CancellationToken cancellationToken = default);

    ValueTask<CheckpointLogPage> GetLogAsync(
        string threadId, long? afterSequence = null, int limit = 100, CancellationToken cancellationToken = default);
}

public sealed record ThreadQuery
{
    public IReadOnlySet<ThreadState>? States { get; init; }                        // null: any state
    public IReadOnlyDictionary<string, string> Metadata { get; init; } = Empty;    // every pair must match
    public DateTimeOffset? UpdatedAfter { get; init; }                              // exclusive
    public DateTimeOffset? UpdatedBefore { get; init; }                             // exclusive
    public int Limit { get; init; } = 100;                                          // 1..1000
    public string? Cursor { get; init; }                                            // from the previous page
}

public enum ThreadState { Interrupted, Failed, Cancelled, Resumed, Completed }

public sealed record ThreadSummary(
    string ThreadId,
    long Sequence,                                // of the latest checkpoint
    ThreadState State,
    CheckpointStatus Status,                      // the latest checkpoint's own status
    string? SuspendedNode,
    string? NextNode,
    IReadOnlyList<string> WaitingAt,              // as Checkpoint.WaitingAt()
    IReadOnlyDictionary<string, string> Metadata, // the whole map, engine keys included
    DateTimeOffset UpdatedAt,
    string SchemaVersion);

public sealed record ThreadPage(IReadOnlyList<ThreadSummary> Threads, string? NextCursor);

public sealed record CheckpointLogEntry(long Sequence, Checkpoint Checkpoint);
public sealed record CheckpointLogPage(IReadOnlyList<CheckpointLogEntry> Entries, long? NextAfterSequence);
```

```rust
pub trait CheckpointQuery: Send + Sync {
    fn list_threads<'a>(&'a self, query: &'a ThreadQuery)
        -> BoxFuture<'a, Result<ThreadPage, CheckpointStoreError>>;
    fn get_log<'a>(&'a self, thread_id: &'a str, after_sequence: Option<i64>, limit: usize)
        -> BoxFuture<'a, Result<CheckpointLogPage, CheckpointStoreError>>;
}

let page = store.list_threads(
    &ThreadQuery::default()
        .with_states([ThreadState::Interrupted])
        .with_metadata("tenant", "acme")
        .with_limit(50),
).await?;
```

Both live in the core (`Datalium.Strata.Interruption` / `datalium_strata::checkpoint`) beside the store port. The Rust types mirror the .NET records: `ThreadState` and `ThreadSummary` are `#[non_exhaustive]`, timestamps are `chrono::DateTime<Utc>`, and sequences are `i64`. Every built-in adapter implements both ports, so one object serves the engine and the query side. A caller that only has an `ICheckpointStore` checks for the query port with `store is ICheckpointQuery q` (.NET). In Rust it keeps an `Arc<PostgresCheckpointStore>` and coerces it to whichever trait object it needs.

**Why a separate port:** a store that can only save and load (a key-value service, the SDK-side Cloud store of SG-18, a test double) stays a complete store. Listing needs an index that not every backend can afford, so a store opts in.

### `ThreadState`, and why not just the status

The latest checkpoint's status alone answers the wrong question. A thread that was interrupted, approved, resumed and completed still has `status = interrupted` on its latest row: the resume stamp. Nothing is written when the resumed run completes. So "list interrupted threads" by status returns every thread that was **ever** waiting.

The state is derived from the latest checkpoint:

| Latest checkpoint | State | Meaning |
|---|---|---|
| `resumed_at` set (any status) | `Resumed` | A resume began from it. The resumed run may still be running, may have completed (it writes nothing), or may have died with its process. The store can't tell which until SG-10. If the resumed run had stopped again, a newer checkpoint would be the latest. |
| `interrupted`, not resumed | `Interrupted` | Waiting for a human. |
| `failed`, not resumed | `Failed` | Stopped by a node failure. Resumable once (design/16). |
| `cancelled`, not resumed | `Cancelled` | Stopped by cancellation or a deadline. Resumable once (design/17). |
| `completed` | `Completed` | Nothing in the engine writes this today. It is here for SG-10, and for logs that other writers produce. |

`ThreadSummary` carries both the derived `State` and the raw `Status`, so a caller can tell a resumed failure from a resumed interrupt.

### Listing rules

- **Order.** Most recently updated first, ties broken by thread id descending. `UpdatedAt` is `resumed_at` when it is set, otherwise `created_at`: the time the latest row was written, because the resume stamp keeps the original `created_at`.
- **Cursor.** An opaque string encoding the last returned `(updated_at, thread_id)`. Both engines encode it the same way (base64url of a two-element JSON array, documented in `schema/README.md`), so a cursor from one engine works with the other or with the CLI. A cursor is only meaningful with the query that produced it.
- **Consistency while paging.** Keyset paging over a log that is still being written never returns a thread twice in one pass. A thread that is written *during* the pass moves ahead of the cursor and is not returned. That is the right failure for every StrataOps use: a recovery sweep misses a thread only while something is actively writing it, and the next sweep catches it.
- **Short pages.** A page may hold fewer than `Limit` threads even when more follow. Only a null cursor means the end. This lets an adapter bound the work of one call (see "Redis").
- **Metadata.** It is matched against the latest checkpoint's whole `metadata` map, engine keys included, so `error_kind = transient` is a valid filter. Caller metadata is carried onto every checkpoint (design/18), so a thread keeps matching `tenant = acme` through its whole life.
- **`UpdatedAfter` / `UpdatedBefore`** are exclusive, and compared at second precision, the precision timestamps are written at.
- **Limits.** `Limit` must be between 1 and 1000. Anything else is refused: `ArgumentOutOfRangeException` in .NET, a `CheckpointStoreError` in Rust. `GetLog` has the same bounds.

### `GetLog`

It returns every checkpoint of one thread in sequence order, oldest first, as `(sequence, checkpoint)` pairs. `afterSequence` is exclusive, so a page continues from the last sequence returned. `NextAfterSequence` is that sequence when the page was full, and null otherwise. Sequences are the log's own: the `sequence` column in SQL, and the list position in Redis, which starts at 0 like the SQL logs. An unknown thread gives an empty page, not an error. A single checkpoint (the CLI's `checkpoint diff` / `replay`) is `GetLog(thread, sequence - 1, limit: 1)`.

### What it can answer before SG-10, and what it can't

| StrataOps need | Answered? |
|---|---|
| Approvals page: interrupted threads, per tenant | **Yes.** `States = {Interrupted}`, `Metadata = {tenant}`. |
| Recovery sweep: failed and cancelled threads to resume | **Yes.** `States = {Failed, Cancelled}`, plus the owner in metadata. |
| Recovery sweep: threads that were **running** when a worker died | **No.** A fresh run is invisible until it first writes. A resumed run shows `Resumed` whether it is running, done or dead. Both need SG-10's per-node checkpoints and a `running` status, and SG-12's leases to know the owner is dead. `Resumed` threads are the candidate set to cross-check against StrataOps' own index until then. |
| "Is this thread finished?" | **Only negatively.** `Interrupted` / `Failed` / `Cancelled` mean not finished. `Resumed` means unknown. |
| Incident timeline | **Yes, at today's write points:** interrupts, failures, cancellations and resume stamps. With SG-10, the same call returns every node's checkpoint. |
| Graph version per thread | **No.** That is SG-13. |

So StrataOps' `ExecutionIndex` shrinks to: running state (until SG-10), lease ownership (until SG-12), and graph version (until SG-13).

---

## Storage

### SQL: a trigger-maintained head table

`checkpoints` is unchanged. Each SQL store creates one new table beside it, plus a trigger that keeps it current:

```sql
-- SQLite (Postgres: BIGINT and JSONB, the same names)
CREATE TABLE IF NOT EXISTS checkpoint_threads (
    thread_id  TEXT PRIMARY KEY,
    sequence   INTEGER NOT NULL,     -- the latest checkpoint's
    state      TEXT NOT NULL,        -- interrupted | failed | cancelled | resumed | completed
    updated_at TEXT NOT NULL,        -- COALESCE(resumed_at, created_at)
    metadata   TEXT                  -- the latest checkpoint's, for filtering
);
CREATE INDEX IF NOT EXISTS checkpoint_threads_by_state   ON checkpoint_threads (state, updated_at, thread_id);
CREATE INDEX IF NOT EXISTS checkpoint_threads_by_updated ON checkpoint_threads (updated_at, thread_id);

CREATE TRIGGER IF NOT EXISTS checkpoints_head AFTER INSERT ON checkpoints
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
```

Postgres has the same table, a `plpgsql` trigger function `strata_checkpoints_head()` with the same upsert, and a GIN index on `metadata` (`jsonb_path_ops`) for `@>` filters. Every statement runs under the existing DDL advisory lock. The trigger is created with `CREATE OR REPLACE TRIGGER`, which needs Postgres 14 or later. CI and the compose file run 16, and StrataOps runs 17.

**Why a trigger, rather than the writers updating the head row:**

- **Every writer keeps it correct.** That includes both runtimes, a runtime **older than this change** writing to the same database, the CLI's test seeding, and the raw-SQL fixture inserts the layout tests already do. With writer-maintained rows, any of those would silently leave the index stale. Then the store would need a rebuild operation, and every reader would have to wonder whether it was current.
- **No writer changes.** The five `INSERT` statements (two SQLite stores, two Postgres stores, the CLI) are untouched.
- **Same transaction, same lock.** The head row commits atomically with the log row, under the per-thread advisory lock Postgres saves already take. The `WHERE excluded.sequence > …` guard makes it order-safe regardless.

**Why a head table at all, rather than deriving "latest per thread" at query time** (the CLI's current `GROUP BY` join): that query reads every row of the log on every call. It is fine for a local SQLite file. With SG-10 writing about 20 rows per run, it makes the approvals page and the recovery sweep scan the whole history, which grows forever until SG-15. The head table makes a listing cost proportional to the threads it returns.

**Listing** joins the head to the latest log row for the summary's remaining fields:

```sql
SELECT h.thread_id, h.sequence, h.state, h.updated_at,
       c.schema_version, c.status, c.suspended_node, c.next_node, c.metadata, c.nested
FROM checkpoint_threads h
JOIN checkpoints c ON c.thread_id = h.thread_id AND c.sequence = h.sequence
WHERE (h.state IN (…))                                   -- when states are given
  AND EXISTS (SELECT 1 FROM json_each(h.metadata) j      -- SQLite, once per pair;
              WHERE j.key = ? AND j.value = ?)           -- Postgres: h.metadata @> $n::jsonb
  AND h.updated_at > ? AND h.updated_at < ?              -- when given
  AND (h.updated_at, h.thread_id) < (?, ?)               -- the cursor
ORDER BY h.updated_at DESC, h.thread_id DESC
LIMIT ?
```

`WaitingAt` is computed from the joined row's `suspended_node` and `nested` JSON in code, with the same `WaitingAt()` the engine uses. SQLite triggers can't run a recursive query, and a JSON path search would match fields in user state that happen to be called `suspended_node`.

**Migration.** A store opening a database whose `checkpoint_threads` table doesn't exist yet creates it, its indexes and the trigger, and then **backfills** it from the log in the same transaction:

```sql
INSERT INTO checkpoint_threads (…)
SELECT … FROM checkpoints c
JOIN (SELECT thread_id, MAX(sequence) AS s FROM checkpoints GROUP BY thread_id) l
  ON c.thread_id = l.thread_id AND c.sequence = l.s;
```

This runs once per database, like the `nested`-column upgrade. Postgres does it under the DDL lock, so concurrent first opens from either runtime can't double-backfill. SQLite does it in an immediate transaction, and a loser's `CREATE … IF NOT EXISTS` is a no-op. A database that already has the table skips the check entirely.

### Redis: index sorted sets

Redis has no tables to trigger on, so the index is maintained by the writer, atomically with the append, in one Lua script:

| Key | Type | Content |
|---|---|---|
| `{prefix}:checkpoints:{thread_id}` | list | Unchanged: one wire-JSON checkpoint per save. |
| `{prefix}:threads` | sorted set | Every thread. Score = `updated_at` as Unix seconds. |
| `{prefix}:threads:{state}` | sorted set, one per state | The threads whose latest checkpoint is in that state. Same score. |
| `{prefix}:threads:indexed` | string | Present once the keyspace has been indexed (see below). |

```lua
-- KEYS: the list, {prefix}:threads, then the five state sets in a fixed order
-- ARGV: checkpoint JSON, thread id, score, index of this checkpoint's state in KEYS
redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
for i = 3, 7 do redis.call('ZREM', KEYS[i], ARGV[2]) end
redis.call('ZADD', KEYS[2 + tonumber(ARGV[4])], ARGV[3], ARGV[2])
```

The writer computes the state and the score from the checkpoint it is saving, so the script parses nothing. The script text is identical in both engines, and a fixture pins the keys it leaves behind. Every key is passed in `KEYS`, as Redis requires for scripts to be routable. Redis Cluster is still unsupported, though, because the keys of one save hash to different slots.

**Listing:**

1. Take candidates from the requested state sets, or from `{prefix}:threads` when no state is given, with `ZRANGE … BYSCORE REV` from the cursor's score. Several state sets are merged in order on the client.
2. Read each candidate's latest checkpoint with `LINDEX key -1`, pipelined. The summary needs it anyway, so an unfiltered listing costs one round trip for the candidates and one pipelined batch for the page.
3. Apply metadata filters on those checkpoints, on the client.

A metadata-filtered listing reads candidates it then discards, so its cost is proportional to the threads it **examines**, not the ones it returns. One call examines at most 10 × `Limit` candidates and then returns what it has, with a cursor (a short page). That bound is why the listing rules allow short pages. For filtered listings over large keyspaces, use Postgres. That is documented on the adapter, and StrataOps uses Postgres.

**Existing keyspaces.** A store connecting to a prefix without `{prefix}:threads:indexed` indexes it once. It `SCAN`s `{prefix}:checkpoints:*` and indexes each thread from its latest checkpoint, in a second small script that reads `LINDEX -1` itself, so a concurrent save can't be overwritten by an older state. Then it sets the marker. `RebuildIndexAsync` / `rebuild_index` runs the same pass on demand.

**Old writers.** A runtime older than this change `RPUSH`es without indexing, so its writes are invisible to listings until a rebuild. Redis has no trigger to prevent that. So every writer to one Redis prefix must be upgraded together. This is documented in `schema/README.md`'s Redis layout and on the adapters. SQL has no such caveat.

### In-memory stores

Both in-memory stores keep **the whole log** per thread (a list per thread, not a single checkpoint), so they answer `GetLog` like every other adapter. Their listing is a scan over the latest checkpoint of each thread, with the same ordering and cursor. This is a memory change for long-lived in-memory stores: they now grow with every save, like the SQL and Redis logs. Retention is SG-15. The in-memory stores are for tests and development.

---

## The CLI

**Decision: the CLI moves onto `strata-memory`'s query port and drops its own `sqlx` reader.**

- `thread list` uses `list_threads`. `--status` keeps its name and gains `resumed` as a value. It now filters on the derived state, so **a thread that was resumed no longer lists as `interrupted`**. That is a deliberate fix to what the column shows, called out in "Migration". `--meta` maps to the metadata filter. Paging is internal: the CLI follows the cursor to the end.
- `thread inspect` and `checkpoint log` use `get_log`. `checkpoint diff` / `replay` use `get_log(thread, sequence - 1, 1)`.
- `store::open`'s DDL, the `nested`-column upgrade, the reserved-key list and the status mapping are deleted in favour of `strata-memory`'s. The CLI's own copy of the layout disappears. That was the third place the DDL had to be kept in lockstep.
- **It keeps reading logs written by both engines.** It reads them through the same adapter the Rust runtime writes with, against fixtures that both engines produce, and `runtime_written_log.rs` stays.
- `--json` output re-serializes parsed checkpoints instead of echoing column text. The fields are the same. Object key order inside `frozen_state` may differ, because it is no longer the stored text.

**Why move:** a third implementation of the listing semantics (state derivation, cursor, metadata matching) would drift. The shared query fixtures pin the semantics, and they would have to run against the CLI's SQL too. The CLI is a tokio application already, so the async adapters fit it as they are.

**SG-31's read side falls out of it, and is included.** Once the CLI reads through the query port, `--storage` can take a `postgres://` or `redis://` URL as well as a path, and every read-only command works against all three backends. The change is a URL check and two constructors, tested by the CLI job, which already has the CI service containers. SG-31's other half, validating group and subgraph internals, is still out of scope.

---

## Wire format and fixtures

**No checkpoint wire-format change.** There is no `schema_version` bump, and `checkpoints` keeps its columns in every SQL layout. The Redis lists are unchanged. New layout fixtures in `schema/fixtures/checkpoint-log/`:

- `threads-columns.json` / `postgres-threads-columns.json`: the expected `checkpoint_threads` columns.
- `threads.rows.json`: the head rows expected after inserting a given sequence of raw log rows (frozen, resume stamp, failed, cancelled, nested), including an out-of-order insert that the trigger must ignore. Every SQL store in both engines inserts the raw rows with plain SQL, as the existing row fixtures do, so the test proves the **trigger**, not the writer.
- `redis-index.json`: the sorted-set members and scores expected after a given sequence of saves.

A new **query fixture**, `schema/fixtures/checkpoint-query/cases.json`, holds a seed (a list of checkpoints per thread, with fixed timestamps) and a list of queries, each with its expected thread ids in order, states, waiting paths, and page boundaries. It covers states, metadata, time bounds, cursors across ties at one second, short pages, and `GetLog` paging. **Every adapter in both engines, and the CLI, seeds the fixture and must answer every case identically.** This is the cross-language check for the query half. For Redis, a case asserts the union of its pages, because short pages are allowed.

`schema/README.md` gains the head table, the trigger, the Redis index keys and script, and the cursor encoding.

---

## Migration

**.NET** (source-compatible, additive):

- `ICheckpointStore` gains `SaveAsync` / `LoadAsync` with default implementations. Existing custom stores compile and behave as before.
- There are new types: `ICheckpointQuery`, `ThreadQuery`, `ThreadState`, `ThreadSummary`, `ThreadPage`, `CheckpointLogEntry` and `CheckpointLogPage`. The built-in adapters implement `ICheckpointQuery`. `RedisCheckpointStore.RebuildIndexAsync` is new.
- **Behaviour:**
  - The executor no longer blocks a thread on Postgres, Redis or SQLite I/O.
  - `InMemoryCheckpointStore` keeps every checkpoint.
  - Opening a SQL store creates `checkpoint_threads` and its trigger, and backfills them once.
  - A Redis store indexes an unindexed prefix on first connect.
- A class that called an adapter's `Save` / `Load` directly still can. Those calls are now sync-over-async wrappers.

**Rust** (breaking, as PR #13 was):

- **Store implementors:** `CheckpointStore` and `HistoryStore` return `BoxFuture`. Wrap an existing sync implementation by renaming its trait to `SyncCheckpointStore` / `SyncHistoryStore` and passing `BlockingCheckpointStore::new(store)`.
- **Callers:** `run` / `resume` are unchanged. Direct store calls need `.await`. `PostgresCheckpointStore::connect`, `PostgresHistoryStore::connect` and the Redis `connect`s become `async fn`, and must be called inside the runtime that will use the store. `from_client` takes a `tokio_postgres::Client`.
- **Handlers:** `ctx.history()` calls need `.await`, and the `spawn_blocking` workaround from design/17 goes away.
- **Dependencies:** `postgres` is replaced by `tokio-postgres`. `redis` gains `tokio-comp`, `aio` and `script`. `strata-memory` depends on `tokio`.

**CLI:**

- `thread list --status interrupted` no longer lists threads that were resumed. They are `resumed`.
- `sqlx` is dropped.
- `--json` re-serializes `frozen_state`.
- `--storage` also takes `postgres://` and `redis://` URLs.

**Downstream:**

- `strata-demos-rust` needs porting: demos 05, 08, 09 and 12 construct stores or call history directly. `strata-demos-dotnet` keeps compiling.
- StrataOps bumps `strata.lock` in its own PR. `RecoverySweeper`, the approvals list and the incident timeline read from `ICheckpointQuery`, and `ExecutionIndex` keeps only running state, leases and graph version.

---

## Tests

Mirrored per engine, as for every feature so far:

- **Store contract** (`checkpoint_store_contract.rs` / `StoreContractTests.cs`), run against all four adapters:
  - async save and load round-trip, flat, nested and cancelled
  - concurrent saves to one thread from many tasks (Postgres, Redis)
  - `GetLog` paging and an unknown thread
  - in-memory now keeping the log
  - the `BlockingCheckpointStore` adapter (Rust)
  - a sync-only custom store still driving the engine through the default methods (.NET)
- **Query fixture parity:** `cases.json` against every adapter in both engines and the CLI.
- **Layout fixtures:**
  - the head-table columns and `threads.rows.json` from raw inserts, for both SQLite stores and both Postgres stores
  - the backfill of a log created before this change (seeded from the existing row fixtures, then opened)
  - concurrent first opens of an old Postgres log from several connections
  - `redis-index.json`, plus the first-connect indexing of a pre-existing keyspace, with a save racing it
- **Engine:**
  - a store whose `SaveAsync` / `save` is genuinely asynchronous (it completes after a delay) for every checkpoint kind, flat and nested
  - a cancellation arriving while the `cancelled` checkpoint's save is in flight, which must still complete and persist
  - `ObservedStore` timing and `CheckpointSaved` unchanged
  - a store that throws or panics still ending the run's telemetry
- **.NET-specific:** an async `EfCoreCheckpointStore` shared by concurrent runs (the semaphore).
- **Rust-specific:** the Postgres adapter called directly on a current-thread runtime. This is the case that panicked under design/17.
- **CLI:**
  - existing suites pass on the shared adapter
  - `thread list --status resumed`
  - `--storage postgres://…` / `redis://…`, env-gated like the store tests
- **Mutation checks to run:**
  - write the head row from the writer instead of the trigger (the raw-insert fixture fails)
  - pass the run's token to the `cancelled` save (the in-flight cancellation test fails)
  - derive state from `status` alone (the resumed cases in `cases.json` fail)
  - drop the `excluded.sequence >` guard (the out-of-order row case fails)
  - order the cursor by `updated_at` only (the same-second tie case fails)

---

## Review decisions

Reviewed 2026-10-02. The questions below were put to the review, each with a recommendation. Every recommendation was accepted, Q5's after a follow-up on which option makes the engines better:

- **Q6 (CLI):** the CLI moves onto the shared port, and the Postgres/Redis URLs (SG-31's read side) are included in this change.
- **Q3 (Rust `HistoryStore`):** it becomes async in this change.
- **Q5 (Redis):** full listing, not the "`GetLog` only" fallback. A query port that some stores answer only in part makes callers write backend-specific code and find out at runtime. The cost, upgrading every writer to one prefix together, is documented, and `RebuildIndexAsync` / `rebuild_index` repairs an index that a stale writer left behind.

### The questions as asked

1. **.NET: default interface methods on `ICheckpointStore`, or a separate `IAsyncCheckpointStore`?** The recommendation is default methods. A separate interface either makes every call with a built-in adapter ambiguous, or hides behind a runtime type check that still needs the sync interface. The cost is that an async-first store must still declare `Save` / `Load`.
2. **Rust: `BoxFuture`, or `async-trait`?** The recommendation is `BoxFuture`: no new dependency, and it matches `node.rs`. **Keep sync stores** through `BlockingCheckpointStore`? The recommendation is yes: it is about 30 lines, and it is the migration path for every existing `TestStore` and for any blocking client.
3. **Make Rust's `HistoryStore` async in this change?** The recommendation is yes. design/17 deferred it to SG-11, .NET's is already async, and otherwise `strata-memory` carries two Postgres clients. It does widen the Rust break to handler code that uses history.
4. **SQL: a trigger-maintained head table?** The recommendation is yes. The alternatives are writer-maintained rows, which go stale under older writers and raw inserts, and no head table, which makes every listing scan the whole log and gets worse with SG-10.
5. **Redis: full listing with index sets and client-side metadata filtering, bounded per call?** The recommendation is yes. The fallback the handoff allows ("`GetLog` but no filtered listing") saves the Lua script and the index, but leaves Redis users with no listing at all. The cost is that every writer to one prefix must be upgraded together.
6. **CLI: move onto the shared port and take Postgres and Redis URLs (SG-31's read side) in this PR?** The recommendation is to move: a third copy of the semantics would drift. The URL support is the open choice. It is small once the CLI is on the shared port, but it adds a CLI surface to this PR. Without it, the CLI moves and stays SQLite-only, and SG-31 becomes a follow-up of about a day.
7. **Write a `completed` checkpoint when a resumed run completes, so `Resumed` narrows to "running or died"?** The recommendation is not here. It would make fresh and resumed runs behave differently, and SG-10's checkpoint policy is where "what is written when" gets decided once. Until then, `Resumed` is documented as "running, completed or dead".
8. **Store calls never cancelled by the run's token?** The recommendation is yes. It is today's behaviour, and design/17's guarantee that a cancelled run leaves a `cancelled` checkpoint depends on it. A hung store is bounded by the store's own timeouts, not by the run's deadline.

---

## Implementation Status

Built 2026-10-02 in both engines, every adapter and the CLI, in one change, as specified above. These are the differences and details.

**Where it diverged, and why:**

- **The .NET enum is `CheckpointThreadState`, not `ThreadState`.** `System.Threading.ThreadState` comes in through implicit usings in every .NET project, so a caller writing `ThreadState.Interrupted` beside `using Datalium.Strata.Interruption;` would get an ambiguous-reference error (CS0104). The build hit exactly that. Its helpers are `CheckpointThreadStates` (`Of`, `Name`, `Parse`, `All`). Rust keeps `ThreadState`. The values and their stored names are the same in both.
- **Redis needs a parseable timestamp to save.** A save scores the thread by its `updated_at`, so a checkpoint whose `created_at` / `resumed_at` isn't RFC 3339 is refused with a store error. Before, any text was accepted. Both engines always write RFC 3339, so only hand-built checkpoints are affected. Two Rust tests that used `created_at` as a free-form marker were changed to use real timestamps.
- **The CLI's `thread list --json` fields changed.** They are now `thread_id`, `sequence`, `state`, `status`, `suspended_node`, `next_node`, `waiting_at`, `metadata` and `updated_at`. `created_at` and `resumed_at` went away, because the summary doesn't carry them, and `updated_at` replaces them.
  - The table's `Status` column is now `State`.
  - `thread inspect` gains a `Thread state:` line and a `state` field.
  - `--status` refuses a name that isn't a state (exit 2).
  - `--meta` matches any metadata key, the engine's included. Before, it matched caller keys only. That is the query port's filter, as specified, and the CLI test that asserted `--meta error_kind=fatal` matched nothing now asserts it matches the failed thread.
- **`ObservedStore`'s synchronous members throw.** The engine only calls the async ones, and a wrapper that silently blocked would hide a regression.
- **Postgres head-table columns use the `"C"` collation** on `thread_id` and `updated_at`. Without it, Postgres would order thread ids by the database's locale, and the shared query cases need byte order everywhere.
- **The SQL head table and trigger are only created when the table is missing**, not re-created on every open. Re-creating the trigger on every open would lock `checkpoints` against inserts each time a process starts.
- **The SQLite `nested`-column upgrade lost its race handling.** It now runs inside the same immediate transaction as the head-table migration, so two processes can't both see the column missing.
- **Small additions:**
  - `BoxFuture` is public at the crate root (`datalium_strata::BoxFuture`). `node.rs` uses the same alias.
  - `CheckpointStoreError` and `HistoryStoreError` now implement `Display` and `Error`.
  - The Rust Redis connection sets a 5 s response timeout to match StackExchange.Redis; the crate's default is 500 ms.
- **The Rust query types are `#[non_exhaustive]`, with builder methods** (`ThreadQuery::default().with_states(…).with_metadata(…)`). Their metadata filter is a `BTreeMap`. .NET uses an init-only record.

**How it was built:**

- **Rust core:**
  - `store.rs` (new) holds the ports and query types: `CheckpointStore`, `SyncCheckpointStore`, `BlockingCheckpointStore`, `CheckpointQuery`, `ThreadState`, `ThreadQuery`, `ThreadSummary`, `ThreadPage`, the log page types, and `ThreadCursor` (encode, decode and `admits`). `checkpoint.rs` re-exports them.
  - `history.rs` gains `SyncHistoryStore` and `BlockingHistoryStore`.
  - The engine's `save_checkpoint` / `load_checkpoint` await the store directly. The `spawn_blocking` hop is gone, and `CapturingStore` returns ready futures.
  - New dependency: `base64`.
- **`strata-memory`:**
  - `support.rs` (new) holds the blocking-pool hop, the status names, and `page_from_latest`, the in-code listing the in-memory and Redis stores share.
  - The SQLite stores run on `spawn_blocking` behind an `Arc<Mutex<Connection>>`.
  - Postgres moved to `tokio-postgres`, and Redis to `MultiplexedConnection` plus `redis::Script`.
  - The sync `postgres` crate is now a test-only dependency: sandbox setup and raw-row inspection, run on a thread of its own because it panics inside a runtime.
- **.NET core:**
  - `ICheckpointStore` gains the default `SaveAsync` / `LoadAsync`. `ICheckpointQuery.cs` holds the port and its records, `CheckpointThreadStates`, `ThreadCursor` (base64url via `System.Buffers.Text.Base64Url`) and `CheckpointTimestamps`.
  - The executor awaits `SaveAsync` / `LoadAsync` with `CancellationToken.None`. `CancelAt` and `FreezeAroundChild` became `…Async`.
- **.NET storage:**
  - `CheckpointListing` (internal) holds the status names and `PageFromLatest`.
  - `EfCoreCheckpointStore` is async behind a `SemaphoreSlim`. It runs its migration in `BEGIN IMMEDIATE` (Microsoft.Data.Sqlite's non-deferred transaction) and lists over ADO on the context's connection.
  - `PostgresCheckpointStore` is async Npgsql throughout. Its migration runs in the constructor under the DDL lock, as before, through the new `PostgresSchema.DdlLock`.
  - `RedisCheckpointStore` indexes an unindexed prefix in its constructor. It reads each candidate's latest checkpoint and log length in one `MULTI` per chunk, as Rust does, so a summary's sequence matches its checkpoint.
  - In all three, the synchronous members wrap the async ones.
- **CLI:**
  - `store.rs` is now a `Storage` handle over `strata-memory`'s stores: SQLite path, `postgres://` or `redis://` (`?prefix=` for another Redis prefix). It has `threads`, `log`, `checkpoint` and a test-only `insert`. `sqlx` is gone.
  - Layout tests read the SQLite file with `rusqlite`.

**Tests:**

- **New suites, twins in each engine:**
  - `async_store.rs` (4) / `AsyncStoreTests.cs` (6): a store that really suspends (freeze, resume, failure, nested), a cancellation during a boundary save, a run started cancelled, a store that panics or throws. .NET adds a sync-only custom store driving the engine through the default members, and one EF Core store shared by 16 concurrent runs.
  - The shared query cases against all four stores (Rust `checkpoint_store_contract.rs`, .NET `CheckpointQueryContractTests.cs`), plus a log contract and Rust tests of the blocking adapters.
  - Head-table and index tests:
    - Rust: `strata-memory`'s SQLite unit tests, `postgres_stores.rs`, `redis_stores.rs`.
    - .NET: `CheckpointHeadTableTests.cs`.
    - Both cover the column fixtures, the trigger from raw inserts, the one-time backfill (eight concurrent first opens on Postgres), the Redis index fixture, rebuild parity, indexing on first connect, and a thread pushed without the index.
- **CLI:** `storage_backends.rs` runs the same commands on the query seed against SQLite, Postgres and Redis. The layout tests now cover the head table too.
- **Totals when this landed, all with live Postgres and Redis, none skipped:** .NET 356 (+ 1 in the example), `rust/` 327, `cli/` 40, `examples/rust-agent` 1.
- **Mutation checks:**

  | Mutation | Rust | .NET |
  |---|---|---|
  | Race a save against the run's token | 2 fail (both cancellation tests) | 1 fails for the boundary save, 1 for the `cancelled` save |
  | Derive the state from `status` alone | 4 fail (the query cases on every store) | 4 fail (the query cases on every store) |
  | Drop the trigger's `excluded.sequence >` guard | the SQLite trigger fixture fails | the Postgres trigger fixture fails |
  | Order the cursor on `updated_at` only | 2 fail (the same-second tie in the in-memory and Redis stores) | 2 fail (the same-second tie in the in-memory and Redis stores) |
  | Let the EF Core store's semaphore admit 64 callers | n/a | the concurrent-runs test fails |

  The SQL stores order and page inside the database, so their tie handling is pinned by the same query cases rather than by this mutation.

## Not built (deliberately out of scope)

- ~~**Per-node checkpoints, a `running` status and crash-resume** (SG-10).~~ Built 2026-10-10 (`33-Durable-Execution.md`): `ThreadState::Running` / `CheckpointThreadState.Running`, and `Completed` is now written, for a graph compiled with `CheckpointPolicy.EveryNode`. For any other graph the query still can't see running threads (see "What it can answer before SG-10").
- **Leases** (SG-12) and **graph identity** (SG-13).
- **Deltas and batching** (SG-15). ~~Retention and TTL.~~ Built 2026-10-10 (`34-Storage-Hardening.md`): `PurgeAsync` / `purge` on every store, and an optional ttl for finished threads in Redis. Until an application calls it, every log, the in-memory ones included, still grows.
- **Rust connection pooling** for Postgres: still one connection per store, as before. **Redis Cluster**, which needs the index keys to share a hash slot.
- ~~**Reconnect and retry policies** (SG-30).~~ Built 2026-10-10 (`34-Storage-Hardening.md`).
- **A query over a thread's nested children as threads of their own.** A subgraph's child is not a thread (design/15). It appears only inside its parent's checkpoints.
