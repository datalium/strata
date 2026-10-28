# Datalium Strata: Checkpoint Efficiency

Every checkpoint a store writes is the whole checkpoint, and every save is a transaction of its own. With a checkpoint per node (`design/33-Durable-Execution.md`) that costs twice over:

- **Writes don't scale with writers.** A Postgres save is a transaction (the thread's advisory lock, the lease token when fenced, the conditional insert, the head-table trigger, the commit), about three round trips; a SQLite save is a transaction and an fsync. Measured (`benchmarks/RESULTS.md`, 1 KiB checkpoints): SQLite about 390 saves a second whatever the number of writers, Postgres 800 from one writer and 6,200 from 16 in .NET, and **590 from 16 in Rust, whose Postgres store has one connection** (`Mutex<Client>`, a "not built" of `design/38`). StrataOps' M6 storm is 1,000 incidents a second × about 20 nodes: 20,000 saves a second.
- **Bytes grow with state, not with change.** A node that changes one field of a 64 KiB state writes 64 KiB, and twenty nodes write it twenty times. A value that never changes (the evidence an investigation gathered first) is written again at every node and every interrupt.

StrataOps keeps its state small today, holding evidence by reference, and runs its storm "at the rate the store can sustain, reported honestly". This document closes the gap in the engine's stores: **SG-15**. The SG numbers refer to the gap log of StrataOps (not public yet). SG-15 listed four things; the fourth, retention and expiry of finished threads, was built by SG-30 (`design/34-Storage-Hardening.md`: `PurgeAsync`, the Redis finished-thread ttl, the hosting retention driver). The other three are here:

1. **Group commit**: concurrent saves flushed together, one transaction (or one script call) for many, in the Postgres, SQLite and Redis stores; and a **connection pool for Rust's Postgres stores**.
2. **Delta checkpoints**: a `running` checkpoint stored as the changes to the thread's previous one.
3. **Value offload**: a large value stored once, in a blob store, and referenced.

It lands as **one blueprint and three PRs**, in that order, each with .NET and Rust together (strict parity, StrataOps ADR 0003). The first changes no wire format, so its measured gain is known before the other two change the stored form.

It was written as a blueprint before implementation. "Review decisions" lists the questions for review with the answers the build follows (the first eight were decided before the blueprint was written), and "Implementation Status" at the end records, PR by PR, where the build diverged.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| Where | **In each store adapter**, opt-in through its options. The engine, run results, observers and the Cloud store never see a batch, a delta or a blob reference: a save is acknowledged as before, and a load returns the whole checkpoint as before. |
| Which stores | **Group commit:** Postgres, SQLite, Redis. **Deltas and offload:** the same three. The in-memory store keeps objects and needs none; the Cloud store is Cloud's API and is unchanged. |
| Group commit | **Natural**: a save that finds no flush running starts one at once; saves that arrive while it runs queue and go together in the next. No timer, so nothing waits at low load. Each row keeps its own answer (saved, lost the race, lease lost), one thread's saves keep their order, and a save is acknowledged only after its flush commits. |
| The Rust Postgres pool | **`deadpool-postgres`**, in PR 1. Every Postgres call takes a connection from a pool (16 by default); the pool replaces a closed connection, as the store's own reconnect does today. |
| Deltas | **Top-level keys**: a `running` checkpoint's state at each level is stored as the keys that changed or were added (`set`) and the keys removed (`unset`), against the row just before it in the thread's log. Every other checkpoint (interrupted, failed, cancelled, completed, ended) and every `FullEvery`-th row (16) is whole, so a load reads at most `FullEvery` rows. |
| Offload | **A blob port, `ICheckpointBlobStore` / `BlobStore`**, keyed by thread and content hash: a top-level value whose JSON is at least `Threshold` (64 KiB) is stored there once and referenced. Built in: in the checkpoint store's own database (the default), in memory, on the filesystem. S3 and the like are the application's, through the port. |
| Stored form | **A checkpoint with a new `schema_version`** and an envelope in place of each level's `frozen_state`: `1.8.0` when it has a delta, `1.9.0` when it has blob references. Only a store writes it and only a store reads it. A checkpoint that needs neither is stored exactly as today. An older runtime refuses an encoded row by its version, as it refuses any version it doesn't know. |
| Wire format | **PR 1: none** (a new Redis script, no new keys). **PR 2: checkpoint `1.8.0`**, no database change. **PR 3: checkpoint `1.9.0`**, a `checkpoint_blobs` table (the `checkpoints` component's schema version 2) and a Redis hash per thread. |
| Measuring | **The profile, extended, reported, no target**: more writers (64, 256), group commit on and off, concurrent durable runs, a large state saved whole, as deltas and offloaded, and loading a delta chain. In both engines. |

---

## Usage

```csharp
// PR 1: group commit. Off by default; each option turns its part on.
var store = new PostgresCheckpointStore(dataSource, new PostgresStoreOptions
{
    GroupCommit = new GroupCommitOptions { MaxBatch = 256 },
    // PR 2: running checkpoints stored as changes, a whole one every 16 rows.
    Deltas = new DeltaOptions { FullEvery = 16 },
    // PR 3: values of 64 KiB or more stored once, here in the database's checkpoint_blobs table.
    Offload = new OffloadOptions { Threshold = 64 * 1024 },
});

// Offloaded values elsewhere:
var redis = await RedisCheckpointStore.ConnectAsync(url, new RedisStoreOptions
{
    GroupCommit = new GroupCommitOptions(),
    Offload = new OffloadOptions { Blobs = new FileSystemCheckpointBlobStore("/var/lib/strata/blobs") },
});

// Nothing else changes: the engine saves and loads as before.
var result = await graph.RunAsync(threadId, initial, store);
```

```rust
let store = PostgresCheckpointStore::connect_with(
    url,
    PostgresStoreOptions::new()
        .with_pool_size(32)
        .with_group_commit(GroupCommit::new().with_max_batch(256))
        .with_deltas(Deltas::new().with_full_every(16))
        .with_offload(Offload::new().with_threshold(64 * 1024)),
)
.await?;

let redis = RedisCheckpointStore::connect_with(
    url,
    RedisStoreOptions::new()
        .with_group_commit(GroupCommit::new())
        .with_offload(Offload::new().with_blobs(Arc::new(FileSystemBlobStore::new("/var/lib/strata/blobs")))),
)
.await?;
```

Hosting:

```json
{
  "Strata": {
    "Postgres": {
      "ConnectionStringName": "strata",
      "GroupCommit": { "MaxBatch": 256 },
      "Deltas": { "FullEvery": 16 },
      "Offload": { "Threshold": 65536, "Path": null }
    }
  }
}
```

A section that is present turns its part on; `Offload:Path` puts blobs on the filesystem instead of in the database. The same three sections go under `Strata:Redis` and `Strata:Sqlite`. Rust's `PostgresSettings`, `RedisSettings` and `SqliteSettings` gain `group_commit`, `deltas` and `offload` (each `Option<…>`, the same fields in `snake_case`), and `PostgresSettings` gains `pool_size`.

---

## The API

### .NET (`Datalium.Strata.Storage`)

```csharp
namespace Datalium.Strata.Storage;

/// PR 1. Saves that arrive while a flush runs are flushed together.
public sealed record GroupCommitOptions
{
    public int MaxBatch { get; init; } = 256;              // rows per flush, at least 1
    public int MaxConcurrentFlushes { get; init; } = 1;    // at least 1; the SQLite store always uses 1
}

/// PR 2. Running checkpoints stored as the changes to the row before.
public sealed record DeltaOptions
{
    public int FullEvery { get; init; } = 16;              // at least 2: the longest chain a load reads
    public int CachedThreads { get; init; } = 10_000;      // threads whose latest row the store remembers
}

/// PR 3. Large values stored once and referenced.
public sealed record OffloadOptions
{
    public int Threshold { get; init; } = 64 * 1024;       // bytes of a value's JSON, at least 1
    public ICheckpointBlobStore? Blobs { get; init; }      // null: the checkpoint store's own database
}

/// PR 3. Where offloaded values live. Content-addressed within a thread.
public interface ICheckpointBlobStore
{
    /// Stores content under (threadId, hash). Storing what is already there is a no-op.
    ValueTask PutAsync(string threadId, string hash, ReadOnlyMemory<byte> content, CancellationToken cancellationToken = default);

    /// The content stored under (threadId, hash), or null.
    ValueTask<ReadOnlyMemory<byte>?> GetAsync(string threadId, string hash, CancellationToken cancellationToken = default);

    /// Deletes every blob of threadId. What a purge calls.
    ValueTask DeleteThreadAsync(string threadId, CancellationToken cancellationToken = default);
}

public sealed class InMemoryCheckpointBlobStore : ICheckpointBlobStore;
public sealed class FileSystemCheckpointBlobStore(string root) : ICheckpointBlobStore;
```

- `PostgresStoreOptions` and `RedisStoreOptions` gain `GroupCommit`, `Deltas` and `Offload` (each `null` by default: off).
- `EfCoreCheckpointStore` gains an optional `SqliteStoreOptions` (`GroupCommit`, `Deltas`, `Offload`), the SQLite store's first options record.
- Invalid options are an `ArgumentOutOfRangeException` from the store's constructor.

### Rust (`datalium_strata_memory`)

- `GroupCommit::new()` with `with_max_batch(usize)`, `with_max_concurrent_flushes(usize)`.
- `Deltas::new()` with `with_full_every(u32)`, `with_cached_threads(usize)`.
- `Offload::new()` with `with_threshold(usize)`, `with_blobs(Arc<dyn BlobStore>)`.
- `trait BlobStore: Send + Sync` with `put(thread_id, hash, Vec<u8>)`, `get(thread_id, hash) -> Option<Vec<u8>>`, `delete_thread(thread_id)`, each a `BoxFuture` returning `Result<_, CheckpointStoreError>`; `InMemoryBlobStore`, `FileSystemBlobStore::new(root)`.
- `PostgresStoreOptions::with_pool_size(usize)` (16), `with_group_commit`, `with_deltas`, `with_offload`; `RedisStoreOptions` the last three; `SqliteStoreOptions::new()` with the same three, and `SqliteCheckpointStore::open_with(path, SqliteStoreOptions)`.
- Invalid options are a `CheckpointStoreError` from the constructor.

`BlobStore` lives in `strata-memory`, not `strata-core`: the engine never calls it.

---

## Semantics

### Group commit (PR 1)

A store with `GroupCommit` set keeps a queue of saves. A save of any form (plain `SaveAsync`, conditional `SaveIfLatestAsync`, fenced `SaveUnderLeaseAsync`):

1. **Starts a flush at once** when fewer than `MaxConcurrentFlushes` are running, taking itself and whatever is queued, up to `MaxBatch`. Otherwise it **waits in the queue**; when a flush ends, the next takes up to `MaxBatch` queued saves. There is no timer: a save alone is flushed at once, alone.
2. **Keeps its thread's order.** A save of a thread that has a save in a running flush waits for a later flush. Two saves of one thread in one flush are applied in arrival order.
3. **Is answered when its flush commits**, with the answer it would have had alone: a plain save completes; a conditional one returns `true`, or `false` when something was appended since its expected sequence; a fenced one completes, or throws `LeaseLostException` (Rust `LeasedSave::LeaseLost`) when its token is no longer the thread's latest, writing nothing. One row's refusal doesn't touch the others.

**A flush** is one transaction in Postgres and SQLite, and one script call in Redis:

- **Postgres:** the advisory lock of every thread in the flush, taken in one statement in the order of their lock keys (so flushes and single saves never deadlock), then a conditional insert per row that checks the expected sequence and the lease token in its own `WHERE`, and returns which it was: written, lost the race, lease lost. Sent as one batch (Npgsql `NpgsqlBatch`, `tokio-postgres` pipelining), with the commit: one round trip when nothing needs a second look.
- **SQLite:** one `BEGIN IMMEDIATE` transaction, the rows' inserts, one commit: one fsync for the flush.
- **Redis:** `SAVE_BATCH_SCRIPT` / `SaveBatchScript`, which takes every row's keys and arguments and applies to each, in order, exactly what the save, conditional save or fenced save script would (index sets, finished-thread ttl and all), returning a result per row.

**Failures.** A transient failure is repeated under the store's retry policy (`design/34`), and a flush whose commit may have happened is looked up first, **row by row**: each row's fate is read (for a conditional row, the entry right after its expected sequence; for any other, the thread's latest), in one query for the flush; rows found saved or lost are answered, and only the rest are flushed again. A **non-transient** failure of a flush of more than one row makes each of its rows save alone, so only a row at fault fails.

**Cancellation.** A .NET caller's token withdraws a save that is still queued (`OperationCanceledException`); once its flush has started, the save is answered by the flush. In Rust a flush runs on a task of its own, so dropping a save's future withdraws it while queued and stops nobody else's save after. The engine passes no token to its stores, as before.

**Disposal.** A store disposed (dropped) with saves queued flushes them first.

**What isn't batched:** loads, the log and listings, lease acquisition, renewal and release, purges. Only saves.

### The Rust Postgres pool (PR 1)

`PostgresCheckpointStore` and `PostgresHistoryStore` hold a `deadpool-postgres` pool of `pool_size` connections (16) instead of one client. Each call takes a connection for its attempt and gives it back. The pool's recycling drops a connection whose socket has closed, so the "reconnect before an attempt" of `design/34` becomes the pool's, with the same result: a store built with `connect` / `connect_with` keeps working after its connections are terminated. A store built with `from_client` / `from_client_with` is a pool of that one client, which still can't reconnect and says so. Retries, the fault seam and migrations are unchanged; migration runs on one connection from the pool.

### Encoded checkpoints (PR 2 and PR 3)

A store with `Deltas` or `Offload` set may store a checkpoint **encoded**: the checkpoint's wire JSON, with `schema_version` `1.8.0` (it has a delta) or `1.9.0` (it has blob references), and each level's `frozen_state` replaced by an envelope. The outermost level's:

```json
{
  "checkpoint_version": "1.7.0",
  "delta_of": 41,
  "depth": 3,
  "set": { "step": 12, "findings": ["…"] },
  "unset": ["draft"],
  "blobs": { "evidence": { "hash": "sha256:9f2c…", "bytes": 1048576 } }
}
```

- `checkpoint_version` is the checkpoint's own `schema_version`, which a load restores.
- `delta_of` is the sequence of the row this one is a delta of, always the row just before it; `null` when no level is a delta. `depth` counts the rows back to one with no delta (0 with no `delta_of`).
- A level is **whole** (`"state": {…}`) or a **delta** (`"set"`, `"unset"`, against the same level, by depth, of the row `delta_of`). A nested level's envelope has `state`, or `set` and `unset`, and `blobs`, and none of the first three keys.
- `blobs` (`1.9.0`) names the top-level keys whose value is in the blob store: `hash` is `sha256:` and the lowercase hex SHA-256 of the value's JSON text (UTF-8), `bytes` its length. A key in `blobs` is absent from `state` and `set`.

A checkpoint that needs neither (no level a delta, no value offloaded) is stored exactly as today, at its own version. **Every store decodes every row it reads, whatever its own options**, so a store with deltas off reads a log one with deltas on wrote, and the CLI reads everything. The engine, observers and every caller of `LoadAsync`, `LoadLatestAsync`, `GetLogAsync` see only decoded checkpoints.

**In each store the encoded form uses the existing layout.** SQL: the row's `schema_version` column holds `1.8.0` / `1.9.0`, `frozen_state` holds the outermost envelope and `nested` the nested levels with theirs; every other column (status, nodes, metadata, wait, graph, the head-table trigger's inputs) is the checkpoint's own. Redis: the list entry is the encoded JSON; the index script reads only fields the encoding doesn't touch.

### Deltas (PR 2)

A save is stored as a delta when **all** of these hold:

- The store has `Deltas` set, and the checkpoint is `running`.
- The store knows the thread's latest row, its sequence `n` and each level's state, because it wrote or loaded that row itself and still has it cached (`CachedThreads`, least recently used out).
- The row at `n` is less than `FullEvery − 1` rows from a whole one.

Each level is then a delta if its state and the cached row's same level are JSON objects, and whole otherwise. A key is in `set` when it is new or its value's fingerprint (the SHA-256 of its JSON text) differs from the cached one; in `unset` when it is gone. The cache keeps fingerprints, not values, so it costs about 40 bytes a key.

The row is written as a delta **only if the thread's latest is still `n`**, decided in the same atomic step as the append (under the thread's lock in SQL, in the script in Redis). Otherwise (another writer appended, an earlier row of the same flush was refused) the store writes it whole instead, in the same flush. So every chain is contiguous, and correctness never depends on the cache: a stale or missing cache only makes a row whole.

**A load** reads the latest row and, if it is a delta, the `depth` rows before it (one range read), and applies them in order. Decoded state keeps the base's key order, with changed keys in place, added keys after and removed keys gone. Fate lookups (`design/34`, "A save that may have happened") compare decoded checkpoints.

### Offload (PR 3)

A save with `Offload` set moves each top-level value of each level whose JSON text is at least `Threshold` bytes into the blob store, under the thread and the value's hash, and lists it in the level's `blobs`. In a delta row only a changed value is offloaded; an unchanged one, offloaded or not, is inherited from the row before. The store remembers which hashes it has stored for a thread and doesn't send one twice.

- **The checkpoint store's own database** (`Blobs` null): a `checkpoint_blobs` table in SQL and a `{prefix}:blobs:{thread}` hash in Redis. A blob is written in the same transaction as its row (SQL) or just before the save script (Redis, `HSETNX`).
- **Any other blob store**: written before the row is saved. A save that then fails can leave a blob nobody references; it goes when the thread is purged.

**A load** fetches each blob the decoded state needs, once per hash, and checks its hash: a missing blob or one whose content doesn't match fails the load with `checkpoint '<thread>' #<sequence> refers to blob <hash> at '<key>', which the blob store doesn't have` (or `…whose content doesn't match`).

**Retention.** `PurgeAsync` deletes a purged thread's blobs: in the same transaction (SQL) or script (Redis) when they are in the database, and through `DeleteThreadAsync` after the thread's rows when they are elsewhere (a failure there is the purge's failure; the rows are already gone and the blobs are orphans until `DeleteThreadAsync` is called again for that id). The Redis finished-thread ttl expires a thread's blob hash with its list.

---

## Hosting

**.NET.** `StrataPostgresOptions`, `StrataRedisOptions` and `StrataSqliteOptions` gain `GroupCommit` (`MaxBatch`, `MaxConcurrentFlushes`), `Deltas` (`FullEvery`, `CachedThreads`) and `Offload` (`Threshold`, `Path`), bound from `Strata:<Store>:GroupCommit`, `:Deltas`, `:Offload`; a section that is present turns its part on. `Offload:Path` makes a `FileSystemCheckpointBlobStore`; an application with its own registers an `ICheckpointBlobStore`, which the store registrations use when there is one. Start-up validates the options with the store's.

**Rust.** `PostgresSettings.pool_size` and `group_commit`, `deltas`, `offload` (`Option<GroupCommitSettings>`, `Option<DeltaSettings>`, `Option<OffloadSettings>` with `path`) on `PostgresSettings`, `RedisSettings` and `SqliteSettings`; `open()` applies them.

Each part lands in hosting with its PR.

---

## Telemetry

| | |
|---|---|
| **`strata.store.flush.size`** | Histogram, `{checkpoint}`: rows per flush, by `strata.store` (`postgres`, `sqlite`, `redis`). PR 1. |
| **`strata.store.flush.wait`** | Histogram, `s`: from a save's arrival to the start of its flush. Next to nothing when the store isn't busy. PR 1. |
| **`strata.store.checkpoint.size`** | Histogram, `By`: the stored JSON of each saved checkpoint, by `strata.store` and `strata.store.encoding` (`plain`, `delta`, `offloaded`, `delta_offloaded`). PR 1 records `plain`; the others come with their PRs. |
| **`strata.store.blobs`** | Counter, `{blob}`, and **`strata.store.blob.size`**, histogram, `By`: values offloaded and their size, by `strata.store`. Blobs a store already had aren't counted. PR 3. |

.NET: the `Datalium.Strata.Storage` meter, beside `strata.store.retries`. Rust: the `metrics` facade, names beside the retry metric's. A flush repeated after a transient failure counts as one retry of operation `save`.

---

## Wire format

- **PR 1: none.** No checkpoint, definition or database change. Redis gains `SAVE_BATCH_SCRIPT` / `SaveBatchScript` (the same text in both engines); its per-row effects are the three save scripts', so every key it touches already exists.
- **PR 2: checkpoint `1.8.0`**, the encoded form above with deltas, a superset of `1.7.0`. `schema/checkpoint.schema.json` describes the envelope. No database change: the SQL layout and the Redis keys are as they were. New fixture **`checkpoint-log/delta.rows.json`**: a thread's stored rows (a plain one; a delta changing and adding keys; one removing a key, with a nested level that is a delta and one that is whole; a row whose base is itself a delta) and the checkpoint each decodes to. Every adapter's tests in both engines store the rows as they are and load them, and the CLI's tests read them.
- **PR 3: checkpoint `1.9.0`**, `1.8.0` with `blobs`. The **`checkpoints` component's schema version 2**, the first step after SG-30's baseline, adds:

  ```sql
  CREATE TABLE IF NOT EXISTS checkpoint_blobs (
      thread_id TEXT NOT NULL,
      hash      TEXT NOT NULL,          -- 'sha256:<hex>'
      content   BYTEA NOT NULL,         -- BLOB in SQLite: the value's JSON text, UTF-8
      PRIMARY KEY (thread_id, hash)
  )
  ```

  Redis: `{prefix}:blobs:{thread}`, a hash from hash to content. The save scripts, the batch script and `PURGE_SCRIPT` take the thread's blob key (to expire, keep and delete it with the thread's list), so every Redis writer to a prefix must be current. The filesystem store: `<root>/<thread id as lowercase hex of its UTF-8>/<hex of the hash>`. New fixtures **`checkpoint-log/offloaded.rows.json`** (rows with blobs, the blobs, the decoded checkpoints), **`blobs-columns.json`**, **`postgres-blobs-columns.json`**; `schema-versions.json` moves `checkpoints` to 2.

All new fixtures are listed in `schema/README.md`.

---

## Migration

**Additive and off by default.** A store with none of the three options set writes exactly what it writes today. From PR 2's build on, every store reads encoded rows whatever its options; from PR 3's, it reads blobs from its own database, or from the blob store its options name.

**Order.** The compatibility rule (`schema/README.md`) applies: upgrade every reader of a store, the CLI included, before a writer turns deltas or offload on, because an older reader refuses an encoded row (it doesn't know `1.8.0` or `1.9.0`). Turning them off later leaves the encoded rows in place, and they keep loading. PR 3's schema step moves the `checkpoints` component to 2 when a current store opens a database, so an older runtime then refuses the database (`design/34`): the step runs whether or not offload is on, as every step does.

**Rust:** a Postgres store opens up to `pool_size` connections, where it held one: size Postgres's `max_connections` for it. `from_client` keeps one. `deadpool-postgres` is a new dependency of `datalium-strata-memory`; `sha2` is a new one from PR 2.

**StrataOps:** M6 turns on `Strata:Postgres:GroupCommit` and measures; deltas are worth turning on with `EveryNode`, since every row after the first of a run is a delta; offload isn't needed while evidence is held by reference.

---

## Tests

Mirrored in both engines (`GroupCommitTests`, `DeltaCheckpointTests`, `OffloadTests` / `tests/group_commit.rs`, `tests/deltas.rs`, `tests/offload.rs`), each against every adapter it applies to (SQLite always; Postgres and Redis when `STRATA_TEST_POSTGRES_URL` / `STRATA_TEST_REDIS_URL` are set, and the PRs report runs with both):

**PR 1, group commit.**
- Many concurrent saves of many threads: every one acknowledged, every log whole and in order, fewer flushes than saves (the flush-size metric).
- In one flush: a conditional save that lost its race returns `false`, a fenced save whose lease was taken over is refused, and the rows around each are written; two saves of one thread keep their order; a thread's save waits for its earlier save's flush.
- A save alone is flushed at once (no linger): with a fake clock, nothing waits on time.
- A transient failure before the commit repeats the flush; one after the commit looks each row up (the fault seam's `save:commit`, `save:after-commit`), and nothing is written twice; a non-transient failure of one row's data fails that row only.
- A queued save withdrawn by its token (.NET) or a dropped future (Rust) is not written; one already flushing is answered.
- Disposal flushes what is queued.
- A durable run, a resume's claim, an end and a fenced run work unchanged over a batched store (the existing engine suites rerun against one).
- Rust pool: concurrent calls use several connections; the store keeps working after `pg_terminate_backend` of every connection; `from_client` still reports that it can't reconnect.
- The Redis batch script, row by row, does what the three save scripts do (index sets, ttl, fencing).

**PR 2, deltas.**
- The shared rows fixture loads to its checkpoints in every adapter and the CLI.
- A durable run with deltas on: rows after the first are deltas, every `FullEvery`-th is whole, every non-running row is whole; loading at every point equals the checkpoint that was saved.
- A store with deltas off reads a log written with them on.
- A row whose base moved (another writer appended; an earlier row of the same flush refused) is written whole.
- A cold cache (a new store instance, an evicted thread) writes whole, then deltas after a load.
- Nested levels: a delta level, a whole level beside it, a level that appears or disappears.
- Fate lookups after an ambiguous delta save.

**PR 3, offload.**
- Values over the threshold go to the blob store once per thread; unchanged ones aren't sent again; a load restores them.
- Each blob store: own database, in memory, filesystem.
- A missing blob and a tampered one fail the load with their messages.
- Purge removes the thread's blobs (all three); the Redis ttl expires the blob hash with the list.
- Schema version 2: a version-1 database is migrated, the columns match the fixtures, a version-2 database refuses an older build.
- The CLI reads offloaded rows from the database, and from a filesystem store with `--blobs <dir>`.

**Mutation checks**, both engines, reported in each PR: a flush that applies a row's refusal to every row; one that reorders a thread's saves; a delta written without checking that its base is still the latest; a decoder that ignores `unset`; a load that skips the blob hash check.

---

## Measuring

The profile (`design/37`) gains, in both engines, under the names both use:

| Scenario | What | Metrics |
|---|---|---|
| `store_save` | Writers 1, 16, **64, 256**, each store, and **`…_batched`** with group commit on (Postgres, SQLite, Redis) | `ops_per_sec`, save latency percentiles |
| `durable_throughput` | 64 concurrent ten-node durable runs on each store, group commit off and on | `runs_per_sec`, `saves_per_sec` |
| `store_save_large` | The `Work` state with a 64 KiB `payload` that never changes and a `step` that does, saved as a chain: `whole`, `delta`, `offload`, `delta_offload` | `ops_per_sec`, `stored_bytes` per save |
| `store_load_chain` | Loading a thread whose latest row is 15 deltas from a whole one, and one whose state has an offloaded value | load latency percentiles |

PR 1 adds the first two, PR 2 and PR 3 the rest. There is no target: `benchmarks/RESULTS.md` is replaced by hand after each PR from one named machine, and the PR reports what changed.

---

## Review decisions

Decided by Nick before the blueprint (2026-10-15): 1 to 8. The rest are this document's; the build follows each recommendation.

1. **Scope?** **All three: group commit, delta checkpoints, value offload.** Retention was SG-30's.
2. **The Rust Postgres pool?** **Inside SG-15, with `deadpool-postgres`**, a new dependency of the published `datalium-strata-memory`. Batching on one connection would still cap Rust far below .NET.
3. **Where batching lives?** **In each store adapter, opt-in through its options; natural group commit**, not an engine-level coalescer.
4. **Which stores batch?** **Postgres, SQLite and Redis.**
5. **Measuring?** **Extend the profile and report; no target.**
6. **How it ships?** **One blueprint, three PRs**: group commit and the pool, then deltas, then offload.
7. **Where offloaded values go?** **A blob port with built-in stores**: the checkpoint store's own database, in memory, the filesystem. Cloud object stores through the port.
8. **How deltas work?** **A top-level JSON diff against the previous row, with a whole row periodically and for every checkpoint that isn't `running`.**
9. **Encoding in the store, not the engine?** **The store.** A delta's base is a sequence in the log, which only the store knows (a plain save doesn't return one); the engine, observers, run results and the Cloud store stay as they are; and every reader of a store, the CLI included, decodes in one place.
10. **The stored form: new columns, or a new `schema_version` with an envelope?** **The envelope.** PR 2 then needs no database change, every store keeps one layout, and an older runtime refuses an encoded row by the version check it already has, in SQL and in Redis alike (Redis has no schema table to refuse with).
11. **Which checkpoints are deltas?** **Only `running` ones.** A waiting, failed or finished thread is loaded, listed and inspected far more than it is written, and its row should stand alone; durable runs, where the volume is, write `running` rows.
12. **Contiguous chains?** **Yes: a delta's base is always the row just before it, checked when it is appended; otherwise it is written whole.** A load is then one range read of at most `FullEvery` rows, and a cache that is stale can only make a row bigger, never wrong.
13. **Fingerprints?** **SHA-256 of each top-level value's JSON**, cached per thread (`sha2`, a new Rust dependency; the BCL has it). A 64-bit hash's collision would drop a change silently; comparing cached values would keep every state in memory twice.
14. **Blobs per thread or shared?** **Per thread** (keyed thread and hash). Purge deletes a thread's blobs with it and nothing counts references. Repeats are within a thread (a value carried node to node), so sharing across threads buys little.
15. **How fine a delta or an offload?** **Top-level keys.** It is where states change (a record's fields), it costs a lookup per key, and it is the same granularity for both, so an offloaded value is inherited by a delta row like any other.
16. **A linger time for group commit?** **None.** A save alone goes at once; batches form only when saves arrive faster than flushes finish, which is when they help.
17. **One bad row in a flush?** **The flush's rows are saved alone, and only that row fails.** One run's malformed state must not fail other tenants' saves.
18. **`MaxConcurrentFlushes`?** **1 by default.** One flush in flight is classic group commit; a second can help Postgres with many writers, and the measurements decide whether the default changes.
19. **Group commit on by default?** **No**, as the brief proposed: opt-in, and revisited with the numbers.
20. **The pool's default size?** **16**, the profile's largest writer count before this document. Npgsql's default (100) is for an application's whole database traffic, not one store's.
21. **Redis batching?** **One batch script per flush.** The multiplexers already pipeline concurrent saves (34,000 a second from 16 writers), so the gain is the script calls saved; PR 1 measures it, and leaves the batch script out, saying so, if it gains nothing.
22. **Blobs in another store, written before the row?** **Yes.** A reference must never point at nothing; an orphan blob costs only space, and goes with its thread.

---

## Implementation Status

Each PR records its part here: what it built, where it diverged and why, its tests, mutation checks and measurements.

- **PR 1** (group commit, the Rust pool, the first profile scenarios): built 2026-10-15, below.
- **PR 2** (deltas, `1.8.0`): built 2026-10-15, below.
- **PR 3** (offload, `1.9.0`, schema version 2): built 2026-10-15, below. It also closed the gap PR 2 left open (the base row checked by its sequence only), as Nick asked.

### PR 1: group commit and the Rust Postgres pool

Built 2026-10-15 in both engines and both hosting packages, as specified above except where listed. Decisions 1 to 8 were Nick's; of this document's own, 18, 19 and 21 changed with the measurements.

**Where it diverged, and why:**

- **Redis has no group commit** (decision 21, as it provided). It was built in .NET: a `SAVE_BATCH_SCRIPT` that applied each row as the three save scripts do, and wrote its results to a short-lived marker key (`{prefix}:batches:{id}`, 10 minutes) so a flush whose reply was lost could be looked up instead of run again. Measured against the store without it (1 KiB checkpoints, the probe's own threads): 64 writers 43,068 saves a second without, 37,027 to 49,661 with (4 to 16 flushes at once); 256 writers 68,042 without, 42,614 to 75,667 with; 16 writers 20,763 to 22,643 without, 11,414 to 20,278 with. The StackExchange.Redis multiplexer and the `redis` crate's connection already pipeline concurrent saves, so a batch saves round trips that are already shared. Not built in Rust, and taken out of .NET before review; nothing of it ships. One finding came with it: on this machine a single Redis command of 32 to 64 KB takes 44 ms however it is sent (a plain `SET` included), and 1 KB or 128 KB commands don't: Docker Desktop's port forwarding and TCP's delayed acknowledgement. Batches above that size are slowed by it, and so would a checkpoint of that size be.
- **`MaxConcurrentFlushes` defaults to 4, not 1** (decision 18). With one flush at a time, saves from a fixed set of writers arrive in alternating batches (the writers answered by one flush come back while the next, holding the one late writer, runs), and Postgres with 16 writers was slower than without group commit (4,139 saves a second against 5,765). Measured in .NET, 16 and 64 writers, 1, 2, 4 and 8 flushes: 4 was the best or level at both (6,599 and 17,297 against 5,765 and 7,434 without). SQLite keeps one: it has one connection.
- **A Postgres flush is one statement per wave, not per row.** The first build sent one conditional insert per row in a batch; Postgres planned each, and a flush of 8 to 15 rows took 5 ms. Now the rows of distinct threads go in one statement (`FlushWaveSql` / `FLUSH_WAVE_SQL`) as one array per column through `unnest`, each row's verdict decided in SQL, and a thread's second save goes in the next wave. The lock statement and the waves are sent together (Npgsql `NpgsqlBatch`, prepared; `tokio-postgres` pipelining with prepared statements, which the pool caches per connection). A row's verdict comes back with the sequence it got, so a flush whose commit may have happened is looked up at exactly those sequences, which also settles two plain saves of one thread in one flush (the single save's "look at the latest" can't).
- **The Rust plain save prepares its statements once per connection too** (`prepare_cached`): `tokio-postgres` otherwise prepares a statement given as text every time, a round trip each. One writer went from 551 saves a second to 1,010. The .NET store's plain save is unchanged.
- **A flush's lookup is forgotten only once it has answered.** The first Rust build took the in-doubt verdicts before looking them up; a lookup that failed would then have flushed again blind. Found reading the code, before a test.
- **Hosting has an `Enabled` switch** (`Strata:Postgres:GroupCommit:Enabled`, Rust `group_commit.enabled`) instead of "a section that is present": .NET's configuration binder doesn't create an empty section's object, so presence can't be read reliably, and Rust's settings follow for parity. `Strata:Redis:GroupCommit` doesn't exist.
- **The SQLite store's options are new in both engines:** .NET `SqliteStoreOptions` as `EfCoreCheckpointStore`'s optional fourth constructor argument (and `CreateInMemory`'s second), Rust `SqliteCheckpointStore::open_with` / `open_in_memory_with`.
- **Disposal drains in .NET only.** A Rust store can't be dropped while a save borrows it, and a flush that has started runs on a task of its own to the end; a save whose future was dropped before its flush is skipped.
- **Test seams:** the existing Postgres fault points also fire in a flush (`save:before-commit`, `save:commit`, `save:after-commit`, `save:recheck`); the SQLite stores gained `flush:start` (.NET `internal`, Rust `#[doc(hidden)] with_fault`). A Rust test that holds a flush in a runtime worker blocks through `block_in_place`: a plain blocking wait there stopped tokio's timer with the worker.
- **The profile** measures 64 and 256 writers after the single durable run, so the checkpoints they leave in the in-memory store don't weigh on it, and with at least 32 saves per writer, so those cases last long enough to measure (8 a writer gave 7,500 to 17,700 for the same .NET build).

**Measured** (`benchmarks/RESULTS.md`, 2026-10-15; 1 KiB checkpoints, defaults):

| | Rust without | Rust with | .NET without | .NET with |
|---|---:|---:|---:|---:|
| Postgres, 64 writers, saves/s | 7,097 | 17,882 | 7,092 | 12,619 |
| Postgres, 256 writers, saves/s | 7,039 | 35,978 | 3,543 | 17,090 |
| Postgres, 64 concurrent durable runs, saves/s | 6,952 | 19,527 | 8,860 | 15,438 |
| SQLite, 64 writers, saves/s | 415 | 6,433 | 410 | 6,362 |
| SQLite, 64 concurrent durable runs, saves/s | 431 | 5,819 | 424 | 7,352 |
| Postgres, 1 writer, saves/s | 1,010 | 773 | 868 | 733 |

The Rust Postgres store without group commit went from 593 saves a second with 16 writers (one connection) to 6,849 (the pool). The .NET figures with group commit vary from run to run (six runs: 5,900 to 16,200 with 64 writers, 5,100 to 20,800 with 256), the Rust ones much less (four runs: 17,900 to 21,600 and 26,400 to 36,000); the cause isn't known. StrataOps' storm needs about 20,000 checkpoints a second; 64 concurrent durable runs reach 19,500 in Rust and 15,400 in .NET on this machine, against one local Postgres in Docker.

**Tests.** .NET: `Backends/GroupCommitTests.cs` (21: the queue on its own, then SQLite and Postgres: each row of one flush answered on its own, concurrent writers, the store contracts and a durable run with group commit, a flush in doubt looked up row by row, a failure before the commit repeated, the row Postgres refuses (a NUL in JSONB) failing alone, disposal flushing what is queued), one in `Hosting/HostingTests.cs`. Rust: `src/group_commit.rs` (8, the queue), `tests/group_commit.rs` (9, the same store cases, and the pool: several connections up to its size, a size of 0 refused), group-commit runs of the contract suite in `tests/checkpoint_store_contract.rs` (2) and of the durable run in `tests/durable_execution.rs` (2), one in `strata-host/tests/settings.rs`. The SG-30 reconnect tests now run through the pool unchanged. Verified locally with Postgres and Redis running and `STRATA_REQUIRE_BACKENDS=1`: `dotnet test` 902 passed and 2 skipped (the opt-in Cloud live tests), plus the example's 1; `cargo test --all-features` 838 in `rust/`, 80 in `cli/`, 1 in `examples/rust-agent/`; `dotnet format --verify-no-changes`, `cargo fmt --check` and `cargo +1.99.0 clippy -D warnings` clean.

**Mutation checks** (each broken, the group-commit suites run, then restored), every one caught in both engines:

- A row's refusal applied to every row of its flush: 2 tests in each engine (the held flush's rows, the flush in doubt).
- A thread's later save not held back while its earlier save's flush runs: 1 in each (the queue's order test).
- A flush whose commit may have happened flushed again without looking: 1 in each (the flush in doubt).
- The SQLite flush ignoring a conditional save's expected sequence: 2 in .NET (the held flush, the contracts), 1 in Rust (the contracts).

`cargo +1.99.0 package --workspace --exclude strata-bench` passes. With the local default toolchain (1.91.1) the same command fails verifying `datalium-strata-cloud` against a `datalium-strata` without SG-25's `NotAdmitted`: that cargo resolves an older copy of the engine while verifying, and nothing here touches either crate.

### PR 2: delta checkpoints

Built 2026-10-15 in both engines and both hosting packages, with the CLI reading the result, as specified above except where listed. Decisions 8 to 13 and 15 held as written.

**Where it diverged, and why:**

- **A thread with two saves in one flush has both stored whole**, not only "a row whose base moved". A Postgres flush sends all its waves before it reads a verdict, so when a thread's second save is encoded, the row before it isn't known. And a second save found to be a delta of the wrong row would have to be written after the waves, out of its thread's order, with its thread's later rows already judged against the wrong latest. One thread's saves share a flush only when an application saves the thread concurrently, which the engine never does. The SQLite flush follows the same rule, for parity, though it decides row by row.
- **A delta whose base moved is written whole by a second step, not by sending both forms.**
  - Postgres, a single save: the insert carries the base (`AND latest.sequence = base`, `RETURNING sequence`). When it inserts nothing, the whole checkpoint goes in by the same statement, in the same transaction, under the same lock.
  - Postgres, a flush: the wave statement gains a `base` column and a fourth verdict, `rebase`. The rebased rows go whole in one more wave statement, in the same transaction, before the commit.
  - SQLite: the insert's own `WHERE`, then whole. Its flush checks each row inside its immediate transaction.
  - Redis: each save script takes the base last and returns `-2` without writing; the store then sends the whole checkpoint.
  - Sending both forms in one call would have doubled what every delta save sends (and made Postgres parse both), to save a round trip only when another writer had come between.
- **The Redis save scripts changed.** Each takes one more argument (`ARGV[7]`, `ARGV[8]`, `ARGV[9]`). The fenced script now returns the list's length when it saves, where it returned 1, because the cache needs the sequence a fenced save got. The text is identical in both engines. A runtime from before sends its own scripts, so mixed writers on one prefix stay safe; such a runtime can't read a delta row, as with any new version.
- **Fingerprints are of canonical JSON**, object keys sorted at every depth. A `JSONB` column returns keys in its own order, so a fingerprint of the stored text would make every object-valued key look changed after a load. A value written differently (a number's form) still prints differently; that only makes a delta bigger.
- **What the cache learns, and when it forgets.** An entry is a row's sequence, its depth, and each level's fingerprints.
  - A save that wrote a row replaces the thread's entry.
  - A load (`load` and `load_latest`, not the log) replaces it only if the entry isn't newer, since a save may finish while the load runs.
  - A lost conditional save, a save looked up after a failure in doubt, and the store's own purge forget the thread.
- **Correctness rests on the base's sequence, which leaves one gap.** The append checks that the thread's latest sequence is still the one the delta is of. Suppose a thread id is purged (or, in Redis, expires with the finished-thread ttl) and then run again by **another process**, until it reaches the sequence this store still remembers. That check would then pass against a row this store never saw. Closing the gap means checking the base row's content as well (its `created_at`, or a hash), which the Redis scripts can't do cheaply. It is left open and raised for review. **PR 3 closed it** (see its section): the base is checked by its sequence and a token.
- **How a store reads a chain.**
  - SQL: a load is one statement: a CTE picks the target row (the latest, or one sequence for a lookup) and its depth (`frozen_state->>'depth'` in Postgres, `json_extract` in SQLite), then reads the rows from `sequence − depth` to it.
  - Redis: `LINDEX` and `LLEN` together, then `LRANGE` of the chain by absolute position. The list is append-only, so the chain doesn't move between the two reads.
  - The log: a page whose first row is a delta reads the rows before it in one more range read.
  - A listing reads no state, and restores only the version (`frozen_state->>'checkpoint_version'`, `json_extract`, or the Redis element's).
  - A decoder checks that each delta's `delta_of` is the row before it, and otherwise fails the read: `checkpoint '<thread>' #<n> is a delta of #<m>, which wasn't read with it`.
- **The Redis store now records `strata.store.checkpoint.size`**, `plain` and `delta`; part 1 recorded it only in Postgres and SQLite.
- **The .NET SQLite store's three single saves are now one insert**, the Rust store's: conditional on the expected sequence, the lease token and the base, `RETURNING sequence`. Before, they were three EF Core statements.
- **`CachedThreads` must be at least 1.** The design left the minimum open.
- **Hosting has an `Enabled` switch**, as group commit does and for the same reason, and Redis has one too: `Strata:Redis:Deltas`, Rust `redis.deltas`. Rust's `RedisSettings` gained a public field, so a struct literal of it needs `..RedisSettings::default()`.
- **The fixture holds checkpoints, not columns.** `delta.rows.json` gives each stored row as checkpoint JSON with its sequence, and what it decodes to. Each adapter's tests write the columns, or the Redis element, from it, as a writer would.
- **The profile runs the two scenarios as one group, `--only large_state`.** Rust reads `stored_bytes` through a global `metrics` recorder that records nothing until that group switches it on. `metrics` is a new dependency of the unpublished `strata-bench`.

**Measured** (`benchmarks/RESULTS.md`, "A large state, whole and as deltas"; two runs each). The state has a 64 KiB payload that never changes, saved as a durable run would, twenty rows to a thread:

| | Rust whole | Rust delta | .NET whole | .NET delta |
|---|---:|---:|---:|---:|
| Bytes stored per save (mean of a thread's 20) | 65,579 | 6,650 | 65,579 | 6,650 |
| SQLite, saves/s | 370–385 | 434–443 | 344–353 | 400–419 |
| Postgres, saves/s | 22 | 174–178 | 22 | 169–173 |
| Redis, saves/s | 22 | 205–206 | 23 | 184–192 |
| Load P50, SQLite (whole, 15 deltas) | 77–204 µs | 145–180 µs | 294 µs | 327–340 µs |
| Load P50, Postgres | 521–541 µs | 659–669 µs | 821–835 µs | 0.90–1.01 ms |
| Load P50, Redis | 279–288 µs | 583–588 µs | 372–384 µs | 698–721 µs |

- A delta row is about 100 bytes, against 65.6 KB whole. The mean of 6.6 KB is the thread's two whole rows: its first, which the store doesn't know yet, and its sixteenth. Both engines store the same bytes.
- The Postgres and Redis whole saves take about 45 ms each. That is the large-command latency of Docker Desktop's port forwarding that part 1 found with Redis, which Postgres shows too. The delta rates are then mostly those two whole rows in twenty. It was reported, not chased.
- A chain load costs a second read in Redis, so about twice a whole load. In SQL it is one statement either way.

**Tests.**

- .NET:
  - `Backends/DeltaCheckpointTests.cs` (26):
    - the shared rows;
    - a chain with every fourth row and the interrupted row whole, also read by a store with deltas off;
    - a moved base under plain, conditional and fenced saves;
    - a cold cache, a load warming it, and a cache of one thread;
    - nested levels appearing, changing and going;
    - a delta save in doubt (Postgres, Redis);
    - group commit (SQLite, Postgres);
    - an `EveryNode` run against the in-memory store's;
    - the store contracts and the durable run with deltas, and with group commit too;
    - the options.
  - One test in `Hosting/HostingTests.cs`.
- Rust:
  - `src/deltas.rs` (6): the codec, the cache, and a decode refused without its base.
  - `tests/deltas.rs` (23): the same store cases, and every constructor refusing bad options.
  - Runs with deltas on of the contract suite in `tests/checkpoint_store_contract.rs` (3) and of the durable run in `tests/durable_execution.rs` (3).
  - One test in `strata-host/tests/settings.rs`.
- The CLI replays, diffs and logs the shared rows (1, `cli/tests/checkpoint_log_layout.rs`).
- Verified locally with Postgres and Redis running and `STRATA_REQUIRE_BACKENDS=1`:
  - `dotnet test`: 929 passed and 2 skipped (the opt-in Cloud live tests), plus the example's 1.
  - `cargo test --all-features`: 874 in `rust/`, 81 in `cli/`, 1 in `examples/rust-agent/`.
  - `dotnet format --verify-no-changes`, `cargo fmt --check` and `cargo +1.99.0 clippy -D warnings` are clean.
  - `cargo +1.99.0 package --workspace --exclude strata-bench` passes in a fresh target directory. In the shared one it failed, verifying `datalium-strata-host` against a `datalium-strata-memory` without deltas. Cargo treats a registry crate's build as unchanging, so it reused the build of the same version left by part 1's run. That is also the stale engine copy part 1 saw with the 1.91 toolchain. CI builds from nothing, so it isn't affected; locally, give the check a target directory of its own (`CARGO_TARGET_DIR`).
- One unrelated test failed once in a filtered run: `StorageHardeningTests.ASaveThatFailedBeforeItsCommitIsRepeatedAndWritesOnce` counted 3 retries for 2. Its retry listener is global, and saw another class's Postgres retries. It passes alone and in the full run.

**Mutation checks** (each check broken, the delta suites run, then restored). Every one was caught in both engines:

- A delta appended without checking that its base is still the latest, store by store:
  - Postgres: 2 tests in each engine (the moved base, the flush).
  - SQLite: 2 in each (the same).
  - Redis: 1 in each (the moved base).
- A decoder that ignores `unset`: 6 tests in each engine (the shared rows and the nested levels, in all three stores), plus the codec's chain test in Rust.

### PR 3: value offload

Built 2026-10-15 in both engines, both hosting packages and the CLI, as specified above except where listed. Decisions 7, 14, 15 and 22 held. Nick decided at the start that this PR also closes the gap PR 2 left open.

**Where it diverged, and why:**

- **The blob's hash is the value's fingerprint.** Both are the SHA-256 of the value's canonical JSON: keys sorted at every depth, compact. The blob is that same text, so `hash` names exactly the bytes stored, and a reader checks it by hashing what it read. One hash serves both: a save hashes each top-level value once, whether to compare it with the row before or to name its blob, and a load takes a blob's hash as the value's fingerprint for the cache instead of hashing 64 KiB again. The .NET canonical writer now leaves non-ASCII characters unescaped, as `serde_json` does; fingerprints were never stored, so nothing else changes. What it costs: a decoded offloaded value has its objects' keys sorted, and in a whole level it comes after the level's other keys. State is read by name, and checkpoints compare without regard to key order.
- **The base row is checked by its sequence and a token** (the gap PR 2 left open). A save relies on the row before it when it is a delta of it, or when it leaves out a blob the store remembers the thread has. Such a save is appended only if that row is still the thread's latest by both:
  - SQL: the row at that sequence must have the `created_at` the store remembers. SQLite and Postgres single saves check it with `EXISTS` in the insert; the Postgres flush has a `base_created` column; the SQLite flush reads it in its transaction.
  - Redis: the SHA-1 of the latest element must be the one the store remembers, checked by `redis.sha1hex` in the save scripts. The client hashes the element it sent or read (`sha1_smol`, already in the tree through `redis`, now a direct dependency).
  - Otherwise the save is written whole, every blob with it, as a moved base already was.

  The Redis token is exact. The SQL token is `created_at`, which the engines write to the second, and that is exact enough for a purge. A purge takes only threads whose latest row is older than its cutoff (exclusive, at the second), and a thread run again after it writes later rows. It is not exact for a thread deleted by hand, or by a purge with a cutoff in the future, and run again by another process to the same sequence within the same second. That case is recorded as a known limit. A hash in SQL would need the database to compute it, since Postgres gives `JSONB` back reordered, or a function SQLite doesn't have.
- **A store remembers the blobs of the thread's latest row, not every blob it ever wrote.** After a save that relied on the row before it, that is the blobs its row refers to plus those the row before had for values it still has. After any other save, it is the blobs its row refers to. After a load, it is the blobs the decoded row refers to. The set stays as small as the row. A value that comes back after changing is written again, which costs only bytes.
- **Redis blobs are written by the save scripts, not just before them.** The scripts take the blob hash as a key and the blobs as trailing pairs of hash and content, and `HSETNX` them once every check has passed. So a refused save leaves no blob, and a finished thread's ttl can't expire the hash between the `HSETNX` and the script. Every save script's arguments changed, and every Redis writer to a prefix must be current, as the blueprint already said.
- **Postgres writes a blob in the same statement as its row**: a data-modifying CTE that inserts the blobs only if the row went in, in the single save and in every flush wave (`$21` to `$23`, each blob with its row's position). SQLite writes them in the row's immediate transaction, after the row and only if it went in.
- **With group commit and a blob store of its own, every save of a flush writes all its blobs, before the flush.** A flush decides its rebases inside its transaction, where it can't wait for another store, so no row it writes whole may need a blob it didn't write. A blob store's put of a blob it has is a no-op. Single saves leave out the blobs they remember.
- **Blobs are read from one place**: the blob store the options name, else the store's own database. There is no fallback from one to the other. Rows whose blobs are elsewhere keep loading only while the options name that blob store; a threshold of `int.MaxValue` / `usize::MAX` reads without offloading.
- **When only offload is on, the cache holds 10,000 threads**, `DeltaOptions.CachedThreads`' default. `OffloadOptions` has no size of its own.
- **The filesystem store names a file by the hash's hex digest**, without `sha256:`, which isn't a valid file name everywhere. It refuses a hash that isn't `sha256:` and lowercase hex.
- **Hosting has an `Enabled` switch**, as group commit and deltas do. `Path` makes a filesystem blob store. A registered `ICheckpointBlobStore` is used when there is one. A registered one and a `Path` together are refused rather than one silently winning. Rust has `path` only: a Rust application gives another blob store in code, with the store's options.
- **`strata.store.blobs` counts the blobs a save wrote**: those it didn't remember the thread having. A whole row written after a moved base counts its blobs again, though the database may have had them.
- **The CLI takes `--blobs` on `thread inspect` and `checkpoint log`, `diff` and `replay`**, the commands that read state. `thread list` reads none.
- **The schema step's test seam moved up**: tests' later steps are 3 and 4 now. A store's own steps run before them.

**Measured** (`benchmarks/RESULTS.md`, "A large state, offloaded"; two runs each). The same state as PR 2's, with its 64 KiB payload offloaded to the store's own database:

| | Rust | .NET |
|---|---:|---:|
| Bytes stored per save, offload (row and blob, mean of 20) | 3,489 (Redis 3,686) | 3,489 (Redis 3,686) |
| Postgres saves/s: whole, delta, offload | 22, 177–178, 291–296 | 22, 170–175, 272 |
| Redis saves/s: whole, delta, offload | 23, 214–217, 394–399 | 22–23, 198–199, 345 |
| SQLite saves/s: whole, delta, offload | 351–384, 404–436, 363–429 | 339, 383–389, 318–345 |
| Load P50, Postgres: whole, offloaded | 501–532 µs, 1.06–1.08 ms | 818–877 µs, 1.02–1.08 ms |

- A save stores the payload once per thread and a row of about 150 bytes.
- Postgres and Redis save faster with offload than with deltas because only a thread's first save carries a 64 KB command. Deltas carry one twice in twenty, and on this machine such a command takes about 45 ms (PR 1 and PR 2).
- A load reads the blob too: one more query or command, and the SHA-256 of 64 KiB.

**Tests.**

- .NET:
  - `Backends/OffloadTests.cs` (38):
    - the blob stores on their own;
    - the shared rows, from the database and from a filesystem blob store;
    - a value offloaded once and inherited, into each blob store, with its telemetry;
    - a missing and a tampered blob;
    - purges, and the Redis ttl;
    - a version-1 database gaining the table (SQLite, Postgres);
    - a base row another process replaced, under deltas and under offload alone;
    - group commit;
    - a durable run;
    - the store contracts with every value offloaded (also through group commit, and into a filesystem blob store);
    - the options.
  - One test in `Hosting/HostingTests.cs`.
- Rust:
  - `src/deltas.rs` (3 more): offloading and inheriting, a missing and a tampered blob, the fingerprint as a hash.
  - `src/offload.rs` (3): the blob stores and the options.
  - `tests/offload.rs` (33): the same store cases.
  - Offload runs of the contract suite in `tests/checkpoint_store_contract.rs` (3).
  - One test in `strata-host/tests/settings.rs`.
- The CLI reads the shared rows from the database and with `--blobs`, and checks the blob table's columns (`cli/tests/checkpoint_log_layout.rs`).
- The schema-version tests now expect 2.
- Verified locally with Postgres and Redis running and `STRATA_REQUIRE_BACKENDS=1`:
  - `dotnet test`: 968 passed and 2 skipped (the opt-in Cloud live tests), plus the example's 1.
  - `cargo test --all-features`: 917 in `rust/`, 82 in `cli/`, 1 in `examples/rust-agent/`.
  - `dotnet format --verify-no-changes`, `cargo fmt --check` and `cargo +1.99.0 clippy -D warnings` are clean.
  - `pack-smoke.sh` passes.
  - `cargo +1.99.0 package --workspace --exclude strata-bench` passes in a target directory never used before. One left by an earlier run of the same version failed it in the way PR 2 describes, so the directory must be new, not merely separate.
- Two .NET retry-count tests (`StorageHardeningTests.ARedisSaveThatMayHaveRunIsNotWrittenTwice`, `…InDoubtIsMadeAgain…`) failed once in a full run. They pass alone and in every later full run. A diagnostic of every retry in that run found only other tests' injected faults: the shared listener PR 2 recorded, with more tests now sharing it.

**Mutation checks** (each check broken, the offload suites run, then restored). Every one was caught in both engines:

- A load that skips the blob hash check: 3 tests in each engine (a tampered blob, in each store).
- A purge that leaves the blobs: 1 in each engine for each store (SQLite, Postgres, Redis).
- The base row checked by its sequence only, store by store: 1 in each engine for each store (the replaced row).
- A save that leaves out a remembered blob without relying on the row before it: 3 tests in Rust and 1 in .NET (the replaced row, under offload alone; the .NET run filtered to SQLite).

**Known limits.**

- The SQL token is `created_at`, at the second (above).
- A purge deletes the rows before it calls a blob store's `DeleteThreadAsync`. A run of the same thread id in between can therefore lose a blob it wrote; its load then fails, naming the blob, rather than reading wrong data.
- An orphan blob, from a save that failed after writing to a blob store, goes only with its thread (as decided).

---

## Not built (deliberately out of scope)

- **An engine-level write coalescer**, and batching anything but saves (loads, leases, listings).
- **The Cloud store**: its wire contract is Cloud's. An application that needs smaller Cloud checkpoints keeps its state small.
- **History stores**: their appends are unchanged.
- **Compression** of stored state, and MessagePack: `schema/README.md`'s "don't add it speculatively" stands.
- **Finer diffs** (RFC 6902 patches, nested keys), and deltas of checkpoints that aren't `running`.
- **S3, Azure Blob and GCS adapters**: the port is there for them.
- **Sharing blobs across threads**, and a sweeper for orphan blobs beyond purge.
- **Choosing what to offload by anything but size** (a per-field attribute, a type).
- **A regression gate** on the new benchmarks.
