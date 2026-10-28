# Datalium Strata: Memory & Persistence Architecture

In Datalium Strata, memory and chat history are designed with a strict separating layer: State vs. Persistence.

Python frameworks often pass full, bloated message objects directly through the graph nodes, causing heavy serialization bottlenecks. Datalium Strata keeps the execution graph lightweight by treating memory as a decoupled, zero-copy structural layer.

---

## 🏛️ The Architectural Design Principles

- **Separation of Concerns:** Nodes only query or append to memory via contextual handles; they do not carry the weight of the entire chat history in their active state payload.
- **Zero-Overhead Serialization:** History stores leverage high-performance native formats (e.g., protocol buffers or memory-mapped streams) rather than heavy JSON strings.
- **Pluggable Persistence Layer:** Out-of-the-box support for ephemeral in-memory caches, local embedded databases, and distributed enterprise storage systems.

---

## 📦 Memory Architecture Blueprint

```text
 ┌────────────────────────────────────────────────────────┐
 │                   Datalium Strata Engine               │
 └───────────────────────────┬────────────────────────────┘
                             │ Spawns Context
                             ▼
 ┌────────────────────────────────────────────────────────┐
 │                IExecutionContext (Ctx)                 │
 └───────┬────────────────────────────────────────┬───────┘
         │                                        │
         ▼ (Lightweight State)                    ▼ (Thread-Safe Access)
 ┌───────────────┐                        ┌───────────────┐
 │ Graph State   │                        │ Memory Check  │
 │ (Task/Query)  │                        │   (History)   │
 └───────────────┘                        └───────────────┘
                                                  │
                                                  ▼ Direct IO / Thread-Pool
                          ┌───────────────────────┴───────────────────────┐
                          │          IHistoryStore / IMementoSink         │
                          └───────────────────────┬───────────────────────┘
                                                  │
                ┌─────────────────────────────────┼────────────────────────────────┐
                ▼ (Local Dev)                     ▼ (Embedded)                     ▼ (Enterprise Tier)
        ┌───────────────┐                 ┌───────────────┐                ┌───────────────┐
        │   In-Memory   │                 │    SQLite /   │                │ Redis / Vault │
        │ ConcurrentRing│                 │   RocksDB     │                │   PostgreSQL  │
        └───────────────┘                 └───────────────┘                └───────────────┘
```

## 🛠️ Cross-Language Implementation Blueprint

Here is how this high-performance persistence architecture maps out into actual C# and Rust code designs.

### 🔷 C# Architecture (Datalium.Strata.Memory)

In C#, we use abstract interfaces that lean heavily on thread-safe asynchronous streams (`IAsyncEnumerable`) and zero-allocation memory spans to read historical contexts cleanly.

```csharp
namespace Datalium.Strata.Memory;

// 1. Immutable message structure utilizing efficient C# types
public record ChatMessage(MessageRole Role, ReadOnlyMemory<char> Content, DateTime Timestamp);
public enum MessageRole { System, User, Assistant, Tool }

// 2. High-performance, non-blocking storage interface
public interface IHistoryStore
{
    Task AppendAsync(string conversationId, ChatMessage message, CancellationToken ct = default);
    IAsyncEnumerable<ChatMessage> GetHistoryAsync(string conversationId, int limit = 50);
}

// 3. Example usage inside a Strata Graph Node
var graph = new StateGraph<MyState>(initialState)
    .AddNode("GenerateResponse", async (state, ctx) =>
    {
        // Fetch only what's necessary, streamed seamlessly into memory
        var history = await ctx.History.GetHistoryAsync(state.SessionId).ToListAsync();

        var aiResponse = await ctx.Models.CallWithHistoryAsync("gpt-4o", history, state.CurrentInput);

        // Non-blocking fire-and-forget or awaited append
        await ctx.History.AppendAsync(state.SessionId, new ChatMessage(MessageRole.Assistant, aiResponse.AsMemory()));

        return state with { IsCompleted = true };
    });
```

## 🦀 Rust Architecture (datalium-strata-memory)

In Rust, we utilize traits backed by asynchronous runtimes (tokio) and lifetimes to guarantee that checking chat history never creates variable reference collisions or runtime race conditions.

```rust
use async_trait::async_trait;
use std::sync::Arc;
use chrono::{DateTime, Utc};

// 1. Memory-optimized message representation
#[derive(Clone, Debug)]
pub struct ChatMessage {
    pub role: MessageRole,
    pub content: Arc<str>, // Reference-counted string slice to prevent heap-copy bloat
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug)]
pub enum MessageRole { System, User, Assistant, Tool }

// 2. Thread-safe persistence trait
#[async_trait]
pub trait HistoryStore: Send + Sync {
    async fn append(&self, session_id: &str, message: ChatMessage) -> Result<(), StrataError>;
    async fn get_history(&self, session_id: &str, limit: usize) -> Result<Vec<ChatMessage>, StrataError>;
}

// 3. Node orchestration within the Rust execution engine
graph.add_node("GenerateResponse", |state, ctx| async move {
    // Zero-copy reference fetching from the context store
    let history = ctx.history()
        .get_history(&state.session_id, 20)
        .await?;

    let response = ctx.model("gpt-4o")
        .chat_with_history(&history, &state.current_input)
        .await?;

    ctx.history()
        .append(&state.session_id, ChatMessage {
            role: MessageRole::Assistant,
            content: Arc::from(response.as_str()),
            timestamp: Utc::now(),
        })
        .await?;

    Ok(state)
});
```

## Key Architectural Differentiators Over Python

- **The Zero-Copy Advantage:** Using `ReadOnlyMemory<char>` in C# and `Arc<str>` in Rust prevents the system from constantly duplicating strings in memory as data moves through multi-agent validation loops.
- **Asynchronous Backpressure:** Both language designs natively support asynchronous streams. If an agent needs to scan a massive conversation log, it streams it sequentially from the database instead of loading a gigantic array into RAM all at once.
- **Decoupled State Management:** The history store updates outside the main graph execution loop. If a node fails or crashes, your historical conversational records are already safely flushed to the database.


---

## ✅ Implementation Status (as of demo 12, 2026-09-20)

The blueprint above is the design intent. What is actually built differs in shape, so read this before relying on the snippets.

**Built — every tier of the diagram, for both stores, in both languages.** `HistoryStore`/`IHistoryStore` *and* `CheckpointStore`/`ICheckpointStore` each have an in-memory, a SQLite, a Postgres and a Redis adapter:

| Backend | Rust (`strata-memory`) | C# (`Datalium.Strata.Storage`) |
|---|---|---|
| In-memory | `InMemoryHistoryStore`, `InMemoryCheckpointStore` | same names |
| SQLite | `SqliteHistoryStore`, `SqliteCheckpointStore` (`rusqlite`) | `EfCoreHistoryStore`, `EfCoreCheckpointStore` (EF Core) |
| Postgres | `postgres_store::{PostgresHistoryStore, PostgresCheckpointStore}` (`tokio-postgres`) | `Postgres.{PostgresHistoryStore, PostgresCheckpointStore}` (plain Npgsql) |
| Redis | `redis_store::{RedisHistoryStore, RedisCheckpointStore}` (async `redis` crate) | `Redis.{RedisHistoryStore, RedisCheckpointStore}` (StackExchange.Redis) |

The traits needed **no changes** to admit a backend that is not SQL-shaped — the Redis adapters have no table, no `sequence` column and no `MAX(sequence) + 1` (a checkpoint log or a session is one Redis `LIST`; the sequence is the list position). The same contract test suites (`rust/strata-memory/tests/{checkpoint,history}_store_contract.rs`, `StoreContractTests.cs`) run against all four backends, including a real freeze → resume through the engine.

**Cross-runtime.** A Postgres database or a Redis keyspace written by either runtime can be read — and a frozen thread resumed — by the other; layouts and fixtures are in `schema/README.md` ("Postgres checkpoint log", "Redis layout"). Unlike SQLite, the Postgres adapters support **concurrent writers**: a per-thread/session `pg_advisory_xact_lock` serializes the choice of the next `sequence`, and table creation takes a database-wide advisory lock because `CREATE TABLE IF NOT EXISTS` is not race-safe in Postgres. Redis needs no lock (`RPUSH` is atomic).

**Differs from the blueprint:**

- Every store is **async** in both runtimes since SG-11 (`design/19-Async-Queryable-Store.md`): Rust's `CheckpointStore` and `HistoryStore` return boxed futures (a blocking store is wrapped in `BlockingCheckpointStore` / `BlockingHistoryStore`), and .NET's `ICheckpointStore` gained `SaveAsync` / `LoadAsync` beside its synchronous members. Every checkpoint store also implements the query port (`ICheckpointQuery` / `CheckpointQuery`): list threads with filters, and read a thread's log.
- There is no `IMementoSink`, no Vault tier, and no ring buffer in front of a database.
- The SQLite history stores are **not** cross-runtime (the EF Core one stores the role as `"User"`, Rust as `"user"`). The Postgres and Redis history stores are.

**Not built:**

- ~~The `strata` CLI cannot read a Postgres or Redis checkpoint log.~~ It can since SG-11: `--storage` takes a `postgres://` or `redis://` URL (`design/19-Async-Queryable-Store.md`).
- **TLS** (Postgres: pass your own `postgres::Client` / `NpgsqlDataSource`; Redis: the `redis` crate's TLS features are not enabled), **reconnection** (a dropped connection is an error on the next call), **connection pooling in Rust** (one connection per store behind a `tokio::sync::Mutex`), **migrations**, **Redis Cluster/Sentinel**, and **expiry/eviction** of finished threads in Redis.
- **Dapper.** `CLAUDE.md`/`design/06-Project-Structure.md` list "EF Core / Dapper / Redis"; the Postgres adapter uses plain Npgsql, not Dapper or EF Core.
- No RocksDB tier, and no live cross-runtime CI job (the two workflows are separate jobs) — the layouts are pinned by shared fixtures asserted in each language's tests instead.
