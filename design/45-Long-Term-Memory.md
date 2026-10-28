# Datalium Strata: Long-Term Memory

Strata remembers a conversation (`IHistoryStore` / `HistoryStore`: role and text per session) and a run (checkpoints), but nothing an agent could recall from one run to the next by meaning. The first application to need it built its own: StrataOps' episodic memory (ADR 0054, `src/Fabrics/StrataOps.Memory/`) keeps one row per incident in `memory.incidents`, embeds the incident's symptoms, and recalls the tenant's most similar past incidents with pgvector's exact cosine distance, filtered by embedding model and excluding the incident itself (`RecallLimit` 3, `MinSimilarity` 0.75). StrataOps' plan has two more memories coming on the same pattern: procedural (`memory.runbooks`, embedded from `config/runbooks` at start-up, M4 PR 5) and semantic (`memory.facts`: topology, ownership, baselines).

This document closes that gap: **SG-32** (StrataOps' gap log: "episodic, semantic and procedural memory ports with similarity search … upstream a port once its shape is proven"). It lands in both engines (strict parity, StrataOps ADR 0003), in **three PRs** under this one blueprint:

- **PR 1, the store:** the memory store port and the record, an in-memory adapter, the Postgres adapter (pgvector), the retention port, the record's JSON schema, the shared fixtures and search cases, and the `memory` schema-version component.
- **PR 2, more adapters:** SQLite with sqlite-vec, and Redis with the Query Engine's vector search.
- **PR 3, embeddings and hosting:** an observed embedding call (.NET over `Microsoft.Extensions.AI`'s `IEmbeddingGenerator`; Rust a new `Embedder` trait with an OpenAI-compatible client), the `Memory` helper that embeds and stores or searches, hosting registration and a memory retention driver.

**It was written as a blueprint on 2026-10-21**, with the scope settled with Nick that day (below), and its build waited for StrataOps' ADR 0054 to be Accepted, so that the shape it upstreams had been proven. ADR 0054 was accepted the same day, as measured on real models: recall ranked right (119 of 120 runs recalled their own scenario first) and cost nothing measurable, and `MinSimilarity` 0.75 filtered nothing (OpenAI's similarities ran 0.98 to 1.00), so ranking, not a threshold, is what tells incidents apart. That is why a search's minimum similarity is optional and unset by default, and why ties are broken by id. StrataOps' runbooks (procedural memory) hadn't landed, so PR 1 is built from the episodic shape. "Review decisions" lists the questions still open with the answer the build will follow, and "Implementation Status" at the end will record where the build diverges.

---

## Decisions at a glance

| Question | Decision |
|---|---|
| When | **Blueprint now, build after ADR 0054 is Accepted** (settled 2026-10-21). |
| Which memories | **One generic memory store**, not one port per memory (settled 2026-10-21). A record is a namespace, a tenant, an id, the application's JSON content, string metadata, the text it was embedded from, and an embedding (its model and vector). Episodic, semantic and procedural memory are namespaces an application uses, not types Strata knows. |
| Where it lives | **The port, the record types and the in-memory adapter in the engine** (.NET `Datalium.Strata.Memory`, beside `IHistoryStore`; Rust `strata-core`'s `memory` module), with no new dependency. **The Postgres, SQLite and Redis adapters in the stores' packages** (`Datalium.Strata.Storage`, `datalium-strata-memory`). |
| Adapters | **In-memory** (exact cosine), **Postgres with pgvector** (SQL only, no EF plugin), **SQLite with sqlite-vec**, **Redis with the Query Engine** (settled 2026-10-21). |
| Embeddings | **The store takes vectors and never calls a model** (settled 2026-10-21). An optional, observed embedding call and a `Memory` helper (store + embedder) live in the AI packages: .NET over `IEmbeddingGenerator<string, Embedding<float>>`, Rust a new `Embedder` trait with `OpenAiEmbedder` (`/v1/embeddings`: OpenAI, Azure, Ollama, vLLM). |
| Search | **Exact cosine similarity by default**, `1 − cosine distance`, from −1 to 1, most similar first, ties by id. Filters (settled 2026-10-21): namespace (required), tenant, embedding model (required; vectors of different models are never compared), ids to exclude, metadata equality, a limit and a minimum similarity. From a node the tenant defaults to the run's metadata `tenant`. |
| Index | **None by default** (exact search). Postgres and Redis take an HNSW option; SQLite stays exact. |
| Dimensions | **Fixed per store**: a store is opened with its dimensions, a record or query vector of another length is refused, and opening a database whose memory was made with other dimensions is refused. |
| Wire format | **A new record schema** (`schema/memory-record.schema.json`, `1.0.0`), fixtures pinning each store's layout, shared search cases, and a **`memory` component** in `strata_schema_version` at version 1 (settled 2026-10-21). Checkpoints, definitions and history are unchanged. |
| CLI | **None in SG-32** (settled 2026-10-21); the layouts are pinned so a later `strata memory list` can read them. |
| Retention | **A purge port** like design/34's (settled 2026-10-21): by age, optionally by namespace and tenant, oldest first, up to a limit. A hosting driver calls it; nothing else does. |
| History with tool calls | **Not in SG-32** (settled 2026-10-21): design/44's review decision 9 stays. A conversation with tool calls is a different record from a memory, and an agent's conversation still lives for one attempt. |

---

## Usage

### .NET

```csharp
// Hosting: a Postgres memory store of 768 dimensions, an embedder, and the helper.
builder.Services.AddEmbeddingGenerator(sp => new OllamaEmbeddingGenerator(new Uri("http://localhost:11434"), "nomic-embed-text"));
builder.Services.AddStrata()
    .AddPostgresCheckpoints()
    .AddPostgresMemory(o => o.Dimensions = 768)          // Strata:Memory, Strata:Postgres's connection
    .AddMemory()                                         // a StrataMemory over the store and the registered generator
    .AddMemoryRetention(o => o.OlderThan = TimeSpan.FromDays(365));

// In a node: remember an incident by its symptoms ...
graph.AddNode("remember", async (state, ctx, ct) =>
{
    var memory = ctx.Services.GetRequiredService<StrataMemory>();
    await memory.RememberAsync(new MemoryEntry("incidents", state.IncidentId.ToString(), state.Symptoms)
    {
        Content = JsonSerializer.SerializeToNode(new { state.RootCause, state.Action, state.Recovered }),
        Metadata = new Dictionary<string, string> { ["service"] = state.Service },
    }, ctx, ct);                                          // tenant from ctx's run metadata "tenant"
    return state;
});

// ... and recall the ones like a new one.
graph.AddNode("recall", async (state, ctx, ct) =>
{
    var memory = ctx.Services.GetRequiredService<StrataMemory>();
    var similar = await memory.RecallAsync("incidents", state.Symptoms, new RecallOptions
    {
        Limit = 3,
        MinSimilarity = 0.75,
        Exclude = [state.IncidentId.ToString()],
    }, ctx, ct);
    return state with { Similar = similar.Select(m => m.Record.Content!.Deserialize<PastIncident>()!).ToList() };
});

// The store alone, with vectors the application made.
await store.UpsertAsync(new MemoryRecord("runbooks", "", "restart-pod", vector, "openai/text-embedding-3-small") { Text = markdown });
var matches = await store.SearchAsync(new MemorySearch("runbooks", query, "openai/text-embedding-3-small") { Limit = 5 });
```

### Rust

```rust
let store = Arc::new(PostgresMemoryStore::connect(url, PostgresMemoryOptions::new(768)).await?);
let embedder = Arc::new(OpenAiEmbedder::new(OpenAiOptions::ollama("http://localhost:11434"), "nomic-embed-text"));
let memory = Memory::new(store.clone(), embedder);

// In an async node:
memory
    .remember(
        MemoryEntry::new("incidents", incident_id, &state.symptoms)
            .with_content(Map::from_iter([("root_cause".to_string(), json!(state.root_cause))]))
            .with_metadata("service", &state.service),
        &ctx,
    )
    .await?; // a MemoryError, which converts into a NodeError
let similar = memory
    .recall("incidents", &state.symptoms, RecallOptions::new().limit(3).min_similarity(0.75).exclude(incident_id), &ctx)
    .await?;
```

---

## The API

### Engine (both): the record, the port, the in-memory adapter

| .NET (`Datalium.Strata.Memory`) | Rust (`datalium_strata::memory`) | What |
|---|---|---|
| `MemoryRecord(Namespace, Tenant, Id, Vector, Model) { Content, Text, Metadata, CreatedAt, UpdatedAt }` (`sealed record`; `Vector` a `ReadOnlyMemory<float>`; `Content` a `JsonObject?`; `Metadata` `IReadOnlyDictionary<string, string>`) | `MemoryRecord { namespace, tenant, id, embedding: Embedding, content: Option<Map<String, Value>>, text: Option<String>, metadata: BTreeMap<String, String>, created_at, updated_at }` (non-exhaustive, `MemoryRecord::new(ns, tenant, id, Embedding::new(model, vector))` and builders) | One memory. The key is (namespace, tenant, id). `Tenant` is `""` when the application has none. The timestamps are the store's (set on upsert, `CreatedAt` kept on a replace); a caller's are ignored. |
| `MemoryKey(Namespace, Tenant, Id)` | `MemoryKey` | A record's key. |
| `MemorySearch(Namespace, Vector, Model) { Tenant = "", Limit = 10, MinSimilarity, Exclude, Metadata }` | `MemorySearch::new(ns, Embedding)` with `.tenant()`, `.limit()`, `.min_similarity()`, `.exclude()`, `.with_metadata()` | A search. `Limit` 1 to 1,000. `MinSimilarity` from −1 to 1, none by default. `Exclude` ids within the namespace and tenant. `Metadata` is equality on every pair given. |
| `MemoryMatch(Record, Similarity)` | `MemoryMatch { record, similarity: f64 }` | A result; `Similarity` = 1 − cosine distance. |
| `MemoryQuery(Namespace) { Tenant, Metadata, Limit = 100, Cursor }` → `MemoryPage(Records, NextCursor)` | `MemoryQuery` → `MemoryPage` | A listing by id, for an application that syncs a namespace (runbooks at start-up) and for a later CLI. Keyset cursor on `id`, encoded like `ThreadQuery`'s. |
| `IMemoryStore`: `Dimensions`, `UpsertAsync(record)`, `UpsertManyAsync(records)`, `GetAsync(key)`, `DeleteAsync(key)` → `bool`, `SearchAsync(search)` → `IReadOnlyList<MemoryMatch>`, `ListAsync(query)` → `MemoryPage` (each with a `CancellationToken`) | `MemoryStore` trait, the same members returning `BoxFuture<'_, Result<_, MemoryStoreError>>`, plus `dimensions()` | The port. `UpsertManyAsync` is atomic in the SQL stores (one transaction) and per record in Redis. |
| `IMemoryRetention.PurgeAsync(MemoryPurge(OlderThan) { Namespace, Tenant, Limit = 1000 })` → `int` | `MemoryRetention::purge(&MemoryPurge)` → `usize` | Deletes records whose `UpdatedAt` is before `OlderThan` (exclusive, at the second), oldest first, up to the limit. Call again while it returns the limit. |
| `InMemoryMemoryStore(dimensions, TimeProvider?)` | `InMemoryMemoryStore::new(dimensions)` / `.with_clock(..)` | Exact cosine over a dictionary, behind a lock. Implements both ports. |
| `MemoryStoreException` | `MemoryStoreError(String)` | A store failure, as `HistoryStoreError` is. |

**Refused on input** (.NET `ArgumentException`, Rust `MemoryStoreError` before any I/O): an empty namespace or id, a namespace, tenant or id longer than 256 bytes of UTF-8, a vector whose length isn't the store's dimensions, a vector with a non-finite element or all zeros (cosine is undefined), an empty model, a metadata key that is empty or longer than 128 bytes, a limit out of range, a minimum similarity outside −1 to 1. (Content that isn't a JSON object can't be given: both engines type it as one.) The messages are the same in both engines and pinned by the shared cases, such as `the vector has 2 dimensions, and this store has 3`.

### Stores: the adapters

| .NET (`Datalium.Strata.Storage`) | Rust (`datalium-strata-memory`) | Notes |
|---|---|---|
| `PostgresMemoryStore(connectionString \| dataSource, PostgresMemoryOptions { Dimensions, Schema, Index = None \| Hnsw(m, efConstruction), Retry, AutoMigrate }, ownsDataSource)`; `OpenAsync`, `MigrateAsync` | `PostgresMemoryStore::connect(url, PostgresMemoryOptions::new(dims))`, `connect_with`, `from_pool`; `migrate` | PR 1. Needs the `vector` extension: the migration runs `CREATE EXTENSION IF NOT EXISTS vector` and, without the privilege, refuses with `memory needs pgvector: CREATE EXTENSION vector failed (…)`. Retries as design/34's stores (`StoreRetryPolicy` / `StoreRetry`, `strata.store.operation` `memory`). Rust shares the `deadpool-postgres` pool type. No constructor touches the network (.NET). |
| `SqliteMemoryStore(path, SqliteMemoryOptions { Dimensions, Extension })` | `SqliteMemoryStore::open(path, SqliteMemoryOptions::new(dims))`, `open_in_memory` | PR 2. sqlite-vec's scalar `vec_distance_cosine` over an ordinary table, so filters are plain `WHERE` clauses and search is exact. See "SQLite and sqlite-vec". |
| `RedisMemoryStore(multiplexer, RedisMemoryOptions { Dimensions, Prefix, Index = Flat \| Hnsw(..), Retry })`; `ConnectAsync` | `RedisMemoryStore::connect(url, RedisMemoryOptions::new(dims))` | PR 2. Hashes under `{prefix}:memory:` and one `FT` index. Needs a server with the Query Engine (Redis 8, or Redis Stack). See "Redis". |

### AI packages: embeddings and the helper (PR 3)

| .NET (`Datalium.Strata.AI`) | Rust (`datalium-strata-ai`) | What |
|---|---|---|
| — (`IEmbeddingGenerator<string, Embedding<float>>` from `Microsoft.Extensions.AI.Abstractions`, already referenced) | `Embedder` trait: `provider()`, `default_model()`, `embed(EmbeddingRequest { inputs, model, dimensions })` → `BoxFuture<Result<EmbeddingResponse { vectors, model, usage }, ChatError>>` | The embedding abstraction. Rust's is object safe like `ChatClient`. |
| — | `OpenAiEmbedder::new(OpenAiOptions, model)` (feature `openai`) | `POST /v1/embeddings`, `encoding_format` `float`, `dimensions` sent when asked (OpenAI's `text-embedding-3-*`). OpenAI, Azure, and compatible servers (Ollama, vLLM). Anthropic has no embeddings endpoint, so there is no Anthropic embedder. |
| `StrataEmbeddings.GenerateAsync(generator, texts, EmbeddingGenerationOptions?, NodeContext?, StrataChatOptions?, ct)` → `GeneratedEmbeddings<Embedding<float>>` | `embed(&dyn Embedder, EmbeddingRequest, &NodeContext)` / `embed_with(…, Option<&NodeContext>, &CallOptions)` | One observed call: a model-call scope with operation `embeddings`, faults classified as model calls' are (transient with the hint, or fatal), batched as given. |
| `StrataMemory(IMemoryStore, IEmbeddingGenerator<…>, StrataMemoryOptions { Model, TenantKey = "tenant" })`: `RememberAsync(MemoryEntry, ctx?, ct)`, `RememberManyAsync`, `RecallAsync(ns, text, RecallOptions, ctx?, ct)` | `Memory::new(Arc<dyn MemoryStore>, Arc<dyn Embedder>)` with `.with_model()`, `.with_tenant_key()`; `remember`, `remember_many`, `recall` | Embeds the text (and records it as `Text`), labels the vector `<provider>/<model>` unless `Model` is given, takes the tenant from the run's metadata at `TenantKey` when the entry or options don't name one, and stores or searches. |
| `MemoryEntry(Namespace, Id, Text) { Tenant, Content, Metadata }` | `MemoryEntry::new(ns, id, text)` with builders | What `remember` takes: text instead of a vector. |

---

## Semantics

### The record and the key

A record is identified by (namespace, tenant, id). An upsert replaces every field but `CreatedAt`. Namespaces separate kinds of memory (`incidents`, `runbooks`, `facts`); a search never crosses one. A tenant is a string, `""` for none; a search with tenant `""` reads only tenant-less records, never every tenant's (recall across tenants isn't built, as in ADR 0054). `Text` is optional and never searched. It is kept so that an application can re-embed a namespace when it changes models, which a vector alone wouldn't allow.

### The embedding model

Every record carries the model that made its vector, as free text compared exactly (the helper writes `<provider>/<model>`, as StrataOps' `ITextEmbedder.Model` does). A search names a model and reads only records of that model: two models' vectors are never compared, even at the same dimensions. Changing models therefore leaves old records unread until they are re-embedded, which is the safe failure.

### Search

Similarity is `1 − cosine distance`, from −1 to 1, with no normalization by the store (cosine doesn't need it). Results are ordered most similar first, ties by id (ordinal), and cut at the limit; `MinSimilarity` drops matches below it, so a search may return fewer than the limit. Every filter is applied **before** ranking: a record that fails a filter never takes a place in the limit. That holds with an index too (Postgres's iterative scan, Redis's pre-filter; see each store).

Scores differ between stores in the last bits (float32 arithmetic in pgvector, sqlite-vec and Redis; float64 in the in-memory store). The shared cases compare scores to 1e-5 and choose vectors whose ranks don't depend on it.

### Dimensions

A store is opened with its dimensions (1 to 16,000, pgvector's limit for an un-indexed column; 2,000 with an HNSW index). The first open records them (Postgres in the column type `vector(N)`, SQLite in `memory_settings`, Redis in the index's `DIM`), and a later open with other dimensions is refused: `memory was created with 768 dimensions, and this store was opened with 1536`. An application with two embedding sizes uses two stores (a Postgres or SQLite `Table` option, a Redis `Prefix`).

### Postgres

```sql
CREATE TABLE memory_records (
    namespace   TEXT NOT NULL,
    tenant      TEXT NOT NULL,
    id          TEXT NOT NULL,
    model       TEXT NOT NULL,
    embedding   vector(768) NOT NULL,
    content     JSONB,
    text        TEXT,
    metadata    JSONB NOT NULL DEFAULT '{}',
    created_at  TEXT NOT NULL,         -- RFC 3339, UTC, whole seconds (as checkpoints' created_at)
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (namespace, tenant, id)
);
CREATE INDEX memory_records_scope ON memory_records (namespace, tenant, model);
CREATE INDEX memory_records_updated ON memory_records (updated_at);
```

The vector is sent as text cast to `vector` (`CAST($1 AS vector)`, as StrataOps does), so neither engine needs pgvector's client type mapping or the EF plugin. Search:

```sql
SELECT …, 1 - (embedding <=> CAST($q AS vector)) AS similarity
FROM memory_records
WHERE namespace = $ns AND tenant = $t AND model = $m
  AND id <> ALL($exclude) AND metadata @> $meta
ORDER BY embedding <=> CAST($q AS vector), id
LIMIT $limit
```

`MinSimilarity` is applied by the reader, which stops at the first row below it. `Index = Hnsw` creates `USING hnsw (embedding vector_cosine_ops)` and sets `hnsw.iterative_scan = relaxed_order` for the search's transaction (pgvector 0.8), so filters don't starve the limit. The results are then re-sorted by exact distance. An approximate index may still miss a near neighbour; exact search is the default for that reason.

### SQLite and sqlite-vec

The table is the same with SQLite types: `embedding` a BLOB of little-endian float32 (sqlite-vec's own format, so rows are readable without the extension), `content` and `metadata` JSON text, timestamps RFC 3339 text at whole seconds, as in Postgres. Search uses sqlite-vec's scalar `vec_distance_cosine(embedding, ?)` with ordinary `WHERE` clauses (metadata through `json_extract`), so it is exact and filters behave as Postgres's do. The `vec0` virtual table and its KNN are not used, since its metadata filtering is narrower and SQLite's scale doesn't need them.

Loading the extension differs by engine:

- **Rust:** the `sqlite-vec` crate compiles sqlite-vec's C source into the binary and registers `sqlite3_vec_init` with `sqlite3_auto_extension` before the store opens a connection. It needs a `rusqlite` with `bundled`, which `datalium-strata-memory` already uses.
- **.NET:** SQLite loads extensions with the platform's own loader, not .NET's NuGet runtime resolution. The store therefore calls `SqliteConnection.LoadExtension(SqliteMemoryOptions.Extension)` on each connection it opens. `Extension` is a path or a file name and defaults to `vec0`. The store first looks in the application's `runtimes/<rid>/native/` folder.

Both are pre-1.0 (the crate 0.1.7, the NuGet package `sqlite-vec` an alpha prerelease). Review decision 3 covers that risk. A missing extension is refused on first use: `memory on SQLite needs the sqlite-vec extension: …`.

### Redis

A record is a hash at `{prefix}:memory:{namespace}:{tenant}:{id}` (each part escaped). It has fields `namespace`, `tenant`, `id` and `model` (TAG), `meta` (TAG, one `key=value` per metadata pair, separator `\x1f`), `embedding` (float32 little-endian bytes), `content`, `text` (not indexed), and `created_at` / `updated_at` (NUMERIC, Unix ms). One index per store is created on first use:

```
FT.CREATE {prefix}:memory:idx ON HASH PREFIX 1 {prefix}:memory:
  SCHEMA namespace TAG tenant TAG id TAG model TAG meta TAG SEPARATOR "\x1f" updated_at NUMERIC SORTABLE
         embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 768 DISTANCE_METRIC COSINE
```

`Index = Hnsw` uses `HNSW` instead of `FLAT`. A search is `FT.SEARCH … "(@namespace:{ns} @tenant:{t} @model:{m} -@id:{a|b} @meta:{k\=v})=>[KNN $limit @embedding $vec AS distance]" SORTBY distance DIALECT 2`. The filter is applied before the KNN, as the Query Engine's hybrid query does. An `FT.CREATE` against a server without the Query Engine fails with an unknown command. The store refuses with `memory on Redis needs the Query Engine (Redis 8 or Redis Stack): FT.CREATE is unknown`. An existing index with another `DIM` is refused as above. Purge is a script over `FT.SEARCH @updated_at:[-inf (cutoff]` in pages. Listing is `FT.SEARCH` sorted by `id`. .NET sends `FT.*` through `IDatabase.ExecuteAsync` and Rust through `redis::cmd`, so neither needs a client library beyond the current ones.

### Schema version and migration

A new component, **`memory`, at version 1**, in the existing `strata_schema_version` table of each SQL database (design/34: SQLite memory stores always migrate on open; Postgres follows `AutoMigrate` / `MigrateAsync`; a database at a newer version is refused). Redis records the version in `{prefix}:memory:schema`. The checkpoint and history components are untouched.

### Retention

`PurgeAsync` deletes records with `updated_at < OlderThan` (at the second, exclusive), filtered by namespace and tenant when given, oldest first, up to `Limit`, in one statement per page (SQL) or a script per page (Redis), and returns how many it deleted. Nothing calls it but the hosting driver.

### The helper (PR 3)

`Memory.RememberAsync(entry, ctx)` embeds `entry.Text` through one observed call and upserts with the vector, `Text`, the model label and the tenant. `RememberManyAsync` embeds every text in one call and upserts them in one `UpsertManyAsync`. `RecallAsync(ns, text, options, ctx)` embeds the text and searches. A failure to embed is the embedding call's node failure (transient with the provider's hint, or fatal). A store failure is a `MemoryStoreException` / `MemoryStoreError` for the caller to handle. ADR 0054's "memory that can't be read recalls nothing" is the application's choice, not the helper's.

---

## Telemetry

- **The embedding call (PR 3)** goes through the engine's `ModelCallScope` with `ModelCallInfo.Operation` (already in both engines since design/44, `chat` by default) set to `embeddings`. The span is `embeddings <model>`, with `gen_ai.operation.name` `embeddings` and `gen_ai.usage.input_tokens`. The run events are `ModelCallStarted` / `ModelCallCompleted`, gaining an `Operation` field so an observer can tell the two apart. The metrics are `gen_ai.client.token.usage` (input only) and `gen_ai.client.operation.duration`. No text or vector is ever recorded.
- **The stores** (not the in-memory one) record `strata.memory.operations` (counter) and `strata.memory.operation.duration` (histogram, s), by `strata.store` (`postgres`, `sqlite`, `redis`) and `strata.memory.operation` (`upsert`, `get`, `delete`, `search`, `list`, `purge`). `strata.memory.search.results` is a histogram of the matches returned. They use the meter `Datalium.Strata.Storage` (.NET) and the `metrics` facade (Rust). Retries are counted as design/34's are, with `strata.store.operation` `memory`.
- **No run events for searches.** A recall is the node's business. Review decision 6 covers this.

---

## Packaging

- **Engine:** the record, the ports and the in-memory store, with no new dependency.
- **Storage:** Postgres needs nothing new (text casts). SQLite gains the `sqlite-vec` crate (Rust) and the `sqlite-vec` NuGet package or an application-supplied binary (.NET; see review decision 3). Redis needs nothing new.
- **AI:** `OpenAiEmbedder` behind the existing `openai` feature. .NET needs nothing new.
- **Hosting:** see below. The package set stays at five NuGet packages and six crates.

### Hosting (PR 3)

.NET adds `AddInMemoryMemory()`, `AddPostgresMemory()`, `AddSqliteMemory()`, `AddRedisMemory()` and `AddMemoryStore<TStore>(factory)`. Each registers one singleton under `IMemoryStore` and `IMemoryRetention`, with one memory store per container. Each reuses `Strata:Postgres` / `Strata:Sqlite` / `Strata:Redis` for the connection, and `Strata:Memory` (`Dimensions` required, `Index`, `Table` / `Prefix`, `Extension`) for the rest. The start-up check migrates or checks the `memory` component and the dimensions. `AddMemory()` registers a `Memory` over the store and the container's `IEmbeddingGenerator<string, Embedding<float>>`, and `Strata:Memory:Model` overrides the label. `AddMemoryRetention(o => o.OlderThan = …)` is a driver like `AddRetention` (every hour, `OlderThan` required, `Namespace` / `Tenant` optional).

Rust adds a `StrataSettings.memory` (`MemorySettings`: `dimensions`, `index`, `table` / `prefix`, `extension`) with `open()` on each store's settings, and `Host::retain_memory(store, MemoryRetentionDriver)`.

---

## Tests

- **Shared cases** in `schema/fixtures/memory/search-cases.json`: records, then searches with their expected ids in order and their scores (to 1e-5). They cover each filter, exclusion, metadata, ties by id, `MinSimilarity` cutting short, another model never read, another tenant never read, an upsert replacing and keeping `created_at`, delete, list paging, and purge by age, namespace and tenant. Every adapter runs them in both engines: in-memory always, Postgres and Redis when `STRATA_TEST_POSTGRES_URL` / `STRATA_TEST_REDIS_URL` are set, SQLite always (file and in-memory).
- **Layout fixtures:** `memory/postgres-columns.json`, `sqlite-columns.json`, `redis-record.json` (the hash and the `FT.CREATE` arguments) and `memory.rows.json` (rows one engine wrote, read by the other). `checkpoint-log/schema-versions.json` gains the `memory` component.
- **Refusals:** each input refusal, the dimension mismatch on open, a missing pgvector, sqlite-vec or Query Engine (Redis 7), and a newer schema version.
- **PR 3:** the embedding call against a scripted `IEmbeddingGenerator` / `Embedder` (events, span attributes, usage, fault mapping), `OpenAiEmbedder` against the raw local HTTP server with fixtures, the helper's tenant default, and live tests opt-in on `STRATA_TEST_OPENAI_*` as design/44's are.
- **CI:** Postgres's service image becomes `pgvector/pgvector:pg16`, the image StrataOps' tests already use. Redis's becomes a Redis 8 image (the checkpoint stores' tests run on it too; to confirm that the Alpine variant carries the Query Engine). The package smoke app gains an in-memory memory search.

---

## Wire format

- **New:** `schema/memory-record.schema.json` (`schema_version` `1.0.0`): `namespace`, `tenant`, `id`, `embedding` (`model`, `vector`: an array of numbers), `content` (an object or null), `text`, `metadata` (string to string), `created_at` and `updated_at` (RFC 3339). It is the record's JSON form, for fixtures and a later CLI. The stores keep their own columns, pinned by the layout fixtures.
- **New:** the `memory` component in `strata_schema_version` (version 1), and the memory tables and keys above.
- **Changed:** none. Checkpoints, graph definitions and history records are unchanged. `ModelCallStarted` / `ModelCallCompleted` gain `Operation`, which is additive.
- `CHANGELOG.md`'s entry lists the memory record `1.0.0` among what the version writes and reads.

---

## Review decisions

Recommendations are in bold; the build will follow each unless review changes it.

1. **One generic store or a port per memory?** **One generic store** (settled 2026-10-21). Episodic, procedural and semantic memory differ in what they hold and who writes them, not in how they are stored or searched; namespaces separate them.
2. **Does the store embed?** **No** (settled 2026-10-21). The store takes vectors; the helper embeds. A store that called models would tie storage to providers, retries and cost.
3. **sqlite-vec is pre-1.0. Depend on it, or score in code?** **Depend on it, as chosen (settled 2026-10-21), with the layout independent of it**: vectors are float32 BLOBs that any reader can score, so if sqlite-vec stalls, the store can score in code with no migration. At build time, verify the NuGet package's native binaries per RID and the crate's `rusqlite` range; if the NuGet package can't be shipped, the .NET store takes the binary's path from the application (`Extension`) and the smoke app leaves SQLite memory out.
4. **Redis needs the Query Engine.** **Require it** (settled 2026-10-21 by choosing Redis). That means Redis 8 or Redis Stack, and a clear refusal otherwise. Redis 8's vector sets (`VADD` / `VSIM`) were not chosen: they are in beta, and they can't pre-filter by tenant and metadata in one query.
5. **Exact search by default, or HNSW?** **Exact.** It matches ADR 0054 and returns true nearest neighbours; an index is an option for stores past tens of thousands of records per (namespace, tenant, model).
6. **Run events for memory operations (`MemoryRecalled`)?** **No.** The store is a port the application calls, as `IHistoryStore` is; metrics cover operations, and the embedding call is already observed. Add one when an application needs searches in its run timeline.
7. **A ready-made memory tool for agents** (`memory.search` as a `ToolRegistry` tool, as StrataOps' `MemoryTools` does)? **Not in SG-32.** It needs choices that belong to the application: which namespaces, whose tenant, and how results read to a model. It can follow once two applications want the same one.
8. **The tenant default from run metadata.** **`tenant`, configurable** (`TenantKey`), as the scheduler's tenant is (design/40). A node without one writes and reads tenant `""`.
9. **`UpsertManyAsync` atomic?** **In SQL, yes** (one transaction); **in Redis, per record** (a script over many hashes would block the server for a large sync). The difference is documented.
10. **Embedding observation: reuse `ModelCallScope` or a new scope?** **Reuse it** with `Operation` `embeddings` (the field exists; only the events gain it): the same events, span and metrics with a different operation name, as the GenAI conventions do. A second scope would duplicate it.

---

## Implementation Status

**PR 1** (2026-10-21, branch `sg-32-memory-store`), after ADR 0054 was accepted. Settled with it:

- **Timestamps are RFC 3339 text at whole seconds** in Postgres (and SQLite), as checkpoints' `created_at` is, so both engines write the same strings and a purge compares them as text.
- **Shared fixtures:** `schema/memory-record.schema.json`, and in `schema/fixtures/memory/`: `record.sample.json`, `postgres-columns.json` and `search-cases.json` (eight cases: ranking and ties, the namespace, tenant and model filters, metadata, upserts keeping `created_at`, deletes, listing in pages, purging oldest first, and the refusals with their exact messages).
- **CI's Postgres** is the `pgvector/pgvector:pg16` image in both workflows.

PRs 2 and 3 follow in the order above. Where PR 1's build diverges from or completes the text above:

- **`COLLATE "C"`** on `namespace`, `tenant` and `id`, and on every ordering and keyset comparison, so ties and pages sort by bytes in both engines whatever the database's locale.
- **`Table`** may be `schema.name` (each part `[a-z_][a-z0-9_]{0,62}`). Indexes are named after the last part: `<table>_scope`, `<table>_updated`, `<table>_embedding_hnsw`.
- **Every preparation ensures its table.** The `memory` component row is per database, and several tables may share it. So preparation always:
  - creates the extension if `pg_extension` lacks it, the table and its indexes `IF NOT EXISTS`, and the HNSW index when asked;
  - checks the `embedding` column's dimensions.
  
  With `AutoMigrate` off, a missing table is refused (`the memory table 'x' doesn't exist: run its migration`).
- **An HNSW search** takes its candidates from a `MATERIALIZED` CTE limited to twice the limit, then re-sorts by exact distance and id and cuts. An inner limit equal to the limit could let a tie at the cut be decided by index order.
- **Refusals beyond the list above:**
  - `the metadata key 'k' has no value`;
  - `the limit is N; it must be at least 1` (purge);
  - `'x' is not a memory listing cursor`;
  - .NET: `the tenant is null; use "" for none`.
  
  A store's other failures are `MemoryStoreException` / `MemoryStoreError`: `memory <operation> failed: …`, and the shared schema-version refusals.
- **The in-memory store's dimensions:** Rust's `InMemoryMemoryStore::new` panics outside 1 to 16,000, where .NET throws. Postgres refuses HNSW above 2,000 dimensions.
- **Rust's `PostgresMemoryOptions` takes a clock** (`with_clock`), and .NET's a `TimeProvider`, so the shared cases can set it. By default it is the process clock, not the server's.
- **Rust's `upsert_many`** is one prepared statement per record in one transaction.
- **Metrics** carry no outcome tag. They are recorded for failed calls too, but not for calls refused for bad input.
- **Not verified locally:** no Postgres was available, so the Postgres tests first ran in CI.

**PR 2** (2026-10-21, branch `sg-32-memory-sqlite-redis`): the SQLite and Redis stores, both engines. Both pass the eight shared cases: SQLite on a file and in memory, Redis on FLAT and HNSW. New layout fixtures: `memory/sqlite-columns.json`, `sqlite-settings-columns.json` and `redis-record.json`. CI's Redis is `redis:8` in both workflows, which the checkpoint stores' Redis tests use too. Where PR 2's build diverges from or completes the text above:

- **sqlite-vec in .NET is supplied by the application** (review decision 3's fallback). The only NuGet package, `sqlite-vec` 0.1.7-alpha.2.1, has three problems:
  - it compiles a public `Microsoft.Data.Sqlite.SqliteVectorExtensions` class into any project that references it (a `contentFiles` source);
  - its build targets fail an exe whose `PlatformTarget` is arm64;
  - it ships no win-arm64 or win-x86 binary.

  `Datalium.Strata.Storage` therefore doesn't reference it, and its package set is unchanged. The store calls `LoadExtension(SqliteMemoryOptions.Extension)` on each connection it opens. `Extension` defaults to `vec0`, which Microsoft.Data.Sqlite finds in `runtimes/<rid>/native/` through the native search directories, so an application that references the package with `ExcludeAssets="contentFiles"` needs nothing else. The test project does exactly that, on win-x64 locally and linux-x64 in CI. The smoke app (PR 3) leaves SQLite memory out.
- **sqlite-vec in Rust** is the crate's 0.1.9, the latest stable release rather than 0.1.7. It only needs `cc` to build, and its C is compiled with `SQLITE_CORE` against rusqlite's bundled SQLite. It is registered **per connection**, by calling `sqlite3_vec_init` on the connection's handle, not process-wide with `sqlite3_auto_extension`, so other connections in the process don't gain its functions.
- **SQLite layout:**
  - `Table` is one lowercase name: SQLite has no `schema.` part.
  - The dimensions are in **`memory_settings (table_name, dimensions)`**, one row per memory table, not a one-row table, so two tables in one file work as they do in Postgres.
  - Preparation runs in one `BEGIN IMMEDIATE` transaction, after sqlite-vec is checked:
    1. refuse a newer `memory` version;
    2. create the settings table, the memory table and its indexes;
    3. record the `memory` component at 1 in the file's `strata_schema_version`;
    4. insert or check the dimensions.

  A file shared with the SQLite checkpoint store keeps both components.
- **SQLite search** filters with `json_each`, not `json_extract`, for both the excluded ids (one JSON array parameter) and each metadata pair, since a key may contain `"` or `.`. Results are ordered `ORDER BY distance, id` in SQL, with similarity `1 − distance`. SQLite's default `BINARY` collation is byte order.
- **One SQLite connection per store**, with calls one at a time (.NET a semaphore; Rust a mutex, calls on the blocking pool), so `:memory:` works without a shared cache. .NET opens a plain path with `Pooling=False`, so disposing releases the file.
- **The Redis layout** (`redis-record.json`) differs from the text above:
  - **Record keys:** records live under **`{prefix}:memory:records:`**, so no record key can collide with the store's other keys (`ids:`, `updated`, `schema`). Key parts are escaped `%` to `%25` and `:` to `%3A`.
  - **Hex tags:** the index doesn't tag the raw namespace, tenant, id, model and metadata. It tags **hex encodings** of them (`x` + hex of the UTF-8): `_scope` (namespace + tenant + model), `_id`, and `_meta`, one per pair. The raw values stay in their own unindexed fields. A tag then needs no query escaping, is never empty (a tenant `""` would need `INDEXEMPTY`), isn't case-folded or trimmed, and can't contain the TAG separator.
  - **Timestamps** are RFC 3339 text, as in SQL. There is no NUMERIC `updated_at` field, since nothing queries it through the index.
  - **`{prefix}:memory:schema` is a hash** (`version`, `dimensions`): the dimensions are checked from it, not parsed out of `FT.INFO`. It is written with `HSETNX` and read again, so two openers racing agree.
- **Redis listing and purge use two sorted sets, not `FT.SEARCH`:**
  - **Listing:** `{prefix}:memory:ids:{ns}:{tenant}` (ids at score 0) gives byte order and a keyset cursor through `ZRANGEBYLEX`, which a TAG field can't give. Metadata is filtered in code until the page is full.
  - **Purge:** `{prefix}:memory:updated` (record keys scored by the `updated_at` text's digits, as the checkpoint index scores) is read oldest first. Each score group is ordered by (namespace, tenant, id) in code, as SQL orders it. Each record is deleted by a script that checks its `updated_at` is still before the cutoff. A transient failure repeats that one call, not the whole purge, which could otherwise delete past its limit.
  - **Stale entries:** an entry whose hash is gone is skipped.
- **The Redis scripts:** upsert and delete are byte-identical in both engines (`UpsertScript` / `UPSERT_SCRIPT`, `DeleteScript` / `DELETE_SCRIPT`). Each keeps `created_at` and both sorted sets in step.
- **The Redis search** is a KNN over the pre-filtered set:
  - It starts with K = 2 × limit and doubles while K came back and the K-th distance is within 1e-5 of the limit-th's, up to 10,000, so a tie across the cut is complete.
  - Candidates are **re-scored in f64** from their stored vectors, as the in-memory store does, ordered by (similarity, id), and cut at `MinSimilarity` and the limit.
  - Both engines take only `__distance` (`RETURN 1 __distance`) and read the hashes in one pipeline, then check each record against the filters again. .NET used to take the fields through `RETURN`, but on Redis 8 a reply that returns the documents' fields can leave documents out: in CI, 18 of 480 such searches straight after a confirmed write missed one to all six records, while the same search for `__distance` alone, run just before or after each, never did (2026-10-23). The index had every record; only the reply with fields dropped some.
- **An existing Redis index is kept as it is.** Opening with HNSW doesn't rebuild a FLAT index, and the reverse holds too. The index is created only when missing.
- **Opening Redis records the dimensions before it creates the index.** The steps are:
  1. read `{prefix}:memory:schema`, refusing a newer version or other dimensions;
  2. `HSETNX` the version and dimensions, read them again and check;
  3. only then `FT.CREATE`.

  The other order let two first openers with different dimensions race: the loser of `HSETNX` was refused, but the winner went on against an index created at the loser's `DIM`, and its records were never indexed. On Redis 7 the refusal comes after the schema hash is written, which leaves that hash behind; it is harmless.
- **A Redis delete whose reply was lost** reports `false` when its retry finds the record gone, as any store's retry would. For the same reason a purge can delete one record more than its limit for each such lost reply. A purge repeats each read and each delete on its own, not the whole purge.
- **SQLite ranks by sqlite-vec's float32 distance**, as Postgres ranks by pgvector's. Two records whose similarities are equal mathematically but round differently aren't ordered by id. The in-memory and Redis stores re-score in f64 and are.
- **The Redis search sends `TIMEOUT 0`**, so the Query Engine's default query timeout can't return a partial top K.
- **Byte order everywhere:** .NET's in-memory store ordered ids with `StringComparer.Ordinal` (UTF-16). It now uses **`MemoryRecords.ByteOrder`** (UTF-8 byte order), as SQLite, Redis, Postgres's `COLLATE "C"` and Rust do.
- **Refusals added:**
  - `the memory table 'x' isn't a lowercase SQL name` (SQLite);
  - `the memory prefix is empty` (Redis);
  - `memory on SQLite needs the sqlite-vec extension: …`;
  - `memory on Redis needs the Query Engine (Redis 8 or Redis Stack): FT.CREATE is unknown`, when an `FT.CREATE` reply says "unknown command";
  - the shared dimensions and newer-version refusals.

  Other store failures are wrapped as in PR 1: .NET's say `memory … failed: …` / `memory couldn't be prepared: …`, and Rust's pass the store's message through, as its Postgres store does.
- **Not verified locally:** no Redis 8 was available, so the Redis stores first ran in CI.
- **A Redis write returns only once the index finds it** (found after PR 3, 2026-10-21). PR 3's CI failed once on the HNSW shared case: a search missed one of six records written just before it, and passed when run again. A diagnostic in CI (Redis 8.10.2, `FT.CONFIG GET WORKERS` = 2) wrote the six records and searched at once, 150 times per setup. Results:
  - HNSW missed 6 times.
  - HNSW with the records written one at a time missed 4 times.
  - HNSW with a 200 ms wait before the search missed once.
  - HNSW without the exact-duplicate vector missed 3 times.
  - **FLAT missed twice.**

  Every miss was found by the same search 500 ms later, and the missing record varied. So the Query Engine's worker threads index a hash **after** its write returns, for any index type, and a `remember` followed at once by a `recall` could miss. Every other store reads its own writes.

  Each upsert therefore confirms its records before it returns:
  1. **The query:** a `KNN 1` query per record, filtered to its scope, id and metadata tags, with the record's own vector, the same tags a search builds.
  2. **Confirmed:** when the index returns that record at distance ≤ 1e-4, so an updated vector is seen too.
  3. **Rounds:** unconfirmed records are asked again together, 1 ms apart, doubling to 50 ms.
  4. **The bound:** 5 s of real time. Past it the upsert fails with `memory on Redis didn't index record '<id>' within 5 s of writing it`. The record is written by then, and a retry rewrites it.

  The cost is one more round trip per record written. Listing and purging read the sorted sets, so they were never affected. A deleted record is already left out, because both engines re-read or check its hash.

**PR 3** (2026-10-21, branch `sg-32-memory-helper`): the embedding call, the helper and hosting, both engines. **SG-32 is complete.** Shared cases: `schema/fixtures/ai/memory-cases.json` (11 cases: labels, where the tenant comes from, one call for many entries, failures, refusals), replayed by `strata-ai/tests/memory_cases.rs` and `AI/MemoryCaseTests.cs`. Where PR 3's build diverges from or completes the text above:

- **.NET names the helper `StrataMemory`** (options `StrataMemoryOptions`), not `Memory`. A type named `Memory` would be hidden by the namespace `Datalium.Strata.Memory` in every file under `Datalium.Strata.*`, hosting and the tests included. The AI package's other .NET types are `StrataChat` and `StrataAgent`. Rust keeps `Memory`, as its agent is `Agent`.
- **The embedding call:**
  - With no inputs it makes no call and records no event.
  - It checks the answer: fewer or more vectors than inputs is a fatal `invalid_response` (`expected 2 vectors, the embedder returned 1`).
  - It records input tokens only, the answer's model, and no finish reason.
  - It never streams.
  - .NET shares the fault handling with `StrataChat` (an internal `StrataChat.Settle`), and adds `StrataEmbeddings.Operation` (`embeddings`).
- **`OpenAiEmbedder`:**
  - It orders vectors by each item's `index`. A missing, duplicate or out-of-range index, or an embedding that isn't an array of numbers, is a fatal `invalid_response`.
  - It shares the chat client's request builder (a crate-private `post`), so headers, the key, timeouts and fault mapping are the chat client's.
  - Fixtures: `strata-ai/tests/fixtures/openai/embeddings-request.json` and `embeddings-response.json`.
- **Labels:**
  - The helper's `Model` option is used as given. Otherwise the label is `<provider>/<model>` from the embedder's **default** model, not the model the answer names, since a server may report a name other than the one asked for.
  - An empty or missing provider is `unknown`.
  - An empty or whitespace label is refused before any I/O with the store's own `the model is empty` (.NET's constructor; Rust each call). An empty default model counts as none, and an empty `Strata:Memory:Model` as unset.
  - With no model at all, .NET's constructor refuses (`the embedding generator names no model: label the memory's vectors with StrataMemoryOptions.Model`). Rust's `Memory::new` can't fail before `with_model`, so each call refuses before any I/O (`the embedder names no model: label the memory's vectors with with_model`). The helper asks the embedder for no model and no dimensions.
- **Checks before the call.** The helper checks keys, metadata and search options with the store's own messages before it embeds anything, so bad input never costs a model call. For this the engine's validators became public:
  - .NET: `MemoryRecords.CheckMetadata`, and the new `MemoryRecords.CheckFilters(MemorySearch)`.
  - Rust: `validate_scope`, `validate_metadata` and `MemorySearch::validate_filters`. `validate` calls them in the same order as before.
- **Rust's errors** are `MemoryError::{Embedding(NodeError), Store(MemoryStoreError)}`. `From<MemoryError> for NodeError` passes the embedding failure through and makes a store failure fatal. The context argument takes `&ctx` or `None`. `Memory` also exposes `embedder()` and `label()`.
- **.NET hosting references `Datalium.Strata.AI`** (settled with Nick), so `AddMemory()` is an `IStrataBuilder` method; the package set stays at five. **`MemoryIndex` moved** from `Datalium.Strata.Storage.Postgres` to `Datalium.Strata.Storage` before anything was published (settled with Nick).
- **.NET hosting's settings:**
  - `StrataMemoryHostOptions` is bound from `Strata:Memory`: `Dimensions`, `Index` (`Kind` `None`/`Hnsw`, `M`, `EfConstruction`), `Table`, `Prefix` (default `Strata:Redis:Prefix`), `Extension`, `Model` and `TenantKey`.
  - The retention driver has its own `MemoryRetentionOptions`, bound from `Strata:Memory:Retention` like `RetentionOptions`: `Interval` 1 h, `BatchSize` 1,000, `OlderThan` required, `Namespace`, `Tenant`.
  - A Postgres memory store with `AutoMigrate` off is checked at start-up by a probe read (`GetAsync` of key `strata.health`), which prepares it without migrating.
  - An empty `Prefix` falls back to the Redis store's prefix, as an unset one does (Rust too).
  - The memory retention driver's start-up check also validates `Namespace` and `Tenant` with the purge's own rules, so a bad one stops the host rather than failing every round (Rust too).
  - The SQLite memory store opens `Strata:Sqlite:Path` as a plain path, so disposing the container releases the file.
- **Rust hosting:**
  - Settings: `MemorySettings { dimensions, index: MemoryIndexSettings { kind, m, ef_construction }, table, prefix, retention: MemoryRetentionDriver }`. There is no `extension` (sqlite-vec is compiled in), and no `model` or `tenant_key`: `strata-host` doesn't depend on `strata-ai`, so the application builds its `Memory`.
  - `dimensions` 0 is refused like a missing one. Over 16,000, `open_in_memory` returns the stores' refusal rather than panicking.
  - The index kind is matched without regard to case, as .NET's enum binds. An unknown kind is refused by every `open_memory`, including the stores that ignore it.
  - `Host::retain_memory` accepts `Arc<dyn MemoryRetention>`.
  - `PostgresSettings::memory_options` and `RedisSettings::memory_options` build a store's options from the settings, as `options()` does for the checkpoint stores; `open_memory` uses them.
- **No health check for the memory store.** Start-up proves it once; `strata-checkpoints` stays the only store probe.
- **Fixed on the way:** `datalium-strata-ai` built without its `anthropic` feature failed, because the Anthropic client's `pub use` had no `cfg` and the agent's had the one meant for it.
- **Not verified locally:** the Postgres and Redis hosting tests and the live embedding tests (no servers or keys here). The server tests first run in CI.

---

## Not built (deliberately out of scope)

- **A history port that holds tool calls** (design/44 review decision 9; settled 2026-10-21 as separate).
- **CLI commands** (`strata memory list` / `search`): later, on the pinned layouts.
- **Recall across tenants**, and sharing a record between tenants.
- **Hybrid search** (vector plus full text), re-ranking, and maximal marginal relevance.
- **Re-embedding a namespace** when the model changes: the application's loop over `ListAsync`, with `Text` kept for it.
- **Memory in the Strata Cloud store**: a Cloud decision. Cloud has no memory routes.
- **A memory tool for agents** (review decision 7).
- **Summarizing or consolidating memories** (an agent's job, not a store's).
- **RocksDB, Qdrant, pgvecto.rs or other vector stores**: an application can implement `IMemoryStore` / `MemoryStore`, and the shared cases tell it whether it behaves.
