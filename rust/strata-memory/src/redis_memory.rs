//! The Redis long-term memory store (`design/45-Long-Term-Memory.md`,
//! "Redis"): a hash per record and one Query Engine index, searched by a
//! hybrid query (the filters, then the nearest vectors) and ranked again
//! exactly. Needs a server with the Query Engine: Redis 8, or Redis Stack.
//!
//! **Layout** (`schema/fixtures/memory/redis-record.json`), with `{p}` the
//! prefix and each key part escaped (`%` to `%25`, then `:` to `%3A`):
//!
//! | Key | Content |
//! |---|---|
//! | `{p}:memory:records:{namespace}:{tenant}:{id}` | the record: `namespace`, `tenant`, `id`, `model`, `embedding` (little-endian float32 bytes), `content` and `text` (absent when none), `metadata` (a compact JSON object, keys in byte order), `created_at`, `updated_at` (RFC 3339, whole seconds), and the indexed tags `_scope`, `_id`, `_meta` |
//! | `{p}:memory:ids:{namespace}:{tenant}` | each id of a namespace and tenant at score 0, so a listing reads it in byte order with `ZRANGEBYLEX` |
//! | `{p}:memory:updated` | each record's key, scored by its `updated_at` text's digits, so a purge reads it oldest first |
//! | `{p}:memory:schema` | `version` (the `memory` component's, 1) and `dimensions` |
//! | `{p}:memory:idx` | the index over `{p}:memory:records:` |
//!
//! **Tags** are `x` and the lowercase hex of a value's UTF-8 bytes, so a query
//! needs no escaping and no tag is empty, folded or split: `_scope` is the
//! namespace's, tenant's and model's tags together, `_id` the id's, `_meta`
//! one key-and-value tag per metadata pair, joined by `,`.
//!
//! **Searching.** The Query Engine's hybrid query applies every filter before
//! the nearest neighbours are taken (`FLAT`, exact, by default;
//! [`MemoryIndex::Hnsw`] for an `HNSW` vector field). Its distances are
//! float32, so the store takes twice the limit's candidates (more while the
//! last candidate ties the limit-th), reads each one's hash, and ranks them
//! again by cosine similarity in f64 from the stored vectors, ties by id, as
//! the in-memory store does.
//!
//! **Writes.** An upsert is one script per record (it keeps `created_at`,
//! replaces the hash and both sorted sets' entries atomically); an
//! [`upsert_many`](MemoryStore::upsert_many) checks every record first and is
//! then one script per record, **not atomic across records** (a script over
//! many hashes would block the server through a large sync). A delete is one
//! script; a purge deletes each record with it, only if it is still old
//! enough.
//!
//! **An upsert returns once the index has the records it wrote.** A Query
//! Engine with worker threads indexes a hash after the write that made it
//! returns, so a search right after an upsert could miss the record. After
//! its scripts, an upsert asks the index for each record it wrote, by its
//! scope, id and metadata tags and nearest its own vector, until the reply
//! has the record at a cosine distance of at most `1e-4` (its current vector,
//! not one it replaced): every record not yet confirmed again, in one
//! pipeline, after a wait of 1 ms doubling to 50 ms. Past 5 s from the end of
//! the write (on tokio's clock, not the store's) it fails; the record is
//! written, and an upsert made again rewrites it, keeping `created_at`.
//!
//! **Shared with the Redis checkpoint store:** the connection manager (a
//! multiplexed connection that replaces itself when it drops), the
//! [`StoreRetry`] every call runs under, with its transient classification
//! (a retry is counted as `strata.store.operation` `memory`), and the default
//! prefix. Every call is idempotent and is made again whole after a transient
//! failure, except a purge, whose reads and deletes are each repeated rather
//! than the whole purge. A delete whose reply was lost and which is repeated
//! finds the record gone and reports `false`, as with any store retry: a
//! purge then counts it as not deleted and may delete beyond its limit by
//! that many. Mirrors the .NET `RedisMemoryStore`; both write the same keys.

use std::collections::{BTreeMap, HashMap};

use datalium_strata::BoxFuture;
use datalium_strata::memory::{
    MAX_DIMENSIONS, MemoryKey, MemoryMatch, MemoryPage, MemoryPurge, MemoryQuery, MemoryRecord,
    MemoryRetention, MemorySearch, MemoryStore, MemoryStoreError, cosine_similarity,
    metadata_matches,
};
use datalium_strata::store::timestamp_text;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use std::sync::LazyLock;
use std::time::Duration;

use crate::memory::{
    MemoryIndex, StoredRecord, content_text, metadata_text, observed, record_search_results,
    vector_bytes,
};
use crate::redis_store::{self, DEFAULT_PREFIX};
use crate::retry::{Failure, Retrier, StoreRetry};
use crate::schema_versions;
use crate::support::{Clock, system_clock};

/// The store's name on its metrics.
const STORE: &str = "redis";

/// What a retry of any of its calls is counted as.
const OPERATION: &str = "memory";

/// The most candidates one search asks the index for.
pub const MAX_CANDIDATES: usize = 10_000;

/// How close two float32 distances are taken to tie.
const TIE: f64 = 1e-5;

/// What an open says to a server without the Query Engine.
pub const QUERY_ENGINE_MISSING: &str =
    "memory on Redis needs the Query Engine (Redis 8 or Redis Stack): FT.CREATE is unknown";

/// Writes a record, keeping when it was created. Identical to
/// `RedisMemoryStore.UpsertScript` in `Datalium.Strata.Storage`.
///
/// `KEYS`: the record's hash, its namespace and tenant's ids set, the updated
/// set. `ARGV`: the id, the time now (the `created_at` of a new record), the
/// `updated_at` score, then the hash's other fields and values.
pub const UPSERT_SCRIPT: &str = r"local created = redis.call('HGET', KEYS[1], 'created_at') or ARGV[2]
redis.call('DEL', KEYS[1])
redis.call('HSET', KEYS[1], 'created_at', created, unpack(ARGV, 4))
redis.call('ZADD', KEYS[2], 0, ARGV[1])
redis.call('ZADD', KEYS[3], ARGV[3], KEYS[1])
return 1";

/// Deletes a record and its sorted sets' entries; returns 1 if the hash
/// existed. Identical to `RedisMemoryStore.DeleteScript`.
///
/// `KEYS` as [`UPSERT_SCRIPT`]'s. `ARGV`: the id, then a cutoff (RFC 3339) or
/// `''`: with a cutoff, a record whose `updated_at` isn't before it (written
/// again since a purge read it) is kept, and 0 returned.
pub const DELETE_SCRIPT: &str = r"if ARGV[2] ~= '' then
  local updated = redis.call('HGET', KEYS[1], 'updated_at')
  if not updated or updated >= ARGV[2] then return 0 end
end
local existed = redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], KEYS[1])
return existed";

static UPSERT: LazyLock<redis::Script> = LazyLock::new(|| redis::Script::new(UPSERT_SCRIPT));
static DELETE: LazyLock<redis::Script> = LazyLock::new(|| redis::Script::new(DELETE_SCRIPT));

/// How many members of a sorted set a listing or a purge reads at a time.
const PAGE: usize = 500;

/// How long an upsert waits, from the end of its write, for the index to have
/// every record it wrote.
const INDEXED_WITHIN: Duration = Duration::from_secs(5);

/// The wait before an upsert asks the index for its records a second time.
const FIRST_CONFIRM_WAIT: Duration = Duration::from_millis(1);

/// The longest wait between two of an upsert's confirmation rounds.
const MAX_CONFIRM_WAIT: Duration = Duration::from_millis(50);

/// The farthest a confirmation may find a record from its own vector: a
/// float32 cosine distance of the vector to itself, not to one it replaced.
const CONFIRMED_DISTANCE: f64 = 1e-4;

/// Options of a [`RedisMemoryStore`]. Mirrors the .NET `RedisMemoryOptions`.
#[derive(Clone)]
pub struct RedisMemoryOptions {
    dimensions: usize,
    prefix: String,
    index: MemoryIndex,
    retry: StoreRetry,
    clock: Clock,
}

impl std::fmt::Debug for RedisMemoryOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisMemoryOptions")
            .field("dimensions", &self.dimensions)
            .field("prefix", &self.prefix)
            .field("index", &self.index)
            .field("retry", &self.retry)
            .finish_non_exhaustive()
    }
}

impl RedisMemoryOptions {
    /// A store of vectors of `dimensions` (1 to [`MAX_DIMENSIONS`]) under the
    /// checkpoint store's default prefix, `strata`, with a `FLAT` (exact)
    /// index.
    pub fn new(dimensions: usize) -> Self {
        Self {
            dimensions,
            prefix: DEFAULT_PREFIX.to_string(),
            index: MemoryIndex::None,
            retry: StoreRetry::new(),
            clock: system_clock(),
        }
    }

    /// The key namespace: keys are `{prefix}:memory:…`. Not empty. Two
    /// embedding sizes are two stores on two prefixes.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// The index's vector field: [`MemoryIndex::None`], `FLAT` and exact, by
    /// default, or `HNSW`. Chosen when the index is created: an existing
    /// index keeps the kind it was created with.
    pub fn with_index(mut self, index: MemoryIndex) -> Self {
        self.index = index;
        self
    }

    /// How a call that failed for a transient reason is repeated.
    /// [`StoreRetry::none`] turns it off.
    pub fn with_retry(mut self, retry: StoreRetry) -> Self {
        self.retry = retry;
        self
    }

    /// Stamps records on `clock` instead of the system clock: how a test makes
    /// time pass.
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn index(&self) -> MemoryIndex {
        self.index
    }

    /// Refuses options no store can open with, before anything connects.
    fn check(&self) -> Result<(), MemoryStoreError> {
        if !(1..=MAX_DIMENSIONS).contains(&self.dimensions) {
            return Err(MemoryStoreError(format!(
                "a memory store has 1 to {MAX_DIMENSIONS} dimensions, not {}",
                self.dimensions
            )));
        }
        if self.prefix.is_empty() {
            return Err(MemoryStoreError("the memory prefix is empty".to_string()));
        }
        Ok(())
    }
}

/// A key part with `%` and `:` escaped, in that order, so `:` only ever
/// separates parts.
fn escape(part: &str) -> String {
    part.replace('%', "%25").replace(':', "%3A")
}

/// A value as a tag: `x` and the lowercase hex of its UTF-8 bytes.
fn tag(value: &str) -> String {
    let mut tag = String::with_capacity(1 + 2 * value.len());
    tag.push('x');
    for byte in value.bytes() {
        tag.push_str(&format!("{byte:02x}"));
    }
    tag
}

/// The `_scope` tag of a namespace, tenant and model.
fn scope_tag(namespace: &str, tenant: &str, model: &str) -> String {
    format!("{}{}{}", tag(namespace), tag(tenant), tag(model))
}

/// The tag of one metadata pair.
fn pair_tag(key: &str, value: &str) -> String {
    format!("{}{}", tag(key), tag(value))
}

/// The `_meta` field: each pair's tag, keys in byte order, joined by `,`;
/// `None` without metadata.
fn meta_tags(metadata: &BTreeMap<String, String>) -> Option<String> {
    if metadata.is_empty() {
        return None;
    }
    Some(
        metadata
            .iter()
            .map(|(key, value)| pair_tag(key, value))
            .collect::<Vec<_>>()
            .join(","),
    )
}

/// The `updated` set's score of a stored timestamp: its digits, as a number
/// (`2026-10-21T14:05:47Z` scores `20261021140547`).
fn score(timestamp: &str) -> u64 {
    timestamp
        .chars()
        .filter(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .unwrap_or(0)
}

/// `FT.CREATE`'s arguments after the command name
/// (`schema/fixtures/memory/redis-record.json`, `ft_create` and
/// `ft_create_hnsw`).
fn ft_create_args(prefix: &str, dimensions: usize, index: MemoryIndex) -> Vec<String> {
    let mut args: Vec<String> = [
        format!("{prefix}:memory:idx"),
        "ON".into(),
        "HASH".into(),
        "PREFIX".into(),
        "1".into(),
        format!("{prefix}:memory:records:"),
        "SCHEMA".into(),
        "_scope".into(),
        "TAG".into(),
        "_id".into(),
        "TAG".into(),
        "_meta".into(),
        "TAG".into(),
        "embedding".into(),
        "VECTOR".into(),
    ]
    .into();
    let vector = [
        "TYPE".to_string(),
        "FLOAT32".into(),
        "DIM".into(),
        dimensions.to_string(),
        "DISTANCE_METRIC".into(),
        "COSINE".into(),
    ];
    match index {
        MemoryIndex::None => {
            args.extend(["FLAT".to_string(), "6".into()]);
            args.extend(vector);
        }
        MemoryIndex::Hnsw { m, ef_construction } => {
            args.extend(["HNSW".to_string(), "10".into()]);
            args.extend(vector);
            args.extend([
                "M".to_string(),
                m.to_string(),
                "EF_CONSTRUCTION".into(),
                ef_construction.to_string(),
            ]);
        }
    }
    args
}

/// What an `FT.CREATE` that failed with `message` means.
#[derive(Debug, PartialEq, Eq)]
enum CreateRefusal {
    /// The index is there already: fine.
    Exists,
    /// The server has no Query Engine.
    Unknown,
    /// Anything else: the call's failure.
    Other,
}

/// What an `FT.CREATE` that failed with `err` means, read from the server's
/// reply as sent (`Index already exists` has no error code, so the client
/// takes `Index` for one).
fn create_refusal(err: &redis::RedisError) -> CreateRefusal {
    let reply = match (err.code(), err.detail()) {
        (Some(code), Some(detail)) => format!("{code} {detail}"),
        (Some(code), None) => code.to_string(),
        _ => err.to_string(),
    };
    refusal_of(&reply)
}

fn refusal_of(reply: &str) -> CreateRefusal {
    let message = reply.to_ascii_lowercase();
    if message.contains("index already exists") {
        CreateRefusal::Exists
    } else if message.contains("unknown command") {
        CreateRefusal::Unknown
    } else {
        CreateRefusal::Other
    }
}

/// A search's hybrid query: its filters, then its nearest `$k`.
fn search_query(search: &MemorySearch) -> String {
    let mut query = format!(
        "(@_scope:{{{}}}",
        scope_tag(&search.namespace, &search.tenant, &search.embedding.model)
    );
    if !search.exclude.is_empty() {
        let excluded: Vec<String> = search.exclude.iter().map(|id| tag(id)).collect();
        query.push_str(&format!(" -@_id:{{{}}}", excluded.join("|")));
    }
    for (key, value) in &search.metadata {
        query.push_str(&format!(" @_meta:{{{}}}", pair_tag(key, value)));
    }
    query.push_str(")=>[KNN $k @embedding $vec AS __distance]");
    query
}

/// The query that confirms the index has a record as it was written: its
/// scope, its id and each of its metadata pairs (the tags a search filters
/// by), nearest its own vector.
fn confirm_query(record: &MemoryRecord) -> String {
    let mut query = format!(
        "(@_scope:{{{}}} @_id:{{{}}}",
        scope_tag(&record.namespace, &record.tenant, &record.embedding.model),
        tag(&record.id)
    );
    for (key, value) in &record.metadata {
        query.push_str(&format!(" @_meta:{{{}}}", pair_tag(key, value)));
    }
    query.push_str(")=>[KNN 1 @embedding $vec AS __distance]");
    query
}

/// The wait after confirmation round `round` (from 0) before the next: 1 ms,
/// doubling, at most 50 ms.
fn confirm_wait(round: u32) -> Duration {
    FIRST_CONFIRM_WAIT
        .saturating_mul(2u32.saturating_pow(round))
        .min(MAX_CONFIRM_WAIT)
}

/// Whether a confirmation's candidates have the record at `key`, at its own
/// vector.
fn confirmed(found: &[(String, f64)], key: &str) -> bool {
    found
        .iter()
        .any(|(found, distance)| found == key && *distance <= CONFIRMED_DISTANCE)
}

/// What an upsert says of a record the index didn't have in time.
fn not_indexed(id: &str) -> String {
    format!(
        "memory on Redis didn't index record '{id}' within {} s of writing it",
        INDEXED_WITHIN.as_secs()
    )
}

/// A reply's text, whatever the protocol made of it.
fn text_of(value: &redis::Value) -> Option<String> {
    match value {
        redis::Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
        redis::Value::SimpleString(text) => Some(text.clone()),
        redis::Value::VerbatimString { text, .. } => Some(text.clone()),
        redis::Value::Int(number) => Some(number.to_string()),
        redis::Value::Double(number) => Some(number.to_string()),
        redis::Value::Okay => Some("OK".to_string()),
        redis::Value::Attribute { data, .. } => text_of(data),
        _ => None,
    }
}

fn number_of(value: &redis::Value) -> Option<f64> {
    match value {
        redis::Value::Double(number) => Some(*number),
        redis::Value::Int(number) => Some(*number as f64),
        other => text_of(other)?.parse().ok(),
    }
}

/// A map's or a flat array's pairs.
fn pairs_of(value: &redis::Value) -> Vec<(&redis::Value, &redis::Value)> {
    match value {
        redis::Value::Map(pairs) => pairs.iter().map(|(k, v)| (k, v)).collect(),
        redis::Value::Array(items) => items
            .as_chunks::<2>()
            .0
            .iter()
            .map(|[key, value]| (key, value))
            .collect(),
        redis::Value::Attribute { data, .. } => pairs_of(data),
        _ => Vec::new(),
    }
}

fn field<'a>(value: &'a redis::Value, name: &str) -> Option<&'a redis::Value> {
    pairs_of(value)
        .into_iter()
        .find(|(key, _)| text_of(key).as_deref() == Some(name))
        .map(|(_, value)| value)
}

/// The candidates of an `FT.SEARCH … RETURN 1 __distance` reply, in its
/// order: each record's key and distance. RESP2 is an array (the total, then
/// each key and its fields); RESP3 a map whose `results` hold each `id` and
/// its `extra_attributes`.
fn search_reply(reply: &redis::Value) -> Result<Vec<(String, f64)>, Failure> {
    let bad = || Failure::fatal("memory search failed: FT.SEARCH gave a reply it doesn't give");
    let candidate = |key: &redis::Value, fields: &redis::Value| -> Result<(String, f64), Failure> {
        let key = text_of(key).ok_or_else(bad)?;
        // A record deleted while the query ran may come back without its
        // fields: its hash is gone too, so it is dropped when it is read.
        let distance = field(fields, "__distance")
            .and_then(number_of)
            .unwrap_or(f64::INFINITY);
        Ok((key, distance))
    };
    match reply {
        redis::Value::Attribute { data, .. } => search_reply(data),
        redis::Value::Array(items) => items
            .get(1..)
            .unwrap_or_default()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|[key, fields]| candidate(key, fields))
            .collect(),
        redis::Value::Map(_) => match field(reply, "results") {
            Some(redis::Value::Array(results)) => results
                .iter()
                .map(|result| {
                    let key = field(result, "id").ok_or_else(bad)?;
                    let fields = field(result, "extra_attributes").ok_or_else(bad)?;
                    candidate(key, fields)
                })
                .collect(),
            _ => Err(bad()),
        },
        _ => Err(bad()),
    }
}

/// A `WITHSCORES` reply's members and scores: flat in RESP2, pairs in RESP3.
fn scored_members(reply: &redis::Value) -> Result<Vec<(String, f64)>, Failure> {
    let bad = || Failure::fatal("memory purge failed: ZRANGEBYSCORE gave a reply it doesn't give");
    let redis::Value::Array(items) = reply else {
        return Err(bad());
    };
    let pair = |member: &redis::Value, score: &redis::Value| -> Result<(String, f64), Failure> {
        Ok((
            text_of(member).ok_or_else(bad)?,
            number_of(score).ok_or_else(bad)?,
        ))
    };
    if items
        .iter()
        .all(|item| matches!(item, redis::Value::Array(_)))
    {
        items
            .iter()
            .map(|item| match item {
                redis::Value::Array(pair_items) if pair_items.len() == 2 => {
                    pair(&pair_items[0], &pair_items[1])
                }
                _ => Err(bad()),
            })
            .collect()
    } else {
        items
            .as_chunks::<2>()
            .0
            .iter()
            .map(|[member, score]| pair(member, score))
            .collect()
    }
}

/// A record's hash, as `HGETALL` gives it, as a record.
fn record_of_hash(key: &str, mut hash: HashMap<String, Vec<u8>>) -> Result<MemoryRecord, Failure> {
    let mut text = |name: &str| -> Result<Option<String>, Failure> {
        hash.remove(name)
            .map(|bytes| {
                String::from_utf8(bytes).map_err(|_| {
                    Failure::fatal(format!(
                        "memory record '{key}' has a {name} that isn't text"
                    ))
                })
            })
            .transpose()
    };
    let required = |value: Option<String>, name: &str| {
        value.ok_or_else(|| Failure::fatal(format!("memory record '{key}' has no {name}")))
    };
    let stored = StoredRecord {
        namespace: required(text("namespace")?, "namespace")?,
        tenant: required(text("tenant")?, "tenant")?,
        id: required(text("id")?, "id")?,
        model: required(text("model")?, "model")?,
        content: text("content")?,
        text: text("text")?,
        metadata: text("metadata")?.unwrap_or_else(|| "{}".to_string()),
        created_at: required(text("created_at")?, "created_at")?,
        updated_at: required(text("updated_at")?, "updated_at")?,
        embedding: hash
            .remove("embedding")
            .ok_or_else(|| Failure::fatal(format!("memory record '{key}' has no embedding")))?,
    };
    Ok(stored.into_record()?)
}

/// A record's hash fields and values, `created_at` left to the script.
fn hash_fields(record: &MemoryRecord, now: &str) -> Vec<(&'static str, Vec<u8>)> {
    let mut fields: Vec<(&'static str, Vec<u8>)> = vec![
        ("namespace", record.namespace.clone().into_bytes()),
        ("tenant", record.tenant.clone().into_bytes()),
        ("id", record.id.clone().into_bytes()),
        ("model", record.embedding.model.clone().into_bytes()),
        ("embedding", vector_bytes(&record.embedding.vector)),
    ];
    if let Some(content) = content_text(&record.content) {
        fields.push(("content", content.into_bytes()));
    }
    if let Some(text) = &record.text {
        fields.push(("text", text.clone().into_bytes()));
    }
    fields.push(("metadata", metadata_text(&record.metadata).into_bytes()));
    fields.push(("updated_at", now.as_bytes().to_vec()));
    fields.push((
        "_scope",
        scope_tag(&record.namespace, &record.tenant, &record.embedding.model).into_bytes(),
    ));
    fields.push(("_id", tag(&record.id).into_bytes()));
    if let Some(meta) = meta_tags(&record.metadata) {
        fields.push(("_meta", meta.into_bytes()));
    }
    fields
}

/// The Redis [`MemoryStore`] and [`MemoryRetention`]
/// (`design/45-Long-Term-Memory.md`). See the module's documentation.
pub struct RedisMemoryStore {
    conn: ConnectionManager,
    prefix: String,
    dimensions: usize,
    index: MemoryIndex,
    retry: Retrier,
    clock: Clock,
}

/// A record to upsert, made once for every attempt.
struct Upsert {
    key: String,
    ids: String,
    id: String,
    fields: Vec<(&'static str, Vec<u8>)>,
    /// The query that confirms the index has it ([`confirm_query`]).
    confirm: String,
    /// Its vector's bytes, the confirmation's `$vec`.
    vector: Vec<u8>,
}

impl RedisMemoryStore {
    /// Connects to `url` and opens the store, in this order: refuses a prefix
    /// a newer build moved on or made with other dimensions; records the
    /// `memory` component's version and the dimensions (`HSETNX`) and checks
    /// them again, so of two stores opening an empty prefix with other
    /// dimensions one is refused; then creates the index when it is missing,
    /// refusing a server without the Query Engine. On such a server (Redis 7)
    /// the refused open leaves `{prefix}:memory:schema` behind, which a later
    /// open on a server with the Query Engine accepts.
    pub async fn connect(url: &str, options: RedisMemoryOptions) -> Result<Self, MemoryStoreError> {
        options.check()?;
        let retry = Retrier::new(options.retry.clone(), "redis");
        let conn = retry
            .run(OPERATION, || async {
                redis_store::connect(url).await.map_err(Failure::from)
            })
            .await?;
        let store = Self {
            conn,
            prefix: options.prefix,
            dimensions: options.dimensions,
            index: options.index,
            retry,
            clock: options.clock,
        };
        store.retry.run(OPERATION, || store.open_once()).await?;
        Ok(store)
    }

    fn record_key(&self, namespace: &str, tenant: &str, id: &str) -> String {
        format!(
            "{}:memory:records:{}:{}:{}",
            self.prefix,
            escape(namespace),
            escape(tenant),
            escape(id)
        )
    }

    fn ids_key(&self, namespace: &str, tenant: &str) -> String {
        format!(
            "{}:memory:ids:{}:{}",
            self.prefix,
            escape(namespace),
            escape(tenant)
        )
    }

    fn updated_key(&self) -> String {
        format!("{}:memory:updated", self.prefix)
    }

    fn schema_key(&self) -> String {
        format!("{}:memory:schema", self.prefix)
    }

    /// The index's name.
    pub fn index_name(&self) -> String {
        format!("{}:memory:idx", self.prefix)
    }

    /// Refuses a prefix whose schema hash says a newer build or other
    /// dimensions.
    fn check_schema(&self, schema: &HashMap<String, String>) -> Result<(), Failure> {
        let known = schema_versions::MEMORY_VERSION;
        if let Some(version) = schema.get("version") {
            let found: i64 = version.parse().map_err(|_| {
                Failure::fatal(format!(
                    "{} has version '{version}', which isn't a number",
                    self.schema_key()
                ))
            })?;
            if found > known {
                return Err(Failure::fatal(schema_versions::newer(
                    schema_versions::MEMORY,
                    found,
                    known,
                )));
            }
        }
        if let Some(dimensions) = schema.get("dimensions")
            && dimensions != &self.dimensions.to_string()
        {
            return Err(Failure::fatal(format!(
                "memory was created with {dimensions} dimensions, and this store was opened with {}",
                self.dimensions
            )));
        }
        Ok(())
    }

    /// One attempt of opening the store.
    async fn open_once(&self) -> Result<(), Failure> {
        let mut conn = self.conn.clone();
        let schema: HashMap<String, String> = conn.hgetall(self.schema_key()).await?;
        self.check_schema(&schema)?;
        // The dimensions are claimed before the index is made: two stores
        // opening an empty prefix with other dimensions race on `HSETNX`, and
        // the loser is refused before it can accept the winner's index as its
        // own ("already exists") and write vectors it would never index.
        let _: bool = conn
            .hset_nx(
                self.schema_key(),
                "version",
                schema_versions::MEMORY_VERSION,
            )
            .await?;
        let _: bool = conn
            .hset_nx(self.schema_key(), "dimensions", self.dimensions)
            .await?;
        // Another store may have opened the prefix at the same time.
        let schema: HashMap<String, String> = conn.hgetall(self.schema_key()).await?;
        self.check_schema(&schema)?;
        let created: Result<(), redis::RedisError> = redis::cmd("FT.CREATE")
            .arg(ft_create_args(&self.prefix, self.dimensions, self.index))
            .query_async(&mut conn)
            .await;
        match created {
            Ok(()) => Ok(()),
            Err(err) => match create_refusal(&err) {
                CreateRefusal::Exists => Ok(()),
                CreateRefusal::Unknown => Err(Failure::fatal(QUERY_ENGINE_MISSING)),
                CreateRefusal::Other => Err(Failure::from(err)),
            },
        }
    }

    async fn upsert_rows(&self, records: Vec<MemoryRecord>) -> Result<(), MemoryStoreError> {
        for record in &records {
            record.validate(self.dimensions)?;
        }
        if records.is_empty() {
            return Ok(());
        }
        // One time for every attempt, so a repeated attempt writes the same.
        let now = timestamp_text((self.clock)());
        let updated = score(&now);
        let upserts: Vec<Upsert> = records
            .iter()
            .map(|record| Upsert {
                key: self.record_key(&record.namespace, &record.tenant, &record.id),
                ids: self.ids_key(&record.namespace, &record.tenant),
                id: record.id.clone(),
                fields: hash_fields(record, &now),
                confirm: confirm_query(record),
                vector: vector_bytes(&record.embedding.vector),
            })
            .collect();
        observed(STORE, "upsert", async {
            self.retry
                .run(OPERATION, || async {
                    let mut conn = self.conn.clone();
                    for upsert in &upserts {
                        let mut invocation = UPSERT.prepare_invoke();
                        invocation
                            .key(&upsert.key)
                            .key(&upsert.ids)
                            .key(self.updated_key())
                            .arg(&upsert.id)
                            .arg(&now)
                            .arg(updated);
                        for (name, value) in &upsert.fields {
                            invocation.arg(*name).arg(&value[..]);
                        }
                        let _: i64 = invocation.invoke_async(&mut conn).await?;
                    }
                    Ok(())
                })
                .await?;
            self.until_indexed(&upserts).await
        })
        .await?;
        Ok(())
    }

    /// Waits until the index has every record `upserts` wrote, as written
    /// (see the module's documentation), or fails past [`INDEXED_WITHIN`].
    /// Each round's queries are repeated under the store's retry policy.
    async fn until_indexed(&self, upserts: &[Upsert]) -> Result<(), Failure> {
        let deadline = tokio::time::Instant::now() + INDEXED_WITHIN;
        // A key written twice is confirmed at its last write only.
        let mut pending: Vec<&Upsert> = upserts
            .iter()
            .enumerate()
            .filter(|(at, upsert)| {
                upserts[at + 1..]
                    .iter()
                    .all(|later| later.key != upsert.key)
            })
            .map(|(_, upsert)| upsert)
            .collect();
        let mut round = 0;
        loop {
            let found = self
                .retry
                .run(OPERATION, || self.confirmations(&pending))
                .await?;
            pending = pending
                .into_iter()
                .zip(found)
                .filter(|(upsert, found)| !confirmed(found, &upsert.key))
                .map(|(upsert, _)| upsert)
                .collect();
            let Some(first) = pending.first() else {
                return Ok(());
            };
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(Failure::fatal(not_indexed(&first.id)));
            }
            tokio::time::sleep(confirm_wait(round).min(deadline - now)).await;
            round += 1;
        }
    }

    /// One confirmation round: each of `upserts`' [`confirm_query`], sent
    /// together in one pipeline, and the candidates each found.
    async fn confirmations(&self, upserts: &[&Upsert]) -> Result<Vec<Vec<(String, f64)>>, Failure> {
        let mut pipe = redis::pipe();
        for upsert in upserts {
            pipe.cmd("FT.SEARCH")
                .arg(self.index_name())
                .arg(&upsert.confirm)
                .arg("PARAMS")
                .arg(2)
                .arg("vec")
                .arg(&upsert.vector[..])
                .arg("RETURN")
                .arg(1)
                .arg("__distance")
                .arg("LIMIT")
                .arg(0)
                .arg(1)
                .arg("TIMEOUT")
                .arg(0)
                .arg("DIALECT")
                .arg(2);
        }
        let replies: Vec<redis::Value> = pipe.query_async(&mut self.conn.clone()).await?;
        replies.iter().map(search_reply).collect()
    }

    async fn get_row(&self, key: &MemoryKey) -> Result<Option<MemoryRecord>, MemoryStoreError> {
        key.validate()?;
        let record_key = self.record_key(&key.namespace, &key.tenant, &key.id);
        Ok(observed(
            STORE,
            "get",
            self.retry.run(OPERATION, || async {
                let hash: HashMap<String, Vec<u8>> = self.conn.clone().hgetall(&record_key).await?;
                if hash.is_empty() {
                    return Ok(None);
                }
                record_of_hash(&record_key, hash).map(Some)
            }),
        )
        .await?)
    }

    /// Deletes a record (with `cutoff`, only one last written before it).
    async fn delete_key(
        &self,
        namespace: &str,
        tenant: &str,
        id: &str,
        cutoff: &str,
    ) -> Result<bool, Failure> {
        let deleted: i64 = DELETE
            .key(self.record_key(namespace, tenant, id))
            .key(self.ids_key(namespace, tenant))
            .key(self.updated_key())
            .arg(id)
            .arg(cutoff)
            .invoke_async(&mut self.conn.clone())
            .await?;
        Ok(deleted > 0)
    }

    /// A delete whose reply was lost and which is repeated finds the record
    /// gone and reports `false`, though this call deleted it.
    async fn delete_row(&self, key: &MemoryKey) -> Result<bool, MemoryStoreError> {
        key.validate()?;
        Ok(observed(
            STORE,
            "delete",
            self.retry.run(OPERATION, || {
                self.delete_key(&key.namespace, &key.tenant, &key.id, "")
            }),
        )
        .await?)
    }

    /// The index's nearest `k` candidates for `search`, nearest first.
    async fn candidates(
        &self,
        query: &str,
        vector: &[u8],
        k: usize,
    ) -> Result<Vec<(String, f64)>, Failure> {
        let reply: redis::Value = redis::cmd("FT.SEARCH")
            .arg(self.index_name())
            .arg(query)
            .arg("RETURN")
            .arg(1)
            .arg("__distance")
            .arg("SORTBY")
            .arg("__distance")
            .arg("ASC")
            .arg("LIMIT")
            .arg(0)
            .arg(k)
            .arg("PARAMS")
            .arg(4)
            .arg("k")
            .arg(k)
            .arg("vec")
            .arg(vector)
            // No timeout: the Query Engine's default (500 ms, on-timeout
            // `RETURN`) could otherwise give a partial top K as if whole.
            .arg("TIMEOUT")
            .arg(0)
            .arg("DIALECT")
            .arg(2)
            .query_async(&mut self.conn.clone())
            .await?;
        search_reply(&reply)
    }

    /// Each of `keys`' hash, `None` for one that is gone.
    async fn hashes(&self, keys: &[String]) -> Result<Vec<Option<MemoryRecord>>, Failure> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipe = redis::pipe();
        for key in keys {
            pipe.hgetall(key);
        }
        let hashes: Vec<HashMap<String, Vec<u8>>> =
            pipe.query_async(&mut self.conn.clone()).await?;
        keys.iter()
            .zip(hashes)
            .map(|(key, hash)| {
                if hash.is_empty() {
                    Ok(None)
                } else {
                    record_of_hash(key, hash).map(Some)
                }
            })
            .collect()
    }

    async fn search_once(&self, search: &MemorySearch) -> Result<Vec<MemoryMatch>, Failure> {
        let query = search_query(search);
        let vector = vector_bytes(&search.embedding.vector);
        let limit = search.limit;
        let mut k = (2 * limit).min(MAX_CANDIDATES);
        let found = loop {
            let found = self.candidates(&query, &vector, k).await?;
            // A tie across the K-th candidate could hide a record that ranks
            // above the limit-th by id: ask for more until none can.
            if found.len() < k
                || k >= MAX_CANDIDATES
                || (found[k - 1].1 - found[limit - 1].1)
                    .abs()
                    .partial_cmp(&TIE)
                    .is_none_or(|order| order.is_gt())
            {
                break found;
            }
            k = (2 * k).min(MAX_CANDIDATES);
        };
        let keys: Vec<String> = found.into_iter().map(|(key, _)| key).collect();
        let mut matches: Vec<MemoryMatch> = self
            .hashes(&keys)
            .await?
            .into_iter()
            .flatten()
            // Written again since the index answered: checked again.
            .filter(|record| {
                record.namespace == search.namespace
                    && record.tenant == search.tenant
                    && record.embedding.model == search.embedding.model
                    && record.embedding.vector.len() == self.dimensions
                    && !search.exclude.contains(&record.id)
                    && metadata_matches(&record.metadata, &search.metadata)
            })
            .map(|record| MemoryMatch {
                similarity: cosine_similarity(&search.embedding.vector, &record.embedding.vector),
                record,
            })
            .collect();
        matches.sort_by(|a, b| {
            b.similarity
                .total_cmp(&a.similarity)
                .then_with(|| a.record.id.cmp(&b.record.id))
        });
        if let Some(min) = search.min_similarity {
            matches.retain(|found| found.similarity >= min);
        }
        matches.truncate(limit);
        Ok(matches)
    }

    async fn search_rows(
        &self,
        search: &MemorySearch,
    ) -> Result<Vec<MemoryMatch>, MemoryStoreError> {
        search.validate(self.dimensions)?;
        let matches = observed(
            STORE,
            "search",
            self.retry.run(OPERATION, || self.search_once(search)),
        )
        .await?;
        record_search_results(STORE, matches.len());
        Ok(matches)
    }

    async fn list_once(
        &self,
        query: &MemoryQuery,
        after: Option<&str>,
    ) -> Result<Vec<MemoryRecord>, Failure> {
        let ids_key = self.ids_key(&query.namespace, &query.tenant);
        let wanted = query.limit + 1;
        let chunk = wanted.max(100);
        let mut min = after.map_or_else(|| "-".to_string(), |id| format!("({id}"));
        let mut records = Vec::with_capacity(wanted);
        loop {
            let ids: Vec<String> = redis::cmd("ZRANGEBYLEX")
                .arg(&ids_key)
                .arg(&min)
                .arg("+")
                .arg("LIMIT")
                .arg(0)
                .arg(chunk)
                .query_async(&mut self.conn.clone())
                .await?;
            let keys: Vec<String> = ids
                .iter()
                .map(|id| self.record_key(&query.namespace, &query.tenant, id))
                .collect();
            for record in self.hashes(&keys).await?.into_iter().flatten() {
                if metadata_matches(&record.metadata, &query.metadata) {
                    records.push(record);
                    if records.len() == wanted {
                        return Ok(records);
                    }
                }
            }
            match ids.last() {
                Some(last) if ids.len() == chunk => min = format!("({last}"),
                _ => return Ok(records),
            }
        }
    }

    async fn list_rows(&self, query: &MemoryQuery) -> Result<MemoryPage, MemoryStoreError> {
        let after = query.validate()?;
        let records = observed(
            STORE,
            "list",
            self.retry
                .run(OPERATION, || self.list_once(query, after.as_deref())),
        )
        .await?;
        Ok(MemoryPage::of(records, query.limit))
    }

    /// Up to [`PAGE`] members of the updated set from `lower` to before
    /// `upper`, with their scores.
    async fn updated_page(
        &self,
        lower: &str,
        upper: &str,
        whole: bool,
    ) -> Result<Vec<(String, f64)>, Failure> {
        let mut command = redis::cmd("ZRANGEBYSCORE");
        command
            .arg(self.updated_key())
            .arg(lower)
            .arg(upper)
            .arg("WITHSCORES");
        if !whole {
            command.arg("LIMIT").arg(0).arg(PAGE);
        }
        let reply: redis::Value = command.query_async(&mut self.conn.clone()).await?;
        scored_members(&reply)
    }

    /// Each member's namespace, tenant, id and `updated_at`, `None` for a
    /// hash that is gone.
    async fn keys_of(
        &self,
        members: &[String],
    ) -> Result<Vec<Option<(String, String, String, String)>>, Failure> {
        let mut pipe = redis::pipe();
        for member in members {
            pipe.cmd("HMGET")
                .arg(member)
                .arg(&["namespace", "tenant", "id", "updated_at"]);
        }
        let found: Vec<Vec<Option<String>>> = pipe.query_async(&mut self.conn.clone()).await?;
        Ok(found
            .into_iter()
            .map(|fields| match fields.as_slice() {
                [Some(namespace), Some(tenant), Some(id), Some(updated)] => Some((
                    namespace.clone(),
                    tenant.clone(),
                    id.clone(),
                    updated.clone(),
                )),
                _ => None,
            })
            .collect())
    }

    async fn purge_rows(&self, purge: &MemoryPurge) -> Result<usize, MemoryStoreError> {
        purge.validate()?;
        let cutoff = purge.older_than_text();
        let upper = format!("({}", score(&cutoff));
        let purged: Result<usize, Failure> = observed(STORE, "purge", async {
            let mut lower = "-inf".to_string();
            let mut deleted = 0;
            loop {
                if deleted == purge.limit {
                    return Ok(deleted);
                }
                let page = self
                    .retry
                    .run(OPERATION, || self.updated_page(&lower, &upper, false))
                    .await?;
                if page.is_empty() {
                    return Ok(deleted);
                }
                let full = page.len() == PAGE;
                // Members by score, in the set's order.
                let mut groups: Vec<(f64, Vec<String>)> = Vec::new();
                for (member, score) in page {
                    match groups.last_mut() {
                        Some((last, members)) if *last == score => members.push(member),
                        _ => groups.push((score, vec![member])),
                    }
                }
                if full {
                    if groups.len() == 1 {
                        // One second fills the page: read all of it.
                        let at = groups[0].0.to_string();
                        let whole = self
                            .retry
                            .run(OPERATION, || self.updated_page(&at, &at, true))
                            .await?;
                        groups[0].1 = whole.into_iter().map(|(member, _)| member).collect();
                    } else {
                        // The last second may go on past the page.
                        groups.pop();
                    }
                }
                for (score, members) in groups {
                    if deleted == purge.limit {
                        return Ok(deleted);
                    }
                    let found = self.retry.run(OPERATION, || self.keys_of(&members)).await?;
                    let mut doomed: Vec<(String, String, String, String)> = found
                        .into_iter()
                        .flatten()
                        .filter(|(namespace, tenant, _, updated)| {
                            purge.namespace.as_ref().is_none_or(|n| n == namespace)
                                && purge.tenant.as_ref().is_none_or(|t| t == tenant)
                                && updated.as_str() < cutoff.as_str()
                        })
                        .map(|(namespace, tenant, id, updated)| (updated, namespace, tenant, id))
                        .collect();
                    doomed.sort();
                    for (_, namespace, tenant, id) in &doomed {
                        if self
                            .retry
                            .run(OPERATION, || {
                                self.delete_key(namespace, tenant, id, &cutoff)
                            })
                            .await?
                        {
                            deleted += 1;
                            if deleted == purge.limit {
                                return Ok(deleted);
                            }
                        }
                    }
                    lower = format!("({score}");
                }
                if !full {
                    return Ok(deleted);
                }
            }
        })
        .await;
        Ok(purged?)
    }
}

impl MemoryStore for RedisMemoryStore {
    fn dimensions(&self) -> usize {
        self.dimensions
    }

    fn upsert(&self, record: MemoryRecord) -> BoxFuture<'_, Result<(), MemoryStoreError>> {
        Box::pin(self.upsert_rows(vec![record]))
    }

    fn upsert_many(
        &self,
        records: Vec<MemoryRecord>,
    ) -> BoxFuture<'_, Result<(), MemoryStoreError>> {
        Box::pin(self.upsert_rows(records))
    }

    fn get<'a>(
        &'a self,
        key: &'a MemoryKey,
    ) -> BoxFuture<'a, Result<Option<MemoryRecord>, MemoryStoreError>> {
        Box::pin(self.get_row(key))
    }

    fn delete<'a>(&'a self, key: &'a MemoryKey) -> BoxFuture<'a, Result<bool, MemoryStoreError>> {
        Box::pin(self.delete_row(key))
    }

    fn search<'a>(
        &'a self,
        search: &'a MemorySearch,
    ) -> BoxFuture<'a, Result<Vec<MemoryMatch>, MemoryStoreError>> {
        Box::pin(self.search_rows(search))
    }

    fn list<'a>(
        &'a self,
        query: &'a MemoryQuery,
    ) -> BoxFuture<'a, Result<MemoryPage, MemoryStoreError>> {
        Box::pin(self.list_rows(query))
    }
}

impl MemoryRetention for RedisMemoryStore {
    fn purge<'a>(
        &'a self,
        purge: &'a MemoryPurge,
    ) -> BoxFuture<'a, Result<usize, MemoryStoreError>> {
        Box::pin(self.purge_rows(purge))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datalium_strata::memory::Embedding;
    use redis::Value;
    use serde_json::Value as Json;

    fn fixture() -> Json {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../schema/fixtures/memory/redis-record.json");
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    fn strings(value: &Json) -> Vec<String> {
        value
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item.as_str().unwrap().to_string())
            .collect()
    }

    fn bulk(text: &str) -> Value {
        Value::BulkString(text.as_bytes().to_vec())
    }

    #[test]
    fn key_parts_are_escaped_as_the_fixture_says() {
        let fixture = fixture();
        for case in fixture["escaped_key_parts"].as_array().unwrap() {
            assert_eq!(
                escape(case["part"].as_str().unwrap()),
                case["escaped"].as_str().unwrap()
            );
        }
        // `%` first: an escaped `:` is not escaped again.
        assert_eq!(escape("%3A"), "%253A");
    }

    #[test]
    fn tags_are_the_hex_of_the_utf8_bytes() {
        let fixture = fixture();
        let record = &fixture["record"];
        let fields = &fixture["hash"]["fields"];
        let text = |value: &Json| value.as_str().unwrap().to_string();
        assert_eq!(
            scope_tag(
                &text(&record["namespace"]),
                &text(&record["tenant"]),
                &text(&record["embedding"]["model"])
            ),
            text(&fields["_scope"])
        );
        assert_eq!(tag(&text(&record["id"])), text(&fields["_id"]));
        let metadata: BTreeMap<String, String> =
            serde_json::from_value(record["metadata"].clone()).unwrap();
        assert_eq!(meta_tags(&metadata).unwrap(), text(&fields["_meta"]));
        assert_eq!(meta_tags(&BTreeMap::new()), None);
        assert_eq!(tag(""), "x");
        assert_eq!(tag("é:"), "xc3a93a");
    }

    #[test]
    fn the_record_key_and_the_hash_fields_are_the_fixtures() {
        let fixture = fixture();
        let runner_record = &fixture["record"];
        let mut record = MemoryRecord::new(
            runner_record["namespace"].as_str().unwrap(),
            runner_record["tenant"].as_str().unwrap(),
            runner_record["id"].as_str().unwrap(),
            Embedding::new(
                runner_record["embedding"]["model"].as_str().unwrap(),
                vec![0.125, -0.5, 0.25, 0.75],
            ),
        )
        .with_content(runner_record["content"].as_object().unwrap().clone())
        .with_text(runner_record["text"].as_str().unwrap());
        record.metadata = serde_json::from_value(runner_record["metadata"].clone()).unwrap();
        let updated_at = fixture["updated_at"].as_str().unwrap();
        let expected = fixture["hash"]["fields"].as_object().unwrap();
        let fields = hash_fields(&record, updated_at);
        assert_eq!(
            fields.len(),
            expected.len() - 1,
            "every field but created_at"
        );
        for (name, value) in fields {
            let want = expected[name].as_str().unwrap();
            match name {
                "embedding" => {
                    let hex: String = value.iter().map(|b| format!("{b:02x}")).collect();
                    assert_eq!(hex, want);
                }
                "content" | "metadata" => assert_eq!(
                    serde_json::from_slice::<Json>(&value).unwrap(),
                    serde_json::from_str::<Json>(want).unwrap(),
                    "{name}"
                ),
                _ => assert_eq!(String::from_utf8(value).unwrap(), want, "{name}"),
            }
        }
        // The metadata text itself is sorted.
        assert_eq!(
            expected["metadata"].as_str().unwrap(),
            metadata_text(&record.metadata)
        );
        assert_eq!(score(updated_at), 20261021140547);
        assert_eq!(
            format!(
                "memory:records:{}:{}:{}",
                escape(&record.namespace),
                escape(&record.tenant),
                escape(&record.id)
            ),
            fixture["hash"]["key"].as_str().unwrap()
        );
    }

    #[test]
    fn ft_create_takes_the_fixtures_arguments() {
        let fixture = fixture();
        assert_eq!(
            ft_create_args("strata", 4, MemoryIndex::None),
            strings(&fixture["ft_create"])
        );
        let hnsw = ft_create_args("strata", 4, MemoryIndex::hnsw());
        let field = hnsw.iter().position(|arg| arg == "embedding").unwrap();
        assert_eq!(hnsw[field..], strings(&fixture["ft_create_hnsw"])[..]);
        assert_eq!(
            hnsw[..field],
            ft_create_args("strata", 4, MemoryIndex::None)[..field]
        );
    }

    #[test]
    fn a_server_without_the_query_engine_is_told_from_an_existing_index() {
        // The replies as the server sends them, parsed as the client does.
        let error = |reply: &str| -> redis::RedisError {
            redis::parse_redis_value(format!("-{reply}\r\n").as_bytes())
                .and_then(Value::extract_error)
                .expect_err("an error reply")
        };
        assert_eq!(
            create_refusal(&error(
                "ERR unknown command 'FT.CREATE', with args beginning with: 'strata:memory:idx' 'ON' "
            )),
            CreateRefusal::Unknown
        );
        assert_eq!(
            create_refusal(&error("Index already exists")),
            CreateRefusal::Exists
        );
        assert_eq!(
            create_refusal(&error("ERR Index already exists")),
            CreateRefusal::Exists
        );
        assert_eq!(
            create_refusal(&error(
                "NOPERM User default has no permissions to run the 'ft.create' command"
            )),
            CreateRefusal::Other
        );
        assert_eq!(
            create_refusal(&error("LOADING Redis is loading the dataset in memory")),
            CreateRefusal::Other
        );
        assert_eq!(
            QUERY_ENGINE_MISSING,
            "memory on Redis needs the Query Engine (Redis 8 or Redis Stack): FT.CREATE is unknown"
        );
    }

    #[test]
    fn a_search_query_filters_by_tag_before_its_nearest() {
        let search = MemorySearch::new("incidents", Embedding::new("m", vec![1.0, 0.0]))
            .tenant("acme")
            .exclude("b")
            .exclude("a")
            .with_metadata("service", "api");
        assert_eq!(
            search_query(&search),
            format!(
                "(@_scope:{{{}}} -@_id:{{{}|{}}} @_meta:{{{}}})=>[KNN $k @embedding $vec AS __distance]",
                scope_tag("incidents", "acme", "m"),
                tag("a"),
                tag("b"),
                pair_tag("service", "api")
            )
        );
        let plain = MemorySearch::new("n", Embedding::new("m", vec![1.0]));
        assert_eq!(
            search_query(&plain),
            format!(
                "(@_scope:{{{}}})=>[KNN $k @embedding $vec AS __distance]",
                scope_tag("n", "", "m")
            )
        );
    }

    #[test]
    fn a_confirmation_asks_for_the_record_by_its_tags_nearest_its_own_vector() {
        let record =
            MemoryRecord::new("incidents", "acme", "inc:1", Embedding::new("m", vec![1.0]))
                .with_metadata("team", "sre")
                .with_metadata("service", "api");
        assert_eq!(
            confirm_query(&record),
            format!(
                "(@_scope:{{{}}} @_id:{{{}}} @_meta:{{{}}} @_meta:{{{}}})=>[KNN 1 @embedding $vec AS __distance]",
                scope_tag("incidents", "acme", "m"),
                tag("inc:1"),
                pair_tag("service", "api"),
                pair_tag("team", "sre")
            )
        );
        let plain = MemoryRecord::new("n", "", "a", Embedding::new("m", vec![1.0]));
        assert_eq!(
            confirm_query(&plain),
            "(@_scope:{x6exx6d} @_id:{x61})=>[KNN 1 @embedding $vec AS __distance]"
        );
    }

    #[test]
    fn a_confirmation_needs_the_record_at_its_own_vector() {
        let key = "p:memory:records:n::a";
        assert!(confirmed(&[(key.to_string(), 0.0)], key));
        assert!(confirmed(&[(key.to_string(), 1e-4)], key));
        assert!(!confirmed(&[(key.to_string(), 0.01)], key), "an old vector");
        assert!(!confirmed(&[(key.to_string(), f64::INFINITY)], key));
        assert!(!confirmed(
            &[("p:memory:records:n::b".to_string(), 0.0)],
            key
        ));
        assert!(!confirmed(&[], key));
    }

    #[test]
    fn confirmations_wait_1_ms_doubling_to_50() {
        let waits: Vec<u64> = (0..10)
            .map(|round| confirm_wait(round).as_millis() as u64)
            .collect();
        assert_eq!(waits, [1, 2, 4, 8, 16, 32, 50, 50, 50, 50]);
        assert_eq!(confirm_wait(u32::MAX), MAX_CONFIRM_WAIT);
        assert_eq!(INDEXED_WITHIN, Duration::from_secs(5));
        assert_eq!(
            not_indexed("inc:1"),
            "memory on Redis didn't index record 'inc:1' within 5 s of writing it"
        );
    }

    #[test]
    fn a_resp2_search_reply_is_read() {
        let reply = Value::Array(vec![
            Value::Int(2),
            bulk("p:memory:records:n::a"),
            Value::Array(vec![bulk("__distance"), bulk("0")]),
            bulk("p:memory:records:n::b"),
            Value::Array(vec![bulk("__distance"), bulk("0.292893230915")]),
        ]);
        let found = search_reply(&reply).unwrap();
        assert_eq!(found.len(), 2);
        assert_eq!(found[0], ("p:memory:records:n::a".to_string(), 0.0));
        assert!((found[1].1 - 0.292_893_23).abs() < 1e-8);
        assert!(
            search_reply(&Value::Array(vec![Value::Int(0)]))
                .unwrap()
                .is_empty()
        );
        let gone = Value::Array(vec![Value::Int(1), bulk("k"), Value::Nil]);
        assert_eq!(
            search_reply(&gone).unwrap(),
            vec![("k".to_string(), f64::INFINITY)]
        );
    }

    #[test]
    fn a_resp3_search_reply_is_read() {
        let result = |key: &str, distance: Value| {
            Value::Map(vec![
                (Value::SimpleString("id".into()), bulk(key)),
                (
                    Value::SimpleString("extra_attributes".into()),
                    Value::Map(vec![(bulk("__distance"), distance)]),
                ),
                (Value::SimpleString("values".into()), Value::Array(vec![])),
            ])
        };
        let reply = Value::Map(vec![
            (
                Value::SimpleString("attributes".into()),
                Value::Array(vec![]),
            ),
            (
                Value::SimpleString("format".into()),
                Value::SimpleString("STRING".into()),
            ),
            (
                Value::SimpleString("results".into()),
                Value::Array(vec![
                    result("k1", bulk("0.5")),
                    result("k2", Value::Double(1.5)),
                ]),
            ),
            (Value::SimpleString("total_results".into()), Value::Int(2)),
            (Value::SimpleString("warning".into()), Value::Array(vec![])),
        ]);
        assert_eq!(
            search_reply(&reply).unwrap(),
            vec![("k1".to_string(), 0.5), ("k2".to_string(), 1.5)]
        );
        assert!(search_reply(&Value::Okay).is_err());
    }

    #[test]
    fn scored_members_are_read_flat_or_in_pairs() {
        let flat = Value::Array(vec![
            bulk("a"),
            bulk("20261021140547"),
            bulk("b"),
            bulk("2"),
        ]);
        let pairs = Value::Array(vec![
            Value::Array(vec![bulk("a"), Value::Double(20261021140547.0)]),
            Value::Array(vec![bulk("b"), Value::Double(2.0)]),
        ]);
        let expected = vec![("a".to_string(), 20261021140547.0), ("b".to_string(), 2.0)];
        assert_eq!(scored_members(&flat).unwrap(), expected);
        assert_eq!(scored_members(&pairs).unwrap(), expected);
        assert!(scored_members(&Value::Array(vec![])).unwrap().is_empty());
    }

    #[test]
    fn options_no_store_can_open_with_are_refused() {
        assert_eq!(
            RedisMemoryOptions::new(0).check().unwrap_err().0,
            "a memory store has 1 to 16000 dimensions, not 0"
        );
        assert_eq!(
            RedisMemoryOptions::new(3)
                .with_prefix("")
                .check()
                .unwrap_err()
                .0,
            "the memory prefix is empty"
        );
        let options = RedisMemoryOptions::new(768);
        assert!(options.check().is_ok());
        assert_eq!(options.prefix(), "strata");
        assert_eq!(options.index(), MemoryIndex::None);
    }
}
