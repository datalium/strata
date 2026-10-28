//! Long-term memory (`design/45-Long-Term-Memory.md`): records an application
//! stores with an embedding and finds again by meaning, from one run to the
//! next. [`MemoryStore`] is the port and [`MemoryRetention`] the purge port;
//! [`InMemoryMemoryStore`] is the adapter for tests and development, and the
//! Postgres, SQLite and Redis adapters live in `strata-memory`, as the history
//! and checkpoint adapters do.
//!
//! One generic store, not one per kind of memory: episodic, semantic and
//! procedural memory are namespaces an application uses (`incidents`,
//! `facts`, `runbooks`). A record is identified by (namespace, tenant, id),
//! and a search reads one namespace, one tenant and one embedding model, so
//! two models' vectors are never compared. The store takes vectors and never
//! calls a model.
//!
//! Every store refuses bad input the same way before any I/O, with the
//! messages the shared cases pin (`schema/fixtures/memory/search-cases.json`):
//! [`MemoryRecord::validate`], [`MemoryKey::validate`],
//! [`MemorySearch::validate`], [`MemoryQuery::validate`] and
//! [`MemoryPurge::validate`]. Mirrors the .NET `Datalium.Strata.Memory`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, SubsecRound, Utc};
use serde_json::{Map, Value};

use crate::store::BoxFuture;

/// The longest namespace, tenant or id, in bytes of UTF-8.
pub const MAX_KEY_BYTES: usize = 256;

/// The longest metadata key, in bytes of UTF-8.
pub const MAX_METADATA_KEY_BYTES: usize = 128;

/// The most dimensions a store has: pgvector's limit for a `vector` column,
/// and every store's, so a namespace can move between them.
pub const MAX_DIMENSIONS: usize = 16_000;

/// The most matches a search returns, and the most records a listing page
/// holds.
pub const MAX_LIMIT: usize = 1000;

/// What made a vector, and the vector.
#[derive(Debug, Clone, PartialEq)]
pub struct Embedding {
    /// Free text compared exactly (the helper writes `<provider>/<model>`).
    /// A search reads only records of its own model.
    pub model: String,
    /// As many finite numbers as the store's dimensions, not all zero.
    pub vector: Vec<f32>,
}

impl Embedding {
    pub fn new(model: impl Into<String>, vector: impl Into<Vec<f32>>) -> Self {
        Self {
            model: model.into(),
            vector: vector.into(),
        }
    }

    fn validate(&self, dimensions: usize) -> Result<(), MemoryStoreError> {
        if self.model.is_empty() {
            return Err(refused("the model is empty"));
        }
        if self.vector.len() != dimensions {
            return Err(refused(format!(
                "the vector has {} dimensions, and this store has {dimensions}",
                self.vector.len()
            )));
        }
        if self.vector.iter().any(|value| !value.is_finite()) {
            return Err(refused("the vector has a value that isn't finite"));
        }
        // Cosine is undefined for a vector of no length.
        if self.vector.iter().all(|value| *value == 0.0) {
            return Err(refused("the vector is all zeros"));
        }
        Ok(())
    }
}

/// A record's key: unique per store.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MemoryKey {
    pub namespace: String,
    /// `""` when the application has no tenants.
    pub tenant: String,
    pub id: String,
}

impl MemoryKey {
    pub fn new(
        namespace: impl Into<String>,
        tenant: impl Into<String>,
        id: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            tenant: tenant.into(),
            id: id.into(),
        }
    }

    /// Refuses an empty namespace or id, and a namespace, tenant or id longer
    /// than [`MAX_KEY_BYTES`].
    pub fn validate(&self) -> Result<(), MemoryStoreError> {
        check_scope(&self.namespace, &self.tenant)?;
        check_id(&self.id)
    }
}

/// One memory: the application's content, the text its vector was made from,
/// string metadata a search can filter on, and the embedding.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct MemoryRecord {
    /// The kind of memory (`incidents`, `runbooks`, `facts`). A search never
    /// crosses one.
    pub namespace: String,
    /// Whose memory it is; `""` when the application has none. A search reads
    /// one tenant only.
    pub tenant: String,
    /// Unique within (namespace, tenant).
    pub id: String,
    pub embedding: Embedding,
    /// The application's payload, never searched.
    pub content: Option<Map<String, Value>>,
    /// The text the vector was made from, kept so a namespace can be embedded
    /// again by another model. Never searched.
    pub text: Option<String>,
    /// Pairs a search and a listing filter on by equality.
    pub metadata: BTreeMap<String, String>,
    /// When the record was first written, on the store's clock at whole
    /// seconds. Set by the store (a caller's is ignored) and kept by a
    /// replacing upsert.
    pub created_at: Option<DateTime<Utc>>,
    /// When the record was last written. Set by the store; what retention
    /// purges by.
    pub updated_at: Option<DateTime<Utc>>,
}

impl MemoryRecord {
    pub fn new(
        namespace: impl Into<String>,
        tenant: impl Into<String>,
        id: impl Into<String>,
        embedding: Embedding,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            tenant: tenant.into(),
            id: id.into(),
            embedding,
            content: None,
            text: None,
            metadata: BTreeMap::new(),
            created_at: None,
            updated_at: None,
        }
    }

    pub fn with_content(mut self, content: Map<String, Value>) -> Self {
        self.content = Some(content);
        self
    }

    pub fn with_text(mut self, text: impl Into<String>) -> Self {
        self.text = Some(text.into());
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn key(&self) -> MemoryKey {
        MemoryKey::new(&self.namespace, &self.tenant, &self.id)
    }

    /// Refuses a record a store of `dimensions` can't hold: a bad key, an
    /// empty model, a vector of another length, with a value that isn't
    /// finite or all zeros, and a metadata key that is empty or longer than
    /// [`MAX_METADATA_KEY_BYTES`].
    pub fn validate(&self, dimensions: usize) -> Result<(), MemoryStoreError> {
        check_scope(&self.namespace, &self.tenant)?;
        check_id(&self.id)?;
        self.embedding.validate(dimensions)?;
        check_metadata(&self.metadata)
    }
}

/// A search: the records of one namespace, tenant and model most similar to a
/// vector, most similar first, ties by id.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct MemorySearch {
    pub namespace: String,
    /// `""` (the default) reads only tenant-less records, never every
    /// tenant's.
    pub tenant: String,
    /// The vector to compare with, and the model whose records are read.
    pub embedding: Embedding,
    /// 1 to [`MAX_LIMIT`]; 10 by default.
    pub limit: usize,
    /// Drops matches less similar than this (−1 to 1), so a search may return
    /// fewer than the limit. None by default.
    pub min_similarity: Option<f64>,
    /// Ids, within the namespace and tenant, never returned.
    pub exclude: BTreeSet<String>,
    /// Only records whose metadata has every pair.
    pub metadata: BTreeMap<String, String>,
}

impl MemorySearch {
    pub fn new(namespace: impl Into<String>, embedding: Embedding) -> Self {
        Self {
            namespace: namespace.into(),
            tenant: String::new(),
            embedding,
            limit: 10,
            min_similarity: None,
            exclude: BTreeSet::new(),
            metadata: BTreeMap::new(),
        }
    }

    pub fn tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = tenant.into();
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    pub fn min_similarity(mut self, min_similarity: f64) -> Self {
        self.min_similarity = Some(min_similarity);
        self
    }

    /// Never returns the record `id`; call again for each id.
    pub fn exclude(mut self, id: impl Into<String>) -> Self {
        self.exclude.insert(id.into());
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Refuses a search a store of `dimensions` can't run: a bad namespace or
    /// tenant, an embedding a record couldn't have, a limit outside 1 to
    /// [`MAX_LIMIT`], a minimum similarity outside −1 to 1, and a bad
    /// metadata key.
    pub fn validate(&self, dimensions: usize) -> Result<(), MemoryStoreError> {
        check_scope(&self.namespace, &self.tenant)?;
        self.embedding.validate(dimensions)?;
        self.validate_filters()
    }

    /// What [`Self::validate`] refuses but the scope and the embedding: a
    /// limit outside 1 to [`MAX_LIMIT`], a minimum similarity outside −1 to 1,
    /// and a bad metadata key. With [`MemoryKey`]'s scope check, it is how a
    /// caller that embeds the query itself (`datalium-strata-ai`'s `Memory`,
    /// `design/45-Long-Term-Memory.md`) refuses a search before the embedding
    /// call, with the store's messages.
    pub fn validate_filters(&self) -> Result<(), MemoryStoreError> {
        check_limit(self.limit)?;
        if let Some(min) = self.min_similarity
            && !(-1.0..=1.0).contains(&min)
        {
            return Err(refused(format!(
                "the minimum similarity is {min}; it must be from -1 to 1"
            )));
        }
        check_metadata(&self.metadata)
    }
}

/// A search's result.
#[derive(Debug, Clone, PartialEq)]
pub struct MemoryMatch {
    pub record: MemoryRecord,
    /// 1 − cosine distance, from −1 to 1.
    pub similarity: f64,
}

/// A listing of one namespace and tenant by id, in pages: for an application
/// that syncs a namespace, and for a later CLI.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct MemoryQuery {
    pub namespace: String,
    /// `""` by default.
    pub tenant: String,
    /// Only records whose metadata has every pair.
    pub metadata: BTreeMap<String, String>,
    /// 1 to [`MAX_LIMIT`]; 100 by default.
    pub limit: usize,
    /// Where to continue: the previous page's [`MemoryPage::next_cursor`].
    pub cursor: Option<String>,
}

impl MemoryQuery {
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            tenant: String::new(),
            metadata: BTreeMap::new(),
            limit: 100,
            cursor: None,
        }
    }

    pub fn tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = tenant.into();
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    pub fn cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }

    /// Refuses a bad namespace, tenant or metadata key, a limit outside 1 to
    /// [`MAX_LIMIT`] and a cursor that isn't one; returns the id the page
    /// continues after.
    pub fn validate(&self) -> Result<Option<String>, MemoryStoreError> {
        check_scope(&self.namespace, &self.tenant)?;
        check_metadata(&self.metadata)?;
        check_limit(self.limit)?;
        self.cursor.as_deref().map(decode_cursor).transpose()
    }
}

/// One page of [`MemoryStore::list`].
#[derive(Debug, Clone, PartialEq)]
pub struct MemoryPage {
    pub records: Vec<MemoryRecord>,
    /// Pass this as [`MemoryQuery::cursor`] for the next page; `None` at the
    /// end.
    pub next_cursor: Option<String>,
}

impl MemoryPage {
    /// The page of `records`, read with one more than `limit` to tell whether
    /// another follows.
    pub fn of(mut records: Vec<MemoryRecord>, limit: usize) -> Self {
        let more = records.len() > limit;
        records.truncate(limit);
        let next_cursor = if more {
            records.last().map(|record| encode_cursor(&record.id))
        } else {
            None
        };
        Self {
            records,
            next_cursor,
        }
    }
}

/// The position a listing continues from, the last returned record's id,
/// encoded as a thread listing's cursor is: base64url (no padding) of the
/// JSON array `[id]`, the same in both engines.
pub fn encode_cursor(id: &str) -> String {
    URL_SAFE_NO_PAD.encode(serde_json::json!([id]).to_string())
}

/// The id [`encode_cursor`] encoded.
pub fn decode_cursor(cursor: &str) -> Result<String, MemoryStoreError> {
    let bad = || refused(format!("'{cursor}' is not a memory listing cursor"));
    let bytes = URL_SAFE_NO_PAD.decode(cursor).map_err(|_| bad())?;
    let (id,): (String,) = serde_json::from_slice(&bytes).map_err(|_| bad())?;
    Ok(id)
}

/// What [`MemoryRetention::purge`] deletes: records last written before
/// `older_than`, optionally of one namespace and tenant.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct MemoryPurge {
    /// Exclusive, at the second.
    pub older_than: DateTime<Utc>,
    /// Every namespace when `None`.
    pub namespace: Option<String>,
    /// Every tenant when `None`; `Some("")` is the tenant-less records.
    pub tenant: Option<String>,
    /// At least 1; 1,000 by default.
    pub limit: usize,
}

impl MemoryPurge {
    pub fn new(older_than: DateTime<Utc>) -> Self {
        Self {
            older_than,
            namespace: None,
            tenant: None,
            limit: 1000,
        }
    }

    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Refuses an empty or too long namespace, a too long tenant and a limit
    /// of 0.
    pub fn validate(&self) -> Result<(), MemoryStoreError> {
        if let Some(namespace) = &self.namespace {
            check_scope(namespace, "")?;
        }
        if let Some(tenant) = &self.tenant {
            check_length("tenant", tenant)?;
        }
        if self.limit == 0 {
            return Err(refused("the limit is 0; it must be at least 1"));
        }
        Ok(())
    }

    /// `older_than` as stored timestamps are, at whole seconds: a record whose
    /// `updated_at` text sorts before it is purged.
    pub fn older_than_text(&self) -> String {
        crate::store::timestamp_text(self.older_than)
    }
}

/// The long-term memory port. Every method refuses bad input with a
/// [`MemoryStoreError`] before any I/O (see the module's documentation).
/// Mirrors the .NET `IMemoryStore`.
pub trait MemoryStore: Send + Sync {
    /// The length every vector the store holds or searches with has.
    fn dimensions(&self) -> usize;

    /// Writes `record`, replacing the one with its key but keeping when that
    /// was created. The store sets the timestamps.
    fn upsert(&self, record: MemoryRecord) -> BoxFuture<'_, Result<(), MemoryStoreError>>;

    /// [`Self::upsert`] for each record; every record is checked before any
    /// is written. Atomic in the SQL stores.
    fn upsert_many(
        &self,
        records: Vec<MemoryRecord>,
    ) -> BoxFuture<'_, Result<(), MemoryStoreError>>;

    fn get<'a>(
        &'a self,
        key: &'a MemoryKey,
    ) -> BoxFuture<'a, Result<Option<MemoryRecord>, MemoryStoreError>>;

    /// Whether there was a record to delete.
    fn delete<'a>(&'a self, key: &'a MemoryKey) -> BoxFuture<'a, Result<bool, MemoryStoreError>>;

    /// The records of the search's namespace, tenant and model that pass its
    /// filters, most similar first, ties by id (ordinal), up to its limit.
    /// Every filter is applied before ranking.
    fn search<'a>(
        &'a self,
        search: &'a MemorySearch,
    ) -> BoxFuture<'a, Result<Vec<MemoryMatch>, MemoryStoreError>>;

    /// One page of the query's namespace and tenant, by id (ordinal).
    fn list<'a>(
        &'a self,
        query: &'a MemoryQuery,
    ) -> BoxFuture<'a, Result<MemoryPage, MemoryStoreError>>;
}

/// Deletes records nobody has written for a while
/// (`design/45-Long-Term-Memory.md`, "Retention"). Separate from
/// [`MemoryStore`], like [`crate::store::CheckpointRetention`]. Mirrors the
/// .NET `IMemoryRetention`.
pub trait MemoryRetention: Send + Sync {
    /// Deletes records whose `updated_at` is before `purge.older_than` (at the
    /// second, exclusive), of its namespace and tenant when given, oldest
    /// first (then by namespace, tenant and id), up to its limit. Returns how
    /// many it deleted: call it again while it returns the limit.
    ///
    /// Nothing in the engine calls this.
    fn purge<'a>(
        &'a self,
        purge: &'a MemoryPurge,
    ) -> BoxFuture<'a, Result<usize, MemoryStoreError>>;
}

/// A memory store's failure, or its refusal of bad input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryStoreError(pub String);

impl std::fmt::Display for MemoryStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for MemoryStoreError {}

fn refused(message: impl Into<String>) -> MemoryStoreError {
    MemoryStoreError(message.into())
}

fn check_length(what: &str, value: &str) -> Result<(), MemoryStoreError> {
    if value.len() > MAX_KEY_BYTES {
        return Err(refused(format!(
            "the {what} is longer than {MAX_KEY_BYTES} bytes"
        )));
    }
    Ok(())
}

fn check_scope(namespace: &str, tenant: &str) -> Result<(), MemoryStoreError> {
    if namespace.is_empty() {
        return Err(refused("the namespace is empty"));
    }
    check_length("namespace", namespace)?;
    check_length("tenant", tenant)
}

fn check_id(id: &str) -> Result<(), MemoryStoreError> {
    if id.is_empty() {
        return Err(refused("the id is empty"));
    }
    check_length("id", id)
}

/// Refuses a namespace that is empty and a namespace or tenant longer than
/// [`MAX_KEY_BYTES`], as every store does: the scope of a search, or of a key
/// without its id.
pub fn validate_scope(namespace: &str, tenant: &str) -> Result<(), MemoryStoreError> {
    check_scope(namespace, tenant)
}

/// Refuses a metadata key that is empty or longer than
/// [`MAX_METADATA_KEY_BYTES`], as every store does: for a caller that checks
/// a record before it has its vector (`datalium-strata-ai`'s `Memory`,
/// `design/45-Long-Term-Memory.md`).
pub fn validate_metadata(metadata: &BTreeMap<String, String>) -> Result<(), MemoryStoreError> {
    check_metadata(metadata)
}

fn check_metadata(metadata: &BTreeMap<String, String>) -> Result<(), MemoryStoreError> {
    for key in metadata.keys() {
        if key.is_empty() {
            return Err(refused("a metadata key is empty"));
        }
        if key.len() > MAX_METADATA_KEY_BYTES {
            return Err(refused(format!(
                "the metadata key '{key}' is longer than {MAX_METADATA_KEY_BYTES} bytes"
            )));
        }
    }
    Ok(())
}

fn check_limit(limit: usize) -> Result<(), MemoryStoreError> {
    if !(1..=MAX_LIMIT).contains(&limit) {
        return Err(refused(format!(
            "the limit is {limit}; it must be from 1 to {MAX_LIMIT}"
        )));
    }
    Ok(())
}

/// The cosine similarity of two vectors of the same length, neither all
/// zeros, in f64.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
    let (mut dot, mut norm_a, mut norm_b) = (0f64, 0f64, 0f64);
    for (x, y) in a.iter().zip(b) {
        let (x, y) = (f64::from(*x), f64::from(*y));
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }
    (dot / (norm_a.sqrt() * norm_b.sqrt())).clamp(-1.0, 1.0)
}

/// Whether `metadata` has every pair of `filter`.
pub fn metadata_matches(
    metadata: &BTreeMap<String, String>,
    filter: &BTreeMap<String, String>,
) -> bool {
    filter
        .iter()
        .all(|(key, value)| metadata.get(key) == Some(value))
}

/// The clock [`InMemoryMemoryStore`] stamps records with: the system clock
/// unless [`InMemoryMemoryStore::with_clock`] gives it another, as a test
/// does. The same type as `strata-memory`'s `Clock`.
pub type MemoryClock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

/// The in-memory store's records, keyed by (namespace, tenant, id), so one
/// namespace and tenant's records are a range in id order.
type Records = BTreeMap<(String, String, String), MemoryRecord>;

/// Keeps every record in memory and searches them all with exact cosine
/// similarity in f64: for tests and development, and the reference the shared
/// cases are written against. Records are stamped on its clock at whole
/// seconds.
pub struct InMemoryMemoryStore {
    dimensions: usize,
    records: Mutex<Records>,
    clock: MemoryClock,
}

impl InMemoryMemoryStore {
    /// A store of vectors of `dimensions` (1 to [`MAX_DIMENSIONS`]).
    ///
    /// # Panics
    /// When `dimensions` is outside that range.
    pub fn new(dimensions: usize) -> Self {
        assert!(
            (1..=MAX_DIMENSIONS).contains(&dimensions),
            "a memory store has 1 to {MAX_DIMENSIONS} dimensions, not {dimensions}"
        );
        Self {
            dimensions,
            records: Mutex::default(),
            clock: Arc::new(Utc::now),
        }
    }

    /// Stamps records on `clock` instead of the system clock: how a test makes
    /// time pass.
    pub fn with_clock(mut self, clock: MemoryClock) -> Self {
        self.clock = clock;
        self
    }

    fn now(&self) -> DateTime<Utc> {
        (self.clock)().trunc_subsecs(0)
    }

    fn records(&self) -> Result<std::sync::MutexGuard<'_, Records>, MemoryStoreError> {
        self.records
            .lock()
            .map_err(|_| MemoryStoreError("memory store lock poisoned".to_string()))
    }

    fn upsert_all(&self, records: Vec<MemoryRecord>) -> Result<(), MemoryStoreError> {
        for record in &records {
            record.validate(self.dimensions)?;
        }
        let now = self.now();
        let mut stored = self.records()?;
        for mut record in records {
            let key = (
                record.namespace.clone(),
                record.tenant.clone(),
                record.id.clone(),
            );
            record.created_at = Some(
                stored
                    .get(&key)
                    .and_then(|existing| existing.created_at)
                    .unwrap_or(now),
            );
            record.updated_at = Some(now);
            stored.insert(key, record);
        }
        Ok(())
    }

    fn get_now(&self, key: &MemoryKey) -> Result<Option<MemoryRecord>, MemoryStoreError> {
        key.validate()?;
        Ok(self
            .records()?
            .get(&(key.namespace.clone(), key.tenant.clone(), key.id.clone()))
            .cloned())
    }

    fn delete_now(&self, key: &MemoryKey) -> Result<bool, MemoryStoreError> {
        key.validate()?;
        Ok(self
            .records()?
            .remove(&(key.namespace.clone(), key.tenant.clone(), key.id.clone()))
            .is_some())
    }

    /// The records of one namespace and tenant, in id order.
    fn scope<'a>(
        stored: &'a Records,
        namespace: &str,
        tenant: &str,
    ) -> impl Iterator<Item = &'a MemoryRecord> {
        let from = (namespace.to_string(), tenant.to_string(), String::new());
        stored
            .range(from..)
            .take_while(move |((n, t, _), _)| n == namespace && t == tenant)
            .map(|(_, record)| record)
    }

    fn search_now(&self, search: &MemorySearch) -> Result<Vec<MemoryMatch>, MemoryStoreError> {
        search.validate(self.dimensions)?;
        let stored = self.records()?;
        let mut matches: Vec<MemoryMatch> = Self::scope(&stored, &search.namespace, &search.tenant)
            .filter(|record| record.embedding.model == search.embedding.model)
            .filter(|record| !search.exclude.contains(&record.id))
            .filter(|record| metadata_matches(&record.metadata, &search.metadata))
            .map(|record| MemoryMatch {
                similarity: cosine_similarity(&search.embedding.vector, &record.embedding.vector),
                record: record.clone(),
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
        matches.truncate(search.limit);
        Ok(matches)
    }

    fn list_now(&self, query: &MemoryQuery) -> Result<MemoryPage, MemoryStoreError> {
        let after = query.validate()?;
        let stored = self.records()?;
        let records = Self::scope(&stored, &query.namespace, &query.tenant)
            .filter(|record| {
                after
                    .as_deref()
                    .is_none_or(|after| record.id.as_str() > after)
            })
            .filter(|record| metadata_matches(&record.metadata, &query.metadata))
            .take(query.limit + 1)
            .cloned()
            .collect();
        Ok(MemoryPage::of(records, query.limit))
    }

    fn purge_now(&self, purge: &MemoryPurge) -> Result<usize, MemoryStoreError> {
        purge.validate()?;
        let older_than = purge.older_than.trunc_subsecs(0);
        let mut stored = self.records()?;
        let mut doomed: Vec<(DateTime<Utc>, (String, String, String))> = stored
            .iter()
            .filter(|((namespace, tenant, _), record)| {
                purge.namespace.as_ref().is_none_or(|n| n == namespace)
                    && purge.tenant.as_ref().is_none_or(|t| t == tenant)
                    && record.updated_at.is_some_and(|at| at < older_than)
            })
            .map(|(key, record)| (record.updated_at.unwrap_or(older_than), key.clone()))
            .collect();
        doomed.sort();
        doomed.truncate(purge.limit);
        for (_, key) in &doomed {
            stored.remove(key);
        }
        Ok(doomed.len())
    }
}

impl MemoryStore for InMemoryMemoryStore {
    fn dimensions(&self) -> usize {
        self.dimensions
    }

    fn upsert(&self, record: MemoryRecord) -> BoxFuture<'_, Result<(), MemoryStoreError>> {
        Box::pin(async move { self.upsert_all(vec![record]) })
    }

    fn upsert_many(
        &self,
        records: Vec<MemoryRecord>,
    ) -> BoxFuture<'_, Result<(), MemoryStoreError>> {
        Box::pin(async move { self.upsert_all(records) })
    }

    fn get<'a>(
        &'a self,
        key: &'a MemoryKey,
    ) -> BoxFuture<'a, Result<Option<MemoryRecord>, MemoryStoreError>> {
        Box::pin(async move { self.get_now(key) })
    }

    fn delete<'a>(&'a self, key: &'a MemoryKey) -> BoxFuture<'a, Result<bool, MemoryStoreError>> {
        Box::pin(async move { self.delete_now(key) })
    }

    fn search<'a>(
        &'a self,
        search: &'a MemorySearch,
    ) -> BoxFuture<'a, Result<Vec<MemoryMatch>, MemoryStoreError>> {
        Box::pin(async move { self.search_now(search) })
    }

    fn list<'a>(
        &'a self,
        query: &'a MemoryQuery,
    ) -> BoxFuture<'a, Result<MemoryPage, MemoryStoreError>> {
        Box::pin(async move { self.list_now(query) })
    }
}

impl MemoryRetention for InMemoryMemoryStore {
    fn purge<'a>(
        &'a self,
        purge: &'a MemoryPurge,
    ) -> BoxFuture<'a, Result<usize, MemoryStoreError>> {
        Box::pin(async move { self.purge_now(purge) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cursor_round_trips_and_a_bad_one_is_refused() {
        let cursor = encode_cursor("restart-pod");
        assert_eq!(decode_cursor(&cursor).unwrap(), "restart-pod");
        assert_eq!(
            decode_cursor("nope!").unwrap_err().0,
            "'nope!' is not a memory listing cursor"
        );
        // A thread listing's cursor isn't a memory listing's.
        let thread = crate::store::ThreadCursor::new("2026-10-01T00:00:00Z", "t").encode();
        assert!(decode_cursor(&thread).is_err());
    }

    #[test]
    fn numbers_in_refusals_are_written_the_shortest_way() {
        let search = MemorySearch::new("n", Embedding::new("m", vec![1.0, 0.0]));
        assert_eq!(
            search
                .clone()
                .min_similarity(1.5)
                .validate(2)
                .unwrap_err()
                .0,
            "the minimum similarity is 1.5; it must be from -1 to 1"
        );
        assert_eq!(
            search
                .clone()
                .min_similarity(-2.0)
                .validate(2)
                .unwrap_err()
                .0,
            "the minimum similarity is -2; it must be from -1 to 1"
        );
        assert!(search.min_similarity(-1.0).validate(2).is_ok());
    }

    #[test]
    fn keys_and_metadata_keys_are_limited_in_bytes() {
        let long = "é".repeat(129); // 258 bytes
        let embedding = Embedding::new("m", vec![1.0]);
        let refusal = |record: MemoryRecord| record.validate(1).unwrap_err().0;
        assert_eq!(
            refusal(MemoryRecord::new(&long, "", "a", embedding.clone())),
            "the namespace is longer than 256 bytes"
        );
        assert_eq!(
            refusal(MemoryRecord::new("n", &long, "a", embedding.clone())),
            "the tenant is longer than 256 bytes"
        );
        assert_eq!(
            refusal(MemoryRecord::new("n", "", &long, embedding.clone())),
            "the id is longer than 256 bytes"
        );
        let key = "k".repeat(129);
        assert_eq!(
            refusal(MemoryRecord::new("n", "", "a", embedding.clone()).with_metadata(&key, "v")),
            format!("the metadata key '{key}' is longer than 128 bytes")
        );
        assert!(
            MemoryRecord::new("n", "", "a", embedding)
                .with_metadata("k".repeat(128), "v")
                .validate(1)
                .is_ok()
        );
    }

    #[test]
    fn a_vector_with_a_value_that_isnt_finite_is_refused() {
        let record = MemoryRecord::new("n", "", "a", Embedding::new("m", vec![1.0, f32::NAN]));
        assert_eq!(
            record.validate(2).unwrap_err().0,
            "the vector has a value that isn't finite"
        );
        let record = MemoryRecord::new("n", "", "a", Embedding::new("m", vec![f32::INFINITY, 0.0]));
        assert_eq!(
            record.validate(2).unwrap_err().0,
            "the vector has a value that isn't finite"
        );
    }

    #[test]
    fn a_listing_and_a_purge_refuse_a_limit_out_of_range() {
        assert_eq!(
            MemoryQuery::new("n").limit(0).validate().unwrap_err().0,
            "the limit is 0; it must be from 1 to 1000"
        );
        assert_eq!(
            MemoryQuery::new("n").limit(1001).validate().unwrap_err().0,
            "the limit is 1001; it must be from 1 to 1000"
        );
        assert_eq!(
            MemoryPurge::new(Utc::now())
                .limit(0)
                .validate()
                .unwrap_err()
                .0,
            "the limit is 0; it must be at least 1"
        );
        assert!(MemoryPurge::new(Utc::now()).limit(5000).validate().is_ok());
    }
}
