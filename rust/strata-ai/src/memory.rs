//! The memory helper (`design/45-Long-Term-Memory.md`, "The helper"): a
//! memory store and an embedder together, so a node remembers and recalls by
//! text. Mirrors .NET's `StrataMemory`.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use datalium_strata::context::NodeContext;
use datalium_strata::graph::NodeError;
use datalium_strata::memory::{
    Embedding, MemoryKey, MemoryMatch, MemoryRecord, MemorySearch, MemoryStore, MemoryStoreError,
    validate_metadata, validate_scope,
};
use serde_json::{Map, Value};

use crate::call::CallOptions;
use crate::embed::{Embedder, EmbeddingRequest, embed_with};

/// A memory store with an embedder (`design/45-Long-Term-Memory.md`): it
/// embeds a text through one observed call ([`embed_with`], operation
/// `embeddings`) and stores the record with it, or searches with it.
///
/// - **The label.** Every vector is stored and searched under one model
///   label: [`Self::with_model`]'s when given, else `<provider>/<model>` from
///   the embedder (a blank provider is `unknown`). A blank label, and an
///   embedder with no default model (or a blank one) and no label, are
///   refused by every call, before any I/O. The
///   embedding request names no model, so the embedder's default is the one
///   labelled.
/// - **The tenant.** An entry's or a recall's own tenant when set (`""`
///   included); else, with a node context, the run's metadata at the tenant
///   key (`tenant` unless [`Self::with_tenant_key`]); else `""`.
/// - **Refusals first.** The key, the metadata and a recall's options are
///   checked with the store's own rules and messages before the embedding
///   call ([`MemoryError::Store`]).
/// - **No retries, nothing swallowed.** An embedding failure is the node
///   failure [`embed_with`] returns ([`MemoryError::Embedding`]: transient
///   with the provider's hint, or fatal), and nothing is stored; a store
///   failure is the store's ([`MemoryError::Store`]). `?` in a fallible node
///   turns either into a [`NodeError`].
#[derive(Clone)]
pub struct Memory {
    store: Arc<dyn MemoryStore>,
    embedder: Arc<dyn Embedder>,
    model: Option<String>,
    tenant_key: String,
}

impl Memory {
    pub fn new(store: Arc<dyn MemoryStore>, embedder: Arc<dyn Embedder>) -> Self {
        Self {
            store,
            embedder,
            model: None,
            tenant_key: "tenant".to_string(),
        }
    }

    /// Labels the vectors `label` (used as is) instead of
    /// `<provider>/<model>`: for a deployment whose provider or model name
    /// changes while its vectors stay comparable, or an embedder that names
    /// no model.
    pub fn with_model(mut self, label: impl Into<String>) -> Self {
        self.model = Some(label.into());
        self
    }

    /// Reads the tenant from the run's metadata at `key` instead of `tenant`.
    pub fn with_tenant_key(mut self, key: impl Into<String>) -> Self {
        self.tenant_key = key.into();
        self
    }

    pub fn store(&self) -> &Arc<dyn MemoryStore> {
        &self.store
    }

    pub fn embedder(&self) -> &Arc<dyn Embedder> {
        &self.embedder
    }

    /// The label every vector is stored and searched under, or the refusal
    /// when there is none: a blank [`Self::with_model`] label is the store's
    /// `the model is empty`, and an embedder whose default model is missing
    /// or blank names no model.
    pub fn label(&self) -> Result<String, MemoryError> {
        if let Some(model) = &self.model {
            if model.trim().is_empty() {
                return Err(MemoryStoreError("the model is empty".to_string()).into());
            }
            return Ok(model.clone());
        }
        let Some(model) = self
            .embedder
            .default_model()
            .filter(|model| !model.trim().is_empty())
        else {
            return Err(MemoryError::Embedding(NodeError::Fatal(
                "the embedder names no model: label the memory's vectors with with_model"
                    .to_string(),
            )));
        };
        let provider = match self.embedder.provider() {
            provider if provider.trim().is_empty() => "unknown",
            provider => provider,
        };
        Ok(format!("{provider}/{model}"))
    }

    /// Embeds `entry.text` and writes the record (replacing the one with its
    /// key): the vector under the label, the text as its `text`, the entry's
    /// content and metadata, in the entry's tenant or the run's.
    pub async fn remember<'a>(
        &self,
        entry: MemoryEntry,
        ctx: impl Into<Option<&'a NodeContext>>,
    ) -> Result<(), MemoryError> {
        let ctx = ctx.into();
        let label = self.label()?;
        let tenant = self.tenant(entry.tenant.as_deref(), ctx);
        check(&entry, &tenant)?;
        let mut vectors = self.embed(vec![entry.text.clone()], ctx).await?;
        let record = entry.into_record(tenant, &label, vectors.remove(0));
        Ok(self.store.upsert(record).await?)
    }

    /// [`Self::remember`] for each entry: every entry is checked first, every
    /// text embedded in one call in order, and the records written by one
    /// `upsert_many`. Nothing to remember makes no call, once the label is
    /// known to be good.
    pub async fn remember_many<'a>(
        &self,
        entries: Vec<MemoryEntry>,
        ctx: impl Into<Option<&'a NodeContext>>,
    ) -> Result<(), MemoryError> {
        let label = self.label()?;
        if entries.is_empty() {
            return Ok(());
        }
        let ctx = ctx.into();
        let mut tenants = Vec::with_capacity(entries.len());
        for entry in &entries {
            let tenant = self.tenant(entry.tenant.as_deref(), ctx);
            check(entry, &tenant)?;
            tenants.push(tenant);
        }
        let texts = entries.iter().map(|entry| entry.text.clone()).collect();
        let vectors = self.embed(texts, ctx).await?;
        let records = entries
            .into_iter()
            .zip(tenants)
            .zip(vectors)
            .map(|((entry, tenant), vector)| entry.into_record(tenant, &label, vector))
            .collect();
        Ok(self.store.upsert_many(records).await?)
    }

    /// The records of `namespace` most similar to `text`, under the label, in
    /// the options' tenant or the run's: embeds `text` and searches with the
    /// options' limit, minimum similarity, exclusions and metadata. Returns
    /// the store's matches.
    pub async fn recall<'a>(
        &self,
        namespace: &str,
        text: &str,
        options: RecallOptions,
        ctx: impl Into<Option<&'a NodeContext>>,
    ) -> Result<Vec<MemoryMatch>, MemoryError> {
        let ctx = ctx.into();
        let label = self.label()?;
        let tenant = self.tenant(options.tenant.as_deref(), ctx);
        validate_scope(namespace, &tenant)?;
        let mut search = MemorySearch::new(namespace, Embedding::new(label, Vec::new()))
            .tenant(tenant)
            .limit(options.limit);
        if let Some(min) = options.min_similarity {
            search = search.min_similarity(min);
        }
        for id in options.exclude {
            search = search.exclude(id);
        }
        search.metadata = options.metadata;
        search.validate_filters()?;
        let mut vectors = self.embed(vec![text.to_string()], ctx).await?;
        search.embedding.vector = vectors.remove(0);
        Ok(self.store.search(&search).await?)
    }

    fn tenant(&self, own: Option<&str>, ctx: Option<&NodeContext>) -> String {
        own.map(str::to_string)
            .or_else(|| ctx.and_then(|ctx| ctx.metadata().get(&self.tenant_key).cloned()))
            .unwrap_or_default()
    }

    /// One vector per text, in order; [`embed_with`] has checked the count.
    async fn embed(
        &self,
        texts: Vec<String>,
        ctx: Option<&NodeContext>,
    ) -> Result<Vec<Vec<f32>>, MemoryError> {
        let response = embed_with(
            &*self.embedder,
            EmbeddingRequest::new(texts),
            ctx,
            &CallOptions::default(),
        )
        .await
        .map_err(MemoryError::Embedding)?;
        Ok(response.vectors)
    }
}

impl fmt::Debug for Memory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Memory")
            .field("provider", &self.embedder.provider())
            .field("label", &self.label().ok())
            .field("tenant_key", &self.tenant_key)
            .field("dimensions", &self.store.dimensions())
            .finish_non_exhaustive()
    }
}

/// The store's refusals of an entry's key and metadata, before any I/O.
fn check(entry: &MemoryEntry, tenant: &str) -> Result<(), MemoryError> {
    MemoryKey::new(&entry.namespace, tenant, &entry.id).validate()?;
    validate_metadata(&entry.metadata)?;
    Ok(())
}

/// What [`Memory::remember`] takes: a record with text in place of a vector.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct MemoryEntry {
    pub namespace: String,
    pub id: String,
    /// What is embedded, and kept as the record's `text`.
    pub text: String,
    /// The run's tenant (or `""` outside a run) when `None`.
    pub tenant: Option<String>,
    pub content: Option<Map<String, Value>>,
    pub metadata: BTreeMap<String, String>,
}

impl MemoryEntry {
    pub fn new(
        namespace: impl Into<String>,
        id: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            id: id.into(),
            text: text.into(),
            tenant: None,
            content: None,
            metadata: BTreeMap::new(),
        }
    }

    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    pub fn with_content(mut self, content: Map<String, Value>) -> Self {
        self.content = Some(content);
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    fn into_record(self, tenant: String, label: &str, vector: Vec<f32>) -> MemoryRecord {
        let mut record = MemoryRecord::new(
            self.namespace,
            tenant,
            self.id,
            Embedding::new(label, vector),
        )
        .with_text(self.text);
        record.content = self.content;
        record.metadata = self.metadata;
        record
    }
}

/// How [`Memory::recall`] searches.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct RecallOptions {
    /// The run's tenant (or `""` outside a run) when `None`.
    pub tenant: Option<String>,
    /// 1 to 1,000; 10 by default.
    pub limit: usize,
    /// Drops matches less similar than this (−1 to 1). None by default.
    pub min_similarity: Option<f64>,
    /// Ids never returned.
    pub exclude: Vec<String>,
    /// Only records whose metadata has every pair.
    pub metadata: BTreeMap<String, String>,
}

impl Default for RecallOptions {
    fn default() -> Self {
        Self {
            tenant: None,
            limit: 10,
            min_similarity: None,
            exclude: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }
}

impl RecallOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
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
        self.exclude.push(id.into());
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// Why a [`Memory`] call failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum MemoryError {
    /// The embedding call failed (transient with the provider's hint, or
    /// fatal), or the memory has no label. Nothing was stored.
    Embedding(NodeError),
    /// The store refused the input (before any embedding call) or failed.
    Store(MemoryStoreError),
}

impl fmt::Display for MemoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Embedding(error) => f.write_str(error.message()),
            Self::Store(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for MemoryError {}

impl From<MemoryStoreError> for MemoryError {
    fn from(error: MemoryStoreError) -> Self {
        Self::Store(error)
    }
}

impl From<MemoryError> for NodeError {
    /// The embedding call's node failure as it is; a store's failure fatal.
    fn from(error: MemoryError) -> Self {
        match error {
            MemoryError::Embedding(error) => error,
            MemoryError::Store(error) => NodeError::Fatal(error.to_string()),
        }
    }
}
