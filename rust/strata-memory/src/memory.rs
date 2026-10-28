//! What the long-term memory adapters share (`design/45-Long-Term-Memory.md`):
//! the vector index option, the operation metrics, and the stored form of a
//! record the SQLite and Redis adapters both write (a vector as little-endian
//! float32 bytes, metadata and content as compact JSON text, timestamps as
//! RFC 3339 text). The port, the record and the in-memory store are the
//! engine's (`datalium_strata::memory`); the adapters are
//! [`crate::postgres_memory`], [`crate::sqlite_memory`] and
//! [`crate::redis_memory`].

use std::collections::BTreeMap;
use std::future::Future;
use std::time::Instant;

use chrono::{DateTime, Utc};
use datalium_strata::memory::{Embedding, MemoryRecord, MemoryStoreError};
use serde_json::{Map, Value};

use crate::retry::STORE_LABEL;

/// Counter, `{operation}`: calls a memory store made, by [`STORE_LABEL`]
/// (`postgres`, `sqlite`, `redis`) and [`MEMORY_OPERATION_LABEL`]. The
/// in-memory store records none. A call refused for bad input isn't counted.
pub const MEMORY_OPERATIONS_METRIC: &str = "strata.memory.operations";

/// Histogram, seconds: how long a memory store's call took, retries included,
/// by [`STORE_LABEL`] and [`MEMORY_OPERATION_LABEL`].
pub const MEMORY_OPERATION_DURATION_METRIC: &str = "strata.memory.operation.duration";

/// Histogram, `{match}`: how many matches a search returned, by
/// [`STORE_LABEL`].
pub const MEMORY_SEARCH_RESULTS_METRIC: &str = "strata.memory.search.results";

/// The label naming a memory store's call: `upsert` (one record or many),
/// `get`, `delete`, `search`, `list` or `purge`.
pub const MEMORY_OPERATION_LABEL: &str = "strata.memory.operation";

/// How a store finds a search's nearest records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MemoryIndex {
    /// No index: every record of the search's namespace, tenant and model is
    /// compared, so the result is exact. The default.
    #[default]
    None,
    /// An HNSW index (pgvector's `hnsw` with `vector_cosine_ops`, the Redis
    /// Query Engine's `HNSW` vector field): approximate, for stores past tens
    /// of thousands of records per namespace, tenant and model. It may miss a
    /// near neighbour. `m` and `ef_construction` are the index's build
    /// parameters (16 and 64 are pgvector's defaults, [`MemoryIndex::hnsw`]).
    /// The SQLite store has no index option: it always searches exactly.
    Hnsw { m: u32, ef_construction: u32 },
}

impl MemoryIndex {
    /// HNSW with pgvector's default parameters.
    pub fn hnsw() -> Self {
        Self::Hnsw {
            m: 16,
            ef_construction: 64,
        }
    }
}

/// Runs `call`, a memory store's `operation`, and records it in
/// [`MEMORY_OPERATIONS_METRIC`] and [`MEMORY_OPERATION_DURATION_METRIC`],
/// whatever it returns.
pub(crate) async fn observed<T, E>(
    store: &'static str,
    operation: &'static str,
    call: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    let started = Instant::now();
    let result = call.await;
    metrics::counter!(
        MEMORY_OPERATIONS_METRIC,
        STORE_LABEL => store,
        MEMORY_OPERATION_LABEL => operation
    )
    .increment(1);
    metrics::histogram!(
        MEMORY_OPERATION_DURATION_METRIC,
        STORE_LABEL => store,
        MEMORY_OPERATION_LABEL => operation
    )
    .record(started.elapsed().as_secs_f64());
    result
}

/// Records a search's match count in [`MEMORY_SEARCH_RESULTS_METRIC`].
pub(crate) fn record_search_results(store: &'static str, matches: usize) {
    metrics::histogram!(MEMORY_SEARCH_RESULTS_METRIC, STORE_LABEL => store).record(matches as f64);
}

/// A vector as sqlite-vec and the Redis Query Engine store one: each value's
/// little-endian float32 bytes, in order.
pub(crate) fn vector_bytes(vector: &[f32]) -> Vec<u8> {
    vector
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect()
}

/// The vector [`vector_bytes`] wrote, or `None` when `bytes` aren't whole
/// float32 values.
pub(crate) fn vector_of_bytes(bytes: &[u8]) -> Option<Vec<f32>> {
    if !bytes.len().is_multiple_of(4) {
        return None;
    }
    Some(
        bytes
            .as_chunks::<4>()
            .0
            .iter()
            .map(|chunk| f32::from_le_bytes(*chunk))
            .collect(),
    )
}

/// Metadata as stored: a compact JSON object, keys in byte order, `{}` when
/// there is none.
pub(crate) fn metadata_text(metadata: &BTreeMap<String, String>) -> String {
    serde_json::to_string(metadata).unwrap_or_else(|_| "{}".to_string())
}

/// Content as stored: compact JSON text, or `None`.
pub(crate) fn content_text(content: &Option<Map<String, Value>>) -> Option<String> {
    content
        .as_ref()
        .map(|content| Value::Object(content.clone()).to_string())
}

/// A record as the SQLite and Redis stores keep it: text, and the vector's
/// bytes.
pub(crate) struct StoredRecord {
    pub namespace: String,
    pub tenant: String,
    pub id: String,
    pub model: String,
    pub embedding: Vec<u8>,
    pub content: Option<String>,
    pub text: Option<String>,
    pub metadata: String,
    pub created_at: String,
    pub updated_at: String,
}

impl StoredRecord {
    /// The record, or what is wrong with what was stored (in the words of the
    /// Postgres store's refusals).
    pub fn into_record(self) -> Result<MemoryRecord, MemoryStoreError> {
        let id = self.id;
        let bad = |what: &str| MemoryStoreError(format!("memory record '{id}' has {what}"));
        let vector = vector_of_bytes(&self.embedding)
            .ok_or_else(|| bad("an embedding that isn't a vector"))?;
        let content = match self.content {
            None => None,
            Some(text) => match serde_json::from_str::<Value>(&text) {
                Ok(Value::Null) => None,
                Ok(Value::Object(content)) => Some(content),
                _ => return Err(bad("content that isn't a JSON object")),
            },
        };
        let metadata = serde_json::from_str::<BTreeMap<String, String>>(&self.metadata)
            .map_err(|_| bad("metadata that isn't string pairs"))?;
        let time = |column: &str, text: &str| -> Result<DateTime<Utc>, MemoryStoreError> {
            DateTime::parse_from_rfc3339(text)
                .map(|time| time.with_timezone(&Utc))
                .map_err(|_| bad(&format!("a {column} that isn't a timestamp: '{text}'")))
        };
        let created_at = time("created_at", &self.created_at)?;
        let updated_at = time("updated_at", &self.updated_at)?;
        let mut record = MemoryRecord::new(
            self.namespace,
            self.tenant,
            id.clone(),
            Embedding::new(self.model, vector),
        );
        record.content = content;
        record.text = self.text;
        record.metadata = metadata;
        record.created_at = Some(created_at);
        record.updated_at = Some(updated_at);
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_vector_is_stored_as_little_endian_float32_bytes() {
        // memory/redis-record.json's vector and its hex.
        let bytes = vector_bytes(&[0.125, -0.5, 0.25, 0.75]);
        let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex, "0000003e000000bf0000803e0000403f");
        assert_eq!(
            vector_of_bytes(&bytes).unwrap(),
            vec![0.125, -0.5, 0.25, 0.75]
        );
        assert!(vector_of_bytes(&[0, 0, 0]).is_none());
    }

    #[test]
    fn metadata_is_a_compact_object_with_keys_in_byte_order() {
        let mut metadata = BTreeMap::new();
        assert_eq!(metadata_text(&metadata), "{}");
        metadata.insert("service".to_string(), "checkout-api".to_string());
        metadata.insert("scenario".to_string(), "db-saturation".to_string());
        metadata.insert("Z".to_string(), "\"quoted\"".to_string());
        assert_eq!(
            metadata_text(&metadata),
            r#"{"Z":"\"quoted\"","scenario":"db-saturation","service":"checkout-api"}"#
        );
    }
}
