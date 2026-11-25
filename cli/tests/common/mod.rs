//! What the CLI's tests share. The CLI opens every store read-only
//! (`design/51-Read-Only-Store-Open.md`), so a test writes the log it reads
//! as an application would: through `strata-memory`'s writable stores, on the
//! same `--storage` the CLI is given. And the read-only errors the CLI
//! reports come from `schema/fixtures/checkpoint-log/read-only-errors.json`,
//! shared with both engines.
#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

use datalium_strata::checkpoint::{CheckpointStore, ThreadLease};
use datalium_strata::schema::Checkpoint;
use datalium_strata_memory::checkpoint::SqliteCheckpointStore;
use datalium_strata_memory::postgres_store::PostgresCheckpointStore;
use datalium_strata_memory::redis_store::RedisCheckpointStore;
use datalium_strata_memory::{RedisStoreOptions, SqliteStoreOptions};

/// A writable store on what a `--storage` names, for seeding.
pub struct Seed {
    store: Arc<dyn CheckpointStore>,
}

/// Opens `storage` (a SQLite path, `postgres://…` or `redis://…?prefix=…`,
/// as the CLI reads it) writable: a SQLite file is created if missing, as an
/// application's store does.
pub async fn seed(storage: &str) -> Seed {
    let lower = storage.to_ascii_lowercase();
    let store: Arc<dyn CheckpointStore> = if lower.starts_with("postgres://")
        || lower.starts_with("postgresql://")
    {
        Arc::new(PostgresCheckpointStore::connect(storage).await.unwrap())
    } else if lower.starts_with("redis://") {
        let (url, prefix) = cli::store::redis_prefix(storage);
        Arc::new(
            RedisCheckpointStore::connect_with(&url, RedisStoreOptions::new().with_prefix(prefix))
                .await
                .unwrap(),
        )
    } else {
        Arc::new(SqliteCheckpointStore::open_with(storage, SqliteStoreOptions::new()).unwrap())
    };
    Seed { store }
}

impl Seed {
    /// Appends `checkpoint` to its thread's log and returns its sequence.
    pub async fn insert(&self, checkpoint: &Checkpoint) -> Result<i64, String> {
        self.store.save(checkpoint.clone()).await.map_err(|e| e.0)?;
        let latest = self
            .store
            .load_latest(&checkpoint.thread_id)
            .await
            .map_err(|e| e.0)?;
        Ok(latest.and_then(|latest| latest.sequence).unwrap_or(0))
    }

    /// `owner` takes `thread_id`'s lease for `ttl`, if it is free.
    pub async fn hold(
        &self,
        thread_id: &str,
        owner: &str,
        ttl: std::time::Duration,
    ) -> Result<Option<ThreadLease>, String> {
        let attempt = self
            .store
            .try_acquire_lease(thread_id, owner, ttl)
            .await
            .map_err(|e| e.0)?;
        Ok(attempt.acquired())
    }
}

/// An empty, current SQLite checkpoint log at `path`, as a runtime's store
/// creates it.
pub fn create_sqlite(path: impl AsRef<Path>) {
    drop(SqliteCheckpointStore::open(path.as_ref()).unwrap());
}

/// `schema/fixtures/checkpoint-log/read-only-errors.json`.
pub fn read_only_errors() -> serde_json::Value {
    let path = format!(
        "{}/../schema/fixtures/checkpoint-log/read-only-errors.json",
        env!("CARGO_MANIFEST_DIR")
    );
    serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap()
}

/// The fixture's open error `case`, its `{name}`s replaced by `values`.
pub fn open_error(case: &str, values: &[(&str, &str)]) -> String {
    let mut message = read_only_errors()["open"][case]
        .as_str()
        .unwrap_or_else(|| panic!("read-only-errors.json has no open.{case}"))
        .to_string();
    for (name, value) in values {
        message = message.replace(&format!("{{{name}}}"), value);
    }
    assert!(!message.contains('{'), "open.{case}: {message}");
    message
}

/// The SHA-256 of the file at `path`, as hex.
pub fn sha256(path: &Path) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(std::fs::read(path).unwrap())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
