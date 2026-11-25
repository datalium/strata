//! Opening a checkpoint store read-only (`design/51-Read-Only-Store-Open.md`,
//! FR-MP-12): what the read-only factories of the SQLite, Postgres and Redis
//! stores share. Opening creates nothing and never migrates, reads never
//! write, and every write is refused before it reaches the database. The
//! messages are the same in both engines and the `strata` CLI, pinned by
//! `schema/fixtures/checkpoint-log/read-only-errors.json`.

use std::sync::Arc;

use datalium_strata::BoxFuture;
use datalium_strata::checkpoint::CheckpointStoreError;

use crate::offload::{BlobStore, Offload};
use crate::schema_versions;

/// The write operations a read-only store refuses, as its message names them.
pub(crate) const SAVE: &str = "save";
pub(crate) const ACQUIRE_LEASE: &str = "acquire lease";
pub(crate) const RENEW_LEASE: &str = "renew lease";
pub(crate) const RELEASE_LEASE: &str = "release lease";
pub(crate) const PURGE: &str = "purge";
/// `PostgresCheckpointStore::migrate`.
pub(crate) const MIGRATE: &str = "migrate";
/// `RedisCheckpointStore::rebuild_index`.
pub(crate) const REBUILD_INDEX: &str = "rebuild index";

/// Why a read-only store refuses `operation`.
pub(crate) fn refused(operation: &str) -> CheckpointStoreError {
    CheckpointStoreError(format!(
        "the store was opened read-only: {operation} is refused"
    ))
}

/// [`refused`], as a store call's future.
pub(crate) fn refuse<'a, T: Send + 'a>(
    operation: &str,
) -> BoxFuture<'a, Result<T, CheckpointStoreError>> {
    Box::pin(std::future::ready(Err(refused(operation))))
}

/// `path` names no file.
pub(crate) fn missing_sqlite_file(path: &str) -> CheckpointStoreError {
    CheckpointStoreError(format!(
        "no SQLite store at '{path}': the file doesn't exist"
    ))
}

/// The database `store` names (a SQLite path, a Postgres database name) has
/// no `checkpoints` table.
pub(crate) fn no_strata_log(store: &str) -> CheckpointStoreError {
    CheckpointStoreError(format!("'{store}' has no Strata checkpoint log"))
}

/// A SQLite file whose writer stopped mid-write: a read-only connection can't
/// roll its journal back, and SQLite won't read it until someone does.
pub(crate) fn hot_journal(path: &str) -> CheckpointStoreError {
    CheckpointStoreError(format!(
        "'{path}' has an unfinished write: open it once with the application that writes it"
    ))
}

/// A Redis prefix no current runtime has indexed.
pub(crate) fn redis_prefix_not_indexed(prefix: &str) -> CheckpointStoreError {
    CheckpointStoreError(format!(
        "no Strata thread index under prefix '{prefix}': nothing was written there, or no current runtime has connected to it"
    ))
}

/// A filesystem blob store whose root isn't there.
pub(crate) fn missing_blob_directory(dir: &str) -> CheckpointStoreError {
    CheckpointStoreError(format!("blob directory '{dir}' doesn't exist"))
}

/// Refuses a checkpoints component at `found` that a read-only store of a
/// build that knows `known` can't read: a newer one, with the message every
/// store refuses it with, and version 0, a log from before the version table,
/// whose head table only a migration creates. Versions 1 to `known` are read
/// (review decision 3).
pub(crate) fn check_version(found: i64, known: i64) -> Result<(), CheckpointStoreError> {
    if found > known {
        return Err(CheckpointStoreError(schema_versions::newer(
            schema_versions::CHECKPOINTS,
            found,
            known,
        )));
    }
    if found < schema_versions::BASELINE_VERSION {
        return Err(CheckpointStoreError(format!(
            "database is at {} schema version {found}, and a read-only store reads {} to {known}: open it once with a runtime to migrate it",
            schema_versions::CHECKPOINTS,
            schema_versions::BASELINE_VERSION,
        )));
    }
    Ok(())
}

/// The blob store `offload` names, which a read-only store reads offloaded
/// values from instead of its own database, checked to be there.
pub(crate) fn blobs(
    offload: Option<&Offload>,
) -> Result<Option<Arc<dyn BlobStore>>, CheckpointStoreError> {
    let blobs = offload.and_then(|offload| offload.blobs().cloned());
    if let Some(blobs) = &blobs {
        blobs.check_readable()?;
    }
    Ok(blobs)
}
