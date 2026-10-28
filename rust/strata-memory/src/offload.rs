//! Value offload (`design/41-Checkpoint-Efficiency.md`, "Offload"): a store
//! with [`Offload`] stores each top-level value of a level's state whose JSON
//! is at least [`Offload::with_threshold`] bytes once, in a [`BlobStore`], and
//! refers to it from the row. Mirrors the .NET `OffloadOptions`,
//! `ICheckpointBlobStore`, `InMemoryCheckpointBlobStore` and
//! `FileSystemCheckpointBlobStore`.
//!
//! The blob port lives here, not in the engine: the engine never calls it. A
//! store's own database is the default place for blobs (a `checkpoint_blobs`
//! table in SQL, a hash per thread in Redis); [`InMemoryBlobStore`] and
//! [`FileSystemBlobStore`] are the others built in, and S3 and the like are an
//! application's, through the trait.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use datalium_strata::BoxFuture;
use datalium_strata::checkpoint::CheckpointStoreError;
use sha2::{Digest, Sha256};

use crate::support::poisoned;

/// How a store offloads values. The default offloads a value of 64 KiB or
/// more to the checkpoint store's own database. Mirrors the .NET
/// `OffloadOptions`.
#[derive(Clone)]
pub struct Offload {
    threshold: usize,
    blobs: Option<Arc<dyn BlobStore>>,
}

impl Default for Offload {
    fn default() -> Self {
        Self {
            threshold: 64 * 1024,
            blobs: None,
        }
    }
}

impl std::fmt::Debug for Offload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Offload")
            .field("threshold", &self.threshold)
            .field("blobs", &self.blobs.as_ref().map(|_| "BlobStore"))
            .finish()
    }
}

impl Offload {
    pub fn new() -> Self {
        Self::default()
    }

    /// The bytes of a value's JSON from which it is offloaded. At least 1.
    pub fn with_threshold(mut self, threshold: usize) -> Self {
        self.threshold = threshold;
        self
    }

    /// Where offloaded values go, and where the store reads them from,
    /// instead of the checkpoint store's own database.
    pub fn with_blobs(mut self, blobs: Arc<dyn BlobStore>) -> Self {
        self.blobs = Some(blobs);
        self
    }

    pub fn threshold(&self) -> usize {
        self.threshold
    }

    /// The blob store values go to, when it isn't the checkpoint store's own
    /// database.
    pub fn blobs(&self) -> Option<&Arc<dyn BlobStore>> {
        self.blobs.as_ref()
    }

    pub(crate) fn check(&self) -> Result<(), CheckpointStoreError> {
        if self.threshold == 0 {
            return Err(CheckpointStoreError(
                "offload's threshold must be at least 1".to_string(),
            ));
        }
        Ok(())
    }
}

/// Where offloaded values live: content addressed within a thread, by
/// `sha256:` and the lowercase hex SHA-256 of the content. A checkpoint store
/// calls it; the engine never does. Mirrors the .NET `ICheckpointBlobStore`.
pub trait BlobStore: Send + Sync {
    /// Stores `content` under (`thread_id`, `hash`). Storing what is already
    /// there is a no-op.
    fn put<'a>(
        &'a self,
        thread_id: &'a str,
        hash: &'a str,
        content: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), CheckpointStoreError>>;

    /// The content stored under (`thread_id`, `hash`), if any.
    fn get<'a>(
        &'a self,
        thread_id: &'a str,
        hash: &'a str,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, CheckpointStoreError>>;

    /// Deletes every blob of `thread_id`: what a purge calls.
    fn delete_thread<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<(), CheckpointStoreError>>;
}

/// Blobs in a map: for tests, and for a store in memory.
#[derive(Default)]
pub struct InMemoryBlobStore {
    threads: Mutex<HashMap<String, HashMap<String, Vec<u8>>>>,
}

impl InMemoryBlobStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// How many blobs `thread_id` has.
    pub fn count(&self, thread_id: &str) -> usize {
        self.threads
            .lock()
            .map_or(0, |threads| threads.get(thread_id).map_or(0, HashMap::len))
    }
}

impl BlobStore for InMemoryBlobStore {
    fn put<'a>(
        &'a self,
        thread_id: &'a str,
        hash: &'a str,
        content: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), CheckpointStoreError>> {
        Box::pin(async move {
            self.threads
                .lock()
                .map_err(|_| poisoned())?
                .entry(thread_id.to_string())
                .or_default()
                .entry(hash.to_string())
                .or_insert(content);
            Ok(())
        })
    }

    fn get<'a>(
        &'a self,
        thread_id: &'a str,
        hash: &'a str,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, CheckpointStoreError>> {
        Box::pin(async move {
            Ok(self
                .threads
                .lock()
                .map_err(|_| poisoned())?
                .get(thread_id)
                .and_then(|blobs| blobs.get(hash))
                .cloned())
        })
    }

    fn delete_thread<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<(), CheckpointStoreError>> {
        Box::pin(async move {
            self.threads
                .lock()
                .map_err(|_| poisoned())?
                .remove(thread_id);
            Ok(())
        })
    }
}

/// Blobs as files: `<root>/<thread id as lowercase hex of its UTF-8>/<hex of
/// the hash>`, the hash's hex digest without its `sha256:`. A file is written
/// under a temporary name and renamed into place, so a reader never sees half
/// of one. The work runs on tokio's blocking pool. Mirrors the .NET
/// `FileSystemCheckpointBlobStore`.
#[derive(Debug, Clone)]
pub struct FileSystemBlobStore {
    root: PathBuf,
}

impl FileSystemBlobStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The directory the store keeps its threads under.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The directory of `thread_id`'s blobs.
    pub fn thread_directory(&self, thread_id: &str) -> PathBuf {
        self.root.join(hex(thread_id.as_bytes()))
    }

    fn file(&self, thread_id: &str, hash: &str) -> Result<PathBuf, CheckpointStoreError> {
        match hash.strip_prefix(HASH_PREFIX) {
            Some(digest)
                if !digest.is_empty()
                    && digest
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) =>
            {
                Ok(self.thread_directory(thread_id).join(digest))
            }
            _ => Err(CheckpointStoreError(format!(
                "'{hash}' is not a sha256: hash"
            ))),
        }
    }
}

/// Runs `work` on the blocking pool, as a store error.
async fn on_disk<R: Send + 'static>(
    work: impl FnOnce() -> std::io::Result<R> + Send + 'static,
) -> Result<R, CheckpointStoreError> {
    crate::support::blocking(
        move || work().map_err(|err| CheckpointStoreError(err.to_string())),
        CheckpointStoreError,
    )
    .await
}

impl BlobStore for FileSystemBlobStore {
    fn put<'a>(
        &'a self,
        thread_id: &'a str,
        hash: &'a str,
        content: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), CheckpointStoreError>> {
        Box::pin(async move {
            let path = self.file(thread_id, hash)?;
            on_disk(move || {
                if path.exists() {
                    return Ok(());
                }
                if let Some(directory) = path.parent() {
                    std::fs::create_dir_all(directory)?;
                }
                let temporary =
                    path.with_extension(format!("{}-{}.tmp", std::process::id(), unique()));
                std::fs::write(&temporary, content)?;
                let moved = std::fs::rename(&temporary, &path);
                let _ = std::fs::remove_file(&temporary);
                match moved {
                    // Content addressed: whoever renamed first wrote what this
                    // would have.
                    Err(_) if path.exists() => Ok(()),
                    other => other,
                }
            })
            .await
        })
    }

    fn get<'a>(
        &'a self,
        thread_id: &'a str,
        hash: &'a str,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, CheckpointStoreError>> {
        Box::pin(async move {
            let path = self.file(thread_id, hash)?;
            on_disk(move || match std::fs::read(path) {
                Ok(content) => Ok(Some(content)),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(err) => Err(err),
            })
            .await
        })
    }

    fn delete_thread<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<(), CheckpointStoreError>> {
        Box::pin(async move {
            let directory = self.thread_directory(thread_id);
            on_disk(move || match std::fs::remove_dir_all(directory) {
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
                other => other,
            })
            .await
        })
    }
}

/// A number no other temporary file of this process has.
fn unique() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

const HASH_PREFIX: &str = "sha256:";

/// `bytes` as lowercase hex.
pub(crate) fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push(DIGITS[(byte >> 4) as usize] as char);
        text.push(DIGITS[(byte & 0xf) as usize] as char);
    }
    text
}

/// The name of a blob with this SHA-256: `sha256:` and its hex.
pub(crate) fn hash_name(digest: &[u8]) -> String {
    format!("{HASH_PREFIX}{}", hex(digest))
}

/// Whether `content` is what `hash` names.
pub(crate) fn matches(hash: &str, content: &[u8]) -> bool {
    hash == hash_name(&Sha256::digest(content))
}

/// A value a save stores in a blob store: its hash and its JSON text.
#[derive(Debug, Clone)]
pub(crate) struct Blob {
    pub hash: String,
    pub content: Arc<[u8]>,
}

/// The contents `hashes` name in `store`, those it has.
pub(crate) async fn read(
    store: &dyn BlobStore,
    thread_id: &str,
    hashes: &[String],
) -> Result<HashMap<String, Vec<u8>>, CheckpointStoreError> {
    let mut found = HashMap::with_capacity(hashes.len());
    for hash in hashes {
        if let Some(content) = store.get(thread_id, hash).await? {
            found.insert(hash.clone(), content);
        }
    }
    Ok(found)
}

/// Writes `blobs` to `store`, before the row that refers to them.
pub(crate) async fn put_all(
    store: &dyn BlobStore,
    thread_id: &str,
    blobs: &[Blob],
) -> Result<(), CheckpointStoreError> {
    for blob in blobs {
        store
            .put(thread_id, &blob.hash, blob.content.to_vec())
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_filesystem_store_keeps_a_blob_per_thread_and_hash_and_deletes_a_thread() {
        let root = std::env::temp_dir().join(format!("strata-blobs-{}", std::process::id()));
        let store = FileSystemBlobStore::new(&root);
        let content = br#"{"a":1}"#.to_vec();
        let hash = hash_name(&Sha256::digest(&content));
        store.put("t/1", &hash, content.clone()).await.unwrap();
        store.put("t/1", &hash, content.clone()).await.unwrap();
        assert_eq!(store.get("t/1", &hash).await.unwrap(), Some(content));
        assert_eq!(store.get("t/2", &hash).await.unwrap(), None);
        assert!(store.thread_directory("t/1").ends_with(Path::new("742f31")));
        assert!(store.get("t/1", "md5:00").await.is_err());
        store.delete_thread("t/1").await.unwrap();
        store.delete_thread("t/1").await.unwrap();
        assert_eq!(store.get("t/1", &hash).await.unwrap(), None);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn a_hash_names_its_content() {
        let hash = hash_name(&Sha256::digest(b"x"));
        assert!(hash.starts_with("sha256:") && hash.len() == 71);
        assert!(matches(&hash, b"x"));
        assert!(!matches(&hash, b"y"));
    }

    #[test]
    fn a_threshold_of_zero_is_refused() {
        assert!(Offload::new().with_threshold(0).check().is_err());
        assert_eq!(Offload::new().threshold(), 65536);
    }
}
