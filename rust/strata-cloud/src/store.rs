//! The checkpoint store backed by Strata Cloud (`design/30-Strata-Cloud-Store.md`,
//! leases and the query port in `design/43-Cloud-Leases-And-Listing.md`).

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use datalium_strata::BoxFuture;
use datalium_strata::schema::{Checkpoint, CheckpointStatus, CheckpointWait, GraphIdentity};
use datalium_strata::store::{
    CheckpointLogEntry, CheckpointLogPage, CheckpointQuery, CheckpointStore, CheckpointStoreError,
    LatestCheckpoint, LeaseAttempt, LeasedSave, ThreadLease, ThreadPage, ThreadQuery, ThreadState,
    ThreadSummary, check_lease, check_lease_thread, check_lease_ttl, check_limit, lease_millis,
    timestamp_text,
};
use reqwest::Method;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::equivalence::equivalent;
use crate::error::CloudError;
use crate::options::StrataCloudOptions;
use crate::transport::{self, Request, Response, Transport};
use crate::wire;

/// What a save came to, for a caller of
/// [`StrataCloudCheckpointStore::save_checkpoint`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaveOutcome {
    /// Cloud has the checkpoint as the thread's latest: appended (`201`), or
    /// already the latest (`200`, the idempotent retry).
    Saved,
    /// A conditional save Cloud refused (`412`): something was appended since
    /// the expected sequence, and nothing was stored.
    NotLatest,
}

/// The checkpoint store backed by Strata Cloud: a drop-in [`CheckpointStore`]
/// and [`CheckpointQuery`] over the SDK wire contract v1. A save is
/// `POST /v1/sdk/checkpoints`, a load `GET /v1/sdk/checkpoints/latest`; a
/// thread Cloud doesn't know loads as `None`.
///
/// Failures are retried as contract §8 says, then reach the engine as a
/// [`CheckpointStoreError`] whose message is the [`CloudError`]'s
/// (match it with [`is_entitlement_exceeded`](crate::is_entitlement_exceeded));
/// [`save_checkpoint`](Self::save_checkpoint) and
/// [`load_checkpoint`](Self::load_checkpoint) return the typed error. The
/// engine never cancels a store call: each attempt is bounded by the request
/// timeout.
///
/// **Racing resumes.** [`load_latest`](CheckpointStore::load_latest) reports a
/// sequence, and [`save_if_latest`](CheckpointStore::save_if_latest) is a real
/// compare-and-set, only when Cloud answers with the
/// [`SEQUENCE_HEADER`](wire::SEQUENCE_HEADER) header. A Cloud without it gives
/// no sequence, so the engine's resume stamp is saved unconditionally: two
/// workers that receive the same decision can both resume the thread
/// (`design/28-Ending-A-Thread.md`, "Races"). Until Cloud has it, give workers
/// disjoint threads.
///
/// **Leases** (`design/43-Cloud-Leases-And-Listing.md`): the five lease
/// methods are Cloud's `/v1/sdk/leases` routes, on Cloud's clock, and a save
/// under a lease sends its token as
/// [`LEASE_TOKEN_HEADER`](wire::LEASE_TOKEN_HEADER): Cloud's `423` is
/// [`LeasedSave::LeaseLost`], never retried. An acquisition sends an
/// [`IDEMPOTENCY_KEY_HEADER`](wire::IDEMPOTENCY_KEY_HEADER), new for each
/// call, so a retry after a lost answer gets the lease back.
///
/// **The query port**: [`list_threads`](CheckpointQuery::list_threads) is
/// `GET /v1/sdk/threads`, one request per call, Cloud's page and cursor as
/// they come (a page with a metadata or graph filter may be short while more
/// follow); [`get_log`](CheckpointQuery::get_log) is
/// `GET /v1/sdk/checkpoints`, following Cloud's pages until it has `limit`
/// entries or the log ends. There is no retention port: purging is Cloud's.
pub struct StrataCloudCheckpointStore {
    transport: Transport,
}

impl StrataCloudCheckpointStore {
    pub fn new(options: StrataCloudOptions) -> Result<Self, CloudError> {
        Ok(Self {
            transport: Transport::new(options)?,
        })
    }

    /// Appends `checkpoint` to its thread; with `expected_sequence`, only if
    /// the thread's latest checkpoint is still at it, sent as
    /// [`IF_SEQUENCE_HEADER`](wire::IF_SEQUENCE_HEADER) (a Cloud that predates
    /// the header ignores it and appends).
    pub async fn save_checkpoint(
        &self,
        checkpoint: &Checkpoint,
        expected_sequence: Option<i64>,
    ) -> Result<SaveOutcome, CloudError> {
        Ok(
            match self.save(checkpoint, expected_sequence, None).await? {
                LeasedSave::Saved => SaveOutcome::Saved,
                _ => SaveOutcome::NotLatest,
            },
        )
    }

    /// The thread's latest checkpoint with the sequence Cloud reports in
    /// [`SEQUENCE_HEADER`](wire::SEQUENCE_HEADER), or no sequence without it;
    /// `None` when Cloud has no checkpoint for the thread.
    pub async fn load_checkpoint(
        &self,
        thread_id: &str,
    ) -> Result<Option<LatestCheckpoint>, CloudError> {
        let request = Request::new(
            wire::LOAD_ROUTE,
            Method::GET,
            wire::LOAD_PATH,
            Some(thread_id),
        )
        .with_query("thread_id", thread_id);
        let response = self.send(&request, read_retryable).await?;
        match response.status {
            404 if transport::read_code(&response.body).as_deref()
                == Some(wire::THREAD_NOT_FOUND_CODE) =>
            {
                Ok(None)
            }
            200 => {
                let checkpoint = serde_json::from_slice::<Checkpoint>(&response.body)
                    .map_err(|err| transport::invalid(&response, err.to_string()))?;
                Ok(Some(LatestCheckpoint {
                    checkpoint,
                    sequence: response.sequence,
                }))
            }
            _ => Err(transport::failure(&response)),
        }
    }

    async fn send(
        &self,
        request: &Request<'_>,
        retryable: fn(u16) -> bool,
    ) -> Result<Response, CloudError> {
        let options = &self.transport.options;
        self.transport
            .send(
                request,
                retryable,
                options.retry.max_attempts,
                options.request_timeout,
            )
            .await
    }

    /// The save behind every save method: conditional with
    /// `expected_sequence`, fenced with `lease`.
    async fn save(
        &self,
        checkpoint: &Checkpoint,
        expected_sequence: Option<i64>,
        lease: Option<&ThreadLease>,
    ) -> Result<LeasedSave, CloudError> {
        let body = serde_json::to_vec(checkpoint)
            .map_err(|err| CloudError::Config(format!("can't serialize the checkpoint: {err}")))?;
        let mut request = Request::new(
            wire::SAVE_ROUTE,
            Method::POST,
            wire::SAVE_PATH,
            Some(&checkpoint.thread_id),
        );
        request.body = Some(body);
        request.if_sequence = expected_sequence;
        request.lease_token = lease.map(|lease| lease.token);
        let response = self.send(&request, save_retryable).await?;
        match (response.status, expected_sequence) {
            (200 | 201, _) => Ok(LeasedSave::Saved),
            (412, Some(expected)) => {
                // Unleased, an earlier attempt that may have committed makes
                // this 412 possibly our own write. Under a lease Cloud
                // answers our own committed retry 200, so a 412 is always
                // another writer (contract §11).
                if lease.is_none()
                    && response.ambiguous
                    && self.is_latest(checkpoint, expected + 1).await?
                {
                    Ok(LeasedSave::Saved)
                } else {
                    Ok(LeasedSave::NotLatest)
                }
            }
            (423, _)
                if lease.is_some()
                    && transport::read_code(&response.body).as_deref()
                        == Some(wire::LEASE_LOST_CODE) =>
            {
                Ok(LeasedSave::LeaseLost)
            }
            _ => Err(transport::failure(&response)),
        }
    }

    /// Whether the thread's latest checkpoint is `checkpoint`, at `sequence`.
    async fn is_latest(&self, checkpoint: &Checkpoint, sequence: i64) -> Result<bool, CloudError> {
        Ok(match self.load_checkpoint(&checkpoint.thread_id).await? {
            Some(latest) => {
                latest.sequence.is_none_or(|s| s == sequence)
                    && equivalent(&latest.checkpoint, checkpoint)
            }
            None => false,
        })
    }

    async fn acquire(
        &self,
        thread_id: &str,
        owner: &str,
        ttl: Duration,
    ) -> Result<LeaseAttempt, CloudError> {
        let mut request = Request::new(
            wire::ACQUIRE_ROUTE,
            Method::POST,
            wire::LEASES_PATH,
            Some(thread_id),
        )
        .with_json(&json!({ "thread_id": thread_id, "owner": owner, "ttl_ms": lease_millis(ttl) }));
        // One key for the call, the same on every attempt: a retry after a
        // lost answer gets back the lease the first attempt took, where
        // without it the thread would be held by our own name (contract §11).
        request.idempotency_key = Some(uuid::Uuid::now_v7().to_string());
        let response = self.send(&request, read_retryable).await?;
        let answer: AcquireAnswer = read(&response)?;
        let lease = answer.lease.into_lease(&response)?;
        Ok(if answer.acquired {
            LeaseAttempt::Acquired(lease)
        } else {
            LeaseAttempt::Held(lease)
        })
    }

    async fn renew(
        &self,
        lease: &ThreadLease,
        ttl: Duration,
    ) -> Result<Option<ThreadLease>, CloudError> {
        let request = Request::new(
            wire::RENEW_ROUTE,
            Method::PUT,
            wire::LEASES_PATH,
            Some(&lease.thread_id),
        )
        .with_json(&json!({
            "thread_id": lease.thread_id,
            "owner": lease.owner,
            "token": lease.token,
            "ttl_ms": lease_millis(ttl),
        }));
        let response = self.send(&request, read_retryable).await?;
        let answer: LeaseAnswer = read(&response)?;
        answer
            .lease
            .map(|lease| lease.into_lease(&response))
            .transpose()
    }

    async fn release(&self, lease: &ThreadLease) -> Result<bool, CloudError> {
        let request = Request::new(
            wire::RELEASE_ROUTE,
            Method::DELETE,
            wire::LEASES_PATH,
            Some(&lease.thread_id),
        )
        .with_query("thread_id", lease.thread_id.as_str())
        .with_query("token", lease.token.to_string());
        let response = self.send(&request, read_retryable).await?;
        let answer: ReleaseAnswer = read(&response)?;
        Ok(answer.released)
    }

    async fn lease_of(&self, thread_id: &str) -> Result<Option<ThreadLease>, CloudError> {
        let request = Request::new(
            wire::GET_LEASE_ROUTE,
            Method::GET,
            wire::LEASES_PATH,
            Some(thread_id),
        )
        .with_query("thread_id", thread_id);
        let response = self.send(&request, read_retryable).await?;
        let answer: LeaseAnswer = read(&response)?;
        answer
            .lease
            .map(|lease| lease.into_lease(&response))
            .transpose()
    }

    async fn list(&self, query: &ThreadQuery) -> Result<ThreadPage, CloudError> {
        let mut request = Request::new(wire::THREADS_ROUTE, Method::GET, wire::THREADS_PATH, None);
        for state in &query.states {
            request = request.with_query("state", state.as_str());
        }
        if let Some(due_by) = query.due_by {
            request = request.with_query("due_by", timestamp_text(due_by));
        }
        if let Some(after) = query.updated_after {
            request = request.with_query("updated_after", timestamp_text(after));
        }
        if let Some(before) = query.updated_before {
            request = request.with_query("updated_before", timestamp_text(before));
        }
        for (key, value) in &query.metadata {
            request = request.with_query(format!("meta.{key}"), value.as_str());
        }
        if let Some(id) = &query.graph_id {
            request = request.with_query("graph_id", id.as_str());
        }
        if let Some(version) = &query.graph_version {
            request = request.with_query("graph_version", version.as_str());
        }
        request = request.with_query("limit", query.limit.to_string());
        if let Some(cursor) = &query.cursor {
            request = request.with_query("cursor", cursor.as_str());
        }
        let response = self.send(&request, read_retryable).await?;
        let answer: ThreadsAnswer = read(&response)?;
        let threads = answer
            .threads
            .into_iter()
            .map(|row| row.into_summary())
            .collect::<Result<Vec<_>, String>>()
            .map_err(|message| transport::invalid(&response, message))?;
        Ok(ThreadPage {
            threads,
            next_cursor: answer.next_cursor,
        })
    }

    async fn log(
        &self,
        thread_id: &str,
        after_sequence: Option<i64>,
        limit: usize,
    ) -> Result<CheckpointLogPage, CloudError> {
        let mut entries = Vec::new();
        let mut after = after_sequence;
        // Cloud may stop a page early, at about 4 MB of stored state, with
        // more to come: follow it until the page is full or the log ends.
        while entries.len() < limit {
            let mut request = Request::new(
                wire::LOG_ROUTE,
                Method::GET,
                wire::LOG_PATH,
                Some(thread_id),
            )
            .with_query("thread_id", thread_id);
            if let Some(after) = after {
                request = request.with_query("after_sequence", after.to_string());
            }
            request = request.with_query("limit", (limit - entries.len()).to_string());
            let response = self.send(&request, read_retryable).await?;
            let answer: LogAnswer = read(&response)?;
            let received = answer.entries.len();
            for entry in answer.entries {
                let checkpoint = serde_json::from_value::<Checkpoint>(entry.checkpoint)
                    .map_err(|err| transport::invalid(&response, err.to_string()))?;
                entries.push(CheckpointLogEntry {
                    sequence: entry.sequence,
                    checkpoint,
                });
            }
            match answer.next_after_sequence {
                Some(next) if received > 0 => after = Some(next),
                _ => break,
            }
        }
        entries.truncate(limit);
        Ok(CheckpointLogPage::new(entries, limit))
    }
}

/// Saves are retried on a `409` too: Cloud lost a race for a sequence and
/// says to try again. Never on `412` or `423`.
fn save_retryable(status: u16) -> bool {
    matches!(status, 408 | 409 | 429 | 500 | 502 | 503 | 504)
}

/// Every other call: loads, the lease routes and the listing.
fn read_retryable(status: u16) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
}

fn store_error(err: CloudError) -> CheckpointStoreError {
    CheckpointStoreError(err.to_string())
}

/// A `200`'s body as `T`; any other status is the call's failure.
fn read<T: serde::de::DeserializeOwned>(response: &Response) -> Result<T, CloudError> {
    if response.status != 200 {
        return Err(transport::failure(response));
    }
    serde_json::from_slice(&response.body)
        .map_err(|err| transport::invalid(response, err.to_string()))
}

#[derive(Deserialize)]
struct WireLease {
    thread_id: String,
    owner: String,
    token: i64,
    expires_at: String,
}

impl WireLease {
    fn into_lease(self, response: &Response) -> Result<ThreadLease, CloudError> {
        let expires_at = parse_time(&self.expires_at)
            .map_err(|message| transport::invalid(response, message))?;
        Ok(ThreadLease {
            thread_id: self.thread_id,
            owner: self.owner,
            token: self.token,
            expires_at,
        })
    }
}

#[derive(Deserialize)]
struct AcquireAnswer {
    acquired: bool,
    lease: WireLease,
}

#[derive(Deserialize)]
struct LeaseAnswer {
    lease: Option<WireLease>,
}

#[derive(Deserialize)]
struct ReleaseAnswer {
    released: bool,
}

#[derive(Deserialize)]
struct ThreadsAnswer {
    threads: Vec<WireSummary>,
    next_cursor: Option<String>,
}

/// A row of `GET /v1/sdk/threads`: the query port's `ThreadSummary` as
/// `strata thread list --json` prints it (contract §12).
#[derive(Deserialize)]
struct WireSummary {
    thread_id: String,
    sequence: i64,
    state: String,
    status: CheckpointStatus,
    #[serde(default)]
    suspended_node: Option<String>,
    #[serde(default)]
    next_node: Option<String>,
    #[serde(default)]
    waiting_at: Vec<String>,
    #[serde(default)]
    metadata: Option<HashMap<String, String>>,
    updated_at: String,
    schema_version: String,
    #[serde(default)]
    wait: Option<CheckpointWait>,
    #[serde(default)]
    graph: Option<GraphIdentity>,
}

impl WireSummary {
    fn into_summary(self) -> Result<ThreadSummary, String> {
        let state = ThreadState::parse(&self.state)
            .ok_or_else(|| format!("unknown thread state '{}'", self.state))?;
        let mut summary = ThreadSummary::new(
            self.thread_id,
            self.sequence,
            state,
            self.status,
            parse_time(&self.updated_at)?,
            self.schema_version,
        );
        summary.suspended_node = self.suspended_node;
        summary.next_node = self.next_node;
        summary.waiting_at = self.waiting_at;
        summary.metadata = self.metadata.unwrap_or_default();
        summary.wait = self.wait;
        summary.graph = self.graph;
        Ok(summary)
    }
}

#[derive(Deserialize)]
struct LogAnswer {
    entries: Vec<WireLogEntry>,
    next_after_sequence: Option<i64>,
}

#[derive(Deserialize)]
struct WireLogEntry {
    sequence: i64,
    checkpoint: Value,
}

fn parse_time(text: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(text)
        .map(|time| time.with_timezone(&Utc))
        .map_err(|err| format!("bad timestamp '{text}': {err}"))
}

impl CheckpointStore for StrataCloudCheckpointStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        Box::pin(async move {
            self.save_checkpoint(&checkpoint, None)
                .await
                .map(|_| ())
                .map_err(store_error)
        })
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        Box::pin(async move {
            self.load_checkpoint(thread_id)
                .await
                .map(|latest| latest.map(|latest| latest.checkpoint))
                .map_err(store_error)
        })
    }

    fn load_latest<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<LatestCheckpoint>, CheckpointStoreError>> {
        Box::pin(async move { self.load_checkpoint(thread_id).await.map_err(store_error) })
    }

    fn save_if_latest(
        &self,
        checkpoint: Checkpoint,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'_, Result<bool, CheckpointStoreError>> {
        Box::pin(async move {
            self.save_checkpoint(&checkpoint, expected_sequence)
                .await
                .map(|outcome| outcome == SaveOutcome::Saved)
                .map_err(store_error)
        })
    }

    fn try_acquire_lease<'a>(
        &'a self,
        thread_id: &'a str,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<LeaseAttempt, CheckpointStoreError>> {
        Box::pin(async move {
            check_lease(owner, ttl)?;
            self.acquire(thread_id, owner, ttl)
                .await
                .map_err(store_error)
        })
    }

    fn renew_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        Box::pin(async move {
            check_lease_ttl(ttl)?;
            self.renew(lease, ttl).await.map_err(store_error)
        })
    }

    fn release_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
    ) -> BoxFuture<'a, Result<bool, CheckpointStoreError>> {
        Box::pin(async move { self.release(lease).await.map_err(store_error) })
    }

    fn get_lease<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        Box::pin(async move { self.lease_of(thread_id).await.map_err(store_error) })
    }

    fn save_under_lease<'a>(
        &'a self,
        checkpoint: Checkpoint,
        lease: &'a ThreadLease,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'a, Result<LeasedSave, CheckpointStoreError>> {
        Box::pin(async move {
            check_lease_thread(&checkpoint, lease)?;
            self.save(&checkpoint, expected_sequence, Some(lease))
                .await
                .map_err(store_error)
        })
    }
}

impl CheckpointQuery for StrataCloudCheckpointStore {
    fn list_threads<'a>(
        &'a self,
        query: &'a ThreadQuery,
    ) -> BoxFuture<'a, Result<ThreadPage, CheckpointStoreError>> {
        Box::pin(async move {
            // What `ThreadQuery::validate` checks, but the cursor: Cloud's is
            // its own, opaque, and passed back as it came.
            check_limit(query.limit)?;
            if query.graph_version.is_some() && query.graph_id.is_none() {
                return Err(CheckpointStoreError(
                    "a thread query's graph_version needs a graph_id".to_string(),
                ));
            }
            self.list(query).await.map_err(store_error)
        })
    }

    fn get_log<'a>(
        &'a self,
        thread_id: &'a str,
        after_sequence: Option<i64>,
        limit: usize,
    ) -> BoxFuture<'a, Result<CheckpointLogPage, CheckpointStoreError>> {
        Box::pin(async move {
            check_limit(limit)?;
            self.log(thread_id, after_sequence, limit)
                .await
                .map_err(store_error)
        })
    }
}
