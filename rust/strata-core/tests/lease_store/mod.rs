//! A checkpoint store with thread leases for the engine's lease tests
//! (`design/32-Thread-Leases.md`): what `strata-memory`'s in-memory store
//! does, kept here because this crate's tests can't depend on it.
//!
//! Its clock is tokio's, so a paused runtime moves it with the run's own, plus
//! an offset a test adds to let time pass in the store alone: the worker that
//! stalls while the world goes on.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use datalium_strata::BoxFuture;
use datalium_strata::checkpoint::{
    CheckpointLogPage, CheckpointQuery, CheckpointStore, CheckpointStoreError, LatestCheckpoint,
    LeaseAttempt, ThreadCursor, ThreadLease, ThreadPage, ThreadQuery, ThreadSummary,
};
use datalium_strata::schema::Checkpoint;
use datalium_strata::store::{
    LeasedSave, check_lease, check_lease_thread, check_lease_ttl, updated_at_text,
};

/// The ttl every test's lease has.
pub const TTL: Duration = Duration::from_secs(30);

/// A thread's lease: its latest token, and who holds it until when (`None`
/// once released).
struct LeaseRow {
    token: i64,
    holder: Option<(String, DateTime<Utc>)>,
}

pub struct LeaseStore {
    logs: Mutex<HashMap<String, Vec<Checkpoint>>>,
    leases: Mutex<HashMap<String, LeaseRow>>,
    started: tokio::time::Instant,
    ahead: Mutex<Duration>,
}

impl LeaseStore {
    /// Must be created inside the runtime whose clock it follows.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            logs: Mutex::default(),
            leases: Mutex::default(),
            started: tokio::time::Instant::now(),
            ahead: Mutex::default(),
        })
    }

    /// Noon on 2026-10-09 when the store was created, moving with tokio's
    /// clock and whatever [`Self::advance`] added.
    pub fn now(&self) -> DateTime<Utc> {
        let elapsed = self.started.elapsed() + *self.ahead.lock().unwrap();
        Utc.with_ymd_and_hms(2026, 10, 9, 12, 0, 0).unwrap()
            + chrono::Duration::from_std(elapsed).unwrap()
    }

    /// Lets `by` pass in the store alone.
    pub fn advance(&self, by: Duration) {
        *self.ahead.lock().unwrap() += by;
    }

    /// The thread's lease runs out on the store's clock and `worker-2` takes it.
    pub fn take_over(&self, thread_id: &str) -> ThreadLease {
        self.advance(TTL + Duration::from_millis(1));
        self.acquire(thread_id, "worker-2")
            .expect("the expired lease is free to take")
    }

    /// `owner` takes the thread for [`TTL`], if it is free.
    pub fn acquire(&self, thread_id: &str, owner: &str) -> Option<ThreadLease> {
        self.try_acquire(thread_id, owner, TTL).unwrap().acquired()
    }

    pub fn held(&self, thread_id: &str) -> Option<ThreadLease> {
        let leases = self.leases.lock().unwrap();
        let row = leases.get(thread_id)?;
        let (owner, expires_at) = row.holder.as_ref()?;
        (*expires_at > self.now()).then(|| ThreadLease {
            thread_id: thread_id.to_string(),
            owner: owner.clone(),
            token: row.token,
            expires_at: *expires_at,
        })
    }

    pub fn log(&self, thread_id: &str) -> Vec<Checkpoint> {
        self.logs
            .lock()
            .unwrap()
            .get(thread_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn latest(&self, thread_id: &str) -> Checkpoint {
        self.log(thread_id).last().cloned().expect("a checkpoint")
    }

    fn push(&self, checkpoint: Checkpoint, expected: Option<i64>) -> bool {
        let mut logs = self.logs.lock().unwrap();
        let log = logs.entry(checkpoint.thread_id.clone()).or_default();
        let saved = expected.is_none_or(|expected| log.len() as i64 - 1 == expected);
        if saved {
            log.push(checkpoint);
        }
        saved
    }

    fn try_acquire(
        &self,
        thread_id: &str,
        owner: &str,
        ttl: Duration,
    ) -> Result<LeaseAttempt, CheckpointStoreError> {
        check_lease(owner, ttl)?;
        if let Some(held) = self.held(thread_id) {
            return Ok(LeaseAttempt::Held(held));
        }
        let mut leases = self.leases.lock().unwrap();
        let lease = ThreadLease {
            thread_id: thread_id.to_string(),
            owner: owner.to_string(),
            token: leases.get(thread_id).map_or(0, |row| row.token) + 1,
            expires_at: self.now() + chrono::Duration::from_std(ttl).unwrap(),
        };
        leases.insert(
            thread_id.to_string(),
            LeaseRow {
                token: lease.token,
                holder: Some((lease.owner.clone(), lease.expires_at)),
            },
        );
        Ok(LeaseAttempt::Acquired(lease))
    }

    fn renew(
        &self,
        lease: &ThreadLease,
        ttl: Duration,
    ) -> Result<Option<ThreadLease>, CheckpointStoreError> {
        check_lease_ttl(ttl)?;
        let mut leases = self.leases.lock().unwrap();
        let Some(row) = leases
            .get_mut(&lease.thread_id)
            .filter(|row| row.token == lease.token)
        else {
            return Ok(None);
        };
        let renewed = ThreadLease {
            expires_at: self.now() + chrono::Duration::from_std(ttl).unwrap(),
            ..lease.clone()
        };
        row.holder = Some((renewed.owner.clone(), renewed.expires_at));
        Ok(Some(renewed))
    }

    pub fn release(&self, lease: &ThreadLease) -> bool {
        let mut leases = self.leases.lock().unwrap();
        match leases
            .get_mut(&lease.thread_id)
            .filter(|row| row.token == lease.token)
        {
            Some(row) => {
                row.holder = None;
                true
            }
            None => false,
        }
    }

    fn save_leased(
        &self,
        checkpoint: Checkpoint,
        lease: &ThreadLease,
        expected: Option<i64>,
    ) -> Result<LeasedSave, CheckpointStoreError> {
        check_lease_thread(&checkpoint, lease)?;
        // Held across the append, so no acquisition can come between them.
        let leases = self.leases.lock().unwrap();
        if leases.get(&lease.thread_id).map(|row| row.token) != Some(lease.token) {
            return Ok(LeasedSave::LeaseLost);
        }
        Ok(if self.push(checkpoint, expected) {
            LeasedSave::Saved
        } else {
            LeasedSave::NotLatest
        })
    }
}

impl CheckpointStore for LeaseStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        self.push(checkpoint, None);
        Box::pin(std::future::ready(Ok(())))
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        Box::pin(std::future::ready(Ok(self.log(thread_id).last().cloned())))
    }

    fn load_latest<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<LatestCheckpoint>, CheckpointStoreError>> {
        let log = self.log(thread_id);
        let latest = log.last().map(|checkpoint| LatestCheckpoint {
            checkpoint: checkpoint.clone(),
            sequence: Some(log.len() as i64 - 1),
        });
        Box::pin(std::future::ready(Ok(latest)))
    }

    fn save_if_latest(
        &self,
        checkpoint: Checkpoint,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'_, Result<bool, CheckpointStoreError>> {
        Box::pin(std::future::ready(Ok(
            self.push(checkpoint, expected_sequence)
        )))
    }

    fn try_acquire_lease<'a>(
        &'a self,
        thread_id: &'a str,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<LeaseAttempt, CheckpointStoreError>> {
        Box::pin(std::future::ready(self.try_acquire(thread_id, owner, ttl)))
    }

    fn renew_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        Box::pin(std::future::ready(self.renew(lease, ttl)))
    }

    fn release_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
    ) -> BoxFuture<'a, Result<bool, CheckpointStoreError>> {
        Box::pin(std::future::ready(Ok(self.release(lease))))
    }

    fn get_lease<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        Box::pin(std::future::ready(Ok(self.held(thread_id))))
    }

    fn save_under_lease<'a>(
        &'a self,
        checkpoint: Checkpoint,
        lease: &'a ThreadLease,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'a, Result<LeasedSave, CheckpointStoreError>> {
        Box::pin(std::future::ready(self.save_leased(
            checkpoint,
            lease,
            expected_sequence,
        )))
    }
}

impl CheckpointQuery for LeaseStore {
    fn list_threads<'a>(
        &'a self,
        query: &'a ThreadQuery,
    ) -> BoxFuture<'a, Result<ThreadPage, CheckpointStoreError>> {
        let page = (|| {
            let cursor = query.validate()?;
            let logs = self.logs.lock().unwrap();
            let mut latest: Vec<(i64, Checkpoint)> = logs
                .values()
                .filter_map(|log| Some((log.len() as i64 - 1, log.last()?.clone())))
                .filter(|(_, checkpoint)| query.matches(checkpoint))
                .filter(|(_, checkpoint)| {
                    cursor.as_ref().is_none_or(|cursor| {
                        cursor.admits(updated_at_text(checkpoint), &checkpoint.thread_id)
                    })
                })
                .collect();
            latest.sort_by(|(_, a), (_, b)| {
                (updated_at_text(b), &b.thread_id).cmp(&(updated_at_text(a), &a.thread_id))
            });
            let more = latest.len() > query.limit;
            latest.truncate(query.limit);
            let next_cursor = more
                .then(|| latest.last())
                .flatten()
                .map(|(_, c)| ThreadCursor::new(updated_at_text(c), &c.thread_id).encode());
            Ok(ThreadPage {
                threads: latest
                    .iter()
                    .map(|(sequence, checkpoint)| ThreadSummary::of(*sequence, checkpoint))
                    .collect::<Result<_, _>>()?,
                next_cursor,
            })
        })();
        Box::pin(std::future::ready(page))
    }

    fn get_log<'a>(
        &'a self,
        _thread_id: &'a str,
        _after_sequence: Option<i64>,
        limit: usize,
    ) -> BoxFuture<'a, Result<CheckpointLogPage, CheckpointStoreError>> {
        Box::pin(std::future::ready(Ok(CheckpointLogPage::new(
            Vec::new(),
            limit,
        ))))
    }
}
