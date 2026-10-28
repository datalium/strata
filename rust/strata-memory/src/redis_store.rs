//! Redis `CheckpointStore` and `HistoryStore` adapters — the other half of the
//! "Redis/PostgreSQL" tier from `design/03-Architecture.md`'s memory blueprint,
//! and deliberately *not* SQL-shaped: no tables, no `sequence` column, no
//! `MAX(sequence) + 1`. Async, on the `redis` crate's connection manager: a
//! multiplexed connection that replaces itself when it drops
//! (`design/19-Async-Queryable-Store.md`, `design/34-Storage-Hardening.md`).
//!
//! **Layout.** One Redis `LIST` per thread and per session, plus the index
//! sorted sets listing threads reads:
//!
//! | Key | Content |
//! |---|---|
//! | `{prefix}:checkpoints:{thread_id}` | one JSON `Checkpoint` (the wire shape of `schema/checkpoint.schema.json`) per `save` |
//! | `{prefix}:threads` | every thread, scored by its latest checkpoint's `updated_at` |
//! | `{prefix}:threads:{state}` | the threads whose latest checkpoint is in that `ThreadState` |
//! | `{prefix}:threads:waking` | the waiting threads with a `wake_at` (`design/29-Event-Waits-And-Timers.md`), scored like the others |
//! | `{prefix}:threads:indexed` | present once the keyspace has been indexed |
//! | `{prefix}:leases:{thread_id}` | a held thread's lease, a hash of `owner` and `token` that expires with it (`design/32-Thread-Leases.md`) |
//! | `{prefix}:lease-tokens:{thread_id}` | the thread's latest lease token, a counter that never expires |
//! | `{prefix}:blobs:{thread_id}` | the thread's offloaded values, hash to content (`design/41-Checkpoint-Efficiency.md`), kept, expired and deleted with its list |
//! | `{prefix}:history:{session_id}` | one JSON `{"role","content","timestamp"}` per `append` |
//!
//! A score is the `updated_at` text's digits as a number (`2026-10-02T09:15:00Z`
//! scores `20261002091500`), which sorts like the text. A checkpoint's or
//! message's `sequence` is its position in its list, so it is not stored.
//! `save` is one Lua script ([`SAVE_SCRIPT`]) that appends and updates the
//! index atomically, so any number of concurrent writers — either runtime — is
//! safe. `Datalium.Strata.Storage`'s `RedisCheckpointStore` /
//! `RedisHistoryStore` use the same keys, JSON and scripts, so either runtime
//! can resume and list the other's threads.
//!
//! **Every writer to one prefix must maintain the index.** A runtime from
//! before SG-11 only `RPUSH`es, so its threads don't appear in listings until
//! [`RedisCheckpointStore::rebuild_index`] runs. A prefix is indexed once,
//! automatically, the first time a store connects to it.
//!
//! **Hardening** (`design/34-Storage-Hardening.md`). Every call runs under a
//! [`StoreRetry`]: a transient failure (a dropped connection, a timeout, a
//! server that is loading or busy) is repeated. A save is one script, so an
//! attempt that failed after it was sent may have run: the next attempt looks
//! at the list first, and only saves again if the checkpoint isn't there.
//! [`CheckpointRetention::purge`] deletes finished threads' lists, and
//! [`RedisStoreOptions::with_finished_thread_ttl`] lets Redis expire them by
//! itself: the save scripts give a completed or ended thread's list that long
//! to live, and clear the expiry if the thread is saved again in another
//! state. A thread whose list is gone leaves the index when a listing next
//! finds it, or with [`RedisCheckpointStore::rebuild_index`].
//!
//! **Not built:** TLS (the `redis` crate's TLS features aren't enabled, so
//! `rediss://` URLs don't work) and Redis Cluster/Sentinel (one save's keys
//! hash to different slots). Redis persistence (RDB/AOF) decides whether a
//! checkpoint survives a Redis restart; the adapter can't.

use std::collections::HashSet;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use datalium_strata::BoxFuture;
use datalium_strata::checkpoint::{
    CheckpointLogEntry, CheckpointLogPage, CheckpointQuery, CheckpointRetention, CheckpointStore,
    CheckpointStoreError, LatestCheckpoint, LeaseAttempt, PurgeStates, ThreadCursor, ThreadLease,
    ThreadPage, ThreadQuery, ThreadState, ThreadSummary,
};
use datalium_strata::history::{ChatMessage, HistoryStore, HistoryStoreError, MessageRole};
use datalium_strata::schema::Checkpoint;
use datalium_strata::store::{
    LeasedSave, check_lease, check_lease_thread, check_lease_ttl, check_limit, lease_millis,
    parse_timestamp, timestamp_text, updated_at_text,
};
use redis::AsyncCommands;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use serde_json::json;

use crate::deltas::{self, CheckpointWriter, Decoded, Deltas, Encoding, depth_of};
use crate::group_commit::record_saved;
use crate::offload::{self, BlobStore, Offload};
use crate::retry::{Failure, FaultHook, Retrier, SaveFate, StoreRetry, fate_of, fault_at};
use crate::support::{from_millis, json_err};

/// The key namespace used when none is given.
pub const DEFAULT_PREFIX: &str = "strata";

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Appends a checkpoint and moves its thread to the right index sets, in one
/// atomic step. Identical to `RedisCheckpointStore.SaveScript` in
/// `Datalium.Strata.Storage`.
///
/// `KEYS`: the thread's list, `{prefix}:threads`, `{prefix}:threads:waking`,
/// then the state sets in [`ThreadState::ALL`] order, then the thread's blob
/// hash (`{prefix}:blobs:{thread_id}`). `ARGV`: the checkpoint JSON, the
/// thread id, its score, its state's position (1 to the number of state sets),
/// and `1` if it is waiting with a `wake_at`
/// (`design/29-Event-Waits-And-Timers.md`), else `0`, in that order.
///
/// `ARGV[6]` is the finished-thread ttl in milliseconds
/// (`design/34-Storage-Hardening.md`, "Retention"; 0 or absent: none): a save
/// that leaves the thread completed or ended (positions 5 and 6) gives the
/// list and the blob hash that long to live, and any other save clears their
/// expiry.
///
/// `ARGV[7]`, when not empty, is the sequence of the row the checkpoint
/// relies on (`design/41-Checkpoint-Efficiency.md`, "Deltas" and "Offload"),
/// and `ARGV[8]` the SHA-1 of that row's element: when the thread's latest
/// element isn't that row any more the script writes nothing and returns -2,
/// and the store saves the checkpoint whole. From `ARGV[9]` on, pairs of a
/// blob's hash and content, written to the blob hash (`HSETNX`) once the
/// checks pass.
pub const SAVE_SCRIPT: &str = r"local blobs = KEYS[#KEYS]
if ARGV[7] and ARGV[7] ~= '' and (redis.call('LLEN', KEYS[1]) - 1 ~= tonumber(ARGV[7])
  or redis.sha1hex(redis.call('LINDEX', KEYS[1], -1) or '') ~= ARGV[8]) then return -2 end
for i = 9, #ARGV, 2 do redis.call('HSETNX', blobs, ARGV[i], ARGV[i + 1]) end
redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
if ARGV[5] == '1' then
  redis.call('ZADD', KEYS[3], ARGV[3], ARGV[2])
else
  redis.call('ZREM', KEYS[3], ARGV[2])
end
local position = tonumber(ARGV[4])
for i = 4, #KEYS - 1 do
  if i - 3 == position then
    redis.call('ZADD', KEYS[i], ARGV[3], ARGV[2])
  else
    redis.call('ZREM', KEYS[i], ARGV[2])
  end
end
if position == 5 or position == 6 then
  local ttl = tonumber(ARGV[6] or '') or 0
  if ttl > 0 then
    redis.call('PEXPIRE', KEYS[1], ttl)
    redis.call('PEXPIRE', blobs, ttl)
  end
else
  redis.call('PERSIST', KEYS[1])
  redis.call('PERSIST', blobs)
end
return redis.call('LLEN', KEYS[1])";

/// [`SAVE_SCRIPT`], only if the thread's list still ends at the sequence
/// `ARGV[6]` (`design/28-Ending-A-Thread.md`, "Races"): returns 1 if it saved,
/// 0 if anything was appended since. `KEYS` and the first five `ARGV` as for
/// [`SAVE_SCRIPT`]; the ttl is `ARGV[7]`, the row relied on `ARGV[8]` and
/// `ARGV[9]` (-2 when it moved), the blobs from `ARGV[10]` on. Identical to
/// `RedisCheckpointStore.SaveIfLatestScript`.
pub const SAVE_IF_LATEST_SCRIPT: &str = r"local blobs = KEYS[#KEYS]
if redis.call('LLEN', KEYS[1]) - 1 ~= tonumber(ARGV[6]) then return 0 end
if ARGV[8] and ARGV[8] ~= '' and (redis.call('LLEN', KEYS[1]) - 1 ~= tonumber(ARGV[8])
  or redis.sha1hex(redis.call('LINDEX', KEYS[1], -1) or '') ~= ARGV[9]) then return -2 end
for i = 10, #ARGV, 2 do redis.call('HSETNX', blobs, ARGV[i], ARGV[i + 1]) end
redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
if ARGV[5] == '1' then
  redis.call('ZADD', KEYS[3], ARGV[3], ARGV[2])
else
  redis.call('ZREM', KEYS[3], ARGV[2])
end
local position = tonumber(ARGV[4])
for i = 4, #KEYS - 1 do
  if i - 3 == position then
    redis.call('ZADD', KEYS[i], ARGV[3], ARGV[2])
  else
    redis.call('ZREM', KEYS[i], ARGV[2])
  end
end
if position == 5 or position == 6 then
  local ttl = tonumber(ARGV[7] or '') or 0
  if ttl > 0 then
    redis.call('PEXPIRE', KEYS[1], ttl)
    redis.call('PEXPIRE', blobs, ttl)
  end
else
  redis.call('PERSIST', KEYS[1])
  redis.call('PERSIST', blobs)
end
return 1";

/// [`SAVE_IF_LATEST_SCRIPT`], only if the thread's lease token is still
/// `ARGV[7]` (`design/32-Thread-Leases.md`, "Fencing"): returns -1, having
/// written nothing, when another owner acquired the thread since; else the
/// list's length if it saved, 0 if anything was appended since the sequence
/// `ARGV[6]` (empty for an unconditional save). `KEYS` as for
/// [`SAVE_SCRIPT`], then the thread's token counter last; the first five
/// `ARGV` as for [`SAVE_SCRIPT`]; the ttl is `ARGV[8]`, and the token counter
/// lives and dies with the list; the row relied on is `ARGV[9]` and
/// `ARGV[10]` (-2 when it moved), the blobs from `ARGV[11]` on. Identical to
/// `RedisCheckpointStore.SaveUnderLeaseScript`.
pub const SAVE_UNDER_LEASE_SCRIPT: &str = r"local blobs = KEYS[#KEYS - 1]
if redis.call('GET', KEYS[#KEYS]) ~= ARGV[7] then return -1 end
if ARGV[6] ~= '' and redis.call('LLEN', KEYS[1]) - 1 ~= tonumber(ARGV[6]) then return 0 end
if ARGV[9] and ARGV[9] ~= '' and (redis.call('LLEN', KEYS[1]) - 1 ~= tonumber(ARGV[9])
  or redis.sha1hex(redis.call('LINDEX', KEYS[1], -1) or '') ~= ARGV[10]) then return -2 end
for i = 11, #ARGV, 2 do redis.call('HSETNX', blobs, ARGV[i], ARGV[i + 1]) end
redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
if ARGV[5] == '1' then
  redis.call('ZADD', KEYS[3], ARGV[3], ARGV[2])
else
  redis.call('ZREM', KEYS[3], ARGV[2])
end
local position = tonumber(ARGV[4])
for i = 4, #KEYS - 2 do
  if i - 3 == position then
    redis.call('ZADD', KEYS[i], ARGV[3], ARGV[2])
  else
    redis.call('ZREM', KEYS[i], ARGV[2])
  end
end
if position == 5 or position == 6 then
  local ttl = tonumber(ARGV[8] or '') or 0
  if ttl > 0 then
    redis.call('PEXPIRE', KEYS[1], ttl)
    redis.call('PEXPIRE', blobs, ttl)
    redis.call('PEXPIRE', KEYS[#KEYS], ttl)
  end
else
  redis.call('PERSIST', KEYS[1])
  redis.call('PERSIST', blobs)
  redis.call('PERSIST', KEYS[#KEYS])
end
return redis.call('LLEN', KEYS[1])";

/// Takes a thread's lease unless it is held (`design/32-Thread-Leases.md`).
/// `KEYS`: the thread's lease hash (`{prefix}:leases:{thread_id}`, which
/// expires with the lease) and its token counter
/// (`{prefix}:lease-tokens:{thread_id}`, which never does). `ARGV`: the owner
/// and the ttl in milliseconds. Returns `{1, token, owner, expires_at}` for
/// the new lease, or `{0, …}` with the lease that holds the thread;
/// `expires_at` is Unix milliseconds on Redis's clock. Identical to
/// `RedisCheckpointStore.AcquireLeaseScript`.
pub const ACQUIRE_LEASE_SCRIPT: &str = r"local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
if redis.call('EXISTS', KEYS[1]) == 1 then
  local held = redis.call('HMGET', KEYS[1], 'owner', 'token')
  return {0, tonumber(held[2]), held[1], now + redis.call('PTTL', KEYS[1])}
end
local token = redis.call('INCR', KEYS[2])
redis.call('HSET', KEYS[1], 'owner', ARGV[1], 'token', token)
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return {1, token, ARGV[1], now + tonumber(ARGV[2])}";

/// Extends a lease whose token (`ARGV[3]`) is still the thread's latest,
/// restoring the hash if it had expired: returns the new `expires_at`, or 0
/// when another owner acquired the thread. `KEYS` as for
/// [`ACQUIRE_LEASE_SCRIPT`]; `ARGV`: the owner, the ttl in milliseconds, the
/// token. Identical to `RedisCheckpointStore.RenewLeaseScript`.
pub const RENEW_LEASE_SCRIPT: &str = r"if redis.call('GET', KEYS[2]) ~= ARGV[3] then return 0 end
redis.call('HSET', KEYS[1], 'owner', ARGV[1], 'token', ARGV[3])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
local time = redis.call('TIME')
return tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000) + tonumber(ARGV[2])";

/// Gives up a lease whose token (`ARGV[1]`) is still the thread's latest: 1
/// if it did, 0 when another owner acquired the thread. The token counter
/// stays. `KEYS` as for [`ACQUIRE_LEASE_SCRIPT`]. Identical to
/// `RedisCheckpointStore.ReleaseLeaseScript`.
pub const RELEASE_LEASE_SCRIPT: &str = r"if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1])
return 1";

/// The lease a thread is held under: `{token, owner, expires_at}`, or nil when
/// nobody holds it. `KEYS`: the thread's lease hash. Identical to
/// `RedisCheckpointStore.GetLeaseScript`.
pub const GET_LEASE_SCRIPT: &str = r"local held = redis.call('HMGET', KEYS[1], 'owner', 'token')
if not held[1] then return false end
local time = redis.call('TIME')
return {tonumber(held[2]), held[1],
  tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000) + redis.call('PTTL', KEYS[1])}";

/// Indexes one thread from its own latest checkpoint, read inside the script so
/// a save racing the index pass can't be overwritten by an older state. A
/// thread whose list is gone (it expired, or was deleted) is taken out of
/// every set. Identical to `RedisCheckpointStore.IndexScript`. `KEYS` as for
/// [`SAVE_SCRIPT`]; `ARGV`: the thread id.
pub const INDEX_SCRIPT: &str = r"local latest = redis.call('LINDEX', KEYS[1], -1)
if not latest then
  for i = 2, #KEYS do redis.call('ZREM', KEYS[i], ARGV[1]) end
  return 0
end
local checkpoint = cjson.decode(latest)
local state = checkpoint.status
local updated = checkpoint.created_at
if checkpoint.resumed_at ~= nil and checkpoint.resumed_at ~= cjson.null then
  state = 'resumed'
  updated = checkpoint.resumed_at
end
local level = checkpoint
while level.nested ~= nil and level.nested ~= cjson.null do
  level = level.nested
end
local wait = level.wait
local waking = state == 'interrupted' and wait ~= nil and wait ~= cjson.null
  and wait.wake_at ~= nil and wait.wake_at ~= cjson.null
local score = string.gsub(updated, '%D', '')
local states = {'interrupted', 'failed', 'cancelled', 'resumed', 'completed', 'ended', 'running'}
redis.call('ZADD', KEYS[2], score, ARGV[1])
if waking then
  redis.call('ZADD', KEYS[3], score, ARGV[1])
else
  redis.call('ZREM', KEYS[3], ARGV[1])
end
for i = 4, #KEYS do
  if states[i - 3] == state then
    redis.call('ZADD', KEYS[i], score, ARGV[1])
  else
    redis.call('ZREM', KEYS[i], ARGV[1])
  end
end
return 1";

/// Deletes one thread if it is done with (`design/34-Storage-Hardening.md`,
/// "Retention"), decided from its own latest checkpoint in the same atomic
/// step: returns 1 if it deleted the thread's list, token counter, blob hash
/// and index entries, 0 if the thread changed, is too new, isn't in a named
/// state, or is held. `KEYS`: the thread's list and the index sets as for
/// [`INDEX_SCRIPT`], then the thread's lease hash, its token counter and its
/// blob hash. `ARGV`: the thread id, the score its latest checkpoint must be
/// below, then the states to purge. Identical to
/// `RedisCheckpointStore.PurgeScript`.
pub const PURGE_SCRIPT: &str = r"local sets = #KEYS - 3
local latest = redis.call('LINDEX', KEYS[1], -1)
if not latest then
  for i = 2, sets do redis.call('ZREM', KEYS[i], ARGV[1]) end
  return 0
end
local checkpoint = cjson.decode(latest)
local state = checkpoint.status
local updated = checkpoint.created_at
if checkpoint.resumed_at ~= nil and checkpoint.resumed_at ~= cjson.null then
  state = 'resumed'
  updated = checkpoint.resumed_at
end
local score = string.gsub(updated, '%D', '')
if tonumber(score) >= tonumber(ARGV[2]) then return 0 end
local named = false
for i = 3, #ARGV do
  if ARGV[i] == state then named = true end
end
if not named then return 0 end
if redis.call('EXISTS', KEYS[#KEYS - 2]) == 1 then return 0 end
redis.call('DEL', KEYS[1], KEYS[#KEYS - 1], KEYS[#KEYS])
for i = 2, sets do redis.call('ZREM', KEYS[i], ARGV[1]) end
return 1";

/// Up to `ARGV[3]` members of the sorted set `KEYS[1]` after a cursor, newest
/// first, with their scores — positioned atomically, so a thread that moves
/// while the page is read can't shift the ones after it. `ARGV[1]`/`ARGV[2]`
/// are the cursor's score and thread id (empty for none). Identical to
/// `RedisCheckpointStore.CandidatesScript`.
pub const CANDIDATES_SCRIPT: &str = r"local start = 0
if ARGV[1] ~= '' then
  local score = redis.call('ZSCORE', KEYS[1], ARGV[2])
  if score and tonumber(score) == tonumber(ARGV[1]) then
    start = redis.call('ZREVRANK', KEYS[1], ARGV[2]) + 1
  else
    start = redis.call('ZCOUNT', KEYS[1], '(' .. ARGV[1], '+inf')
    local ties = redis.call('ZRANGE', KEYS[1], ARGV[1], ARGV[1], 'BYSCORE')
    for _, member in ipairs(ties) do
      if member > ARGV[2] then start = start + 1 end
    end
  end
end
return redis.call('ZRANGE', KEYS[1], start, start + tonumber(ARGV[3]) - 1, 'REV', 'WITHSCORES')";

static SAVE: LazyLock<redis::Script> = LazyLock::new(|| redis::Script::new(SAVE_SCRIPT));
static SAVE_IF_LATEST: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(SAVE_IF_LATEST_SCRIPT));
static SAVE_UNDER_LEASE: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(SAVE_UNDER_LEASE_SCRIPT));
static ACQUIRE_LEASE: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(ACQUIRE_LEASE_SCRIPT));
static RENEW_LEASE: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(RENEW_LEASE_SCRIPT));
static RELEASE_LEASE: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(RELEASE_LEASE_SCRIPT));
static GET_LEASE: LazyLock<redis::Script> = LazyLock::new(|| redis::Script::new(GET_LEASE_SCRIPT));
static INDEX: LazyLock<redis::Script> = LazyLock::new(|| redis::Script::new(INDEX_SCRIPT));
static PURGE: LazyLock<redis::Script> = LazyLock::new(|| redis::Script::new(PURGE_SCRIPT));
static CANDIDATES: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(CANDIDATES_SCRIPT));

/// How many candidates one listing call examines at most, per page slot.
const EXAMINED_PER_SLOT: usize = 10;

/// Options of the Redis stores (`design/34-Storage-Hardening.md`). Mirrors
/// the .NET `RedisStoreOptions`.
#[derive(Clone)]
pub struct RedisStoreOptions {
    prefix: String,
    retry: StoreRetry,
    finished_thread_ttl: Option<Duration>,
    deltas: Option<Deltas>,
    offload: Option<Offload>,
    fault: Option<FaultHook>,
}

impl Default for RedisStoreOptions {
    fn default() -> Self {
        Self {
            prefix: DEFAULT_PREFIX.to_string(),
            retry: StoreRetry::new(),
            finished_thread_ttl: None,
            deltas: None,
            offload: None,
            fault: None,
        }
    }
}

impl std::fmt::Debug for RedisStoreOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisStoreOptions")
            .field("prefix", &self.prefix)
            .field("retry", &self.retry)
            .field("finished_thread_ttl", &self.finished_thread_ttl)
            .field("deltas", &self.deltas)
            .field("offload", &self.offload)
            .finish_non_exhaustive()
    }
}

impl RedisStoreOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// The key namespace: keys are `{prefix}:checkpoints:…`. The way to share
    /// one Redis between applications or environments.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// How a call that failed for a transient reason is repeated.
    /// [`StoreRetry::none`] turns it off.
    pub fn with_retry(mut self, retry: StoreRetry) -> Self {
        self.retry = retry;
        self
    }

    /// Lets a thread's keys expire `ttl` after it completes or ends, and stop
    /// expiring if it is saved again in another state. Off unless set; at
    /// least a millisecond. The history stores ignore it.
    pub fn with_finished_thread_ttl(mut self, ttl: Duration) -> Self {
        self.finished_thread_ttl = Some(ttl);
        self
    }

    /// Stores a `running` checkpoint as the changes to the thread's previous
    /// row, with a whole row every [`Deltas::with_full_every`]
    /// (`design/41-Checkpoint-Efficiency.md`, "Deltas"). Off by default. A
    /// store reads rows stored this way whatever its own options say. The
    /// history stores ignore it.
    pub fn with_deltas(mut self, deltas: Deltas) -> Self {
        self.deltas = Some(deltas);
        self
    }

    /// The deltas the checkpoint store writes, if any.
    pub fn deltas(&self) -> Option<&Deltas> {
        self.deltas.as_ref()
    }

    /// Stores each top-level value of a level's state whose JSON is at least
    /// [`Offload::with_threshold`] bytes once, in a blob store, and refers to
    /// it (`design/41-Checkpoint-Efficiency.md`, "Offload"). Off by default.
    /// The blobs are read from where these options say, whatever the
    /// threshold: the thread's blob hash in this Redis unless
    /// [`Offload::with_blobs`] names another store. The history stores ignore
    /// it.
    pub fn with_offload(mut self, offload: Offload) -> Self {
        self.offload = Some(offload);
        self
    }

    /// The offload the checkpoint store does, if any.
    pub fn offload(&self) -> Option<&Offload> {
        self.offload.as_ref()
    }

    /// For tests: `fault` is called at named points of a call
    /// (`"save:before"`, `"save:after"`, `"save:recheck"`), where `true` fails
    /// the call as a lost connection would.
    #[doc(hidden)]
    pub fn with_fault(mut self, fault: impl Fn(&str) -> bool + Send + Sync + 'static) -> Self {
        self.fault = Some(Arc::new(fault));
        self
    }
}

/// Whether `code`, a Redis error reply's first word, says the server is
/// there and can't answer yet (`design/34-Storage-Hardening.md`, "Retries"):
/// it is loading its data, busy running a script, or asks to be tried again.
pub fn is_transient_reply(code: &str) -> bool {
    matches!(code, "LOADING" | "BUSY" | "TRYAGAIN")
}

impl From<redis::RedisError> for Failure {
    fn from(err: redis::RedisError) -> Self {
        let transient = err.is_io_error()
            || err.is_timeout()
            || err.is_connection_dropped()
            || err.code().is_some_and(is_transient_reply);
        Failure {
            message: err.to_string(),
            transient,
            // A connection that was refused carried nothing to the server.
            never_sent: err.is_connection_refusal(),
            class: format!("{:?}", err.kind()),
        }
    }
}

impl From<redis::ParsingError> for Failure {
    fn from(err: redis::ParsingError) -> Self {
        Failure::fatal(err.to_string())
    }
}

/// A connection that replaces itself when it drops: the command that finds it
/// dropped fails, the ones made while the manager reconnects wait for it, and
/// the store's retry policy makes the failed ones again.
pub(crate) async fn connect(url: &str) -> Result<ConnectionManager, redis::RedisError> {
    let client = redis::Client::open(url)?;
    // One plain attempt first. The manager tries an unreachable server several
    // times before it says so; the first connection is the store's own policy
    // to repeat, so a wrong URL is an error in seconds, not minutes.
    let probe = redis::AsyncConnectionConfig::new()
        .set_connection_timeout(Some(CONNECT_TIMEOUT))
        .set_response_timeout(Some(RESPONSE_TIMEOUT));
    drop(
        client
            .get_multiplexed_async_connection_with_config(&probe)
            .await?,
    );
    let config = ConnectionManagerConfig::new()
        .set_connection_timeout(Some(CONNECT_TIMEOUT))
        .set_response_timeout(Some(RESPONSE_TIMEOUT));
    ConnectionManager::new_lazy_with_config(client, config)
}

/// [`connect`] under `retry`.
async fn connect_retrying(url: &str, retry: &Retrier) -> Result<ConnectionManager, Failure> {
    retry
        .run("migrate", || async {
            connect(url).await.map_err(Failure::from)
        })
        .await
}

/// The index score of a stored timestamp: its digits, as a number.
fn score(updated_at: &str) -> Result<u64, CheckpointStoreError> {
    let canonical = timestamp_text(parse_timestamp(updated_at)?);
    let digits: String = canonical.chars().filter(char::is_ascii_digit).collect();
    digits
        .parse()
        .map_err(|_| CheckpointStoreError(format!("bad timestamp '{updated_at}'")))
}

/// The inverse of [`score`], for cursors.
fn score_text(score: u64) -> String {
    let d = format!("{score:014}");
    format!(
        "{}-{}-{}T{}:{}:{}Z",
        &d[0..4],
        &d[4..6],
        &d[6..8],
        &d[8..10],
        &d[10..12],
        &d[12..14]
    )
}

/// `text` with Redis glob characters escaped, for `SCAN MATCH`.
fn glob_escape(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for c in text.chars() {
        if matches!(c, '*' | '?' | '[' | ']' | '\\') {
            escaped.push('\\');
        }
        escaped.push(c);
    }
    escaped
}

/// Redis-backed [`CheckpointStore`]; see the module docs for the layout.
pub struct RedisCheckpointStore {
    conn: ConnectionManager,
    prefix: String,
    retry: Retrier,
    /// The finished-thread ttl in milliseconds the save scripts are given; 0
    /// for none.
    finished_ttl: i64,
    fault: Option<FaultHook>,
    writer: Option<CheckpointWriter>,
    /// Where offloaded values go when not in this Redis.
    blobs: Option<Arc<dyn BlobStore>>,
}

/// What a save script did.
enum Appended {
    /// Saved, at this sequence.
    Saved(i64),
    NotLatest,
    LeaseLost,
    /// A save whose base isn't the thread's latest any more: nothing written.
    Rebase,
}

/// The token a row is checked by when a save relies on it: the SHA-1 of its
/// element, as Redis's `sha1hex` gives it.
fn token(element: &str) -> String {
    sha1_smol::Sha1::from(element).digest().to_string()
}

impl RedisCheckpointStore {
    pub async fn connect(url: &str) -> Result<Self, CheckpointStoreError> {
        Self::connect_with(url, RedisStoreOptions::new()).await
    }

    /// Like [`connect`](Self::connect), but keys are `{prefix}:checkpoints:…` —
    /// the way to share one Redis between applications or environments. Indexes
    /// the prefix's existing threads the first time any store connects to it.
    pub async fn connect_with_prefix(
        url: &str,
        prefix: &str,
    ) -> Result<Self, CheckpointStoreError> {
        Self::connect_with(url, RedisStoreOptions::new().with_prefix(prefix)).await
    }

    /// [`connect`](Self::connect) with `options`: the prefix, the retry
    /// policy and the finished-thread ttl.
    pub async fn connect_with(
        url: &str,
        options: RedisStoreOptions,
    ) -> Result<Self, CheckpointStoreError> {
        let finished_ttl = match options.finished_thread_ttl {
            None => 0,
            Some(ttl) if ttl < Duration::from_millis(1) => {
                return Err(CheckpointStoreError(
                    "a finished-thread ttl must be at least a millisecond".to_string(),
                ));
            }
            Some(ttl) => i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX),
        };
        let writer = CheckpointWriter::new(options.deltas.as_ref(), options.offload.as_ref())?;
        let blobs = options
            .offload
            .as_ref()
            .and_then(|offload| offload.blobs().cloned());
        let retry = Retrier::new(options.retry, "redis");
        let store = Self {
            conn: connect_retrying(url, &retry).await?,
            prefix: options.prefix,
            retry,
            finished_ttl,
            fault: options.fault,
            writer,
            blobs,
        };
        store
            .retry
            .run("migrate", || store.ensure_indexed())
            .await?;
        Ok(store)
    }

    fn key(&self, thread_id: &str) -> String {
        format!("{}:checkpoints:{thread_id}", self.prefix)
    }

    fn threads_key(&self) -> String {
        format!("{}:threads", self.prefix)
    }

    fn state_key(&self, state: ThreadState) -> String {
        format!("{}:threads:{}", self.prefix, state.as_str())
    }

    fn indexed_key(&self) -> String {
        format!("{}:threads:indexed", self.prefix)
    }

    /// The waiting threads with a `wake_at`, scored by `updated_at` like every
    /// other index set, so a due listing keeps the listing order and cursor
    /// (`design/29-Event-Waits-And-Timers.md`).
    fn waking_key(&self) -> String {
        format!("{}:threads:waking", self.prefix)
    }

    /// A held thread's lease, `owner` and `token`, expiring with it
    /// (`design/32-Thread-Leases.md`).
    fn lease_key(&self, thread_id: &str) -> String {
        format!("{}:leases:{thread_id}", self.prefix)
    }

    /// A thread's latest lease token: a counter that outlives every lease.
    fn lease_token_key(&self, thread_id: &str) -> String {
        format!("{}:lease-tokens:{thread_id}", self.prefix)
    }

    /// A thread's offloaded values, hash to content
    /// (`design/41-Checkpoint-Efficiency.md`, "Offload"), kept, expired and
    /// deleted with its list.
    fn blob_key(&self, thread_id: &str) -> String {
        format!("{}:blobs:{thread_id}", self.prefix)
    }

    /// The script `KEYS` for `thread_id`: its list, the all-threads set, the
    /// waking set and the state sets.
    fn index_keys(&self, thread_id: &str) -> Vec<String> {
        let mut keys = vec![self.key(thread_id), self.threads_key(), self.waking_key()];
        keys.extend(ThreadState::ALL.iter().map(|state| self.state_key(*state)));
        keys
    }

    async fn ensure_indexed(&self) -> Result<(), Failure> {
        let mut conn = self.conn.clone();
        let indexed: bool = conn.exists(self.indexed_key()).await?;
        if !indexed {
            self.index_all().await?;
            let () = conn.set(self.indexed_key(), "1").await?;
        }
        Ok(())
    }

    /// Indexes every thread under this prefix from its latest checkpoint:
    /// repairs the index after writes from a runtime that predates it, and
    /// drops threads whose lists are gone. Safe to run while others write;
    /// returns how many threads it indexed.
    pub async fn rebuild_index(&self) -> Result<usize, CheckpointStoreError> {
        Ok(self.retry.run("migrate", || self.index_all()).await?)
    }

    /// Takes `thread_id` out of the index if its list is gone, or indexes it
    /// from its latest checkpoint: 1 if it is indexed, 0 if it is gone.
    async fn index_one(&self, thread_id: &str) -> Result<i64, Failure> {
        let mut invocation = INDEX.prepare_invoke();
        for index_key in self.index_keys(thread_id) {
            invocation.key(index_key);
        }
        Ok(invocation
            .arg(thread_id)
            .invoke_async(&mut self.conn.clone())
            .await?)
    }

    async fn index_all(&self) -> Result<usize, Failure> {
        let mut conn = self.conn.clone();
        let list_prefix = format!("{}:checkpoints:", self.prefix);
        let pattern = format!("{}*", glob_escape(&list_prefix));
        let mut cursor: u64 = 0;
        let mut indexed = 0;
        loop {
            let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(500)
                .query_async(&mut conn)
                .await?;
            for key in keys {
                let Some(thread_id) = key.strip_prefix(&list_prefix) else {
                    continue;
                };
                indexed += self.index_one(thread_id).await? as usize;
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
        // A thread whose list expired is in no scan: look for it in the set
        // of every thread.
        let listed: Vec<String> = conn.zrange(self.threads_key(), 0, -1).await?;
        for chunk in listed.chunks(500) {
            let mut pipe = redis::pipe();
            for thread_id in chunk {
                pipe.exists(self.key(thread_id));
            }
            let present: Vec<bool> = pipe.query_async(&mut conn).await?;
            for (thread_id, present) in chunk.iter().zip(present) {
                if !present {
                    self.index_one(thread_id).await?;
                }
            }
        }
        Ok(indexed)
    }

    async fn save_now(&self, checkpoint: Checkpoint) -> Result<(), CheckpointStoreError> {
        self.save_if_latest_now(checkpoint, None).await.map(|_| ())
    }

    async fn save_if_latest_now(
        &self,
        checkpoint: Checkpoint,
        expected: Option<i64>,
    ) -> Result<bool, CheckpointStoreError> {
        let saved = self.append(checkpoint, expected, None).await?;
        Ok(saved == LeasedSave::Saved)
    }

    async fn save_under_lease_now(
        &self,
        checkpoint: Checkpoint,
        lease: &ThreadLease,
        expected: Option<i64>,
    ) -> Result<LeasedSave, CheckpointStoreError> {
        check_lease_thread(&checkpoint, lease)?;
        self.append(checkpoint, expected, Some(lease.token)).await
    }

    /// An acquisition that may have run is not made again: the store can't
    /// tell its own lease from another process's with the same owner name.
    /// Only a script that never reached the server is repeated
    /// (`design/34-Storage-Hardening.md`).
    async fn acquire_lease_now(
        &self,
        thread_id: &str,
        owner: &str,
        ttl: Duration,
    ) -> Result<LeaseAttempt, CheckpointStoreError> {
        check_lease(owner, ttl)?;
        let mut number = 1;
        loop {
            match self.acquire_once(thread_id, owner, ttl).await {
                Ok(attempt) => return Ok(attempt),
                Err(failure) => {
                    if !failure.never_sent || !self.retry.again("lease", number, &failure).await {
                        return Err(failure.into());
                    }
                }
            }
            number += 1;
        }
    }

    async fn acquire_once(
        &self,
        thread_id: &str,
        owner: &str,
        ttl: Duration,
    ) -> Result<LeaseAttempt, Failure> {
        let (acquired, token, owner, expires_at): (i64, i64, String, i64) = ACQUIRE_LEASE
            .key(self.lease_key(thread_id))
            .key(self.lease_token_key(thread_id))
            .arg(owner)
            .arg(lease_millis(ttl))
            .invoke_async(&mut self.conn.clone())
            .await?;
        let lease = ThreadLease {
            thread_id: thread_id.to_string(),
            owner,
            token,
            expires_at: from_millis(expires_at)?,
        };
        Ok(if acquired == 1 {
            LeaseAttempt::Acquired(lease)
        } else {
            LeaseAttempt::Held(lease)
        })
    }

    /// Renewing twice is renewing once, so a transient failure is repeated.
    async fn renew_lease_now(
        &self,
        lease: &ThreadLease,
        ttl: Duration,
    ) -> Result<Option<ThreadLease>, CheckpointStoreError> {
        check_lease_ttl(ttl)?;
        Ok(self
            .retry
            .run("lease", || self.renew_once(lease, ttl))
            .await?)
    }

    async fn renew_once(
        &self,
        lease: &ThreadLease,
        ttl: Duration,
    ) -> Result<Option<ThreadLease>, Failure> {
        let expires_at: i64 = RENEW_LEASE
            .key(self.lease_key(&lease.thread_id))
            .key(self.lease_token_key(&lease.thread_id))
            .arg(&lease.owner)
            .arg(lease_millis(ttl))
            .arg(lease.token)
            .invoke_async(&mut self.conn.clone())
            .await?;
        if expires_at == 0 {
            return Ok(None);
        }
        Ok(Some(ThreadLease {
            expires_at: from_millis(expires_at)?,
            ..lease.clone()
        }))
    }

    /// Releasing twice is releasing once: the token is still the thread's
    /// latest, and the hash is already gone.
    async fn release_lease_now(&self, lease: &ThreadLease) -> Result<bool, CheckpointStoreError> {
        Ok(self.retry.run("lease", || self.release_once(lease)).await?)
    }

    async fn release_once(&self, lease: &ThreadLease) -> Result<bool, Failure> {
        let released: i64 = RELEASE_LEASE
            .key(self.lease_key(&lease.thread_id))
            .key(self.lease_token_key(&lease.thread_id))
            .arg(lease.token)
            .invoke_async(&mut self.conn.clone())
            .await?;
        Ok(released == 1)
    }

    async fn get_lease_now(
        &self,
        thread_id: &str,
    ) -> Result<Option<ThreadLease>, CheckpointStoreError> {
        Ok(self
            .retry
            .run("lease", || self.get_lease_once(thread_id))
            .await?)
    }

    async fn get_lease_once(&self, thread_id: &str) -> Result<Option<ThreadLease>, Failure> {
        let held: Option<(i64, String, i64)> = GET_LEASE
            .key(self.lease_key(thread_id))
            .invoke_async(&mut self.conn.clone())
            .await?;
        Ok(held
            .map(|(token, owner, expires_at)| {
                Ok::<_, CheckpointStoreError>(ThreadLease {
                    thread_id: thread_id.to_string(),
                    owner,
                    token,
                    expires_at: from_millis(expires_at)?,
                })
            })
            .transpose()?)
    }

    /// Makes a save under the retry policy so that it happens once
    /// (`design/34-Storage-Hardening.md`, "A save that may have happened"). A
    /// save is one script, so an attempt that failed after it was sent may
    /// have run: the next attempt looks at the list first, where the
    /// checkpoint would be, and only saves again if it isn't there. A
    /// conditional save whose place was taken by another checkpoint lost its
    /// race.
    async fn append(
        &self,
        checkpoint: Checkpoint,
        expected: Option<i64>,
        token: Option<i64>,
    ) -> Result<LeasedSave, CheckpointStoreError> {
        let mut encoding = self.writer.as_ref().map_or_else(Encoding::plain, |writer| {
            writer.prepare(&checkpoint, true, false)
        });
        if let Some(blobs) = &self.blobs {
            // Blobs in another store go before the row that refers to them.
            offload::put_all(&**blobs, &checkpoint.thread_id, &encoding.blobs).await?;
        }
        let mut json = serde_json::to_string(encoding.stored(&checkpoint)).map_err(json_err)?;
        let mut may_be_written = false;
        let mut number = 1;
        loop {
            match self
                .append_attempt(
                    &checkpoint,
                    &mut encoding,
                    &mut json,
                    expected,
                    token,
                    &mut may_be_written,
                )
                .await
            {
                Ok(saved) => return Ok(saved),
                Err(failure) => {
                    if !self.retry.again("save", number, &failure).await {
                        return Err(failure.into());
                    }
                }
            }
            number += 1;
        }
    }

    async fn append_attempt(
        &self,
        checkpoint: &Checkpoint,
        encoding: &mut Encoding,
        json: &mut String,
        expected: Option<i64>,
        token: Option<i64>,
        may_be_written: &mut bool,
    ) -> Result<LeasedSave, Failure> {
        if *may_be_written {
            fault_at(&self.fault, "save:recheck")?;
            // Where the checkpoint would be: right after the expected
            // sequence, or at the end. Whatever is there, the store no longer
            // knows which row is the thread's latest.
            let next = self
                .row(&checkpoint.thread_id, expected.map(|expected| expected + 1))
                .await?
                .map(|(_, stored, _, _)| stored.checkpoint);
            self.forget(&checkpoint.thread_id);
            match fate_of(checkpoint, next.as_ref(), expected.is_some()) {
                SaveFate::Saved => return Ok(LeasedSave::Saved),
                SaveFate::Lost => return Ok(LeasedSave::NotLatest),
                SaveFate::NotWritten => *may_be_written = false,
            }
        }
        let saved = match fault_at(&self.fault, "save:before") {
            Ok(()) => {
                self.append_encoded(checkpoint, encoding, json, expected, token)
                    .await
            }
            Err(injected) => Err(injected),
        };
        if let Err(failure) = &saved {
            *may_be_written = failure.transient && !failure.never_sent;
        }
        saved
    }

    /// Appends `json`, `checkpoint` as `encoding` stores it, or, when the row
    /// it relies on is no longer the thread's latest, the checkpoint whole,
    /// every blob written again; then records what it wrote.
    async fn append_encoded(
        &self,
        checkpoint: &Checkpoint,
        encoding: &mut Encoding,
        json: &mut String,
        expected: Option<i64>,
        token: Option<i64>,
    ) -> Result<LeasedSave, Failure> {
        let mut appended = self
            .append_once(checkpoint, json, expected, token, encoding)
            .await?;
        if matches!(appended, Appended::Rebase) {
            *encoding = encoding.whole(checkpoint);
            if let Some(blobs) = &self.blobs {
                offload::put_all(&**blobs, &checkpoint.thread_id, &encoding.blobs).await?;
            }
            *json = serde_json::to_string(encoding.stored(checkpoint)).map_err(json_err)?;
            appended = self
                .append_once(checkpoint, json, expected, token, encoding)
                .await?;
        }
        fault_at(&self.fault, "save:after")?;
        Ok(match appended {
            Appended::Saved(sequence) => {
                record_saved("redis", encoding, json.len());
                if let Some(writer) = &self.writer {
                    writer.saved(checkpoint, encoding, sequence, &self::token(json));
                }
                LeasedSave::Saved
            }
            Appended::LeaseLost => LeasedSave::LeaseLost,
            Appended::NotLatest | Appended::Rebase => {
                self.forget(&checkpoint.thread_id);
                LeasedSave::NotLatest
            }
        })
    }

    /// Forgets which row is `thread_id`'s latest, if the store remembers it.
    fn forget(&self, thread_id: &str) {
        if let Some(writer) = &self.writer {
            writer.forget(thread_id);
        }
    }

    /// Appends with [`SAVE_SCRIPT`], with [`SAVE_IF_LATEST_SCRIPT`] when there
    /// is a sequence to compare against, or with [`SAVE_UNDER_LEASE_SCRIPT`]
    /// when there is a lease `token` to check in the same atomic step; for a
    /// save that relies on the row before it, only if that row is still the
    /// thread's latest; with the blobs `encoding` writes when they are kept in
    /// this Redis.
    async fn append_once(
        &self,
        checkpoint: &Checkpoint,
        json: &str,
        expected: Option<i64>,
        token: Option<i64>,
        encoding: &Encoding,
    ) -> Result<Appended, Failure> {
        let state = ThreadState::of(checkpoint);
        let position = ThreadState::ALL
            .iter()
            .position(|s| *s == state)
            .expect("ALL holds every state")
            + 1;
        let waking = state == ThreadState::Interrupted
            && checkpoint
                .innermost_wait()
                .is_some_and(|wait| wait.wake_at.is_some());
        let mut invocation = match (token, expected) {
            (Some(_), _) => SAVE_UNDER_LEASE.prepare_invoke(),
            (None, Some(_)) => SAVE_IF_LATEST.prepare_invoke(),
            (None, None) => SAVE.prepare_invoke(),
        };
        for key in self.index_keys(&checkpoint.thread_id) {
            invocation.key(key);
        }
        invocation.key(self.blob_key(&checkpoint.thread_id));
        invocation
            .arg(json)
            .arg(&checkpoint.thread_id)
            .arg(score(updated_at_text(checkpoint))?)
            .arg(position)
            .arg(if waking { "1" } else { "0" });
        let base = encoding
            .base_sequence()
            .map_or_else(String::new, |base| base.to_string());
        let base_token = encoding.base_token().unwrap_or_default();
        // The blobs, after the arguments every script has.
        let blobs = |invocation: &mut redis::ScriptInvocation<'_>| {
            if self.blobs.is_none() {
                for blob in &encoding.blobs {
                    invocation.arg(&blob.hash).arg(&blob.content[..]);
                }
            }
        };
        Ok(if let Some(token) = token {
            invocation.key(self.lease_token_key(&checkpoint.thread_id));
            match expected {
                Some(expected) => invocation.arg(expected),
                None => invocation.arg(""),
            };
            invocation
                .arg(token)
                .arg(self.finished_ttl)
                .arg(&base)
                .arg(base_token);
            blobs(&mut invocation);
            let saved: i64 = invocation.invoke_async(&mut self.conn.clone()).await?;
            match saved {
                -2 => Appended::Rebase,
                -1 => Appended::LeaseLost,
                0 => Appended::NotLatest,
                length => Appended::Saved(length - 1),
            }
        } else if let Some(expected) = expected {
            invocation
                .arg(expected)
                .arg(self.finished_ttl)
                .arg(&base)
                .arg(base_token);
            blobs(&mut invocation);
            let saved: i64 = invocation.invoke_async(&mut self.conn.clone()).await?;
            match saved {
                -2 => Appended::Rebase,
                1 => Appended::Saved(expected + 1),
                _ => Appended::NotLatest,
            }
        } else {
            invocation.arg(self.finished_ttl).arg(&base).arg(base_token);
            blobs(&mut invocation);
            let length: i64 = invocation.invoke_async(&mut self.conn.clone()).await?;
            if length == -2 {
                Appended::Rebase
            } else {
                Appended::Saved(length - 1)
            }
        })
    }

    async fn load_latest_now(
        &self,
        thread_id: &str,
    ) -> Result<Option<LatestCheckpoint>, CheckpointStoreError> {
        Ok(self
            .retry
            .run("load", || self.load_latest_once(thread_id))
            .await?)
    }

    /// The newest element and the list's length, read together; the
    /// sequence is its position. A delta is read with the elements before it
    /// that it needs, in one more range read; the store then knows the
    /// thread's latest.
    async fn load_latest_once(&self, thread_id: &str) -> Result<Option<LatestCheckpoint>, Failure> {
        let Some((sequence, decoded, depth, token)) = self.row(thread_id, None).await? else {
            return Ok(None);
        };
        if let Some(writer) = &self.writer {
            writer.loaded(sequence, depth, &decoded, &token);
        }
        Ok(Some(LatestCheckpoint {
            checkpoint: decoded.checkpoint,
            sequence: Some(sequence),
        }))
    }

    async fn load_now(&self, thread_id: &str) -> Result<Option<Checkpoint>, CheckpointStoreError> {
        Ok(self
            .retry
            .run("load", || self.load_latest_once(thread_id))
            .await?
            .map(|latest| latest.checkpoint))
    }

    /// The thread's element at `sequence` (the newest when `None`), decoded,
    /// its offloaded values read, with its sequence, how far it is from a
    /// whole row and its token; one attempt.
    async fn row(
        &self,
        thread_id: &str,
        sequence: Option<i64>,
    ) -> Result<Option<(i64, Decoded, u32, String)>, Failure> {
        let (json, at) = match sequence {
            Some(sequence) => {
                let json: Option<String> = self
                    .conn
                    .clone()
                    .lindex(self.key(thread_id), sequence as isize)
                    .await?;
                (json, sequence)
            }
            None => {
                let (json, length): (Option<String>, i64) = redis::pipe()
                    .atomic()
                    .lindex(self.key(thread_id), -1)
                    .llen(self.key(thread_id))
                    .query_async(&mut self.conn.clone())
                    .await?;
                (json, length - 1)
            }
        };
        let Some(json) = json else {
            return Ok(None);
        };
        let stored: Checkpoint = serde_json::from_str(&json).map_err(json_err)?;
        let depth = depth_of(&stored);
        let mut rows = if depth == 0 {
            Vec::new()
        } else {
            self.range(thread_id, at - i64::from(depth), at - 1).await?
        };
        rows.push((at, stored));
        let from = rows.len() - 1;
        let decoded = self.resolve(thread_id, deltas::decode(rows, from)?).await?;
        Ok(decoded
            .into_iter()
            .next_back()
            .map(|row| (row.sequence, row, depth, token(&json))))
    }

    /// Reads the offloaded values `decoded` refers to, from the blob store
    /// the options name or from the thread's blob hash in one `HMGET`, and
    /// puts them in place.
    async fn resolve(
        &self,
        thread_id: &str,
        decoded: Vec<Decoded>,
    ) -> Result<Vec<Decoded>, Failure> {
        let wanted = deltas::wanted(&decoded);
        if wanted.is_empty() {
            return Ok(decoded);
        }
        let found = match &self.blobs {
            Some(blobs) => offload::read(&**blobs, thread_id, &wanted).await?,
            None => {
                let contents: Vec<Option<Vec<u8>>> = redis::cmd("HMGET")
                    .arg(self.blob_key(thread_id))
                    .arg(&wanted)
                    .query_async(&mut self.conn.clone())
                    .await?;
                wanted
                    .iter()
                    .zip(contents)
                    .filter_map(|(hash, content)| content.map(|content| (hash.clone(), content)))
                    .collect()
            }
        };
        Ok(deltas::resolve(decoded, &found)?)
    }

    /// The thread's elements from `from` to `to`, as stored, with their
    /// sequences.
    async fn range(
        &self,
        thread_id: &str,
        from: i64,
        to: i64,
    ) -> Result<Vec<(i64, Checkpoint)>, Failure> {
        let elements: Vec<String> = self
            .conn
            .clone()
            .lrange(self.key(thread_id), from as isize, to as isize)
            .await?;
        Ok(elements
            .iter()
            .zip(from..)
            .map(|(json, sequence)| Ok((sequence, serde_json::from_str(json).map_err(json_err)?)))
            .collect::<Result<Vec<_>, CheckpointStoreError>>()?)
    }

    /// Up to `count` `(score, thread id)` candidates from `set` after `start`.
    async fn candidates(
        &self,
        set: &str,
        start: Option<&ThreadCursor>,
        count: usize,
    ) -> Result<Vec<(u64, String)>, Failure> {
        let (start_score, start_thread) = match start {
            Some(cursor) => (
                score(&cursor.updated_at)?.to_string(),
                cursor.thread_id.clone(),
            ),
            None => (String::new(), String::new()),
        };
        let flat: Vec<String> = CANDIDATES
            .key(set)
            .arg(start_score)
            .arg(start_thread)
            .arg(count)
            .invoke_async(&mut self.conn.clone())
            .await?;
        Ok(scored(&flat)?)
    }

    async fn list_now(&self, query: &ThreadQuery) -> Result<ThreadPage, CheckpointStoreError> {
        Ok(self.retry.run("list", || self.list_once(query)).await?)
    }

    /// One page of `query` (`design/19-Async-Queryable-Store.md`, "Redis: index
    /// sorted sets"). Candidates come from the index sets in listing order; each
    /// one's latest checkpoint is then read, pipelined, and filtered. At most
    /// [`EXAMINED_PER_SLOT`] × `limit` candidates are examined, so a filtered
    /// page may come back short with a cursor.
    async fn list_once(&self, query: &ThreadQuery) -> Result<ThreadPage, Failure> {
        let cursor = query.validate()?;
        // Start at the cursor, or just below `updated_before` if that is lower.
        let mut start = cursor.clone();
        if let Some(before) = query.updated_before_text() {
            let below = ThreadCursor::new(before, "");
            if start.as_ref().is_none_or(|start| {
                (start.updated_at.as_str(), start.thread_id.as_str())
                    > (below.updated_at.as_str(), "")
            }) {
                start = Some(below);
            }
        }
        let after = query
            .updated_after_text()
            .map(|after| score(&after))
            .transpose()?;

        // A due listing reads only the threads that can be due; every other
        // filter is applied to each one.
        let sets: Vec<String> = if query.due_by.is_some() {
            vec![self.waking_key()]
        } else if query.states.is_empty() {
            vec![self.threads_key()]
        } else {
            let mut states = query.states.clone();
            states.sort();
            states.dedup();
            states
                .into_iter()
                .map(|state| self.state_key(state))
                .collect()
        };
        let budget = query.limit * EXAMINED_PER_SLOT;
        let mut candidates: Vec<(u64, String)> = Vec::new();
        // The lowest candidate known to be in order: each set that filled the
        // budget may hold more below its last one, which could rank above
        // candidates from the other sets.
        let mut floor: Option<(u64, String)> = None;
        for set in &sets {
            let fetched = self.candidates(set, start.as_ref(), budget).await?;
            if fetched.len() == budget {
                let last = fetched.last().cloned().expect("budget is at least 1");
                if floor.as_ref().is_none_or(|floor| last > *floor) {
                    floor = Some(last);
                }
            }
            candidates.extend(fetched);
        }
        candidates.sort_by(|a, b| b.cmp(a));
        candidates.dedup_by(|a, b| a.1 == b.1);
        let mut more = floor.is_some();
        if let Some(floor) = &floor {
            candidates.retain(|candidate| candidate >= floor);
        }
        if let Some(after) = after
            && let Some(cut) = candidates.iter().position(|(score, _)| *score <= after)
        {
            candidates.truncate(cut);
            more = false;
        }
        if candidates.len() > budget {
            candidates.truncate(budget);
            more = true;
        }

        let mut threads = Vec::new();
        let mut last_examined: Option<&(u64, String)> = None;
        let mut last_returned: Option<&(u64, String)> = None;
        'chunks: for chunk in candidates.chunks(query.limit) {
            // Each candidate's latest checkpoint and log length, read together.
            let mut pipe = redis::pipe();
            pipe.atomic();
            for (_, thread_id) in chunk {
                pipe.cmd("LINDEX").arg(self.key(thread_id)).arg(-1);
                pipe.cmd("LLEN").arg(self.key(thread_id));
            }
            let replies: Vec<redis::Value> = pipe.query_async(&mut self.conn.clone()).await?;
            for (candidate, reply) in chunk.iter().zip(replies.chunks(2)) {
                last_examined = Some(candidate);
                let json: Option<String> = redis::from_redis_value_ref(&reply[0])?;
                let Some(json) = json else {
                    // The thread's list is gone (it expired, or was deleted):
                    // take it out of the index.
                    self.index_one(&candidate.1).await?;
                    continue;
                };
                let length: i64 = redis::from_redis_value_ref(&reply[1])?;
                // A summary needs no state: an encoded element gives only the
                // version it was saved with.
                let checkpoint = deltas::listed(serde_json::from_str(&json).map_err(json_err)?);
                // The index may lag a thread that moved since: its own latest
                // checkpoint decides, so a moved thread is never returned twice.
                let admitted = start.as_ref().is_none_or(|start| {
                    start.admits(updated_at_text(&checkpoint), &checkpoint.thread_id)
                });
                if !admitted || !query.matches(&checkpoint) {
                    continue;
                }
                threads.push(ThreadSummary::of(length - 1, &checkpoint)?);
                last_returned = Some(candidate);
                if threads.len() == query.limit {
                    break 'chunks;
                }
            }
        }

        // A full page continues after its last thread if anything may follow;
        // a short one only stops early when the examination budget ran out.
        let examined_all = last_examined == candidates.last();
        let position = if threads.len() == query.limit {
            if more || !examined_all {
                last_returned
            } else {
                None
            }
        } else if more {
            last_examined
        } else {
            None
        };
        let next_cursor = position
            .map(|(score, thread_id)| ThreadCursor::new(score_text(*score), thread_id).encode());
        Ok(ThreadPage {
            threads,
            next_cursor,
        })
    }

    async fn log_now(
        &self,
        thread_id: &str,
        after_sequence: Option<i64>,
        limit: usize,
    ) -> Result<CheckpointLogPage, CheckpointStoreError> {
        check_limit(limit)?;
        let start = after_sequence.map_or(0, |after| (after + 1).max(0));
        let entries = self
            .retry
            .run("load", || self.log_once(thread_id, start, limit))
            .await?;
        Ok(CheckpointLogPage::new(entries, limit))
    }

    async fn log_once(
        &self,
        thread_id: &str,
        start: i64,
        limit: usize,
    ) -> Result<Vec<CheckpointLogEntry>, Failure> {
        let rows = self
            .range(thread_id, start, start + limit as i64 - 1)
            .await?;
        // A page whose first row is a delta is read with the rows it needs.
        let depth = rows.first().map_or(0, |(_, first)| depth_of(first)) as i64;
        let decoded = if depth == 0 {
            deltas::decode(rows, 0)?
        } else {
            let first = rows[0].0;
            let mut all = self.range(thread_id, first - depth, first - 1).await?;
            let from = all.len();
            all.extend(rows);
            deltas::decode(all, from)?
        };
        Ok(self
            .resolve(thread_id, decoded)
            .await?
            .into_iter()
            .map(|row| CheckpointLogEntry {
                sequence: row.sequence,
                checkpoint: row.checkpoint,
            })
            .collect())
    }

    /// Deletes the lists of threads that are done with
    /// (`design/34-Storage-Hardening.md`, "Retention"), oldest first.
    /// Candidates come from the named states' index sets; each is then deleted
    /// by [`PURGE_SCRIPT`], which decides from the thread's own latest
    /// checkpoint in the same atomic step, so a thread that ran again
    /// meanwhile, or that a lease holds, is kept. Its blob hash goes in the
    /// same script; blobs in another blob store go after it
    /// (`design/41-Checkpoint-Efficiency.md`, "Retention"), and a failure
    /// there is the purge's.
    async fn purge_now(
        &self,
        older_than: DateTime<Utc>,
        states: &PurgeStates,
        limit: usize,
    ) -> Result<usize, CheckpointStoreError> {
        check_limit(limit)?;
        if states.is_empty() {
            return Ok(0);
        }
        let below = score(&timestamp_text(older_than))?;
        let names = states.names();

        // The script decides each thread, so some candidates are kept (held,
        // or changed since they were indexed). They take no place in the
        // limit: the next round looks past them.
        let mut kept: HashSet<String> = HashSet::new();
        let mut purged = 0;
        while purged < limit {
            let wanted = limit - purged + kept.len();
            let mut round: Vec<(u64, String)> = Vec::new();
            for state in states.iter() {
                round.extend(
                    self.retry
                        .run("purge", || self.oldest(state, below, wanted))
                        .await?,
                );
            }
            round.retain(|(_, thread_id)| !kept.contains(thread_id));
            round.sort();
            round.truncate(limit - purged);
            if round.is_empty() {
                break;
            }
            for (_, thread_id) in round {
                // Purging twice is purging once: a repeat finds the list gone
                // and deletes nothing.
                if self
                    .retry
                    .run("purge", || self.purge_one(&thread_id, below, &names))
                    .await?
                {
                    purged += 1;
                    self.forget(&thread_id);
                    if let Some(blobs) = &self.blobs {
                        blobs.delete_thread(&thread_id).await?;
                    }
                } else {
                    kept.insert(thread_id);
                }
            }
        }
        Ok(purged)
    }

    /// Up to `count` threads of `state` scored below `below`, oldest first.
    async fn oldest(
        &self,
        state: ThreadState,
        below: u64,
        count: usize,
    ) -> Result<Vec<(u64, String)>, Failure> {
        let flat: Vec<String> = redis::cmd("ZRANGEBYSCORE")
            .arg(self.state_key(state))
            .arg("-inf")
            .arg(format!("({below}"))
            .arg("WITHSCORES")
            .arg("LIMIT")
            .arg(0)
            .arg(count)
            .query_async(&mut self.conn.clone())
            .await?;
        Ok(scored(&flat)?)
    }

    async fn purge_one(
        &self,
        thread_id: &str,
        below: u64,
        names: &[String],
    ) -> Result<bool, Failure> {
        let mut invocation = PURGE.prepare_invoke();
        for key in self.index_keys(thread_id) {
            invocation.key(key);
        }
        invocation
            .key(self.lease_key(thread_id))
            .key(self.lease_token_key(thread_id))
            .key(self.blob_key(thread_id))
            .arg(thread_id)
            .arg(below);
        for name in names {
            invocation.arg(name);
        }
        let purged: i64 = invocation.invoke_async(&mut self.conn.clone()).await?;
        Ok(purged == 1)
    }
}

/// A flat `member, score, member, score…` reply as `(score, member)` pairs.
fn scored(flat: &[String]) -> Result<Vec<(u64, String)>, CheckpointStoreError> {
    flat.chunks(2)
        .map(|pair| {
            let score = pair[1]
                .parse::<f64>()
                .map_err(|_| CheckpointStoreError(format!("bad index score '{}'", pair[1])))?;
            Ok((score as u64, pair[0].clone()))
        })
        .collect()
}

impl CheckpointStore for RedisCheckpointStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        Box::pin(self.save_now(checkpoint))
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        Box::pin(self.load_now(thread_id))
    }

    fn load_latest<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<LatestCheckpoint>, CheckpointStoreError>> {
        Box::pin(self.load_latest_now(thread_id))
    }

    fn save_if_latest(
        &self,
        checkpoint: Checkpoint,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'_, Result<bool, CheckpointStoreError>> {
        Box::pin(self.save_if_latest_now(checkpoint, expected_sequence))
    }

    fn try_acquire_lease<'a>(
        &'a self,
        thread_id: &'a str,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<LeaseAttempt, CheckpointStoreError>> {
        Box::pin(self.acquire_lease_now(thread_id, owner, ttl))
    }

    fn renew_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        Box::pin(self.renew_lease_now(lease, ttl))
    }

    fn release_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
    ) -> BoxFuture<'a, Result<bool, CheckpointStoreError>> {
        Box::pin(self.release_lease_now(lease))
    }

    fn get_lease<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        Box::pin(self.get_lease_now(thread_id))
    }

    fn save_under_lease<'a>(
        &'a self,
        checkpoint: Checkpoint,
        lease: &'a ThreadLease,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'a, Result<LeasedSave, CheckpointStoreError>> {
        Box::pin(self.save_under_lease_now(checkpoint, lease, expected_sequence))
    }
}

impl CheckpointQuery for RedisCheckpointStore {
    fn list_threads<'a>(
        &'a self,
        query: &'a ThreadQuery,
    ) -> BoxFuture<'a, Result<ThreadPage, CheckpointStoreError>> {
        Box::pin(self.list_now(query))
    }

    fn get_log<'a>(
        &'a self,
        thread_id: &'a str,
        after_sequence: Option<i64>,
        limit: usize,
    ) -> BoxFuture<'a, Result<CheckpointLogPage, CheckpointStoreError>> {
        Box::pin(self.log_now(thread_id, after_sequence, limit))
    }
}

impl CheckpointRetention for RedisCheckpointStore {
    fn purge<'a>(
        &'a self,
        older_than: DateTime<Utc>,
        states: &'a PurgeStates,
        limit: usize,
    ) -> BoxFuture<'a, Result<usize, CheckpointStoreError>> {
        Box::pin(self.purge_now(older_than, states, limit))
    }
}

/// Redis-backed [`HistoryStore`]; see the module docs for the layout.
///
/// A read is repeated after a transient failure
/// (`design/34-Storage-Hardening.md`); an append only when it never reached
/// the server, since the store can't tell a message that was written from one
/// that wasn't.
pub struct RedisHistoryStore {
    conn: ConnectionManager,
    prefix: String,
    retry: Retrier,
}

impl RedisHistoryStore {
    pub async fn connect(url: &str) -> Result<Self, HistoryStoreError> {
        Self::connect_with(url, RedisStoreOptions::new()).await
    }

    /// See [`RedisCheckpointStore::connect_with_prefix`].
    pub async fn connect_with_prefix(url: &str, prefix: &str) -> Result<Self, HistoryStoreError> {
        Self::connect_with(url, RedisStoreOptions::new().with_prefix(prefix)).await
    }

    /// [`connect`](Self::connect) with `options`' prefix and retry policy.
    pub async fn connect_with(
        url: &str,
        options: RedisStoreOptions,
    ) -> Result<Self, HistoryStoreError> {
        let retry = Retrier::new(options.retry, "redis");
        Ok(Self {
            conn: connect_retrying(url, &retry).await?,
            prefix: options.prefix,
            retry,
        })
    }

    fn key(&self, session_id: &str) -> String {
        format!("{}:history:{session_id}", self.prefix)
    }

    async fn append_now(
        &self,
        session_id: &str,
        message: ChatMessage,
    ) -> Result<(), HistoryStoreError> {
        let json = json!({
            "role": role_to_str(message.role),
            "content": message.content.as_ref(),
            "timestamp": message.timestamp.to_rfc3339_opts(SecondsFormat::AutoSi, true),
        })
        .to_string();
        let mut number = 1;
        loop {
            match self.append_once(session_id, &json).await {
                Ok(()) => return Ok(()),
                Err(failure) => {
                    if !failure.never_sent || !self.retry.again("append", number, &failure).await {
                        return Err(failure.into());
                    }
                }
            }
            number += 1;
        }
    }

    async fn append_once(&self, session_id: &str, json: &str) -> Result<(), Failure> {
        let _len: i64 = self.conn.clone().rpush(self.key(session_id), json).await?;
        Ok(())
    }

    async fn history_now(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<ChatMessage>, HistoryStoreError> {
        // `LRANGE key -0 -1` would be the whole list, not nothing.
        if limit == 0 {
            return Ok(Vec::new());
        }
        let start = -i64::try_from(limit).unwrap_or(i64::MAX);
        let elements: Vec<String> = self
            .retry
            .run("load", || async {
                self.conn
                    .clone()
                    .lrange(self.key(session_id), start as isize, -1)
                    .await
                    .map_err(Failure::from)
            })
            .await?;

        elements
            .iter()
            .map(|element| {
                let value: serde_json::Value =
                    serde_json::from_str(element).map_err(|e| HistoryStoreError(e.to_string()))?;
                let field = |name: &str| {
                    value[name].as_str().ok_or_else(|| {
                        HistoryStoreError(format!("history element is missing string '{name}'"))
                    })
                };
                Ok(ChatMessage {
                    role: role_from_str(field("role")?)?,
                    content: Arc::from(field("content")?),
                    timestamp: DateTime::parse_from_rfc3339(field("timestamp")?)
                        .map_err(|e| HistoryStoreError(e.to_string()))?
                        .with_timezone(&Utc),
                })
            })
            .collect()
    }
}

fn role_to_str(role: MessageRole) -> &'static str {
    match role {
        MessageRole::System => "system",
        MessageRole::User => "user",
        MessageRole::Assistant => "assistant",
        MessageRole::Tool => "tool",
    }
}

fn role_from_str(role: &str) -> Result<MessageRole, HistoryStoreError> {
    match role {
        "system" => Ok(MessageRole::System),
        "user" => Ok(MessageRole::User),
        "assistant" => Ok(MessageRole::Assistant),
        "tool" => Ok(MessageRole::Tool),
        other => Err(HistoryStoreError(format!(
            "unknown role '{other}' in history store"
        ))),
    }
}

impl HistoryStore for RedisHistoryStore {
    fn append<'a>(
        &'a self,
        session_id: &'a str,
        message: ChatMessage,
    ) -> BoxFuture<'a, Result<(), HistoryStoreError>> {
        Box::pin(self.append_now(session_id, message))
    }

    fn get_history<'a>(
        &'a self,
        session_id: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<ChatMessage>, HistoryStoreError>> {
        Box::pin(self.history_now(session_id, limit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    fn failure(err: redis::RedisError) -> Failure {
        Failure::from(err)
    }

    #[test]
    fn a_lost_connection_a_timeout_and_a_server_that_cannot_answer_yet_are_transient() {
        for kind in [
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::UnexpectedEof,
            io::ErrorKind::TimedOut,
        ] {
            let lost = failure(io::Error::new(kind, "gone").into());
            assert!(lost.transient, "{kind:?}");
            assert!(!lost.never_sent, "{kind:?}");
        }
        for code in ["LOADING", "BUSY", "TRYAGAIN"] {
            let waiting = failure(redis::make_extension_error(
                code.to_string(),
                Some("not yet".to_string()),
            ));
            assert!(waiting.transient, "{code}");
        }
    }

    #[test]
    fn a_refused_connection_carried_nothing_to_the_server() {
        let refused = failure(io::Error::new(io::ErrorKind::ConnectionRefused, "refused").into());
        assert!(refused.transient);
        assert!(refused.never_sent);
    }

    #[test]
    fn any_other_failure_is_the_calls() {
        let wrong_type = failure(redis::make_extension_error(
            "WRONGTYPE".to_string(),
            Some("Operation against a key holding the wrong kind of value".to_string()),
        ));
        assert!(!wrong_type.transient);
        let client = failure((redis::ErrorKind::Client, "a bug").into());
        assert!(!client.transient);
        // The class is the error's kind, never its text.
        assert_eq!(client.class, "Client");
    }
}
