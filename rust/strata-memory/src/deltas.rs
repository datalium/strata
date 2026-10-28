//! Delta checkpoints and value offload (`design/41-Checkpoint-Efficiency.md`,
//! "Deltas" and "Offload"): a store with [`Deltas`] stores a `running`
//! checkpoint as the top-level keys of each level's state that changed since
//! the thread's previous row, when it knows that row, and every other
//! checkpoint whole; a store with [`Offload`] stores each large top-level
//! value once, in a blob store, and refers to it.
//!
//! The encoded form ("Encoded checkpoints") is a checkpoint with
//! [`DELTA_CHECKPOINT_SCHEMA_VERSION`] or [`OFFLOAD_CHECKPOINT_SCHEMA_VERSION`]
//! whose levels' `frozen_state` are envelopes. Only a store writes it and only
//! a store reads it: every store decodes every row it reads, whatever its own
//! options. Mirrors the .NET `DeltaOptions`, `CheckpointEncoding`,
//! `DeltaCache` and `CheckpointWriter`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use datalium_strata::checkpoint::CheckpointStoreError;
use datalium_strata::schema::{Checkpoint, CheckpointStatus};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::offload::{Blob, Offload, hash_name, matches};

/// The `schema_version` of a checkpoint stored with a delta. Never a
/// checkpoint the engine sees.
pub const DELTA_CHECKPOINT_SCHEMA_VERSION: &str = "1.8.0";

/// The `schema_version` of a checkpoint stored with offloaded values, a delta
/// or not. Never a checkpoint the engine sees.
pub const OFFLOAD_CHECKPOINT_SCHEMA_VERSION: &str = "1.9.0";

/// The encoded versions, as a SQL list.
pub(crate) const ENCODED_VERSIONS_SQL: &str = "'1.8.0', '1.9.0'";

/// How a store stores deltas. The default stores every 16th row of a thread
/// whole and remembers the latest rows of 10,000 threads. Mirrors the .NET
/// `DeltaOptions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deltas {
    full_every: u32,
    cached_threads: usize,
}

impl Default for Deltas {
    fn default() -> Self {
        Self {
            full_every: 16,
            cached_threads: DEFAULT_CACHED_THREADS,
        }
    }
}

/// The threads a store remembers when nothing says otherwise.
const DEFAULT_CACHED_THREADS: usize = 10_000;

impl Deltas {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every this many rows of a thread one is stored whole, so a load reads
    /// at most this many. At least 2.
    pub fn with_full_every(mut self, full_every: u32) -> Self {
        self.full_every = full_every;
        self
    }

    /// How many threads' latest rows the store remembers (their fingerprints,
    /// about 40 bytes a key), the least recently used forgotten first. A
    /// thread the store doesn't remember is saved whole. At least 1.
    pub fn with_cached_threads(mut self, cached_threads: usize) -> Self {
        self.cached_threads = cached_threads;
        self
    }

    pub fn full_every(&self) -> u32 {
        self.full_every
    }

    pub fn cached_threads(&self) -> usize {
        self.cached_threads
    }

    pub(crate) fn check(&self) -> Result<(), CheckpointStoreError> {
        if self.full_every < 2 {
            return Err(CheckpointStoreError(
                "deltas' full every must be at least 2".to_string(),
            ));
        }
        if self.cached_threads == 0 {
            return Err(CheckpointStoreError(
                "deltas' cached threads must be at least 1".to_string(),
            ));
        }
        Ok(())
    }
}

/// The SHA-256 of a value's JSON text, with object keys in order. It is also
/// the hash an offloaded value is stored under: the blob is that same text.
type Fingerprint = [u8; 32];

/// The fingerprint of `value`, and, when its text is at least `keep_from`
/// bytes long, that text: what an offloaded value is stored as.
fn fingerprint(value: &Value, keep_from: usize) -> (Fingerprint, Option<Vec<u8>>) {
    let mut text = Vec::with_capacity(256);
    canonical(value, &mut text);
    let print = Sha256::digest(&text).into();
    (print, (text.len() >= keep_from).then_some(text))
}

/// `value`'s JSON with every object's keys sorted, whatever order the map
/// keeps them in, so a value read back from a `JSONB` column, which reorders
/// them, prints as it was saved. A value that still prints differently (a
/// number written another way) only makes a delta bigger.
fn canonical(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Object(map) => {
            out.push(b'{');
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            for (i, key) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                // A string always serializes.
                let _ = serde_json::to_writer(&mut *out, key);
                out.push(b':');
                canonical(&map[key], out);
            }
            out.push(b'}');
        }
        Value::Array(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                canonical(item, out);
            }
            out.push(b']');
        }
        other => {
            let _ = serde_json::to_writer(&mut *out, other);
        }
    }
}

/// The fingerprint a blob's `sha256:` hash names.
fn digest_of(hash: &str) -> Option<Fingerprint> {
    let hex = hash.strip_prefix("sha256:")?.as_bytes();
    if hex.len() != 64 {
        return None;
    }
    let mut digest = [0u8; 32];
    for (byte, pair) in digest.iter_mut().zip(hex.chunks(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(digest)
}

/// By level and key, the JSON text of each top-level value large enough to
/// offload.
type Large = Vec<Option<HashMap<String, Arc<[u8]>>>>;

/// The fingerprints of each level's state, outermost first: a key's
/// fingerprint per top-level key, or `None` for a level whose state isn't a
/// JSON object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LevelPrints(Vec<Option<HashMap<String, Fingerprint>>>);

impl LevelPrints {
    pub fn of(checkpoint: &Checkpoint) -> Self {
        Self::of_with(checkpoint, None).0
    }

    /// The fingerprints of a decoded `checkpoint`, its offloaded values'
    /// (`slots`) taken from their hashes, which are those fingerprints, rather
    /// than printed and hashed again.
    fn of_known(checkpoint: &Checkpoint, slots: &[BlobSlot]) -> Self {
        if slots.is_empty() {
            return Self::of(checkpoint);
        }
        Self(
            levels(checkpoint)
                .into_iter()
                .enumerate()
                .map(|(i, level)| match &level.frozen_state {
                    Value::Object(state) => Some(
                        state
                            .iter()
                            .map(|(key, value)| {
                                let known = slots
                                    .iter()
                                    .find(|slot| slot.level == i && slot.key == *key)
                                    .and_then(|slot| digest_of(&slot.hash));
                                (
                                    key.clone(),
                                    known.unwrap_or_else(|| fingerprint(value, usize::MAX).0),
                                )
                            })
                            .collect(),
                    ),
                    _ => None,
                })
                .collect(),
        )
    }

    /// The fingerprints of `checkpoint`, and with an `offload_from`, by level
    /// and key, the JSON text of each top-level value at least that many
    /// bytes long.
    fn of_with(checkpoint: &Checkpoint, offload_from: Option<usize>) -> (Self, Option<Large>) {
        let keep_from = offload_from.unwrap_or(usize::MAX);
        let mut large: Large = Vec::new();
        let prints = levels(checkpoint)
            .into_iter()
            .map(|level| match &level.frozen_state {
                Value::Object(state) => {
                    let mut texts: Option<HashMap<String, Arc<[u8]>>> = None;
                    let prints = state
                        .iter()
                        .map(|(key, value)| {
                            let (print, text) = fingerprint(value, keep_from);
                            if let Some(text) = text {
                                texts
                                    .get_or_insert_with(HashMap::new)
                                    .insert(key.clone(), text.into());
                            }
                            (key.clone(), print)
                        })
                        .collect();
                    large.push(texts);
                    Some(prints)
                }
                _ => {
                    large.push(None);
                    None
                }
            })
            .collect();
        (Self(prints), offload_from.map(|_| large))
    }
}

/// A thread's latest row as a store remembers it.
#[derive(Debug, Clone)]
pub(crate) struct CachedRow {
    pub sequence: i64,
    /// How many rows back it is from one with no delta.
    pub depth: u32,
    pub prints: Arc<LevelPrints>,
    /// What the store checks the row by when a save relies on it: its
    /// `created_at` in SQL, the SHA-1 of its element in Redis.
    pub token: Arc<str>,
    /// The offloaded values it refers to, which the thread's blob store
    /// therefore has.
    pub blobs: Arc<HashSet<String>>,
}

/// How a checkpoint is about to be stored: whole, or as `stored`, with its
/// levels' states as the changes to the row at `delta_of`, its large values in
/// the blob store, or both. `prints` are the checkpoint's own, for the cache.
#[derive(Debug, Clone, Default)]
pub(crate) struct Encoding {
    pub stored: Option<Checkpoint>,
    pub delta_of: Option<i64>,
    pub depth: u32,
    pub prints: Option<Arc<LevelPrints>>,
    large: Option<Arc<Large>>,
    /// The row this save relies on still being the thread's latest, when it is
    /// a delta of it or leaves out blobs the store remembers the thread has:
    /// the store appends it only if that row, by sequence and token, is still
    /// the latest, and otherwise writes [`Self::whole`].
    pub base: Option<CachedRow>,
    /// The values this save writes to the blob store.
    pub blobs: Vec<Blob>,
    /// The blobs the store can count on the thread having once this row is
    /// written.
    pub remembered: Arc<HashSet<String>>,
    /// Whether the row refers to offloaded values.
    pub offloaded: bool,
}

impl Encoding {
    /// Stored as it is.
    pub fn plain() -> Self {
        Self::default()
    }

    pub fn is_delta(&self) -> bool {
        self.delta_of.is_some()
    }

    /// Whether the save relies on the row before it.
    pub fn is_based(&self) -> bool {
        self.base.is_some()
    }

    /// The `strata.store.encoding` it is recorded under.
    pub fn label(&self) -> &'static str {
        match (self.is_delta(), self.offloaded) {
            (true, true) => "delta_offloaded",
            (true, false) => "delta",
            (false, true) => "offloaded",
            (false, false) => "plain",
        }
    }

    /// What is written for `checkpoint`: the encoded form, or the checkpoint
    /// itself.
    pub fn stored<'a>(&'a self, checkpoint: &'a Checkpoint) -> &'a Checkpoint {
        self.stored.as_ref().unwrap_or(checkpoint)
    }

    /// The sequence of the row the save relies on, if any.
    pub fn base_sequence(&self) -> Option<i64> {
        self.base.as_ref().map(|base| base.sequence)
    }

    /// The token of the row the save relies on, if any.
    pub fn base_token(&self) -> Option<&str> {
        self.base.as_ref().map(|base| &*base.token)
    }

    /// `checkpoint` stored without relying on the row before it: what a store
    /// writes instead when that row is no longer the thread's latest. No
    /// delta, and every value it offloads written again.
    pub fn whole(&self, checkpoint: &Checkpoint) -> Self {
        match (&self.base, &self.prints) {
            (Some(_), Some(prints)) => {
                encode(checkpoint, prints, self.large.as_ref(), None, false, false)
            }
            _ => self.clone(),
        }
    }
}

/// The levels of `checkpoint`, outermost first.
pub(crate) fn levels(checkpoint: &Checkpoint) -> Vec<&Checkpoint> {
    let mut levels = vec![checkpoint];
    let mut level = checkpoint;
    while let Some(child) = level.nested.as_deref() {
        levels.push(child);
        level = child;
    }
    levels
}

/// The state of `checkpoint`'s level `index`, outermost 0.
fn state_at_mut(checkpoint: &mut Checkpoint, index: usize) -> Option<&mut Value> {
    let mut level = checkpoint;
    for _ in 0..index {
        level = level.nested.as_deref_mut()?;
    }
    Some(&mut level.frozen_state)
}

/// `checkpoint` with each level's state replaced, outermost first.
fn with_states(mut checkpoint: Checkpoint, states: &mut std::vec::IntoIter<Value>) -> Checkpoint {
    checkpoint.frozen_state = states.next().unwrap_or(Value::Null);
    checkpoint.nested = checkpoint
        .nested
        .map(|child| Box::new(with_states(*child, states)));
    checkpoint
}

/// `checkpoint` as a store writes it. With `can_delta`, each level whose
/// state and `cached`'s same level are objects is the keys that changed or are
/// new (`set`) and the keys that are gone (`unset`); any other level is whole.
/// Each top-level value in `large` that the row carries (all of a whole
/// level's, a delta level's changed ones) goes to the blob store instead and is
/// named in the level's `blobs`; one the thread's blob store has, by
/// `cached`, isn't written again unless `write_all`. Stored as it is when no
/// level is a delta and nothing is offloaded.
fn encode(
    checkpoint: &Checkpoint,
    prints: &Arc<LevelPrints>,
    large: Option<&Arc<Large>>,
    cached: Option<&CachedRow>,
    can_delta: bool,
    write_all: bool,
) -> Encoding {
    let levels = levels(checkpoint);
    let mut envelopes = Vec::with_capacity(levels.len());
    let mut any_delta = false;
    let mut omitted = false;
    let mut referred: HashSet<String> = HashSet::new();
    let mut written: Vec<Blob> = Vec::new();
    let mut large_hashes: HashSet<String> = HashSet::new();

    for (i, level) in levels.iter().enumerate() {
        let mut envelope = Map::new();
        let texts = large
            .and_then(|large| large.get(i))
            .and_then(Option::as_ref);
        let now = prints.0.get(i).and_then(Option::as_ref);
        let (Value::Object(state), Some(now)) = (&level.frozen_state, now) else {
            envelope.insert("state".to_string(), level.frozen_state.clone());
            envelopes.push(Value::Object(envelope));
            continue;
        };
        if let Some(texts) = texts {
            large_hashes.extend(texts.keys().map(|key| hash_name(&now[key])));
        }
        // The reference to this level's value at `key`, when it is
        // offloaded; the blob is written unless the store remembers the
        // thread has it.
        let mut reference = |key: &str| -> Option<Value> {
            let text = texts?.get(key)?;
            let hash = hash_name(&now[key]);
            if referred.insert(hash.clone()) {
                if !write_all && cached.is_some_and(|cached| cached.blobs.contains(&hash)) {
                    omitted = true;
                } else {
                    written.push(Blob {
                        hash: hash.clone(),
                        content: text.clone(),
                    });
                }
            }
            Some(serde_json::json!({ "hash": hash, "bytes": text.len() }))
        };
        let mut blobs = Map::new();
        let before = cached
            .filter(|_| can_delta)
            .and_then(|cached| cached.prints.0.get(i))
            .and_then(Option::as_ref);
        if let Some(before) = before {
            let mut set = Map::new();
            for (key, value) in state {
                if before.get(key) != now.get(key) {
                    match reference(key) {
                        Some(reference) => {
                            blobs.insert(key.clone(), reference);
                        }
                        None => {
                            set.insert(key.clone(), value.clone());
                        }
                    }
                }
            }
            let mut unset: Vec<&String> = before
                .keys()
                .filter(|key| !state.contains_key(*key))
                .collect();
            unset.sort_unstable();
            envelope.insert("set".to_string(), Value::Object(set));
            envelope.insert(
                "unset".to_string(),
                Value::Array(
                    unset
                        .into_iter()
                        .map(|key| Value::String(key.clone()))
                        .collect(),
                ),
            );
            any_delta = true;
        } else {
            let mut whole = Map::new();
            for (key, value) in state {
                match reference(key) {
                    Some(reference) => {
                        blobs.insert(key.clone(), reference);
                    }
                    None => {
                        whole.insert(key.clone(), value.clone());
                    }
                }
            }
            envelope.insert("state".to_string(), Value::Object(whole));
        }
        if !blobs.is_empty() {
            envelope.insert("blobs".to_string(), Value::Object(blobs));
        }
        envelopes.push(Value::Object(envelope));
    }

    if !any_delta && referred.is_empty() {
        return Encoding {
            prints: Some(prints.clone()),
            ..Encoding::default()
        };
    }
    let delta_of = cached.filter(|_| any_delta).map(|cached| cached.sequence);
    let depth = cached
        .filter(|_| any_delta)
        .map_or(0, |cached| cached.depth + 1);
    if let Some(Value::Object(inner)) = envelopes.first_mut() {
        let mut outer = Map::new();
        outer.insert(
            "checkpoint_version".to_string(),
            Value::String(checkpoint.schema_version.clone()),
        );
        outer.insert(
            "delta_of".to_string(),
            delta_of.map_or(Value::Null, Value::from),
        );
        outer.insert("depth".to_string(), Value::from(depth));
        outer.append(inner);
        *inner = outer;
    }
    // A row that relies on the one before (a delta of it, or leaving out
    // blobs it says the thread has) keeps what that row's blobs were for the
    // values it still has; any other row, what it refers to itself.
    let based = any_delta || omitted;
    let offloaded = !referred.is_empty();
    let mut remembered = referred;
    if let Some(cached) = cached.filter(|_| based) {
        remembered.extend(
            cached
                .blobs
                .iter()
                .filter(|hash| large_hashes.contains(*hash))
                .cloned(),
        );
    }
    let mut stored = with_states(checkpoint.clone(), &mut envelopes.into_iter());
    stored.schema_version = if offloaded {
        OFFLOAD_CHECKPOINT_SCHEMA_VERSION
    } else {
        DELTA_CHECKPOINT_SCHEMA_VERSION
    }
    .to_string();
    Encoding {
        stored: Some(stored),
        delta_of,
        depth,
        prints: Some(prints.clone()),
        large: large.cloned(),
        base: cached.filter(|_| based).cloned(),
        blobs: written,
        remembered: Arc::new(remembered),
        offloaded,
    }
}

pub(crate) fn is_encoded(schema_version: &str) -> bool {
    schema_version == DELTA_CHECKPOINT_SCHEMA_VERSION
        || schema_version == OFFLOAD_CHECKPOINT_SCHEMA_VERSION
}

/// How many rows back the row `stored` is from one with no delta: 0 when it
/// has none.
pub(crate) fn depth_of(stored: &Checkpoint) -> u32 {
    if !is_encoded(&stored.schema_version) {
        return 0;
    }
    stored
        .frozen_state
        .get("depth")
        .and_then(Value::as_u64)
        .map_or(0, |depth| depth as u32)
}

/// The version a listing reports for a row stored as `schema_version`, given
/// its envelope's `checkpoint_version`: listings read no state, so they decode
/// no more than this.
pub(crate) fn listed_version(schema_version: String, checkpoint_version: Option<String>) -> String {
    match checkpoint_version {
        Some(version) if is_encoded(&schema_version) => version,
        _ => schema_version,
    }
}

/// `stored` with the version it was saved with, for a listing; its states are
/// left encoded.
pub(crate) fn listed(mut stored: Checkpoint) -> Checkpoint {
    if is_encoded(&stored.schema_version)
        && let Some(version) = stored
            .frozen_state
            .get("checkpoint_version")
            .and_then(Value::as_str)
    {
        stored.schema_version = version.to_string();
    }
    stored
}

fn broken(stored: &Checkpoint, sequence: i64, why: String) -> CheckpointStoreError {
    CheckpointStoreError(format!(
        "checkpoint '{}' #{sequence} {why}",
        stored.thread_id
    ))
}

/// Where a decoded state's offloaded value goes: the level, the key, the
/// blob, and the row that refers to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlobSlot {
    pub level: usize,
    pub key: String,
    pub hash: String,
    pub sequence: i64,
}

/// A row decoded: its checkpoint, and the offloaded values it refers to
/// (already in place once [`resolve`]d).
#[derive(Debug, Clone)]
pub(crate) struct Decoded {
    pub sequence: i64,
    pub checkpoint: Checkpoint,
    pub blobs: Vec<BlobSlot>,
}

/// The checkpoints `rows` (consecutive rows of one thread, oldest first)
/// decode to, from `from` on, with their offloaded values still to fetch
/// ([`resolve`]): a `null` holds each one's place. A delta row's base must be
/// the row before it in `rows`, so the first row must have none: a store
/// reads a row together with the rows its `depth` says it needs. A decoded
/// state keeps the base's keys, with changed keys replaced, added keys added
/// and removed keys gone.
pub(crate) fn decode(
    rows: Vec<(i64, Checkpoint)>,
    from: usize,
) -> Result<Vec<Decoded>, CheckpointStoreError> {
    if rows.iter().all(|(_, row)| !is_encoded(&row.schema_version)) {
        return Ok(rows
            .into_iter()
            .skip(from)
            .map(|(sequence, checkpoint)| Decoded {
                sequence,
                checkpoint,
                blobs: Vec::new(),
            })
            .collect());
    }
    let count = rows.len();
    let mut decoded = Vec::with_capacity(count.saturating_sub(from));
    // Each level's state as of the row before, changed in place by the next
    // delta, and the offloaded values it refers to, by key.
    let mut current: Option<Vec<Value>> = None;
    let mut pending: Option<Vec<BTreeMap<String, BlobSlot>>> = None;
    let mut previous: Option<i64> = None;
    for (r, (sequence, mut stored)) in rows.into_iter().enumerate() {
        let emit = r >= from;
        if !is_encoded(&stored.schema_version) {
            let states = if emit {
                levels(&stored)
                    .into_iter()
                    .map(|level| level.frozen_state.clone())
                    .collect()
            } else {
                take_states(&mut stored)
            };
            pending = Some(states.iter().map(|_| BTreeMap::new()).collect());
            current = Some(states);
            previous = Some(sequence);
            if emit {
                decoded.push(Decoded {
                    sequence,
                    checkpoint: stored,
                    blobs: Vec::new(),
                });
            }
            continue;
        }

        let mut envelopes = take_states(&mut stored);
        let (version, delta_of) = match envelopes.first() {
            Some(Value::Object(outer)) => (
                outer
                    .get("checkpoint_version")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                outer.get("delta_of").and_then(Value::as_i64),
            ),
            _ => {
                return Err(broken(
                    &stored,
                    sequence,
                    format!("is stored as {} without an envelope", stored.schema_version),
                ));
            }
        };
        let version = version
            .ok_or_else(|| broken(&stored, sequence, "has no checkpoint_version".to_string()))?;
        // A row with no delta_of has no delta level, and needs nothing from
        // the row before.
        let (mut before, mut before_pending) = match delta_of {
            Some(delta_of) if previous != Some(delta_of) => {
                return Err(broken(
                    &stored,
                    sequence,
                    format!("is a delta of #{delta_of}, which wasn't read with it"),
                ));
            }
            Some(_) => (current.take(), pending.take()),
            None => (None, None),
        };

        let mut states = Vec::with_capacity(envelopes.len());
        let mut slots = Vec::with_capacity(envelopes.len());
        for (i, envelope) in envelopes.iter_mut().enumerate() {
            let Value::Object(envelope) = envelope else {
                return Err(broken(
                    &stored,
                    sequence,
                    "has a level without an envelope".to_string(),
                ));
            };
            let (mut state, mut level_slots) = if let Some(state) = envelope.remove("state") {
                (state, BTreeMap::new())
            } else {
                let basis = before
                    .as_mut()
                    .and_then(|before| before.get_mut(i))
                    .map(std::mem::take);
                let Some(Value::Object(mut basis)) = basis else {
                    return Err(broken(
                        &stored,
                        sequence,
                        format!("has a delta at level {i}, and the row before has no object there"),
                    ));
                };
                let mut level_slots = before_pending
                    .as_mut()
                    .and_then(|pending| pending.get_mut(i))
                    .map(std::mem::take)
                    .unwrap_or_default();
                if let Some(Value::Array(unset)) = envelope.get("unset") {
                    for key in unset.iter().filter_map(Value::as_str) {
                        basis.remove(key);
                        level_slots.remove(key);
                    }
                }
                if let Some(Value::Object(set)) = envelope.remove("set") {
                    for (key, value) in set {
                        level_slots.remove(&key);
                        basis.insert(key, value);
                    }
                }
                (Value::Object(basis), level_slots)
            };
            if let Some(Value::Object(blobs)) = envelope.remove("blobs") {
                let Value::Object(target) = &mut state else {
                    return Err(broken(
                        &stored,
                        sequence,
                        format!("has blobs at level {i}, whose state isn't an object"),
                    ));
                };
                for (key, reference) in blobs {
                    let Some(hash) = reference.get("hash").and_then(Value::as_str) else {
                        return Err(broken(
                            &stored,
                            sequence,
                            format!("has a blob at '{key}' without a hash"),
                        ));
                    };
                    target.insert(key.clone(), Value::Null);
                    level_slots.insert(
                        key.clone(),
                        BlobSlot {
                            level: i,
                            key,
                            hash: hash.to_string(),
                            sequence,
                        },
                    );
                }
            }
            states.push(state);
            slots.push(level_slots);
        }

        previous = Some(sequence);
        if emit {
            let own = if r + 1 == count {
                std::mem::take(&mut states)
            } else {
                states.clone()
            };
            let mut checkpoint = with_states(stored, &mut own.into_iter());
            checkpoint.schema_version = version;
            decoded.push(Decoded {
                sequence,
                checkpoint,
                blobs: slots
                    .iter()
                    .flat_map(|level| level.values().cloned())
                    .collect(),
            });
        }
        current = Some(states);
        pending = Some(slots);
    }
    Ok(decoded)
}

/// Takes each level's state out of `checkpoint`, outermost first.
fn take_states(checkpoint: &mut Checkpoint) -> Vec<Value> {
    let mut states = vec![std::mem::take(&mut checkpoint.frozen_state)];
    let mut level = checkpoint.nested.as_deref_mut();
    while let Some(child) = level {
        states.push(std::mem::take(&mut child.frozen_state));
        level = child.nested.as_deref_mut();
    }
    states
}

/// The blobs `decoded` refers to, each once.
pub(crate) fn wanted(decoded: &[Decoded]) -> Vec<String> {
    let mut seen = HashSet::new();
    decoded
        .iter()
        .flat_map(|row| &row.blobs)
        .filter(|slot| seen.insert(slot.hash.as_str()))
        .map(|slot| slot.hash.clone())
        .collect()
}

/// Puts each offloaded value `decoded` refers to in its place, from `found`
/// (what the blob store had of [`wanted`]), checked against its hash.
pub(crate) fn resolve(
    mut decoded: Vec<Decoded>,
    found: &HashMap<String, Vec<u8>>,
) -> Result<Vec<Decoded>, CheckpointStoreError> {
    let mut values: HashMap<&str, Value> = HashMap::new();
    for slot in decoded.iter().flat_map(|row| &row.blobs) {
        if values.contains_key(slot.hash.as_str()) {
            continue;
        }
        let refers = || {
            format!(
                "checkpoint '{}' #{} refers to blob {} at '{}'",
                decoded[0].checkpoint.thread_id, slot.sequence, slot.hash, slot.key
            )
        };
        let Some(content) = found.get(&slot.hash) else {
            return Err(CheckpointStoreError(format!(
                "{}, which the blob store doesn't have",
                refers()
            )));
        };
        if !matches(&slot.hash, content) {
            return Err(CheckpointStoreError(format!(
                "{}, whose content doesn't match",
                refers()
            )));
        }
        let value = serde_json::from_slice(content)
            .map_err(|err| CheckpointStoreError(format!("{}: {err}", refers())))?;
        values.insert(slot.hash.as_str(), value);
    }
    let values: HashMap<String, Value> = values
        .into_iter()
        .map(|(hash, value)| (hash.to_string(), value))
        .collect();
    for row in &mut decoded {
        for slot in &row.blobs {
            if let Some(Value::Object(state)) = state_at_mut(&mut row.checkpoint, slot.level) {
                state.insert(slot.key.clone(), values[&slot.hash].clone());
            }
        }
    }
    Ok(decoded)
}

/// The threads whose latest rows a store remembers, the least recently used
/// forgotten first: what a save is a delta of, and which blobs it needn't
/// write again.
pub(crate) struct DeltaCache {
    capacity: usize,
    state: Mutex<CacheState>,
}

#[derive(Default)]
struct CacheState {
    rows: HashMap<String, (CachedRow, u64)>,
    order: BTreeMap<u64, String>,
    tick: u64,
}

impl CacheState {
    fn touch(&mut self, thread_id: &str) -> u64 {
        self.tick += 1;
        if let Some((_, used)) = self.rows.get_mut(thread_id) {
            self.order.remove(used);
            *used = self.tick;
        }
        self.order.insert(self.tick, thread_id.to_string());
        self.tick
    }
}

impl DeltaCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(CacheState::default()),
        }
    }

    pub fn get(&self, thread_id: &str) -> Option<CachedRow> {
        let mut state = self.state.lock().ok()?;
        let row = state.rows.get(thread_id)?.0.clone();
        state.touch(thread_id);
        Some(row)
    }

    /// Remembers `row` as the thread's latest. A save's row replaces whatever
    /// is there (it was the latest when it was written); a load's only one
    /// that isn't newer, since a save may have finished while the load ran.
    pub fn put(&self, thread_id: &str, row: CachedRow, from_save: bool) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if let Some((cached, _)) = state.rows.get(thread_id)
            && !from_save
            && cached.sequence > row.sequence
        {
            return;
        }
        let tick = state.touch(thread_id);
        state.rows.insert(thread_id.to_string(), (row, tick));
        if state.rows.len() > self.capacity
            && let Some((_, oldest)) = state.order.pop_first()
        {
            state.rows.remove(&oldest);
        }
    }

    pub fn forget(&self, thread_id: &str) {
        if let Ok(mut state) = self.state.lock()
            && let Some((_, used)) = state.rows.remove(thread_id)
        {
            state.order.remove(&used);
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.state.lock().map_or(0, |state| state.rows.len())
    }
}

/// A store's half of delta checkpoints and value offload: decides how each
/// save is stored, and remembers what it wrote and read. Correctness never
/// rests on what it remembers: a save that relies on the row before it (a
/// delta of it, or one that leaves out blobs it says the thread has) is
/// appended only if that row is still the thread's latest, by its sequence and
/// its token, checked in the same atomic step as the append, and otherwise is
/// stored whole, with every blob it offloads written again.
pub(crate) struct CheckpointWriter {
    full_every: Option<u32>,
    threshold: Option<usize>,
    pub(crate) cache: DeltaCache,
}

impl CheckpointWriter {
    /// A writer for a store with `deltas` or `offload`; none when both are
    /// off.
    pub fn new(
        deltas: Option<&Deltas>,
        offload: Option<&Offload>,
    ) -> Result<Option<Self>, CheckpointStoreError> {
        if let Some(deltas) = deltas {
            deltas.check()?;
        }
        if let Some(offload) = offload {
            offload.check()?;
        }
        if deltas.is_none() && offload.is_none() {
            return Ok(None);
        }
        Ok(Some(Self {
            full_every: deltas.map(Deltas::full_every),
            threshold: offload.map(Offload::threshold),
            cache: DeltaCache::new(deltas.map_or(DEFAULT_CACHED_THREADS, Deltas::cached_threads)),
        }))
    }

    pub fn writes_deltas(&self) -> bool {
        self.full_every.is_some()
    }

    pub fn offloads(&self) -> bool {
        self.threshold.is_some()
    }

    /// How `checkpoint` is stored: a delta of the thread's latest row when it
    /// is running, the store remembers that row, and its chain is still
    /// shorter than [`Deltas::with_full_every`]; with its large values
    /// offloaded, those the store remembers the thread has not written again
    /// (all of them with `write_all_blobs`); whole and with every blob written
    /// when not `alone` (another save of its thread is in the same flush, so
    /// the row before it isn't known yet).
    pub fn prepare(&self, checkpoint: &Checkpoint, alone: bool, write_all_blobs: bool) -> Encoding {
        let (prints, large) = LevelPrints::of_with(checkpoint, self.threshold);
        let (prints, large) = (Arc::new(prints), large.map(Arc::new));
        let cached = if alone {
            self.cache.get(&checkpoint.thread_id)
        } else {
            None
        };
        let can_delta = self.full_every.is_some_and(|every| {
            checkpoint.status == CheckpointStatus::Running
                && cached
                    .as_ref()
                    .is_some_and(|cached| cached.depth + 1 < every)
        });
        encode(
            checkpoint,
            &prints,
            large.as_ref(),
            cached.as_ref(),
            can_delta,
            write_all_blobs,
        )
    }

    /// Remembers the row `checkpoint` was written as, `encoding`, at
    /// `sequence`, checked by `token`.
    pub fn saved(&self, checkpoint: &Checkpoint, encoding: &Encoding, sequence: i64, token: &str) {
        let prints = encoding
            .prints
            .clone()
            .unwrap_or_else(|| Arc::new(LevelPrints::of(checkpoint)));
        self.cache.put(
            &checkpoint.thread_id,
            CachedRow {
                sequence,
                depth: encoding.depth,
                prints,
                token: token.into(),
                blobs: encoding.remembered.clone(),
            },
            true,
        );
    }

    /// Remembers a thread's latest row, read at `sequence`, `depth` rows from
    /// a whole one, with `token`, referring to `decoded`'s offloaded values.
    pub fn loaded(&self, sequence: i64, depth: u32, decoded: &Decoded, token: &str) {
        self.cache.put(
            &decoded.checkpoint.thread_id,
            CachedRow {
                sequence,
                depth,
                prints: Arc::new(LevelPrints::of_known(&decoded.checkpoint, &decoded.blobs)),
                token: token.into(),
                blobs: Arc::new(decoded.blobs.iter().map(|slot| slot.hash.clone()).collect()),
            },
            false,
        );
    }

    pub fn forget(&self, thread_id: &str) {
        self.cache.forget(thread_id);
    }
}

/// The threads that have one save in `checkpoints`: only theirs may be
/// deltas, since the row before a thread's second save in a flush isn't known
/// until its first has its verdict.
pub(crate) fn alone<'a>(threads: impl IntoIterator<Item = &'a str>) -> HashSet<&'a str> {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for thread in threads {
        *counts.entry(thread).or_default() += 1;
    }
    counts
        .into_iter()
        .filter(|(_, count)| *count == 1)
        .map(|(thread, _)| thread)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn running(thread: &str, state: Value) -> Checkpoint {
        Checkpoint {
            schema_version: "1.7.0".to_string(),
            thread_id: thread.to_string(),
            status: CheckpointStatus::Running,
            suspended_node: Some("A".to_string()),
            next_node: Some("B".to_string()),
            frozen_state: state,
            metadata: None,
            created_at: "2026-10-15T00:00:00Z".to_string(),
            resumed_at: None,
            nested: None,
            decision: None,
            wait: None,
            graph: None,
        }
    }

    fn deltas(options: Deltas) -> CheckpointWriter {
        CheckpointWriter::new(Some(&options), None)
            .unwrap()
            .unwrap()
    }

    /// Decodes `rows`, giving each offloaded value from `blobs`.
    fn read(
        rows: Vec<(i64, Checkpoint)>,
        from: usize,
        blobs: &HashMap<String, Vec<u8>>,
    ) -> Result<Vec<Checkpoint>, CheckpointStoreError> {
        Ok(resolve(decode(rows, from)?, blobs)?
            .into_iter()
            .map(|row| row.checkpoint)
            .collect())
    }

    #[test]
    fn a_chain_of_deltas_decodes_to_what_was_saved_with_every_full_every_th_row_whole() {
        let writer = deltas(Deltas::new().with_full_every(3));
        let mut rows = Vec::new();
        let mut saved = Vec::new();
        for n in 0..7i64 {
            let checkpoint = running(
                "t",
                json!({ "step": n, "payload": "evidence", "gone": if n % 2 == 0 { json!(1) } else { Value::Null } }),
            );
            let mut checkpoint = checkpoint;
            if n % 2 == 1 {
                checkpoint
                    .frozen_state
                    .as_object_mut()
                    .unwrap()
                    .remove("gone");
            }
            let encoding = writer.prepare(&checkpoint, true, false);
            rows.push((n, encoding.stored(&checkpoint).clone()));
            writer.saved(&checkpoint, &encoding, n, "token");
            saved.push(checkpoint);
        }
        let depths: Vec<u32> = rows.iter().map(|(_, row)| depth_of(row)).collect();
        assert_eq!(depths, vec![0, 1, 2, 0, 1, 2, 0]);
        assert_eq!(read(rows.clone(), 0, &HashMap::new()).unwrap(), saved);
        // A row read with only the rows its depth needs.
        let tail = rows[3..6].to_vec();
        assert_eq!(read(tail, 2, &HashMap::new()).unwrap()[0], saved[5]);
    }

    #[test]
    fn a_delta_carries_only_the_keys_that_changed_and_those_that_went() {
        let writer = deltas(Deltas::new());
        let first = running("t", json!({ "step": 1, "payload": "x", "draft": true }));
        writer.saved(&first, &writer.prepare(&first, true, false), 0, "t0");
        let second = running("t", json!({ "step": 2, "payload": "x", "added": [1] }));
        let encoding = writer.prepare(&second, true, false);
        assert_eq!(
            encoding.stored(&second).frozen_state,
            json!({
                "checkpoint_version": "1.7.0",
                "delta_of": 0,
                "depth": 1,
                "set": { "step": 2, "added": [1] },
                "unset": ["draft"],
            })
        );
        assert_eq!(encoding.base_token(), Some("t0"));
        // Not alone in its flush, or not running: whole.
        assert!(!writer.prepare(&second, false, false).is_delta());
        let mut interrupted = second.clone();
        interrupted.status = CheckpointStatus::Interrupted;
        assert!(!writer.prepare(&interrupted, true, false).is_delta());
    }

    #[test]
    fn a_decoder_refuses_a_delta_read_without_its_base() {
        let writer = deltas(Deltas::new());
        let first = running("t", json!({ "step": 1 }));
        writer.saved(&first, &writer.prepare(&first, true, false), 4, "t4");
        let second = running("t", json!({ "step": 2 }));
        let stored = writer.prepare(&second, true, false).stored(&second).clone();
        let refused = decode(vec![(5, stored)], 0).unwrap_err();
        assert!(refused.0.contains("#5 is a delta of #4"), "{refused:?}");
    }

    #[test]
    fn the_cache_forgets_the_least_recently_used_thread() {
        let writer = deltas(Deltas::new().with_cached_threads(2));
        for thread in ["a", "b"] {
            let checkpoint = running(thread, json!({}));
            writer.saved(
                &checkpoint,
                &writer.prepare(&checkpoint, true, false),
                0,
                "x",
            );
        }
        assert!(writer.cache.get("a").is_some());
        let c = running("c", json!({}));
        writer.saved(&c, &writer.prepare(&c, true, false), 0, "x");
        assert_eq!(writer.cache.len(), 2);
        assert!(writer.cache.get("b").is_none());
        assert!(writer.cache.get("a").is_some());
        // A load doesn't move a thread back.
        writer.loaded(
            -1,
            0,
            &Decoded {
                sequence: -1,
                checkpoint: c.clone(),
                blobs: Vec::new(),
            },
            "y",
        );
        assert_eq!(writer.cache.get("c").unwrap().sequence, 0);
    }

    #[test]
    fn invalid_options_are_refused() {
        assert!(Deltas::new().with_full_every(1).check().is_err());
        assert!(Deltas::new().with_cached_threads(0).check().is_err());
        assert!(CheckpointWriter::new(None, Some(&Offload::new().with_threshold(0))).is_err());
        assert!(CheckpointWriter::new(None, None).unwrap().is_none());
        assert_eq!(Deltas::new().full_every(), 16);
    }

    #[test]
    fn fingerprints_ignore_key_order() {
        let a: Value = serde_json::from_str(r#"{"b":1,"a":{"y":2,"x":3}}"#).unwrap();
        let b: Value = serde_json::from_str(r#"{"a":{"x":3,"y":2},"b":1}"#).unwrap();
        assert_eq!(fingerprint(&a, usize::MAX).0, fingerprint(&b, usize::MAX).0);
    }

    /// A store's blobs, kept by what it writes.
    fn keep(blobs: &mut HashMap<String, Vec<u8>>, encoding: &Encoding) {
        for blob in &encoding.blobs {
            blobs.insert(blob.hash.clone(), blob.content.to_vec());
        }
    }

    #[test]
    fn a_large_value_is_offloaded_once_and_a_delta_inherits_it() {
        let writer = CheckpointWriter::new(
            Some(&Deltas::new()),
            Some(&Offload::new().with_threshold(16)),
        )
        .unwrap()
        .unwrap();
        let evidence = json!({ "z": "e".repeat(20), "a": [1, 2] });
        let mut blobs = HashMap::new();
        let mut rows = Vec::new();
        let mut saved = Vec::new();
        for step in 0..4 {
            let mut checkpoint = running("t", json!({ "step": step, "evidence": evidence }));
            if step == 3 {
                checkpoint.status = CheckpointStatus::Interrupted;
            }
            let encoding = writer.prepare(&checkpoint, true, false);
            keep(&mut blobs, &encoding);
            rows.push((step, encoding.stored(&checkpoint).clone()));
            writer.saved(&checkpoint, &encoding, step, &format!("t{step}"));
            saved.push((checkpoint, encoding));
        }
        // The first row offloads it whole; the deltas inherit it; the
        // interrupted row, whole again, refers to it without writing it.
        let (hash, bytes) = (
            saved[0].1.blobs[0].hash.clone(),
            saved[0].1.blobs[0].content.len(),
        );
        assert_eq!(blobs.len(), 1);
        assert_eq!(
            saved.iter().map(|(_, e)| e.label()).collect::<Vec<_>>(),
            ["offloaded", "delta", "delta", "offloaded"]
        );
        assert_eq!(
            rows[0].1.frozen_state,
            json!({
                "checkpoint_version": "1.7.0", "delta_of": null, "depth": 0,
                "state": { "step": 0 },
                "blobs": { "evidence": { "hash": hash, "bytes": bytes } },
            })
        );
        assert_eq!(rows[0].1.schema_version, OFFLOAD_CHECKPOINT_SCHEMA_VERSION);
        assert_eq!(rows[1].1.schema_version, DELTA_CHECKPOINT_SCHEMA_VERSION);
        assert!(saved[3].1.blobs.is_empty() && saved[3].1.is_based());
        assert_eq!(saved[3].1.base_token(), Some("t2"));
        // Decoded, the value is back, its keys sorted as the blob holds them.
        let decoded = read(rows.clone(), 0, &blobs).unwrap();
        assert_eq!(
            decoded,
            saved.iter().map(|(c, _)| c.clone()).collect::<Vec<_>>()
        );
        // Written whole, it writes the blob again.
        let whole = saved[3].1.whole(&saved[3].0);
        assert!(!whole.is_based() && whole.blobs.len() == 1);
    }

    #[test]
    fn an_offloaded_values_fingerprint_is_its_hash() {
        let value = json!({ "z": "e".repeat(20), "a": [1, 2] });
        let (print, text) = fingerprint(&value, 1);
        assert_eq!(digest_of(&hash_name(&print)), Some(print));
        assert!(matches(&hash_name(&print), &text.unwrap()));
        assert_eq!(digest_of("md5:00"), None);
    }

    #[test]
    fn a_missing_or_tampered_blob_fails_the_read() {
        let writer = CheckpointWriter::new(None, Some(&Offload::new().with_threshold(4)))
            .unwrap()
            .unwrap();
        let checkpoint = running("t", json!({ "big": "abcdefgh" }));
        let encoding = writer.prepare(&checkpoint, true, false);
        let rows = vec![(7, encoding.stored(&checkpoint).clone())];
        let hash = &encoding.blobs[0].hash;
        let missing = read(rows.clone(), 0, &HashMap::new()).unwrap_err();
        assert_eq!(
            missing.0,
            format!(
                "checkpoint 't' #7 refers to blob {hash} at 'big', which the blob store doesn't have"
            )
        );
        let tampered = HashMap::from([(hash.clone(), br#""abcdefgX""#.to_vec())]);
        let mismatch = read(rows, 0, &tampered).unwrap_err();
        assert!(
            mismatch.0.ends_with("whose content doesn't match"),
            "{mismatch:?}"
        );
    }
}
