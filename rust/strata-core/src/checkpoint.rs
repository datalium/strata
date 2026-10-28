//! The executor: human-in-the-loop interruption (STRATA-SPEC-002,
//! Freeze/Flush/Evict), streaming (STRATA-SPEC-001), error recovery, and
//! cancellation (`design/17-Async-Execution.md`).
//!
//! [`CheckpointStore`] is the port; concrete adapters live in `strata-memory`,
//! which depends on this crate for the trait and the [`schema::Checkpoint`]
//! shape — not the other way around. The store is async
//! (`design/19-Async-Queryable-Store.md`) and the executor awaits it directly;
//! the port and the query side are defined in [`crate::store`] and re-exported
//! here. `run` / `resume` take an `Arc<dyn CheckpointStore>` because a
//! streaming run moves it into a spawned task.
//!
//! `run` / `resume` are `async fn`s and must be polled inside a tokio runtime
//! (either flavour). An async handler is awaited and a sync one runs on the
//! blocking pool, so the executor never blocks a runtime thread on a node.
//! "Evict" means that once `Interrupted` (or `Cancelled`) is returned, nothing
//! of the run is still being waited for; the only thing that survives is
//! whatever the store persisted. A handler the engine gave up on (a timeout, a
//! cancellation) may still be running, but nothing waits for it and its result
//! is discarded.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::StrataError;
use crate::context::{LevelScope, Progress, RootScope, RunConfig, RunScope};
use crate::graph::{CompiledGraph, NodeError, NodeFailure, jitter_sample};
use crate::node::ResumeCheck;
use crate::node::{AttemptInputs, BoxFuture, EdgePlan, NodeEntry, NodeKind};
use crate::observability::{
    self, CatchUnwind, NodeResult, NodeScope, RunMode, RunOutcome, RunTelemetry, panic_message,
};
use crate::recovery::{
    RECOVERIES_KEY, RecoverOptions, RecoveredThread, RecoveryReport, continues_at, recoveries_of,
};
use crate::schema::{
    self, Checkpoint, CheckpointStatus, CheckpointWait, GraphIdentity, HumanDecision, WaitKind,
    versioned,
};
use crate::store::{LeasedSave, NO_LEASES, check_limit, is_due, timestamp_text, updated_at_text};
use crate::streaming::{StreamChunk, StreamChunkType, StreamContext};
use crate::subgraph::{AttemptError, NestedResume, Step};
pub use crate::wait::{EventSignal, WakeReason};
use crate::wait::{SkippedThread, WakeOptions, WakeReport, WokenThread, wake_text};

pub use crate::lease::LeaseOptions;
pub use crate::store::{
    BlockingCheckpointStore, CheckpointLogEntry, CheckpointLogPage, CheckpointQuery,
    CheckpointRetention, CheckpointStore, CheckpointStoreError, LatestCheckpoint, LeaseAttempt,
    MAX_PAGE_SIZE, PurgeStates, SyncCheckpointStore, ThreadCursor, ThreadLease, ThreadPage,
    ThreadQuery, ThreadState, ThreadSummary,
};

/// Human approval data injected on resume (STRATA-SPEC-002's `HumanSignal`).
/// Its overrides are merged into the frozen state's JSON object before it's
/// rehydrated back into `S`: `field_overrides` as JSON strings, `overrides`
/// with their JSON type (`design/21-Typed-Human-Signals.md`).
///
/// A resume that answers an interrupt boundary records the signal as the
/// level's [`HumanDecision`], which every node after it sees
/// ([`NodeContext::decision`](crate::context::NodeContext::decision)) and every
/// checkpoint the level writes afterwards carries. A boundary added with
/// [`StateGraph::add_interrupt_boundary_with_reject`](crate::graph::StateGraph::add_interrupt_boundary_with_reject)
/// continues at its reject route when `is_approved` is `false`.
///
/// Resuming a `failed` or `cancelled` thread takes a signal too, but uses
/// only its overrides: no decision is recorded, no reject route is taken,
/// and the level keeps the decision it already had, so a retry keeps the
/// original approver. `is_approved` and `reviewer_token` mean nothing there.
/// By convention, pass `HumanSignal::approve(who)` with the overrides. An
/// outer level's entry in `LevelSignals` is the same
/// (`design/16-Nested-Interrupts.md`, "Resuming a failed thread").
#[derive(Debug, Clone, Default)]
pub struct HumanSignal {
    /// The human's answer. At a boundary with a reject route, `false` takes
    /// the route.
    pub is_approved: bool,
    /// Who answered: an identity such as a user id or an email address, never
    /// a credential. The engine persists it in checkpoints and shows it to
    /// every node and observer.
    pub reviewer_token: String,
    /// Overrides written as JSON strings, whatever the field's type.
    pub field_overrides: HashMap<String, String>,
    /// Overrides written with their JSON type: `3`, `true`,
    /// `serde_json::to_value(&plan)?`. A JSON null sets the field to null.
    /// Each replaces a top-level field of the state. A key may not also be in
    /// `field_overrides`.
    pub overrides: HashMap<String, Value>,
}

impl HumanSignal {
    /// An approval by `reviewer`, with no overrides.
    pub fn approve(reviewer: impl Into<String>) -> Self {
        Self {
            is_approved: true,
            reviewer_token: reviewer.into(),
            ..Self::default()
        }
    }

    /// A rejection by `reviewer`, with no overrides.
    pub fn reject(reviewer: impl Into<String>) -> Self {
        Self {
            is_approved: false,
            reviewer_token: reviewer.into(),
            ..Self::default()
        }
    }

    /// This signal with one more typed override (replacing any for `key`).
    /// `impl Into<Value>` covers `&str` and `String` (a JSON string), numbers,
    /// `bool`, `Option`, `Vec` and `Value` itself.
    pub fn with_override(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.overrides.insert(key.into(), value.into());
        self
    }

    /// This signal with one more override written as a JSON string.
    pub fn with_field_override(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.field_overrides.insert(key.into(), value.into());
        self
    }

    /// Every override as a JSON value, sorted by key: the string ones as JSON
    /// strings, the typed ones as they are. A key set in both maps is an
    /// error.
    pub(crate) fn effective_overrides(&self) -> Result<serde_json::Map<String, Value>, String> {
        let mut effective: serde_json::Map<String, Value> = self
            .field_overrides
            .iter()
            .map(|(key, value)| (key.clone(), Value::String(value.clone())))
            .collect();
        let mut typed: Vec<(&String, &Value)> = self.overrides.iter().collect();
        typed.sort_by(|a, b| a.0.cmp(b.0));
        for (key, value) in typed {
            if self.field_overrides.contains_key(key) {
                return Err(format!(
                    "override '{key}' is set as both a string and a typed value"
                ));
            }
            effective.insert(key.clone(), value.clone());
        }
        Ok(effective)
    }

    /// The decision this signal makes when it answers `boundary`.
    fn decide(&self, boundary: &str, decided_at: &str) -> HumanDecision {
        HumanDecision {
            boundary: boundary.to_string(),
            is_approved: self.is_approved,
            reviewer: self.reviewer_token.clone(),
            overrides: self.effective_overrides().unwrap_or_default(),
            decided_at: decided_at.to_string(),
        }
    }
}

/// The human's answers to one `resume`: a [`HumanSignal`] for the innermost
/// graph (what a bare `HumanSignal` has always meant), plus optionally one for
/// each outer level, when the thread froze inside subgraphs
/// (`design/16-Nested-Interrupts.md`).
///
/// A level is addressed by the names of the subgraph nodes leading to it from
/// the graph `resume` is called on: `&[]` is that graph itself, `&["Work"]` the
/// child running inside its `Work` node, `&["Work", "Inner"]` that child's
/// child. A signal for an outer level applies its `field_overrides` to the state
/// the level's subgraph node started with, before the node re-enters its child,
/// so they reach the `output` mapper that folds the child's result back. Only
/// its overrides are used there: an outer level isn't waiting at a boundary of
/// its own, so its signal records no [`HumanDecision`] and takes no reject
/// route (`design/21-Typed-Human-Signals.md`).
///
/// `resume` takes anything that converts into this, so a plain `HumanSignal`
/// still works. A level the checkpoint doesn't have, or the innermost level
/// addressed twice (it already has the signal passed to [`Self::new`]), refuses
/// the resume before anything is changed.
#[derive(Debug, Clone, Default)]
pub struct LevelSignals {
    innermost: HumanSignal,
    outer: HashMap<Vec<String>, HumanSignal>,
    /// When the resume these signals belong to began: the `decided_at` of the
    /// decision it records, the same at every level and on every retry. Set
    /// once by the outermost resume.
    decided_at: Option<String>,
    /// What answers the innermost level (`design/29-Event-Waits-And-Timers.md`):
    /// a human (the default), an event signal, a due timer, or a timeout. Only
    /// a human's answer records a decision.
    wake: WakeReason,
    /// The event type, when `wake` is [`WakeReason::Event`].
    event_type: Option<String>,
    /// For [`Self::due`] and [`Self::due_at`], not yet decided: the time the
    /// thread must be due at. The outermost resume replaces these signals
    /// with the wake the thread is due for, or refuses them.
    due: Option<DueAt>,
}

/// When a [`LevelSignals::due`] resume decides whether the thread is due.
#[derive(Debug, Clone, Copy)]
enum DueAt {
    /// `Utc::now()` at the resume's load.
    Now,
    /// The caller's time.
    At(DateTime<Utc>),
}

impl LevelSignals {
    /// Signals with `innermost` for the deepest level the thread froze at.
    pub fn new(innermost: HumanSignal) -> Self {
        Self {
            innermost,
            outer: HashMap::new(),
            decided_at: None,
            wake: WakeReason::Human,
            event_type: None,
            due: None,
        }
    }

    /// Wakes a thread that is due (`design/42-Waking-Through-A-Resume.md`),
    /// through [`CompiledGraph::resume`] or [`CompiledGraph::resume_streaming`]:
    /// what [`CompiledGraph::wake_due`] does for one thread. The resume decides
    /// at its own load, at `Utc::now()`, that the thread is due: a timer whose
    /// time has come continues at the edge out of it, and a wait for a human
    /// or an event whose timeout has passed takes its timeout route or, with
    /// none, is ended with `strata.end_reason` `timed_out` (the result is
    /// `Cancelled` with [`CancelReason::Ended`]). A thread that isn't due,
    /// that waits with no time, or whose wait isn't one of this graph's is
    /// refused with `Failed` before anything is written. The stamp and the end
    /// are conditional, so this resume and a wake driver can't both wake one
    /// thread.
    pub fn due() -> Self {
        Self {
            due: Some(DueAt::Now),
            ..Self::for_wake(WakeReason::Human, None)
        }
    }

    /// [`Self::due`], decided at `now` instead of `Utc::now()`, as
    /// [`CompiledGraph::wake_due`] takes `now`.
    pub fn due_at(now: DateTime<Utc>) -> Self {
        Self {
            due: Some(DueAt::At(now)),
            ..Self::for_wake(WakeReason::Human, None)
        }
    }

    /// Answers a thread waiting for `signal`'s event, through
    /// [`CompiledGraph::resume`] or [`CompiledGraph::resume_streaming`], as
    /// [`CompiledGraph::signal`] does (`design/42-Waking-Through-A-Resume.md`):
    /// its overrides go to the waiting level's state, and it records no
    /// decision. A blank event type refuses the resume before anything is
    /// loaded.
    pub fn event(signal: EventSignal) -> Self {
        Self::for_wake(WakeReason::Event, Some(&signal))
    }

    /// Refuses signals that can't answer anything, before the resume loads:
    /// an event signal with no event type.
    fn validate(&self) -> Result<(), String> {
        let blank = self
            .event_type
            .as_deref()
            .is_none_or(|event| event.trim().is_empty());
        if self.wake == WakeReason::Event && blank {
            return Err("a signal needs an event type".to_string());
        }
        Ok(())
    }

    /// What `checkpoint`, a thread's latest, is due for at `due` (whole
    /// seconds): a timer's wake or a timeout, by the stored wait; `None` when
    /// it isn't due.
    pub(crate) fn due_for(checkpoint: &Checkpoint, due: &str) -> Option<WakeReason> {
        if !is_due(checkpoint, due) {
            return None;
        }
        Some(match checkpoint.innermost_wait().map(|wait| wait.kind) {
            Some(WaitKind::Timer) => WakeReason::Timer,
            _ => WakeReason::Timeout,
        })
    }

    /// For [`Self::due`]: the wake `checkpoint` is due for, or these signals
    /// unchanged when it isn't due (the resume then refuses them, after its
    /// own refusals).
    fn when_due(self, checkpoint: &Checkpoint) -> Self {
        let Some(at) = self.due else {
            return self;
        };
        let now = match at {
            DueAt::Now => Utc::now(),
            DueAt::At(now) => now,
        };
        match Self::due_for(checkpoint, &timestamp_text(now)) {
            Some(wake) => Self {
                wake,
                due: None,
                ..self
            },
            None => self,
        }
    }

    /// Why `checkpoint`, which [`Self::due`] found not due, isn't.
    fn not_due(thread_id: &str, checkpoint: &Checkpoint) -> String {
        if checkpoint.status != CheckpointStatus::Interrupted {
            return format!(
                "thread '{thread_id}' is not due: its latest checkpoint is {}",
                ThreadState::of(checkpoint).as_str()
            );
        }
        let wait = checkpoint.innermost_wait();
        let at = format!("[{}]", checkpoint.waiting_at().join("/"));
        let what = wait.map_or_else(|| "a human".to_string(), CheckpointWait::describe);
        match wait.and_then(|wait| wait.wake_at.as_deref()) {
            Some(wake_at) => format!(
                "thread '{thread_id}' is not due until {wake_at}: it is waiting for {what} at {at}"
            ),
            None => format!(
                "thread '{thread_id}' is not due: it is waiting for {what} at {at}, with no timeout"
            ),
        }
    }

    /// Signals for a wake that isn't a human's: the event's overrides (as an
    /// innermost signal that approves, with no reviewer), or none for a timer
    /// or a timeout.
    pub(crate) fn for_wake(wake: WakeReason, signal: Option<&EventSignal>) -> Self {
        Self {
            innermost: HumanSignal {
                is_approved: true,
                overrides: signal
                    .map(|signal| signal.overrides.clone())
                    .unwrap_or_default(),
                ..HumanSignal::default()
            },
            outer: HashMap::new(),
            decided_at: None,
            wake,
            event_type: signal.map(|signal| signal.event_type.clone()),
            due: None,
        }
    }

    /// Signals that answer nothing and override nothing: what a recovery
    /// continues a thread with (`design/33-Durable-Execution.md`).
    pub(crate) fn none() -> Self {
        Self::for_wake(WakeReason::Human, None)
    }

    pub(crate) fn wake(&self) -> WakeReason {
        self.wake
    }

    pub(crate) fn event_type(&self) -> Option<&str> {
        self.event_type.as_deref()
    }

    pub(crate) fn innermost(&self) -> &HumanSignal {
        &self.innermost
    }

    /// These signals, stamped with the time the resume began.
    fn stamped(mut self, decided_at: String) -> Self {
        self.decided_at = Some(decided_at);
        self
    }

    /// Adds a signal for the level at `path` (see the type's docs).
    pub fn at(mut self, path: &[&str], signal: HumanSignal) -> Self {
        self.outer
            .insert(path.iter().map(|node| node.to_string()).collect(), signal);
        self
    }

    /// Refuses signals that address a level `checkpoint` doesn't have.
    fn check(&self, checkpoint: &Checkpoint) -> Result<(), String> {
        // Only a human's resume addresses outer levels
        // (design/42-Waking-Through-A-Resume.md).
        if !self.outer.is_empty() && (self.wake != WakeReason::Human || self.due.is_some()) {
            return Err(
                "a wake or an event signal answers the innermost level only, and takes no signals for outer levels"
                    .to_string(),
            );
        }
        let mut levels: Vec<Vec<String>> = vec![Vec::new()];
        let mut level = checkpoint;
        while let Some(child) = level.nested.as_deref() {
            let node = level
                .next_node
                .clone()
                .or_else(|| level.suspended_node.clone())
                .unwrap_or_default();
            let mut path = levels.last().cloned().unwrap_or_default();
            path.push(node);
            levels.push(path);
            level = child;
        }
        let show = |path: &[String]| {
            if path.is_empty() {
                "[]".to_string()
            } else {
                format!("[{}]", path.join("/"))
            }
        };
        let innermost = levels.last().cloned().unwrap_or_default();
        let mut addressed: Vec<&Vec<String>> = self.outer.keys().collect();
        addressed.sort();
        for path in addressed {
            if *path == innermost {
                return Err(format!(
                    "a signal was addressed to level {}, which is the innermost level and already has the innermost signal",
                    show(path)
                ));
            }
            if !levels.contains(path) {
                return Err(format!(
                    "a signal was addressed to level {}, but the checkpoint's levels are {}",
                    show(path),
                    levels
                        .iter()
                        .map(|l| show(l))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
        }
        Ok(())
    }

    /// The signal addressed to the level being resumed itself, if any.
    fn own(&self) -> Option<&HumanSignal> {
        self.outer.get(&Vec::new())
    }

    /// The signals for the level inside `node`, addressed relative to it.
    fn descend(&self, node: &str) -> LevelSignals {
        let outer = self
            .outer
            .iter()
            .filter_map(|(path, signal)| match path.split_first() {
                Some((first, rest)) if first == node => Some((rest.to_vec(), signal.clone())),
                _ => None,
            })
            .collect();
        LevelSignals {
            innermost: self.innermost.clone(),
            outer,
            decided_at: self.decided_at.clone(),
            wake: self.wake,
            event_type: self.event_type.clone(),
            due: self.due,
        }
    }
}

impl From<HumanSignal> for LevelSignals {
    fn from(signal: HumanSignal) -> Self {
        Self::new(signal)
    }
}

/// Merges `signal`'s overrides into the object `state`: string ones as JSON
/// strings, typed ones with their JSON type. A key set in both maps is refused
/// before this.
fn apply_overrides(state: &mut Value, signal: Option<&HumanSignal>) {
    let (Some(signal), Some(map)) = (signal, state.as_object_mut()) else {
        return;
    };
    for (key, value) in signal.effective_overrides().unwrap_or_default() {
        map.insert(key, value);
    }
}

pub enum GraphExecutionResult<S> {
    Completed(S),
    Interrupted {
        thread_id: String,
        suspended_node: String,
        next_node: String,
        frozen_state: S,
        /// Where the thread is actually waiting, one node per level from the
        /// outermost graph in: `["Approve"]` for a boundary in this graph,
        /// `["Work", "Approve"]` for one at `Approve` inside the subgraph node
        /// `Work` (whose name is then also `suspended_node`).
        waiting_at: Vec<String>,
        /// What the innermost level waits for, when it is more than a human
        /// with no timeout (`design/29-Event-Waits-And-Timers.md`): an event,
        /// a timer, or a human with a timeout, and when it wakes or times out.
        /// The checkpoint's innermost `wait`.
        wait: Option<CheckpointWait>,
    },
    Failed(StrataError),
    /// The run was cancelled through its token, or its deadline passed
    /// (`design/17-Async-Execution.md`). A `CheckpointStatus::Cancelled`
    /// checkpoint was persisted before this was returned: `node` is the node
    /// that was running or about to start, and `state` the last committed
    /// state, the one that node started with. Resuming the thread re-enters
    /// `node`. `stopped_at` is where the run stopped, one node per level from
    /// the outermost graph in, like `Interrupted::waiting_at`.
    ///
    /// Deliberately not a `Failed`: a cancellation is not a failure, and no
    /// recovery policy sees it.
    ///
    /// [`CompiledGraph::end`] returns it too, with [`CancelReason::Ended`]
    /// (`design/28-Ending-A-Thread.md`): then the checkpoint is `ended`, `node`
    /// is where the thread stood, and the thread can't be resumed.
    ///
    /// With [`CancelReason::LeaseLost`] (`design/32-Thread-Leases.md`) no
    /// checkpoint was persisted: the run lost its thread's lease to another
    /// owner, and the thread's history is theirs.
    Cancelled {
        thread_id: String,
        node: String,
        reason: CancelReason,
        state: S,
        stopped_at: Vec<String>,
    },
}

/// Why a run ended [`GraphExecutionResult::Cancelled`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelReason {
    /// The token passed in [`RunConfig::with_cancellation`] was cancelled.
    Requested,
    /// The deadline passed in [`RunConfig::with_deadline`] passed.
    Deadline,
    /// The thread was ended by [`CompiledGraph::end`]
    /// (`design/28-Ending-A-Thread.md`): its latest checkpoint is
    /// [`CheckpointStatus::Ended`], and it can't be resumed.
    Ended,
    /// The run lost its thread's lease ([`RunConfig::with_lease`],
    /// `design/32-Thread-Leases.md`): another owner acquired the thread, or no
    /// renewal succeeded for a whole ttl. The run stopped and wrote nothing
    /// more, not even a `cancelled` checkpoint: the thread is the new owner's.
    LeaseLost,
}

/// Who ends a thread with [`CompiledGraph::end_with`]
/// (`design/28-Ending-A-Thread.md`). Mirrors the .NET engine's `ThreadEnd`.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEnd {
    /// Who ended it, when known: an operator, a service, a reviewer. Recorded
    /// as [`ENDED_BY_KEY`].
    pub ended_by: Option<String>,
    /// Why: [`END_REQUESTED`] for every caller's end, [`END_TIMED_OUT`] for
    /// the engine's.
    pub(crate) reason: &'static str,
}

impl Default for ThreadEnd {
    fn default() -> Self {
        Self {
            ended_by: None,
            reason: END_REQUESTED,
        }
    }
}

impl ThreadEnd {
    /// An end by nobody in particular.
    pub fn new() -> Self {
        Self::default()
    }

    /// An end by `ended_by`.
    pub fn by(ended_by: impl Into<String>) -> Self {
        Self {
            ended_by: Some(ended_by.into()),
            ..Self::default()
        }
    }
}

/// The checkpoint `metadata` key an end's reason is recorded under
/// (`design/28-Ending-A-Thread.md`). Mirrors .NET's `ThreadEnd.ReasonKey`.
pub const END_REASON_KEY: &str = "strata.end_reason";

/// The checkpoint `metadata` key [`ThreadEnd::ended_by`] is recorded under.
/// Mirrors .NET's `ThreadEnd.EndedByKey`.
pub const ENDED_BY_KEY: &str = "strata.ended_by";

/// The reason a caller's end records: it was asked for.
pub const END_REQUESTED: &str = "requested";

/// The reason an end records when a wait timed out with no timeout route
/// (`design/29-Event-Waits-And-Timers.md`). Only the engine ends a thread with
/// it. Mirrors .NET's `ThreadEnd.TimedOut`.
pub const END_TIMED_OUT: &str = "timed_out";

/// How many times an end reloads a thread that changed under it before giving
/// up.
const END_ATTEMPTS: u32 = 3;

/// What running a node resolved to.
enum NodeOutcome<S> {
    /// An attempt succeeded; this is the new committed state.
    Committed(S),
    /// The node couldn't be recovered by retrying, and its policy names a
    /// fallback: continue there with the pre-failure state, telling it why
    /// ([`crate::context::NodeContext::last_failure`]).
    Fallback(String, NodeFailure),
    /// A subgraph node's child froze at an interrupt boundary; the run freezes
    /// around its checkpoint.
    Interrupted(Box<Checkpoint>),
    /// The run was cancelled while the node ran (or before it started).
    /// Carries the child's `cancelled` checkpoint when the node is a subgraph,
    /// for the run's to nest.
    Cancelled(Option<Box<Checkpoint>>),
    /// The node can't be executed, or failed with no fallback (in which case
    /// the `Failed` checkpoint has already been persisted). Carries the result
    /// to return from the run.
    Terminal(GraphExecutionResult<S>),
}

/// How one attempt of a node ended.
enum Attempt<S> {
    Finished(Result<Step<S>, AttemptError>),
    /// Its token fired (the run was cancelled, or it timed out) and the
    /// engine stopped waiting for it.
    Stopped,
}

/// An attempt that failed: its error (with the child's checkpoint, for a
/// subgraph), the error's telemetry tag, whether it was the node's timeout,
/// and which attempt it was.
struct FailedAttempt {
    failure: AttemptError,
    error_type: &'static str,
    timed_out: bool,
    attempts: u32,
}

/// What every node of one level of a run shares.
#[derive(Clone, Copy)]
pub(crate) struct RunRefs<'a> {
    pub(crate) thread_id: &'a str,
    pub(crate) store: &'a Arc<dyn CheckpointStore>,
    pub(crate) stream: Option<&'a StreamContext>,
    pub(crate) level: &'a LevelScope,
}

/// Whether a resume of `checkpoint` answers an interrupt boundary of its own
/// graph: it is interrupted at one of that graph's boundaries, with nothing
/// nested.
fn answers_boundary(checkpoint: &Checkpoint) -> bool {
    checkpoint.status == CheckpointStatus::Interrupted && checkpoint.nested.is_none()
}

/// The checkpoint `metadata` key holding a level's hop count
/// (`design/24-Loop-Circuit-Breaker.md`, "Persistence"): the hops the level
/// made before the checkpoint's next node, which is the count a resume from it
/// starts from. Mirrors the .NET engine's `HopLimit.MetadataKey`.
pub(crate) const HOPS_KEY: &str = "strata.hops";

/// The count `checkpoint`'s level resumes from: its `strata.hops`, or 0 when
/// it has none (written before SG-05, or while the level had no limit). `Err`
/// carries a stored value that is not a non-negative integer.
fn stored_hops(checkpoint: &Checkpoint) -> Result<u32, String> {
    match checkpoint
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get(HOPS_KEY))
    {
        None => Ok(0),
        Some(stored) if !stored.is_empty() && stored.bytes().all(|b| b.is_ascii_digit()) => {
            stored.parse().map_err(|_| stored.clone())
        }
        Some(stored) => Err(stored.clone()),
    }
}

/// The checkpoint `metadata` key a `failed` checkpoint records a hinted final
/// failure under (`design/25-Retry-Backoff.md`): the hint in whole
/// milliseconds, as a decimal string, so a subgraph node can carry its child's
/// hint and an operator can see it. Mirrors the .NET engine's
/// `RetryAfterHint.MetadataKey`.
pub(crate) const RETRY_AFTER_KEY: &str = "strata.retry_after_ms";

/// The retry hint `metadata` records, if it records a readable one.
pub(crate) fn stored_retry_after(metadata: Option<&HashMap<String, String>>) -> Option<Duration> {
    let stored = metadata?.get(RETRY_AFTER_KEY)?;
    if stored.is_empty() || !stored.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    stored.parse().ok().map(Duration::from_millis)
}

/// Waits `delay` before a retry, or until `level`'s token fires (the run was
/// cancelled, its deadline passed, or an enclosing subgraph timed out),
/// whichever comes first. The retry loop then sees the token as it would at
/// the top of any attempt. A zero delay doesn't wait at all. The wait is on
/// tokio's clock, so a test can pause it (`design/25-Retry-Backoff.md`).
async fn back_off(delay: Duration, level: &LevelScope) {
    if delay.is_zero() {
        return;
    }
    tokio::select! {
        _ = tokio::time::sleep(delay) => {}
        _ = level.token.cancelled() => {}
    }
}

pub(crate) fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Awaits the store's save. Never raced against the run's token: a cancelled
/// run must still write its `cancelled` checkpoint.
///
/// At the outermost level, where `store` is the caller's, the save is timed
/// and reported as
/// `CheckpointSaved` (`design/18-Run-Observability.md`). A subgraph's child
/// saves to its own capturing store, which isn't reported.
///
/// A run with a lease saves under it at the outermost level
/// (`design/32-Thread-Leases.md`): `Ok(false)` means the lease is lost and
/// nothing was written, and the run then ends with [`lease_lost_at`].
async fn save_checkpoint(
    store: &Arc<dyn CheckpointStore>,
    checkpoint: Checkpoint,
    level: &LevelScope,
) -> Result<bool, CheckpointStoreError> {
    let observed = level.is_root().then(|| {
        (
            checkpoint.status,
            checkpoint.waiting_at(),
            checkpoint.resumed_at.is_some(),
            running_copy(&checkpoint, level),
        )
    });
    let started = Instant::now();
    let saved = match level.run.lease.as_ref().filter(|_| level.is_root()) {
        Some(lease) => lease.save(store, checkpoint, None).await? != LeasedSave::LeaseLost,
        None => {
            store.save(checkpoint).await?;
            true
        }
    };
    if let (true, Some((status, waiting_at, resume_stamp, running))) = (saved, observed) {
        level.run.saved(running);
        level
            .run
            .telemetry
            .checkpoint_saved(status, waiting_at, resume_stamp, started.elapsed());
    }
    Ok(saved)
}

/// What [`RunScope::saved`] is told once `checkpoint` is saved at the
/// outermost level: the checkpoint itself if it is `running` (it is then what
/// the thread's log ends with), nothing otherwise. Only a durable run keeps
/// one, so only it pays for the copy.
fn running_copy(checkpoint: &Checkpoint, level: &LevelScope) -> Option<Checkpoint> {
    (level.run.durable && checkpoint.status == CheckpointStatus::Running)
        .then(|| checkpoint.clone())
}

/// [`save_checkpoint`] for the outermost level's conditional saves (a
/// resume's stamp, an end), through [`CheckpointStore::save_if_latest`]:
/// saves only if the thread's latest sequence is still `expected`, and
/// reports the save only when it wrote (`design/28-Ending-A-Thread.md`,
/// "Races").
async fn save_checkpoint_if_latest(
    store: &Arc<dyn CheckpointStore>,
    checkpoint: Checkpoint,
    expected: Option<i64>,
    level: &LevelScope,
) -> Result<LeasedSave, CheckpointStoreError> {
    let observed = (
        checkpoint.status,
        checkpoint.waiting_at(),
        checkpoint.resumed_at.is_some(),
    );
    let running = running_copy(&checkpoint, level);
    let started = Instant::now();
    let saved = match &level.run.lease {
        Some(lease) => lease.save(store, checkpoint, expected).await?,
        None if store.save_if_latest(checkpoint, expected).await? => LeasedSave::Saved,
        None => LeasedSave::NotLatest,
    };
    if saved == LeasedSave::Saved {
        let (status, waiting_at, resume_stamp) = observed;
        level.run.saved(running);
        level
            .run
            .telemetry
            .checkpoint_saved(status, waiting_at, resume_stamp, started.elapsed());
    }
    Ok(saved)
}

/// Takes the thread's lease for a run that has one
/// ([`RunConfig::with_lease`], `design/32-Thread-Leases.md`), before its first
/// save. Returns the refusal when another owner holds the thread (or the store
/// has no leases), `None` when the run may go on: it holds the lease now, or
/// runs without one. Only the outermost level saves, so only it acquires.
async fn acquire_lease<S>(
    thread_id: &str,
    store: &Arc<dyn CheckpointStore>,
    level: &LevelScope,
) -> Option<GraphExecutionResult<S>> {
    let lease = level.run.lease.as_ref().filter(|_| level.is_root())?;
    let refusal = match lease.acquire(store, thread_id).await {
        Ok(None) => return None,
        Ok(Some(holder)) => {
            // Rounded up to the second, so the thread isn't said to be free
            // before it is.
            let held = format!(
                "held by '{}' until {}",
                holder.owner,
                wake_text(holder.expires_at)
            );
            let refusal = format!("thread '{thread_id}' is {held}");
            level.run.set_held(held);
            refusal
        }
        Err(err) => err.0,
    };
    Some(GraphExecutionResult::Failed(StrataError::ExecutionAborted(
        refusal,
    )))
}

/// For a run without a lease of its own that is about to take over a `running`
/// thread (a recovery, an end): the refusal when a live lease holds the thread,
/// since whoever holds it is running it (`design/33-Durable-Execution.md`).
/// `None` when nobody does, when the store has no leases, and for a run with a
/// lease, which has acquired it by now.
async fn held_by_another<S>(
    thread_id: &str,
    store: &Arc<dyn CheckpointStore>,
    level: &LevelScope,
) -> Option<GraphExecutionResult<S>> {
    if level.run.lease.is_some() {
        return None;
    }
    let refusal = match store.get_lease(thread_id).await {
        Ok(None) => return None,
        Err(err) if err.0 == NO_LEASES => return None,
        Ok(Some(live)) => {
            let held = format!(
                "held by '{}' until {}",
                live.owner,
                wake_text(live.expires_at)
            );
            let refusal = format!("thread '{thread_id}' is {held}");
            level.run.set_held(held);
            refusal
        }
        Err(err) => err.0,
    };
    Some(GraphExecutionResult::Failed(StrataError::ExecutionAborted(
        refusal,
    )))
}

/// A level's `running` checkpoint (`design/33-Durable-Execution.md`): `node`
/// committed (`None` before the first), `next` is where the level goes on, and
/// `state` is what it starts with. With `child`, the level is inside the
/// subgraph node `next`, which a recovery re-enters. `level` gives the hops
/// made before `next`.
fn running_checkpoint(
    thread_id: &str,
    node: Option<&str>,
    next: &str,
    state: Value,
    level: &LevelScope,
    child: Option<Checkpoint>,
    graph: Option<GraphIdentity>,
) -> Checkpoint {
    versioned(Checkpoint {
        schema_version: schema::RUNNING_CHECKPOINT_SCHEMA_VERSION.to_string(),
        thread_id: thread_id.to_string(),
        status: CheckpointStatus::Running,
        suspended_node: node.map(str::to_string),
        next_node: Some(next.to_string()),
        frozen_state: state,
        metadata: level.checkpoint_metadata(None),
        created_at: now_rfc3339(),
        resumed_at: None,
        nested: child.map(Box::new),
        decision: level.decision(),
        wait: None,
        graph,
    })
}

/// For a durable run that is ending without having said how
/// (`design/33-Durable-Execution.md`, "What a durable run writes"): it
/// returned `Failed` for something no checkpoint recorded (an edge with no
/// matching condition, a node it can't run), or a panic is propagating out of
/// it, and its thread's log still ends with a `running` checkpoint of its own.
/// Saves a `failed` one in its place, so a process that is alive never leaves a
/// thread `running`. A run that lost its lease writes nothing, as always, and a
/// store that fails now leaves the thread running, for a recovery.
async fn fail_running(level: &LevelScope, store: &Arc<dyn CheckpointStore>, error: &str) {
    let Some(running) = level.run.last_running() else {
        return;
    };
    // What the run is ending with stands.
    let _ = save_checkpoint(store, failing(running, error, &now_rfc3339()), level).await;
}

/// `running` as a `failed` checkpoint, at every level: failed at the node that
/// was to run, with the state it would have started with, so a resume starts
/// it.
fn failing(mut running: Checkpoint, error: &str, now: &str) -> Checkpoint {
    let node = running
        .next_node
        .take()
        .or_else(|| running.suspended_node.take());
    let metadata = running.metadata.get_or_insert_with(HashMap::new);
    metadata.insert("error".to_string(), error.to_string());
    metadata.insert("error_kind".to_string(), "fatal".to_string());
    metadata.remove(RECOVERIES_KEY);
    running.status = CheckpointStatus::Failed;
    running.suspended_node = node.clone();
    running.next_node = node;
    running.created_at = now.to_string();
    running.nested = running
        .nested
        .take()
        .map(|child| Box::new(failing(*child, error, now)));
    versioned(running)
}

/// A `running` checkpoint with each level suspended at the node it was to run,
/// which is where a running thread stands: what ending it records, as a failed
/// or cancelled thread's checkpoint does.
fn standing(mut running: Checkpoint) -> Checkpoint {
    if running.next_node.is_some() {
        running.suspended_node = running.next_node.clone();
    }
    running.nested = running
        .nested
        .take()
        .map(|child| Box::new(standing(*child)));
    running
}

/// How a run that lost its lease ends: cancelled at `node` with `state`, as
/// any cancellation reports them, with nothing written.
fn lease_lost_at<S>(
    thread_id: &str,
    node: &str,
    state: S,
    stopped_at: Vec<String>,
) -> GraphExecutionResult<S> {
    GraphExecutionResult::Cancelled {
        thread_id: thread_id.to_string(),
        node: node.to_string(),
        reason: CancelReason::LeaseLost,
        state,
        stopped_at,
    }
}

/// Loads the caller's checkpoint for a resume or an end, with its sequence,
/// timed for `strata.checkpoint.latency`.
async fn load_checkpoint(
    store: &Arc<dyn CheckpointStore>,
    thread_id: &str,
) -> Result<Option<LatestCheckpoint>, CheckpointStoreError> {
    let started = Instant::now();
    let result = store.load_latest(thread_id).await;
    if let Ok(loaded) = &result {
        RunTelemetry::checkpoint_loaded(
            loaded.as_ref().map(|latest| latest.checkpoint.status),
            started.elapsed(),
        );
    }
    result
}

/// Runs `run` inside the run's span and ends the run's telemetry with its
/// result, or with the panic propagating out of it
/// (`design/18-Run-Observability.md`). The run's lease, if it has one, is
/// given up first, however the run ends (`design/32-Thread-Leases.md`).
/// `store` is the caller's, for the `failed` checkpoint of a durable run that
/// ends without one.
async fn observed<S>(
    root: &RootScope,
    store: &Arc<dyn CheckpointStore>,
    run: impl Future<Output = GraphExecutionResult<S>>,
) -> GraphExecutionResult<S> {
    let telemetry = root.telemetry();
    let span = telemetry.span.clone();
    let outcome = CatchUnwind::new(run.instrument(span)).await;
    // A durable run never leaves its thread running when it returns
    // (design/33-Durable-Execution.md).
    match &outcome {
        Ok(GraphExecutionResult::Failed(error)) => {
            fail_running(&root.level, store, &observability::error_message(error)).await;
        }
        Err(panic) => fail_running(&root.level, store, &panic_message(&**panic)).await,
        Ok(_) => {}
    }
    if let Some(lease) = &root.level.run.lease {
        lease.release().await;
    }
    match outcome {
        Ok(result) => {
            telemetry.completed_with(&result);
            result
        }
        Err(panic) => {
            telemetry.run_completed(RunOutcome::Failed, Some(panic_message(&*panic)), None);
            std::panic::resume_unwind(panic)
        }
    }
}

/// A run its config refuses, before anything starts (a reserved metadata key).
fn refused<S>(message: String) -> GraphExecutionResult<S> {
    GraphExecutionResult::Failed(StrataError::ExecutionAborted(message))
}

/// The engine's own chunk (`NodeEnd`, `Retry`, `Error`): sent at once when
/// there is room, else waited for until `token` fires, when it is dropped. A
/// run that is ending doesn't wait for a reader that may have stopped reading.
async fn emit(
    ctx: Option<&StreamContext>,
    chunk_type: StreamChunkType,
    message: &str,
    token: &CancellationToken,
) {
    if let Some(ctx) = ctx {
        tokio::select! {
            biased;
            _ = ctx.emit_diagnostic(chunk_type, message) => {}
            _ = token.cancelled() => {}
        }
    }
}

fn timed_out(node: &str, timeout: Option<Duration>) -> String {
    format!(
        "node '{node}' timed out after {} ms",
        timeout.unwrap_or_default().as_millis()
    )
}

/// One attempt of a node. The engine stops waiting the moment the attempt's
/// token fires or its timeout elapses (cancelling the token), and drops the
/// handler's future: it never waits for a handler it gave up on. A subgraph is
/// the exception: it is a graph, not a handler, so it is told (through its
/// token) and awaited, and winds down by itself, handing back the checkpoint
/// of where its child stopped. A map is awaited too, while its items wind
/// down (`design/27-Map-Fan-Out.md`), and is then reported stopped.
async fn attempt<S>(
    entry: &NodeEntry<S>,
    state: S,
    inputs: AttemptInputs,
    token: &CancellationToken,
    timeout: Option<Duration>,
) -> Attempt<S> {
    let mut future = (entry.handler)(state, inputs);
    let expiry = async {
        match timeout {
            Some(duration) => tokio::time::sleep(duration).await,
            None => std::future::pending().await,
        }
    };
    if entry.is_subgraph {
        return Attempt::Finished(tokio::select! {
            biased;
            result = &mut future => result,
            _ = expiry => {
                token.cancel();
                future.await
            }
        });
    }
    if entry.winds_down {
        // The map watches the token itself, and returns once its items have
        // stopped; a failure then is the token's doing.
        let result = tokio::select! {
            biased;
            result = &mut future => result,
            _ = expiry => {
                token.cancel();
                future.await
            }
        };
        return match result {
            Err(_) if token.is_cancelled() => Attempt::Stopped,
            result => Attempt::Finished(result),
        };
    }
    tokio::select! {
        biased;
        result = future => Attempt::Finished(result),
        _ = token.cancelled() => Attempt::Stopped,
        _ = expiry => {
            token.cancel();
            Attempt::Stopped
        }
    }
}

/// A run's stream ends when the run does, however it ends (`StreamContext::close`).
struct CloseOnDrop(StreamContext);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        self.0.close();
    }
}

type StreamingRun<S> = (
    JoinHandle<GraphExecutionResult<S>>,
    tokio::sync::mpsc::Receiver<StreamChunk>,
);

impl<S> CompiledGraph<S>
where
    S: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    /// Executes from the entry point. Runs nodes to completion, or until an
    /// interrupt boundary is hit, in which case the state is frozen and
    /// flushed to `store` before returning `Interrupted`.
    pub async fn run(
        &self,
        thread_id: impl Into<String>,
        initial_state: S,
        store: Arc<dyn CheckpointStore>,
    ) -> GraphExecutionResult<S> {
        self.run_with(thread_id, initial_state, store, RunConfig::default())
            .await
    }

    /// [`Self::run`] with run-level inputs (`design/17-Async-Execution.md`): a
    /// cancellation token and deadline, and a history store and services for
    /// every async handler's [`crate::context::NodeContext`]. A cancelled run
    /// ends [`GraphExecutionResult::Cancelled`] with a `cancelled` checkpoint
    /// at the node that was running, which a resume re-enters. Dropping the
    /// returned future is *not* a cancellation: it persists nothing.
    pub async fn run_with(
        &self,
        thread_id: impl Into<String>,
        initial_state: S,
        store: Arc<dyn CheckpointStore>,
        config: RunConfig,
    ) -> GraphExecutionResult<S> {
        let thread_id = thread_id.into();
        if let Err(message) = config.validate() {
            return refused(message);
        }
        // Waits for the run's scheduler's slot first (design/40-Run-Scheduler.md).
        let mut config = config;
        let _slot = match config.admit(None, None).await {
            Ok(slot) => slot,
            Err(refused) => return GraphExecutionResult::Failed(refused.into()),
        };
        let root = RootScope::start(&thread_id, config, RunMode::Run, self.durable());
        let run = RunRefs {
            thread_id: &thread_id,
            store: &store,
            stream: None,
            level: &root.level,
        };
        observed(&root, &store, self.start(run, initial_state)).await
    }

    /// A fresh run of the outermost graph: takes the thread's lease, when the
    /// run has one (`design/32-Thread-Leases.md`), before the first node
    /// starts, then runs from the entry point. A durable run first says it is
    /// running (`design/33-Durable-Execution.md`): a process that dies in the
    /// first node then leaves a thread to recover, not nothing.
    async fn start(&self, run: RunRefs<'_>, initial_state: S) -> GraphExecutionResult<S> {
        let entry: &str = &self.definition.entry_point;
        run.level.run.telemetry.run_started(entry, self.identity());
        if let Some(held) = acquire_lease(run.thread_id, run.store, run.level).await {
            return held;
        }
        if run.level.run.durable {
            let state = match serde_json::to_value(&initial_state) {
                Ok(value) => value,
                Err(err) => {
                    return GraphExecutionResult::Failed(StrataError::ExecutionAborted(format!(
                        "failed to persist state before '{entry}': {err}"
                    )));
                }
            };
            let running = running_checkpoint(
                run.thread_id,
                None,
                entry,
                state,
                &run.level.with_hops(Some(0)),
                None,
                self.identity.clone(),
            );
            match save_checkpoint(run.store, running, run.level).await {
                Ok(true) => {}
                Ok(false) => {
                    let stopped_at = vec![entry.to_string()];
                    return lease_lost_at(run.thread_id, entry, initial_state, stopped_at);
                }
                Err(err) => {
                    return GraphExecutionResult::Failed(StrataError::ExecutionAborted(err.0));
                }
            }
        }
        self.execute_from(run, entry, initial_state, None, 0).await
    }

    /// Rehydrates the frozen state for `thread_id` from `store`, merges in
    /// `signal.field_overrides`, and continues execution from the
    /// checkpoint's `next_node`.
    ///
    /// If the checkpoint nests a child's (an interrupt inside a subgraph — see
    /// [`crate::graph::StateGraph::add_subgraph`]), the overrides go to the
    /// innermost graph's state instead, which is the one a human reviewed, and
    /// execution re-enters the child where it froze. Pass [`LevelSignals`]
    /// instead of a bare signal to send different overrides to outer levels too.
    ///
    /// A `Failed` or `Cancelled` checkpoint can be resumed as well: execution
    /// re-enters the node that failed or was stopped, with the last committed
    /// state, once per checkpoint (stopping again writes a new one). For a
    /// subgraph node it re-enters the child at the node that failed or was
    /// stopped there, so what a human answered before is not asked again.
    pub async fn resume(
        &self,
        thread_id: impl Into<String>,
        signal: impl Into<LevelSignals>,
        store: Arc<dyn CheckpointStore>,
    ) -> GraphExecutionResult<S> {
        self.resume_with(thread_id, signal, store, RunConfig::default())
            .await
    }

    /// [`Self::resume`] with run-level inputs, as [`Self::run_with`].
    pub async fn resume_with(
        &self,
        thread_id: impl Into<String>,
        signal: impl Into<LevelSignals>,
        store: Arc<dyn CheckpointStore>,
        config: RunConfig,
    ) -> GraphExecutionResult<S> {
        let thread_id = thread_id.into();
        let signals = signal.into();
        if let Err(message) = config.validate().and_then(|()| signals.validate()) {
            return refused(message);
        }
        // Waits for the run's scheduler's slot first (design/40-Run-Scheduler.md).
        let mut config = config;
        let _slot = match config.admit(None, None).await {
            Ok(slot) => slot,
            Err(refused) => return GraphExecutionResult::Failed(refused.into()),
        };
        let root = RootScope::start(&thread_id, config, RunMode::Resume, self.durable());
        let run = RunRefs {
            thread_id: &thread_id,
            store: &store,
            stream: None,
            level: &root.level,
        };
        observed(&root, &store, self.resume_from_store(run, signals)).await
    }

    /// Like [`Self::resume`], but drives the resumed run on a tokio task and
    /// streams its [`StreamChunk`]s to the returned receiver, the way
    /// [`Self::run_streaming`] does for a fresh run — so a streaming node the
    /// run reaches *after* the interrupt boundary works.
    ///
    /// The stream starts at the node the run resumes into (`NodeStart` for
    /// it) and says nothing about the nodes that ran before the freeze. When
    /// the checkpoint nests a child's, the parent's subgraph node emits a new
    /// `NodeStart` (its first one, from the run that froze, never got a
    /// `NodeEnd`) and the child's chunks follow, named
    /// `"<subgraph>/<child node>"`. A checkpoint that can't be resumed (none
    /// for the thread, already resumed, unsupported version) ends the stream
    /// with no chunks and the handle yields `Failed`, exactly as
    /// [`Self::resume`] would return it. Must be called inside a tokio runtime.
    pub fn resume_streaming(
        self: Arc<Self>,
        thread_id: impl Into<String>,
        signal: impl Into<LevelSignals>,
        store: Arc<dyn CheckpointStore>,
    ) -> StreamingRun<S> {
        self.resume_streaming_with(thread_id, signal, store, RunConfig::default())
    }

    /// [`Self::resume_streaming`] with run-level inputs, as [`Self::run_with`].
    pub fn resume_streaming_with(
        self: Arc<Self>,
        thread_id: impl Into<String>,
        signal: impl Into<LevelSignals>,
        store: Arc<dyn CheckpointStore>,
        config: RunConfig,
    ) -> StreamingRun<S> {
        let thread_id = thread_id.into();
        let signals = signal.into();
        let (ctx, receiver) = StreamContext::channel(thread_id.clone());
        // The task carries the caller's current span, so the run's span is its child.
        let handle = tokio::spawn(
            async move {
                let _close = CloseOnDrop(ctx.clone());
                if let Err(message) = config.validate().and_then(|()| signals.validate()) {
                    return refused(message);
                }
                // Waits for the run's scheduler's slot first (design/40-Run-Scheduler.md).
                let mut config = config;
                let _slot = match config.admit(None, None).await {
                    Ok(slot) => slot,
                    Err(refused) => return GraphExecutionResult::Failed(refused.into()),
                };
                let root = RootScope::start(&thread_id, config, RunMode::Resume, self.durable());
                let run = RunRefs {
                    thread_id: &thread_id,
                    store: &store,
                    stream: Some(&ctx),
                    level: &root.level,
                };
                observed(
                    &root,
                    &store,
                    Box::pin(self.resume_from_store(run, signals)),
                )
                .await
            }
            .in_current_span(),
        );
        (handle, receiver)
    }

    /// A resume of the outermost graph: loads its checkpoint, settles the
    /// run's metadata (the stored caller keys with the resume's laid over
    /// them), and starts the run.
    async fn resume_from_store(
        &self,
        run: RunRefs<'_>,
        signals: LevelSignals,
    ) -> GraphExecutionResult<S> {
        // With its sequence, so the resume stamp can't land after another
        // resume's, or an end (design/28-Ending-A-Thread.md, "Races").
        let loaded = load_checkpoint(run.store, run.thread_id).await;
        self.resume_loaded(run, loaded, signals).await
    }

    /// [`Self::resume_from_store`] for a latest checkpoint already loaded,
    /// with its sequence: what a wake does after deciding the thread is due
    /// (`design/29-Event-Waits-And-Timers.md`).
    async fn resume_loaded(
        &self,
        run: RunRefs<'_>,
        loaded: Result<Option<LatestCheckpoint>, CheckpointStoreError>,
        signals: LevelSignals,
    ) -> GraphExecutionResult<S> {
        let thread_id = run.thread_id;
        let stored = loaded
            .as_ref()
            .ok()
            .and_then(Option::as_ref)
            .map(|latest| &latest.checkpoint);
        // A wake through a resume is decided at this load, the one the stamp
        // is conditional on (design/42-Waking-Through-A-Resume.md).
        let signals = match stored {
            Some(checkpoint) => signals.when_due(checkpoint),
            None => signals,
        };
        let telemetry = &run.level.run.telemetry;
        telemetry.adopt_stored(stored.and_then(|checkpoint| checkpoint.metadata.as_ref()));
        telemetry.run_started(
            &stored
                .and_then(|checkpoint| self.starts_at(checkpoint, &signals))
                .unwrap_or_default(),
            self.identity(),
        );
        let latest = match loaded {
            Ok(Some(latest)) => latest,
            Ok(None) => {
                return GraphExecutionResult::Failed(StrataError::NodeNotFound(format!(
                    "no checkpoint found for thread '{thread_id}'"
                )));
            }
            Err(err) => return GraphExecutionResult::Failed(StrataError::ExecutionAborted(err.0)),
        };

        self.resume_checkpoint_at(
            thread_id,
            latest.checkpoint,
            latest.sequence,
            signals,
            run.store,
            run.stream,
            run.level,
            false,
        )
        .await
    }

    /// [`Self::resume`] for a checkpoint already in hand: what a subgraph node
    /// calls to resume its child from the checkpoint nested in the parent's.
    /// `stream` is the child's scoped context, if the run it is inside is
    /// streaming, and `level` the child's level of that run.
    pub(crate) fn resume_checkpoint<'a>(
        &'a self,
        thread_id: &'a str,
        checkpoint: Checkpoint,
        signals: LevelSignals,
        store: &'a Arc<dyn CheckpointStore>,
        stream: Option<&'a StreamContext>,
        level: &'a LevelScope,
    ) -> BoxFuture<'a, GraphExecutionResult<S>> {
        self.resume_checkpoint_at(
            thread_id, checkpoint, None, signals, store, stream, level, false,
        )
    }

    /// Resumes `checkpoint`. At the outermost level, `sequence` is its
    /// sequence in the thread's log (`None` when the store can't tell), and
    /// the resume stamp is only written if nothing was appended to the thread
    /// since.
    ///
    /// With `recovering` it continues a `running` thread whose run died
    /// instead (`design/33-Durable-Execution.md`): `signals` answer nothing,
    /// and only a `running` checkpoint is taken. A durable run's claim, for a
    /// resume or a recovery, is a `running` checkpoint ([`Self::claim`]) in
    /// place of the stamp.
    #[allow(clippy::too_many_arguments)]
    fn resume_checkpoint_at<'a>(
        &'a self,
        thread_id: &'a str,
        checkpoint: Checkpoint,
        sequence: Option<i64>,
        signals: LevelSignals,
        store: &'a Arc<dyn CheckpointStore>,
        stream: Option<&'a StreamContext>,
        level: &'a LevelScope,
        recovering: bool,
    ) -> BoxFuture<'a, GraphExecutionResult<S>> {
        Box::pin(async move {
            if !schema::is_supported_checkpoint_version(&checkpoint.schema_version) {
                return GraphExecutionResult::Failed(StrataError::ExecutionAborted(format!(
                    "checkpoint for thread '{thread_id}' has unsupported schema_version '{}'",
                    checkpoint.schema_version
                )));
            }

            // A running thread isn't waiting for anyone: its run is going on,
            // or died and is recovered. And only a running thread is
            // recovered. A nested level is whatever its outermost level is.
            if level.is_root() && recovering != (checkpoint.status == CheckpointStatus::Running) {
                return GraphExecutionResult::Failed(StrataError::ExecutionAborted(
                    if recovering {
                        format!(
                            "thread '{thread_id}' is not running: its latest checkpoint is {}",
                            ThreadState::of(&checkpoint).as_str()
                        )
                    } else {
                        format!(
                            "thread '{thread_id}' is running: only a waiting, failed or cancelled thread can be resumed"
                        )
                    },
                ));
            }

            if checkpoint.status == CheckpointStatus::Ended {
                return GraphExecutionResult::Failed(StrataError::ExecutionAborted(format!(
                    "thread '{thread_id}' was ended: an ended thread can't be resumed"
                )));
            }

            if checkpoint.status == CheckpointStatus::Completed {
                return GraphExecutionResult::Failed(StrataError::ExecutionAborted(format!(
                    "checkpoint for thread '{thread_id}' is not resumable (status: {:?}): only an interrupted, failed or cancelled checkpoint can be",
                    checkpoint.status
                )));
            }
            if checkpoint.resumed_at.is_some() {
                return GraphExecutionResult::Failed(StrataError::ExecutionAborted(format!(
                    "checkpoint for thread '{thread_id}' was already resumed"
                )));
            }

            // A wake the thread wasn't due for, decided at the load
            // (design/42-Waking-Through-A-Resume.md).
            if signals.due.is_some() {
                return GraphExecutionResult::Failed(StrataError::ExecutionAborted(
                    LevelSignals::not_due(thread_id, &checkpoint),
                ));
            }

            // Another graph's thread is refused, and another version's
            // migrated, at every level, before anything else is checked
            // against it (design/31-Graph-Identity.md).
            let checkpoint = match self.adopt(&checkpoint, thread_id, &[]) {
                Ok(adopted) => adopted.unwrap_or(checkpoint),
                Err(problem) => {
                    return GraphExecutionResult::Failed(StrataError::ExecutionAborted(problem));
                }
            };

            if let Err(message) = signals.check(&checkpoint) {
                return GraphExecutionResult::Failed(StrataError::ExecutionAborted(format!(
                    "cannot resume thread '{thread_id}': {message}"
                )));
            }

            // Everything that can refuse the resume is checked before the
            // checkpoint is stamped, at every level the signals reach, so a
            // resume that can't start leaves the thread resumable
            // (design/21-Typed-Human-Signals.md, "Preflight").
            let check = self.check_resume(&checkpoint, &signals, thread_id, &[]);
            if let Some(problem) = check.problem {
                return GraphExecutionResult::Failed(StrataError::ExecutionAborted(problem));
            }

            // A timeout with nowhere to go ends the thread instead, where it
            // stands: `wake_due` decides that before resuming, and a resume
            // given `LevelSignals::due` here
            // (design/42-Waking-Through-A-Resume.md).
            if check.ends_on_timeout {
                if level.is_root() {
                    return end_on_timeout(thread_id, checkpoint, sequence, store, level).await;
                }
                return GraphExecutionResult::Failed(StrataError::ExecutionAborted(format!(
                    "cannot resume thread '{thread_id}': its wait has no timeout route"
                )));
            }

            // The outermost resume fixes the time once, so every level's
            // decision, and every retry's, has the same `decided_at`.
            let now = now_rfc3339();
            let signals = match signals.decided_at {
                Some(_) => signals,
                None => signals.stamped(now.clone()),
            };
            // A durable run claims the thread with the checkpoint it continues
            // from, so what the resume decided is stored in the same step that
            // says it began (design/33-Durable-Execution.md). Any other run
            // stamps the checkpoint it resumes.
            let recoveries = recoveries_of(&checkpoint) + 1;
            let claim = if level.is_root() && level.run.durable {
                let mut claim = self.claim(&checkpoint, &signals, &level.run, &now);
                if recovering {
                    claim
                        .metadata
                        .get_or_insert_with(HashMap::new)
                        .insert(RECOVERIES_KEY.to_string(), recoveries.to_string());
                }
                claim
            } else {
                let mut stamped = checkpoint.clone();
                stamped.resumed_at = Some(now);
                stamped
            };
            let continues = continues_at(&claim);
            let (from, waiting_at) = (checkpoint.status, checkpoint.waiting_at());

            // Only a level waiting at its own boundary or wait is answered by
            // this resume: it may take the boundary's reject route or the
            // wait's timeout route, and a human's answer is recorded as its
            // decision. Every other level, and a level answered by an event, a
            // timer or a timeout, keeps the decision its checkpoint carries
            // (design/29-Event-Waits-And-Timers.md).
            let answers = answers_boundary(&checkpoint);
            let start = match (answers, &check.route) {
                (true, Some(route)) => route.clone(),
                _ => checkpoint.next_node.clone().unwrap_or_default(),
            };
            let decision = if answers && signals.wake == WakeReason::Human {
                Some(signals.innermost.decide(
                    checkpoint.suspended_node.as_deref().unwrap_or_default(),
                    signals.decided_at.as_deref().unwrap_or_default(),
                ))
            } else {
                checkpoint.decision.clone()
            };

            // Checked in preflight, so it reads.
            let hops = stored_hops(&checkpoint).unwrap_or_default();
            let mut state_value = checkpoint.frozen_state;
            // With a child nested inside, this graph's frozen state is just the
            // state its subgraph node started with. The human reviewed the
            // child's, so the innermost signal travels down to it; a signal
            // addressed to this level applies here, and the rest go on down
            // with the child.
            let nested = match checkpoint.nested {
                Some(child) => {
                    apply_overrides(&mut state_value, signals.own());
                    Some(Box::new(NestedResume {
                        checkpoint: *child,
                        signals: signals.descend(&start),
                    }))
                }
                None => {
                    apply_overrides(&mut state_value, Some(&signals.innermost));
                    None
                }
            };

            let state: S = match serde_json::from_value(state_value) {
                Ok(state) => state,
                Err(err) => {
                    return GraphExecutionResult::Failed(StrataError::ExecutionAborted(format!(
                        "failed to rehydrate state for thread '{thread_id}': {err}"
                    )));
                }
            };

            if level.is_root() {
                // Nothing can refuse the resume any more, so it takes the
                // thread now, and a refused resume never took it from anyone
                // (design/32-Thread-Leases.md).
                if let Some(held) = acquire_lease(thread_id, store, level).await {
                    return held;
                }
                // Whoever holds a running thread is running it.
                if recovering && let Some(live) = held_by_another(thread_id, store, level).await {
                    return live;
                }
                // Conditional, so of two callers that loaded the same
                // checkpoint only one resumes it, and a thread ended meanwhile
                // stays ended (design/28-Ending-A-Thread.md, "Races").
                match save_checkpoint_if_latest(store, claim, sequence, level).await {
                    Ok(LeasedSave::Saved) => {}
                    Ok(LeasedSave::NotLatest) => {
                        level.run.superseded.store(true, Ordering::Relaxed);
                        return GraphExecutionResult::Failed(StrataError::ExecutionAborted(
                            if recovering {
                                format!(
                                    "thread '{thread_id}' changed before it could be recovered: its run went on, or another recovery got there first"
                                )
                            } else {
                                format!(
                                    "checkpoint for thread '{thread_id}' changed before it could be resumed: another resume or end got there first"
                                )
                            },
                        ));
                    }
                    Ok(LeasedSave::LeaseLost) => {
                        return lease_lost_at(thread_id, &start, state, waiting_at);
                    }
                    Err(err) => {
                        return GraphExecutionResult::Failed(StrataError::ExecutionAborted(err.0));
                    }
                }
                if recovering {
                    level.run.telemetry.recovered(continues, recoveries);
                } else {
                    level
                        .run
                        .telemetry
                        .resumed(from, waiting_at, &signals, check.route.as_deref());
                }
            } else if let Err(err) = save_checkpoint(store, claim, level).await {
                return GraphExecutionResult::Failed(StrataError::ExecutionAborted(err.0));
            }

            let level = level.with_decision(decision);
            let level = if recovering {
                level.recovering()
            } else {
                level
            };
            let run = RunRefs {
                thread_id,
                store,
                stream,
                level: &level,
            };
            self.execute_from(run, &start, state, nested, hops).await
        })
    }

    /// A level of the `running` checkpoint a durable resume or a recovery
    /// claims its thread with (`design/33-Durable-Execution.md`, "A resume
    /// under EveryNode"), and through each subgraph node's child every level
    /// nested below: `checkpoint` as the run continues it. The level's
    /// overrides are applied to its state; the innermost level takes the route
    /// and records the decision a resume answers its boundary with; nothing
    /// waits any more. For a recovery, whose `signals` answer nothing, it is
    /// the checkpoint again, written by this run. The resume check has passed,
    /// so nothing here can fail.
    pub(crate) fn claim(
        &self,
        checkpoint: &Checkpoint,
        signals: &LevelSignals,
        run: &RunScope,
        now: &str,
    ) -> Checkpoint {
        let start = self
            .route_for(checkpoint, signals)
            .or_else(|| checkpoint.next_node.clone())
            .unwrap_or_default();
        let decision = if answers_boundary(checkpoint) && signals.wake == WakeReason::Human {
            Some(signals.innermost.decide(
                checkpoint.suspended_node.as_deref().unwrap_or_default(),
                signals.decided_at.as_deref().unwrap_or(now),
            ))
        } else {
            checkpoint.decision.clone()
        };

        let mut frozen_state = checkpoint.frozen_state.clone();
        apply_overrides(
            &mut frozen_state,
            match checkpoint.nested {
                Some(_) => signals.own(),
                None => Some(&signals.innermost),
            },
        );

        // Checked in preflight, so it reads.
        let hops = stored_hops(checkpoint).unwrap_or_default();
        let nested = checkpoint.nested.as_deref().and_then(|child| {
            self.nodes
                .get(&start)
                .and_then(|entry| entry.claim.as_ref())
                .map(|claim_child| Box::new(claim_child(child, &signals.descend(&start), run, now)))
        });

        versioned(Checkpoint {
            schema_version: String::new(),
            thread_id: checkpoint.thread_id.clone(),
            status: CheckpointStatus::Running,
            suspended_node: checkpoint.suspended_node.clone(),
            next_node: Some(start),
            frozen_state,
            metadata: run.telemetry.checkpoint_metadata(Some(HashMap::from([(
                HOPS_KEY.to_string(),
                hops.to_string(),
            )]))),
            created_at: now.to_string(),
            resumed_at: None,
            nested,
            decision,
            wait: None,
            graph: self.identity.clone(),
        })
    }

    /// The node a resume of `checkpoint` with `signals` starts at, for its
    /// `RunStarted`: the route it takes, or its next node, or for a timeout
    /// that ends the thread where it stands, the node it waits at.
    fn starts_at(&self, checkpoint: &Checkpoint, signals: &LevelSignals) -> Option<String> {
        if let Some(route) = self.route_for(checkpoint, signals) {
            return Some(route);
        }
        if signals.wake == WakeReason::Timeout && answers_boundary(checkpoint) {
            checkpoint.suspended_node.clone()
        } else {
            checkpoint.next_node.clone()
        }
    }

    /// The route a resume of `checkpoint` with `signals` continues at instead
    /// of its next node, if it takes one at this level: the checkpoint answers
    /// one of this graph's boundaries or waits, and either a human rejects at a
    /// boundary with a reject route (`design/21-Typed-Human-Signals.md`,
    /// "Exactly when the reject route applies"), or the wait timed out and has
    /// a timeout route (`design/29-Event-Waits-And-Timers.md`).
    fn route_for(&self, checkpoint: &Checkpoint, signals: &LevelSignals) -> Option<String> {
        if !answers_boundary(checkpoint) {
            return None;
        }
        let node = checkpoint.suspended_node.as_deref()?;
        match signals.wake {
            WakeReason::Human if !signals.innermost.is_approved => {
                self.reject_routes.get(node).cloned()
            }
            WakeReason::Timeout => self.timeout_routes.get(node).cloned(),
            _ => None,
        }
    }

    /// Why `signals`' wake can't answer `checkpoint`, the innermost level, at
    /// `path` (`design/29-Event-Waits-And-Timers.md`, "Answering a wait");
    /// `None` if it can. A human resumes anything but an event wait or a timer;
    /// a signal only an event wait for its type; a timer wake only a timer; a
    /// timeout only a wait with a timeout. The wait must be one of this
    /// graph's, which catches a thread of another graph.
    fn wake_problem(
        &self,
        checkpoint: &Checkpoint,
        signals: &LevelSignals,
        thread_id: &str,
        path: &[String],
    ) -> Option<String> {
        let waiting = checkpoint.status == CheckpointStatus::Interrupted;
        let wait = if waiting {
            checkpoint.wait.as_ref()
        } else {
            None
        };
        let node = checkpoint.suspended_node.as_deref().unwrap_or_default();
        let mut levels = path.to_vec();
        levels.push(node.to_string());
        let at = format!("[{}]", levels.join("/"));
        let spec = self.waits.get(node);
        let describe = |wait: Option<&CheckpointWait>| {
            wait.map_or_else(|| "a human".to_string(), CheckpointWait::describe)
        };
        match signals.wake {
            WakeReason::Human => match wait.map(|wait| wait.kind) {
                Some(WaitKind::Event) => Some(format!(
                    "thread '{thread_id}' is waiting for event '{}' at {at}: only a signal of that event answers it",
                    wait.and_then(|wait| wait.event_type.as_deref())
                        .unwrap_or_default()
                )),
                Some(WaitKind::Timer) => Some(format!(
                    "thread '{thread_id}' is waiting for a timer at {at} until {}: it wakes when due",
                    wait.and_then(|wait| wait.wake_at.as_deref())
                        .unwrap_or_default()
                )),
                _ => None,
            },
            WakeReason::Event => {
                let event = signals.event_type.as_deref().unwrap_or_default();
                if !waiting {
                    return Some(format!(
                        "thread '{thread_id}' is not waiting for event '{event}': its latest checkpoint is {}",
                        observability::status_tag(checkpoint.status)
                    ));
                }
                let answered = wait.is_some_and(|wait| {
                    wait.kind == WaitKind::Event && wait.event_type.as_deref() == Some(event)
                });
                if !answered {
                    return Some(format!(
                        "thread '{thread_id}' is not waiting for event '{event}': it is waiting for {} at {at}",
                        describe(wait)
                    ));
                }
                let ours = spec.is_some_and(|spec| {
                    spec.kind == WaitKind::Event && spec.event_type.as_deref() == Some(event)
                });
                (!ours).then(|| {
                    format!(
                        "thread '{thread_id}' waits at {at}, which is not a wait for event '{event}' in this graph"
                    )
                })
            }
            WakeReason::Timer => {
                if !wait.is_some_and(|wait| wait.kind == WaitKind::Timer) {
                    return Some(format!(
                        "thread '{thread_id}' is not waiting for a timer at {at}"
                    ));
                }
                (!spec.is_some_and(|spec| spec.kind == WaitKind::Timer)).then(|| {
                    format!(
                        "thread '{thread_id}' waits at {at}, which is not a timer in this graph"
                    )
                })
            }
            WakeReason::Timeout => {
                let Some(wait) =
                    wait.filter(|wait| wait.wake_at.is_some() && wait.kind != WaitKind::Timer)
                else {
                    return Some(format!(
                        "thread '{thread_id}' is not waiting with a timeout at {at}"
                    ));
                };
                let timed = match wait.kind {
                    WaitKind::Human => {
                        self.boundary_timeouts.contains_key(node)
                            && self.interrupt_boundaries.contains(node)
                    }
                    _ => spec
                        .is_some_and(|spec| spec.kind == WaitKind::Event && spec.timeout.is_some()),
                };
                (!timed).then(|| {
                    format!(
                        "thread '{thread_id}' waits at {at}, which has no timeout in this graph"
                    )
                })
            }
        }
    }

    /// Checks, without changing anything, that a resume of `checkpoint` with
    /// `signals` can start at this level and every level nested below it:
    /// there is a node to continue at, no override is set twice, the overrides
    /// fit the level's state type, the hop count (`strata.hops`) reads, and a
    /// nested child sits under a subgraph node whose own child passes the same
    /// check. `thread_id` is the
    /// outermost thread, and `path` this level (`[]` at the top), for
    /// messages. Reports the innermost level's reject route, if it takes one.
    pub(crate) fn check_resume(
        &self,
        checkpoint: &Checkpoint,
        signals: &LevelSignals,
        thread_id: &str,
        path: &[String],
    ) -> ResumeCheck {
        let refuse = |problem: String| ResumeCheck {
            problem: Some(problem),
            route: None,
            ends_on_timeout: false,
        };
        let at = if path.is_empty() {
            String::new()
        } else {
            format!(" at level [{}]", path.join("/"))
        };
        if !path.is_empty() && !schema::is_supported_checkpoint_version(&checkpoint.schema_version)
        {
            return refuse(format!(
                "checkpoint for thread '{thread_id}'{at} has unsupported schema_version '{}'",
                checkpoint.schema_version
            ));
        }

        // Resetting the count of a hand-edited log would defeat the breaker,
        // so refuse it instead.
        if let Err(invalid) = stored_hops(checkpoint) {
            return refuse(format!(
                "checkpoint for thread '{thread_id}'{at} has an invalid '{HOPS_KEY}' value '{invalid}'"
            ));
        }

        if checkpoint.nested.is_none()
            && let Some(problem) = self.wake_problem(checkpoint, signals, thread_id, path)
        {
            return refuse(problem);
        }

        let route = self.route_for(checkpoint, signals);
        if checkpoint.nested.is_none() && signals.wake == WakeReason::Timeout && route.is_none() {
            return ResumeCheck {
                problem: None,
                route: None,
                ends_on_timeout: true,
            };
        }
        let Some(start) = route.clone().or_else(|| checkpoint.next_node.clone()) else {
            return refuse(format!(
                "checkpoint for thread '{thread_id}'{at} has no next_node to resume from"
            ));
        };

        let applied = match checkpoint.nested {
            Some(_) => signals.own(),
            None => Some(&signals.innermost),
        };
        if let Some(Err(twice)) = applied.map(HumanSignal::effective_overrides) {
            return refuse(format!("cannot resume thread '{thread_id}': {twice}"));
        }

        let mut state_value = checkpoint.frozen_state.clone();
        apply_overrides(&mut state_value, applied);
        if let Err(err) = serde_json::from_value::<S>(state_value) {
            return refuse(format!(
                "failed to rehydrate state for thread '{thread_id}'{at}: {err}"
            ));
        }

        let Some(child) = checkpoint.nested.as_deref() else {
            return ResumeCheck {
                problem: None,
                route,
                ends_on_timeout: false,
            };
        };
        match self
            .nodes
            .get(&start)
            .and_then(|entry| entry.check_resume.as_ref())
        {
            Some(check_child) => {
                let mut child_path = path.to_vec();
                child_path.push(start.clone());
                check_child(child, &signals.descend(&start), thread_id, &child_path)
            }
            None => refuse(format!(
                "checkpoint nests a child graph under '{start}', which is not a subgraph node"
            )),
        }
    }

    /// Like [`Self::run`], but drives the graph on a tokio task and streams
    /// [`StreamChunk`]s to the returned receiver as nodes run, instead of only
    /// handing back a final state once the whole graph finishes. This is the
    /// integration point STRATA-SPEC-001 describes, via
    /// [`StreamContext::for_node`] (one context per node, sharing a single
    /// channel).
    ///
    /// Every node — streaming or plain — gets automatic
    /// [`StreamChunkType::NodeStart`]/[`StreamChunkType::NodeEnd`] chunks
    /// around its execution; streaming nodes (and an async node, through
    /// [`crate::context::NodeContext::stream`]) can also emit their own
    /// [`StreamChunkType::Token`] chunks from inside the handler, as can the
    /// streaming branches of a parallel group (see
    /// [`crate::parallel::ParallelBranches::streaming_branch`]), attributed
    /// to `"<group>/<branch>"`. A fallible node about to be retried emits a
    /// [`StreamChunkType::Retry`] first.
    ///
    /// A subgraph node (see [`crate::graph::StateGraph::add_subgraph`]) streams
    /// its child's chunks too — all of them, lifecycle chunks included — named
    /// `"<subgraph>/<child node>"`, nested to any depth. They sit between the
    /// subgraph node's own `NodeStart` and `NodeEnd`.
    ///
    /// The stream ends when the run ends, whatever the outcome; a node stopped
    /// by cancellation gets no `NodeEnd`, and the handle says why the stream
    /// ended. Must be called inside a tokio runtime; `self` and `store` are
    /// `Arc`-wrapped because the task outlives this call.
    pub fn run_streaming(
        self: Arc<Self>,
        thread_id: impl Into<String>,
        initial_state: S,
        store: Arc<dyn CheckpointStore>,
    ) -> StreamingRun<S> {
        self.run_streaming_with(thread_id, initial_state, store, RunConfig::default())
    }

    /// [`Self::run_streaming`] with run-level inputs, as [`Self::run_with`].
    pub fn run_streaming_with(
        self: Arc<Self>,
        thread_id: impl Into<String>,
        initial_state: S,
        store: Arc<dyn CheckpointStore>,
        config: RunConfig,
    ) -> StreamingRun<S> {
        let thread_id = thread_id.into();
        let entry = self.definition.entry_point.clone();
        let (ctx, receiver) = StreamContext::channel(entry.clone());
        // The task carries the caller's current span, so the run's span is its child.
        let handle = tokio::spawn(
            async move {
                let _close = CloseOnDrop(ctx.clone());
                if let Err(message) = config.validate() {
                    return refused(message);
                }
                // Waits for the run's scheduler's slot first (design/40-Run-Scheduler.md).
                let mut config = config;
                let _slot = match config.admit(None, None).await {
                    Ok(slot) => slot,
                    Err(refused) => return GraphExecutionResult::Failed(refused.into()),
                };
                let root = RootScope::start(&thread_id, config, RunMode::Run, self.durable());
                let run = RunRefs {
                    thread_id: &thread_id,
                    store: &store,
                    stream: Some(&ctx),
                    level: &root.level,
                };
                observed(&root, &store, Box::pin(self.start(run, initial_state))).await
            }
            .in_current_span(),
        );
        (handle, receiver)
    }

    /// The hop limit in effect at `level` of a run of this graph
    /// (`design/24-Loop-Circuit-Breaker.md`): the run's own, at the outermost
    /// graph, or the compiled one.
    fn hop_limit_at(&self, level: &LevelScope) -> Option<u32> {
        match level.run.max_node_hops {
            Some(replaced) if level.is_root() => Some(replaced),
            _ => self.max_node_hops,
        }
    }

    /// The executor loop, from `current`: what a run, a resume, and a
    /// subgraph's child (with its own `run`) all end up in. `hops` is how many
    /// nodes the level already started in earlier runs of the thread, from the
    /// checkpoint resumed (`design/24-Loop-Circuit-Breaker.md`).
    pub(crate) fn execute_from<'a>(
        &'a self,
        run: RunRefs<'a>,
        current: &'a str,
        state: S,
        resume: Option<Box<NestedResume>>,
        hops: u32,
    ) -> BoxFuture<'a, GraphExecutionResult<S>> {
        Box::pin(async move {
            let (mut state, mut resume, mut hops) = (state, resume, hops);
            let mut current = self.name_of(current);
            // Only the first node is resumed into, and only a subgraph node can be.
            if resume.is_some() && !self.nodes.get(&*current).is_some_and(|e| e.is_subgraph) {
                return GraphExecutionResult::Failed(StrataError::ExecutionAborted(format!(
                    "checkpoint nests a child graph under '{current}', which is not a subgraph node"
                )));
            }

            let limit = self.hop_limit_at(run.level);
            // A level that checkpoints every node records its hops whether or
            // not it has a limit: a node's idempotency key is made of them,
            // and a recovery has to count on from them
            // (design/33-Durable-Execution.md).
            let checkpoints = run.level.checkpoints();
            let counted = limit.is_some() || checkpoints;
            // Whether the node about to start may have run before its process
            // died: the first node a recovery starts at this level, and its
            // fallback, until a node commits. A level that isn't checkpointed
            // (a map's item) is run again whole, so every node of it may have.
            let mut recovering = run.level.recovering;
            // Why the node a fallback came from failed, for the fallback node
            // only (design/25-Retry-Backoff.md).
            let mut last_failure: Option<NodeFailure> = None;
            loop {
                let node_resume = resume.take();
                // Everything the loop needs to know about the node, in one
                // lookup (design/38-Rust-Executor-Overhead.md).
                let entry = self.nodes.get(&*current);

                // What a checkpoint about `current` records: the hops made
                // before it, which a resume re-entering it starts from, so it
                // gets the same hop number again.
                let mut node_level =
                    run.level
                        .at_hop(counted.then_some(hops), hops + 1, recovering);
                if let Some(failure) = last_failure.take() {
                    node_level = node_level.with_last_failure(failure);
                }
                let run = RunRefs {
                    level: &node_level,
                    ..run
                };

                // Not for a subgraph being resumed into: its child stops at its
                // own innermost node, after the human's signal has been applied
                // there, so nothing answered is lost.
                // (This exit and the loop's other uncommon ones each run in a
                // future of their own, so the loop's stays small:
                // design/38-Rust-Executor-Overhead.md.)
                if node_resume.is_none() && run.level.token.is_cancelled() {
                    return Box::pin(self.cancel_at(run, &current, state, None)).await;
                }

                // The breaker trips before the node that would be one hop too
                // many starts, so nothing of it runs. A subgraph being resumed
                // into keeps the child it was resuming, so nothing answered is
                // lost.
                if let Some(limit) = limit
                    && hops >= limit
                {
                    let child = node_resume.map(|resume| resume.checkpoint);
                    return Box::pin(self.hop_limit_at_node(run, &current, state, limit, child))
                        .await;
                }
                hops += 1;

                let node_ctx = run.stream.map(|ctx| ctx.for_node(current.clone()));
                if let Some(ctx) = &node_ctx {
                    let start = ctx.emit_diagnostic(StreamChunkType::NodeStart, &current);
                    if node_resume.is_some() {
                        let _ = start.await;
                    } else {
                        tokio::select! {
                            biased;
                            _ = start => {}
                            // Cancelled while a full channel held the node back: it
                            // never started.
                            _ = run.level.token.cancelled() => {
                                return Box::pin(self.cancel_at(run, &current, state, None)).await;
                            }
                        }
                    }
                }

                // Every NodeStarted gets exactly one NodeCompleted, however the
                // node ends (design/18-Run-Observability.md). A failure path
                // ends it itself, before its checkpoint.
                let scope = NodeScope::start(
                    &run.level.run.telemetry,
                    current.clone(),
                    run.level.path_arc(&current),
                );
                let executed = CatchUnwind::new(
                    self.execute_node(run, entry, &state, node_ctx.as_ref(), node_resume, &scope)
                        .instrument(scope.span.clone()),
                )
                .await;
                let outcome = match executed {
                    Ok(outcome) => outcome,
                    Err(panic) => {
                        scope.panicked(&*panic);
                        std::panic::resume_unwind(panic);
                    }
                };

                match outcome {
                    NodeOutcome::Committed(next_state) => {
                        // At the outermost level a node's path is its name,
                        // and its series is ready.
                        scope.committed(
                            entry
                                .and_then(|entry| entry.plan.committed.as_ref())
                                .filter(|_| run.level.is_root()),
                        );
                        state = next_state;
                        recovering &= !checkpoints;
                    }
                    NodeOutcome::Fallback(node, failure) => {
                        // The failed node never completed, so no NodeEnd and no
                        // interrupt boundary check: continue at the fallback with
                        // the pre-failure state.
                        RunTelemetry::transition(&scope.path, &run.level.path_of(&node));
                        current = self.name_of(&node);
                        last_failure = Some(failure);
                        continue;
                    }
                    NodeOutcome::Interrupted(child) => {
                        // The child froze, so this node never completed: no
                        // NodeEnd, and its own boundary (if it is one) only
                        // applies once the child has finished.
                        scope.completed(NodeResult::Interrupted, None, None, None);
                        return Box::pin(self.freeze_around_child(run, &current, state, *child))
                            .await;
                    }
                    NodeOutcome::Cancelled(child) => {
                        // The node never completed: no NodeEnd. `state` is still
                        // what it started with.
                        scope.completed(NodeResult::Cancelled, None, None, None);
                        let child = child.map(|child| *child);
                        return Box::pin(self.cancel_at(run, &current, state, child)).await;
                    }
                    NodeOutcome::Terminal(result) => {
                        // Refused before it ran; a node that failed has already
                        // ended its scope.
                        if let GraphExecutionResult::Failed(error) = &result {
                            let message = observability::error_message(error);
                            scope.completed(
                                NodeResult::Failed,
                                Some(&message),
                                Some("fatal"),
                                None,
                            );
                        }
                        return result;
                    }
                }

                emit(
                    node_ctx.as_ref(),
                    StreamChunkType::NodeEnd,
                    &current,
                    &run.level.token,
                )
                .await;

                // A node that committed is one the graph has.
                let Some(entry) = entry else {
                    return GraphExecutionResult::Failed(StrataError::NodeNotFound(
                        current.to_string(),
                    ));
                };

                // A wait node freezes like a boundary, with what it waits for
                // (design/29-Event-Waits-And-Timers.md).
                if entry.plan.freezes {
                    let next_node = match self.next_edge(entry, &state) {
                        Ok(next) => next.map(|edge| edge.to.to_string()),
                        Err(err) => return GraphExecutionResult::Failed(err),
                    };

                    let frozen_state = match serde_json::to_value(&state) {
                        Ok(value) => value,
                        Err(err) => {
                            return GraphExecutionResult::Failed(StrataError::ExecutionAborted(
                                format!("failed to freeze state at '{current}': {err}"),
                            ));
                        }
                    };

                    let checkpoint = versioned(Checkpoint {
                        schema_version: schema::SCHEMA_VERSION.to_string(),
                        thread_id: run.thread_id.to_string(),
                        status: CheckpointStatus::Interrupted,
                        suspended_node: Some(current.to_string()),
                        next_node: next_node.clone(),
                        frozen_state,
                        // A resume goes on to the next node, so the boundary
                        // node is among the hops made.
                        metadata: run
                            .level
                            .with_hops(counted.then_some(hops))
                            .checkpoint_metadata(None),
                        created_at: now_rfc3339(),
                        resumed_at: None,
                        nested: None,
                        decision: run.level.decision(),
                        wait: self.wait_after(&current, &state, chrono::Utc::now()),
                        graph: self.identity.clone(),
                    });

                    let wait = checkpoint.wait.clone();
                    match save_checkpoint(run.store, checkpoint, run.level).await {
                        Ok(true) => {}
                        Ok(false) => {
                            let stopped_at = vec![current.to_string()];
                            return lease_lost_at(run.thread_id, &current, state, stopped_at);
                        }
                        Err(err) => {
                            return GraphExecutionResult::Failed(StrataError::ExecutionAborted(
                                err.0,
                            ));
                        }
                    }

                    // FREEZE + FLUSH are done above. EVICT is implicit: nothing
                    // of this run is awaited once this returns — the only thing
                    // that survives is what the store persisted.
                    return GraphExecutionResult::Interrupted {
                        thread_id: run.thread_id.to_string(),
                        waiting_at: vec![current.to_string()],
                        suspended_node: current.to_string(),
                        next_node: next_node.unwrap_or_default(),
                        frozen_state: state,
                        wait,
                    };
                }

                let edge = match self.next_edge(entry, &state) {
                    Ok(Some(edge)) => edge,
                    Ok(None) => {
                        // A durable run says it finished, so its thread isn't
                        // left running, and a stale owner's completion is
                        // fenced like any other save
                        // (design/33-Durable-Execution.md). A child's
                        // completion is its subgraph node's commit, which its
                        // parent records.
                        if run.level.is_root() && run.level.run.durable {
                            let frozen_state = match serde_json::to_value(&state) {
                                Ok(value) => value,
                                Err(err) => {
                                    return GraphExecutionResult::Failed(
                                        StrataError::ExecutionAborted(format!(
                                            "failed to persist state after '{current}': {err}"
                                        )),
                                    );
                                }
                            };
                            let completed = versioned(Checkpoint {
                                schema_version: schema::SCHEMA_VERSION.to_string(),
                                thread_id: run.thread_id.to_string(),
                                status: CheckpointStatus::Completed,
                                suspended_node: Some(current.to_string()),
                                next_node: None,
                                frozen_state,
                                metadata: run.level.with_hops(Some(hops)).checkpoint_metadata(None),
                                created_at: now_rfc3339(),
                                resumed_at: None,
                                nested: None,
                                decision: run.level.decision(),
                                wait: None,
                                graph: self.identity.clone(),
                            });
                            match save_checkpoint(run.store, completed, run.level).await {
                                Ok(true) => {}
                                Ok(false) => {
                                    let stopped_at = vec![current.to_string()];
                                    return lease_lost_at(
                                        run.thread_id,
                                        &current,
                                        state,
                                        stopped_at,
                                    );
                                }
                                Err(err) => {
                                    return GraphExecutionResult::Failed(
                                        StrataError::ExecutionAborted(err.0),
                                    );
                                }
                            }
                        }
                        return GraphExecutionResult::Completed(state);
                    }
                    Err(err) => return GraphExecutionResult::Failed(err),
                };
                let next = edge.to.clone();

                // The node committed and an edge leads on: a durable level
                // says where it is now, before the next node starts
                // (design/33-Durable-Execution.md).
                if checkpoints {
                    let frozen_state = match serde_json::to_value(&state) {
                        Ok(value) => value,
                        Err(err) => {
                            return GraphExecutionResult::Failed(StrataError::ExecutionAborted(
                                format!("failed to persist state after '{current}': {err}"),
                            ));
                        }
                    };
                    let running = running_checkpoint(
                        run.thread_id,
                        Some(&current),
                        &next,
                        frozen_state,
                        &run.level.with_hops(Some(hops)),
                        None,
                        self.identity.clone(),
                    );
                    let saved = match &run.level.progress {
                        Some(up) => up(running).await.map(|()| true),
                        None => save_checkpoint(run.store, running, run.level).await,
                    };
                    match saved {
                        Ok(true) => {}
                        Ok(false) => {
                            let stopped_at = vec![next.to_string()];
                            return lease_lost_at(run.thread_id, &next, state, stopped_at);
                        }
                        Err(err) => {
                            return GraphExecutionResult::Failed(StrataError::ExecutionAborted(
                                err.0,
                            ));
                        }
                    }
                }

                if run.level.is_root() {
                    RunTelemetry::transition_keyed(&edge.transition);
                } else {
                    RunTelemetry::transition(
                        &run.level.path_of(&current),
                        &run.level.path_of(&next),
                    );
                }
                current = next;
            }
        })
    }

    /// Refuses a node this run can't execute, otherwise runs it under its
    /// recovery policy and timeout.
    async fn execute_node(
        &self,
        run: RunRefs<'_>,
        entry: Option<&NodeEntry<S>>,
        state: &S,
        node_ctx: Option<&StreamContext>,
        resume: Option<Box<NestedResume>>,
        scope: &NodeScope,
    ) -> NodeOutcome<S> {
        let current: &str = &scope.name;
        let Some(entry) = entry else {
            return NodeOutcome::Terminal(GraphExecutionResult::Failed(StrataError::NodeNotFound(
                current.to_string(),
            )));
        };

        if node_ctx.is_none() && entry.needs_stream {
            if entry.kind == NodeKind::Streaming {
                return NodeOutcome::Terminal(GraphExecutionResult::Failed(
                    StrataError::ExecutionAborted(format!(
                        "node '{current}' is a streaming node but this run has no StreamContext \u{2014} use run_streaming instead of run"
                    )),
                ));
            }
            // Not when resuming into a subgraph without a stream (a plain
            // `resume`): the child may have done its streaming before it
            // froze. If it reaches a streaming node after all, the child's own
            // run fails there, as a flat graph's would.
            if resume.is_none() {
                return NodeOutcome::Terminal(GraphExecutionResult::Failed(
                    StrataError::ExecutionAborted(format!(
                        "node '{current}' is a parallel group with a streaming branch, or a subgraph that streams, but this run has no StreamContext \u{2014} use run_streaming instead of run"
                    )),
                ));
            }
        }

        self.run_attempts(run, entry, state, node_ctx, resume, scope)
            .await
    }

    /// Runs a node under its [`RecoveryPolicy`] (default: no retries, no
    /// fallback) and its timeout, if it has one.
    ///
    /// Every attempt gets its own clone of the same committed `state`, so a
    /// failed attempt can't leak partial changes into the next attempt, the
    /// fallback node, or the `Failed` checkpoint. A `Transient` error (or a
    /// timeout, which is one) is retried up to `max_retries` more times; a
    /// `Fatal` one, or a `Transient` one out of retries, is terminal: route to
    /// the policy's fallback if it has one, otherwise persist a
    /// `CheckpointStatus::Failed` checkpoint and fail the run.
    ///
    /// A subgraph whose child freezes is not a failure, and neither is a
    /// cancelled run: both end the loop untouched by the policy. `resume` is
    /// what a subgraph node is resuming from, given to every attempt alike, and
    /// `scope` the node's telemetry, which also names it.
    async fn run_attempts(
        &self,
        run: RunRefs<'_>,
        entry: &NodeEntry<S>,
        state: &S,
        node_ctx: Option<&StreamContext>,
        resume: Option<Box<NestedResume>>,
        scope: &NodeScope,
    ) -> NodeOutcome<S> {
        let node: &str = &scope.name;
        let timeout = entry.plan.timeout;
        let level = run.level;

        // A subgraph's child hands up a running checkpoint each time one of
        // its nodes commits. This level nests it in its own, suspended at the
        // subgraph node with the state the node started with, and saves that,
        // or hands it up in turn (design/33-Durable-Execution.md, "Subgraphs,
        // groups and maps"). A lost lease cancels the run's token, which is
        // what stops the child.
        let reporting;
        let level = if entry.is_subgraph && level.checkpoints() {
            let started = match serde_json::to_value(state) {
                Ok(value) => value,
                Err(err) => {
                    return NodeOutcome::Terminal(GraphExecutionResult::Failed(
                        StrataError::ExecutionAborted(format!(
                            "failed to persist state before '{node}': {err}"
                        )),
                    ));
                }
            };
            let (around, store, thread_id, name, graph) = (
                level.clone(),
                run.store.clone(),
                run.thread_id.to_string(),
                node.to_string(),
                self.identity.clone(),
            );
            let sink: Progress = Arc::new(move |child| {
                let running = running_checkpoint(
                    &thread_id,
                    Some(&name),
                    &name,
                    started.clone(),
                    &around,
                    Some(child),
                    graph.clone(),
                );
                let (around, store) = (around.clone(), store.clone());
                Box::pin(async move {
                    match &around.progress {
                        Some(up) => up(running).await,
                        None => save_checkpoint(&store, running, &around).await.map(|_| ()),
                    }
                })
            });
            reporting = level.with_child_progress(sink);
            &reporting
        } else {
            level
        };

        let mut attempts = 0;
        loop {
            attempts += 1;
            if resume.is_none() && level.token.is_cancelled() {
                return NodeOutcome::Cancelled(None);
            }

            // The attempt's span is current while its handler runs, so the
            // handler's own spans nest under it (design/18-Run-Observability.md).
            let span = scope.start_attempt(attempts);
            // A token of the attempt's own only where something cancels it
            // apart from the level: the node's timeout, or a group, subgraph
            // or map, which cancel what they started. Any other node watches
            // the level's token, and its handler's token is made if it asks
            // (design/38-Rust-Executor-Overhead.md).
            let own = (timeout.is_some() || entry.composite).then(|| level.token.child_token());
            let inputs = AttemptInputs {
                context: level.context_for(
                    &scope.name,
                    &scope.path,
                    attempts,
                    node_ctx.cloned(),
                    own.clone(),
                    span.clone(),
                ),
                stream: node_ctx.cloned(),
                resume: resume.clone(),
                level: level.clone(),
            };
            let token: &CancellationToken = own.as_ref().unwrap_or(&level.token);
            let attempted = CatchUnwind::new(
                attempt(entry, state.clone(), inputs, token, timeout).instrument(span.clone()),
            )
            .await;
            let attempted = match attempted {
                Ok(attempted) => attempted,
                Err(panic) => {
                    observability::fail(&span, &panic_message(&*panic), Some("panic"));
                    std::panic::resume_unwind(panic);
                }
            };
            let (failure, error_type, timed_out) = match attempted {
                Attempt::Finished(Ok(Step::Done(next_state))) => {
                    return NodeOutcome::Committed(next_state);
                }
                Attempt::Finished(Ok(Step::Interrupted(child))) => {
                    return NodeOutcome::Interrupted(child);
                }
                Attempt::Finished(Ok(Step::Cancelled(child))) if level.token.is_cancelled() => {
                    return NodeOutcome::Cancelled(Some(child));
                }
                // Only this node's timeout cancels a child while the run goes
                // on. Keep where the child stopped, so a resume re-enters it there.
                Attempt::Finished(Ok(Step::Cancelled(child))) => (
                    AttemptError {
                        error: NodeError::Transient(timed_out(node, timeout)),
                        child: Some(child),
                    },
                    "timeout",
                    true,
                ),
                Attempt::Finished(Err(error)) => {
                    let error_type = observability::node_error_tag(&error.error);
                    (error, error_type, false)
                }
                Attempt::Stopped if level.token.is_cancelled() => {
                    return NodeOutcome::Cancelled(None);
                }
                Attempt::Stopped => (
                    NodeError::Transient(timed_out(node, timeout)).into(),
                    "timeout",
                    true,
                ),
            };
            observability::fail(&span, failure.error.message(), Some(error_type));
            drop(span);

            // A failure is the uncommon end of an attempt. What follows it (a
            // wait before the retry, or a checkpoint) runs in a future of its
            // own, so the one every node runs in stays small
            // (design/38-Rust-Executor-Overhead.md).
            let failed = FailedAttempt {
                failure,
                error_type,
                timed_out,
                attempts,
            };
            let settled =
                Box::pin(self.after_failure(run, entry, state, node_ctx, scope, level, failed));
            if let Some(outcome) = settled.await {
                return outcome;
            }
        }
    }

    /// What follows a failed attempt of a node, under its [`RecoveryPolicy`].
    /// `None`: the failure is transient and the policy has a retry left, and
    /// the policy's backoff has been waited out, so the node is attempted
    /// again. Otherwise how the node ends: at its fallback, or failed, with
    /// its `failed` checkpoint saved. `level` is the level the attempts ran
    /// at, which for a subgraph node reports its child's progress.
    #[allow(clippy::too_many_arguments)]
    async fn after_failure(
        &self,
        run: RunRefs<'_>,
        entry: &NodeEntry<S>,
        state: &S,
        node_ctx: Option<&StreamContext>,
        scope: &NodeScope,
        level: &LevelScope,
        failed: FailedAttempt,
    ) -> Option<NodeOutcome<S>> {
        let node: &str = &scope.name;
        let policy = &entry.plan.policy;
        let FailedAttempt {
            failure:
                AttemptError {
                    error,
                    child: failed_child,
                },
            error_type,
            timed_out,
            attempts,
        } = failed;

        if error.is_transient() && attempts <= policy.max_retries {
            // Retry from the same committed `state`, after the policy's
            // backoff (design/25-Retry-Backoff.md). Whatever the failed
            // attempt streamed can't be recalled, so tell the consumer.
            let delay = policy
                .backoff
                .map(|backoff| backoff.delay_before(attempts, error.retry_after(), jitter_sample()))
                .unwrap_or_default();
            scope.retry(error.message(), error_type, timed_out, delay);
            emit(
                node_ctx,
                StreamChunkType::Retry,
                error.message(),
                &level.token,
            )
            .await;
            back_off(delay, level).await;
            return None;
        }

        let message = error.message().to_string();
        let kind = if error.is_transient() {
            "transient"
        } else {
            "fatal"
        };

        // The node ends here, before its checkpoint is saved, so a run's events
        // read in the order things happened: NodeCompleted, then CheckpointSaved.
        scope.completed(
            NodeResult::Failed,
            Some(&message),
            Some(error_type),
            policy.fallback.as_deref(),
        );
        emit(node_ctx, StreamChunkType::Error, &message, &level.token).await;

        if let Some(fallback) = &policy.fallback {
            return Some(NodeOutcome::Fallback(
                fallback.clone(),
                NodeFailure {
                    node: node.to_string(),
                    node_path: level.path_of(node),
                    error,
                    attempts,
                    timed_out,
                },
            ));
        }

        let mut engine_metadata = HashMap::from([
            ("error".to_string(), message.clone()),
            ("error_kind".to_string(), kind.to_string()),
            ("attempts".to_string(), attempts.to_string()),
        ]);
        if let Some(hint) = error.retry_after() {
            engine_metadata.insert(RETRY_AFTER_KEY.to_string(), hint.as_millis().to_string());
        }

        let frozen_state = match serde_json::to_value(state) {
            Ok(value) => value,
            Err(err) => {
                return Some(NodeOutcome::Terminal(GraphExecutionResult::Failed(
                    StrataError::ExecutionAborted(format!(
                        "failed to persist state after '{node}' failed: {err}"
                    )),
                )));
            }
        };

        // A subgraph whose child failed at one of its own nodes (or was stopped
        // by this node's timeout) keeps the child's checkpoint, so a resume can
        // re-enter it there.
        let checkpoint = versioned(Checkpoint {
            schema_version: schema::SCHEMA_VERSION.to_string(),
            thread_id: run.thread_id.to_string(),
            status: CheckpointStatus::Failed,
            suspended_node: Some(node.to_string()),
            // The node a replay would re-run.
            next_node: Some(node.to_string()),
            frozen_state,
            metadata: run.level.checkpoint_metadata(Some(engine_metadata)),
            created_at: now_rfc3339(),
            resumed_at: None,
            nested: failed_child,
            decision: run.level.decision(),
            wait: None,
            graph: self.identity.clone(),
        });

        let stopped_at = checkpoint.waiting_at();
        match save_checkpoint(run.store, checkpoint, run.level).await {
            Ok(true) => {}
            // The failure isn't the thread's history: another owner has it
            // (design/32-Thread-Leases.md).
            Ok(false) => {
                return Some(NodeOutcome::Terminal(lease_lost_at(
                    run.thread_id,
                    node,
                    state.clone(),
                    stopped_at,
                )));
            }
            Err(err) => {
                return Some(NodeOutcome::Terminal(GraphExecutionResult::Failed(
                    StrataError::ExecutionAborted(err.0),
                )));
            }
        }

        Some(NodeOutcome::Terminal(GraphExecutionResult::Failed(
            StrataError::NodeFailed {
                node: node.to_string(),
                attempts,
                message,
            },
        )))
    }

    /// Freezes the run around a subgraph node whose child froze at one of its
    /// own interrupt boundaries (`design/16-Nested-Interrupts.md`).
    ///
    /// The checkpoint is suspended *at the subgraph node* — the node a resume
    /// re-enters, so `next_node` is the node itself — and holds `state`, the
    /// state that node started with (its `output` mapper needs it once the
    /// child completes), with the child's own checkpoint nested inside.
    async fn freeze_around_child(
        &self,
        run: RunRefs<'_>,
        node: &str,
        state: S,
        child: Checkpoint,
    ) -> GraphExecutionResult<S> {
        let frozen_state = match serde_json::to_value(&state) {
            Ok(value) => value,
            Err(err) => {
                return GraphExecutionResult::Failed(StrataError::ExecutionAborted(format!(
                    "failed to freeze state at '{node}': {err}"
                )));
            }
        };

        let checkpoint = versioned(Checkpoint {
            schema_version: schema::NESTED_CHECKPOINT_SCHEMA_VERSION.to_string(),
            thread_id: run.thread_id.to_string(),
            status: CheckpointStatus::Interrupted,
            suspended_node: Some(node.to_string()),
            next_node: Some(node.to_string()),
            frozen_state,
            metadata: run.level.checkpoint_metadata(None),
            created_at: now_rfc3339(),
            resumed_at: None,
            nested: Some(Box::new(child)),
            decision: run.level.decision(),
            wait: None,
            graph: self.identity.clone(),
        });

        let waiting_at = checkpoint.waiting_at();
        let wait = checkpoint.innermost_wait().cloned();
        match save_checkpoint(run.store, checkpoint, run.level).await {
            Ok(true) => {}
            Ok(false) => return lease_lost_at(run.thread_id, node, state, waiting_at),
            Err(err) => return GraphExecutionResult::Failed(StrataError::ExecutionAborted(err.0)),
        }

        GraphExecutionResult::Interrupted {
            thread_id: run.thread_id.to_string(),
            suspended_node: node.to_string(),
            next_node: node.to_string(),
            frozen_state: state,
            waiting_at,
            wait,
        }
    }

    /// Ends the run at `node`, which its level's hop limit refused to start
    /// (`design/24-Loop-Circuit-Breaker.md`): persists a `Failed` checkpoint
    /// holding `state`, the last committed state, with `error_kind`
    /// `hop_limit` and `next_node` the node itself, so a resume with a higher
    /// limit starts it. No attempt was made, so there is no `attempts`.
    /// `child` is the checkpoint a subgraph node was being resumed into, if it
    /// was.
    async fn hop_limit_at_node(
        &self,
        run: RunRefs<'_>,
        node: &str,
        state: S,
        limit: u32,
        child: Option<Checkpoint>,
    ) -> GraphExecutionResult<S> {
        let hops = run.level.hops.unwrap_or_default();
        let error = StrataError::HopLimitExceeded {
            node: node.to_string(),
            hops,
            limit,
        };
        run.level
            .run
            .telemetry
            .hop_limit_exceeded(&run.level.path_of(node), hops, limit);
        let frozen_state = match serde_json::to_value(&state) {
            Ok(value) => value,
            Err(err) => {
                return GraphExecutionResult::Failed(StrataError::ExecutionAborted(format!(
                    "failed to persist state at '{node}' on reaching the hop limit: {err}"
                )));
            }
        };

        let checkpoint = versioned(Checkpoint {
            schema_version: schema::SCHEMA_VERSION.to_string(),
            thread_id: run.thread_id.to_string(),
            status: CheckpointStatus::Failed,
            suspended_node: Some(node.to_string()),
            next_node: Some(node.to_string()),
            frozen_state,
            metadata: run.level.checkpoint_metadata(Some(HashMap::from([
                ("error".to_string(), observability::error_message(&error)),
                ("error_kind".to_string(), "hop_limit".to_string()),
            ]))),
            created_at: now_rfc3339(),
            resumed_at: None,
            nested: child.map(Box::new),
            decision: run.level.decision(),
            wait: None,
            graph: self.identity.clone(),
        });

        let stopped_at = checkpoint.waiting_at();
        match save_checkpoint(run.store, checkpoint, run.level).await {
            Ok(true) => {}
            Ok(false) => return lease_lost_at(run.thread_id, node, state, stopped_at),
            Err(err) => return GraphExecutionResult::Failed(StrataError::ExecutionAborted(err.0)),
        }

        GraphExecutionResult::Failed(error)
    }

    /// Ends the run as cancelled at `node`, the node that was running or about
    /// to start (`design/17-Async-Execution.md`): persists a `cancelled`
    /// checkpoint holding `state`, the last committed state, with `next_node`
    /// the node itself so a resume re-enters it. For a subgraph node, `child`
    /// is where its child stopped.
    async fn cancel_at(
        &self,
        run: RunRefs<'_>,
        node: &str,
        state: S,
        child: Option<Checkpoint>,
    ) -> GraphExecutionResult<S> {
        let frozen_state = match serde_json::to_value(&state) {
            Ok(value) => value,
            Err(err) => {
                return GraphExecutionResult::Failed(StrataError::ExecutionAborted(format!(
                    "failed to persist state at '{node}' on cancellation: {err}"
                )));
            }
        };

        let checkpoint = versioned(Checkpoint {
            schema_version: schema::CANCELLED_CHECKPOINT_SCHEMA_VERSION.to_string(),
            thread_id: run.thread_id.to_string(),
            status: CheckpointStatus::Cancelled,
            suspended_node: Some(node.to_string()),
            next_node: Some(node.to_string()),
            frozen_state,
            metadata: run.level.checkpoint_metadata(Some(HashMap::from([(
                "cancel_reason".to_string(),
                run.level.wire_reason().to_string(),
            )]))),
            created_at: now_rfc3339(),
            resumed_at: None,
            nested: child.map(Box::new),
            decision: run.level.decision(),
            wait: None,
            graph: self.identity.clone(),
        });

        let stopped_at = checkpoint.waiting_at();
        match save_checkpoint(run.store, checkpoint, run.level).await {
            Ok(true) => {}
            // A run that lost its lease writes nothing, this checkpoint
            // included (design/32-Thread-Leases.md).
            Ok(false) => return lease_lost_at(run.thread_id, node, state, stopped_at),
            Err(err) => return GraphExecutionResult::Failed(StrataError::ExecutionAborted(err.0)),
        }

        GraphExecutionResult::Cancelled {
            thread_id: run.thread_id.to_string(),
            node: node.to_string(),
            reason: run.level.run.reason(),
            state,
            stopped_at,
        }
    }
}

impl<S> CompiledGraph<S> {
    /// Picks the edge to follow out of `node`, evaluating conditional
    /// edges' predicates against `state` in declaration order — the first
    /// match wins, so a plain `.add_edge(...)` added after one or more
    /// `.add_conditional_edge(...)`s acts as a default "else" branch.
    ///
    /// `Ok(None)` means `node` has no outgoing edges at all (a terminal
    /// node — not an error). `Err` means `node` has outgoing edges but
    /// none of their conditions matched the given state, which
    /// `.compile()` can't catch since it depends on runtime state rather
    /// than graph shape.
    fn next_edge<'a>(
        &'a self,
        entry: &'a NodeEntry<S>,
        state: &S,
    ) -> Result<Option<&'a EdgePlan>, StrataError> {
        for edge in &entry.plan.edges {
            let matches = match &self.edge_conditions[edge.index] {
                Some(condition) => condition(state),
                None => true,
            };
            if matches {
                return Ok(Some(edge));
            }
        }

        if entry.plan.edges.is_empty() {
            Ok(None)
        } else {
            Err(StrataError::NoMatchingEdge(entry.plan.name.to_string()))
        }
    }

    /// `node`'s name as the graph holds it, shared: what the executor's loop
    /// carries from node to node without copying it.
    fn name_of(&self, node: &str) -> Arc<str> {
        match self.nodes.get(node) {
            Some(entry) => entry.plan.name.clone(),
            None => Arc::from(node),
        }
    }
}

/// Ending a thread where it stands (`design/28-Ending-A-Thread.md`, SG-34).
impl<S> CompiledGraph<S>
where
    S: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    /// Ends `thread_id` where it stands: a thread waiting at an interrupt
    /// boundary (at any depth), or stopped by a failure or a cancellation,
    /// gets an `ended` checkpoint and can't be resumed again. Returns
    /// [`GraphExecutionResult::Cancelled`] with [`CancelReason::Ended`]: where
    /// the thread stood, and its last committed state.
    ///
    /// Ending an ended thread returns the same result and writes nothing. A
    /// thread with no checkpoint, a completed one, and one whose resume began
    /// (it may still be running) are refused with
    /// [`GraphExecutionResult::Failed`], and nothing is written.
    ///
    /// A `running` thread (`design/33-Durable-Execution.md`) is ended at the
    /// node that was to run, unless a live lease holds it: then somebody is
    /// running it, and the end is refused. Without leases nothing stops a run
    /// that is alive after all from writing after the end.
    ///
    /// The save is conditional ([`CheckpointStore::save_if_latest`]): if the
    /// thread changes between the load and the save, the end loads it again
    /// and decides again.
    pub async fn end(
        &self,
        thread_id: impl Into<String>,
        store: Arc<dyn CheckpointStore>,
    ) -> GraphExecutionResult<S> {
        self.end_with(thread_id, ThreadEnd::new(), store, RunConfig::default())
            .await
    }

    /// [`Self::end`] with `end`'s [`ThreadEnd::ended_by`], and run-level
    /// inputs: metadata for the ended checkpoint (laid over the stored caller
    /// keys, as for a resume), a run id, and an observer. The token, deadline,
    /// hop limit, history and services don't apply: nothing runs.
    pub async fn end_with(
        &self,
        thread_id: impl Into<String>,
        end: ThreadEnd,
        store: Arc<dyn CheckpointStore>,
        config: RunConfig,
    ) -> GraphExecutionResult<S> {
        let thread_id = thread_id.into();
        if let Err(message) = config.validate() {
            return refused(message);
        }
        let root = RootScope::start(&thread_id, config, RunMode::End, self.durable());
        observed(
            &root,
            &store,
            Box::pin(end_from_store(self, &thread_id, end, &store, &root.level)),
        )
        .await
    }
}

async fn end_from_store<S: DeserializeOwned>(
    graph: &CompiledGraph<S>,
    thread_id: &str,
    end: ThreadEnd,
    store: &Arc<dyn CheckpointStore>,
    level: &LevelScope,
) -> GraphExecutionResult<S> {
    let telemetry = &level.run.telemetry;
    let mut attempt = 0;
    loop {
        attempt += 1;
        let loaded = load_checkpoint(store, thread_id).await;
        if attempt == 1 {
            let stored = loaded
                .as_ref()
                .ok()
                .and_then(Option::as_ref)
                .map(|latest| &latest.checkpoint);
            telemetry.adopt_stored(stored.and_then(|checkpoint| checkpoint.metadata.as_ref()));
            telemetry.run_started(
                stored
                    .and_then(|checkpoint| checkpoint.suspended_node.as_deref())
                    .unwrap_or_default(),
                graph.identity(),
            );
        }
        let latest = match loaded {
            Ok(Some(latest)) => latest,
            Ok(None) => {
                return GraphExecutionResult::Failed(StrataError::NodeNotFound(format!(
                    "no checkpoint found for thread '{thread_id}'"
                )));
            }
            Err(err) => return GraphExecutionResult::Failed(StrataError::ExecutionAborted(err.0)),
        };
        let checkpoint = latest.checkpoint;
        if let Some(refusal) = end_refusal(thread_id, &checkpoint) {
            return GraphExecutionResult::Failed(StrataError::ExecutionAborted(refusal));
        }

        // Another graph's thread is refused, and another version's migrated
        // (design/31-Graph-Identity.md).
        let checkpoint = match graph.adopt(&checkpoint, thread_id, &[]) {
            Ok(adopted) => adopted.unwrap_or(checkpoint),
            Err(problem) => {
                return GraphExecutionResult::Failed(StrataError::ExecutionAborted(problem));
            }
        };
        // A running thread stands at the node that was to run, at every level
        // (design/33-Durable-Execution.md).
        let running = checkpoint.status == CheckpointStatus::Running;
        let checkpoint = if running {
            standing(checkpoint)
        } else {
            checkpoint
        };

        let state: S = match serde_json::from_value(checkpoint.frozen_state.clone()) {
            Ok(state) => state,
            Err(err) => {
                return GraphExecutionResult::Failed(StrataError::ExecutionAborted(format!(
                    "failed to rehydrate state for thread '{thread_id}': {err}"
                )));
            }
        };
        let ended_result = |state: S, reason: CancelReason| GraphExecutionResult::Cancelled {
            thread_id: thread_id.to_string(),
            node: checkpoint.suspended_node.clone().unwrap_or_default(),
            reason,
            state,
            stopped_at: checkpoint.waiting_at(),
        };

        // Already ended: the same answer, and nothing to write.
        if checkpoint.status == CheckpointStatus::Ended {
            return ended_result(state, CancelReason::Ended);
        }

        // A thread another owner holds isn't ended under it
        // (design/32-Thread-Leases.md).
        if let Some(held) = acquire_lease(thread_id, store, level).await {
            return held;
        }
        // Whoever holds a running thread is running it.
        if running && let Some(live) = held_by_another(thread_id, store, level).await {
            return live;
        }

        let ended = ended_checkpoint(&checkpoint, level, &end, &now_rfc3339());
        match save_checkpoint_if_latest(store, ended, latest.sequence, level).await {
            Ok(LeasedSave::Saved) => return ended_result(state, CancelReason::Ended),
            Ok(LeasedSave::LeaseLost) => return ended_result(state, CancelReason::LeaseLost),
            Ok(LeasedSave::NotLatest) => {}
            Err(err) => return GraphExecutionResult::Failed(StrataError::ExecutionAborted(err.0)),
        }

        // Something was appended since the load: a resume, another end, a new
        // run. Decide again.
        if attempt == END_ATTEMPTS {
            return GraphExecutionResult::Failed(StrataError::ExecutionAborted(format!(
                "thread '{thread_id}' kept changing while it was being ended"
            )));
        }
    }
}

/// Why a graph that doesn't checkpoint every node can't recover a thread: it
/// would recover it and then leave it running again when it completed.
const NOT_DURABLE: &str = "this graph doesn't checkpoint every node: only a graph compiled with CheckpointPolicy::EveryNode can recover a thread";

/// Recovering a thread whose run died (`design/33-Durable-Execution.md`, SG-10).
impl<S> CompiledGraph<S>
where
    S: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    /// Continues `thread_id`, whose latest checkpoint is `running`, where that
    /// checkpoint says: at its next node with its state, at every nesting
    /// level. For a thread whose process died. The node that was in flight
    /// runs again ([`NodeContext::is_recovery`](crate::context::NodeContext::is_recovery),
    /// with the same
    /// [`idempotency_key`](crate::context::NodeContext::idempotency_key)); the
    /// nodes before it don't.
    ///
    /// The recovery claims the thread first, by saving the checkpoint again
    /// only if nothing was appended since it was loaded, so of two recoveries
    /// one runs, and a run that turns out to be alive is left alone. A thread
    /// a live lease holds is refused: somebody is running it. So are a thread
    /// that isn't `running`, another graph's thread, and a graph compiled
    /// without [`CheckpointPolicy::EveryNode`](crate::graph::CheckpointPolicy::EveryNode),
    /// each with [`GraphExecutionResult::Failed`] and nothing written.
    ///
    /// Without leases nothing tells a dead run from a slow one: recover only a
    /// thread known to be dead.
    pub async fn recover(
        &self,
        thread_id: impl Into<String>,
        store: Arc<dyn CheckpointStore>,
    ) -> GraphExecutionResult<S> {
        self.recover_with(thread_id, store, RunConfig::default())
            .await
    }

    /// [`Self::recover`] with run-level inputs, as [`Self::run_with`].
    pub async fn recover_with(
        &self,
        thread_id: impl Into<String>,
        store: Arc<dyn CheckpointStore>,
        config: RunConfig,
    ) -> GraphExecutionResult<S> {
        let thread_id = thread_id.into();
        if let Err(message) = config.validate() {
            return refused(message);
        }
        // Waits for the run's scheduler's slot first (design/40-Run-Scheduler.md).
        let mut config = config;
        let _slot = match config.admit(None, None).await {
            Ok(slot) => slot,
            Err(refused) => return GraphExecutionResult::Failed(refused.into()),
        };
        let root = RootScope::start(&thread_id, config, RunMode::Recover, self.durable());
        let run = RunRefs {
            thread_id: &thread_id,
            store: &store,
            stream: None,
            level: &root.level,
        };
        observed(&root, &store, Box::pin(self.recover_from_store(run))).await
    }

    /// [`Self::recover`], streaming the recovered run's chunks as
    /// [`Self::resume_streaming`] does a resumed run's: what a graph with
    /// streaming nodes is recovered with. The stream starts at the node the
    /// recovery starts with. A recovery that is refused ends the stream with
    /// no chunks. Must be called inside a tokio runtime.
    pub fn recover_streaming(
        self: Arc<Self>,
        thread_id: impl Into<String>,
        store: Arc<dyn CheckpointStore>,
    ) -> StreamingRun<S> {
        self.recover_streaming_with(thread_id, store, RunConfig::default())
    }

    /// [`Self::recover_streaming`] with run-level inputs, as [`Self::run_with`].
    pub fn recover_streaming_with(
        self: Arc<Self>,
        thread_id: impl Into<String>,
        store: Arc<dyn CheckpointStore>,
        config: RunConfig,
    ) -> StreamingRun<S> {
        let thread_id = thread_id.into();
        let (ctx, receiver) = StreamContext::channel(thread_id.clone());
        // The task carries the caller's current span, so the run's span is its child.
        let handle = tokio::spawn(
            async move {
                let _close = CloseOnDrop(ctx.clone());
                if let Err(message) = config.validate() {
                    return refused(message);
                }
                // Waits for the run's scheduler's slot first (design/40-Run-Scheduler.md).
                let mut config = config;
                let _slot = match config.admit(None, None).await {
                    Ok(slot) => slot,
                    Err(refused) => return GraphExecutionResult::Failed(refused.into()),
                };
                let root = RootScope::start(&thread_id, config, RunMode::Recover, self.durable());
                let run = RunRefs {
                    thread_id: &thread_id,
                    store: &store,
                    stream: Some(&ctx),
                    level: &root.level,
                };
                observed(&root, &store, Box::pin(self.recover_from_store(run))).await
            }
            .in_current_span(),
        );
        (handle, receiver)
    }

    async fn recover_from_store(&self, run: RunRefs<'_>) -> GraphExecutionResult<S> {
        // With its sequence: the claim is conditional on it.
        let loaded = load_checkpoint(run.store, run.thread_id).await;
        self.recover_loaded(run, loaded).await
    }

    /// [`Self::recover_from_store`] for a latest checkpoint already loaded,
    /// with its sequence: what the driver does after deciding the thread is
    /// dead.
    async fn recover_loaded(
        &self,
        run: RunRefs<'_>,
        loaded: Result<Option<LatestCheckpoint>, CheckpointStoreError>,
    ) -> GraphExecutionResult<S> {
        let thread_id = run.thread_id;
        let stored = loaded
            .as_ref()
            .ok()
            .and_then(Option::as_ref)
            .map(|latest| &latest.checkpoint);
        let telemetry = &run.level.run.telemetry;
        telemetry.adopt_stored(stored.and_then(|checkpoint| checkpoint.metadata.as_ref()));
        telemetry.run_started(
            stored
                .and_then(|checkpoint| checkpoint.next_node.as_deref())
                .unwrap_or_default(),
            self.identity(),
        );
        if !self.durable() {
            return GraphExecutionResult::Failed(StrataError::ExecutionAborted(
                NOT_DURABLE.to_string(),
            ));
        }
        let latest = match loaded {
            Ok(Some(latest)) => latest,
            Ok(None) => {
                return GraphExecutionResult::Failed(StrataError::NodeNotFound(format!(
                    "no checkpoint found for thread '{thread_id}'"
                )));
            }
            Err(err) => return GraphExecutionResult::Failed(StrataError::ExecutionAborted(err.0)),
        };

        self.resume_checkpoint_at(
            thread_id,
            latest.checkpoint,
            latest.sequence,
            LevelSignals::none(),
            run.store,
            run.stream,
            run.level,
            true,
        )
        .await
    }

    /// Recovers the threads of `store` whose run died
    /// (`design/33-Durable-Execution.md`, "Who is dead"): each `running`
    /// thread that no live lease holds and whose latest checkpoint is older
    /// than [`RecoverOptions::with_stale_after`], as [`Self::recover`] does,
    /// one at a time, with [`RecoverOptions::with_config`]'s config (whose
    /// token is also checked between threads), until
    /// [`RecoverOptions::with_limit`] threads have been recovered. `now` is
    /// the caller's clock, as for [`Self::wake_due`].
    ///
    /// Threads are listed by state (this graph's, by identity, and those
    /// matching [`RecoverOptions::with_metadata`]), and each is loaded and
    /// decided again first: one that is no longer running or stale, that a
    /// live lease holds, that isn't this graph's, or that was recovered
    /// [`RecoverOptions::with_max_recoveries`] times already is skipped with
    /// nothing written. The claim is conditional, so of two drivers that list
    /// the same thread one recovers it and the other skips it.
    ///
    /// A library call: the application calls it on its own cadence (no hosted
    /// service). A panic from a recovered run propagates, as from
    /// [`Self::recover`]. Invalid options, a graph compiled without
    /// [`CheckpointPolicy::EveryNode`](crate::graph::CheckpointPolicy::EveryNode),
    /// neither a lease nor a `stale_after`, and a store error listing threads
    /// are returned as `ExecutionAborted`.
    pub async fn recover_stale<T>(
        &self,
        store: Arc<T>,
        now: DateTime<Utc>,
        options: RecoverOptions,
    ) -> Result<RecoveryReport<S>, StrataError>
    where
        T: CheckpointStore + CheckpointQuery + 'static,
    {
        let refuse = |message: String| StrataError::ExecutionAborted(message);
        check_limit(options.limit).map_err(|err| refuse(err.0))?;
        options.config.validate().map_err(refuse)?;
        if options.max_recoveries == Some(0) {
            return Err(refuse("max_recoveries must be at least 1".to_string()));
        }
        if !self.durable() {
            return Err(refuse(NOT_DURABLE.to_string()));
        }
        // Without leases the age of a running checkpoint is the only sign
        // that its run died, and only the caller knows how long its nodes take.
        let stale_after = match options.stale_after {
            Some(age) => age,
            None if options.config.has_lease() => Duration::ZERO,
            None => {
                return Err(refuse(
                    "recover_stale needs RecoverOptions::with_stale_after when its runs have no lease: without leases only the age of a running checkpoint says its run died"
                        .to_string(),
                ));
            }
        };
        let stale_before = (!stale_after.is_zero()).then(|| {
            chrono::Duration::from_std(stale_after)
                .ok()
                .and_then(|age| now.checked_sub_signed(age))
                .unwrap_or(DateTime::<Utc>::MIN_UTC)
        });
        let stale_text = stale_before.map(timestamp_text);

        let as_store: Arc<dyn CheckpointStore> = store.clone();
        // The config's token cancels the run in flight too; the stop token
        // only keeps another from starting.
        let cancelled = || {
            options
                .config
                .cancellation()
                .is_some_and(CancellationToken::is_cancelled)
                || options
                    .stop
                    .as_ref()
                    .is_some_and(CancellationToken::is_cancelled)
        };
        let mut report = RecoveryReport {
            recovered: Vec::new(),
            skipped: Vec::new(),
        };
        let mut cursor = None;
        loop {
            let mut query = ThreadQuery::default().with_states([ThreadState::Running]);
            query.metadata = options.metadata.clone();
            query.updated_before = stale_before;
            // Only this graph's threads, when it has an identity
            // (design/31-Graph-Identity.md); any version it may migrate.
            if let Some(own) = self.identity.as_ref().filter(|_| options.match_graph) {
                query.graph_id = Some(own.id.clone());
                query.graph_version = self
                    .migration
                    .is_none()
                    .then(|| own.version.clone())
                    .flatten();
            }
            query.cursor = cursor.take();
            let page = store
                .list_threads(&query)
                .await
                .map_err(|err| refuse(err.0))?;
            for thread in page.threads {
                if report.recovered.len() >= options.limit || cancelled() {
                    return Ok(report);
                }
                self.recover_one(
                    &thread,
                    stale_text.as_deref(),
                    &options,
                    &as_store,
                    &mut report,
                )
                .await;
            }
            match page.next_cursor {
                Some(next) if report.recovered.len() < options.limit && !cancelled() => {
                    cursor = Some(next);
                }
                _ => return Ok(report),
            }
        }
    }

    /// Decides again whether `thread_id` is running, stale, this graph's and
    /// nobody's, then recovers it.
    async fn recover_one(
        &self,
        listed: &ThreadSummary,
        stale_before: Option<&str>,
        options: &RecoverOptions,
        store: &Arc<dyn CheckpointStore>,
        report: &mut RecoveryReport<S>,
    ) {
        let thread_id = listed.thread_id.as_str();
        let skip = |reason: String| SkippedThread {
            thread_id: thread_id.to_string(),
            reason,
        };
        // With a scheduler, a slot first (design/40-Run-Scheduler.md).
        let mut config = options.config.clone();
        let _slot = match config
            .admit(Some(&listed.metadata), options.stop.as_ref())
            .await
        {
            Ok(slot) => slot,
            Err(refused) => {
                return report
                    .skipped
                    .push(skip(format!("it was not admitted: {refused}")));
            }
        };
        // With its sequence: the claim is conditional on it.
        let mut latest = match load_checkpoint(store, thread_id).await {
            Ok(Some(latest)) => latest,
            Ok(None) => {
                return report
                    .skipped
                    .push(skip("it has no checkpoint".to_string()));
            }
            Err(err) => {
                return report
                    .skipped
                    .push(skip(format!("its checkpoint can't be loaded: {}", err.0)));
            }
        };
        let checkpoint = &latest.checkpoint;
        if !schema::is_supported_checkpoint_version(&checkpoint.schema_version) {
            return report.skipped.push(skip(format!(
                "its checkpoint has unsupported schema_version '{}'",
                checkpoint.schema_version
            )));
        }
        if checkpoint.status != CheckpointStatus::Running {
            return report
                .skipped
                .push(skip("it is no longer running".to_string()));
        }
        if stale_before.is_some_and(|before| updated_at_text(checkpoint) >= before) {
            return report.skipped.push(skip("it is not stale yet".to_string()));
        }
        let recoveries = recoveries_of(checkpoint);
        if options.max_recoveries.is_some_and(|max| recoveries >= max) {
            return report.skipped.push(skip(format!(
                "it was recovered {recoveries} time(s) without a node completing"
            )));
        }
        // Another graph's thread is skipped, and another version's migrated
        // (design/31-Graph-Identity.md).
        match self.adopt(checkpoint, thread_id, &[]) {
            Ok(Some(adopted)) => latest.checkpoint = adopted,
            Ok(None) => {}
            Err(problem) => return report.skipped.push(skip(problem)),
        }
        let checkpoint = &latest.checkpoint;
        let check = self.check_resume(checkpoint, &LevelSignals::none(), thread_id, &[]);
        if let Some(problem) = check.problem {
            return report.skipped.push(skip(problem));
        }
        // Whoever holds a running thread is running it. Checked here, before
        // a run is started for it, so a sweep over live threads is one read
        // each and no events; the recovery checks again when it claims.
        match store.get_lease(thread_id).await {
            Ok(None) => {}
            Err(err) if err.0 == NO_LEASES => {}
            Ok(Some(live)) => {
                return report.skipped.push(skip(format!(
                    "it is held by '{}' until {}",
                    live.owner,
                    wake_text(live.expires_at)
                )));
            }
            Err(err) => {
                return report
                    .skipped
                    .push(skip(format!("its lease can't be read: {}", err.0)));
            }
        }

        let at = continues_at(checkpoint);
        let root = RootScope::start(thread_id, config, RunMode::Recover, self.durable());
        let run = RunRefs {
            thread_id,
            store,
            stream: None,
            level: &root.level,
        };
        let result = observed(
            &root,
            store,
            Box::pin(self.recover_loaded(run, Ok(Some(latest)))),
        )
        .await;
        // A thread another owner holds is theirs
        // (design/32-Thread-Leases.md).
        if let Some(held) = root.level.run.held() {
            return report.skipped.push(skip(format!("it is {held}")));
        }
        if root.level.run.superseded.load(Ordering::Relaxed) {
            report
                .skipped
                .push(skip("another run or recovery got there first".to_string()));
        } else {
            report.recovered.push(RecoveredThread {
                thread_id: thread_id.to_string(),
                at,
                recoveries: recoveries + 1,
                result,
            });
        }
    }
}

/// Event waits and durable timers (`design/29-Event-Waits-And-Timers.md`, SG-16).
impl<S> CompiledGraph<S>
where
    S: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    /// Answers a thread waiting for `signal`'s event at a wait node
    /// ([`StateGraph::add_wait_for_event`](crate::graph::StateGraph::add_wait_for_event)),
    /// at any depth: applies the signal's overrides to the waiting level's
    /// state and continues at the edge out of the wait node, like a resume. It
    /// records no decision: the level keeps the one it had.
    ///
    /// A thread that isn't waiting for that event type (waiting for another,
    /// for a human, for a timer, or not waiting at all) is refused with
    /// [`GraphExecutionResult::Failed`] before anything is written, as is
    /// everything a resume refuses. The resume stamp is conditional, so a
    /// signal racing a timeout, an end or another signal can't both win.
    pub async fn signal(
        &self,
        thread_id: impl Into<String>,
        signal: EventSignal,
        store: Arc<dyn CheckpointStore>,
    ) -> GraphExecutionResult<S> {
        self.signal_with(thread_id, signal, store, RunConfig::default())
            .await
    }

    /// [`Self::signal`] with run-level inputs, as [`Self::run_with`].
    pub async fn signal_with(
        &self,
        thread_id: impl Into<String>,
        signal: EventSignal,
        store: Arc<dyn CheckpointStore>,
        config: RunConfig,
    ) -> GraphExecutionResult<S> {
        let thread_id = thread_id.into();
        if let Err(message) = config.validate() {
            return refused(message);
        }
        if signal.event_type.trim().is_empty() {
            return refused("a signal needs an event type".to_string());
        }
        // Waits for the run's scheduler's slot first (design/40-Run-Scheduler.md).
        let mut config = config;
        let _slot = match config.admit(None, None).await {
            Ok(slot) => slot,
            Err(refused) => return GraphExecutionResult::Failed(refused.into()),
        };
        let root = RootScope::start(&thread_id, config, RunMode::Resume, self.durable());
        let run = RunRefs {
            thread_id: &thread_id,
            store: &store,
            stream: None,
            level: &root.level,
        };
        let signals = LevelSignals::for_wake(WakeReason::Event, Some(&signal));
        observed(
            &root,
            &store,
            Box::pin(self.resume_from_store(run, signals)),
        )
        .await
    }

    /// Wakes every thread of `store` that is due at `now`: a timer whose time
    /// has come continues at the edge out of it, and a wait for a human or an
    /// event whose timeout has passed continues at its timeout route or, with
    /// none, is ended with `strata.end_reason` `timed_out`
    /// (`design/29-Event-Waits-And-Timers.md`, "Waking"). `now` is the
    /// caller's clock: tokio's is monotonic, not wall time.
    ///
    /// Threads are listed with [`ThreadQuery::due_by`], and woken one at a
    /// time, each as a run of its own (a resume, or an end), until 100 have
    /// been woken or skipped; [`Self::wake_due_with`] sets the limit, a
    /// metadata filter and the runs' config. Each is loaded and decided again
    /// first: one that stopped being due, or whose wait isn't one of this
    /// graph's, is skipped with nothing written. The stamp and the end are
    /// conditional, so of two drivers that list the same thread one wakes it
    /// and the other skips it.
    ///
    /// A library call: the application calls it on its own cadence (no hosted
    /// service). A panic from a woken run propagates, as from
    /// [`Self::resume`]. A store error listing threads is returned as
    /// `ExecutionAborted`.
    pub async fn wake_due<T>(
        &self,
        store: Arc<T>,
        now: DateTime<Utc>,
    ) -> Result<WakeReport<S>, StrataError>
    where
        T: CheckpointStore + CheckpointQuery + 'static,
    {
        self.wake_due_with(store, now, WakeOptions::default()).await
    }

    /// [`Self::wake_due`] with `options`: only threads whose metadata has
    /// every pair of [`WakeOptions::with_metadata`] (a store holds many graphs'
    /// threads), at most [`WakeOptions::with_limit`] of them, each run with
    /// [`WakeOptions::with_config`]'s config (whose token is also checked
    /// between threads). An invalid limit or config refuses the call.
    pub async fn wake_due_with<T>(
        &self,
        store: Arc<T>,
        now: DateTime<Utc>,
        options: WakeOptions,
    ) -> Result<WakeReport<S>, StrataError>
    where
        T: CheckpointStore + CheckpointQuery + 'static,
    {
        let refuse = |message: String| StrataError::ExecutionAborted(message);
        check_limit(options.limit).map_err(|err| refuse(err.0))?;
        options.config.validate().map_err(refuse)?;
        let due = timestamp_text(now);
        let as_store: Arc<dyn CheckpointStore> = store.clone();
        // The config's token cancels the run in flight too; the stop token
        // only keeps another from starting.
        let cancelled = || {
            options
                .config
                .cancellation()
                .is_some_and(CancellationToken::is_cancelled)
                || options
                    .stop
                    .as_ref()
                    .is_some_and(CancellationToken::is_cancelled)
        };
        let mut report = WakeReport {
            woken: Vec::new(),
            skipped: Vec::new(),
        };
        let mut cursor = None;
        loop {
            let mut query = ThreadQuery::default().due_by(now).with_limit(options.limit);
            query.metadata = options.metadata.clone();
            // Only this graph's threads, when it has an identity
            // (design/31-Graph-Identity.md); any version it may migrate.
            if let Some(own) = self.identity.as_ref().filter(|_| options.match_graph) {
                query.graph_id = Some(own.id.clone());
                query.graph_version = self
                    .migration
                    .is_none()
                    .then(|| own.version.clone())
                    .flatten();
            }
            query.cursor = cursor.take();
            let page = store
                .list_threads(&query)
                .await
                .map_err(|err| refuse(err.0))?;
            for thread in page.threads {
                if report.woken.len() + report.skipped.len() >= options.limit || cancelled() {
                    return Ok(report);
                }
                self.wake_one(&thread, &due, &as_store, &options, &mut report)
                    .await;
            }
            match page.next_cursor {
                Some(next)
                    if report.woken.len() + report.skipped.len() < options.limit
                        && !cancelled() =>
                {
                    cursor = Some(next);
                }
                _ => return Ok(report),
            }
        }
    }

    /// Decides again whether `thread_id` is due at `due` and one of this
    /// graph's waits, then wakes it: a resume for a timer or a timeout with a
    /// route, an end for a timeout with none.
    async fn wake_one(
        &self,
        listed: &ThreadSummary,
        due: &str,
        store: &Arc<dyn CheckpointStore>,
        options: &WakeOptions,
        report: &mut WakeReport<S>,
    ) {
        let thread_id = listed.thread_id.as_str();
        let skip = |reason: String| SkippedThread {
            thread_id: thread_id.to_string(),
            reason,
        };
        // With a scheduler, a slot first (design/40-Run-Scheduler.md); an end runs in it.
        let mut config = options.config.clone();
        let _slot = match config
            .admit(Some(&listed.metadata), options.stop.as_ref())
            .await
        {
            Ok(slot) => slot,
            Err(refused) => {
                return report
                    .skipped
                    .push(skip(format!("it was not admitted: {refused}")));
            }
        };
        let config = &config;
        // With its sequence: the stamp, or the end, is conditional on it
        // (design/28-Ending-A-Thread.md).
        let mut latest = match load_checkpoint(store, thread_id).await {
            Ok(Some(latest)) => latest,
            Ok(None) => {
                return report
                    .skipped
                    .push(skip("it has no checkpoint".to_string()));
            }
            Err(err) => {
                return report
                    .skipped
                    .push(skip(format!("its checkpoint can't be loaded: {}", err.0)));
            }
        };
        let checkpoint = &latest.checkpoint;
        if !schema::is_supported_checkpoint_version(&checkpoint.schema_version) {
            return report.skipped.push(skip(format!(
                "its checkpoint has unsupported schema_version '{}'",
                checkpoint.schema_version
            )));
        }
        // What is due is the stored wait, whatever a migration makes of it.
        let Some(reason) = LevelSignals::due_for(checkpoint, due) else {
            return report.skipped.push(skip("it is no longer due".to_string()));
        };
        // Another graph's thread is skipped, and another version's migrated
        // (design/31-Graph-Identity.md).
        match self.adopt(checkpoint, thread_id, &[]) {
            Ok(Some(adopted)) => latest.checkpoint = adopted,
            Ok(None) => {}
            Err(problem) => return report.skipped.push(skip(problem)),
        }
        let checkpoint = &latest.checkpoint;
        let signals = LevelSignals::for_wake(reason, None);
        let check = self.check_resume(checkpoint, &signals, thread_id, &[]);
        if let Some(problem) = check.problem {
            return report.skipped.push(skip(problem));
        }

        let (result, superseded, held) = if check.ends_on_timeout {
            let root = RootScope::start(thread_id, config.clone(), RunMode::End, self.durable());
            let result = observed(
                &root,
                store,
                Box::pin(end_timed_out(self, thread_id, latest, store, &root.level)),
            )
            .await;
            let superseded = root.level.run.superseded.load(Ordering::Relaxed);
            (result, superseded, root.level.run.held())
        } else {
            let root = RootScope::start(thread_id, config.clone(), RunMode::Resume, self.durable());
            let run = RunRefs {
                thread_id,
                store,
                stream: None,
                level: &root.level,
            };
            let result = observed(
                &root,
                store,
                Box::pin(self.resume_loaded(run, Ok(Some(latest)), signals)),
            )
            .await;
            (
                result,
                root.level.run.superseded.load(Ordering::Relaxed),
                root.level.run.held(),
            )
        };

        // A thread another owner holds is theirs to wake
        // (design/32-Thread-Leases.md).
        if let Some(held) = held {
            return report.skipped.push(skip(format!("it is {held}")));
        }
        if superseded {
            report
                .skipped
                .push(skip("another driver woke or ended it first".to_string()));
        } else {
            report.woken.push(WokenThread {
                thread_id: thread_id.to_string(),
                reason,
                result,
            });
        }
    }
}

/// [`end_on_timeout`] as the wake driver's run of its own (`RunMode::End`):
/// starts the run's telemetry first.
async fn end_timed_out<S: DeserializeOwned>(
    graph: &CompiledGraph<S>,
    thread_id: &str,
    latest: LatestCheckpoint,
    store: &Arc<dyn CheckpointStore>,
    level: &LevelScope,
) -> GraphExecutionResult<S> {
    let checkpoint = latest.checkpoint;
    let telemetry = &level.run.telemetry;
    telemetry.adopt_stored(checkpoint.metadata.as_ref());
    telemetry.run_started(
        checkpoint.suspended_node.as_deref().unwrap_or_default(),
        graph.identity(),
    );
    end_on_timeout(thread_id, checkpoint, latest.sequence, store, level).await
}

/// Ends a thread whose wait timed out with no timeout route
/// (`design/29-Event-Waits-And-Timers.md`): [`CompiledGraph::end`]'s ended
/// checkpoint with `strata.end_reason` `timed_out`, saved only if `checkpoint`
/// is still the thread's latest, at `sequence`, and counted as a wake when it
/// is. The run has started: the wake driver's end, or a resume given
/// [`LevelSignals::due`] (`design/42-Waking-Through-A-Resume.md`).
async fn end_on_timeout<S: DeserializeOwned>(
    thread_id: &str,
    checkpoint: Checkpoint,
    sequence: Option<i64>,
    store: &Arc<dyn CheckpointStore>,
    level: &LevelScope,
) -> GraphExecutionResult<S> {
    let state: S = match serde_json::from_value(checkpoint.frozen_state.clone()) {
        Ok(state) => state,
        Err(err) => {
            return GraphExecutionResult::Failed(StrataError::ExecutionAborted(format!(
                "failed to rehydrate state for thread '{thread_id}': {err}"
            )));
        }
    };
    let end = ThreadEnd {
        ended_by: None,
        reason: END_TIMED_OUT,
    };
    if let Some(held) = acquire_lease(thread_id, store, level).await {
        return held;
    }
    let ended = ended_checkpoint(&checkpoint, level, &end, &now_rfc3339());
    match save_checkpoint_if_latest(store, ended, sequence, level).await {
        Ok(saved @ (LeasedSave::Saved | LeasedSave::LeaseLost)) => {
            if saved == LeasedSave::Saved {
                RunTelemetry::woken(WakeReason::Timeout);
            }
            GraphExecutionResult::Cancelled {
                thread_id: thread_id.to_string(),
                node: checkpoint.suspended_node.clone().unwrap_or_default(),
                reason: if saved == LeasedSave::Saved {
                    CancelReason::Ended
                } else {
                    CancelReason::LeaseLost
                },
                state,
                stopped_at: checkpoint.waiting_at(),
            }
        }
        Ok(LeasedSave::NotLatest) => {
            level.run.superseded.store(true, Ordering::Relaxed);
            GraphExecutionResult::Failed(StrataError::ExecutionAborted(format!(
                "thread '{thread_id}' changed before its timeout could end it: another driver got there first"
            )))
        }
        Err(err) => GraphExecutionResult::Failed(StrataError::ExecutionAborted(err.0)),
    }
}

/// Why `checkpoint`, a thread's latest, can't be ended; `None` if it can.
fn end_refusal(thread_id: &str, checkpoint: &Checkpoint) -> Option<String> {
    if !schema::is_supported_checkpoint_version(&checkpoint.schema_version) {
        return Some(format!(
            "checkpoint for thread '{thread_id}' has unsupported schema_version '{}'",
            checkpoint.schema_version
        ));
    }
    if let Some(resumed_at) = &checkpoint.resumed_at {
        return Some(format!(
            "thread '{thread_id}' was resumed at {resumed_at} and may still be running: only a waiting, failed or cancelled thread can be ended"
        ));
    }
    (checkpoint.status == CheckpointStatus::Completed).then(|| {
        format!(
            "thread '{thread_id}' has completed: only a waiting, failed or cancelled thread can be ended"
        )
    })
}

/// `checkpoint` ended where it stands, at every level it nests: each keeps
/// where it stood, its state and its decision, has no next node or wait, and carries
/// the end's metadata (the run's, with the reason and who ended it) instead of
/// the engine keys of the stop it ends.
fn ended_checkpoint(
    checkpoint: &Checkpoint,
    level: &LevelScope,
    end: &ThreadEnd,
    now: &str,
) -> Checkpoint {
    let mut engine = HashMap::from([(END_REASON_KEY.to_string(), end.reason.to_string())]);
    if let Some(ended_by) = &end.ended_by {
        engine.insert(ENDED_BY_KEY.to_string(), ended_by.clone());
    }
    versioned(Checkpoint {
        schema_version: String::new(),
        thread_id: checkpoint.thread_id.clone(),
        status: CheckpointStatus::Ended,
        suspended_node: checkpoint.suspended_node.clone(),
        next_node: None,
        frozen_state: checkpoint.frozen_state.clone(),
        metadata: level.run.telemetry.checkpoint_metadata(Some(engine)),
        created_at: now.to_string(),
        resumed_at: None,
        nested: checkpoint
            .nested
            .as_deref()
            .map(|child| Box::new(ended_checkpoint(child, level, end, now))),
        decision: checkpoint.decision.clone(),
        // An ended thread never wakes (design/29-Event-Waits-And-Timers.md).
        wait: None,
        graph: checkpoint.graph.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{RecoveryPolicy, StateGraph};
    use serde::{Deserialize, Serialize};
    use std::sync::Mutex;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct WireTransferState {
        invoice_generated: bool,
        amount: f64,
        currency: String,
        approved_by: Option<String>,
        is_completed: bool,
    }

    /// A minimal `CheckpointStore` test double. Deliberately not
    /// `strata_memory::InMemoryCheckpointStore` — this crate can't depend
    /// on `strata-memory` (that dependency runs the other way), so unit
    /// tests of `run`/`resume` themselves use this tiny stand-in. The real
    /// adapter is exercised by `strata-memory`'s own tests, including the
    /// shared-fixture cross-language checkpoint test.
    #[derive(Default)]
    struct TestStore {
        checkpoints: Mutex<HashMap<String, Checkpoint>>,
    }

    impl TestStore {
        fn save(&self, checkpoint: Checkpoint) -> Result<(), CheckpointStoreError> {
            self.checkpoints
                .lock()
                .unwrap()
                .insert(checkpoint.thread_id.clone(), checkpoint);
            Ok(())
        }

        fn load(&self, thread_id: &str) -> Result<Option<Checkpoint>, CheckpointStoreError> {
            Ok(self.checkpoints.lock().unwrap().get(thread_id).cloned())
        }
    }

    impl CheckpointStore for TestStore {
        fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
            Box::pin(std::future::ready(TestStore::save(self, checkpoint)))
        }

        fn load<'a>(
            &'a self,
            thread_id: &'a str,
        ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
            Box::pin(std::future::ready(TestStore::load(self, thread_id)))
        }
    }

    /// The STRATA-SPEC-002 wire-transfer example: freeze immediately after
    /// preparing the transfer, resume (with human approval) to execute it.
    fn wire_transfer_graph() -> CompiledGraph<WireTransferState> {
        StateGraph::<WireTransferState>::new()
            .add_node("PrepareWireTransfer", |s: WireTransferState| {
                WireTransferState {
                    invoice_generated: true,
                    ..s
                }
            })
            .add_interrupt_boundary("PrepareWireTransfer")
            .add_node("ExecuteWireTransfer", |s: WireTransferState| {
                WireTransferState {
                    is_completed: true,
                    ..s
                }
            })
            .add_edge("PrepareWireTransfer", "ExecuteWireTransfer")
            .compile()
            .unwrap()
    }

    fn initial_state() -> WireTransferState {
        WireTransferState {
            invoice_generated: false,
            amount: 4200.5,
            currency: "USD".to_string(),
            approved_by: None,
            is_completed: false,
        }
    }

    #[tokio::test]
    async fn run_freezes_at_the_interrupt_boundary_and_flushes_to_the_store() {
        let graph = wire_transfer_graph();
        let store = Arc::new(TestStore::default());

        let result = graph
            .run("thread_hitl_demo", initial_state(), store.clone())
            .await;

        match result {
            GraphExecutionResult::Interrupted {
                thread_id,
                suspended_node,
                next_node,
                frozen_state,
                waiting_at,
                ..
            } => {
                assert_eq!(thread_id, "thread_hitl_demo");
                assert_eq!(suspended_node, "PrepareWireTransfer");
                assert_eq!(waiting_at, ["PrepareWireTransfer"]);
                assert_eq!(next_node, "ExecuteWireTransfer");
                assert!(frozen_state.invoice_generated);
                assert!(!frozen_state.is_completed);
            }
            _ => panic!("expected Interrupted"),
        }

        // FLUSH: the checkpoint must actually be in the store, independent
        // of the in-process return value asserted above.
        let persisted = store.load("thread_hitl_demo").unwrap().unwrap();
        assert_eq!(persisted.status, CheckpointStatus::Interrupted);
        assert_eq!(
            persisted.suspended_node.as_deref(),
            Some("PrepareWireTransfer")
        );
        assert_eq!(persisted.next_node.as_deref(), Some("ExecuteWireTransfer"));
        assert_eq!(
            persisted.frozen_state["invoice_generated"],
            serde_json::json!(true)
        );
        assert!(persisted.resumed_at.is_none());
    }

    #[tokio::test]
    async fn resume_injects_field_overrides_and_completes_the_graph() {
        let graph = wire_transfer_graph();
        let store = Arc::new(TestStore::default());
        graph
            .run("thread_hitl_demo", initial_state(), store.clone())
            .await;

        let mut overrides = HashMap::new();
        overrides.insert("approved_by".to_string(), "admin_01".to_string());
        let signal = HumanSignal {
            is_approved: true,
            reviewer_token: "token-abc".to_string(),
            field_overrides: overrides,
            ..Default::default()
        };

        let result = graph
            .resume("thread_hitl_demo", signal, store.clone())
            .await;

        match result {
            GraphExecutionResult::Completed(state) => {
                assert_eq!(
                    state,
                    WireTransferState {
                        invoice_generated: true,
                        amount: 4200.5,
                        currency: "USD".to_string(),
                        approved_by: Some("admin_01".to_string()),
                        is_completed: true,
                    }
                );
            }
            _ => panic!("expected Completed"),
        }

        // The checkpoint stays in the store (audit trail) but is now marked resumed.
        let persisted = store.load("thread_hitl_demo").unwrap().unwrap();
        assert!(persisted.resumed_at.is_some());
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct RoutingState {
        n: i32,
        path: Vec<String>,
    }

    fn routing_graph() -> CompiledGraph<RoutingState> {
        StateGraph::<RoutingState>::new()
            .add_node("Classify", |mut s: RoutingState| {
                s.path.push("Classify".to_string());
                s
            })
            .add_node("EvenPath", |mut s: RoutingState| {
                s.path.push("EvenPath".to_string());
                s
            })
            .add_node("OddPath", |mut s: RoutingState| {
                s.path.push("OddPath".to_string());
                s
            })
            // The conditional edge is the "if"; the plain edge added after
            // it is the "else" — `next_edge` tries them in this order.
            .add_conditional_edge("Classify", "EvenPath", |s: &RoutingState| s.n % 2 == 0)
            .add_edge("Classify", "OddPath")
            .compile()
            .unwrap()
    }

    #[tokio::test]
    async fn conditional_edge_routes_based_on_runtime_state() {
        let graph = routing_graph();
        let store = Arc::new(TestStore::default());

        match graph
            .run(
                "even",
                RoutingState {
                    n: 4,
                    path: Vec::new(),
                },
                store.clone(),
            )
            .await
        {
            GraphExecutionResult::Completed(state) => {
                assert_eq!(state.path, vec!["Classify", "EvenPath"]);
            }
            _ => panic!("expected Completed"),
        }

        match graph
            .run(
                "odd",
                RoutingState {
                    n: 3,
                    path: Vec::new(),
                },
                store.clone(),
            )
            .await
        {
            GraphExecutionResult::Completed(state) => {
                assert_eq!(state.path, vec!["Classify", "OddPath"]);
            }
            _ => panic!("expected Completed"),
        }
    }

    #[tokio::test]
    async fn no_matching_conditional_edge_fails_at_runtime() {
        let graph = StateGraph::<i32>::new()
            .add_node("A", |s: i32| s)
            .add_node("B", |s: i32| s)
            .add_conditional_edge("A", "B", |_: &i32| false)
            .compile()
            .unwrap();

        let store = Arc::new(TestStore::default());
        match graph.run("thread", 1, store.clone()).await {
            GraphExecutionResult::Failed(StrataError::NoMatchingEdge(node)) => {
                assert_eq!(node, "A");
            }
            _ => panic!("expected Failed(NoMatchingEdge)"),
        }
    }

    #[tokio::test]
    async fn guarded_cycle_terminates_via_its_condition() {
        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct RetryState {
            attempts: u32,
        }

        let graph = StateGraph::<RetryState>::new()
            .add_node("Attempt", |s: RetryState| RetryState {
                attempts: s.attempts + 1,
            })
            .add_node("Done", |s: RetryState| s)
            .add_conditional_edge("Attempt", "Attempt", |s: &RetryState| s.attempts < 3)
            .add_edge("Attempt", "Done")
            .compile()
            .unwrap();

        let store = Arc::new(TestStore::default());
        match graph
            .run("retry", RetryState { attempts: 0 }, store.clone())
            .await
        {
            GraphExecutionResult::Completed(state) => assert_eq!(state.attempts, 3),
            _ => panic!("expected Completed"),
        }
    }

    #[tokio::test]
    async fn resume_without_a_checkpoint_fails() {
        let graph = wire_transfer_graph();
        let store = Arc::new(TestStore::default());

        let result = graph
            .resume("does-not-exist", HumanSignal::default(), store.clone())
            .await;
        assert!(matches!(result, GraphExecutionResult::Failed(_)));
    }

    #[tokio::test]
    async fn resume_of_an_already_resumed_checkpoint_fails() {
        let graph = wire_transfer_graph();
        let store = Arc::new(TestStore::default());
        graph
            .run("thread_hitl_demo", initial_state(), store.clone())
            .await;
        graph
            .resume("thread_hitl_demo", HumanSignal::default(), store.clone())
            .await;

        let result = graph
            .resume("thread_hitl_demo", HumanSignal::default(), store.clone())
            .await;
        assert!(matches!(result, GraphExecutionResult::Failed(_)));
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct GreetingState {
        name: String,
        greeted: bool,
    }

    fn greeting_graph() -> Arc<CompiledGraph<GreetingState>> {
        Arc::new(
            StateGraph::<GreetingState>::new()
                .add_streaming_node("Greet", |mut s: GreetingState, ctx: &StreamContext| {
                    ctx.emit_token_blocking("Hello, ").unwrap();
                    ctx.emit_token_blocking(&s.name).unwrap();
                    s.greeted = true;
                    s
                })
                .add_node("Done", |s: GreetingState| s)
                .add_edge("Greet", "Done")
                .compile()
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn run_streaming_emits_tokens_and_lifecycle_chunks_and_completes() {
        let graph = greeting_graph();
        let store: Arc<dyn CheckpointStore> = Arc::new(TestStore::default());
        let initial = GreetingState {
            name: "World".to_string(),
            greeted: false,
        };

        let (handle, mut receiver) = graph.run_streaming("thread_stream", initial, store);

        let mut chunks = Vec::new();
        while let Some(chunk) = receiver.recv().await {
            chunks.push(chunk);
        }

        let result = handle.await.unwrap();
        match result {
            GraphExecutionResult::Completed(state) => {
                assert!(state.greeted);
                assert_eq!(state.name, "World");
            }
            _ => panic!("expected Completed"),
        }

        // NodeStart/NodeEnd surround both nodes automatically; only the
        // streaming node ("Greet") also emits its own Token chunks, in the
        // order it called `emit_token_blocking`.
        let summary: Vec<(StreamChunkType, String)> = chunks
            .iter()
            .map(|c| (c.chunk_type, c.content.to_string()))
            .collect();
        assert_eq!(
            summary,
            vec![
                (StreamChunkType::NodeStart, "Greet".to_string()),
                (StreamChunkType::Token, "Hello, ".to_string()),
                (StreamChunkType::Token, "World".to_string()),
                (StreamChunkType::NodeEnd, "Greet".to_string()),
                (StreamChunkType::NodeStart, "Done".to_string()),
                (StreamChunkType::NodeEnd, "Done".to_string()),
            ]
        );
        assert!(
            chunks
                .iter()
                .all(|c| c.node_name.as_ref() == "Greet" || c.node_name.as_ref() == "Done")
        );
    }

    #[tokio::test]
    async fn plain_run_of_a_graph_with_a_streaming_node_fails_at_that_node() {
        let graph = greeting_graph();
        let store = Arc::new(TestStore::default());

        let result = graph
            .run(
                "thread_no_stream",
                GreetingState {
                    name: "World".to_string(),
                    greeted: false,
                },
                store.clone(),
            )
            .await;

        match result {
            GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
                assert!(message.contains("Greet"), "message was: {message}");
            }
            _ => panic!("expected Failed(ExecutionAborted)"),
        }
    }

    // --- Error recovery (design/04-Error-Recovery.md) ---

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct PaymentState {
        prepared: bool,
        charged: bool,
        notes: Vec<String>,
    }

    fn payment_state() -> PaymentState {
        PaymentState {
            prepared: false,
            charged: false,
            notes: Vec::new(),
        }
    }

    /// `Prepare` (plain) -> `Charge` (fallible, calls counted in `calls`).
    /// The first `failures` calls to `Charge` return `error`; later calls
    /// succeed. A failing call still *builds* a modified state before
    /// returning `Err`, to prove that work never reaches the committed
    /// state.
    fn payment_graph(
        calls: Arc<std::sync::atomic::AtomicU32>,
        failures: u32,
        error: NodeError,
        policy: Option<RecoveryPolicy>,
    ) -> Result<CompiledGraph<PaymentState>, StrataError> {
        use std::sync::atomic::Ordering;

        let mut graph = StateGraph::<PaymentState>::new()
            .add_node("Prepare", |s: PaymentState| PaymentState {
                prepared: true,
                ..s
            })
            .add_fallible_node("Charge", move |s: &PaymentState| {
                let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                let mut next = s.clone();
                next.charged = true;
                next.notes.push(format!("partial work in call {call}"));
                if call <= failures {
                    Err(error.clone())
                } else {
                    Ok(next)
                }
            })
            .add_node("Fallback", |mut s: PaymentState| {
                s.notes.push("fell back".to_string());
                s
            })
            .add_edge("Prepare", "Charge");
        if let Some(policy) = policy {
            graph = graph.set_recovery_policy("Charge", policy);
        }
        graph.compile()
    }

    fn transient() -> NodeError {
        NodeError::Transient("gateway timeout".to_string())
    }

    #[tokio::test]
    async fn a_transient_failure_is_retried_until_it_succeeds() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let calls = Arc::new(AtomicU32::new(0));
        let graph = fallible_only_graph(calls.clone(), 2, transient(), RecoveryPolicy::retry(3));

        match graph
            .run("t", payment_state(), Arc::new(TestStore::default()))
            .await
        {
            GraphExecutionResult::Completed(state) => {
                assert!(state.charged);
                // Only the successful third attempt's work was committed.
                assert_eq!(state.notes, vec!["partial work in call 3"]);
            }
            _ => panic!("expected Completed"),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn exhausted_retries_route_to_the_fallback_with_the_pre_failure_state() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let calls = Arc::new(AtomicU32::new(0));
        let graph = payment_graph(
            calls.clone(),
            u32::MAX,
            transient(),
            Some(RecoveryPolicy::retry(2).with_fallback("Fallback")),
        )
        .unwrap();

        match graph
            .run("t", payment_state(), Arc::new(TestStore::default()))
            .await
        {
            GraphExecutionResult::Completed(state) => {
                // `Prepare` committed, `Charge`'s partial work was rolled back,
                // `Fallback` ran on what was left.
                assert!(state.prepared);
                assert!(!state.charged);
                assert_eq!(state.notes, vec!["fell back"]);
            }
            _ => panic!("expected Completed"),
        }
        // 1 initial attempt + 2 retries.
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn a_fatal_error_skips_retries_and_goes_straight_to_the_fallback() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let calls = Arc::new(AtomicU32::new(0));
        let graph = payment_graph(
            calls.clone(),
            u32::MAX,
            NodeError::Fatal("card declined".to_string()),
            Some(RecoveryPolicy::retry(5).with_fallback("Fallback")),
        )
        .unwrap();

        assert!(matches!(
            graph
                .run("t", payment_state(), Arc::new(TestStore::default()))
                .await,
            GraphExecutionResult::Completed(_)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn terminal_failure_without_a_fallback_persists_a_failed_checkpoint() {
        use std::sync::atomic::AtomicU32;
        let graph = fallible_only_graph(
            Arc::new(AtomicU32::new(0)),
            u32::MAX,
            transient(),
            RecoveryPolicy::retry(1),
        );
        let store = Arc::new(TestStore::default());

        match graph
            .run("thread_failed", payment_state(), store.clone())
            .await
        {
            GraphExecutionResult::Failed(StrataError::NodeFailed {
                node,
                attempts,
                message,
            }) => {
                assert_eq!(node, "Charge");
                assert_eq!(attempts, 2);
                assert_eq!(message, "gateway timeout");
            }
            _ => panic!("expected Failed(NodeFailed)"),
        }

        let persisted = store.load("thread_failed").unwrap().unwrap();
        assert_eq!(persisted.status, CheckpointStatus::Failed);
        assert_eq!(persisted.suspended_node.as_deref(), Some("Charge"));
        assert_eq!(persisted.next_node.as_deref(), Some("Charge"));
        // Rollback: the last committed state (after `Prepare`), not the
        // failed attempts' partial work.
        assert_eq!(
            persisted.frozen_state,
            serde_json::json!({ "prepared": true, "charged": false, "notes": [] })
        );
        let metadata = persisted.metadata.unwrap();
        assert_eq!(metadata["error"], "gateway timeout");
        assert_eq!(metadata["error_kind"], "transient");
        assert_eq!(metadata["attempts"], "2");
        assert!(persisted.resumed_at.is_none());
    }

    /// `Prepare` -> `Charge` with no fallback node at all, so the graph has
    /// no orphan whatever the policy is.
    fn fallible_only_graph(
        calls: Arc<std::sync::atomic::AtomicU32>,
        failures: u32,
        error: NodeError,
        policy: RecoveryPolicy,
    ) -> CompiledGraph<PaymentState> {
        use std::sync::atomic::Ordering;

        StateGraph::<PaymentState>::new()
            .add_node("Prepare", |s: PaymentState| PaymentState {
                prepared: true,
                ..s
            })
            .add_fallible_node("Charge", move |s: &PaymentState| {
                let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                let mut next = s.clone();
                next.charged = true;
                next.notes.push(format!("partial work in call {call}"));
                if call <= failures {
                    Err(error.clone())
                } else {
                    Ok(next)
                }
            })
            .add_edge("Prepare", "Charge")
            .set_recovery_policy("Charge", policy)
            .compile()
            .unwrap()
    }

    #[tokio::test]
    async fn a_fallible_node_with_the_default_policy_fails_on_the_first_error() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let calls = Arc::new(AtomicU32::new(0));
        let graph = fallible_only_graph(
            calls.clone(),
            u32::MAX,
            transient(),
            RecoveryPolicy::default(),
        );
        let store = Arc::new(TestStore::default());

        match graph.run("t", payment_state(), store.clone()).await {
            GraphExecutionResult::Failed(StrataError::NodeFailed { attempts, .. }) => {
                assert_eq!(attempts, 1);
            }
            _ => panic!("expected Failed(NodeFailed)"),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            store.load("t").unwrap().unwrap().status,
            CheckpointStatus::Failed
        );
    }

    #[tokio::test]
    async fn a_failed_checkpoint_resumes_from_the_node_that_failed_with_its_committed_state() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let calls = Arc::new(AtomicU32::new(0));
        // Only the first call to `Charge` fails; the resume is the second.
        let graph = fallible_only_graph(calls.clone(), 1, transient(), RecoveryPolicy::default());
        let store = Arc::new(TestStore::default());
        assert!(matches!(
            graph.run("t", payment_state(), store.clone()).await,
            GraphExecutionResult::Failed(StrataError::NodeFailed { .. })
        ));

        match graph
            .resume("t", HumanSignal::default(), store.clone())
            .await
        {
            GraphExecutionResult::Completed(state) => {
                // `Prepare` did not run again; nothing from the failed call leaked.
                assert!(state.prepared && state.charged);
                assert_eq!(state.notes, ["partial work in call 2"]);
            }
            _ => panic!("expected Completed"),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(store.load("t").unwrap().unwrap().resumed_at.is_some());
    }

    #[tokio::test]
    async fn resuming_a_failed_checkpoint_applies_the_field_overrides() {
        use std::sync::atomic::AtomicU32;
        let graph = fallible_only_graph(
            Arc::new(AtomicU32::new(0)),
            1,
            transient(),
            RecoveryPolicy::default(),
        );
        let store = Arc::new(TestStore::default());
        graph.run("t", payment_state(), store.clone()).await;

        let signal = HumanSignal {
            is_approved: true,
            reviewer_token: "op".to_string(),
            field_overrides: HashMap::from([("prepared".to_string(), "not-a-bool".to_string())]),
            ..Default::default()
        };
        // The override is a string in a bool field, so rehydrating refuses it:
        // proof the override reached the failed node's state.
        match graph.resume("t", signal, store.clone()).await {
            GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
                assert!(
                    message.contains("failed to rehydrate"),
                    "message was: {message}"
                );
            }
            _ => panic!("expected Failed(ExecutionAborted)"),
        }
    }

    #[tokio::test]
    async fn a_failed_checkpoint_can_only_be_resumed_once_per_failure() {
        use std::sync::atomic::AtomicU32;
        // The first two calls to `Charge` fail; the third succeeds.
        let graph = fallible_only_graph(
            Arc::new(AtomicU32::new(0)),
            2,
            transient(),
            RecoveryPolicy::default(),
        );
        let store = Arc::new(TestStore::default());
        graph.run("t", payment_state(), store.clone()).await;

        // The resume fails again, which writes a fresh `Failed` checkpoint...
        assert!(matches!(
            graph
                .resume("t", HumanSignal::default(), store.clone())
                .await,
            GraphExecutionResult::Failed(StrataError::NodeFailed { .. })
        ));
        let again = store.load("t").unwrap().unwrap();
        assert_eq!(again.status, CheckpointStatus::Failed);
        assert!(again.resumed_at.is_none());

        // ...which can be resumed in turn. Once that succeeds, the failure it
        // recovered from is spent.
        assert!(matches!(
            graph
                .resume("t", HumanSignal::default(), store.clone())
                .await,
            GraphExecutionResult::Completed(_)
        ));
        match graph
            .resume("t", HumanSignal::default(), store.clone())
            .await
        {
            GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
                assert!(
                    message.contains("already resumed"),
                    "message was: {message}"
                );
            }
            _ => panic!("expected Failed(ExecutionAborted)"),
        }
    }

    #[tokio::test]
    async fn a_completed_checkpoint_cannot_be_resumed() {
        use std::sync::atomic::AtomicU32;
        let graph = fallible_only_graph(
            Arc::new(AtomicU32::new(0)),
            0,
            transient(),
            RecoveryPolicy::default(),
        );
        let store = Arc::new(TestStore::default());
        store
            .save(Checkpoint {
                schema_version: schema::SCHEMA_VERSION.to_string(),
                thread_id: "t".to_string(),
                status: CheckpointStatus::Completed,
                suspended_node: None,
                next_node: None,
                frozen_state: serde_json::to_value(payment_state()).unwrap(),
                metadata: None,
                created_at: now_rfc3339(),
                resumed_at: None,
                nested: None,
                decision: None,
                wait: None,
                graph: None,
            })
            .unwrap();

        match graph
            .resume("t", HumanSignal::default(), store.clone())
            .await
        {
            GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => {
                assert!(message.contains("not resumable"), "message was: {message}");
            }
            _ => panic!("expected Failed(ExecutionAborted)"),
        }
    }

    #[test]
    fn compile_rejects_a_recovery_policy_on_a_node_that_is_not_fallible() {
        let result = StateGraph::<i32>::new()
            .add_node("A", |s: i32| s)
            .set_recovery_policy("A", RecoveryPolicy::retry(1))
            .compile();

        match result {
            Err(StrataError::CompilationFailed(message)) => {
                assert!(message.contains("'A'"), "message was: {message}");
            }
            _ => panic!("expected CompilationFailed"),
        }
    }

    #[test]
    fn compile_rejects_a_fallback_that_names_a_missing_node() {
        let result = StateGraph::<i32>::new()
            .add_fallible_node("A", |s: &i32| Ok(*s))
            .set_recovery_policy("A", RecoveryPolicy::retry(1).with_fallback("Nowhere"))
            .compile();

        match result {
            Err(StrataError::CompilationFailed(message)) => {
                assert!(message.contains("Nowhere"), "message was: {message}");
            }
            _ => panic!("expected CompilationFailed"),
        }
    }

    #[test]
    fn a_fallback_only_node_is_reachable_only_through_the_recovery_policy() {
        // `exhausted_retries_route_to_the_fallback...` already relies on
        // "Fallback" having no ordinary edge into it; assert the reverse too
        // — without the fallback policy the same node *is* an orphan.
        use crate::validate::ValidationErrorKind;
        use std::sync::atomic::AtomicU32;

        let with_fallback = payment_graph(
            Arc::new(AtomicU32::new(0)),
            0,
            transient(),
            Some(RecoveryPolicy::retry(1).with_fallback("Fallback")),
        );
        assert!(with_fallback.is_ok());

        match payment_graph(Arc::new(AtomicU32::new(0)), 0, transient(), None) {
            Err(StrataError::GraphInvalid(result)) => {
                assert!(
                    result
                        .errors
                        .iter()
                        .any(|e| e.kind == ValidationErrorKind::OrphanNode && e.node == "Fallback")
                );
            }
            _ => panic!("expected GraphInvalid"),
        }
    }

    #[tokio::test]
    async fn run_streaming_emits_an_error_chunk_when_a_fallible_node_gives_up() {
        use std::sync::atomic::AtomicU32;
        let graph = Arc::new(
            payment_graph(
                Arc::new(AtomicU32::new(0)),
                u32::MAX,
                transient(),
                Some(RecoveryPolicy::retry(1).with_fallback("Fallback")),
            )
            .unwrap(),
        );
        let store: Arc<dyn CheckpointStore> = Arc::new(TestStore::default());

        let (handle, mut receiver) = graph.run_streaming("t", payment_state(), store);
        let mut errors = Vec::new();
        while let Some(chunk) = receiver.recv().await {
            if chunk.chunk_type == StreamChunkType::Error {
                errors.push((chunk.node_name.to_string(), chunk.content.to_string()));
            }
        }

        assert!(matches!(
            handle.await.unwrap(),
            GraphExecutionResult::Completed(_)
        ));
        assert_eq!(
            errors,
            vec![("Charge".to_string(), "gateway timeout".to_string())]
        );
    }
}
