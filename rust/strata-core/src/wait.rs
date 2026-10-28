//! Event waits and durable timers (`design/29-Event-Waits-And-Timers.md`,
//! SG-16): the options the builders take, the signal that answers an event
//! wait, and what [`CompiledGraph::wake_due`](crate::graph::CompiledGraph::wake_due)
//! reports.
//!
//! A wait is an interrupt boundary with a wake condition: the thread
//! checkpoints as `interrupted` with a [`CheckpointWait`], holds nothing while
//! it waits, and leaves the wait through a resume, as every interrupted thread
//! does. [`StateGraph::add_wait_for_event`](crate::graph::StateGraph::add_wait_for_event)
//! and [`StateGraph::add_timer`](crate::graph::StateGraph::add_timer) add wait
//! nodes; [`StateGraph::add_interrupt_boundary_with`](crate::graph::StateGraph::add_interrupt_boundary_with)
//! gives a human boundary a timeout.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Timelike, Utc};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::checkpoint::GraphExecutionResult;
use crate::context::RunConfig;
use crate::schema::{CheckpointWait, GraphWaitDef, WaitKind};
use crate::store::{DEFAULT_PAGE_SIZE, timestamp_text};

/// What woke a waiting thread (`design/29-Event-Waits-And-Timers.md`): the
/// `wake` of a `Resumed` event, and the reason
/// [`CompiledGraph::wake_due`](crate::graph::CompiledGraph::wake_due) reports.
/// Mirrors .NET's `WakeReason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum WakeReason {
    /// A human resume (`resume`), of a waiting, failed or cancelled thread.
    #[default]
    Human,
    /// A signal of the event the thread waited for (`signal`).
    Event,
    /// Its timer was due (`wake_due`).
    Timer,
    /// Its wait timed out (`wake_due`): it continued at the wait's timeout
    /// route, or was ended.
    Timeout,
}

impl WakeReason {
    /// The wire name: `human`, `event`, `timer` or `timeout`.
    pub fn as_str(self) -> &'static str {
        match self {
            WakeReason::Human => "human",
            WakeReason::Event => "event",
            WakeReason::Timer => "timer",
            WakeReason::Timeout => "timeout",
        }
    }
}

/// An external event for a thread waiting for it
/// (`design/29-Event-Waits-And-Timers.md`), passed to
/// [`CompiledGraph::signal`](crate::graph::CompiledGraph::signal). Its
/// `overrides` replace top-level fields of the waiting level's state, as a
/// [`HumanSignal`](crate::checkpoint::HumanSignal)'s typed overrides do, so the
/// payload is in the state every later node and checkpoint carries. It records
/// no decision. Mirrors .NET's `EventSignal`.
#[derive(Debug, Clone, Default)]
pub struct EventSignal {
    /// The event, e.g. `deployment.finished`: it must be the one the thread
    /// waits for.
    pub event_type: String,
    /// The payload, by top-level field of the state, with its JSON type. A
    /// JSON null sets the field to null.
    pub overrides: HashMap<String, Value>,
}

impl EventSignal {
    /// A signal of `event_type`, with no payload.
    pub fn new(event_type: impl Into<String>) -> Self {
        Self {
            event_type: event_type.into(),
            overrides: HashMap::new(),
        }
    }

    /// This signal with one more override (replacing any for `key`).
    pub fn with_override(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.overrides.insert(key.into(), value.into());
        self
    }
}

/// How an interrupt boundary waits for its human
/// (`design/21-Typed-Human-Signals.md`, `design/29-Event-Waits-And-Timers.md`),
/// for [`StateGraph::add_interrupt_boundary_with`](crate::graph::StateGraph::add_interrupt_boundary_with):
/// a reject route, and a timeout with an optional route. `.compile()` refuses
/// a zero timeout, and a timeout route with no timeout.
#[derive(Debug, Clone, Default)]
pub struct BoundaryOptions {
    pub(crate) on_reject: Option<String>,
    pub(crate) timeout: Option<Duration>,
    pub(crate) on_timeout: Option<String>,
}

impl BoundaryOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// A resume that rejects continues at `node` instead of the next node.
    pub fn with_reject(mut self, node: impl Into<String>) -> Self {
        self.on_reject = Some(node.into());
        self
    }

    /// Wait for the human at most `timeout`: once it has passed,
    /// [`CompiledGraph::wake_due`](crate::graph::CompiledGraph::wake_due) times
    /// the boundary out, to the timeout route, or, with none, ending the thread
    /// with `strata.end_reason` `timed_out`.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Where a timeout continues: a guarded `on_timeout` edge.
    pub fn with_on_timeout(mut self, node: impl Into<String>) -> Self {
        self.on_timeout = Some(node.into());
        self
    }
}

/// How an event wait times out (`design/29-Event-Waits-And-Timers.md`), for
/// [`StateGraph::add_wait_for_event_with`](crate::graph::StateGraph::add_wait_for_event_with).
/// `.compile()` refuses a zero timeout, and a timeout route with no timeout.
#[derive(Debug, Clone, Default)]
pub struct WaitOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) on_timeout: Option<String>,
}

impl WaitOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Wait for the event at most `timeout`.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Where a timeout continues: a guarded `on_timeout` edge. Without one, a
    /// timeout ends the thread.
    pub fn with_on_timeout(mut self, node: impl Into<String>) -> Self {
        self.on_timeout = Some(node.into());
        self
    }
}

/// A timer's wake time computed from the state.
pub(crate) type WakeAtFn<S> = Arc<dyn Fn(&S) -> DateTime<Utc> + Send + Sync>;

/// A wait node as the builder declared it: an event wait, with its event type
/// and optional timeout, or a timer, with a fixed delay or a time computed
/// from the state.
pub(crate) struct WaitSpec<S> {
    pub(crate) kind: WaitKind,
    pub(crate) event_type: Option<String>,
    pub(crate) delay: Option<Duration>,
    pub(crate) wake_at: Option<WakeAtFn<S>>,
    pub(crate) timeout: Option<Duration>,
}

impl<S> Clone for WaitSpec<S> {
    fn clone(&self) -> Self {
        Self {
            kind: self.kind,
            event_type: self.event_type.clone(),
            delay: self.delay,
            wake_at: self.wake_at.clone(),
            timeout: self.timeout,
        }
    }
}

impl<S> WaitSpec<S> {
    /// The checkpoint `wait` of a run frozen at this node with `state` at `now`.
    pub(crate) fn at(&self, state: &S, now: DateTime<Utc>) -> CheckpointWait {
        match self.kind {
            WaitKind::Timer => CheckpointWait {
                kind: WaitKind::Timer,
                event_type: None,
                wake_at: Some(wake_text(match &self.wake_at {
                    Some(at) => at(state),
                    None => now + self.delay.unwrap_or_default(),
                })),
            },
            _ => CheckpointWait {
                kind: WaitKind::Event,
                event_type: self.event_type.clone(),
                wake_at: self.timeout.map(|timeout| wake_text(now + timeout)),
            },
        }
    }

    /// This wait as the exported definition describes it.
    pub(crate) fn exported(&self) -> GraphWaitDef {
        GraphWaitDef {
            kind: self.kind,
            event_type: self.event_type.clone(),
            delay_ms: self.delay.map(|delay| delay.as_millis() as u64),
            timeout_ms: self.timeout.map(|timeout| timeout.as_millis() as u64),
        }
    }
}

/// `time` rounded **up** to a whole second, as stored text, so a timer never
/// fires early: `12:00:01.2` is written `12:00:02`
/// (`design/29-Event-Waits-And-Timers.md`, "Precision").
pub(crate) fn wake_text(time: DateTime<Utc>) -> String {
    let floored = time.with_nanosecond(0).unwrap_or(time);
    timestamp_text(if floored == time {
        floored
    } else {
        floored + chrono::Duration::seconds(1)
    })
}

/// What [`CompiledGraph::wake_due_with`](crate::graph::CompiledGraph::wake_due_with)
/// wakes, and how it runs them (`design/29-Event-Waits-And-Timers.md`). Mirrors
/// .NET's `WakeOptions`.
#[derive(Clone)]
pub struct WakeOptions {
    pub(crate) metadata: BTreeMap<String, String>,
    pub(crate) limit: usize,
    pub(crate) config: RunConfig,
    pub(crate) match_graph: bool,
    pub(crate) stop: Option<CancellationToken>,
}

impl Default for WakeOptions {
    fn default() -> Self {
        Self {
            metadata: BTreeMap::new(),
            limit: DEFAULT_PAGE_SIZE,
            config: RunConfig::default(),
            match_graph: true,
            stop: None,
        }
    }
}

impl WakeOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Only threads whose latest checkpoint's metadata has this pair. A graph
    /// with an identity lists only its own threads anyway
    /// ([`Self::with_match_graph`]); a graph without one should put its name
    /// in every run's metadata (`graph=investigation`) and wake with the same
    /// pair, since a store holds many graphs' threads.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// The most threads one call wakes or skips: 1 to
    /// [`MAX_PAGE_SIZE`](crate::store::MAX_PAGE_SIZE).
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// The config of every run the call starts (metadata, observer, history,
    /// services, hop limit, cancellation).
    pub fn with_config(mut self, config: RunConfig) -> Self {
        self.config = config;
        self
    }

    /// Whether a graph with an identity lists only the threads it wrote
    /// (`design/31-Graph-Identity.md`): its id, and its version unless it has
    /// a migration. `true` by default. `false` lists by
    /// [`Self::with_metadata`] alone, as before identities, which also wakes
    /// the threads written before the graph had one; another graph's threads
    /// are still skipped. A graph without an identity ignores it.
    pub fn with_match_graph(mut self, match_graph: bool) -> Self {
        self.match_graph = match_graph;
        self
    }

    /// When `stop` fires, the call wakes no more threads and returns what it
    /// did; the thread it is waking finishes. Unlike the config's cancellation
    /// token, which also cancels that run. How a host stops a driver without
    /// leaving a thread cancelled (`design/35-Hosting-Integration.md`,
    /// "Stopping").
    pub fn with_stop(mut self, stop: CancellationToken) -> Self {
        self.stop = Some(stop);
        self
    }
}

/// A thread `wake_due` woke, and what its run returned: the resumed run's
/// result, or, for a timeout with no route, the end's (`Cancelled` with
/// [`CancelReason::Ended`](crate::checkpoint::CancelReason::Ended)).
pub struct WokenThread<S> {
    pub thread_id: String,
    /// [`WakeReason::Timer`] or [`WakeReason::Timeout`].
    pub reason: WakeReason,
    pub result: GraphExecutionResult<S>,
}

/// A due thread `wake_due` left alone, with why: it stopped being due, another
/// driver got there first, or its wait isn't one of this graph's. Nothing was
/// written to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedThread {
    pub thread_id: String,
    pub reason: String,
}

/// What one `wake_due` call did, in the order it did it.
pub struct WakeReport<S> {
    pub woken: Vec<WokenThread<S>>,
    pub skipped: Vec<SkippedThread>,
}
