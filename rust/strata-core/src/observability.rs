//! Run metadata, run events and telemetry (`design/18-Run-Observability.md`:
//! SG-14, SG-20, SG-19).
//!
//! - **Events:** a [`RunObserver`] set with
//!   [`RunConfig::with_observer`](crate::context::RunConfig::with_observer)
//!   receives every [`RunEvent`] of a run, in every run mode. Delivery is off
//!   the executor, through an unbounded per-run queue, so an observer can't
//!   stall the run, and a panic in it is caught and counted.
//! - **Spans:** `tracing` spans `strata.run` → `strata.node` → `strata.attempt`,
//!   with `strata.branch` under a group's attempt, named for OpenTelemetry
//!   through `otel.name`. A handler runs inside its attempt's span, and a tool
//!   call a [`ToolRegistry`](crate::tools::ToolRegistry) dispatches is a
//!   `strata.tool` span under it (`design/20-Tool-Dispatch-And-Registry.md`).
//! - **Metrics:** through the `metrics` facade, under the names in
//!   [`metric_names`].
//!
//! One internal per-run [`RunTelemetry`] drives all three from the same call
//! at each lifecycle point, so they can't disagree. Mirrors the .NET engine's
//! `Datalium.Strata.Observability`.

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Once};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use tracing::Span;
use tracing::field::Empty;

use crate::StrataError;
use crate::checkpoint::{CancelReason, GraphExecutionResult, LevelSignals};
use crate::graph::NodeError;
use crate::schema::{CheckpointStatus, CheckpointWait, GraphIdentity};
use crate::store::ThreadLease;
use crate::tools::ToolCallContext;
use crate::wait::WakeReason;

/// Whether a run started from the entry point, resumed a checkpoint, or ended
/// a thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunMode {
    Run,
    Resume,
    /// [`crate::graph::CompiledGraph::end`] (`design/28-Ending-A-Thread.md`):
    /// no node runs.
    End,
    /// [`crate::graph::CompiledGraph::recover`] /
    /// [`recover_stale`](crate::graph::CompiledGraph::recover_stale)
    /// (`design/33-Durable-Execution.md`): a running thread whose run died
    /// continues where its last checkpoint says.
    Recover,
}

/// How a run ended: the kind of its [`GraphExecutionResult`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    Completed,
    Interrupted,
    Failed,
    Cancelled,
}

/// How the engine finished with a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeResult {
    /// The node completed and its state was committed.
    Committed,
    /// Its policy had nothing left to try, it was refused, or it panicked.
    Failed,
    /// A subgraph node whose child froze at an interrupt boundary.
    Interrupted,
    /// The run was cancelled while the node ran.
    Cancelled,
}

/// How a parallel group's branch ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchResult {
    Succeeded,
    Failed,
    /// The group gave up on it (a sibling failed, it lost a race, the run was
    /// cancelled, the group timed out).
    Cancelled,
}

/// Why a branch ended as it did, when the result alone doesn't say
/// (`design/26-Parallel-Group-Controls-And-Joins.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchEndReason {
    /// On a `Failed` branch: it ran over its group's branch timeout.
    TimedOut,
    /// On a `Cancelled` branch: its group's join was met without it.
    LostRace,
    /// On a `Cancelled` branch: its group failed because of a sibling and gave
    /// up on it.
    SiblingFailed,
}

impl BranchEndReason {
    /// The reason as span attributes write it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TimedOut => "timed_out",
            Self::LostRace => "lost_race",
            Self::SiblingFailed => "sibling_failed",
        }
    }
}

/// How a tool call a [`ToolRegistry`](crate::tools::ToolRegistry) dispatched ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolCallResult {
    Succeeded,
    /// The tool returned an error, or its arguments didn't parse.
    Failed,
    /// Its token was cancelled before it started, it failed after the token
    /// fired, or the dispatch future was dropped.
    Cancelled,
    /// No such tool, or one the registry's subset doesn't permit.
    Refused,
}

/// How a model call ([`ModelCallScope`]) ended (`design/44-LLM-Integration.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelCallResult {
    Succeeded,
    /// The provider refused the request or failed, or the call couldn't be made.
    Failed,
    /// The caller's token stopped it, or its future was dropped.
    Cancelled,
}

/// What became of one candidate of a model route in one call through it
/// (`design/46-Model-Routes.md`), as [`RunEventKind::ModelRouted`] reports it.
/// Mirrors the .NET engine's `ModelRouteOutcome`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ModelRouteOutcome {
    /// It answered, and the answer was accepted: the route's answer.
    Answered,
    /// It was asked and failed: a transient fault the route moved on from, a
    /// fatal one that stopped it, or a stream that broke after its first update.
    Failed,
    /// It refused (finish reason `content_filter`).
    Refused,
    /// It answered, and the route's `accept` rejected the answer.
    Rejected,
    /// Its eligibility check turned the request down; it wasn't asked.
    Ineligible,
    /// Its health key had a `Retry-After` in force; it wasn't asked.
    CoolingDown,
    /// Its health key's breaker was open (or half-open with its one test
    /// call already out); it wasn't asked.
    BreakerOpen,
    /// It was asked, and another candidate answered first (hedging): its call
    /// was cancelled.
    LostRace,
    /// The route never reached it.
    NotTried,
    /// It was asked, and the call through the route was cancelled before it
    /// answered, or a fatal fault elsewhere ended the route.
    Cancelled,
}

impl ModelRouteOutcome {
    /// The outcome as a label: `answered`, `failed`, `refused`, `rejected`,
    /// `ineligible`, `cooling_down`, `breaker_open`, `lost_race`, `not_tried`
    /// or `cancelled`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Answered => "answered",
            Self::Failed => "failed",
            Self::Refused => "refused",
            Self::Rejected => "rejected",
            Self::Ineligible => "ineligible",
            Self::CoolingDown => "cooling_down",
            Self::BreakerOpen => "breaker_open",
            Self::LostRace => "lost_race",
            Self::NotTried => "not_tried",
            Self::Cancelled => "cancelled",
        }
    }
}

/// One candidate's part in a call through a model route
/// (`design/46-Model-Routes.md`): its name, what became of it, why (a
/// failure's error type, a rejection's or ineligibility's reason, how long a
/// cooldown or breaker had left), and, when it was asked, the `call_id` of its
/// own `ModelCallStarted` / `ModelCallCompleted`. Mirrors the .NET engine's
/// `ModelRouteAttempt`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ModelRouteAttempt {
    pub candidate: String,
    pub outcome: ModelRouteOutcome,
    pub reason: Option<String>,
    pub call_id: Option<String>,
}

impl ModelRouteAttempt {
    pub fn new(candidate: impl Into<String>, outcome: ModelRouteOutcome) -> Self {
        Self {
            candidate: candidate.into(),
            outcome,
            reason: None,
            call_id: None,
        }
    }

    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    pub fn with_call_id(mut self, call_id: impl Into<String>) -> Self {
        self.call_id = Some(call_id.into());
        self
    }
}

/// The tokens a model call used, as its provider reported them
/// (`design/44-LLM-Integration.md`); a count the provider didn't report is
/// `None`, never estimated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ModelUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

impl ModelUsage {
    pub fn new(input_tokens: Option<u64>, output_tokens: Option<u64>) -> Self {
        Self {
            input_tokens,
            output_tokens,
        }
    }

    /// Both counts, when either is known.
    pub fn total_tokens(&self) -> Option<u64> {
        match (self.input_tokens, self.output_tokens) {
            (None, None) => None,
            (input, output) => Some(input.unwrap_or(0) + output.unwrap_or(0)),
        }
    }
}

/// Something that happened during a run, as a [`RunObserver`] sees it.
/// Mirrors the .NET engine's `RunEvent` records.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct RunEvent {
    /// The caller's thread id, at every depth.
    pub thread_id: Arc<str>,
    /// The id of the `run` / `resume` call this event belongs to.
    pub run_id: Arc<str>,
    /// 1, 2, 3, … per run, with no gaps. Events are delivered in this order.
    pub sequence: u64,
    /// When the engine recorded the event.
    pub timestamp: DateTime<Utc>,
    /// The node's qualified path (`"Work/Send"`, `"Gather/Acme"`), as
    /// [`crate::context::NodeContext::node_path`] gives it; `None` on
    /// run-level events.
    pub node_path: Option<Arc<str>>,
    /// The node's attempt at that moment (a branch reports its group's);
    /// `None` on run-level events.
    pub attempt: Option<u32>,
    pub kind: RunEventKind,
}

/// What a [`RunEvent`] reports. Non-exhaustive: later versions add kinds, so
/// a `match` needs a wildcard arm.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum RunEventKind {
    /// The first event of every run. For a resume it comes after the
    /// checkpoint is loaded: `node` is the checkpoint's next node, and
    /// `metadata` the stored metadata merged with the resume's. `graph` is the
    /// identity of the graph running (`design/31-Graph-Identity.md`), `None`
    /// when it has none: for a resume, the graph resuming, which after a
    /// migration is not the version that wrote the thread.
    RunStarted {
        mode: RunMode,
        node: String,
        metadata: HashMap<String, String>,
        graph: Option<GraphIdentity>,
        /// How long the run waited for its scheduler's slot
        /// (`design/40-Run-Scheduler.md`), `None` when it has no scheduler.
        queued_for: Option<Duration>,
    },
    /// A resume, after the checkpoint it resumes was stamped `resumed_at`.
    /// `is_approved` and `reviewer` are the innermost signal's: what the person
    /// resuming said, and who they are (on a failed or cancelled thread's
    /// resume, the operator, while `NodeContext::decision` keeps whoever
    /// answered the boundary). `rejected_to` is the reject route this resume
    /// took, if it took one (`design/21-Typed-Human-Signals.md`). Overrides
    /// are not included, like state.
    ///
    /// `wake` is what woke the thread (`design/29-Event-Waits-And-Timers.md`):
    /// [`WakeReason::Human`] for `resume`, [`WakeReason::Event`] for a signal
    /// (with its `event_type`), [`WakeReason::Timer`] or
    /// [`WakeReason::Timeout`] for `wake_due` (with the timeout route taken,
    /// `timed_out_to`). For anything but a human, `is_approved` is `true` and
    /// `reviewer` is empty.
    Resumed {
        from: CheckpointStatus,
        waiting_at: Vec<String>,
        is_approved: bool,
        reviewer: String,
        rejected_to: Option<String>,
        wake: WakeReason,
        event_type: Option<String>,
        timed_out_to: Option<String>,
    },
    /// A recovery, after its claim was saved
    /// (`design/33-Durable-Execution.md`): the thread's `running` checkpoint
    /// written again by this run, which nobody else had changed. Not recorded
    /// for a recovery that was refused. `at` is where the thread continues,
    /// one node per level from the outermost graph in: each level's next
    /// node, the innermost being the node that was in flight when the process
    /// died, which runs again. `recoveries` is how many times the thread has
    /// been recovered from this checkpoint, this one included: recoveries
    /// since a node last committed.
    Recovered { at: Vec<String>, recoveries: u32 },
    /// A node is about to run its first attempt.
    NodeStarted,
    /// An attempt failed transiently and the node will run again; `attempt`
    /// is the one that failed. `delay` is the wait before the next attempt:
    /// the policy's backoff delay, or the failure's capped retry hint; zero
    /// when the policy has no backoff (`design/25-Retry-Backoff.md`).
    RetryScheduled {
        error: String,
        timed_out: bool,
        delay: Duration,
    },
    /// A parallel group launched a branch.
    BranchStarted,
    /// A branch settled, or its group gave up on it. `reason` says why when
    /// the result alone doesn't (`design/26-Parallel-Group-Controls-And-Joins.md`):
    /// a branch timeout, a lost race, or a sibling's failure; `None` for a
    /// success, an ordinary failure, and a branch stopped by a cancelled run or
    /// its group's own timeout.
    BranchCompleted {
        result: BranchResult,
        duration: Duration,
        error: Option<String>,
        reason: Option<BranchEndReason>,
    },
    /// A map read its items, once per attempt, at the map's path, after their
    /// keys were checked and before any item started
    /// (`design/27-Map-Fan-Out.md`). Each item then reports as a branch, at
    /// `"<map>/<key>"`, and its child's nodes at `"<map>/<key>/<node>"`.
    MapExpanded {
        /// The items' keys, in list order; empty for an empty list.
        keys: Vec<String>,
    },
    /// A parallel group whose join isn't `All` decided it, once per attempt,
    /// at the group's path (`design/26-Parallel-Group-Controls-And-Joins.md`).
    /// It comes after the deciding branch's `BranchCompleted` and before the
    /// losers'.
    JoinDecided {
        /// The join: `first_success`, `first_matching` or `quorum`.
        policy: String,
        /// Whether it was met; `false` when it could no longer be.
        met: bool,
        /// The branches that met it, in the order they did; empty when unmet.
        deciding: Vec<String>,
    },
    /// The engine is finished with a node, however it ended. Every
    /// `NodeStarted` gets exactly one. `attempt` is the last attempt made, and
    /// `duration` covers every attempt.
    NodeCompleted {
        result: NodeResult,
        duration: Duration,
        error: Option<String>,
        /// The node the policy falls back to, when it does.
        fallback: Option<String>,
    },
    /// A checkpoint was written to the caller's store. `resume_stamp` is a
    /// resume stamping the old checkpoint `resumed_at`.
    CheckpointSaved {
        status: CheckpointStatus,
        waiting_at: Vec<String>,
        resume_stamp: bool,
        duration: Duration,
    },
    /// The run froze at an interrupt boundary, and the checkpoint is saved.
    /// `next_node` is where a resume continues (for a nested freeze, the
    /// subgraph node). `wait` is what the innermost level waits for, when it
    /// is more than a human with no timeout
    /// (`design/29-Event-Waits-And-Timers.md`).
    Interrupted {
        waiting_at: Vec<String>,
        next_node: String,
        wait: Option<CheckpointWait>,
    },
    /// A [`ToolRegistry`](crate::tools::ToolRegistry) received a tool call
    /// whose context came from a node (`ToolCallContext::from`), before
    /// resolving its name. `node_path` and `attempt` are the calling node's.
    /// The arguments are not included: they can hold secrets.
    ToolCallStarted {
        tool: String,
        call_id: String,
        caller_id: Option<String>,
    },
    /// That call ended, however it ended. Every `ToolCallStarted` gets exactly
    /// one. The result is not included.
    ToolCallCompleted {
        tool: String,
        call_id: String,
        result: ToolCallResult,
        duration: Duration,
        error: Option<String>,
    },
    /// A node made a model call (`design/44-LLM-Integration.md`), recorded
    /// through a [`ModelCallScope`] started with the node's context.
    /// `node_path` and `attempt` are the calling node's. `model` is the model
    /// asked for; `call_id` pairs it with its `ModelCallCompleted` (a UUID v7);
    /// `provider` is the client's, when it says. `route` is the model route the
    /// call is an attempt of (`design/46-Model-Routes.md`), `None` outside
    /// one. `operation` is the GenAI operation ([`ModelCallInfo::operation`]):
    /// `chat`, or `embeddings` for an embedding call
    /// (`design/45-Long-Term-Memory.md`). The prompt is not included.
    ModelCallStarted {
        model: String,
        call_id: String,
        provider: Option<String>,
        route: Option<String>,
        operation: String,
    },
    /// That call ended, however it ended. Every `ModelCallStarted` gets
    /// exactly one. `finish_reason` is why the model stopped (`stop`,
    /// `length`, `tool_calls`, …) when it succeeded; `error` the message when
    /// it failed; `route` and `operation` as on `ModelCallStarted`. The answer
    /// is not included.
    ModelCallCompleted {
        model: String,
        call_id: String,
        result: ModelCallResult,
        usage: ModelUsage,
        finish_reason: Option<String>,
        duration: Duration,
        error: Option<String>,
        route: Option<String>,
        operation: String,
    },
    /// A call through a model route ended (`design/46-Model-Routes.md`),
    /// recorded once, after its last attempt's `ModelCallCompleted`, through a
    /// [`ModelRouteScope`] started with the node's context. `call_id` is the
    /// route call's own (a UUID v7; each attempt's model call has its own,
    /// in `attempts`). `answered` is the candidate whose answer the route
    /// returned and `model` the model that gave it, both `None` when none
    /// did. `attempts` lists every candidate of the route once, in
    /// declaration order; `hedges` counts the attempts hedging started beside
    /// one still running.
    ModelRouted {
        route: String,
        call_id: String,
        answered: Option<String>,
        model: Option<String>,
        attempts: Vec<ModelRouteAttempt>,
        hedges: u32,
        duration: Duration,
    },
    /// The loop circuit breaker tripped at a level
    /// (`design/24-Loop-Circuit-Breaker.md`): `node_path` is the node that
    /// would have been hop `hops + 1`, which never started, and there is no
    /// `attempt`. Recorded at whichever level tripped, so a subgraph's child's
    /// is visible too.
    HopLimitExceeded {
        /// The hops the level had made.
        hops: u32,
        /// The limit in effect at that level.
        limit: u32,
    },
    /// The run acquired its thread's lease
    /// ([`RunConfig::with_lease`](crate::context::RunConfig::with_lease),
    /// `design/32-Thread-Leases.md`), before its first save. Not recorded
    /// when the thread is held: that run fails.
    LeaseAcquired {
        owner: String,
        /// The lease's fencing token: the thread's count of acquisitions.
        token: i64,
        /// When it expires unless renewed, on the store's clock.
        expires_at: DateTime<Utc>,
    },
    /// The run lost its thread's lease, once, when it found out: a renewal or
    /// a save was refused, or no renewal succeeded for a whole ttl. The run
    /// stops and writes nothing more; `RunCompleted` follows with
    /// [`CancelReason::LeaseLost`].
    LeaseLost { owner: String, token: i64 },
    /// The last event of every run.
    RunCompleted {
        outcome: RunOutcome,
        duration: Duration,
        error: Option<String>,
        cancel_reason: Option<CancelReason>,
    },
}

/// Receives a run's events, set with
/// [`RunConfig::with_observer`](crate::context::RunConfig::with_observer).
/// Mirrors the .NET engine's `IRunObserver`.
///
/// `on_event` is called once per event, in sequence order, never concurrently
/// for one run, and never on the executor: a slow observer only grows its own
/// queue, and a panic in it is caught and counted (`strata.observer.errors`),
/// so neither can stall or fail the run. It is called on a tokio task, so it
/// must not block: hand slow work to a channel ([`RunEventChannel`]). The run
/// doesn't wait for delivery: `RunCompleted` may arrive after `run` has returned.
pub trait RunObserver: Send + Sync + 'static {
    fn on_event(&self, event: &RunEvent);
}

/// An observer for one run that forwards its events to a receiver, which ends
/// after `RunCompleted`: `while let Some(event) = events.recv().await` ends
/// with the run. Events from a second run are refused (a panic, counted as an
/// observer error).
pub struct RunEventChannel {
    state: Mutex<ChannelState>,
}

struct ChannelState {
    sender: Option<mpsc::UnboundedSender<RunEvent>>,
    run_id: Option<Arc<str>>,
}

impl RunEventChannel {
    /// The observer, and the receiver its events arrive on.
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> (Arc<RunEventChannel>, mpsc::UnboundedReceiver<RunEvent>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let channel = RunEventChannel {
            state: Mutex::new(ChannelState {
                sender: Some(sender),
                run_id: None,
            }),
        };
        (Arc::new(channel), receiver)
    }
}

impl RunObserver for RunEventChannel {
    fn on_event(&self, event: &RunEvent) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let run_id = state
            .run_id
            .get_or_insert_with(|| event.run_id.clone())
            .clone();
        if run_id != event.run_id {
            drop(state);
            panic!(
                "a RunEventChannel serves one run: it is observing '{run_id}', not '{}'",
                event.run_id
            );
        }
        if let Some(sender) = &state.sender {
            let _ = sender.send(event.clone());
        }
        if matches!(event.kind, RunEventKind::RunCompleted { .. }) {
            state.sender = None;
        }
    }
}

/// The names of the engine's metrics (`design/18-Run-Observability.md`,
/// "Metrics"): the same names and units as the .NET engine's `Meter`. How they
/// are rendered (for Prometheus, say) depends on the recorder installed.
pub mod metric_names {
    /// Counter: runs and resumes started, by `strata.run.mode`.
    pub const GRAPHS_STARTED: &str = "strata.graphs.started";
    /// Gauge (an up-down counter): runs and resumes executing in this process.
    pub const GRAPHS_ACTIVE: &str = "strata.graphs.active";
    /// Histogram, seconds: a run's duration, by `strata.run.mode` and `strata.outcome`.
    pub const GRAPH_DURATION: &str = "strata.graph.duration";
    /// Histogram, seconds: a node's duration, every attempt included, by
    /// `strata.node.path` and `strata.node.result`.
    pub const NODE_DURATION: &str = "strata.node.duration";
    /// Counter: attempts retried, by `strata.node.path` and `error.type`.
    pub const NODE_RETRIES: &str = "strata.node.retries";
    /// Counter: edges followed, by `strata.node.path` and `strata.next_node.path`.
    pub const GRAPH_TRANSITION: &str = "strata.graph.transition";
    /// Counter: runs a graph level's hop limit stopped, by `strata.node.path`
    /// (the node refused; `design/24-Loop-Circuit-Breaker.md`).
    pub const HOP_LIMIT_EXCEEDED: &str = "strata.graph.hop_limit_exceeded";
    /// Histogram, seconds: checkpoint saves and loads, by
    /// `strata.checkpoint.operation` and `strata.checkpoint.status`.
    pub const CHECKPOINT_LATENCY: &str = "strata.checkpoint.latency";
    /// Counter: observer panics, which never fail the run.
    pub const OBSERVER_ERRORS: &str = "strata.observer.errors";
    /// Counter: tool calls a registry dispatched, by `gen_ai.tool.name` (left
    /// off for a name the registry doesn't know) and `strata.tool.result`.
    pub const TOOL_REQUESTS: &str = "strata.tool.requests";
    /// Histogram, seconds: a tool call's duration, by `gen_ai.tool.name` and
    /// `strata.tool.result`.
    pub const TOOL_DURATION: &str = "strata.tool.duration";
    /// Counter: waiting threads a signal or the wake driver woke or ended, by
    /// `strata.wake.reason` (`design/29-Event-Waits-And-Timers.md`).
    pub const THREAD_WAKES: &str = "strata.thread.wakes";
    /// Counter: thread leases a run tried to acquire, by
    /// `strata.lease.outcome` (`acquired` or `held`;
    /// `design/32-Thread-Leases.md`).
    pub const LEASE_ACQUISITIONS: &str = "strata.lease.acquisitions";
    /// Counter: runs that lost their thread's lease and stopped.
    pub const LEASE_LOSSES: &str = "strata.lease.losses";
    /// Counter: running threads a recovery claimed and continued
    /// (`design/33-Durable-Execution.md`).
    pub const THREAD_RECOVERIES: &str = "strata.thread.recoveries";
    /// Counter: runs a scheduler admitted or refused, by
    /// `strata.scheduler.name`, `strata.scheduler.outcome` and
    /// `strata.run.priority` (`design/40-Run-Scheduler.md`).
    pub const SCHEDULER_ADMISSIONS: &str = "strata.scheduler.admissions";
    /// Histogram, seconds: how long a run waited for a scheduler's slot,
    /// admitted or not, by `strata.scheduler.name` and `strata.scheduler.outcome`.
    pub const SCHEDULER_WAIT: &str = "strata.scheduler.wait";
    /// Gauge (an up-down counter): runs waiting for a scheduler's slot, by
    /// `strata.scheduler.name`.
    pub const SCHEDULER_QUEUED: &str = "strata.scheduler.queued";
    /// Gauge (an up-down counter): runs holding a scheduler's slot, by
    /// `strata.scheduler.name`.
    pub const SCHEDULER_ACTIVE: &str = "strata.scheduler.active";
    /// Histogram, tokens: a model call's usage, by `gen_ai.token.type`
    /// (`input`/`output`), `gen_ai.operation.name`, `gen_ai.provider.name`,
    /// `gen_ai.request.model` and `gen_ai.response.model`
    /// (`design/44-LLM-Integration.md`; the OpenTelemetry GenAI name).
    pub const MODEL_TOKEN_USAGE: &str = "gen_ai.client.token.usage";
    /// Histogram, seconds: a model call's duration, by the same labels plus
    /// `error.type` (the OpenTelemetry GenAI name).
    pub const MODEL_DURATION: &str = "gen_ai.client.operation.duration";
    /// Histogram, seconds: how long a streamed model call took to send its
    /// first update.
    pub const MODEL_TIME_TO_FIRST_CHUNK: &str = "strata.model.time_to_first_chunk";
    /// Counter: calls through a model route, by `strata.route` and
    /// `strata.outcome` (`answered`, `exhausted`, `ineligible`, `fatal`,
    /// `failed` for a committed stream that broke or whose answer was refused
    /// or rejected, `cancelled`; `design/46-Model-Routes.md`).
    pub const ROUTE_CALLS: &str = "strata.route.calls";
    /// Counter: candidates a route moved past, by `strata.route` and
    /// `strata.route.reason` (`failed`, `refused`, `rejected`, `cooling_down`,
    /// `breaker_open`, `ineligible`).
    pub const ROUTE_FAILOVERS: &str = "strata.route.failovers";
    /// Counter: attempts hedging started beside one still running, by
    /// `strata.route`.
    pub const ROUTE_HEDGES: &str = "strata.route.hedges";
    /// Counter: a model health key's breaker changing state, by
    /// `strata.model.health_key` and `strata.model.breaker.state` (`open`,
    /// `half_open`, `closed`).
    pub const MODEL_BREAKER_TRANSITIONS: &str = "strata.model.breaker.transitions";
}

pub(crate) fn describe_metrics() {
    static DESCRIBED: Once = Once::new();
    DESCRIBED.call_once(|| {
        use metric_names::*;
        use metrics::Unit;
        metrics::describe_counter!(GRAPHS_STARTED, Unit::Count, "Runs and resumes started.");
        metrics::describe_gauge!(
            GRAPHS_ACTIVE,
            Unit::Count,
            "Runs and resumes executing in this process."
        );
        metrics::describe_histogram!(
            GRAPH_DURATION,
            Unit::Seconds,
            "How long a run or resume took, by outcome."
        );
        metrics::describe_histogram!(
            NODE_DURATION,
            Unit::Seconds,
            "How long a node took, every attempt included, by result."
        );
        metrics::describe_counter!(
            NODE_RETRIES,
            Unit::Count,
            "Attempts that failed transiently and were retried."
        );
        metrics::describe_counter!(
            GRAPH_TRANSITION,
            Unit::Count,
            "Edges followed from one node to the next."
        );
        metrics::describe_counter!(
            HOP_LIMIT_EXCEEDED,
            Unit::Count,
            "Runs a graph level's hop limit stopped, by the node refused."
        );
        metrics::describe_histogram!(
            CHECKPOINT_LATENCY,
            Unit::Seconds,
            "How long a checkpoint save or load took."
        );
        metrics::describe_counter!(
            OBSERVER_ERRORS,
            Unit::Count,
            "Panics in run observers, which never fail the run."
        );
        metrics::describe_counter!(
            TOOL_REQUESTS,
            Unit::Count,
            "Tool calls dispatched, by result."
        );
        metrics::describe_histogram!(
            TOOL_DURATION,
            Unit::Seconds,
            "How long a tool call took, by result."
        );
        metrics::describe_counter!(
            THREAD_WAKES,
            Unit::Count,
            "Waiting threads a signal or the wake driver woke or ended, by reason."
        );
        metrics::describe_counter!(
            LEASE_ACQUISITIONS,
            Unit::Count,
            "Thread leases a run tried to acquire, by outcome (acquired or held)."
        );
        metrics::describe_counter!(
            LEASE_LOSSES,
            Unit::Count,
            "Runs that lost their thread's lease and stopped."
        );
        metrics::describe_counter!(
            THREAD_RECOVERIES,
            Unit::Count,
            "Running threads a recovery claimed and continued."
        );
        metrics::describe_counter!(
            SCHEDULER_ADMISSIONS,
            Unit::Count,
            "Runs a scheduler admitted or refused, by outcome and priority."
        );
        metrics::describe_histogram!(
            SCHEDULER_WAIT,
            Unit::Seconds,
            "How long a run waited for a scheduler's slot, admitted or not, by outcome."
        );
        metrics::describe_gauge!(
            SCHEDULER_QUEUED,
            Unit::Count,
            "Runs waiting for a scheduler's slot."
        );
        metrics::describe_gauge!(
            SCHEDULER_ACTIVE,
            Unit::Count,
            "Runs holding a scheduler's slot."
        );
        metrics::describe_histogram!(
            MODEL_TOKEN_USAGE,
            Unit::Count,
            "Tokens a model call used, by token type (input or output)."
        );
        metrics::describe_histogram!(MODEL_DURATION, Unit::Seconds, "How long a model call took.");
        metrics::describe_histogram!(
            MODEL_TIME_TO_FIRST_CHUNK,
            Unit::Seconds,
            "How long a streamed model call took to send its first update."
        );
        metrics::describe_counter!(
            ROUTE_CALLS,
            Unit::Count,
            "Calls through a model route, by outcome."
        );
        metrics::describe_counter!(
            ROUTE_FAILOVERS,
            Unit::Count,
            "Candidates a model route moved past, by reason."
        );
        metrics::describe_counter!(
            ROUTE_HEDGES,
            Unit::Count,
            "Attempts a model route's hedging started beside one still running."
        );
        metrics::describe_counter!(
            MODEL_BREAKER_TRANSITIONS,
            Unit::Count,
            "Model health breakers changing state, by key and state."
        );
    });
}

// --- Run metadata (SG-14) ---------------------------------------------------

/// Caller metadata on checkpoints. It shares the checkpoint's `metadata` map
/// with the engine's own keys, so those names, and every key under the
/// `strata.` prefix kept for future engine keys, are refused. Mirrors the .NET
/// engine's `RunMetadata`.
pub(crate) mod run_metadata {
    use std::collections::HashMap;

    const ENGINE_KEYS: [&str; 4] = ["error", "error_kind", "attempts", "cancel_reason"];
    const RESERVED_PREFIX: &str = "strata.";

    pub(crate) fn is_reserved(key: &str) -> bool {
        ENGINE_KEYS.contains(&key) || key.starts_with(RESERVED_PREFIX)
    }

    /// Refuses what a run can't start with, before anything is loaded or written.
    pub(crate) fn validate(
        metadata: &HashMap<String, String>,
        run_id: Option<&str>,
    ) -> Result<(), String> {
        let mut keys: Vec<&String> = metadata.keys().collect();
        keys.sort();
        for key in keys {
            if key.is_empty() {
                return Err("run metadata keys can't be empty".to_string());
            }
            if is_reserved(key) {
                return Err(format!("run metadata key '{key}' is reserved"));
            }
        }
        if run_id == Some("") {
            return Err("a run id can't be empty".to_string());
        }
        Ok(())
    }

    /// A resume's metadata: the caller keys of the checkpoint it resumes, with
    /// the resume's own laid over them key by key.
    pub(crate) fn resumed(
        stored: Option<&HashMap<String, String>>,
        given: &HashMap<String, String>,
    ) -> HashMap<String, String> {
        let mut merged: HashMap<String, String> = stored
            .into_iter()
            .flatten()
            .filter(|(key, _)| !is_reserved(key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        merged.extend(given.iter().map(|(k, v)| (k.clone(), v.clone())));
        merged
    }

    /// What a checkpoint's `metadata` holds: the run's keys and `engine`'s,
    /// which can't collide. `None` when both are empty, as a checkpoint has
    /// always had.
    pub(crate) fn for_checkpoint(
        run: &HashMap<String, String>,
        engine: Option<HashMap<String, String>>,
    ) -> Option<HashMap<String, String>> {
        if run.is_empty() {
            return engine;
        }
        let mut metadata = run.clone();
        metadata.extend(engine.unwrap_or_default());
        Some(metadata)
    }
}

// --- The per-run hook layer -------------------------------------------------

/// One run's events on their way to its observer: recording is a lock, an
/// increment and a send, and a spawned pump delivers them in sequence order.
struct EventQueue {
    state: Mutex<(u64, Option<mpsc::UnboundedSender<RunEvent>>)>,
    thread_id: Arc<str>,
    run_id: Arc<str>,
}

impl EventQueue {
    fn start(observer: Arc<dyn RunObserver>, thread_id: Arc<str>, run_id: Arc<str>) -> Self {
        let (sender, mut receiver) = mpsc::unbounded_channel::<RunEvent>();
        tokio::spawn(async move {
            while let Some(event) = receiver.recv().await {
                let delivered =
                    std::panic::catch_unwind(AssertUnwindSafe(|| observer.on_event(&event)));
                if delivered.is_err() {
                    // An observer can't fail the run, or stop the events after
                    // this one; it can only be counted.
                    metrics::counter!(metric_names::OBSERVER_ERRORS).increment(1);
                    tracing::warn!(target: "datalium_strata", "a run observer panicked");
                }
            }
        });
        Self {
            state: Mutex::new((0, Some(sender))),
            thread_id,
            run_id,
        }
    }

    fn record(&self, kind: RunEventKind, node_path: Option<&str>, attempt: Option<u32>) {
        // The sequence is taken under the same lock that sends, so delivery
        // order is sequence order even when concurrent branches record at once.
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let (sequence, sender) = &mut *state;
        let Some(sender) = sender else { return };
        *sequence += 1;
        let _ = sender.send(RunEvent {
            thread_id: self.thread_id.clone(),
            run_id: self.run_id.clone(),
            sequence: *sequence,
            timestamp: Utc::now(),
            node_path: node_path.map(Arc::from),
            attempt,
            kind,
        });
    }

    /// No more events: the pump stops once it has delivered the last one.
    fn complete(&self) {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .1 = None;
    }
}

/// One run's hook layer: the executor calls it once at each lifecycle point,
/// and each call records the event (when there is an observer), opens or
/// closes the span, and updates the metrics. A subgraph's child shares its
/// parent's, since it is the same run.
pub(crate) struct RunTelemetry {
    pub(crate) thread_id: Arc<str>,
    pub(crate) run_id: Arc<str>,
    mode: RunMode,
    metadata: Mutex<Arc<HashMap<String, String>>>,
    events: Option<EventQueue>,
    pub(crate) span: Span,
    started: Instant,
    completed: AtomicBool,
    queued_for: Option<Duration>,
}

impl RunTelemetry {
    /// Opens the run's span (a child of whatever is current) and counts the
    /// run as active. Must be called inside a tokio runtime when there is an
    /// observer, whose pump it spawns.
    pub(crate) fn start(
        thread_id: Arc<str>,
        run_id: Option<String>,
        mode: RunMode,
        metadata: HashMap<String, String>,
        observer: Option<Arc<dyn RunObserver>>,
        queued: Option<(Duration, i32)>,
    ) -> Self {
        describe_metrics();
        let run_id: Arc<str> = match run_id {
            Some(run_id) => Arc::from(run_id),
            None => {
                let mut text = uuid::Uuid::encode_buffer();
                Arc::from(&*uuid::Uuid::now_v7().hyphenated().encode_lower(&mut text))
            }
        };
        let mode_tag = mode_tag(mode);
        let span = tracing::info_span!(
            target: "datalium_strata",
            "strata.run",
            otel.name = match mode {
                RunMode::Run => "strata.run",
                RunMode::Resume => "strata.resume",
                RunMode::End => "strata.end",
                RunMode::Recover => "strata.recover",
            },
            strata.thread_id = %thread_id,
            strata.run_id = %run_id,
            strata.run.mode = mode_tag,
            strata.metadata = Empty,
            strata.graph.id = Empty,
            strata.graph.version = Empty,
            strata.outcome = Empty,
            strata.cancel_reason = Empty,
            strata.wake.reason = Empty,
            strata.recoveries = Empty,
            strata.lease.owner = Empty,
            strata.lease.token = Empty,
            strata.run.priority = Empty,
            strata.scheduler.wait_ms = Empty,
            error.type = Empty,
            otel.status_code = Empty,
            otel.status_message = Empty,
        );
        metrics::with_recorder(|recorder| {
            recorder.register_counter(&RUN_SERIES.started[mode_index(mode)], &METRIC_METADATA)
        })
        .increment(1);
        metrics::gauge!(metric_names::GRAPHS_ACTIVE).increment(1.0);
        // How long it waited for its scheduler's slot (design/40-Run-Scheduler.md).
        if let Some((waited, priority)) = queued {
            span.record("strata.run.priority", priority);
            span.record("strata.scheduler.wait_ms", waited.as_secs_f64() * 1000.0);
        }
        let events =
            observer.map(|observer| EventQueue::start(observer, thread_id.clone(), run_id.clone()));
        Self {
            thread_id,
            run_id,
            mode,
            metadata: Mutex::new(if metadata.is_empty() {
                crate::context::NO_METADATA.clone()
            } else {
                Arc::new(metadata)
            }),
            events,
            span,
            started: Instant::now(),
            completed: AtomicBool::new(false),
            queued_for: queued.map(|(waited, _)| waited),
        }
    }

    /// The run's metadata. A resume's is settled once its checkpoint is loaded.
    pub(crate) fn metadata(&self) -> Arc<HashMap<String, String>> {
        self.metadata
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    /// Lays the resume's metadata over the caller keys of the checkpoint it resumes.
    pub(crate) fn adopt_stored(&self, stored: Option<&HashMap<String, String>>) {
        let mut metadata = self
            .metadata
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        *metadata = Arc::new(run_metadata::resumed(stored, &metadata));
    }

    /// What a checkpoint at any level of this run holds in `metadata`.
    pub(crate) fn checkpoint_metadata(
        &self,
        engine: Option<HashMap<String, String>>,
    ) -> Option<HashMap<String, String>> {
        run_metadata::for_checkpoint(&self.metadata(), engine)
    }

    fn record(
        &self,
        kind: impl FnOnce() -> RunEventKind,
        node_path: Option<&str>,
        attempt: Option<u32>,
    ) {
        if let Some(events) = &self.events {
            events.record(kind(), node_path, attempt);
        }
    }

    pub(crate) fn run_started(&self, node: &str, graph: Option<&GraphIdentity>) {
        let metadata = self.metadata();
        if !metadata.is_empty() {
            let json = serde_json::to_string(&*metadata).unwrap_or_default();
            self.span.record("strata.metadata", json.as_str());
        }
        // Which graph runs (design/31-Graph-Identity.md).
        if let Some(graph) = graph {
            self.span.record("strata.graph.id", graph.id.as_str());
            if let Some(version) = &graph.version {
                self.span.record("strata.graph.version", version.as_str());
            }
        }
        tracing::info!(target: "datalium_strata", parent: &self.span, node, "run started");
        self.record(
            || RunEventKind::RunStarted {
                mode: self.mode,
                node: node.to_string(),
                metadata: (*metadata).clone(),
                graph: graph.cloned(),
                queued_for: self.queued_for,
            },
            None,
            None,
        );
    }

    /// The run acquired its thread's lease (`design/32-Thread-Leases.md`).
    pub(crate) fn lease_acquired(&self, lease: &ThreadLease) {
        metrics::counter!(metric_names::LEASE_ACQUISITIONS, "strata.lease.outcome" => "acquired")
            .increment(1);
        self.span.record("strata.lease.owner", lease.owner.as_str());
        self.span.record("strata.lease.token", lease.token);
        self.record(
            || RunEventKind::LeaseAcquired {
                owner: lease.owner.clone(),
                token: lease.token,
                expires_at: lease.expires_at,
            },
            None,
            None,
        );
    }

    /// The run was refused its thread's lease: another owner holds it.
    pub(crate) fn lease_held() {
        describe_metrics();
        metrics::counter!(metric_names::LEASE_ACQUISITIONS, "strata.lease.outcome" => "held")
            .increment(1);
    }

    /// The run lost its thread's lease, and stops.
    pub(crate) fn lease_lost(&self, lease: &ThreadLease) {
        metrics::counter!(metric_names::LEASE_LOSSES).increment(1);
        tracing::warn!(
            parent: &self.span,
            strata.lease.owner = lease.owner.as_str(),
            strata.lease.token = lease.token,
            "lease lost"
        );
        self.record(
            || RunEventKind::LeaseLost {
                owner: lease.owner.clone(),
                token: lease.token,
            },
            None,
            None,
        );
    }

    /// A resume stamped the checkpoint it resumes: `from` is that
    /// checkpoint's status and `waiting_at` where it was waiting.
    pub(crate) fn resumed(
        &self,
        from: CheckpointStatus,
        waiting_at: Vec<String>,
        signals: &LevelSignals,
        route: Option<&str>,
    ) {
        let wake = signals.wake();
        self.span.record("strata.wake.reason", wake.as_str());
        if wake != WakeReason::Human {
            Self::woken(wake);
        }
        let signal = signals.innermost();
        self.record(
            || RunEventKind::Resumed {
                from,
                waiting_at,
                is_approved: signal.is_approved,
                reviewer: signal.reviewer_token.clone(),
                rejected_to: (wake == WakeReason::Human)
                    .then(|| route.map(str::to_string))
                    .flatten(),
                wake,
                event_type: signals.event_type().map(str::to_string),
                timed_out_to: (wake == WakeReason::Timeout)
                    .then(|| route.map(str::to_string))
                    .flatten(),
            },
            None,
            None,
        );
    }

    /// Counts a thread a signal or `wake_due` woke or ended
    /// (`design/29-Event-Waits-And-Timers.md`).
    /// A recovery claimed its thread (`design/33-Durable-Execution.md`): it
    /// continues at `at`, and has now been recovered `recoveries` times from
    /// that checkpoint.
    pub(crate) fn recovered(&self, at: Vec<String>, recoveries: u32) {
        metrics::counter!(metric_names::THREAD_RECOVERIES).increment(1);
        self.span.record("strata.recoveries", recoveries);
        self.record(|| RunEventKind::Recovered { at, recoveries }, None, None);
    }

    pub(crate) fn woken(reason: WakeReason) {
        metrics::counter!(metric_names::THREAD_WAKES, "strata.wake.reason" => reason.as_str())
            .increment(1);
    }

    pub(crate) fn transition(from: &str, to: &str) {
        Self::transition_keyed(&transition_key(from, to));
    }

    /// [`Self::transition`] with the series already made ([`transition_key`]).
    pub(crate) fn transition_keyed(key: &metrics::Key) {
        metrics::with_recorder(|recorder| recorder.register_counter(key, &METRIC_METADATA))
            .increment(1);
    }

    pub(crate) fn checkpoint_saved(
        &self,
        status: CheckpointStatus,
        waiting_at: Vec<String>,
        resume_stamp: bool,
        duration: Duration,
    ) {
        let status_tag = status_tag(status);
        metrics::histogram!(
            metric_names::CHECKPOINT_LATENCY,
            "strata.checkpoint.operation" => "save",
            "strata.checkpoint.status" => status_tag,
        )
        .record(duration.as_secs_f64());
        tracing::debug!(
            parent: &self.span,
            strata.checkpoint.status = status_tag,
            strata.checkpoint.resume_stamp = resume_stamp,
            strata.checkpoint.duration_ms = duration.as_secs_f64() * 1000.0,
            "checkpoint saved"
        );
        self.record(
            || RunEventKind::CheckpointSaved {
                status,
                waiting_at,
                resume_stamp,
                duration,
            },
            None,
            None,
        );
    }

    /// A level's hop limit refused the node at `path`
    /// (`design/24-Loop-Circuit-Breaker.md`), before its checkpoint is saved.
    /// A group at `path` decided its join
    /// (`design/26-Parallel-Group-Controls-And-Joins.md`): the event, and a
    /// span event under `group_span`, the group's attempt span.
    pub(crate) fn join_decided(
        &self,
        path: &str,
        attempt: u32,
        policy: &str,
        met: bool,
        deciding: Vec<String>,
        group_span: &Span,
    ) {
        tracing::info!(
            parent: group_span,
            strata.join.policy = policy,
            strata.join.met = met,
            strata.join.deciding = %deciding.join(","),
            "join decided"
        );
        self.record(
            || RunEventKind::JoinDecided {
                policy: policy.to_string(),
                met,
                deciding,
            },
            Some(path),
            Some(attempt),
        );
    }

    /// A map at `path` expanded into `keys` (`design/27-Map-Fan-Out.md`): the
    /// event, and a span event under `map_span`, the map's attempt span.
    pub(crate) fn map_expanded(
        &self,
        path: &str,
        attempt: u32,
        keys: Vec<String>,
        map_span: &Span,
    ) {
        tracing::info!(
            parent: map_span,
            strata.map.item_count = keys.len(),
            strata.map.keys = %keys.join(","),
            "map expanded"
        );
        self.record(
            || RunEventKind::MapExpanded { keys },
            Some(path),
            Some(attempt),
        );
    }

    pub(crate) fn hop_limit_exceeded(&self, path: &str, hops: u32, limit: u32) {
        metrics::counter!(
            metric_names::HOP_LIMIT_EXCEEDED,
            "strata.node.path" => path.to_string(),
        )
        .increment(1);
        tracing::warn!(
            parent: &self.span,
            strata.node.path = path,
            strata.hops = hops,
            strata.hop_limit = limit,
            "hop limit exceeded"
        );
        self.record(
            || RunEventKind::HopLimitExceeded { hops, limit },
            Some(path),
            None,
        );
    }

    pub(crate) fn checkpoint_loaded(status: Option<CheckpointStatus>, duration: Duration) {
        metrics::histogram!(
            metric_names::CHECKPOINT_LATENCY,
            "strata.checkpoint.operation" => "load",
            "strata.checkpoint.status" => status.map(status_tag).unwrap_or("none"),
        )
        .record(duration.as_secs_f64());
    }

    /// Ends the run with `result`: `Interrupted` first if it froze, then the
    /// last event, the span, and the run metrics.
    pub(crate) fn completed_with<S>(&self, result: &GraphExecutionResult<S>) {
        match result {
            GraphExecutionResult::Completed(_) => {
                self.run_completed(RunOutcome::Completed, None, None);
            }
            GraphExecutionResult::Interrupted {
                waiting_at,
                next_node,
                wait,
                ..
            } => {
                self.record(
                    || RunEventKind::Interrupted {
                        waiting_at: waiting_at.clone(),
                        next_node: next_node.clone(),
                        wait: wait.clone(),
                    },
                    None,
                    None,
                );
                self.run_completed(RunOutcome::Interrupted, None, None);
            }
            GraphExecutionResult::Failed(error) => {
                if matches!(error, StrataError::HopLimitExceeded { .. }) {
                    self.span.record("error.type", "hop_limit");
                }
                self.run_completed(RunOutcome::Failed, Some(error_message(error)), None);
            }
            GraphExecutionResult::Cancelled { reason, .. } => {
                self.run_completed(RunOutcome::Cancelled, None, Some(*reason));
            }
        }
    }

    /// Ends the run. Only the first call counts.
    pub(crate) fn run_completed(
        &self,
        outcome: RunOutcome,
        error: Option<String>,
        cancel_reason: Option<CancelReason>,
    ) {
        if self.completed.swap(true, Ordering::SeqCst) {
            return;
        }
        let duration = self.started.elapsed();
        let outcome_tag = outcome_tag(outcome);
        metrics::gauge!(metric_names::GRAPHS_ACTIVE).decrement(1.0);
        let series = &RUN_SERIES.duration[mode_index(self.mode)][outcome_index(outcome)];
        metrics::with_recorder(|recorder| recorder.register_histogram(series, &METRIC_METADATA))
            .record(duration.as_secs_f64());

        self.span.record("strata.outcome", outcome_tag);
        if let Some(reason) = cancel_reason {
            self.span.record("strata.cancel_reason", reason_tag(reason));
        }
        if let Some(message) = &error {
            self.span.record("otel.status_code", "ERROR");
            self.span.record("otel.status_message", message.as_str());
        }
        tracing::info!(
            target: "datalium_strata",
            parent: &self.span,
            outcome = outcome_tag,
            "run completed"
        );

        self.record(
            || RunEventKind::RunCompleted {
                outcome,
                duration,
                error,
                cancel_reason,
            },
            None,
            None,
        );
        if let Some(events) = &self.events {
            events.complete();
        }
    }
}

impl Drop for RunTelemetry {
    /// A run future dropped before it finished (`design/17`: not a
    /// cancellation) records nothing more, but no longer counts as active.
    fn drop(&mut self) {
        if !self.completed.swap(true, Ordering::SeqCst) {
            metrics::gauge!(metric_names::GRAPHS_ACTIVE).decrement(1.0);
        }
    }
}

/// What every metric this module records by a key of its own is registered
/// with: what the `metrics` macros give a metric recorded here.
static METRIC_METADATA: metrics::Metadata<'static> =
    metrics::Metadata::new(module_path!(), metrics::Level::INFO, Some(module_path!()));

/// The series every run records to, made once: `strata.graphs.started` by run
/// mode, and `strata.graph.duration` by run mode and outcome. A run then
/// allocates nothing to count itself (`design/38-Rust-Executor-Overhead.md`).
struct RunSeries {
    started: [metrics::Key; 4],
    duration: [[metrics::Key; 4]; 4],
}

const RUN_MODES: [RunMode; 4] = [
    RunMode::Run,
    RunMode::Resume,
    RunMode::End,
    RunMode::Recover,
];

const RUN_OUTCOMES: [RunOutcome; 4] = [
    RunOutcome::Completed,
    RunOutcome::Interrupted,
    RunOutcome::Failed,
    RunOutcome::Cancelled,
];

fn mode_index(mode: RunMode) -> usize {
    RUN_MODES
        .iter()
        .position(|known| *known == mode)
        .unwrap_or(0)
}

fn outcome_index(outcome: RunOutcome) -> usize {
    RUN_OUTCOMES
        .iter()
        .position(|known| *known == outcome)
        .unwrap_or(0)
}

static RUN_SERIES: LazyLock<RunSeries> = LazyLock::new(|| RunSeries {
    started: RUN_MODES.map(|mode| {
        metrics::Key::from_parts(
            metric_names::GRAPHS_STARTED,
            vec![metrics::Label::new("strata.run.mode", mode_tag(mode))],
        )
    }),
    duration: RUN_MODES.map(|mode| {
        RUN_OUTCOMES.map(|outcome| {
            metrics::Key::from_parts(
                metric_names::GRAPH_DURATION,
                vec![
                    metrics::Label::new("strata.run.mode", mode_tag(mode)),
                    metrics::Label::new("strata.outcome", outcome_tag(outcome)),
                ],
            )
        })
    }),
});

/// The `strata.graph.transition` series of the edge between the nodes at
/// these paths. A compiled graph makes one per edge, so following an edge at
/// the outermost level allocates nothing
/// (`design/38-Rust-Executor-Overhead.md`).
pub(crate) fn transition_key(from: &str, to: &str) -> metrics::Key {
    metrics::Key::from_parts(
        metric_names::GRAPH_TRANSITION,
        vec![
            metrics::Label::new("strata.node.path", from.to_string()),
            metrics::Label::new("strata.next_node.path", to.to_string()),
        ],
    )
}

/// The `strata.node.duration` series of the node at `path` ending with `result`.
fn node_duration_key(path: &str, result: NodeResult) -> metrics::Key {
    metrics::Key::from_parts(
        metric_names::NODE_DURATION,
        vec![
            metrics::Label::new("strata.node.path", path.to_string()),
            metrics::Label::new("strata.node.result", node_result_tag(result)),
        ],
    )
}

/// [`node_duration_key`] for a node that commits, which a compiled graph
/// makes once per node.
pub(crate) fn node_committed_key(path: &str) -> metrics::Key {
    node_duration_key(path, NodeResult::Committed)
}

/// One node of a run, from `NodeStarted` to `NodeCompleted`.
pub(crate) struct NodeScope {
    telemetry: Arc<RunTelemetry>,
    /// The node's own name in its graph, and its qualified path in the run.
    pub(crate) name: Arc<str>,
    pub(crate) path: Arc<str>,
    pub(crate) span: Span,
    started: Instant,
    attempt: AtomicU32,
    completed: AtomicBool,
}

impl NodeScope {
    /// Opens the span of the node `name`, at `path` in the run, a child of
    /// whatever is current.
    pub(crate) fn start(telemetry: &Arc<RunTelemetry>, name: Arc<str>, path: Arc<str>) -> Self {
        let span = tracing::info_span!(
            target: "datalium_strata",
            "strata.node",
            otel.name = %path,
            strata.thread_id = %telemetry.thread_id,
            strata.run_id = %telemetry.run_id,
            strata.node.path = %path,
            strata.retry_count = Empty,
            strata.node.result = Empty,
            error.type = Empty,
            otel.status_code = Empty,
            otel.status_message = Empty,
        );
        telemetry.record(|| RunEventKind::NodeStarted, Some(&path), Some(1));
        Self {
            telemetry: telemetry.clone(),
            name,
            path,
            span,
            started: Instant::now(),
            attempt: AtomicU32::new(1),
            completed: AtomicBool::new(false),
        }
    }

    /// Opens attempt `attempt`'s span, the one its handler runs inside.
    pub(crate) fn start_attempt(&self, attempt: u32) -> Span {
        self.attempt.store(attempt, Ordering::SeqCst);
        tracing::info_span!(
            target: "datalium_strata",
            parent: &self.span,
            "strata.attempt",
            otel.name = %format!("{} #{attempt}", self.path),
            strata.thread_id = %self.telemetry.thread_id,
            strata.run_id = %self.telemetry.run_id,
            strata.node.path = %self.path,
            "strata.attempt" = attempt,
            error.type = Empty,
            otel.status_code = Empty,
            otel.status_message = Empty,
        )
    }

    /// The attempt that just ended failed transiently and the node will run
    /// again after `delay` (`design/25-Retry-Backoff.md`).
    pub(crate) fn retry(
        &self,
        error: &str,
        error_type: &'static str,
        timed_out: bool,
        delay: Duration,
    ) {
        let attempt = self.attempt.load(Ordering::SeqCst);
        metrics::counter!(
            metric_names::NODE_RETRIES,
            "strata.node.path" => self.path.to_string(),
            "error.type" => error_type,
        )
        .increment(1);
        tracing::warn!(
            parent: &self.span,
            strata.attempt = attempt,
            error.type = error_type,
            strata.retry.delay_ms = delay.as_secs_f64() * 1000.0,
            "retry scheduled: {error}"
        );
        self.telemetry.record(
            || RunEventKind::RetryScheduled {
                error: error.to_string(),
                timed_out,
                delay,
            },
            Some(&self.path),
            Some(attempt),
        );
    }

    /// Ends the node. Only the first call counts, so a failure path can end it
    /// before its checkpoint is saved and the executor's later call changes
    /// nothing.
    pub(crate) fn completed(
        &self,
        result: NodeResult,
        error: Option<&str>,
        error_type: Option<&str>,
        fallback: Option<&str>,
    ) {
        self.completed_as(result, error, error_type, fallback, None);
    }

    /// The node committed. `series` is its `strata.node.duration` series when
    /// the caller has it ready ([`node_committed_key`] of this node's path).
    pub(crate) fn committed(&self, series: Option<&metrics::Key>) {
        self.completed_as(NodeResult::Committed, None, None, None, series);
    }

    fn completed_as(
        &self,
        result: NodeResult,
        error: Option<&str>,
        error_type: Option<&str>,
        fallback: Option<&str>,
        series: Option<&metrics::Key>,
    ) {
        if self.completed.swap(true, Ordering::SeqCst) {
            return;
        }
        let duration = self.started.elapsed();
        let attempt = self.attempt.load(Ordering::SeqCst);
        let result_tag = node_result_tag(result);
        let made;
        let series = match series {
            Some(series) => series,
            None => {
                made = node_duration_key(&self.path, result);
                &made
            }
        };
        metrics::with_recorder(|recorder| recorder.register_histogram(series, &METRIC_METADATA))
            .record(duration.as_secs_f64());

        self.span.record("strata.retry_count", attempt - 1);
        self.span.record("strata.node.result", result_tag);
        if result == NodeResult::Failed {
            fail(&self.span, error.unwrap_or("failed"), error_type);
            tracing::warn!(
                target: "datalium_strata",
                parent: &self.span,
                "node failed: {}",
                error.unwrap_or("failed")
            );
        }

        self.telemetry.record(
            || RunEventKind::NodeCompleted {
                result,
                duration,
                error: error.map(str::to_string),
                fallback: fallback.map(str::to_string),
            },
            Some(&self.path),
            Some(attempt),
        );
    }

    /// A panic is propagating out of the node.
    pub(crate) fn panicked(&self, panic: &(dyn Any + Send)) {
        self.completed(
            NodeResult::Failed,
            Some(&panic_message(panic)),
            Some("panic"),
            None,
        );
    }
}

/// One branch of a parallel group, from `BranchStarted` to `BranchCompleted`.
pub(crate) struct BranchScope {
    telemetry: Arc<RunTelemetry>,
    path: Arc<str>,
    pub(crate) span: Span,
    attempt: u32,
    started: Instant,
    completed: AtomicBool,
}

impl BranchScope {
    /// Opens the branch's span, a child of whatever is current (the group's attempt).
    pub(crate) fn start(telemetry: &Arc<RunTelemetry>, path: Arc<str>, attempt: u32) -> Self {
        let span = tracing::info_span!(
            target: "datalium_strata",
            "strata.branch",
            otel.name = %path,
            strata.thread_id = %telemetry.thread_id,
            strata.run_id = %telemetry.run_id,
            strata.node.path = %path,
            "strata.attempt" = attempt,
            strata.branch.reason = Empty,
            error.type = Empty,
            otel.status_code = Empty,
            otel.status_message = Empty,
        );
        telemetry.record(|| RunEventKind::BranchStarted, Some(&path), Some(attempt));
        Self {
            telemetry: telemetry.clone(),
            path,
            span,
            attempt,
            started: Instant::now(),
            completed: AtomicBool::new(false),
        }
    }

    /// Ends the branch, saying why when the result alone doesn't. Only the
    /// first call counts.
    pub(crate) fn completed(
        &self,
        result: BranchResult,
        error: Option<&NodeError>,
        reason: Option<BranchEndReason>,
    ) {
        self.complete(
            result,
            error.map(|error| (error.message().to_string(), node_error_tag(error))),
            reason,
        );
    }

    pub(crate) fn panicked(&self, panic: &(dyn Any + Send)) {
        self.complete(
            BranchResult::Failed,
            Some((panic_message(panic), "panic")),
            None,
        );
    }

    fn complete(
        &self,
        result: BranchResult,
        error: Option<(String, &'static str)>,
        reason: Option<BranchEndReason>,
    ) {
        if self.completed.swap(true, Ordering::SeqCst) {
            return;
        }
        let duration = self.started.elapsed();
        if let Some((message, error_type)) = &error {
            fail(&self.span, message, Some(error_type));
        }
        if let Some(reason) = reason {
            self.span.record("strata.branch.reason", reason.as_str());
        }
        self.telemetry.record(
            || RunEventKind::BranchCompleted {
                result,
                duration,
                error: error.map(|(message, _)| message),
                reason,
            },
            Some(&self.path),
            Some(self.attempt),
        );
    }
}

impl Drop for BranchScope {
    /// A group that gives up on a branch (its future dropped because the run
    /// was cancelled or the group timed out) ends it as cancelled.
    fn drop(&mut self) {
        self.complete(BranchResult::Cancelled, None, None);
    }
}

/// One tool call a registry dispatched, from `ToolCallStarted` to
/// `ToolCallCompleted` (`design/20-Tool-Dispatch-And-Registry.md`).
pub(crate) struct ToolCallScope {
    telemetry: Option<Arc<RunTelemetry>>,
    tool: Arc<str>,
    /// Whether `tool` is a name the registry knows, and so a bounded metric label.
    known: bool,
    call_id: Arc<str>,
    node_path: Option<Arc<str>>,
    attempt: Option<u32>,
    pub(crate) span: Span,
    started: Instant,
    completed: AtomicBool,
}

impl ToolCallScope {
    /// Opens the call's span: a child of whatever is current (inside a node,
    /// its attempt span, or a span the handler opened around the call), and
    /// of `ctx`'s span when nothing is (a task spawned without the span).
    pub(crate) fn start(tool: &str, known: bool, ctx: &ToolCallContext) -> Self {
        describe_metrics();
        let call_id: Arc<str> = Arc::from(ctx.call_id().unwrap_or_default());
        let current = Span::current();
        let parent = if current.is_none() {
            ctx.span().clone()
        } else {
            current
        };
        let span = tracing::info_span!(
            target: "datalium_strata",
            parent: &parent,
            "strata.tool",
            otel.name = %format!("execute_tool {tool}"),
            gen_ai.operation.name = "execute_tool",
            gen_ai.tool.name = %tool,
            gen_ai.tool.call.id = %call_id,
            strata.thread_id = Empty,
            strata.run_id = Empty,
            strata.node.path = Empty,
            "strata.attempt" = Empty,
            strata.tool.caller = Empty,
            strata.tool.result = Empty,
            strata.tool.short_circuited = Empty,
            error.type = Empty,
            otel.status_code = Empty,
            otel.status_message = Empty,
        );
        if let Some(thread_id) = ctx.thread_id() {
            span.record("strata.thread_id", thread_id);
        }
        if let Some(run_id) = ctx.run_id() {
            span.record("strata.run_id", run_id);
        }
        if let Some(path) = ctx.node_path() {
            span.record("strata.node.path", path);
        }
        if let Some(attempt) = ctx.attempt() {
            span.record("strata.attempt", attempt);
        }
        if let Some(caller) = ctx.caller_id() {
            span.record("strata.tool.caller", caller);
        }
        let telemetry = ctx.telemetry();
        let node_path: Option<Arc<str>> = ctx.node_path().map(Arc::from);
        if let Some(telemetry) = &telemetry {
            telemetry.record(
                || RunEventKind::ToolCallStarted {
                    tool: tool.to_string(),
                    call_id: call_id.to_string(),
                    caller_id: ctx.caller_id().map(str::to_string),
                },
                node_path.as_deref(),
                ctx.attempt(),
            );
        }
        Self {
            telemetry,
            tool: Arc::from(tool),
            known,
            call_id,
            node_path,
            attempt: ctx.attempt(),
            span,
            started: Instant::now(),
            completed: AtomicBool::new(false),
        }
    }

    /// The middleware chain returned without running the tool
    /// (`design/23-Tool-Middleware.md`).
    pub(crate) fn short_circuited(&self) {
        self.span.record("strata.tool.short_circuited", true);
    }

    /// Ends the call with `result`.
    pub(crate) fn completed(&self, result: &Result<String, StrataError>) {
        match result {
            Ok(_) => self.complete(ToolCallResult::Succeeded, None),
            Err(StrataError::ToolCancelled(_)) => self.complete(ToolCallResult::Cancelled, None),
            Err(error) => {
                let outcome = match error {
                    StrataError::ToolNotFound(_)
                    | StrataError::ToolNotPermitted(_)
                    | StrataError::ToolCallDenied { .. } => ToolCallResult::Refused,
                    _ => ToolCallResult::Failed,
                };
                self.complete(outcome, Some((error_message(error), tool_error_tag(error))));
            }
        }
    }

    fn complete(&self, result: ToolCallResult, error: Option<(String, &'static str)>) {
        if self.completed.swap(true, Ordering::SeqCst) {
            return;
        }
        let duration = self.started.elapsed();
        let result_tag = tool_result_tag(result);
        if self.known {
            let name = self.tool.to_string();
            metrics::counter!(
                metric_names::TOOL_REQUESTS,
                "gen_ai.tool.name" => name.clone(),
                "strata.tool.result" => result_tag,
            )
            .increment(1);
            metrics::histogram!(
                metric_names::TOOL_DURATION,
                "gen_ai.tool.name" => name,
                "strata.tool.result" => result_tag,
            )
            .record(duration.as_secs_f64());
        } else {
            // The name came from the model, so it can't be a metric label.
            metrics::counter!(metric_names::TOOL_REQUESTS, "strata.tool.result" => result_tag)
                .increment(1);
            metrics::histogram!(metric_names::TOOL_DURATION, "strata.tool.result" => result_tag)
                .record(duration.as_secs_f64());
        }
        self.span.record("strata.tool.result", result_tag);
        if let Some((message, error_type)) = &error {
            fail(&self.span, message, Some(error_type));
        }
        if let Some(telemetry) = &self.telemetry {
            telemetry.record(
                || RunEventKind::ToolCallCompleted {
                    tool: self.tool.to_string(),
                    call_id: self.call_id.to_string(),
                    result,
                    duration,
                    error: error.map(|(message, _)| message),
                },
                self.node_path.as_deref(),
                self.attempt,
            );
        }
    }
}

impl Drop for ToolCallScope {
    /// A dispatch future dropped before the call finished (the engine gave up
    /// on the calling node, say) ends the call as cancelled.
    fn drop(&mut self) {
        self.complete(ToolCallResult::Cancelled, None);
    }
}

/// What a model call asks for, as its [`ModelCallScope`] records it
/// (`design/44-LLM-Integration.md`). The messages are not part of it: Strata
/// never records prompts or answers. Mirrors the .NET engine's `ModelCallInfo`.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ModelCallInfo {
    /// The model asked for; `unknown` when the caller doesn't know.
    pub model: String,
    /// The provider (`openai`, `anthropic`, …): `gen_ai.provider.name`.
    pub provider: Option<String>,
    /// The GenAI operation: `chat` (the default), or `embeddings` for an
    /// embedding call (`design/45-Long-Term-Memory.md`). It names the span
    /// (`<operation> <model>`) and is on both run events.
    pub operation: String,
    /// The request's output-token limit, when it set one.
    pub max_tokens: Option<u32>,
    /// The request's temperature, when it set one.
    pub temperature: Option<f64>,
    /// The agent making the call, when an agent does (`strata.agent`).
    pub agent: Option<String>,
    /// The model route this call is an attempt of (`design/46-Model-Routes.md`):
    /// `strata.route` on the span and `route` on the events.
    pub route: Option<String>,
    /// The route's candidate making the attempt (`strata.route.candidate`).
    pub route_candidate: Option<String>,
}

impl ModelCallInfo {
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            provider: None,
            operation: "chat".to_string(),
            max_tokens: None,
            temperature: None,
            agent: None,
            route: None,
            route_candidate: None,
        }
    }

    pub fn with_provider(mut self, provider: impl Into<String>) -> Self {
        self.provider = Some(provider.into());
        self
    }

    pub fn with_operation(mut self, operation: impl Into<String>) -> Self {
        self.operation = operation.into();
        self
    }

    pub fn with_max_tokens(mut self, max_tokens: u32) -> Self {
        self.max_tokens = Some(max_tokens);
        self
    }

    pub fn with_temperature(mut self, temperature: f64) -> Self {
        self.temperature = Some(temperature);
        self
    }

    pub fn with_agent(mut self, agent: impl Into<String>) -> Self {
        self.agent = Some(agent.into());
        self
    }

    /// The call is an attempt of the route named `route`.
    pub fn with_route(mut self, route: impl Into<String>) -> Self {
        self.route = Some(route.into());
        self
    }

    /// The route's candidate making the attempt.
    pub fn with_route_candidate(mut self, candidate: impl Into<String>) -> Self {
        self.route_candidate = Some(candidate.into());
        self
    }
}

/// One model call, from `ModelCallStarted` to `ModelCallCompleted`
/// (`design/44-LLM-Integration.md`): the way a crate that calls models
/// (`datalium-strata-ai`, or an application's own client) puts the call in its
/// run's events and the engine's telemetry, as a
/// [`ToolRegistry`](crate::tools::ToolRegistry) does for a tool call. It opens
/// a `strata.model` span named `<operation> <model>` (`otel.name`) with the
/// OpenTelemetry GenAI attributes, and records `gen_ai.client.token.usage` and
/// `gen_ai.client.operation.duration`. Mirrors the .NET engine's
/// `ModelCallScope`.
///
/// The outcome is recorded once, by whichever of [`Self::succeeded`],
/// [`Self::failed`] or [`Self::cancelled`] comes first; dropping a scope that
/// has none (the call's future was dropped) records it cancelled. Started
/// without a node, it records no run event.
pub struct ModelCallScope {
    telemetry: Option<Arc<RunTelemetry>>,
    info: ModelCallInfo,
    call_id: Arc<str>,
    node_path: Option<Arc<str>>,
    attempt: Option<u32>,
    span: Span,
    started: Instant,
    first_chunk: AtomicBool,
    completed: AtomicBool,
}

impl ModelCallScope {
    /// Starts recording a model call. `ctx` is the calling node's context,
    /// which puts the call in that run's events; `None` outside a run. The
    /// span is a child of whatever is current (inside a node, its attempt
    /// span), and of `ctx`'s span when nothing is.
    ///
    /// # Panics
    ///
    /// When `info.model` is blank.
    pub fn start(ctx: Option<&crate::context::NodeContext>, info: ModelCallInfo) -> Self {
        Self::start_under(ctx, info, None)
    }

    fn start_under(
        ctx: Option<&crate::context::NodeContext>,
        info: ModelCallInfo,
        parent: Option<&Span>,
    ) -> Self {
        assert!(
            !info.model.trim().is_empty(),
            "a model call needs a model name (`unknown` when there is none)"
        );
        describe_metrics();
        let call_id: Arc<str> = {
            let mut text = uuid::Uuid::encode_buffer();
            Arc::from(&*uuid::Uuid::now_v7().hyphenated().encode_lower(&mut text))
        };
        let parent = match parent {
            Some(parent) => parent.clone(),
            None => {
                let current = Span::current();
                match ctx {
                    Some(ctx) if current.is_none() => ctx.span().clone(),
                    _ => current,
                }
            }
        };
        let span = tracing::info_span!(
            target: "datalium_strata",
            parent: &parent,
            "strata.model",
            otel.name = %format!("{} {}", info.operation, info.model),
            otel.kind = "client",
            gen_ai.operation.name = %info.operation,
            gen_ai.provider.name = Empty,
            gen_ai.request.model = %info.model,
            gen_ai.request.max_tokens = Empty,
            gen_ai.request.temperature = Empty,
            gen_ai.response.model = Empty,
            gen_ai.response.id = Empty,
            gen_ai.response.finish_reasons = Empty,
            gen_ai.usage.input_tokens = Empty,
            gen_ai.usage.output_tokens = Empty,
            strata.model.call_id = %call_id,
            strata.agent = Empty,
            strata.route = Empty,
            strata.route.candidate = Empty,
            strata.thread_id = Empty,
            strata.run_id = Empty,
            strata.node.path = Empty,
            "strata.attempt" = Empty,
            error.type = Empty,
            otel.status_code = Empty,
            otel.status_message = Empty,
        );
        if let Some(route) = &info.route {
            span.record("strata.route", route.as_str());
        }
        if let Some(candidate) = &info.route_candidate {
            span.record("strata.route.candidate", candidate.as_str());
        }
        if let Some(provider) = &info.provider {
            span.record("gen_ai.provider.name", provider.as_str());
        }
        if let Some(max_tokens) = info.max_tokens {
            span.record("gen_ai.request.max_tokens", max_tokens);
        }
        if let Some(temperature) = info.temperature {
            span.record("gen_ai.request.temperature", temperature);
        }
        if let Some(agent) = &info.agent {
            span.record("strata.agent", agent.as_str());
        }
        let mut telemetry = None;
        let mut node_path = None;
        let mut attempt = None;
        if let Some(ctx) = ctx {
            span.record("strata.thread_id", ctx.thread_id());
            if !ctx.run_id().is_empty() {
                span.record("strata.run_id", ctx.run_id());
            }
            span.record("strata.node.path", ctx.node_path());
            span.record("strata.attempt", ctx.attempt());
            telemetry = ctx.telemetry().and_then(|weak| weak.upgrade());
            node_path = Some(Arc::from(ctx.node_path()));
            attempt = Some(ctx.attempt());
        }
        if let Some(telemetry) = &telemetry {
            telemetry.record(
                || RunEventKind::ModelCallStarted {
                    model: info.model.clone(),
                    call_id: call_id.to_string(),
                    provider: info.provider.clone(),
                    route: info.route.clone(),
                    operation: info.operation.clone(),
                },
                node_path.as_deref(),
                attempt,
            );
        }
        Self {
            telemetry,
            info,
            call_id,
            node_path,
            attempt,
            span,
            started: Instant::now(),
            first_chunk: AtomicBool::new(false),
            completed: AtomicBool::new(false),
        }
    }

    /// The call's id (a UUID v7), on both its events and its span
    /// (`strata.model.call_id`).
    pub fn call_id(&self) -> &str {
        &self.call_id
    }

    /// The call's span.
    pub fn span(&self) -> &Span {
        &self.span
    }

    /// A streamed call's first update arrived: records
    /// `strata.model.time_to_first_chunk` once.
    pub fn first_chunk(&self) {
        if self.first_chunk.swap(true, Ordering::SeqCst) {
            return;
        }
        let labels = self.labels(None, None);
        metrics::histogram!(metric_names::MODEL_TIME_TO_FIRST_CHUNK, labels)
            .record(self.started.elapsed().as_secs_f64());
    }

    /// The call answered: `usage` as the provider reported it, `finish_reason`
    /// as it said (`stop`, `length`, …), the model that answered and the
    /// provider's id for the answer when it said.
    pub fn succeeded(
        &self,
        usage: ModelUsage,
        finish_reason: Option<&str>,
        response_model: Option<&str>,
        response_id: Option<&str>,
    ) {
        if self.completed.swap(true, Ordering::SeqCst) {
            return;
        }
        if let Some(model) = response_model {
            self.span.record("gen_ai.response.model", model);
        }
        if let Some(id) = response_id {
            self.span.record("gen_ai.response.id", id);
        }
        if let Some(reason) = finish_reason {
            self.span.record("gen_ai.response.finish_reasons", reason);
        }
        if let Some(input) = usage.input_tokens {
            self.span.record("gen_ai.usage.input_tokens", input);
            let mut labels = self.labels(response_model, None);
            labels.push(metrics::Label::new("gen_ai.token.type", "input"));
            metrics::histogram!(metric_names::MODEL_TOKEN_USAGE, labels).record(input as f64);
        }
        if let Some(output) = usage.output_tokens {
            self.span.record("gen_ai.usage.output_tokens", output);
            let mut labels = self.labels(response_model, None);
            labels.push(metrics::Label::new("gen_ai.token.type", "output"));
            metrics::histogram!(metric_names::MODEL_TOKEN_USAGE, labels).record(output as f64);
        }
        self.end(
            ModelCallResult::Succeeded,
            usage,
            finish_reason.map(str::to_string),
            response_model,
            None,
        );
    }

    /// The call failed. `error_type` is a low-cardinality class: the HTTP
    /// status (`429`), `timeout`, `connection`, `stream_broken`, …
    /// (`error.type`).
    pub fn failed(&self, error_type: &str, message: &str) {
        if self.completed.swap(true, Ordering::SeqCst) {
            return;
        }
        fail(&self.span, message, Some(error_type));
        self.end(
            ModelCallResult::Failed,
            ModelUsage::default(),
            None,
            None,
            Some((message.to_string(), error_type.to_string())),
        );
    }

    /// The caller's token stopped the call.
    pub fn cancelled(&self) {
        if self.completed.swap(true, Ordering::SeqCst) {
            return;
        }
        fail(&self.span, "cancelled", Some("cancelled"));
        let duration = self.started.elapsed();
        let labels = self.labels(None, Some("cancelled"));
        metrics::histogram!(metric_names::MODEL_DURATION, labels).record(duration.as_secs_f64());
        self.record_completed(
            ModelCallResult::Cancelled,
            ModelUsage::default(),
            None,
            duration,
            None,
        );
    }

    fn end(
        &self,
        result: ModelCallResult,
        usage: ModelUsage,
        finish_reason: Option<String>,
        response_model: Option<&str>,
        error: Option<(String, String)>,
    ) {
        let duration = self.started.elapsed();
        let labels = self.labels(
            response_model,
            error.as_ref().map(|(_, kind)| kind.as_str()),
        );
        metrics::histogram!(metric_names::MODEL_DURATION, labels).record(duration.as_secs_f64());
        self.record_completed(
            result,
            usage,
            finish_reason,
            duration,
            error.map(|(message, _)| message),
        );
    }

    fn record_completed(
        &self,
        result: ModelCallResult,
        usage: ModelUsage,
        finish_reason: Option<String>,
        duration: Duration,
        error: Option<String>,
    ) {
        if let Some(telemetry) = &self.telemetry {
            telemetry.record(
                || RunEventKind::ModelCallCompleted {
                    model: self.info.model.clone(),
                    call_id: self.call_id.to_string(),
                    result,
                    usage,
                    finish_reason,
                    duration,
                    error,
                    route: self.info.route.clone(),
                    operation: self.info.operation.clone(),
                },
                self.node_path.as_deref(),
                self.attempt,
            );
        }
    }

    // Bounded labels only: no thread, run, node path or call id on metrics
    // (design/18).
    fn labels(
        &self,
        response_model: Option<&str>,
        error_type: Option<&str>,
    ) -> Vec<metrics::Label> {
        let mut labels = vec![
            metrics::Label::new("gen_ai.operation.name", self.info.operation.clone()),
            metrics::Label::new("gen_ai.request.model", self.info.model.clone()),
        ];
        if let Some(provider) = &self.info.provider {
            labels.push(metrics::Label::new(
                "gen_ai.provider.name",
                provider.clone(),
            ));
        }
        if let Some(model) = response_model {
            labels.push(metrics::Label::new(
                "gen_ai.response.model",
                model.to_string(),
            ));
        }
        if let Some(error_type) = error_type {
            labels.push(metrics::Label::new("error.type", error_type.to_string()));
        }
        labels
    }
}

impl Drop for ModelCallScope {
    /// A call future dropped before the call finished (the engine gave up on
    /// the calling node, say) ends the call as cancelled.
    fn drop(&mut self) {
        self.cancelled();
    }
}

/// One call through a model route (`design/46-Model-Routes.md`): the way
/// `datalium-strata-ai`'s `ModelRoute` puts the call in its run's events and
/// the engine's telemetry. It opens a `strata.route` span named
/// `route <name>` (`otel.name`) under the node's attempt, starts each
/// attempt's [`ModelCallScope`] beneath it ([`Self::attempt`]), counts
/// failovers and hedges, and at [`Self::finish`] records the span's outcome,
/// `strata.route.calls`, and [`RunEventKind::ModelRouted`]. Mirrors the .NET
/// engine's `ModelRouteScope`.
///
/// Started without a node, it records no run event; the span and metrics are
/// recorded either way.
pub struct ModelRouteScope {
    telemetry: Option<Arc<RunTelemetry>>,
    route: Arc<str>,
    call_id: Arc<str>,
    node_path: Option<Arc<str>>,
    attempt: Option<u32>,
    span: Span,
    finished: AtomicBool,
}

impl ModelRouteScope {
    /// Starts recording a call through the route named `route`. `ctx` is the
    /// calling node's context, `None` outside a run. The span is a child of
    /// whatever is current, and of `ctx`'s span when nothing is.
    pub fn start(ctx: Option<&crate::context::NodeContext>, route: &str) -> Self {
        describe_metrics();
        let call_id: Arc<str> = {
            let mut text = uuid::Uuid::encode_buffer();
            Arc::from(&*uuid::Uuid::now_v7().hyphenated().encode_lower(&mut text))
        };
        let current = Span::current();
        let parent = match ctx {
            Some(ctx) if current.is_none() => ctx.span().clone(),
            _ => current,
        };
        let span = tracing::info_span!(
            target: "datalium_strata",
            parent: &parent,
            "strata.route",
            otel.name = %format!("route {route}"),
            strata.route = %route,
            strata.route.call_id = %call_id,
            strata.route.answered = Empty,
            strata.route.attempts = Empty,
            strata.thread_id = Empty,
            strata.run_id = Empty,
            strata.node.path = Empty,
            "strata.attempt" = Empty,
            error.type = Empty,
            otel.status_code = Empty,
            otel.status_message = Empty,
        );
        let mut telemetry = None;
        let mut node_path = None;
        let mut attempt = None;
        if let Some(ctx) = ctx {
            span.record("strata.thread_id", ctx.thread_id());
            if !ctx.run_id().is_empty() {
                span.record("strata.run_id", ctx.run_id());
            }
            span.record("strata.node.path", ctx.node_path());
            span.record("strata.attempt", ctx.attempt());
            telemetry = ctx.telemetry().and_then(|weak| weak.upgrade());
            node_path = Some(Arc::from(ctx.node_path()));
            attempt = Some(ctx.attempt());
        }
        Self {
            telemetry,
            route: Arc::from(route),
            call_id,
            node_path,
            attempt,
            span,
            finished: AtomicBool::new(false),
        }
    }

    /// The route call's id (a UUID v7), on its span (`strata.route.call_id`)
    /// and its `ModelRouted`.
    pub fn call_id(&self) -> &str {
        &self.call_id
    }

    /// The route call's span.
    pub fn span(&self) -> &Span {
        &self.span
    }

    /// Starts one attempt's model call, a child of the route's span, carrying
    /// the route's name (`info.route` is set when the caller left it unset).
    pub fn attempt(
        &self,
        ctx: Option<&crate::context::NodeContext>,
        mut info: ModelCallInfo,
    ) -> ModelCallScope {
        if info.route.is_none() {
            info.route = Some(self.route.to_string());
        }
        ModelCallScope::start_under(ctx, info, Some(&self.span))
    }

    /// The route moved past a candidate: `strata.route.failovers` by
    /// `reason` (`failed`, `refused`, `rejected`, `cooling_down`,
    /// `breaker_open`, `ineligible`).
    pub fn failover(&self, reason: &str) {
        metrics::counter!(
            metric_names::ROUTE_FAILOVERS,
            "strata.route" => self.route.to_string(),
            "strata.route.reason" => reason.to_string()
        )
        .increment(1);
    }

    /// Hedging started an attempt beside one still running:
    /// `strata.route.hedges`.
    pub fn hedged(&self) {
        metrics::counter!(metric_names::ROUTE_HEDGES, "strata.route" => self.route.to_string())
            .increment(1);
    }

    /// The call through the route ended, once. `outcome` is
    /// `strata.route.calls`' `strata.outcome` (`answered`, `exhausted`,
    /// `ineligible`, `fatal`, `failed`, `cancelled`); `model` the answering
    /// model when one answered; `attempts` every candidate in declaration
    /// order; `error` the error type and message when the call failed. Records
    /// `ModelRouted` (inside a run), the span's outcome and the metric; a
    /// second call does nothing.
    pub fn finish(
        &self,
        outcome: &str,
        model: Option<&str>,
        attempts: Vec<ModelRouteAttempt>,
        hedges: u32,
        duration: Duration,
        error: Option<(&str, &str)>,
    ) {
        if self.finished.swap(true, Ordering::SeqCst) {
            return;
        }
        let answered = attempts
            .iter()
            .find(|attempt| attempt.outcome == ModelRouteOutcome::Answered)
            .map(|attempt| attempt.candidate.clone());
        if let Some(answered) = &answered {
            self.span.record("strata.route.answered", answered.as_str());
        }
        let asked = attempts.iter().filter(|a| a.call_id.is_some()).count();
        self.span.record("strata.route.attempts", asked as u64);
        if let Some((error_type, message)) = error {
            fail(&self.span, message, Some(error_type));
        }
        metrics::counter!(
            metric_names::ROUTE_CALLS,
            "strata.route" => self.route.to_string(),
            "strata.outcome" => outcome.to_string()
        )
        .increment(1);
        if let Some(telemetry) = &self.telemetry {
            telemetry.record(
                || RunEventKind::ModelRouted {
                    route: self.route.to_string(),
                    call_id: self.call_id.to_string(),
                    answered,
                    model: model.map(str::to_string),
                    attempts,
                    hedges,
                    duration,
                },
                self.node_path.as_deref(),
                self.attempt,
            );
        }
    }
}

/// A model health key's breaker changed state (`design/46-Model-Routes.md`):
/// `strata.model.breaker.transitions` by `strata.model.health_key` and
/// `strata.model.breaker.state` (`open`, `half_open`, `closed`).
pub fn model_breaker_transition(health_key: &str, state: &str) {
    describe_metrics();
    metrics::counter!(
        metric_names::MODEL_BREAKER_TRANSITIONS,
        "strata.model.health_key" => health_key.to_string(),
        "strata.model.breaker.state" => state.to_string()
    )
    .increment(1);
}

fn tool_error_tag(error: &StrataError) -> &'static str {
    match error {
        StrataError::ToolNotFound(_) => "not_found",
        StrataError::ToolNotPermitted(_) => "not_permitted",
        StrataError::ToolArgumentParseFailure(_) => "invalid_arguments",
        StrataError::ToolCallDenied { .. } => "denied",
        _ => "failed",
    }
}

fn tool_result_tag(result: ToolCallResult) -> &'static str {
    match result {
        ToolCallResult::Succeeded => "succeeded",
        ToolCallResult::Failed => "failed",
        ToolCallResult::Cancelled => "cancelled",
        ToolCallResult::Refused => "refused",
    }
}

/// Marks `span` failed.
pub(crate) fn fail(span: &Span, message: &str, error_type: Option<&str>) {
    span.record("otel.status_code", "ERROR");
    span.record("otel.status_message", message);
    if let Some(error_type) = error_type {
        span.record("error.type", error_type);
    }
}

pub(crate) fn node_error_tag(error: &NodeError) -> &'static str {
    match error {
        NodeError::Transient(_) | NodeError::TransientAfter(..) => "transient",
        NodeError::Fatal(_) => "fatal",
    }
}

/// What a panic payload says, as `std` prints it.
pub(crate) fn panic_message(panic: &(dyn Any + Send)) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        message.to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "panic".to_string()
    }
}

/// The message `RunCompleted` carries for a failed run.
pub(crate) fn error_message(error: &StrataError) -> String {
    match error {
        StrataError::NodeFailed { message, .. } => message.clone(),
        StrataError::ExecutionAborted(message) => message.clone(),
        StrataError::NodeNotFound(node) => format!("node '{node}' not found"),
        StrataError::NoMatchingEdge(node) => {
            format!("no outgoing edge condition matched for node '{node}'")
        }
        StrataError::ToolNotFound(tool) => format!("tool '{tool}' not found"),
        StrataError::ToolNotPermitted(tool) => format!("tool '{tool}' is not permitted here"),
        StrataError::ToolArgumentParseFailure(message) => {
            format!("invalid tool arguments: {message}")
        }
        StrataError::ToolExecutionFailure(tool) => format!("tool '{tool}' failed"),
        StrataError::ToolCancelled(tool) => format!("tool '{tool}' was cancelled"),
        StrataError::DuplicateTool(tool) => format!("tool '{tool}' is already registered"),
        StrataError::ToolCallDenied { tool, reason } => {
            format!("tool '{tool}' was denied: {reason}")
        }
        StrataError::ToolCallFailed { tool, message } => format!("tool '{tool}' failed: {message}"),
        StrataError::HopLimitExceeded { node, hops, limit } => format!(
            "hop limit of {limit} reached: node '{node}' would be hop {}",
            u64::from(*hops) + 1
        ),
        other => format!("{other:?}"),
    }
}

fn mode_tag(mode: RunMode) -> &'static str {
    match mode {
        RunMode::Run => "run",
        RunMode::Resume => "resume",
        RunMode::End => "end",
        RunMode::Recover => "recover",
    }
}

fn outcome_tag(outcome: RunOutcome) -> &'static str {
    match outcome {
        RunOutcome::Completed => "completed",
        RunOutcome::Interrupted => "interrupted",
        RunOutcome::Failed => "failed",
        RunOutcome::Cancelled => "cancelled",
    }
}

fn node_result_tag(result: NodeResult) -> &'static str {
    match result {
        NodeResult::Committed => "committed",
        NodeResult::Failed => "failed",
        NodeResult::Interrupted => "interrupted",
        NodeResult::Cancelled => "cancelled",
    }
}

pub(crate) fn status_tag(status: CheckpointStatus) -> &'static str {
    match status {
        CheckpointStatus::Interrupted => "interrupted",
        CheckpointStatus::Completed => "completed",
        CheckpointStatus::Failed => "failed",
        CheckpointStatus::Cancelled => "cancelled",
        CheckpointStatus::Ended => "ended",
        CheckpointStatus::Running => "running",
    }
}

fn reason_tag(reason: CancelReason) -> &'static str {
    match reason {
        CancelReason::Requested => "requested",
        CancelReason::Deadline => "deadline",
        CancelReason::Ended => "ended",
        CancelReason::LeaseLost => "lease_lost",
    }
}

pin_project_lite::pin_project! {
    /// A future whose panics come back as an `Err` instead of unwinding, so
    /// the engine can end a node or a run before resuming the unwind, whoever
    /// drives the future (a caller's `block_on`, or a task tokio would
    /// otherwise catch the panic in first).
    pub(crate) struct CatchUnwind<F> {
        #[pin]
        future: F,
    }
}

impl<F> CatchUnwind<F> {
    /// Holds `future` in place rather than in a box, so catching a panic
    /// costs a node no allocation (`design/38-Rust-Executor-Overhead.md`).
    pub(crate) fn new(future: F) -> Self {
        Self { future }
    }
}

impl<F: Future> Future for CatchUnwind<F> {
    type Output = Result<F::Output, Box<dyn Any + Send>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let future = self.project().future;
        match std::panic::catch_unwind(AssertUnwindSafe(|| future.poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Err(panic) => Poll::Ready(Err(panic)),
        }
    }
}
