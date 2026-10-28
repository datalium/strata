//! The `StateGraph<S>` fluent builder and `.compile()` entry point.
//!
//! This is the minimum graph model everything else (streaming, HITL,
//! tool-calling) hangs off: nodes, edges, and a `.compile()` call that runs
//! `validate::validate` before handing back a [`CompiledGraph`].
//!
//! Every kind of node (plain, fallible, streaming, and a parallel group's
//! branches) takes either a sync handler or an async one
//! (`design/17-Async-Execution.md`): `add_node` / `add_async_node`, and so on.
//! An async handler gets the state by value (a clone; the engine keeps the
//! committed copy) and a [`NodeContext`] (thread id, node path, attempt,
//! stream, history, services, and a token that is cancelled with the run, by
//! the node's timeout, or for a branch by a failing sibling). A sync handler
//! runs on tokio's blocking pool, so it never holds up a runtime worker. See
//! `checkpoint.rs` for the execution loop (`CompiledGraph::run` /
//! `run_streaming`).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::StrataError;
use crate::context::NodeContext;
use crate::map::{
    ItemInputFn, ItemsFn, KeyFn, MapOptions, MapReducerFn, MapShape, MapSpec, run_map,
};
use crate::node::{
    EdgePlan, LevelAdopter, LevelClaimer, NodeEntry, NodeHandler, NodeKind, NodePlan,
    ResumeChecker, Suspension, on_blocking_pool, on_executor,
};
use crate::observability;
use crate::parallel::{
    BranchOutcomes, GroupShape, ParallelBranches, ParallelGroupOptions, ReducerFn, run_branches,
};
use crate::schema::{
    self, Checkpoint, CheckpointWait, GraphBranchDef, GraphDefinition, GraphEdgeDef, GraphIdentity,
    GraphInterruptDef, GraphNodeDef, GraphNodeKind, WaitKind,
};
use crate::streaming::StreamContext;
use crate::subgraph::{AttemptError, Step, SubgraphInputFn, SubgraphOutputFn, run_subgraph};
use crate::validate::validate;
use crate::wait::{BoundaryOptions, WaitOptions, WaitSpec, wake_text};

pub type NodeFn<S> = Box<dyn Fn(S) -> S + Send + Sync>;

/// A node handler that also receives a [`StreamContext`] scoped to itself,
/// so it can push [`crate::streaming::StreamChunk`]s (e.g. LLM response
/// tokens) while it runs instead of only returning a final state. Added via
/// [`StateGraph::add_streaming_node`]; only meaningful under
/// [`crate::checkpoint::CompiledGraph::run_streaming`] — running a graph
/// that contains one via the plain [`crate::checkpoint::CompiledGraph::run`]
/// fails at that node, since there's no `StreamContext` to hand it.
pub type StreamingNodeFn<S> = Box<dyn Fn(S, &StreamContext) -> S + Send + Sync>;

/// Why a fallible node's attempt failed (`design/04-Error-Recovery.md`'s
/// `NodeError::Transient` / `NodeError::Fatal`).
///
/// `Transient` failures (a timeout, a rate limit) are worth retrying under
/// the node's [`RecoveryPolicy`]; `Fatal` ones (a rejected card, bad input)
/// are not, so they skip straight to the policy's fallback — or to a
/// persisted `Failed` checkpoint if it has none. `TransientAfter` is a
/// transient failure whose cause said when to try again (a 429's
/// `Retry-After`): a policy with a [`Backoff`] waits that long before the
/// retry, capped by the backoff's `max` (`design/25-Retry-Backoff.md`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeError {
    Transient(String),
    Fatal(String),
    TransientAfter(String, Duration),
}

impl NodeError {
    /// A transient failure whose cause asked to be retried after `retry_after`.
    pub fn transient_after(message: impl Into<String>, retry_after: Duration) -> Self {
        Self::TransientAfter(message.into(), retry_after)
    }

    pub fn message(&self) -> &str {
        match self {
            Self::Transient(message) | Self::Fatal(message) | Self::TransientAfter(message, _) => {
                message
            }
        }
    }

    /// `Transient` or `TransientAfter`: worth retrying.
    pub fn is_transient(&self) -> bool {
        !matches!(self, Self::Fatal(_))
    }

    /// The hint a `TransientAfter` carries.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::TransientAfter(_, after) => Some(*after),
            _ => None,
        }
    }
}

/// How the engine reacts when a fallible node fails
/// (`design/04-Error-Recovery.md`).
///
/// - A `Transient` error is retried up to `max_retries` more times, each
///   attempt starting from the same committed state, after the `backoff`
///   delay if there is one (`design/25-Retry-Backoff.md`). A node timeout
///   ([`StateGraph::set_timeout`]) is a `Transient` error.
/// - Once retries are exhausted — or immediately, for a `Fatal` error —
///   execution routes to `fallback` if set. The fallback node runs against
///   the state as it was *before* the failed node started, and its
///   [`NodeContext::last_failure`] says why the node failed.
/// - With no `fallback`, the engine persists a `CheckpointStatus::Failed`
///   checkpoint (last committed state + the error) and returns
///   `GraphExecutionResult::Failed(StrataError::NodeFailed { .. })`.
///
/// The default policy is no retries, no backoff and no fallback. A
/// cancelled run is not a failure and never reaches the policy.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecoveryPolicy {
    pub max_retries: u32,
    pub fallback: Option<String>,
    /// How long to wait before each retry. `None` (the default): retries
    /// start at once, and a failure's retry hint is ignored.
    pub backoff: Option<Backoff>,
}

impl RecoveryPolicy {
    /// Retry a `Transient` failure up to `max_retries` more times.
    pub fn retry(max_retries: u32) -> Self {
        Self {
            max_retries,
            ..Self::default()
        }
    }

    /// Route to `node` once the node can't be recovered by retrying.
    pub fn with_fallback(mut self, node: impl Into<String>) -> Self {
        self.fallback = Some(node.into());
        self
    }

    /// Wait before each retry: the n-th waits `initial × 2^(n−1)`, capped at
    /// `max` and jittered, or the failure's retry hint capped at `max` when it
    /// has one. See [`Backoff`]. `.compile()` refuses a `max` below `initial`.
    pub fn with_backoff(mut self, initial: Duration, max: Duration, jitter: Jitter) -> Self {
        self.backoff = Some(Backoff {
            initial,
            max,
            jitter,
        });
        self
    }
}

/// How a [`Backoff`] delay is jittered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Jitter {
    /// Uniformly from `[0, d]`. The default: it spreads synchronized retries best.
    #[default]
    Full,
    /// Uniformly from `[d/2, d]`.
    Equal,
    /// Exactly `d`.
    None,
}

/// Exponential backoff between retries (`design/25-Retry-Backoff.md`): the
/// n-th retry waits `d = initial × 2^(n−1)`, capped at `max`, jittered by
/// `jitter`. A failure that carries a retry hint
/// ([`NodeError::TransientAfter`]) waits the hint instead, capped at `max` and
/// not jittered. Constant backoff is `initial == max`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    /// The delay before the first retry, before jitter.
    pub initial: Duration,
    /// The longest delay, computed or hinted.
    pub max: Duration,
    pub jitter: Jitter,
}

impl Backoff {
    /// The wait before the `retry`-th retry (1-based) after a failure that
    /// carried `hint`, with `sample` (uniform in `[0, 1)`) as the jitter.
    pub(crate) fn delay_before(&self, retry: u32, hint: Option<Duration>, sample: f64) -> Duration {
        if let Some(hint) = hint {
            return hint.min(self.max);
        }
        let ceiling = self.ceiling(retry);
        match self.jitter {
            Jitter::Full => ceiling.mul_f64(sample),
            Jitter::Equal => ceiling / 2 + (ceiling / 2).mul_f64(sample),
            Jitter::None => ceiling,
        }
    }

    /// `initial × 2^(retry−1)`, saturating at `max`.
    fn ceiling(&self, retry: u32) -> Duration {
        let mut delay = self.initial;
        let mut doubled = 1;
        while doubled < retry && !delay.is_zero() && delay < self.max {
            delay = delay.checked_mul(2).unwrap_or(self.max);
            doubled += 1;
        }
        delay.min(self.max)
    }
}

/// A sample uniform in `[0, 1)` for backoff jitter: std's per-hasher random
/// keys, so the engine needs no RNG dependency for it.
pub(crate) fn jitter_sample() -> f64 {
    use std::hash::{BuildHasher, Hasher, RandomState};
    let bits = RandomState::new().build_hasher().finish();
    (bits >> 11) as f64 / (1u64 << 53) as f64
}

/// Why a node failed, as the fallback node its [`RecoveryPolicy`] routed to
/// sees it ([`NodeContext::last_failure`], `design/25-Retry-Backoff.md`): the
/// final failure, the one that made the policy give up. Mirrors the .NET
/// engine's `NodeFailure`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct NodeFailure {
    /// The node that failed, by its own name: `"rca"`.
    pub node: String,
    /// Its qualified path, the one run events use: `"investigate/rca"` inside
    /// the subgraph node `investigate`.
    pub node_path: String,
    /// The final failure: its kind, message and retry hint. For a group or a
    /// subgraph, the combined failure the node reported.
    pub error: NodeError,
    /// Attempts made, retries included.
    pub attempts: u32,
    /// The final attempt ran over the node's timeout
    /// ([`StateGraph::set_timeout`]).
    pub timed_out: bool,
}

impl NodeFailure {
    /// A failure of `node` (also its path) after one attempt, for a context
    /// built by hand.
    pub fn new(node: impl Into<String>, error: NodeError) -> Self {
        let node = node.into();
        Self {
            node_path: node.clone(),
            node,
            error,
            attempts: 1,
            timed_out: false,
        }
    }

    pub fn with_node_path(mut self, path: impl Into<String>) -> Self {
        self.node_path = path.into();
        self
    }

    pub fn with_attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts;
        self
    }

    pub fn with_timed_out(mut self, timed_out: bool) -> Self {
        self.timed_out = timed_out;
        self
    }
}

/// A runtime predicate for a conditional edge (see
/// [`StateGraph::add_conditional_edge`]), evaluated against the state
/// produced by the edge's `from` node.
pub type EdgeCondition<S> = Box<dyn Fn(&S) -> bool + Send + Sync>;

/// A parallel group's reducer as the engine stores it: every reducer becomes
/// one over the branches' outcomes.
type OutcomesReducer<S> = Arc<dyn Fn(&S, BranchOutcomes<S>) -> S + Send + Sync>;

pub struct StateGraph<S> {
    entry_point: Option<String>,
    node_order: Vec<String>,
    // Every node, whatever kind, in the one shape the executor runs (see
    // `NodeEntry`).
    nodes: HashMap<String, NodeEntry<S>>,
    // Group name -> branch names and branch metadata, kept so `.compile()` can
    // reject an empty group or duplicate branch names and the definition can
    // describe the group. The branches themselves live inside the group's
    // handler.
    parallel_groups: HashMap<String, GroupShape>,
    // Subgraph name -> the child's definition, snapshotted when it was added
    // (`design/22-Graph-Structure-In-Definitions.md`).
    subgraphs: HashMap<String, GraphDefinition>,
    // Map name -> its template, exported join, the child's first interrupt
    // boundary and its limits (`design/27-Map-Fan-Out.md`).
    maps: HashMap<String, MapShape>,
    // Node name -> its metadata. Kept apart from the node, so re-adding a name
    // keeps it.
    node_metadata: BTreeMap<String, BTreeMap<String, String>>,
    recovery_policies: HashMap<String, RecoveryPolicy>,
    timeouts: HashMap<String, Duration>,
    edges: Vec<GraphEdgeDef>,
    // Parallel to `edges` (same index = same edge) rather than keyed on the
    // edge itself, since closures aren't `Eq`/`Hash` and `GraphEdgeDef` is
    // the serializable interop shape — it has no room for one anyway.
    edge_conditions: Vec<Option<EdgeCondition<S>>>,
    interrupt_boundaries: HashSet<String>,
    // Boundary -> the node a rejection continues at
    // (`design/21-Typed-Human-Signals.md`).
    reject_routes: HashMap<String, String>,
    // Wait node -> what it waits for (`design/29-Event-Waits-And-Timers.md`).
    // Kept with the node: re-adding the name as another kind of node drops it,
    // and its timeout route.
    waits: HashMap<String, WaitSpec<S>>,
    // Boundary -> how long it waits for a human before timing out.
    boundary_timeouts: HashMap<String, Duration>,
    // Boundary or event wait -> the node a timeout continues at.
    timeout_routes: HashMap<String, String>,
}

impl<S> Default for StateGraph<S> {
    fn default() -> Self {
        Self {
            entry_point: None,
            node_order: Vec::new(),
            nodes: HashMap::new(),
            parallel_groups: HashMap::new(),
            subgraphs: HashMap::new(),
            maps: HashMap::new(),
            node_metadata: BTreeMap::new(),
            recovery_policies: HashMap::new(),
            timeouts: HashMap::new(),
            edges: Vec::new(),
            edge_conditions: Vec::new(),
            interrupt_boundaries: HashSet::new(),
            reject_routes: HashMap::new(),
            waits: HashMap::new(),
            boundary_timeouts: HashMap::new(),
            timeout_routes: HashMap::new(),
        }
    }
}

/// Wraps an async handler's future so it reports a [`Step`].
fn async_handler<S, F, Fut, T: 'static>(
    handler: F,
    done: fn(T) -> Result<S, NodeError>,
) -> NodeHandler<S>
where
    S: Send + 'static,
    F: Fn(S, NodeContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = T> + Send + 'static,
{
    Arc::new(move |state, inputs| {
        let future = handler(state, inputs.context);
        Box::pin(async move {
            done(future.await)
                .map(Step::Done)
                .map_err(AttemptError::from)
        })
    })
}

/// The shape every running graph's state must have: the engine clones the
/// committed state into each attempt, and runs handlers on other threads and
/// tasks (`design/17-Async-Execution.md`).
impl<S> StateGraph<S>
where
    S: Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `name` as a node (entry point + declaration order) and
    /// stores `entry` under it, so re-adding a name replaces its handler
    /// whatever kind the old one was.
    fn claim_node_name(&mut self, name: String, entry: NodeEntry<S>) {
        if self.entry_point.is_none() {
            self.entry_point = Some(name.clone());
        }
        if !self.nodes.contains_key(&name) {
            self.node_order.push(name.clone());
        }
        self.parallel_groups.remove(&name);
        self.subgraphs.remove(&name);
        self.maps.remove(&name);
        if self.waits.remove(&name).is_some() {
            self.timeout_routes.remove(&name);
        }
        self.nodes.insert(name, entry);
    }

    fn entry(kind: NodeKind, handler: NodeHandler<S>) -> NodeEntry<S> {
        NodeEntry {
            kind,
            handler,
            needs_stream: kind == NodeKind::Streaming,
            is_subgraph: false,
            check_resume: None,
            winds_down: false,
            boundary: None,
            adopt: None,
            claim: None,
            composite: false,
            inline: false,
            plan: NodePlan::default(),
        }
    }

    /// Adds a node. The first node added becomes the entry point unless
    /// overridden via [`Self::set_entry_point`]. The handler runs on tokio's
    /// blocking pool. A handler that only computes for a few microseconds is
    /// cheaper as [`Self::add_inline_node`].
    pub fn add_node<F>(mut self, name: impl Into<String>, handler: F) -> Self
    where
        F: Fn(S) -> S + Send + Sync + 'static,
    {
        let handler = Arc::new(handler);
        let run: NodeHandler<S> = Arc::new(move |state, _| {
            let handler = handler.clone();
            on_blocking_pool(move || Ok(handler(state)))
        });
        self.claim_node_name(name.into(), Self::entry(NodeKind::Plain, run));
        self
    }

    /// Adds a node with an async handler (`design/17-Async-Execution.md`). A
    /// timeout set with [`Self::set_timeout`] fails it like a fallible node
    /// with no policy. [`NodeContext::stream`] is `Some` whenever the run is
    /// streaming.
    pub fn add_async_node<F, Fut>(mut self, name: impl Into<String>, handler: F) -> Self
    where
        F: Fn(S, NodeContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = S> + Send + 'static,
    {
        let run = async_handler(handler, Ok);
        self.claim_node_name(name.into(), Self::entry(NodeKind::Plain, run));
        self
    }

    /// Adds a streaming node: like [`Self::add_node`], but the handler also
    /// receives a [`StreamContext`] scoped to this node so it can push
    /// output chunks (e.g. LLM response tokens) as it runs. Only usable
    /// under [`CompiledGraph::run_streaming`] — see [`StreamingNodeFn`]. The
    /// handler runs on tokio's blocking pool, where its `_blocking` emits are
    /// allowed.
    pub fn add_streaming_node<F>(mut self, name: impl Into<String>, handler: F) -> Self
    where
        F: Fn(S, &StreamContext) -> S + Send + Sync + 'static,
    {
        let handler = Arc::new(handler);
        let run: NodeHandler<S> = Arc::new(move |state, inputs| {
            let handler = handler.clone();
            let stream = inputs.stream;
            on_blocking_pool(move || match stream {
                Some(ctx) => Ok(handler(state, &ctx)),
                // The executor refuses a streaming node on a run with no
                // context, so this is only a guard.
                None => Err(NodeError::Fatal(
                    "streaming node has no StreamContext \u{2014} use run_streaming instead of run"
                        .to_string(),
                )),
            })
        });
        self.claim_node_name(name.into(), Self::entry(NodeKind::Streaming, run));
        self
    }

    /// Adds a streaming node with an async handler
    /// (`design/17-Async-Execution.md`): its [`NodeContext::stream`] is always
    /// `Some`, since a plain `run` refuses the node before running it. Emit
    /// with the async methods ([`StreamContext::emit_token`]).
    pub fn add_async_streaming_node<F, Fut>(mut self, name: impl Into<String>, handler: F) -> Self
    where
        F: Fn(S, NodeContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = S> + Send + 'static,
    {
        let run = async_handler(handler, Ok);
        self.claim_node_name(name.into(), Self::entry(NodeKind::Streaming, run));
        self
    }

    /// Adds a node whose handler can fail: it borrows the state and returns
    /// the new state or a [`NodeError`]. What happens on failure is decided
    /// by the node's [`RecoveryPolicy`] — see [`Self::set_recovery_policy`];
    /// without one, the first failure ends the run with a persisted
    /// `Failed` checkpoint. The handler runs on tokio's blocking pool. A
    /// handler that only computes for a few microseconds is cheaper as
    /// [`Self::add_inline_fallible_node`].
    pub fn add_fallible_node<F>(mut self, name: impl Into<String>, handler: F) -> Self
    where
        F: Fn(&S) -> Result<S, NodeError> + Send + Sync + 'static,
    {
        let handler = Arc::new(handler);
        let run: NodeHandler<S> = Arc::new(move |state, _| {
            let handler = handler.clone();
            on_blocking_pool(move || handler(&state))
        });
        self.claim_node_name(name.into(), Self::entry(NodeKind::Fallible, run));
        self
    }

    /// Adds a fallible node with an async handler
    /// (`design/17-Async-Execution.md`): `Err(NodeError::...)` hands the
    /// failure to the node's [`RecoveryPolicy`], as from
    /// [`Self::add_fallible_node`]. Each attempt gets its own clone of the
    /// committed state, [`NodeContext::attempt`] and token.
    pub fn add_async_fallible_node<F, Fut>(mut self, name: impl Into<String>, handler: F) -> Self
    where
        F: Fn(S, NodeContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<S, NodeError>> + Send + 'static,
    {
        let run = async_handler(handler, std::convert::identity);
        self.claim_node_name(name.into(), Self::entry(NodeKind::Fallible, run));
        self
    }

    /// Adds a node whose sync handler runs **on the executor**, not on tokio's
    /// blocking pool (`design/39-Inline-Sync-Handlers.md`): the same handler
    /// as [`Self::add_node`], without the two thread hand-offs a node that
    /// costs there. For a handler that computes and returns: routing, parsing,
    /// merging or checking state.
    ///
    /// **It must not block.** No I/O, no sleeping, no waiting on a lock
    /// another thread may hold for long, no synchronous store call, no
    /// `block_on`: while it runs it holds a runtime worker (on a current-thread
    /// runtime, the whole runtime). Tens of microseconds, not milliseconds. A
    /// handler that may wait belongs in [`Self::add_node`] or an async node.
    ///
    /// **It can't be abandoned.** `.compile()` refuses a timeout on it
    /// ([`Self::set_timeout`]). A cancellation or deadline that arrives while
    /// it runs takes effect when it returns: its result is committed, as if
    /// the token had fired just after the node. It gets no stream context, so
    /// it can't emit. A panic, its span, its events and its metrics are those
    /// of any node.
    pub fn add_inline_node<F>(mut self, name: impl Into<String>, handler: F) -> Self
    where
        F: Fn(S) -> S + Send + Sync + 'static,
    {
        let handler = Arc::new(handler);
        let run: NodeHandler<S> = Arc::new(move |state, _| {
            let handler = handler.clone();
            on_executor(move || Ok(handler(state)))
        });
        let mut entry = Self::entry(NodeKind::Plain, run);
        entry.inline = true;
        self.claim_node_name(name.into(), entry);
        self
    }

    /// A fallible [`Self::add_inline_node`]: the same handler as
    /// [`Self::add_fallible_node`], run on the executor. Its
    /// [`RecoveryPolicy`] applies as to any fallible node (retries, backoff,
    /// fallback). Otherwise as [`Self::add_inline_node`]: it must not block,
    /// and `.compile()` refuses a timeout on it.
    pub fn add_inline_fallible_node<F>(mut self, name: impl Into<String>, handler: F) -> Self
    where
        F: Fn(&S) -> Result<S, NodeError> + Send + Sync + 'static,
    {
        let handler = Arc::new(handler);
        let run: NodeHandler<S> = Arc::new(move |state, _| {
            let handler = handler.clone();
            on_executor(move || handler(&state))
        });
        let mut entry = Self::entry(NodeKind::Fallible, run);
        entry.inline = true;
        self.claim_node_name(name.into(), entry);
        self
    }

    /// Attaches a [`RecoveryPolicy`] to a fallible node (which includes a
    /// parallel group — see [`Self::add_parallel_group`]). `.compile()`
    /// rejects a policy on a node that isn't fallible, or whose fallback
    /// names a node that doesn't exist. The fallback route counts as a
    /// guarded edge for validation (STRATA-SPEC-004), so a fallback node
    /// needs no ordinary edge into it to avoid being flagged as an orphan.
    pub fn set_recovery_policy(mut self, node: impl Into<String>, policy: RecoveryPolicy) -> Self {
        self.recovery_policies.insert(node.into(), policy);
        self
    }

    /// Limits each attempt of `node` to `timeout`
    /// (`design/17-Async-Execution.md`). An attempt that runs over is a
    /// `Transient` failure, `node '<name>' timed out after <n> ms`: a fallible
    /// node's [`RecoveryPolicy`] retries it (each retry gets a fresh timeout)
    /// or falls back, and any other node fails the run with a `Failed`
    /// checkpoint. The handler's token is cancelled and its future dropped; a
    /// sync handler can't see that, so it runs on and its result is
    /// discarded. On a parallel group the timeout covers the whole group, on a
    /// subgraph the whole child. `.compile()` rejects a zero timeout, one on
    /// a node that doesn't exist, and one on an inline node
    /// ([`Self::add_inline_node`]), which nothing can abandon.
    pub fn set_timeout(mut self, node: impl Into<String>, timeout: Duration) -> Self {
        self.timeouts.insert(node.into(), timeout);
        self
    }

    /// Describes `node` in the exported definition
    /// (`design/22-Graph-Structure-In-Definitions.md`): string keys and
    /// values, such as the agent a node runs or its model policy, for a
    /// console or a CI diff to read. The engine never reads it. Works for
    /// every kind of node, a parallel group or subgraph included (a group's
    /// branches take theirs from [`ParallelBranches::set_metadata`]).
    ///
    /// Setting it again replaces it, and re-adding the node keeps it.
    /// `.compile()` rejects metadata for a name that isn't a node, an empty
    /// key, and a key starting `strata.` (kept for the engine, as for run
    /// metadata). Keys are exported sorted, so the same graph exports the
    /// same bytes every time.
    pub fn set_node_metadata<K, V>(
        mut self,
        node: impl Into<String>,
        metadata: impl IntoIterator<Item = (K, V)>,
    ) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.node_metadata.insert(
            node.into(),
            metadata
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        );
        self
    }

    /// Adds a direct edge, always taken once `from` finishes executing.
    /// Only guarded edges (see [`Self::add_conditional_edge`]) are allowed
    /// to close a cycle.
    pub fn add_edge(self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.add_edge_with_guard(from, to, None)
    }

    /// Adds an edge whose `to` node is only taken if `condition` returns
    /// `true` for the state `from` just produced. Exempt from the
    /// unguarded-cycle check (STRATA_ERR_UNGUARDED_CYCLE), since the
    /// runtime predicate — not the graph's static shape — is what keeps
    /// the loop from running forever.
    ///
    /// When a node has more than one outgoing edge, [`CompiledGraph::run`]
    /// tries them in the order they were added and takes the first whose
    /// condition matches; a plain [`Self::add_edge`] always matches, so
    /// adding one last acts as a default "else" branch.
    pub fn add_conditional_edge<F>(
        self,
        from: impl Into<String>,
        to: impl Into<String>,
        condition: F,
    ) -> Self
    where
        F: Fn(&S) -> bool + Send + Sync + 'static,
    {
        self.add_edge_with_guard(from, to, Some(Box::new(condition)))
    }

    fn add_edge_with_guard(
        mut self,
        from: impl Into<String>,
        to: impl Into<String>,
        condition: Option<EdgeCondition<S>>,
    ) -> Self {
        self.edges.push(GraphEdgeDef {
            from: from.into(),
            to: to.into(),
            guarded: condition.is_some(),
            condition: None,
        });
        self.edge_conditions.push(condition);
        self
    }

    pub fn set_entry_point(mut self, name: impl Into<String>) -> Self {
        self.entry_point = Some(name.into());
        self
    }

    /// Marks a node as an interrupt boundary (STRATA-SPEC-002): once this
    /// node finishes executing, the engine freezes, persists, and returns
    /// control rather than continuing to the next node. A name that isn't
    /// (yet) an added node is accepted but simply never triggers. Marking a
    /// boundary that has a reject route or a timeout again with this removes
    /// them.
    pub fn add_interrupt_boundary(self, name: impl Into<String>) -> Self {
        self.add_interrupt_boundary_with(name, BoundaryOptions::new())
    }

    /// [`Self::add_interrupt_boundary`], with a reject route: a resume whose
    /// [`HumanSignal::is_approved`](crate::checkpoint::HumanSignal::is_approved)
    /// is `false` continues at `on_reject` instead of the next node fixed when
    /// the boundary froze (`design/21-Typed-Human-Signals.md`). Only a resume
    /// of this boundary's own interrupted checkpoint takes the route: never a
    /// failed or cancelled thread's, and never an outer level's.
    ///
    /// The route is a guarded `on_reject` edge to validation and in
    /// [`Self::to_definition`], so `.compile()` rejects one to a node that
    /// doesn't exist (and one on a boundary name that isn't a node) as a
    /// dangling edge. Marking the same name again replaces its route.
    pub fn add_interrupt_boundary_with_reject(
        self,
        name: impl Into<String>,
        on_reject: impl Into<String>,
    ) -> Self {
        self.add_interrupt_boundary_with(name, BoundaryOptions::new().with_reject(on_reject))
    }

    /// [`Self::add_interrupt_boundary`] with `options`: a reject route
    /// ([`Self::add_interrupt_boundary_with_reject`]), and a timeout
    /// (`design/29-Event-Waits-And-Timers.md`). With a timeout the boundary
    /// waits for a human at most that long: its checkpoint carries a `wait` of
    /// kind `human` whose `wake_at` is when it times out, and
    /// [`CompiledGraph::wake_due`] times it out once that has passed: it
    /// continues at the timeout route, or, with none, the thread is ended with
    /// `strata.end_reason` `timed_out`. The route is a guarded `on_timeout`
    /// edge, like the reject route. Marking the name again replaces all of it.
    /// `.compile()` refuses a zero timeout, and a timeout route with no timeout.
    pub fn add_interrupt_boundary_with(
        mut self,
        name: impl Into<String>,
        options: BoundaryOptions,
    ) -> Self {
        let name = name.into();
        match options.on_reject {
            Some(route) => self.reject_routes.insert(name.clone(), route),
            None => self.reject_routes.remove(&name),
        };
        match options.timeout {
            Some(timeout) => self.boundary_timeouts.insert(name.clone(), timeout),
            None => self.boundary_timeouts.remove(&name),
        };
        self.set_timeout_route(&name, options.on_timeout);
        self.interrupt_boundaries.insert(name);
        self
    }

    /// Adds a wait node that waits for an external event
    /// (`design/29-Event-Waits-And-Timers.md`): the node passes its state
    /// through, and the run freezes after it, as at an interrupt boundary,
    /// with a `wait` of kind `event` for `event_type`. Only
    /// [`CompiledGraph::signal`] with that event type answers it: its overrides
    /// replace fields of the state, and the thread continues at the edge out
    /// of the node. It never times out; see [`Self::add_wait_for_event_with`].
    pub fn add_wait_for_event(
        self,
        name: impl Into<String>,
        event_type: impl Into<String>,
    ) -> Self {
        self.add_wait_for_event_with(name, event_type, WaitOptions::new())
    }

    /// [`Self::add_wait_for_event`] with `options`: with a timeout,
    /// [`CompiledGraph::wake_due`] times the wait out once that long has
    /// passed: it continues at the timeout route (a guarded `on_timeout`
    /// edge), or, with none, the thread is ended with `strata.end_reason`
    /// `timed_out`.
    pub fn add_wait_for_event_with(
        self,
        name: impl Into<String>,
        event_type: impl Into<String>,
        options: WaitOptions,
    ) -> Self {
        let wait = WaitSpec {
            kind: WaitKind::Event,
            event_type: Some(event_type.into()),
            delay: None,
            wake_at: None,
            timeout: options.timeout,
        };
        self.add_wait(name.into(), wait, options.on_timeout)
    }

    /// Adds a timer (`design/29-Event-Waits-And-Timers.md`): a wait node after
    /// which the run freezes, as at an interrupt boundary, with a `wait` of
    /// kind `timer` due `delay` after the node ran, rounded up to a whole
    /// second. [`CompiledGraph::wake_due`] wakes it once that has passed, and
    /// it continues at the edge out of the node. Nothing else answers a timer;
    /// ending the thread abandons it. The thread holds nothing while it waits.
    /// `.compile()` refuses a zero delay.
    pub fn add_timer(self, name: impl Into<String>, delay: Duration) -> Self {
        let wait = WaitSpec {
            kind: WaitKind::Timer,
            event_type: None,
            delay: Some(delay),
            wake_at: None,
            timeout: None,
        };
        self.add_wait(name.into(), wait, None)
    }

    /// [`Self::add_timer`], due at the time `wake_at` computes from the state
    /// when the node runs, rounded up to a whole second. A time already past
    /// is due at once.
    pub fn add_timer_until<F>(self, name: impl Into<String>, wake_at: F) -> Self
    where
        F: Fn(&S) -> DateTime<Utc> + Send + Sync + 'static,
    {
        let wait = WaitSpec {
            kind: WaitKind::Timer,
            event_type: None,
            delay: None,
            wake_at: Some(Arc::new(wake_at)),
            timeout: None,
        };
        self.add_wait(name.into(), wait, None)
    }

    fn add_wait(mut self, name: String, wait: WaitSpec<S>, on_timeout: Option<String>) -> Self {
        // A wait node does nothing itself: the run freezes after it.
        let pass_through: NodeHandler<S> =
            Arc::new(|state, _| Box::pin(async move { Ok(Step::Done(state)) }));
        self.claim_node_name(name.clone(), Self::entry(NodeKind::Plain, pass_through));
        self.waits.insert(name.clone(), wait);
        self.set_timeout_route(&name, on_timeout);
        self
    }

    fn set_timeout_route(&mut self, name: &str, on_timeout: Option<String>) {
        match on_timeout {
            Some(route) => self.timeout_routes.insert(name.to_string(), route),
            None => self.timeout_routes.remove(name),
        };
    }

    /// The graph as the interop shape `schema/graph-definition.schema.json`
    /// (what `strata graph validate` reads), **without validating it** — so a
    /// graph `.compile()` would reject can still be written out and gated in
    /// CI. Nodes are in declaration order, then the edges in the order they
    /// were added, then one guarded `on_failure` edge per recovery-policy
    /// fallback (in node declaration order), then one guarded `on_reject` edge
    /// per interrupt boundary's reject route
    /// (likewise), then one guarded `on_timeout` edge per wait's timeout route
    /// (`design/29-Event-Waits-And-Timers.md`): the same view `.compile()`
    /// validates, so the CLI and `.compile()` agree on a node reachable only
    /// that way. A wait node is `kind: "wait"`, with what it waits for.
    ///
    /// Each node carries its metadata ([`Self::set_node_metadata`]). A
    /// parallel group is `kind: "parallel_group"` with its branches (name and
    /// metadata, in declaration order), and a subgraph is `kind: "subgraph"`
    /// with its child's whole definition, recursively
    /// (`design/22-Graph-Structure-In-Definitions.md`). A definition with
    /// either at its own level is `schema_version` `1.1.0`; any other is
    /// `1.0.0`, exactly as before. Metadata set for a name that isn't a node or
    /// branch is left out.
    ///
    /// An interrupt boundary that is a node carries `interrupt`, with its
    /// timeout when it has one, and puts its level at `schema_version` `1.6.0`
    /// (`design/48-Boundaries-In-Definitions.md`).
    ///
    /// Only the structure is exported. Handlers, edge predicates (an edge's
    /// `condition` text is empty; only its `guarded` flag is recorded), node
    /// timeouts and recovery policies' retry counts are not.
    pub fn to_definition(&self) -> GraphDefinition {
        with_routes(
            with_routes(
                with_fallback_edges(self.plain_definition(), &self.recovery_policies),
                &self.reject_routes,
                "on_reject",
            ),
            &self.timeout_routes,
            "on_timeout",
        )
    }

    /// Nodes and ordinary edges only — what a compiled graph routes over.
    fn plain_definition(&self) -> GraphDefinition {
        let nodes: Vec<GraphNodeDef> = self
            .node_order
            .iter()
            .map(|name| self.node_definition(name))
            .collect();
        GraphDefinition {
            schema_version: schema::graph_version_of(None, &nodes).to_string(),
            identity: None,
            entry_point: self.entry_point.clone().unwrap_or_default(),
            nodes,
            edges: self.edges.clone(),
        }
    }

    fn node_definition(&self, name: &str) -> GraphNodeDef {
        let mut node = GraphNodeDef {
            name: name.to_string(),
            metadata: self.node_metadata.get(name).cloned(),
            ..GraphNodeDef::default()
        };
        if let Some(group) = self.parallel_groups.get(name) {
            node.kind = Some(GraphNodeKind::ParallelGroup);
            node.branches = Some(
                group
                    .branches
                    .iter()
                    .map(|branch| GraphBranchDef {
                        name: branch.clone(),
                        metadata: group.metadata.get(branch).cloned(),
                    })
                    .collect(),
            );
            node.join = group.join.clone();
        } else if let Some(child) = self.subgraphs.get(name) {
            node.kind = Some(GraphNodeKind::Subgraph);
            node.graph = Some(Box::new(child.clone()));
        } else if let Some(map) = self.maps.get(name) {
            node.kind = Some(GraphNodeKind::Map);
            node.graph = Some(Box::new(map.template.clone()));
            node.join = map.join.clone();
        } else if let Some(wait) = self.waits.get(name) {
            node.kind = Some(GraphNodeKind::Wait);
            node.wait = Some(wait.exported());
        }
        // A wait node can't also be a boundary (`.compile()` refuses it), so
        // the export never says it is (design/48-Boundaries-In-Definitions.md).
        if self.interrupt_boundaries.contains(name) && !self.waits.contains_key(name) {
            node.interrupt = Some(GraphInterruptDef {
                timeout_ms: self
                    .boundary_timeouts
                    .get(name)
                    .map(|timeout| timeout.as_millis() as u64),
            });
        }
        node
    }

    /// `.compile()`'s wait rules (`design/29-Event-Waits-And-Timers.md`): no
    /// zero delay or timeout, no timeout route without a timeout, and no name
    /// that is both a wait node and an interrupt boundary. Checked in node
    /// order, then boundary names in byte order, so the first error doesn't
    /// depend on hashing.
    fn check_waits(&self) -> Result<(), StrataError> {
        for name in &self.node_order {
            let Some(wait) = self.waits.get(name) else {
                continue;
            };
            if wait.delay.is_some_and(|delay| delay.is_zero()) {
                return Err(StrataError::CompilationFailed(format!(
                    "timer '{name}' has a delay of zero; it must be greater than zero"
                )));
            }
            if wait.timeout.is_some_and(|timeout| timeout.is_zero()) {
                return Err(StrataError::CompilationFailed(format!(
                    "wait '{name}' has a timeout of zero; it must be greater than zero"
                )));
            }
            if self.timeout_routes.contains_key(name) && wait.timeout.is_none() {
                return Err(StrataError::CompilationFailed(format!(
                    "wait '{name}' has a timeout route but no timeout"
                )));
            }
            if self.interrupt_boundaries.contains(name) {
                return Err(StrataError::CompilationFailed(format!(
                    "'{name}' is a wait node and can't also be an interrupt boundary"
                )));
            }
        }
        let mut boundaries: Vec<&String> = self.interrupt_boundaries.iter().collect();
        boundaries.sort();
        for name in boundaries {
            if self.waits.contains_key(name) {
                continue;
            }
            if self
                .boundary_timeouts
                .get(name)
                .is_some_and(|timeout| timeout.is_zero())
            {
                return Err(StrataError::CompilationFailed(format!(
                    "interrupt boundary '{name}' has a timeout of zero; it must be greater than zero"
                )));
            }
            if self.timeout_routes.contains_key(name) && !self.boundary_timeouts.contains_key(name)
            {
                return Err(StrataError::CompilationFailed(format!(
                    "interrupt boundary '{name}' has a timeout route but no timeout"
                )));
            }
        }
        Ok(())
    }

    /// `.compile()`'s metadata rules: every node metadata entry names a node,
    /// every branch metadata entry a branch of its group, and no key is empty
    /// or starts `strata.`. Nodes are checked in name order, then groups in
    /// node order, so the first error reported doesn't depend on hashing.
    fn check_metadata(&self) -> Result<(), StrataError> {
        for (node, metadata) in &self.node_metadata {
            if !self.nodes.contains_key(node) {
                return Err(StrataError::CompilationFailed(format!(
                    "metadata set for '{node}', which does not exist"
                )));
            }
            check_metadata_keys(metadata, &format!("node '{node}'"))?;
        }
        for group_name in &self.node_order {
            let Some(group) = self.parallel_groups.get(group_name) else {
                continue;
            };
            for (branch, metadata) in &group.metadata {
                let owner = format!("branch '{branch}' of parallel group '{group_name}'");
                if !group.branches.contains(branch) {
                    return Err(StrataError::CompilationFailed(format!(
                        "metadata set for {owner}, which does not exist"
                    )));
                }
                check_metadata_keys(metadata, &owner)?;
            }
        }
        Ok(())
    }

    /// Runs compile-time validation (STRATA-SPEC-004) and, if the graph is
    /// structurally sound, returns a [`CompiledGraph`]. Structural errors
    /// abort here rather than surfacing at runtime.
    pub fn compile(self) -> Result<CompiledGraph<S>, StrataError> {
        self.compile_with(CompileOptions::default())
    }

    /// [`Self::compile`] with `options`: a loop circuit breaker
    /// ([`CompileOptions::with_max_node_hops`],
    /// `design/24-Loop-Circuit-Breaker.md`). A limit of 0 is refused
    /// (`CompilationFailed`).
    pub fn compile_with(self, options: CompileOptions) -> Result<CompiledGraph<S>, StrataError> {
        if options.max_node_hops == Some(0) {
            return Err(StrataError::CompilationFailed(
                "max_node_hops must be at least 1".to_string(),
            ));
        }
        let identity = options.identity()?;

        for (group, shape) in &self.parallel_groups {
            let branches = &shape.branches;
            if branches.is_empty() {
                return Err(StrataError::CompilationFailed(format!(
                    "parallel group '{group}' has no branches"
                )));
            }
            for (index, branch) in branches.iter().enumerate() {
                if branches[..index].contains(branch) {
                    return Err(StrataError::CompilationFailed(format!(
                        "parallel group '{group}' has more than one branch named '{branch}'"
                    )));
                }
            }
            if shape.max_concurrency == Some(0) {
                return Err(StrataError::CompilationFailed(format!(
                    "parallel group '{group}' has a max_concurrency of 0; it must be at least 1"
                )));
            }
            if shape
                .branch_timeout
                .is_some_and(|timeout| timeout.is_zero())
            {
                return Err(StrataError::CompilationFailed(format!(
                    "parallel group '{group}' has a branch timeout of zero; it must be greater than zero"
                )));
            }
        }

        for name in &self.node_order {
            let Some(map) = self.maps.get(name) else {
                continue;
            };
            if map.max_concurrency == 0 {
                return Err(StrataError::CompilationFailed(format!(
                    "map '{name}' has a max_concurrency of 0; it must be at least 1"
                )));
            }
            if map.item_timeout.is_some_and(|timeout| timeout.is_zero()) {
                return Err(StrataError::CompilationFailed(format!(
                    "map '{name}' has an item timeout of zero; it must be greater than zero"
                )));
            }
            if map.max_items == Some(0) {
                return Err(StrataError::CompilationFailed(format!(
                    "map '{name}' has a max_items of 0; it must be at least 1"
                )));
            }
            if let Some(boundary) = &map.boundary {
                return Err(StrataError::CompilationFailed(format!(
                    "map '{name}' runs a graph with {boundary}; a map item can't freeze"
                )));
            }
        }

        self.check_waits()?;

        for (node, policy) in &self.recovery_policies {
            if self.nodes.get(node).map(|entry| entry.kind) != Some(NodeKind::Fallible) {
                return Err(StrataError::CompilationFailed(format!(
                    "recovery policy set for '{node}', which is not a fallible node"
                )));
            }
            if let Some(fallback) = &policy.fallback
                && !self.node_order.contains(fallback)
            {
                return Err(StrataError::CompilationFailed(format!(
                    "recovery policy for '{node}' falls back to '{fallback}', which does not exist"
                )));
            }
            if let Some(backoff) = &policy.backoff
                && backoff.max < backoff.initial
            {
                return Err(StrataError::CompilationFailed(format!(
                    "recovery policy for '{node}' has a backoff whose max ({} ms) is below its initial delay ({} ms)",
                    backoff.max.as_millis(),
                    backoff.initial.as_millis()
                )));
            }
        }

        for (node, timeout) in &self.timeouts {
            if !self.nodes.contains_key(node) {
                return Err(StrataError::CompilationFailed(format!(
                    "timeout set for '{node}', which does not exist"
                )));
            }
            if timeout.is_zero() {
                return Err(StrataError::CompilationFailed(format!(
                    "timeout for '{node}' must be greater than zero"
                )));
            }
            if self.nodes[node].inline {
                return Err(StrataError::CompilationFailed(format!(
                    "node '{node}' runs inline and can't be timed out: add it with add_node or add_fallible_node to have its timeout honoured"
                )));
            }
        }

        self.check_metadata()?;

        // Validate the exported form, which also carries each fallback route
        // as a guarded edge (taken only on a runtime failure), so a node
        // reachable only through a recovery policy isn't reported as an
        // orphan, and likewise each reject route. The stored `definition` keeps
        // just the ordinary edges: neither route is something
        // `next_edge` should ever pick.
        let result = validate(&self.to_definition());
        if !result.is_valid() {
            return Err(StrataError::GraphInvalid(result));
        }

        let mut definition = self.plain_definition();
        definition.schema_version =
            schema::graph_version_of(identity.as_ref(), &definition.nodes).to_string();
        definition.identity = identity.clone();

        // What the executor reads per node, gathered once
        // (design/38-Rust-Executor-Overhead.md).
        let mut nodes = self.nodes;
        for (name, entry) in &mut nodes {
            let edges = definition
                .edges
                .iter()
                .enumerate()
                .filter(|(_, edge)| edge.from == *name)
                .map(|(index, edge)| EdgePlan {
                    index,
                    to: Arc::from(edge.to.as_str()),
                    transition: observability::transition_key(name, &edge.to),
                })
                .collect();
            entry.plan = NodePlan {
                name: Arc::from(name.as_str()),
                policy: self
                    .recovery_policies
                    .get(name)
                    .cloned()
                    .unwrap_or_default(),
                timeout: self.timeouts.get(name).copied(),
                freezes: self.interrupt_boundaries.contains(name) || self.waits.contains_key(name),
                edges,
                committed: Some(observability::node_committed_key(name)),
            };
        }
        Ok(CompiledGraph {
            definition,
            identity,
            migration: options.migration,
            checkpointing: options.checkpointing,
            nodes,
            recovery_policies: self.recovery_policies,
            edge_conditions: self.edge_conditions,
            interrupt_boundaries: self.interrupt_boundaries,
            reject_routes: self.reject_routes,
            max_node_hops: options.max_node_hops,
            waits: self.waits,
            boundary_timeouts: self.boundary_timeouts,
            timeout_routes: self.timeout_routes,
        })
    }

    /// Adds a parallel fan-out/fan-in node (`design/14-Parallel-Execution.md`).
    ///
    /// When the group runs, every branch in `branches` gets the state as it
    /// was on entry and runs concurrently (a sync branch on the blocking pool,
    /// an async one as a task); once all of them have finished, `reducer`
    /// merges the results into the state that flows on to the next node. The
    /// reducer receives the entry state and the branch results in
    /// *declaration order* (not completion order), so the merge is
    /// deterministic.
    ///
    /// To the rest of the graph the group is a single node: give it ordinary
    /// edges, mark it an interrupt boundary, or attach a [`RecoveryPolicy`]
    /// via [`Self::set_recovery_policy`]. If any branch fails the group
    /// fails, and the policy applies to the group as a whole — a retry
    /// re-runs **every** branch, so branches should be safe to repeat. The
    /// first branch to fail fails the group at once, and its siblings are
    /// cancelled (`design/17-Async-Execution.md`).
    ///
    /// A branch added with [`ParallelBranches::streaming_branch`] gets a
    /// [`StreamContext`] and can push tokens while it runs; a group with one
    /// only runs under [`CompiledGraph::run_streaming`] (a plain `run` fails
    /// at the group, as it does at a streaming node). Chunks are attributed to
    /// `"<group>/<branch>"`.
    ///
    /// `.compile()` rejects a group with no branches or with two branches of
    /// the same name. The exported definition lists the branches, with any
    /// metadata set on them ([`ParallelBranches::set_metadata`]).
    pub fn add_parallel_group<R>(
        self,
        name: impl Into<String>,
        branches: ParallelBranches<S>,
        reducer: R,
    ) -> Self
    where
        R: Fn(&S, Vec<S>) -> S + Send + Sync + 'static,
    {
        self.add_parallel_group_with(name, branches, reducer, ParallelGroupOptions::default())
    }

    /// [`Self::add_parallel_group`] with `options`
    /// (`design/26-Parallel-Group-Controls-And-Joins.md`): a concurrency limit,
    /// a timeout per branch, whether a failure cancels the siblings, partial
    /// results, and a join policy. `reducer` gets the states of the branches
    /// that met the join (every branch for `All`), or, when the join wasn't met
    /// under [`PartialResults::KeepSucceeded`](crate::parallel::PartialResults::KeepSucceeded),
    /// of every branch that succeeded, in declaration order. Use
    /// [`Self::add_parallel_group_with_outcomes`] to see which branch is which,
    /// and how the others ended.
    pub fn add_parallel_group_with<R>(
        self,
        name: impl Into<String>,
        branches: ParallelBranches<S>,
        reducer: R,
        options: ParallelGroupOptions<S>,
    ) -> Self
    where
        R: Fn(&S, Vec<S>) -> S + Send + Sync + 'static,
    {
        let reducer: ReducerFn<S> = Box::new(reducer);
        self.add_group(
            name.into(),
            branches,
            Arc::new(move |entry: &S, results: BranchOutcomes<S>| {
                reducer(entry, results.into_counted_states())
            }),
            options,
        )
    }

    /// A parallel group whose `reducer` sees every branch's
    /// [`BranchOutcome`](crate::parallel::BranchOutcome) (name, status, state
    /// when it succeeded, error), in declaration order, and how the join went
    /// (`design/26-Parallel-Group-Controls-And-Joins.md`). With
    /// [`PartialResults::KeepSucceeded`](crate::parallel::PartialResults::KeepSucceeded)
    /// it runs even when some branches failed or timed out. Otherwise as
    /// [`Self::add_parallel_group_with`].
    pub fn add_parallel_group_with_outcomes<R>(
        self,
        name: impl Into<String>,
        branches: ParallelBranches<S>,
        reducer: R,
        options: ParallelGroupOptions<S>,
    ) -> Self
    where
        R: Fn(&S, BranchOutcomes<S>) -> S + Send + Sync + 'static,
    {
        self.add_group(name.into(), branches, Arc::new(reducer), options)
    }

    fn add_group(
        mut self,
        name: String,
        branches: ParallelBranches<S>,
        reducer: OutcomesReducer<S>,
        options: ParallelGroupOptions<S>,
    ) -> Self {
        let shape = GroupShape {
            join: options.join_def(),
            max_concurrency: options.max_concurrency(),
            branch_timeout: options.branch_timeout(),
            ..branches.shape()
        };
        let needs_stream = branches.has_streaming_branch();
        let branches = Arc::new(branches.into_branches());
        let options = Arc::new(options);
        let group: Arc<str> = Arc::from(name.as_str());
        let run: NodeHandler<S> = Arc::new(move |state, inputs| {
            let (branches, reducer, options, group) = (
                branches.clone(),
                reducer.clone(),
                options.clone(),
                group.clone(),
            );
            Box::pin(async move {
                let entry = state.clone();
                run_branches(&group, &branches, &options, state, inputs)
                    .await
                    .map(|results| Step::Done(reducer(&entry, results)))
                    .map_err(AttemptError::from)
            })
        });
        self.claim_node_name(
            name.clone(),
            NodeEntry {
                kind: NodeKind::Fallible,
                handler: run,
                needs_stream,
                is_subgraph: false,
                check_resume: None,
                winds_down: false,
                boundary: None,
                adopt: None,
                claim: None,
                composite: true,
                inline: false,
                plan: NodePlan::default(),
            },
        );
        self.parallel_groups.insert(name, shape);
        self
    }

    /// Adds a compiled graph as a single node of this one
    /// (`design/15-Subgraph-Composition.md`).
    ///
    /// When the node runs, `input` derives the child's initial state from the
    /// parent's, the child runs to completion (its own edges, conditions,
    /// recovery policies and nested subgraphs all apply), and `output` folds
    /// its final state back into the parent's — it receives the state the node
    /// started with and the child's result. The child has its own state type,
    /// so the two mappers are the whole interface between the graphs. One
    /// compiled child (behind an `Arc`) can back several nodes.
    ///
    /// To the rest of the graph this is one ordinary node: give it ordinary
    /// edges, mark it an interrupt boundary, or attach a [`RecoveryPolicy`]
    /// via [`Self::set_recovery_policy`]. If the child fails, the node fails —
    /// transient when the child's own recovery ran out on a transient error,
    /// fatal otherwise — and the policy applies to the child as a whole: a
    /// retry re-runs it from its entry point, so it should be safe to repeat.
    ///
    /// A child may have interrupt boundaries of its own
    /// (`design/16-Nested-Interrupts.md`). When it freezes at one, the whole
    /// run freezes: the parent's checkpoint is suspended at this node, holds
    /// the state the node started with, and nests the child's checkpoint.
    /// `resume` then continues the child from where it froze — `input` is not
    /// called again — applies the human's field overrides to the child's own
    /// state, and carries on into the parent when the child completes.
    /// Nesting works to any depth.
    ///
    /// A child may stream (`design/15-Subgraph-Composition.md`): under
    /// `run_streaming` everything it emits reaches the same receiver, named
    /// `"<name>/<child node>"`, between this node's own `NodeStart` and
    /// `NodeEnd`. A parent with a child that streams only runs under
    /// `run_streaming`; a plain `run` fails at this node, before running any
    /// of the child.
    ///
    /// The child sees cancellation at any depth
    /// (`design/17-Async-Execution.md`). A cancelled run stops it where it is
    /// and the parent's `cancelled` checkpoint nests the child's; this node's
    /// own timeout fails it transiently, nesting the child's `cancelled`
    /// checkpoint in the `failed` one, so a resume re-enters the child where
    /// it was stopped.
    ///
    /// The exported definition describes the node as a subgraph holding the
    /// child's own definition ([`CompiledGraph::to_definition`]), taken now: a
    /// compiled graph can't change.
    pub fn add_subgraph<T, I, O>(
        mut self,
        name: impl Into<String>,
        child: impl Into<Arc<CompiledGraph<T>>>,
        input: I,
        output: O,
    ) -> Self
    where
        T: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
        I: Fn(&S) -> T + Send + Sync + 'static,
        O: Fn(&S, T) -> S + Send + Sync + 'static,
    {
        let name = name.into();
        let child: Arc<CompiledGraph<T>> = child.into();
        let needs_stream = child.needs_stream();
        let child_definition = child.to_definition();
        let boundary = child.interrupt_boundary();
        let input: Arc<SubgraphInputFn<S, T>> = Arc::new(Box::new(input));
        let output: Arc<SubgraphOutputFn<S, T>> = Arc::new(Box::new(output));
        let node_name: Arc<str> = Arc::from(name.as_str());
        let checked = child.clone();
        let check_resume: ResumeChecker = Arc::new(move |checkpoint, signals, thread_id, path| {
            checked.check_resume(checkpoint, signals, thread_id, path)
        });
        let adopting = child.clone();
        let adopt: LevelAdopter = Arc::new(move |checkpoint, thread_id, path| {
            adopting.adopt(checkpoint, thread_id, path)
        });
        let claiming = child.clone();
        let claim: LevelClaimer = Arc::new(move |checkpoint, signals, run, now| {
            claiming.claim(checkpoint, signals, run, now)
        });
        let run: NodeHandler<S> = Arc::new(move |state, inputs| {
            let (child, input, output, node_name) = (
                child.clone(),
                input.clone(),
                output.clone(),
                node_name.clone(),
            );
            Box::pin(async move {
                run_subgraph(&node_name, &child, &input, &output, state, inputs).await
            })
        });
        self.claim_node_name(
            name.clone(),
            NodeEntry {
                kind: NodeKind::Fallible,
                handler: run,
                needs_stream,
                is_subgraph: true,
                check_resume: Some(check_resume),
                winds_down: false,
                boundary,
                adopt: Some(adopt),
                claim: Some(claim),
                composite: true,
                inline: false,
                plan: NodePlan::default(),
            },
        );
        self.subgraphs.insert(name, child_definition);
        self
    }

    /// Adds a map (`design/27-Map-Fan-Out.md`): a node that runs the compiled
    /// graph `child` once per item of a list read from the state when the node
    /// starts, a few at a time, and merges the children's results.
    ///
    /// When the node runs, `items` gives the list and `key` names each item.
    /// More items than [`MapOptions::with_max_items`], an empty key, a key
    /// containing `/`, or two items with one key fail the map fatally before
    /// anything starts. Then a
    /// [`RunEventKind::MapExpanded`](crate::observability::RunEventKind::MapExpanded)
    /// event lists the keys, and the items run, at most
    /// [`MapOptions::with_max_concurrency`] (8 by default) at once, in list
    /// order: each builds its child's initial state with `input` when it
    /// starts and runs `child` at a level of its own, as a subgraph's child
    /// does, its nodes named `"<name>/<key>/<child node>"`. A child failure
    /// fails its item, transient or fatal as a subgraph node's failure would
    /// be. `reducer` gets the children's final states in list order: every
    /// item's (an empty list gives an empty one), or with
    /// [`Self::add_map_with`]'s options, the deciding items' when the join is
    /// met, else every succeeded item's. [`Self::add_map_with_outcomes`] shows
    /// which item is which, and how the others ended.
    ///
    /// The items are a parallel group's branches: [`MapOptions`] mean what
    /// [`ParallelGroupOptions`]'s do, and each item reports `BranchStarted` /
    /// `BranchCompleted` at `"<name>/<key>"`. An item the map gives up on (a
    /// lost race, a failing sibling, its timeout, a cancelled run) winds down:
    /// its child stops at its current node, and the map waits for that before
    /// reporting it.
    ///
    /// To the rest of the graph the map is one node: give it ordinary edges,
    /// mark it an interrupt boundary, or attach a [`RecoveryPolicy`], which
    /// retries the whole map (every item runs again). It is one hop
    /// ([`CompileOptions::with_max_node_hops`]); each item's child counts its
    /// own. Its checkpoints never nest an item's. Under `run_streaming` an
    /// item's chunks are named `"<name>/<key>/<child node>"`; a child that
    /// streams makes a plain `run` fail at this node. `.compile()` refuses a
    /// `child` with an interrupt boundary at any depth: a map item can't
    /// freeze.
    ///
    /// The exported definition describes the node as a map holding `child`'s
    /// definition as its template, and its join when it isn't the default.
    pub fn add_map<I, T, Items, Key, Input, R>(
        self,
        name: impl Into<String>,
        items: Items,
        key: Key,
        child: impl Into<Arc<CompiledGraph<T>>>,
        input: Input,
        reducer: R,
    ) -> Self
    where
        I: Send + 'static,
        T: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
        Items: Fn(&S) -> Vec<I> + Send + Sync + 'static,
        Key: Fn(&I) -> String + Send + Sync + 'static,
        Input: Fn(&S, I) -> T + Send + Sync + 'static,
        R: Fn(&S, Vec<T>) -> S + Send + Sync + 'static,
    {
        self.add_map_with(
            name,
            items,
            key,
            child,
            input,
            reducer,
            MapOptions::default(),
        )
    }

    /// [`Self::add_map`] with `options` (`design/27-Map-Fan-Out.md`): a
    /// concurrency limit, a timeout per item, whether a failure cancels the
    /// siblings, partial results, a join policy and a maximum item count.
    /// `reducer` gets the children's final states of the items that met the
    /// join (every item for `All`), or, when the join wasn't met under
    /// [`PartialResults::KeepSucceeded`](crate::parallel::PartialResults::KeepSucceeded),
    /// of every item that succeeded, in list order.
    #[allow(clippy::too_many_arguments)]
    pub fn add_map_with<I, T, Items, Key, Input, R>(
        self,
        name: impl Into<String>,
        items: Items,
        key: Key,
        child: impl Into<Arc<CompiledGraph<T>>>,
        input: Input,
        reducer: R,
        options: MapOptions<T>,
    ) -> Self
    where
        I: Send + 'static,
        T: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
        Items: Fn(&S) -> Vec<I> + Send + Sync + 'static,
        Key: Fn(&I) -> String + Send + Sync + 'static,
        Input: Fn(&S, I) -> T + Send + Sync + 'static,
        R: Fn(&S, Vec<T>) -> S + Send + Sync + 'static,
    {
        self.add_map_core(
            name.into(),
            Arc::new(items),
            Arc::new(key),
            child.into(),
            Arc::new(input),
            Arc::new(move |entry: &S, results: BranchOutcomes<T>| {
                reducer(entry, results.into_counted_states())
            }),
            options,
        )
    }

    /// A map whose `reducer` sees every item's
    /// [`BranchOutcome`](crate::parallel::BranchOutcome) (`name` is its key;
    /// status, the child's final state when it succeeded, error), in list
    /// order, and how the join went. With
    /// [`PartialResults::KeepSucceeded`](crate::parallel::PartialResults::KeepSucceeded)
    /// it runs even when some items failed or timed out. Otherwise as
    /// [`Self::add_map_with`].
    #[allow(clippy::too_many_arguments)]
    pub fn add_map_with_outcomes<I, T, Items, Key, Input, R>(
        self,
        name: impl Into<String>,
        items: Items,
        key: Key,
        child: impl Into<Arc<CompiledGraph<T>>>,
        input: Input,
        reducer: R,
        options: MapOptions<T>,
    ) -> Self
    where
        I: Send + 'static,
        T: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
        Items: Fn(&S) -> Vec<I> + Send + Sync + 'static,
        Key: Fn(&I) -> String + Send + Sync + 'static,
        Input: Fn(&S, I) -> T + Send + Sync + 'static,
        R: Fn(&S, BranchOutcomes<T>) -> S + Send + Sync + 'static,
    {
        self.add_map_core(
            name.into(),
            Arc::new(items),
            Arc::new(key),
            child.into(),
            Arc::new(input),
            Arc::new(reducer),
            options,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn add_map_core<I, T>(
        mut self,
        name: String,
        items: ItemsFn<S, I>,
        key: KeyFn<I>,
        child: Arc<CompiledGraph<T>>,
        input: ItemInputFn<S, I, T>,
        reducer: MapReducerFn<S, T>,
        options: MapOptions<T>,
    ) -> Self
    where
        I: Send + 'static,
        T: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
    {
        let shape = options.shape(&child);
        let needs_stream = child.needs_stream();
        let spec = Arc::new(MapSpec {
            name: Arc::from(name.as_str()),
            items,
            key,
            child,
            input,
            reducer,
            options,
        });
        let run: NodeHandler<S> = Arc::new(move |state, inputs| {
            let spec = spec.clone();
            Box::pin(async move { run_map(&spec, state, inputs).await })
        });
        self.claim_node_name(
            name.clone(),
            NodeEntry {
                kind: NodeKind::Fallible,
                handler: run,
                needs_stream,
                is_subgraph: false,
                check_resume: None,
                winds_down: true,
                boundary: None,
                adopt: None,
                claim: None,
                composite: true,
                inline: false,
                plan: NodePlan::default(),
            },
        );
        self.maps.insert(name, shape);
        self
    }
}

pub struct CompiledGraph<S> {
    pub definition: GraphDefinition,
    /// The identity it was compiled with (`design/31-Graph-Identity.md`),
    /// also in `definition.identity`.
    pub(crate) identity: Option<GraphIdentity>,
    /// What accepts a checkpoint level another version of this graph wrote.
    pub(crate) migration: Option<Migration>,
    /// When a run of this graph writes checkpoints
    /// (`design/33-Durable-Execution.md`).
    pub(crate) checkpointing: CheckpointPolicy,
    pub(crate) nodes: HashMap<String, NodeEntry<S>>,
    pub(crate) recovery_policies: HashMap<String, RecoveryPolicy>,
    // Same indexing scheme as `StateGraph::edge_conditions` — parallel to
    // `definition.edges`.
    pub(crate) edge_conditions: Vec<Option<EdgeCondition<S>>>,
    pub(crate) interrupt_boundaries: HashSet<String>,
    pub(crate) reject_routes: HashMap<String, String>,
    pub(crate) max_node_hops: Option<u32>,
    /// Wait nodes and what they wait for (`design/29-Event-Waits-And-Timers.md`).
    pub(crate) waits: HashMap<String, WaitSpec<S>>,
    /// Interrupt boundaries with a timeout, and how long they wait.
    pub(crate) boundary_timeouts: HashMap<String, Duration>,
    /// Boundaries and event waits with a timeout route, and where a timeout
    /// continues.
    pub(crate) timeout_routes: HashMap<String, String>,
}

impl<S> CompiledGraph<S> {
    /// The loop circuit breaker it was compiled with
    /// ([`CompileOptions::with_max_node_hops`]), or `None` for none.
    /// [`RunConfig::with_max_node_hops`](crate::context::RunConfig::with_max_node_hops)
    /// replaces it for one run.
    pub fn max_node_hops(&self) -> Option<u32> {
        self.max_node_hops
    }

    /// The identity it was compiled with ([`CompileOptions::with_graph_id`] /
    /// [`CompileOptions::with_graph_version`],
    /// `design/31-Graph-Identity.md`), written on every checkpoint level it
    /// writes; `None` for none.
    pub fn identity(&self) -> Option<&GraphIdentity> {
        self.identity.as_ref()
    }

    /// When a run of this graph writes checkpoints
    /// ([`CompileOptions::with_checkpointing`],
    /// `design/33-Durable-Execution.md`). As the outermost graph of a run it
    /// decides for every level.
    pub fn checkpointing(&self) -> CheckpointPolicy {
        self.checkpointing
    }

    /// Whether a run of this graph checkpoints every node.
    pub(crate) fn durable(&self) -> bool {
        self.checkpointing == CheckpointPolicy::EveryNode
    }

    /// Whether this graph takes a thread whose latest checkpoint is
    /// `checkpoint` (`design/31-Graph-Identity.md`): its outermost level has
    /// no identity (written before identities, or by a graph without one), or
    /// this graph's id with this graph's version, or with any version when
    /// this graph has a migration (which may still refuse it). What the
    /// decision worker asks before resuming.
    pub fn owns(&self, checkpoint: &Checkpoint) -> bool {
        let Some(stored) = &checkpoint.graph else {
            return true;
        };
        self.identity.as_ref().is_some_and(|own| {
            own.id == stored.id && (own.version == stored.version || self.migration.is_some())
        })
    }

    /// `checkpoint` as this graph takes it, at this level and, through each
    /// subgraph node's child, every level nested below
    /// (`design/31-Graph-Identity.md`, "The rule"): a level with no identity,
    /// or this graph's, as it is (`Ok(None)` when nothing at any level
    /// changed); another version's through the migration, which gets this
    /// graph's identity and keeps its thread id; anything else refused, with
    /// why. `thread_id` is the outermost thread and `path` this level (`[]` at
    /// the top), for messages. A nested level under a node that isn't a
    /// subgraph is left for the resume check to report.
    pub(crate) fn adopt(
        &self,
        checkpoint: &Checkpoint,
        thread_id: &str,
        path: &[String],
    ) -> Result<Option<Checkpoint>, String> {
        let mut migrated = None;
        if let Some(stored) = &checkpoint.graph
            && self.identity.as_ref() != Some(stored)
        {
            let at = if path.is_empty() {
                String::new()
            } else {
                format!(" at level [{}]", path.join("/"))
            };
            let written = format!("thread '{thread_id}'{at} was written by graph {stored}");
            let Some(own) = &self.identity else {
                return Err(format!("{written}, and this graph has no identity"));
            };
            if own.id != stored.id {
                return Err(format!("{written}, not {own}"));
            }
            let versions = format!(
                "{written}, not {}",
                GraphIdentity::version_text(own.version.as_deref())
            );
            let Some(migrate) = &self.migration else {
                return Err(format!("{versions}, and this graph has no migration"));
            };
            let Some(mut level) = migrate(checkpoint.clone()) else {
                return Err(format!("{versions}, and this graph's migration refused it"));
            };
            level.thread_id = checkpoint.thread_id.clone();
            level.graph = Some(own.clone());
            migrated = Some(schema::versioned(level));
        }

        let level = migrated.as_ref().unwrap_or(checkpoint);
        let child = level.nested.as_deref().and_then(|child| {
            let node = level.next_node.as_deref()?;
            let adopt = self.nodes.get(node)?.adopt.as_ref()?;
            Some((child, node, adopt))
        });
        let Some((child, node, adopt)) = child else {
            return Ok(migrated);
        };
        let mut child_path = path.to_vec();
        child_path.push(node.to_string());
        match adopt(child, thread_id, &child_path)? {
            None => Ok(migrated),
            Some(adopted) => {
                let mut level = migrated.unwrap_or_else(|| checkpoint.clone());
                level.nested = Some(Box::new(adopted));
                Ok(Some(schema::versioned(level)))
            }
        }
    }
}

/// Accepts a checkpoint level another version of a graph wrote
/// ([`CompileOptions::with_migration`]).
pub(crate) type Migration = Arc<dyn Fn(Checkpoint) -> Option<Checkpoint> + Send + Sync>;

/// Options for [`StateGraph::compile_with`] (STRATA-SPEC-004). Mirrors the
/// .NET engine's `CompilationOptions`. A builder with private fields, like
/// [`RunConfig`](crate::context::RunConfig), so later options don't break
/// anyone.
#[derive(Clone, Default)]
pub struct CompileOptions {
    max_node_hops: Option<u32>,
    graph_id: Option<String>,
    graph_version: Option<String>,
    migration: Option<Migration>,
    checkpointing: CheckpointPolicy,
}

/// When a run writes checkpoints ([`CompileOptions::with_checkpointing`],
/// `design/33-Durable-Execution.md`). Mirrors the .NET engine's
/// `CheckpointPolicy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CheckpointPolicy {
    /// Only where a run stops: an interrupt boundary or a wait, a failure, a
    /// cancellation, an end. Nothing is saved between nodes, and a run that
    /// completes writes nothing. A thread whose process dies mid-run can't be
    /// continued.
    #[default]
    Boundaries,
    /// Also a `running` checkpoint before the first node and after every node
    /// that commits (a parallel group and a map are one node each), and a
    /// `completed` one when the run finishes. A resume claims its thread with
    /// a `running` checkpoint instead of stamping `resumed_at`. One store
    /// append and one state serialization per node.
    EveryNode,
}

impl std::fmt::Debug for CompileOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompileOptions")
            .field("max_node_hops", &self.max_node_hops)
            .field("graph_id", &self.graph_id)
            .field("graph_version", &self.graph_version)
            .field("migration", &self.migration.is_some())
            .field("checkpointing", &self.checkpointing)
            .finish()
    }
}

impl CompileOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// The loop circuit breaker (`design/24-Loop-Circuit-Breaker.md`): the
    /// most nodes one thread may start at this graph's level, across every run
    /// and resume of it. A retry is not a new hop; a fallback node, a parallel
    /// group and a subgraph node are one each. Starting the node that would be
    /// one more ends the run with `Failed(StrataError::HopLimitExceeded)` and
    /// a `Failed` checkpoint. A subgraph's child counts its own nodes against
    /// its own limit. Must be at least 1.
    pub fn with_max_node_hops(mut self, limit: u32) -> Self {
        self.max_node_hops = Some(limit);
        self
    }

    pub fn max_node_hops(&self) -> Option<u32> {
        self.max_node_hops
    }

    /// The graph's identity (`design/31-Graph-Identity.md`): written on every
    /// checkpoint level this graph writes, and checked on every one it
    /// resumes or ends, so a thread another graph wrote is refused. Without
    /// one (the default) the graph resumes any thread without one, as
    /// before. Must not be blank.
    pub fn with_graph_id(mut self, id: impl Into<String>) -> Self {
        self.graph_id = Some(id.into());
        self
    }

    /// The version of [`Self::with_graph_id`], free text compared exactly: a
    /// thread written by another version is refused unless
    /// [`Self::with_migration`] accepts it. Needs an id; must not be blank.
    pub fn with_graph_version(mut self, version: impl Into<String>) -> Self {
        self.graph_version = Some(version.into());
        self
    }

    /// Accepts a checkpoint level written by another version of this graph
    /// (`design/31-Graph-Identity.md`, "The rule"): returns the level to resume
    /// (or end) instead, with nodes renamed or state reshaped as this version
    /// needs, or `None` to refuse it. It sees the level as stored, its
    /// [`Checkpoint::graph`] saying which version wrote it; the engine then
    /// gives it this graph's identity and keeps its thread id. It should
    /// change its own level only, not `nested`. It runs before anything is
    /// written, and must be pure. Needs an id.
    pub fn with_migration(
        mut self,
        migrate: impl Fn(Checkpoint) -> Option<Checkpoint> + Send + Sync + 'static,
    ) -> Self {
        self.migration = Some(Arc::new(migrate));
        self
    }

    /// When a run of the graph writes checkpoints
    /// (`design/33-Durable-Execution.md`). [`CheckpointPolicy::Boundaries`]
    /// (the default): only where it stops, as before.
    /// [`CheckpointPolicy::EveryNode`]: also before its first node and after
    /// every node that commits, so a thread whose process died can be
    /// recovered ([`CompiledGraph::recover`]). The outermost graph's policy
    /// is the run's, at every depth: a subgraph's child follows its parent's.
    pub fn with_checkpointing(mut self, policy: CheckpointPolicy) -> Self {
        self.checkpointing = policy;
        self
    }

    pub fn checkpointing(&self) -> CheckpointPolicy {
        self.checkpointing
    }

    pub fn graph_id(&self) -> Option<&str> {
        self.graph_id.as_deref()
    }

    pub fn graph_version(&self) -> Option<&str> {
        self.graph_version.as_deref()
    }

    /// The identity these options give a graph, refusing a blank one or parts
    /// without an id.
    fn identity(&self) -> Result<Option<GraphIdentity>, StrataError> {
        let refuse = |message: &str| Err(StrataError::CompilationFailed(message.to_string()));
        let Some(id) = &self.graph_id else {
            if self.graph_version.is_some() {
                return refuse("graph_version needs a graph_id");
            }
            if self.migration.is_some() {
                return refuse("a migration needs a graph_id");
            }
            return Ok(None);
        };
        if id.trim().is_empty() {
            return refuse("graph_id must not be blank");
        }
        if self
            .graph_version
            .as_ref()
            .is_some_and(|version| version.trim().is_empty())
        {
            return refuse("graph_version must not be blank");
        }
        Ok(Some(GraphIdentity::new(id, self.graph_version.clone())))
    }
}

/// Refuses an empty key or one starting `strata.`, the prefix kept for the
/// engine (`design/22-Graph-Structure-In-Definitions.md`).
fn check_metadata_keys(
    metadata: &BTreeMap<String, String>,
    owner: &str,
) -> Result<(), StrataError> {
    for key in metadata.keys() {
        if key.is_empty() {
            return Err(StrataError::CompilationFailed(format!(
                "metadata on {owner} has an empty key"
            )));
        }
        if key.starts_with(RESERVED_METADATA_PREFIX) {
            return Err(StrataError::CompilationFailed(format!(
                "metadata key '{key}' on {owner} is reserved"
            )));
        }
    }
    Ok(())
}

/// The key prefix node and branch metadata may not use.
const RESERVED_METADATA_PREFIX: &str = "strata.";

/// `definition` plus one guarded edge with `condition` per route: an
/// `on_reject` edge per interrupt boundary with a reject route
/// (`design/21-Typed-Human-Signals.md`), or an `on_timeout` edge per wait with
/// a timeout route (`design/29-Event-Waits-And-Timers.md`). In node
/// declaration order, then, for a route on a boundary name that isn't a node
/// (which validation reports as a dangling edge), in byte order. Like a
/// fallback, a route is taken only by a resume, never by ordinary routing.
fn with_routes(
    mut definition: GraphDefinition,
    routes: &HashMap<String, String>,
    condition: &str,
) -> GraphDefinition {
    let mut strays: Vec<&String> = routes
        .keys()
        .filter(|boundary| !definition.nodes.iter().any(|node| &node.name == *boundary))
        .collect();
    strays.sort();
    let sources: Vec<String> = definition
        .nodes
        .iter()
        .map(|node| node.name.clone())
        .filter(|name| routes.contains_key(name))
        .chain(strays.into_iter().cloned())
        .collect();
    definition
        .edges
        .extend(sources.into_iter().map(|boundary| GraphEdgeDef {
            to: routes[&boundary].clone(),
            from: boundary,
            guarded: true,
            condition: Some(condition.to_string()),
        }));
    definition
}

fn with_fallback_edges(
    mut definition: GraphDefinition,
    policies: &HashMap<String, RecoveryPolicy>,
) -> GraphDefinition {
    let fallback_edges: Vec<GraphEdgeDef> = definition
        .nodes
        .iter()
        .filter_map(|node| {
            let fallback = policies.get(&node.name)?.fallback.as_ref()?;
            Some(GraphEdgeDef {
                from: node.name.clone(),
                to: fallback.clone(),
                guarded: true,
                condition: Some("on_failure".to_string()),
            })
        })
        .collect();
    definition.edges.extend(fallback_edges);
    definition
}

impl<S> CompiledGraph<S> {
    /// The graph in the interop shape `strata graph validate` reads — the same
    /// output [`StateGraph::to_definition`] gives for the builder this came
    /// from. Prefer this to the `definition` field for export: the field holds
    /// only the ordinary edges the executor routes over, so a node reachable
    /// only through a recovery policy's fallback, or an interrupt boundary's
    /// reject route, would look like an orphan.
    pub fn to_definition(&self) -> GraphDefinition {
        with_routes(
            with_routes(
                with_fallback_edges(self.definition.clone(), &self.recovery_policies),
                &self.reject_routes,
                "on_reject",
            ),
            &self.timeout_routes,
            "on_timeout",
        )
    }

    /// Whether a run of this graph needs a [`StreamContext`]: it has a
    /// streaming node, or a fallible node that does (a parallel group with a
    /// streaming branch, or a subgraph whose own child streams).
    pub(crate) fn needs_stream(&self) -> bool {
        self.nodes.values().any(|entry| entry.needs_stream)
    }

    /// The first interrupt boundary or wait node
    /// (`design/29-Event-Waits-And-Timers.md`) a run of this graph could
    /// freeze at, in node order, qualified with the subgraph nodes it is inside
    /// (`"Review/Approve"`), or `None`. A boundary name that isn't a node never
    /// triggers, so it isn't one. A map can't run a graph with one
    /// (`design/27-Map-Fan-Out.md`).
    pub(crate) fn interrupt_boundary(&self) -> Option<Suspension> {
        self.definition.nodes.iter().find_map(|node| {
            if self.interrupt_boundaries.contains(&node.name) {
                return Some(Suspension {
                    path: node.name.clone(),
                    is_wait: false,
                });
            }
            if self.waits.contains_key(&node.name) {
                return Some(Suspension {
                    path: node.name.clone(),
                    is_wait: true,
                });
            }
            let inner = self.nodes.get(&node.name)?.boundary.as_ref()?;
            Some(inner.under(&node.name))
        })
    }

    /// The `wait` a checkpoint frozen after `node` carries
    /// (`design/29-Event-Waits-And-Timers.md`): a wait node's, or a timed
    /// boundary's (kind `human`); `None` for a boundary with no timeout.
    pub(crate) fn wait_after(
        &self,
        node: &str,
        state: &S,
        now: DateTime<Utc>,
    ) -> Option<CheckpointWait> {
        if let Some(wait) = self.waits.get(node) {
            return Some(wait.at(state, now));
        }
        self.boundary_timeouts
            .get(node)
            .map(|timeout| CheckpointWait {
                kind: WaitKind::Human,
                event_type: None,
                wake_at: Some(wake_text(now + *timeout)),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validate::ValidationErrorKind;

    fn noop(s: i32) -> i32 {
        s
    }

    #[test]
    fn valid_graph_compiles() {
        let graph = StateGraph::<i32>::new()
            .add_node("A", noop)
            .add_node("B", noop)
            .add_edge("A", "B")
            .compile();

        assert!(graph.is_ok());
        assert_eq!(graph.unwrap().definition.entry_point, "A");
    }

    #[test]
    fn missing_entry_point_is_rejected() {
        let result = StateGraph::<i32>::new()
            .add_node("A", noop)
            .set_entry_point("DoesNotExist")
            .compile();

        match result {
            Err(StrataError::GraphInvalid(validation)) => {
                assert!(
                    validation
                        .errors
                        .iter()
                        .any(|e| e.kind == ValidationErrorKind::MissingEntryPoint)
                );
            }
            Err(other) => panic!("expected GraphInvalid, got {other:?}"),
            Ok(_) => panic!("expected compile to fail"),
        }
    }

    #[test]
    fn orphan_node_is_rejected() {
        let result = StateGraph::<i32>::new()
            .add_node("A", noop)
            .add_node("B", noop)
            .compile();

        match result {
            Err(StrataError::GraphInvalid(validation)) => {
                assert!(
                    validation
                        .errors
                        .iter()
                        .any(|e| e.kind == ValidationErrorKind::OrphanNode && e.node == "B")
                );
            }
            Err(other) => panic!("expected GraphInvalid, got {other:?}"),
            Ok(_) => panic!("expected compile to fail"),
        }
    }

    #[test]
    fn unguarded_cycle_is_rejected() {
        let result = StateGraph::<i32>::new()
            .add_node("A", noop)
            .add_node("B", noop)
            .add_edge("A", "B")
            .add_edge("B", "A")
            .compile();

        match result {
            Err(StrataError::GraphInvalid(validation)) => {
                assert!(
                    validation
                        .errors
                        .iter()
                        .any(|e| e.kind == ValidationErrorKind::UnguardedCycle)
                );
            }
            Err(other) => panic!("expected GraphInvalid, got {other:?}"),
            Ok(_) => panic!("expected compile to fail"),
        }
    }

    #[test]
    fn guarded_cycle_compiles() {
        let graph = StateGraph::<i32>::new()
            .add_node("A", noop)
            .add_node("B", noop)
            .add_edge("A", "B")
            .add_conditional_edge("B", "A", |_: &i32| false)
            .compile();

        assert!(graph.is_ok());
    }
}
