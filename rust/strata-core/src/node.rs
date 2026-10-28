//! How the engine stores and runs a node, whatever kind the caller added
//! (`design/17-Async-Execution.md`): every handler becomes a [`NodeHandler`]
//! returning a boxed future, so the executor has one path for all of them.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::checkpoint::LevelSignals;
use crate::context::{LevelScope, NodeContext, RunScope};
use crate::graph::{NodeError, RecoveryPolicy};
use crate::schema::Checkpoint;
use crate::streaming::StreamContext;
use crate::subgraph::{AttemptError, NestedResume, Step};

pub(crate) use crate::store::BoxFuture;

/// One attempt of a node, as the engine stores it: takes the state (a clone
/// of the committed one, which the engine keeps) and what the attempt is
/// handed besides, and returns a future of the attempt's [`Step`].
pub(crate) type NodeHandler<S> = Arc<
    dyn Fn(S, AttemptInputs) -> BoxFuture<'static, Result<Step<S>, AttemptError>> + Send + Sync,
>;

/// How a node was added.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeKind {
    /// `add_node` / `add_async_node`.
    Plain,
    /// `add_streaming_node` / `add_async_streaming_node`: a plain run refuses it.
    Streaming,
    /// `add_fallible_node` / `add_async_fallible_node`, a parallel group or a
    /// subgraph: the only kind a recovery policy may be set on.
    Fallible,
}

/// A stored node. `needs_stream` marks one a plain run must refuse before
/// running any of it: a streaming node, a parallel group with a streaming
/// branch, or a subgraph whose child streams. `is_subgraph` marks the only
/// nodes a run can resume *into* with a nested checkpoint, and the only ones
/// the engine lets wind down on cancellation instead of dropping. A subgraph
/// also has `check_resume`, which checks a resume of its child before
/// anything is stamped (`design/21-Typed-Human-Signals.md`, "Preflight"), and
/// `boundary`, the first interrupt boundary or wait its child could freeze at,
/// qualified under it ([`crate::graph::CompiledGraph::interrupt_boundary`]).
/// `winds_down` marks a map (`design/27-Map-Fan-Out.md`): like a subgraph it
/// is waited for when its token fires, so its items stop before the node is
/// reported, but it hands back no checkpoint. A subgraph's `adopt` takes its
/// child's level of a stored checkpoint as the child graph does
/// (`design/31-Graph-Identity.md`), and its `claim` makes the child's level of
/// a resume's or a recovery's claim (`design/33-Durable-Execution.md`).
/// `plan` is what the executor needs each time it runs the node, worked out
/// when the graph compiles.
pub(crate) struct NodeEntry<S> {
    pub(crate) kind: NodeKind,
    pub(crate) handler: NodeHandler<S>,
    pub(crate) needs_stream: bool,
    pub(crate) is_subgraph: bool,
    pub(crate) check_resume: Option<ResumeChecker>,
    pub(crate) winds_down: bool,
    pub(crate) boundary: Option<Suspension>,
    pub(crate) adopt: Option<LevelAdopter>,
    pub(crate) claim: Option<LevelClaimer>,
    /// Whether the node hands its attempt's token on: a parallel group (to
    /// its branches), a subgraph (to its child) or a map (to its items).
    pub(crate) composite: bool,
    /// Added with `add_inline_node` / `add_inline_fallible_node`: its sync
    /// handler runs on the executor, so `.compile()` refuses a timeout on it
    /// (`design/39-Inline-Sync-Handlers.md`).
    pub(crate) inline: bool,
    pub(crate) plan: NodePlan,
}

/// What the executor looks up about a node every time it runs it, gathered
/// once at compile time so a run pays for one map lookup per node and
/// allocates nothing for the node's name, its routes or its metric labels
/// (`design/38-Rust-Executor-Overhead.md`). Empty until the graph compiles.
#[derive(Default)]
pub(crate) struct NodePlan {
    pub(crate) name: Arc<str>,
    /// The node's recovery policy, or the default one (no retries, no fallback).
    pub(crate) policy: RecoveryPolicy,
    pub(crate) timeout: Option<Duration>,
    /// Whether a run freezes after the node: an interrupt boundary or a wait.
    pub(crate) freezes: bool,
    /// The edges that leave the node, in the order they were added, which is
    /// the order their conditions are tried in.
    pub(crate) edges: Vec<EdgePlan>,
    /// The `strata.node.duration` series of the node committing at the
    /// outermost level, where its path is its name.
    pub(crate) committed: Option<metrics::Key>,
}

/// One edge out of a node: its place in the definition's edges (and so in
/// the conditions beside them), where it leads, and the
/// `strata.graph.transition` series of following it at the outermost level.
pub(crate) struct EdgePlan {
    pub(crate) index: usize,
    pub(crate) to: Arc<str>,
    pub(crate) transition: metrics::Key,
}

/// Where a graph can freeze ([`crate::graph::CompiledGraph::interrupt_boundary`]):
/// an interrupt boundary, or a wait node (`design/29-Event-Waits-And-Timers.md`),
/// at a path qualified with the subgraph nodes it is inside.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Suspension {
    pub(crate) path: String,
    pub(crate) is_wait: bool,
}

impl Suspension {
    /// This suspension as seen from the graph whose subgraph node `node` holds it.
    pub(crate) fn under(&self, node: &str) -> Self {
        Self {
            path: format!("{node}/{}", self.path),
            is_wait: self.is_wait,
        }
    }
}

impl std::fmt::Display for Suspension {
    /// For messages: `an interrupt boundary at 'Review/Approve'`, `a wait at 'Window'`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_wait {
            write!(f, "a wait at '{}'", self.path)
        } else {
            write!(f, "an interrupt boundary at '{}'", self.path)
        }
    }
}

/// Checks that a resume from a checkpoint with some signals can start,
/// without changing anything: the arguments are the checkpoint, the signals,
/// the outermost thread id and the level's path (`[]` at the top), the last
/// two for messages.
pub(crate) type ResumeChecker =
    Arc<dyn Fn(&Checkpoint, &LevelSignals, &str, &[String]) -> ResumeCheck + Send + Sync>;

/// Takes a level of a stored checkpoint as a graph does
/// ([`crate::graph::CompiledGraph::adopt`]): the arguments are the level, the
/// outermost thread id and the level's path (`[]` at the top). `Ok` is the
/// level to go on with (migrated, if another version wrote it); `Err` why it
/// is refused.
pub(crate) type LevelAdopter =
    Arc<dyn Fn(&Checkpoint, &str, &[String]) -> Result<Option<Checkpoint>, String> + Send + Sync>;

/// A level of the `running` checkpoint a resume or a recovery claims its thread
/// with ([`crate::graph::CompiledGraph::claim`],
/// `design/33-Durable-Execution.md`): the arguments are the level, the signals
/// for it, the run and the time the claim is made.
pub(crate) type LevelClaimer =
    Arc<dyn Fn(&Checkpoint, &LevelSignals, &RunScope, &str) -> Checkpoint + Send + Sync>;

/// What a resume check found: the `problem` that refuses the resume, or, when
/// the innermost level will take a route instead of its next node, the route:
/// its boundary's reject route on a rejection, its wait's timeout route on a
/// timeout (`design/29-Event-Waits-And-Timers.md`). `ends_on_timeout`: the
/// wake is a timeout and the innermost wait has no route, so the thread is
/// ended instead of resumed.
pub(crate) struct ResumeCheck {
    pub(crate) problem: Option<String>,
    pub(crate) route: Option<String>,
    pub(crate) ends_on_timeout: bool,
}

/// What one attempt of a node is handed besides the state: the context its
/// handler sees, the node's stream context (for a group or subgraph to derive
/// its branches' and child's from), what a subgraph node is resuming from, and
/// the level it runs at. The attempt's token (cancelled with the run, by the
/// node's timeout, or for a branch by a failing sibling) is the context's.
pub(crate) struct AttemptInputs {
    pub(crate) context: NodeContext,
    pub(crate) stream: Option<StreamContext>,
    pub(crate) resume: Option<Box<NestedResume>>,
    pub(crate) level: LevelScope,
}

impl AttemptInputs {
    /// The attempt's token: [`NodeContext::cancellation`].
    pub(crate) fn token(&self) -> &CancellationToken {
        self.context.cancellation()
    }
}

/// Runs a sync handler on tokio's blocking pool, not on the executor, so it
/// can't hold up a runtime worker, its `_blocking` emits are allowed, and the
/// engine can stop waiting for it (it can't be stopped, only abandoned). A
/// panic in it resumes on the executor, as it would from an inline call. It
/// runs inside the span that is current when the future is first polled (its
/// attempt's, `design/18-Run-Observability.md`), so its own spans nest there.
pub(crate) fn on_blocking_pool<S, F>(work: F) -> BoxFuture<'static, Result<Step<S>, AttemptError>>
where
    S: Send + 'static,
    F: FnOnce() -> Result<S, NodeError> + Send + 'static,
{
    Box::pin(async move {
        let span = tracing::Span::current();
        match tokio::task::spawn_blocking(move || span.in_scope(work)).await {
            Ok(result) => result.map(Step::Done).map_err(AttemptError::from),
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(_) => Err(NodeError::Fatal(
                "the runtime shut down while the node was running".to_string(),
            )
            .into()),
        }
    })
}

/// Runs a sync handler on the executor, when the future is first polled
/// (`design/39-Inline-Sync-Handlers.md`): inside its attempt's span and the
/// executor's panic catch, like an async handler's body. Nothing can abandon
/// it, so it must not block; the engine sees a cancellation once it returns.
pub(crate) fn on_executor<S, F>(work: F) -> BoxFuture<'static, Result<Step<S>, AttemptError>>
where
    S: Send + 'static,
    F: FnOnce() -> Result<S, NodeError> + Send + 'static,
{
    Box::pin(async move { work().map(Step::Done).map_err(AttemptError::from) })
}
