//! Parallel fan-out / fan-in: a node whose body is several branches run
//! concurrently and merged by a reducer (see `design/14-Parallel-Execution.md`).
//!
//! A parallel group is registered with
//! [`crate::graph::StateGraph::add_parallel_group`] and is, from the graph's
//! point of view, one ordinary node: it has ordinary edges in and out, can be
//! an interrupt boundary, and carries a
//! [`RecoveryPolicy`](crate::graph::RecoveryPolicy). Under the hood it is
//! registered as a *fallible* node whose handler runs [`run_branches`], which
//! is why retry, fallback and `Failed` checkpoints work for it with no
//! executor changes.

use std::any::Any;
use std::collections::BTreeMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span};

use crate::context::NodeContext;
use crate::graph::NodeError;
use crate::node::{AttemptInputs, BoxFuture};
use crate::observability::{BranchEndReason, BranchResult, BranchScope, CatchUnwind, RunTelemetry};
use crate::schema::GraphJoinDef;
use crate::streaming::StreamContext;

/// One branch of a parallel group. Borrows the state as it was when the group
/// started and returns the branch's own result state; it can't see, or race
/// with, its siblings.
pub type BranchFn<S> = Arc<dyn Fn(&S) -> Result<S, NodeError> + Send + Sync>;

/// A branch that can also push chunks while it runs. Its [`StreamContext`]
/// stamps every chunk with `"<group>/<branch>"`, so a consumer can tell
/// concurrent branches apart.
pub type StreamingBranchFn<S> =
    Arc<dyn Fn(&S, &StreamContext) -> Result<S, NodeError> + Send + Sync>;

/// An async branch, as a group stores it (`design/17-Async-Execution.md`).
type AsyncBranchFn<S> =
    Arc<dyn Fn(S, NodeContext) -> BoxFuture<'static, Result<S, NodeError>> + Send + Sync>;

/// A branch of any kind, as a group stores it.
pub(crate) enum Branch<S> {
    Plain(BranchFn<S>),
    Streaming(StreamingBranchFn<S>),
    Async {
        run: AsyncBranchFn<S>,
        streaming: bool,
    },
}

/// Merges a group's branch results back into one state. Receives the state
/// the group started with and every branch's result **in declaration order**
/// — never completion order — so the merge is deterministic however the
/// branches happen to be scheduled.
pub type ReducerFn<S> = Box<dyn Fn(&S, Vec<S>) -> S + Send + Sync>;

/// The named branches of a parallel group, in declaration order. Built with
/// [`ParallelBranches::new`] + [`ParallelBranches::branch`] and handed to
/// [`crate::graph::StateGraph::add_parallel_group`].
pub struct ParallelBranches<S> {
    branches: Vec<(String, Branch<S>)>,
    // Branch name -> its metadata (`design/22-Graph-Structure-In-Definitions.md`).
    metadata: BTreeMap<String, BTreeMap<String, String>>,
}

impl<S> Default for ParallelBranches<S> {
    fn default() -> Self {
        Self {
            branches: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }
}

/// What a group keeps of its branches for `.compile()`'s checks and the
/// exported definition: their names in declaration order, and the metadata
/// set on them (possibly for a name that isn't a branch, which `.compile()`
/// rejects).
#[derive(Debug, Clone, Default)]
pub(crate) struct GroupShape {
    pub(crate) branches: Vec<String>,
    pub(crate) metadata: BTreeMap<String, BTreeMap<String, String>>,
    /// The group's join, when it isn't the default
    /// (`design/26-Parallel-Group-Controls-And-Joins.md`).
    pub(crate) join: Option<GraphJoinDef>,
    /// Kept for `.compile()` to refuse a limit of 0 or a zero timeout.
    pub(crate) max_concurrency: Option<usize>,
    pub(crate) branch_timeout: Option<Duration>,
}

impl<S> ParallelBranches<S> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a branch. Names identify the branch in failure messages and must
    /// be unique within the group (`.compile()` rejects duplicates). A
    /// failing branch reports `Err(NodeError::...)`; a panic is not a
    /// `NodeError` and unwinds through the engine, as it does for any node.
    ///
    /// A sync branch runs on tokio's blocking pool, so sync branches still run
    /// in parallel. It can't see a cancellation, so when the group gives up on
    /// it (a sibling failed, the run was cancelled, the group timed out) it
    /// runs on and its result is discarded.
    pub fn branch<F>(mut self, name: impl Into<String>, handler: F) -> Self
    where
        F: Fn(&S) -> Result<S, NodeError> + Send + Sync + 'static,
    {
        self.branches
            .push((name.into(), Branch::Plain(Arc::new(handler))));
        self
    }

    /// Adds a branch that also receives a [`StreamContext`], so it can push
    /// tokens as it runs — concurrently with its siblings, on the one bounded
    /// channel the run streams through. Chunks arrive attributed to
    /// `"<group>/<branch>"`; each branch's own chunks stay in order, but chunks
    /// from different branches interleave however the threads are scheduled.
    ///
    /// A group with a streaming branch only runs under
    /// [`crate::checkpoint::CompiledGraph::run_streaming`]; a plain `run` fails
    /// at the group, as it does at a streaming node. If the group is retried
    /// (see [`crate::graph::RecoveryPolicy`]) every branch runs again and
    /// streams again, and a [`StreamChunkType::Retry`](crate::streaming::StreamChunkType::Retry)
    /// chunk precedes the re-run so a consumer can discard what it showed.
    /// Otherwise as [`Self::branch`].
    pub fn streaming_branch<F>(mut self, name: impl Into<String>, handler: F) -> Self
    where
        F: Fn(&S, &StreamContext) -> Result<S, NodeError> + Send + Sync + 'static,
    {
        self.branches
            .push((name.into(), Branch::Streaming(Arc::new(handler))));
        self
    }

    /// Adds an async branch (`design/17-Async-Execution.md`). It runs as a
    /// task, not on a thread of its own, and gets its own clone of the state.
    /// Its [`NodeContext`] is named `"<group>/<branch>"` and carries the
    /// group's attempt; its token is cancelled with the run, by the group's
    /// timeout, or when a sibling fails, and the group then drops its future.
    /// Otherwise as [`Self::branch`].
    pub fn async_branch<F, Fut>(mut self, name: impl Into<String>, handler: F) -> Self
    where
        F: Fn(S, NodeContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<S, NodeError>> + Send + 'static,
    {
        let run: AsyncBranchFn<S> = Arc::new(move |state, ctx| Box::pin(handler(state, ctx)));
        self.branches.push((
            name.into(),
            Branch::Async {
                run,
                streaming: false,
            },
        ));
        self
    }

    /// An async [`Self::streaming_branch`]: its [`NodeContext::stream`] is
    /// always `Some`. Otherwise as [`Self::async_branch`].
    pub fn async_streaming_branch<F, Fut>(mut self, name: impl Into<String>, handler: F) -> Self
    where
        F: Fn(S, NodeContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<S, NodeError>> + Send + 'static,
    {
        let run: AsyncBranchFn<S> = Arc::new(move |state, ctx| Box::pin(handler(state, ctx)));
        self.branches.push((
            name.into(),
            Branch::Async {
                run,
                streaming: true,
            },
        ));
        self
    }

    /// Describes `branch` in the exported definition
    /// (`design/22-Graph-Structure-In-Definitions.md`): string keys and
    /// values, for a console or a CI diff to read. The engine never reads it.
    /// Setting it again replaces it, and it may come before or after the
    /// branch is added. `.compile()` rejects metadata for a name that isn't a
    /// branch of the group, an empty key, and a key starting `strata.` (kept
    /// for the engine). Keys are exported sorted.
    pub fn set_metadata<K, V>(
        mut self,
        branch: impl Into<String>,
        metadata: impl IntoIterator<Item = (K, V)>,
    ) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.metadata.insert(
            branch.into(),
            metadata
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        );
        self
    }

    pub(crate) fn shape(&self) -> GroupShape {
        GroupShape {
            branches: self.branches.iter().map(|(name, _)| name.clone()).collect(),
            metadata: self.metadata.clone(),
            join: None,
            max_concurrency: None,
            branch_timeout: None,
        }
    }

    pub(crate) fn has_streaming_branch(&self) -> bool {
        self.branches.iter().any(|(_, branch)| {
            matches!(
                branch,
                Branch::Streaming(_)
                    | Branch::Async {
                        streaming: true,
                        ..
                    }
            )
        })
    }

    pub(crate) fn into_branches(self) -> Vec<(String, Branch<S>)> {
        self.branches
    }
}

fn no_stream() -> NodeError {
    NodeError::Fatal(
        "streaming branch has no StreamContext \u{2014} use run_streaming instead of run"
            .to_string(),
    )
}

/// What a parallel group does when its join can no longer be met
/// (`design/26-Parallel-Group-Controls-And-Joins.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartialResults {
    /// The group fails. The default, and how groups always behaved.
    #[default]
    FailGroup,
    /// The group waits for every branch, then reduces over all of them with
    /// [`BranchOutcomes::join_met`] `false`. It fails only if no branch
    /// succeeded.
    KeepSucceeded,
}

/// Which join a [`Join`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinKind {
    /// Every branch succeeded.
    All,
    /// One branch succeeded.
    FirstSuccess,
    /// A branch succeeded with a result that passes the predicate.
    FirstMatching,
    /// [`Join::count`] branches succeeded.
    Quorum,
}

impl JoinKind {
    /// The policy as events, span attributes and definitions write it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::FirstSuccess => "first_success",
            Self::FirstMatching => "first_matching",
            Self::Quorum => "quorum",
        }
    }
}

type MatchFn<S> = Arc<dyn Fn(&S) -> bool + Send + Sync>;

/// When a parallel group is done
/// (`design/26-Parallel-Group-Controls-And-Joins.md`). Branches still running
/// when it is met are cancelled: they lost the race. Mirrors the .NET engine's
/// `JoinPolicy`.
pub struct Join<S> {
    kind: JoinKind,
    count: Option<usize>,
    predicate: Option<MatchFn<S>>,
    description: Option<String>,
}

impl<S> Clone for Join<S> {
    fn clone(&self) -> Self {
        Self {
            kind: self.kind,
            count: self.count,
            predicate: self.predicate.clone(),
            description: self.description.clone(),
        }
    }
}

impl<S> Join<S> {
    fn of(kind: JoinKind) -> Self {
        Self {
            kind,
            count: None,
            predicate: None,
            description: None,
        }
    }

    /// Done when every branch succeeded. The default.
    pub fn all() -> Self {
        Self::of(JoinKind::All)
    }

    /// Done when one branch succeeded: the first to.
    pub fn first_success() -> Self {
        Self::of(JoinKind::FirstSuccess)
    }

    /// Done when a branch succeeded with a result that passes `predicate`: the
    /// first to. The predicate runs once per succeeded branch, on the engine,
    /// so it must be cheap.
    pub fn first_matching(predicate: impl Fn(&S) -> bool + Send + Sync + 'static) -> Self {
        Self {
            predicate: Some(Arc::new(predicate)),
            ..Self::of(JoinKind::FirstMatching)
        }
    }

    /// Done when `count` branches succeeded: the first `count` to. `.compile()`
    /// refuses a count below 1 or above the group's branch count
    /// (`STRATA_ERR_INVALID_QUORUM`).
    pub fn quorum(count: usize) -> Self {
        Self {
            count: Some(count),
            ..Self::of(JoinKind::Quorum)
        }
    }

    /// Describes a `first_matching` predicate; exported as the join's
    /// `condition`. Ignored for the other policies.
    pub fn described(mut self, description: impl Into<String>) -> Self {
        if self.kind == JoinKind::FirstMatching {
            self.description = Some(description.into());
        }
        self
    }

    pub fn kind(&self) -> JoinKind {
        self.kind
    }

    /// A quorum's count.
    pub fn count(&self) -> Option<usize> {
        self.count
    }

    /// A `first_matching` predicate's description.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }
}

/// How a parallel group runs its branches and decides it is done
/// (`design/26-Parallel-Group-Controls-And-Joins.md`). The defaults are how
/// groups always behaved. Mirrors the .NET engine's `ParallelGroupOptions`.
pub struct ParallelGroupOptions<S> {
    max_concurrency: Option<usize>,
    branch_timeout: Option<Duration>,
    cancel_siblings_on_failure: bool,
    partial: PartialResults,
    join: Join<S>,
}

impl<S> Default for ParallelGroupOptions<S> {
    fn default() -> Self {
        Self {
            max_concurrency: None,
            branch_timeout: None,
            cancel_siblings_on_failure: true,
            partial: PartialResults::FailGroup,
            join: Join::all(),
        }
    }
}

impl<S> ParallelGroupOptions<S> {
    pub fn new() -> Self {
        Self::default()
    }

    /// At most `limit` branches run at once; the rest start in declaration
    /// order as running ones settle. `.compile()` refuses 0. A branch the
    /// group gives up on (timed out, or failed while the group goes on)
    /// settles at once and frees its slot: an async branch's future has been
    /// dropped by then, but a sync branch, abandoned, may still be running
    /// (`design/26`, "Timeouts and the concurrency limit").
    pub fn with_max_concurrency(mut self, limit: usize) -> Self {
        self.max_concurrency = Some(limit);
        self
    }

    /// Each branch's own timeout, from when it starts, on tokio's clock: a
    /// transient failure of that branch only, reported as
    /// [`BranchStatus::TimedOut`]. `.compile()` refuses zero.
    pub fn with_branch_timeout(mut self, timeout: Duration) -> Self {
        self.branch_timeout = Some(timeout);
        self
    }

    /// Under [`PartialResults::FailGroup`], when the join can no longer be
    /// met: cancel the branches still running and fail at once (`true`, the
    /// default), or let them finish and fail then.
    pub fn with_cancel_siblings_on_failure(mut self, cancel: bool) -> Self {
        self.cancel_siblings_on_failure = cancel;
        self
    }

    pub fn with_partial(mut self, partial: PartialResults) -> Self {
        self.partial = partial;
        self
    }

    /// `with_partial(PartialResults::KeepSucceeded)`.
    pub fn keep_succeeded(self) -> Self {
        self.with_partial(PartialResults::KeepSucceeded)
    }

    pub fn with_join(mut self, join: Join<S>) -> Self {
        self.join = join;
        self
    }

    pub fn max_concurrency(&self) -> Option<usize> {
        self.max_concurrency
    }

    pub fn branch_timeout(&self) -> Option<Duration> {
        self.branch_timeout
    }

    pub fn cancel_siblings_on_failure(&self) -> bool {
        self.cancel_siblings_on_failure
    }

    pub fn partial(&self) -> PartialResults {
        self.partial
    }

    pub fn join(&self) -> &Join<S> {
        &self.join
    }

    /// The join the definition exports: `None` for the default (`All`,
    /// failing the group).
    pub(crate) fn join_def(&self) -> Option<GraphJoinDef> {
        if self.join.kind == JoinKind::All && self.partial == PartialResults::FailGroup {
            return None;
        }
        Some(GraphJoinDef {
            policy: self.join.kind,
            count: self.join.count,
            condition: self.join.description.clone(),
            partial: self.partial,
        })
    }
}

/// How one branch of a parallel group ended, as an outcomes reducer sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchStatus {
    Succeeded,
    /// It failed with a [`NodeError`].
    Failed,
    /// It ran over [`ParallelGroupOptions::with_branch_timeout`].
    TimedOut,
    /// The group was done without it: it lost the race, a sibling failed, or
    /// it never started.
    Cancelled,
}

/// One branch's result, as an outcomes reducer sees it.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BranchOutcome<S> {
    pub name: String,
    pub status: BranchStatus,
    /// Its result state: `Some` exactly when it succeeded.
    pub state: Option<S>,
    /// Why it failed: `Some` when it failed or timed out.
    pub error: Option<NodeError>,
}

impl<S> BranchOutcome<S> {
    fn cancelled(name: &str) -> Self {
        Self {
            name: name.to_string(),
            status: BranchStatus::Cancelled,
            state: None,
            error: None,
        }
    }
}

/// Every branch's [`BranchOutcome`], in declaration order, and how the join
/// went: what
/// [`StateGraph::add_parallel_group_with_outcomes`](crate::graph::StateGraph::add_parallel_group_with_outcomes)'s
/// reducer receives.
#[derive(Debug, Clone)]
pub struct BranchOutcomes<S> {
    outcomes: Vec<BranchOutcome<S>>,
    join_met: bool,
    deciding: Vec<String>,
}

impl<S> BranchOutcomes<S> {
    pub fn outcomes(&self) -> &[BranchOutcome<S>] {
        &self.outcomes
    }

    pub fn into_outcomes(self) -> Vec<BranchOutcome<S>> {
        self.outcomes
    }

    /// Whether the join was met. `false` only under
    /// [`PartialResults::KeepSucceeded`], when the group reduces over what it
    /// has after the join could no longer be met.
    pub fn join_met(&self) -> bool {
        self.join_met
    }

    /// The branches that met the join, in the order they did: a race's winner,
    /// a quorum's first `count`, every branch for `All`. Empty when the join
    /// wasn't met.
    pub fn deciding(&self) -> &[String] {
        &self.deciding
    }

    /// The succeeded branches' states, in declaration order.
    pub fn succeeded(&self) -> impl Iterator<Item = &S> {
        self.outcomes
            .iter()
            .filter_map(|outcome| outcome.state.as_ref())
    }

    /// The outcome of the branch named `branch`, if the group has one.
    pub fn get(&self, branch: &str) -> Option<&BranchOutcome<S>> {
        self.outcomes.iter().find(|outcome| outcome.name == branch)
    }

    /// What the plain reducer gets: the deciding branches' states when the
    /// join was met, else every succeeded branch's, in declaration order.
    pub(crate) fn into_counted_states(self) -> Vec<S> {
        let BranchOutcomes {
            outcomes,
            join_met,
            deciding,
        } = self;
        outcomes
            .into_iter()
            .filter(|outcome| !join_met || deciding.contains(&outcome.name))
            .filter_map(|outcome| outcome.state)
            .collect()
    }
}

impl<S> IntoIterator for BranchOutcomes<S> {
    type Item = BranchOutcome<S>;
    type IntoIter = std::vec::IntoIter<BranchOutcome<S>>;

    fn into_iter(self) -> Self::IntoIter {
        self.outcomes.into_iter()
    }
}

/// How one branch settled, as its task reports it.
enum Settled<S> {
    Succeeded(S),
    Failed(NodeError),
    TimedOut(NodeError),
    Cancelled,
    Panicked(Box<dyn Any + Send>),
}

/// One branch's work, resolved to its result or the panic it raised.
pub(crate) type BranchWork<S> =
    BoxFuture<'static, Result<Result<S, NodeError>, Box<dyn Any + Send>>>;

/// What a branch of a fan-out is started with: its qualified path
/// (`"<group>/<branch>"`), its span, and its own token (the siblings' token,
/// cancelled by its own timeout too).
pub(crate) struct Launch {
    pub(crate) path: Arc<str>,
    pub(crate) span: Span,
    pub(crate) token: CancellationToken,
}

/// Starts one branch of a fan-out (a parallel group's branch, or a map's
/// item): its index in declaration order, and what it is started with. Called
/// on the executor; the future it returns is the branch's work.
pub(crate) type Launcher<'a, R> = dyn FnMut(usize, Launch) -> BranchWork<R> + Send + 'a;

/// What a fan-out calls its branches in messages: `branch` for a group,
/// `item` for a map.
#[derive(Clone, Copy)]
pub(crate) struct Noun {
    pub(crate) one: &'static str,
    pub(crate) many: &'static str,
}

impl Noun {
    pub(crate) const BRANCH: Noun = Noun {
        one: "branch",
        many: "branches",
    };
    pub(crate) const ITEM: Noun = Noun {
        one: "item",
        many: "items",
    };
}

/// How a fan-out runs (`design/26-Parallel-Group-Controls-And-Joins.md`,
/// `design/27-Map-Fan-Out.md`): a group's or a map's options, what it calls
/// its branches, and whether a branch it gives up on winds down (`winds_down`:
/// a map item, which is a graph) or is abandoned (a group's branch, which is a
/// handler).
pub(crate) struct FanOut<'a, R> {
    pub(crate) max_concurrency: Option<usize>,
    pub(crate) timeout: Option<Duration>,
    pub(crate) cancel_siblings_on_failure: bool,
    pub(crate) partial: PartialResults,
    pub(crate) join: &'a Join<R>,
    pub(crate) noun: Noun,
    pub(crate) winds_down: bool,
}

/// Runs a sync branch on the blocking pool inside `span`, catching its panic
/// so the group knows which branch it was.
fn on_blocking_pool<S, F>(span: Span, work: F) -> BranchWork<S>
where
    S: Send + 'static,
    F: FnOnce() -> Result<S, NodeError> + Send + 'static,
{
    let handle = tokio::task::spawn_blocking(move || {
        std::panic::catch_unwind(AssertUnwindSafe(|| span.in_scope(work)))
    });
    Box::pin(async move {
        match handle.await {
            Ok(result) => result,
            Err(error) if error.is_panic() => Err(error.into_panic()),
            Err(_) => Ok(Err(NodeError::Fatal(
                "the runtime shut down while the branch was running".to_string(),
            ))),
        }
    })
}

/// One run of a fan-out's branches: what is running, what has settled, and how
/// far the join is.
struct GroupRun<'a, 'l, R> {
    names: &'a [String],
    launch: &'a mut Launcher<'l, R>,
    settings: &'a FanOut<'a, R>,
    inputs: &'a AttemptInputs,
    siblings: CancellationToken,
    telemetry: Arc<RunTelemetry>,
    set: JoinSet<(usize, Settled<R>)>,
    scopes: Vec<Option<BranchScope>>,
    outcomes: Vec<Option<BranchOutcome<R>>>,
    running: Vec<bool>,
    in_flight: usize,
    deciding: Vec<usize>,
    next: usize,
    counted: usize,
    needed: usize,
    decided: bool,
    met: bool,
    start_more: bool,
}

impl<R> GroupRun<'_, '_, R>
where
    R: Send + 'static,
{
    /// Starts branches in declaration order while the limit allows.
    fn start_more(&mut self) {
        let limit = self.settings.max_concurrency.unwrap_or(self.names.len());
        while self.start_more && self.next < self.names.len() && self.in_flight < limit {
            let index = self.next;
            self.next += 1;
            self.start(index);
        }
    }

    /// Starts one branch inside its own span, with its own token (a child of
    /// the siblings' one) and its own timeout, on tokio's clock.
    fn start(&mut self, index: usize) {
        let name = &self.names[index];
        // One copy of the path, shared by the branch's telemetry, its
        // context and (for a map's item) its level.
        let path: Arc<str> = Arc::from(format!("{}/{name}", self.inputs.context.node_path()));
        let scope =
            BranchScope::start(&self.telemetry, path.clone(), self.inputs.context.attempt());
        let token = self.siblings.child_token();
        let mut work = (self.launch)(
            index,
            Launch {
                path,
                span: scope.span.clone(),
                token: token.clone(),
            },
        );
        let timeout = self.settings.timeout;
        let winds_down = self.settings.winds_down;
        // Only a branch with a timeout can time out, so only it needs the words.
        let message = match timeout {
            Some(timeout) => format!(
                "{} '{name}' timed out after {} ms",
                self.settings.noun.one,
                timeout.as_millis()
            ),
            None => String::new(),
        };
        self.set.spawn(async move {
            let expiry = async {
                match timeout {
                    Some(duration) => tokio::time::sleep(duration).await,
                    None => std::future::pending().await,
                }
            };
            let settled = if winds_down {
                // A branch that is a graph is waited for until it has
                // stopped; if its token fired and it didn't succeed, its
                // token is what stopped it.
                let finished = tokio::select! {
                    biased;
                    result = &mut work => Some(result),
                    _ = expiry => {
                        token.cancel();
                        let _ = work.await;
                        None
                    }
                };
                match finished {
                    None => Settled::TimedOut(NodeError::Transient(message)),
                    Some(Ok(Ok(state))) => Settled::Succeeded(state),
                    Some(_) if token.is_cancelled() => Settled::Cancelled,
                    Some(Ok(Err(error))) => Settled::Failed(error),
                    Some(Err(panic)) => Settled::Panicked(panic),
                }
            } else {
                tokio::select! {
                    biased;
                    result = work => match result {
                        Ok(Ok(state)) => Settled::Succeeded(state),
                        Ok(Err(error)) => Settled::Failed(error),
                        Err(panic) => Settled::Panicked(panic),
                    },
                    _ = token.cancelled() => Settled::Cancelled,
                    _ = expiry => {
                        token.cancel();
                        Settled::TimedOut(NodeError::Transient(message))
                    }
                }
            };
            (index, settled)
        });
        self.scopes[index] = Some(scope);
        self.running[index] = true;
        self.in_flight += 1;
    }

    /// Counts a settled branch towards the join and ends its scope. A panic
    /// is re-raised once the rest are told to stop.
    fn record(&mut self, index: usize, settled: Settled<R>) {
        self.running[index] = false;
        self.in_flight -= 1;
        let name = self.names[index].clone();
        let scope = self.scopes[index].as_ref();
        let outcome = match settled {
            Settled::Panicked(panic) => {
                if let Some(scope) = scope {
                    scope.panicked(&*panic);
                }
                self.give_up_now(None);
                std::panic::resume_unwind(panic);
            }
            Settled::Succeeded(state) => {
                if let Some(scope) = scope {
                    scope.completed(BranchResult::Succeeded, None, None);
                }
                let counts = match &self.settings.join.predicate {
                    Some(predicate) => predicate(&state),
                    None => true,
                };
                if !self.decided && counts {
                    self.counted += 1;
                    self.deciding.push(index);
                }
                BranchOutcome {
                    name,
                    status: BranchStatus::Succeeded,
                    state: Some(state),
                    error: None,
                }
            }
            Settled::Failed(error) => {
                if let Some(scope) = scope {
                    scope.completed(BranchResult::Failed, Some(&error), None);
                }
                BranchOutcome {
                    name,
                    status: BranchStatus::Failed,
                    state: None,
                    error: Some(error),
                }
            }
            Settled::TimedOut(error) => {
                if let Some(scope) = scope {
                    scope.completed(
                        BranchResult::Failed,
                        Some(&error),
                        Some(BranchEndReason::TimedOut),
                    );
                }
                BranchOutcome {
                    name,
                    status: BranchStatus::TimedOut,
                    state: None,
                    error: Some(error),
                }
            }
            Settled::Cancelled => {
                if let Some(scope) = scope {
                    scope.completed(BranchResult::Cancelled, None, None);
                }
                BranchOutcome::cancelled(&name)
            }
        };
        self.outcomes[index] = Some(outcome);
    }

    /// Reports the join's decision, for a join other than `All`.
    fn decide(&self, met: bool) {
        let join = self.settings.join;
        if join.kind == JoinKind::All {
            return;
        }
        let deciding = if met {
            self.deciding
                .iter()
                .map(|index| self.names[*index].clone())
                .collect()
        } else {
            Vec::new()
        };
        self.telemetry.join_decided(
            self.inputs.context.node_path(),
            self.inputs.context.attempt(),
            join.kind.as_str(),
            met,
            deciding,
            &Span::current(),
        );
    }

    /// Stops waiting for the branches still running: the ones that had already
    /// settled count as what they were; the rest are told to stop and, when
    /// they wind down, waited for; then they are reported cancelled for
    /// `reason`.
    async fn give_up(&mut self, reason: Option<BranchEndReason>) {
        if self.settings.winds_down && self.in_flight > 0 {
            self.start_more = false;
            self.record_settled();
            self.siblings.cancel();
            // Every task resolves once its branch has stopped; what it
            // resolved to no longer matters.
            while self.set.join_next().await.is_some() {}
        }
        self.give_up_now(reason);
    }

    /// [`Self::give_up`] without waiting. Their tasks are aborted; a sync
    /// branch can't be stopped, so it runs on detached and its result is
    /// discarded.
    fn give_up_now(&mut self, reason: Option<BranchEndReason>) {
        self.start_more = false;
        self.record_settled();
        self.siblings.cancel();
        for index in 0..self.names.len() {
            if self.running[index] {
                if let Some(scope) = &self.scopes[index] {
                    scope.completed(BranchResult::Cancelled, None, reason);
                }
                self.outcomes[index] = Some(BranchOutcome::cancelled(&self.names[index]));
                self.running[index] = false;
            }
        }
        self.in_flight = 0;
        self.set.abort_all();
    }

    /// Records the branches that have settled but not been seen yet.
    fn record_settled(&mut self) {
        while let Some(joined) = self.set.try_join_next() {
            match joined {
                Ok((index, Settled::Panicked(panic))) => {
                    if let Some(scope) = &self.scopes[index] {
                        scope.panicked(&*panic);
                    }
                    self.siblings.cancel();
                    self.set.abort_all();
                    std::panic::resume_unwind(panic);
                }
                Ok((index, settled)) => self.record(index, settled),
                Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
                Err(_) => {}
            }
        }
    }
}

/// Starts a group's branches against `state` — a sync one on the blocking
/// pool, an async one as a task — through [`run_fan_out`]. A branch the group
/// gives up on is abandoned, never waited for.
///
/// The executor drops this future when the attempt's token fires (the run was
/// cancelled, or the group timed out); the branches' tokens are children of
/// that one, and dropping the [`JoinSet`] aborts their tasks.
///
/// `inputs.stream` is the group's own [`StreamContext`] when the run is
/// streaming; each branch gets a copy of it retargeted at `"<group>/<branch>"`.
pub(crate) async fn run_branches<S>(
    group: &str,
    branches: &[(String, Branch<S>)],
    options: &ParallelGroupOptions<S>,
    state: S,
    inputs: AttemptInputs,
) -> Result<BranchOutcomes<S>, NodeError>
where
    S: Clone + Send + Sync + 'static,
{
    let state = Arc::new(state);
    let names: Vec<String> = branches.iter().map(|(name, _)| name.clone()).collect();
    let context = inputs.context.clone();
    let stream = inputs.stream.clone();
    let mut launch = move |index: usize, started: Launch| -> BranchWork<S> {
        let (name, branch) = &branches[index];
        let stream = stream
            .as_ref()
            .map(|ctx| ctx.for_node(format!("{group}/{name}")));
        let state = state.clone();
        match branch {
            Branch::Plain(run) => {
                let run = run.clone();
                on_blocking_pool(started.span, move || run(&state))
            }
            Branch::Streaming(run) => {
                let run = run.clone();
                on_blocking_pool(started.span, move || match stream {
                    Some(ctx) => run(&state, &ctx),
                    None => Err(no_stream()),
                })
            }
            Branch::Async { run, streaming } => {
                let result = if *streaming && stream.is_none() {
                    None
                } else {
                    let ctx = context.for_branch(
                        name,
                        started.path,
                        stream,
                        started.token,
                        started.span.clone(),
                    );
                    Some(run((*state).clone(), ctx))
                };
                let future = async move {
                    match result {
                        Some(future) => future.await,
                        None => Err(no_stream()),
                    }
                }
                .instrument(started.span);
                Box::pin(CatchUnwind::new(future))
            }
        }
    };
    let settings = FanOut {
        max_concurrency: options.max_concurrency,
        timeout: options.branch_timeout,
        cancel_siblings_on_failure: options.cancel_siblings_on_failure,
        partial: options.partial,
        join: &options.join,
        noun: Noun::BRANCH,
        winds_down: false,
    };
    run_fan_out(&names, &mut launch, &settings, &inputs).await
}

/// Starts the branches of a fan-out (a parallel group's, or a map's items), at
/// most `max_concurrency` at a time, in declaration order, and settles them
/// until the join is decided (`design/26-Parallel-Group-Controls-And-Joins.md`).
///
/// The join is met when enough branches succeeded (or, for `first_matching`,
/// matched); the branches still running are then cancelled as having lost the
/// race, and the rest never start. A failure, or a branch timeout, matters
/// only once the join can no longer be met: then
/// [`PartialResults::FailGroup`] fails the fan-out, at once (cancelling the
/// rest) unless `cancel_siblings_on_failure` is off, and
/// [`PartialResults::KeepSucceeded`] waits for every branch and returns the
/// outcomes, flagged unmet, failing only if nothing succeeded. A join that
/// needs nothing (an `All` map with no items) is met before anything starts;
/// one that needs more branches than there are can't be met, which is decided
/// before anything starts too.
///
/// The fan-out fails transiently if a retry could meet the join (each
/// transient failure counting as a branch that might succeed), carrying the
/// longest retry hint among its failures (`design/25-Retry-Backoff.md`); the
/// message names each failure in declaration order. A panicking branch is
/// re-raised as soon as it is seen.
///
/// Each branch that starts runs inside its own span and gets a
/// `BranchStarted` / `BranchCompleted` pair (`design/18-Run-Observability.md`),
/// with a reason when the result alone doesn't say why; a join other than
/// `All` reports its decision as `JoinDecided`. A branch given up on is
/// abandoned, or, when `winds_down`, waited for until it has stopped, so
/// everything it reports comes before its `BranchCompleted`
/// (`design/27-Map-Fan-Out.md`). A fan-out that winds down also watches the
/// attempt's token: when it fires, every branch winds down and the fan-out
/// returns an error the executor discards (it reports the node cancelled or
/// timed out).
pub(crate) async fn run_fan_out<R>(
    names: &[String],
    launch: &mut Launcher<'_, R>,
    settings: &FanOut<'_, R>,
    inputs: &AttemptInputs,
) -> Result<BranchOutcomes<R>, NodeError>
where
    R: Send + 'static,
{
    let count = names.len();
    let join = settings.join;
    let needed = match join.kind {
        JoinKind::All => count,
        JoinKind::Quorum => join.count.unwrap_or(count),
        JoinKind::FirstSuccess | JoinKind::FirstMatching => 1,
    };
    let mut run = GroupRun {
        names,
        launch,
        settings,
        inputs,
        siblings: inputs.token().child_token(),
        telemetry: inputs.level.run.telemetry.clone(),
        set: JoinSet::new(),
        scopes: (0..count).map(|_| None).collect(),
        outcomes: (0..count).map(|_| None).collect(),
        running: vec![false; count],
        in_flight: 0,
        deciding: Vec::new(),
        next: 0,
        counted: 0,
        needed,
        decided: false,
        met: false,
        start_more: true,
    };

    if needed == 0 {
        run.decided = true;
        run.met = true;
    } else if count < needed {
        run.decided = true;
        run.decide(false);
        run.start_more = settings.partial == PartialResults::KeepSucceeded;
    }

    run.start_more();
    while run.in_flight > 0 {
        let joined = if settings.winds_down {
            tokio::select! {
                biased;
                _ = inputs.token().cancelled() => {
                    run.give_up(None).await;
                    return Err(NodeError::Fatal("cancelled".to_string()));
                }
                joined = run.set.join_next() => joined,
            }
        } else {
            run.set.join_next().await
        };
        let Some(joined) = joined else {
            break;
        };
        let (index, settled) = match joined {
            Ok(settled) => settled,
            Err(error) if error.is_panic() => {
                run.siblings.cancel();
                run.set.abort_all();
                std::panic::resume_unwind(error.into_panic());
            }
            // Aborted: not something this fan-out did before giving up.
            Err(_) => continue,
        };
        run.record(index, settled);

        if !run.decided && run.counted >= run.needed {
            run.decided = true;
            run.met = true;
            run.decide(true);
            run.give_up(Some(BranchEndReason::LostRace)).await;
            break;
        }

        if !run.decided && run.counted + run.in_flight + (count - run.next) < run.needed {
            run.decided = true;
            run.decide(false);
            if settings.partial == PartialResults::FailGroup {
                run.start_more = false;
                if settings.cancel_siblings_on_failure {
                    run.give_up(Some(BranchEndReason::SiblingFailed)).await;
                    break;
                }
            }
        }

        run.start_more();
    }

    let met = run.met;
    let deciding: Vec<String> = if met {
        run.deciding
            .iter()
            .map(|index| names[*index].clone())
            .collect()
    } else {
        Vec::new()
    };
    // Dropping the set now detaches the sync branches still running: they
    // can't be stopped, only abandoned.
    drop(run.set);
    let outcomes: Vec<BranchOutcome<R>> = run
        .outcomes
        .into_iter()
        .zip(names)
        .map(|(outcome, name)| outcome.unwrap_or_else(|| BranchOutcome::cancelled(name)))
        .collect();
    let results = BranchOutcomes {
        outcomes,
        join_met: met,
        deciding,
    };
    if met
        || (settings.partial == PartialResults::KeepSucceeded
            && results.succeeded().next().is_some())
    {
        return Ok(results);
    }
    Err(failure(join, needed, &results, settings.noun))
}

/// The fan-out's failure, once its join wasn't met
/// (`design/26-Parallel-Group-Controls-And-Joins.md`): transient if a retry
/// could meet it, with the longest retry hint among the transient failures.
fn failure<R>(join: &Join<R>, needed: usize, results: &BranchOutcomes<R>, noun: Noun) -> NodeError {
    let failures: Vec<(&str, &NodeError)> = results
        .outcomes
        .iter()
        .filter_map(|outcome| {
            outcome
                .error
                .as_ref()
                .map(|error| (outcome.name.as_str(), error))
        })
        .collect();
    let succeeded = results.succeeded().count();
    let mut parts: Vec<String> = Vec::new();
    match join.kind {
        JoinKind::FirstMatching => {
            parts.push(format!(
                "no {} matched the join ({succeeded} succeeded)",
                noun.one
            ));
        }
        JoinKind::Quorum => parts.push(format!(
            "quorum of {needed} not reached: {succeeded} of {} {} succeeded",
            results.outcomes.len(),
            noun.many
        )),
        JoinKind::All | JoinKind::FirstSuccess => {}
    }
    parts.extend(
        failures
            .iter()
            .map(|(name, error)| format!("{} '{name}': {}", noun.one, error.message())),
    );
    if parts.is_empty() {
        parts.push(format!("no {} to run", noun.many));
    }
    let message = parts.join("; ");
    let fatal = failures
        .iter()
        .filter(|(_, error)| !error.is_transient())
        .count();
    if results.outcomes.len() - fatal < needed {
        return NodeError::Fatal(message);
    }
    match failures
        .iter()
        .filter_map(|(_, error)| error.retry_after())
        .max()
    {
        Some(hint) => NodeError::TransientAfter(message, hint),
        None => NodeError::Transient(message),
    }
}
