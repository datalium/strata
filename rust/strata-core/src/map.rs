//! Dynamic fan-out: a compiled child graph run once per item of a list read
//! from the state when the node starts (see `design/27-Map-Fan-Out.md`).
//!
//! A map is registered with [`crate::graph::StateGraph::add_map`] and is, from
//! the graph's point of view, one ordinary node: it has ordinary edges in and
//! out, can be an interrupt boundary, and carries a
//! [`RecoveryPolicy`](crate::graph::RecoveryPolicy). Its items are a parallel
//! group's branches (`design/26-Parallel-Group-Controls-And-Joins.md`), run by
//! the same runner, each running the child at a level of its own, as a
//! subgraph's child runs (`design/15-Subgraph-Composition.md`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::{Instrument, Span};

use crate::checkpoint::{CheckpointStore, GraphExecutionResult, RunRefs};
use crate::graph::{CompiledGraph, NodeError};
use crate::node::{AttemptInputs, BoxFuture, Suspension};
use crate::observability::CatchUnwind;
use crate::parallel::{
    BranchOutcomes, BranchWork, FanOut, Join, JoinKind, Launch, Noun, PartialResults, run_fan_out,
};
use crate::schema::{GraphDefinition, GraphJoinDef};
use crate::subgraph::{AttemptError, CapturingStore, Step, child_failure};

/// How a map runs its items and decides it is done
/// (`design/27-Map-Fan-Out.md`). A map's items are a parallel group's
/// branches, so every option means what it means on
/// [`ParallelGroupOptions`](crate::parallel::ParallelGroupOptions)
/// (`design/26-Parallel-Group-Controls-And-Joins.md`), under item names.
/// Unlike a group's, the concurrency limit is always bounded. Mirrors the .NET
/// engine's `MapOptions`.
pub struct MapOptions<T> {
    max_concurrency: usize,
    item_timeout: Option<Duration>,
    cancel_siblings_on_failure: bool,
    partial: PartialResults,
    join: Join<T>,
    max_items: Option<usize>,
}

impl<T> Default for MapOptions<T> {
    fn default() -> Self {
        Self {
            max_concurrency: Self::DEFAULT_MAX_CONCURRENCY,
            item_timeout: None,
            cancel_siblings_on_failure: true,
            partial: PartialResults::FailGroup,
            join: Join::all(),
            max_items: None,
        }
    }
}

impl<T> MapOptions<T> {
    /// The concurrency limit a map has unless it says otherwise.
    pub const DEFAULT_MAX_CONCURRENCY: usize = 8;

    pub fn new() -> Self {
        Self::default()
    }

    /// At most `limit` items run at once; the rest start in list order as
    /// running ones settle. [`Self::DEFAULT_MAX_CONCURRENCY`] by default.
    /// `.compile()` refuses 0.
    pub fn with_max_concurrency(mut self, limit: usize) -> Self {
        self.max_concurrency = limit;
        self
    }

    /// Each item's own timeout, from when it starts, on tokio's clock: a
    /// transient failure of that item only, reported as
    /// [`BranchStatus::TimedOut`](crate::parallel::BranchStatus::TimedOut),
    /// its child wound down. `.compile()` refuses zero.
    pub fn with_item_timeout(mut self, timeout: Duration) -> Self {
        self.item_timeout = Some(timeout);
        self
    }

    /// Under [`PartialResults::FailGroup`], when the join can no longer be
    /// met: cancel the items still running and fail at once (`true`, the
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

    pub fn with_join(mut self, join: Join<T>) -> Self {
        self.join = join;
        self
    }

    /// More items than `limit` fail the map fatally, before any starts.
    /// `.compile()` refuses 0.
    pub fn with_max_items(mut self, limit: usize) -> Self {
        self.max_items = Some(limit);
        self
    }

    pub fn max_concurrency(&self) -> usize {
        self.max_concurrency
    }

    pub fn item_timeout(&self) -> Option<Duration> {
        self.item_timeout
    }

    pub fn cancel_siblings_on_failure(&self) -> bool {
        self.cancel_siblings_on_failure
    }

    pub fn partial(&self) -> PartialResults {
        self.partial
    }

    pub fn join(&self) -> &Join<T> {
        &self.join
    }

    pub fn max_items(&self) -> Option<usize> {
        self.max_items
    }

    /// The join the definition exports: `None` for the default (`All`,
    /// failing the map).
    pub(crate) fn join_def(&self) -> Option<GraphJoinDef> {
        if self.join.kind() == JoinKind::All && self.partial == PartialResults::FailGroup {
            return None;
        }
        Some(GraphJoinDef {
            policy: self.join.kind(),
            count: self.join.count(),
            condition: self.join.description().map(str::to_string),
            partial: self.partial,
        })
    }
}

/// What a map keeps for `.compile()`'s checks and the exported definition:
/// the child's definition (the template), the join when it isn't the
/// default, the first interrupt boundary the child could freeze at, and the
/// limits `.compile()` refuses at zero.
#[derive(Debug, Clone)]
pub(crate) struct MapShape {
    pub(crate) template: GraphDefinition,
    pub(crate) join: Option<GraphJoinDef>,
    pub(crate) boundary: Option<Suspension>,
    pub(crate) max_concurrency: usize,
    pub(crate) item_timeout: Option<Duration>,
    pub(crate) max_items: Option<usize>,
}

impl<T> MapOptions<T> {
    pub(crate) fn shape<S>(&self, child: &CompiledGraph<S>) -> MapShape {
        MapShape {
            template: child.to_definition(),
            join: self.join_def(),
            boundary: child.interrupt_boundary(),
            max_concurrency: self.max_concurrency,
            item_timeout: self.item_timeout,
            max_items: self.max_items,
        }
    }
}

/// Reads a map's items from the state.
pub(crate) type ItemsFn<S, I> = Arc<dyn Fn(&S) -> Vec<I> + Send + Sync>;
/// Names an item.
pub(crate) type KeyFn<I> = Arc<dyn Fn(&I) -> String + Send + Sync>;
/// Builds an item's child state from the map's state and the item.
pub(crate) type ItemInputFn<S, I, T> = Arc<dyn Fn(&S, I) -> T + Send + Sync>;
/// Merges the items' outcomes back into the map's state.
pub(crate) type MapReducerFn<S, T> = Arc<dyn Fn(&S, BranchOutcomes<T>) -> S + Send + Sync>;

/// Everything a map node runs with.
pub(crate) struct MapSpec<S, I, T> {
    pub(crate) name: Arc<str>,
    pub(crate) items: ItemsFn<S, I>,
    pub(crate) key: KeyFn<I>,
    pub(crate) child: Arc<CompiledGraph<T>>,
    pub(crate) input: ItemInputFn<S, I, T>,
    pub(crate) reducer: MapReducerFn<S, T>,
    pub(crate) options: MapOptions<T>,
}

/// One attempt of a map: its items from `state`, each named by its key; then
/// `MapExpanded`, and the items, each running the child from the state the
/// input builds when it starts; then the reducer over their outcomes.
///
/// More items than [`MapOptions::with_max_items`], an empty key, a key
/// containing `/` or two items with one key fail the map fatally before
/// anything starts: the keys come from the state, so a retry would see them
/// again.
///
/// A map winds down: when the attempt's token fires (the run was cancelled,
/// or the map timed out) every item's child stops at its current node, and the
/// map returns once they have, for the executor to report it stopped.
pub(crate) async fn run_map<S, I, T>(
    spec: &MapSpec<S, I, T>,
    state: S,
    inputs: AttemptInputs,
) -> Result<Step<S>, AttemptError>
where
    S: Send + Sync + 'static,
    I: Send + 'static,
    T: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    let name = &spec.name;
    let list = (spec.items)(&state);
    if let Some(max) = spec.options.max_items
        && list.len() > max
    {
        return Err(NodeError::Fatal(format!(
            "map '{name}' has {} items, more than its limit of {max}",
            list.len()
        ))
        .into());
    }

    let mut keys: Vec<String> = Vec::with_capacity(list.len());
    let mut seen: HashMap<String, usize> = HashMap::new();
    for (index, item) in list.iter().enumerate() {
        let key = (spec.key)(item);
        if key.is_empty() {
            return Err(
                NodeError::Fatal(format!("map '{name}': item {index} has an empty key")).into(),
            );
        }
        if key.contains('/') {
            return Err(NodeError::Fatal(format!(
                "map '{name}': item {index} has the key '{key}', which contains '/'"
            ))
            .into());
        }
        if let Some(first) = seen.get(&key) {
            return Err(NodeError::Fatal(format!(
                "map '{name}': items {first} and {index} have the same key '{key}'"
            ))
            .into());
        }
        seen.insert(key.clone(), index);
        keys.push(key);
    }

    inputs.level.run.telemetry.map_expanded(
        inputs.context.node_path(),
        inputs.context.attempt(),
        keys.clone(),
        &Span::current(),
    );

    let mut items: Vec<Option<I>> = list.into_iter().map(Some).collect();
    let mut launch = |index: usize, started: Launch| -> BranchWork<T> {
        let item = items[index].take().expect("each item starts once");
        let child_state = (spec.input)(&state, item);
        let key = keys[index].clone();
        let stream = inputs
            .stream
            .as_ref()
            .map(|ctx| ctx.for_node(format!("{name}/{key}")).scoped());
        let level = inputs.level.child(started.token, &started.path);
        let child = spec.child.clone();
        let future: BoxFuture<'static, Result<T, NodeError>> = Box::pin(
            async move {
                let capture = Arc::new(CapturingStore::default());
                let store: Arc<dyn CheckpointStore> = capture.clone();
                let run = RunRefs {
                    thread_id: &key,
                    store: &store,
                    stream: stream.as_ref(),
                    level: &level,
                };
                let entry: &str = &child.definition.entry_point;
                let result = child.execute_from(run, entry, child_state, None, 0).await;
                let captured = capture.last.lock().unwrap().take();
                match result {
                    GraphExecutionResult::Completed(state) => Ok(state),
                    // Only the item's token stops its child; the runner sees
                    // it fired and reports the item cancelled or timed out.
                    GraphExecutionResult::Cancelled { node, .. } => {
                        Err(NodeError::Fatal(format!("cancelled at '{node}'")))
                    }
                    // `.compile()` refuses a child with an interrupt boundary,
                    // so this is only a guard.
                    GraphExecutionResult::Interrupted { suspended_node, .. } => {
                        Err(NodeError::Fatal(format!(
                            "froze at '{suspended_node}', which a map item can't do"
                        )))
                    }
                    GraphExecutionResult::Failed(error) => {
                        Err(child_failure(None, error, captured).error)
                    }
                }
            }
            .instrument(started.span),
        );
        Box::pin(CatchUnwind::new(future))
    };
    let options = &spec.options;
    let settings = FanOut {
        max_concurrency: Some(options.max_concurrency),
        timeout: options.item_timeout,
        cancel_siblings_on_failure: options.cancel_siblings_on_failure,
        partial: options.partial,
        join: &options.join,
        noun: Noun::ITEM,
        winds_down: true,
    };
    let outcomes = run_fan_out(&keys, &mut launch, &settings, &inputs).await?;
    Ok(Step::Done((spec.reducer)(&state, outcomes)))
}
