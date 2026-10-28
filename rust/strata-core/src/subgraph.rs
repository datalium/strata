//! Subgraph composition: a compiled graph run as one node of another graph
//! (see `design/15-Subgraph-Composition.md`).
//!
//! A subgraph is registered with
//! [`crate::graph::StateGraph::add_subgraph`] and is, from the parent's point
//! of view, one ordinary node: it has ordinary edges in and out, can be an
//! interrupt boundary, and carries a
//! [`RecoveryPolicy`](crate::graph::RecoveryPolicy). Under the hood it is
//! registered as a *fallible* node whose handler runs [`run_subgraph`], which
//! is why retry, fallback and `Failed` checkpoints work for it with no
//! executor changes.

use std::sync::{Arc, Mutex};

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::StrataError;
use crate::checkpoint::{
    CheckpointStore, CheckpointStoreError, GraphExecutionResult, LevelSignals, RunRefs,
    stored_retry_after,
};
use crate::graph::{CompiledGraph, NodeError};
use crate::node::{AttemptInputs, BoxFuture};
use crate::schema::{Checkpoint, CheckpointStatus};

/// Maps the parent's state to the child graph's initial state.
pub type SubgraphInputFn<S, T> = Box<dyn Fn(&S) -> T + Send + Sync>;

/// Folds the child graph's final state back into the parent's. Receives the
/// state the subgraph node started with and the child's result.
pub type SubgraphOutputFn<S, T> = Box<dyn Fn(&S, T) -> S + Send + Sync>;

/// What one attempt of a fallible node produced: a new state, or — only from a
/// subgraph node — the checkpoint of a child that froze at an interrupt
/// boundary (`design/16-Nested-Interrupts.md`). A freeze is not a failure, so
/// it is not an `Err` and no recovery policy sees it.
pub(crate) enum Step<S> {
    Done(S),
    Interrupted(Box<Checkpoint>),
    /// Only from a subgraph node: its child stopped because its token was
    /// cancelled (the run was cancelled, or this node timed out), and left this
    /// `cancelled` checkpoint (`design/17-Async-Execution.md`). Which of the
    /// two it was is the executor's call.
    Cancelled(Box<Checkpoint>),
}

/// Why one attempt of a fallible node failed, as the engine stores it: the
/// [`NodeError`] the recovery policy acts on, and — only from a subgraph node
/// whose child failed at one of its own nodes — the child's `Failed`
/// checkpoint, for the parent to nest in its own `Failed` one
/// (`design/16-Nested-Interrupts.md`, "Resuming a failed thread").
pub(crate) struct AttemptError {
    pub(crate) error: NodeError,
    pub(crate) child: Option<Box<Checkpoint>>,
}

impl From<NodeError> for AttemptError {
    fn from(error: NodeError) -> Self {
        Self { error, child: None }
    }
}

/// What a subgraph node is given when the run is resuming into it: the child's
/// own checkpoint (frozen or failed), and the signals for the child's level and
/// the levels below it.
#[derive(Clone)]
pub(crate) struct NestedResume {
    pub(crate) checkpoint: Checkpoint,
    pub(crate) signals: LevelSignals,
}

/// The store a child run persists to. Nothing the child writes is kept: the
/// child is not a thread of its own, and the parent persists *its* failure
/// itself. The last checkpoint is remembered so the failure kind
/// (`transient` / `fatal`) the child recorded can reach the parent —
/// `StrataError::NodeFailed` doesn't carry it — and so a child that froze can
/// hand its checkpoint up to be nested in the parent's, as can one that was
/// cancelled.
#[derive(Default)]
pub(crate) struct CapturingStore {
    pub(crate) last: Mutex<Option<Checkpoint>>,
}

impl CheckpointStore for CapturingStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        *self.last.lock().unwrap() = Some(checkpoint);
        Box::pin(std::future::ready(Ok(())))
    }

    fn load<'a>(
        &'a self,
        _thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        Box::pin(std::future::ready(Ok(None)))
    }
}

/// Runs `child` against the state `input` derives from `state` — or, when the
/// run is resuming into this node, resumes it from the checkpoint in `inputs`
/// without calling `input` again — and folds its result back with `output`.
///
/// If the child freezes at an interrupt boundary the node returns
/// [`Step::Interrupted`] with the child's checkpoint, for the parent to nest in
/// its own. A retry after a failed resumed attempt resumes from the same
/// checkpoint again, so the human is not asked twice.
///
/// A child failure becomes a [`NodeError`] on the subgraph node, carrying the
/// child's own `Failed` checkpoint when it left one: `Fatal` if
/// the failing child node's error was fatal, or the failure isn't a node
/// failure at all (a routing gap, a missing node — retrying can't fix those);
/// `Transient` if the child's own recovery ran out on a transient error. So
/// the parent's [`RecoveryPolicy`](crate::graph::RecoveryPolicy) can retry a
/// child that failed for a transient reason, and skips retrying one that
/// didn't. The child's own policies (retries, fallbacks) have already run by
/// then. A transient failure carries the retry hint the child's checkpoint
/// recorded (`strata.retry_after_ms`, `design/25-Retry-Backoff.md`).
///
/// The node's stream context, when the run is streaming, reaches the child
/// scoped ([`crate::streaming::StreamContext::scoped`]), so everything the
/// child streams — lifecycle chunks, tokens, retries — arrives named
/// `"<name>/<child node>"`.
///
/// The child runs at a level of its own whose root token is this attempt's
/// (`design/17-Async-Execution.md`), so a cancelled run or this node's timeout
/// reaches it at any depth. It then stops where it is and hands back a
/// `cancelled` checkpoint as [`Step::Cancelled`], for the executor to nest.
pub(crate) async fn run_subgraph<S, T>(
    name: &str,
    child: &CompiledGraph<T>,
    input: &SubgraphInputFn<S, T>,
    output: &SubgraphOutputFn<S, T>,
    state: S,
    inputs: AttemptInputs,
) -> Result<Step<S>, AttemptError>
where
    T: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    let capture = Arc::new(CapturingStore::default());
    let store: Arc<dyn CheckpointStore> = capture.clone();
    let child_stream = inputs.stream.as_ref().map(|stream| stream.scoped());
    let level = inputs
        .level
        .child(inputs.token().clone(), inputs.context.node_path());
    let result = match inputs.resume {
        None => {
            let run = RunRefs {
                thread_id: name,
                store: &store,
                stream: child_stream.as_ref(),
                level: &level,
            };
            let entry: &str = &child.definition.entry_point;
            child.execute_from(run, entry, input(&state), None, 0).await
        }
        Some(resume) => {
            child
                .resume_checkpoint(
                    name,
                    resume.checkpoint,
                    resume.signals,
                    &store,
                    child_stream.as_ref(),
                    &level,
                )
                .await
        }
    };
    let captured = capture.last.lock().unwrap().take();
    match result {
        GraphExecutionResult::Completed(result) => Ok(Step::Done(output(&state, result))),
        GraphExecutionResult::Interrupted { suspended_node, .. } => match captured {
            Some(frozen) if frozen.status == CheckpointStatus::Interrupted => {
                Ok(Step::Interrupted(Box::new(frozen)))
            }
            _ => Err(NodeError::Fatal(format!(
                "subgraph '{name}' froze at '{suspended_node}' but its checkpoint was not captured"
            ))
            .into()),
        },
        GraphExecutionResult::Cancelled { node, .. } => match captured {
            Some(stopped) if stopped.status == CheckpointStatus::Cancelled => {
                Ok(Step::Cancelled(Box::new(stopped)))
            }
            _ => Err(NodeError::Fatal(format!(
                "subgraph '{name}' was cancelled at '{node}' but its checkpoint was not captured"
            ))
            .into()),
        },
        GraphExecutionResult::Failed(error) => Err(child_failure(Some(name), error, captured)),
    }
}

/// What a child run that failed with `error` becomes on the node that ran it:
/// a subgraph node (`subgraph` is its name, which the message starts with) or
/// a map item (`None`; the map names the item). `Fatal` if the failing child
/// node's error was fatal, or the failure isn't a node failure at all;
/// `Transient`, with the retry hint the child's `failed` checkpoint recorded,
/// if the child's own recovery ran out on a transient error. It carries that
/// checkpoint (`captured`, when it is the child's `failed` one) for a subgraph
/// node to nest.
pub(crate) fn child_failure(
    subgraph: Option<&str>,
    error: StrataError,
    captured: Option<Checkpoint>,
) -> AttemptError {
    let say = |what: String| match subgraph {
        Some(name) => format!("subgraph '{name}' {what}"),
        None => what,
    };
    // The child persisted a `Failed` checkpoint of its own; keep it, so a
    // resume can re-enter the child at the node that failed.
    let failed = captured.filter(|checkpoint| checkpoint.status == CheckpointStatus::Failed);
    match error {
        StrataError::NodeFailed {
            node,
            attempts,
            message,
        } => {
            let metadata = failed
                .as_ref()
                .and_then(|checkpoint| checkpoint.metadata.as_ref());
            let transient = metadata
                .and_then(|metadata| metadata.get("error_kind"))
                .is_some_and(|kind| kind == "transient");
            let message = say(format!(
                "failed at '{node}' after {attempts} attempt(s): {message}"
            ));
            AttemptError {
                error: match (transient, stored_retry_after(metadata)) {
                    (true, Some(hint)) => NodeError::TransientAfter(message, hint),
                    (true, None) => NodeError::Transient(message),
                    (false, _) => NodeError::Fatal(message),
                },
                child: failed.map(Box::new),
            }
        }
        // A fail-safe, not a transient condition: no retry. Keep the child's
        // hop-limit checkpoint, as for any failed child, so a resume re-enters
        // it there.
        error @ StrataError::HopLimitExceeded { .. } => AttemptError {
            error: NodeError::Fatal(say(format!(
                "hit its hop limit: {}",
                crate::observability::error_message(&error)
            ))),
            child: failed.map(Box::new),
        },
        StrataError::NoMatchingEdge(node) => {
            let message = format!("no outgoing edge condition matched for node '{node}'");
            NodeError::Fatal(match subgraph {
                Some(name) => format!("subgraph '{name}': {message}"),
                None => message,
            })
            .into()
        }
        other => NodeError::Fatal(say(format!("failed: {other:?}"))).into(),
    }
}
