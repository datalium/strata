use std::collections::HashMap;
use std::sync::Arc;

// Re-exported so macro-generated code (see `datalium_strata_macros`) can reference
// `::datalium_strata::serde_json::...` without requiring every crate that defines
// `#[strata_tool]` functions to also declare its own direct `serde_json` dependency.
pub use serde_json;

pub mod checkpoint;
pub mod context;
pub mod graph;
pub mod history;
pub mod lease;
pub mod map;
pub mod memory;
mod node;
pub mod observability;
pub mod parallel;
pub mod recovery;
pub mod scheduler;
pub mod schema;
pub mod store;
pub mod streaming;
pub mod subgraph;
pub mod tools;
pub mod validate;
pub mod wait;

use history::HistoryStore;

/// The boxed future every store method returns (`design/19-Async-Queryable-Store.md`).
pub use store::BoxFuture;

/// Re-exported so callers can cancel a run (`context::RunConfig::with_cancellation`)
/// without depending on `tokio-util` themselves.
pub use tokio_util::sync::CancellationToken;

/// Re-exports the pieces needed to define and register `#[strata_tool]` functions,
/// matching STRATA-SPEC-003's `use datalium_strata::prelude::*;` developer interface.
pub mod prelude {
    pub use crate::StrataError;
    pub use crate::tools::{StrataSchema, StrataTool, ToolCallContext, ToolRegistry};
    pub use datalium_strata_macros::{StrataSchema, strata_tool};
}

#[derive(Debug, Clone)]
pub enum StrataError {
    NodeNotFound(String),
    CompilationFailed(String),
    ExecutionAborted(String),
    ChannelClosed,
    /// `.compile()` rejected the graph. Carries every structural error found
    /// (missing entry point, orphan node, unguarded cycle) — see
    /// `validate::ValidationErrorKind`.
    GraphInvalid(validate::ValidationResult),
    /// `ToolRegistry::dispatch()` was asked to invoke a tool name that no
    /// `#[strata_tool]`-generated handle was registered under.
    ToolNotFound(String),
    /// Raw JSON tool-call arguments failed to deserialize into the target
    /// `#[derive(StrataSchema)]` argument struct. Holds what is wrong, as
    /// [`tools::argument_problems`] says it (`argument 'sku' is missing`), or
    /// serde's message when that explains nothing.
    ToolArgumentParseFailure(String),
    /// The underlying tool function returned `Err(_)`.
    ToolExecutionFailure(String),
    /// `ToolRegistry::try_register()` was given a tool whose name is already
    /// registered (`register()` panics instead). Carries the name.
    DuplicateTool(String),
    /// A `ToolRegistry` subset was asked for a tool its parent has but the
    /// subset left out (`design/20-Tool-Dispatch-And-Registry.md`). Carries the name.
    ToolNotPermitted(String),
    /// A tool call's token was cancelled before it started, or the tool
    /// failed after the token fired. Carries the tool name.
    ToolCancelled(String),
    /// A tool middleware refused the call (`design/23-Tool-Middleware.md`):
    /// the tool exists and the caller may use it, but a policy refused this
    /// call. Reported as a refused call with the error type `denied`.
    ToolCallDenied {
        tool: String,
        reason: String,
    },
    /// A tool middleware failed the call for a reason of its own: a timeout,
    /// an unreachable policy backend (`design/23-Tool-Middleware.md`).
    ToolCallFailed {
        tool: String,
        message: String,
    },
    /// A node had one or more outgoing conditional edges, but none of
    /// their predicates matched the state at runtime — a graph author's
    /// routing gap that `.compile()` can't catch since it depends on
    /// runtime state, not just structure. Carries the node name.
    NoMatchingEdge(String),
    /// A fallible node (see `graph::StateGraph::add_fallible_node`) failed
    /// and its recovery policy had nothing left to try: retries were
    /// exhausted (or the error was `Fatal`) and no fallback node was
    /// configured. A `CheckpointStatus::Failed` checkpoint holding the last
    /// committed state was persisted before this was returned.
    NodeFailed {
        node: String,
        /// Total attempts made, including the first (so `max_retries + 1` if
        /// every attempt was `Transient`, or `1` for an immediate `Fatal`).
        attempts: u32,
        message: String,
    },
    /// The loop circuit breaker tripped (`design/24-Loop-Circuit-Breaker.md`):
    /// the graph was about to start `node`, which would have been hop
    /// `hops + 1` with a limit of `limit`. The node never started. A
    /// `CheckpointStatus::Failed` checkpoint at `node`, with `error_kind`
    /// `hop_limit` and the last committed state, was persisted before this was
    /// returned; resuming it trips again unless the resume raises the limit
    /// (`RunConfig::with_max_node_hops`). No recovery policy sees it.
    HopLimitExceeded {
        node: String,
        /// The hops the level had made.
        hops: u32,
        /// The limit in effect.
        limit: u32,
    },
    /// The run's scheduler (`RunConfig::with_scheduler`) gave it no slot
    /// (`design/40-Run-Scheduler.md`): `reason` says why, after `waited`. The
    /// run never started: nothing was loaded or written and no run event or
    /// span was made, so the thread is as it was and the call can be made
    /// again. [`scheduler::NotAdmitted`]'s `Display` is its message.
    NotAdmitted {
        reason: scheduler::NotAdmittedReason,
        waited: std::time::Duration,
    },
}

/// Represents the global context configuration accessible by all running nodes.
///
/// Per `CLAUDE.md` item 6: "Graph nodes access history via `IExecutionContext`
/// handles, never by carrying full chat history in state payloads." Every
/// async node handler gets a [`context::NodeContext`], which carries the
/// history store passed in [`context::RunConfig::with_history`] and can hand
/// out one of these ([`context::NodeContext::execution_context`]) for code that
/// takes it (`design/17-Async-Execution.md`). Sync handlers get no context;
/// this remains a standalone handle a caller can construct themselves.
pub struct ExecutionContext {
    pub thread_id: String,
    pub metadata: HashMap<String, String>,
    history: Arc<dyn HistoryStore>,
}

impl ExecutionContext {
    pub fn new(thread_id: impl Into<String>, history: Arc<dyn HistoryStore>) -> Self {
        Self {
            thread_id: thread_id.into(),
            metadata: HashMap::new(),
            history,
        }
    }

    pub fn history(&self) -> &dyn HistoryStore {
        self.history.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::{ChatMessage, HistoryStoreError, MessageRole};

    /// A minimal `HistoryStore` test double — same reasoning as
    /// `checkpoint::tests::TestStore`: this crate can't depend on
    /// `strata-memory` (the dependency runs the other way), so a unit test
    /// of `ExecutionContext::history()` itself needs its own tiny stand-in.
    /// The real adapters are exercised by `strata-memory`'s contract suite.
    #[derive(Default)]
    struct TestHistoryStore;

    impl HistoryStore for TestHistoryStore {
        fn append<'a>(
            &'a self,
            _session_id: &'a str,
            _message: ChatMessage,
        ) -> BoxFuture<'a, Result<(), HistoryStoreError>> {
            Box::pin(async { Ok(()) })
        }

        fn get_history<'a>(
            &'a self,
            _session_id: &'a str,
            _limit: usize,
        ) -> BoxFuture<'a, Result<Vec<ChatMessage>, HistoryStoreError>> {
            Box::pin(async {
                Ok(vec![ChatMessage {
                    role: MessageRole::User,
                    content: Arc::from("from the context handle"),
                    timestamp: chrono::Utc::now(),
                }])
            })
        }
    }

    #[tokio::test]
    async fn execution_context_exposes_the_history_store_it_was_built_with() {
        let ctx = ExecutionContext::new("thread_1", Arc::new(TestHistoryStore));
        assert_eq!(ctx.thread_id, "thread_1");

        let history = ctx.history().get_history("thread_1", 10).await.unwrap();
        assert_eq!(history[0].content.as_ref(), "from the context handle");
    }
}
