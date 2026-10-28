//! Token streaming (STRATA-SPEC-001).
//!
//! Decouples the graph state pipeline from output delivery: a node writes
//! chunks to a bounded `tokio::sync::mpsc` channel, and a consumer reads
//! them independently. The channel is bounded at
//! [`DEFAULT_CHANNEL_CAPACITY`] (1024) chunks; once full, `emit_token`'s
//! `.await` (or `emit_token_blocking`'s blocking send) pauses the node
//! rather than dropping chunks — this is ordinary bounded-`mpsc`
//! backpressure, not a custom mechanism.
//!
//! `graph::StateGraph::add_streaming_node` / `add_async_streaming_node` and
//! `CompiledGraph::run_streaming` are what wire this primitive into actual
//! node execution. A sync streaming handler runs on tokio's blocking pool and
//! calls the `_blocking` emit methods here; an async one (and any async node,
//! through `NodeContext::stream`) `.await`s the async ones.

use std::sync::{Arc, RwLock};

use tokio::sync::mpsc;

use crate::StrataError;

pub const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamChunkType {
    Token,
    NodeStart,
    NodeEnd,
    Error,
    /// A fallible node's attempt failed transiently and is about to run again;
    /// the content is the failed attempt's message. Anything the node streamed
    /// during that attempt (a parallel group's branches included) will be
    /// streamed again, so a consumer that has been showing those tokens should
    /// discard them.
    Retry,
}

#[derive(Debug, Clone)]
pub struct StreamChunk {
    pub chunk_type: StreamChunkType,
    pub node_name: Arc<str>,
    pub content: Arc<str>,
}

/// Handed to a running node so it can push output chunks without stalling
/// on the graph's own state mutations.
#[derive(Clone)]
pub struct StreamContext {
    // Shared by every context derived from one channel, so the run can close
    // the channel when it ends (see `Self::close`) even while a handler it gave
    // up on still holds a clone of its context.
    sender: Arc<RwLock<Option<mpsc::Sender<StreamChunk>>>>,
    current_node: Arc<str>,
    // Set on the context a subgraph node hands its child (see `Self::scoped`):
    // the qualified name of that node, prefixed to every node name the child
    // derives with `for_node`.
    scope: Option<Arc<str>>,
}

impl StreamContext {
    /// Creates a context/receiver pair bounded at [`DEFAULT_CHANNEL_CAPACITY`].
    pub fn channel(node_name: impl Into<Arc<str>>) -> (Self, mpsc::Receiver<StreamChunk>) {
        Self::channel_with_capacity(node_name, DEFAULT_CHANNEL_CAPACITY)
    }

    pub fn channel_with_capacity(
        node_name: impl Into<Arc<str>>,
        capacity: usize,
    ) -> (Self, mpsc::Receiver<StreamChunk>) {
        let (sender, receiver) = mpsc::channel(capacity);
        (
            Self {
                sender: Arc::new(RwLock::new(Some(sender))),
                current_node: node_name.into(),
                scope: None,
            },
            receiver,
        )
    }

    /// Returns a clone of this context retargeted at `node_name`, sharing
    /// the same underlying channel. This is how the engine (see
    /// `checkpoint::CompiledGraph::execute_from`) gives every node its own
    /// `StreamContext` — with the right `node_name` on each emitted chunk —
    /// out of a single channel created once per run.
    ///
    /// Inside a subgraph (see [`Self::scoped`]) the name is qualified with the
    /// subgraph node's own: `"<subgraph>/<node_name>"`.
    pub fn for_node(&self, node_name: impl Into<Arc<str>>) -> Self {
        let node_name = node_name.into();
        let current_node = match &self.scope {
            Some(scope) => Arc::from(format!("{scope}/{node_name}")),
            None => node_name,
        };
        Self {
            sender: self.sender.clone(),
            current_node,
            scope: self.scope.clone(),
        }
    }

    /// The context a subgraph node hands its child graph: same channel, but
    /// every node the child derives with [`Self::for_node`] is named under this
    /// context's node (`"Research/Draft"`, then `"Research/Deep/Draft"` for a
    /// grandchild), so the child's chunks say which subgraph they came from.
    pub(crate) fn scoped(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            current_node: self.current_node.clone(),
            scope: Some(self.current_node.clone()),
        }
    }

    pub async fn emit_token(&self, token: &str) -> Result<(), StrataError> {
        self.emit(StreamChunkType::Token, token).await
    }

    pub async fn emit_diagnostic(
        &self,
        chunk_type: StreamChunkType,
        message: &str,
    ) -> Result<(), StrataError> {
        self.emit(chunk_type, message).await
    }

    async fn emit(&self, chunk_type: StreamChunkType, content: &str) -> Result<(), StrataError> {
        let chunk = StreamChunk {
            chunk_type,
            node_name: self.current_node.clone(),
            content: Arc::from(content),
        };
        let Some(sender) = self.sender() else {
            return Err(StrataError::ChannelClosed);
        };
        sender
            .send(chunk)
            .await
            .map_err(|_| StrataError::ChannelClosed)
    }

    /// Synchronous counterpart to [`Self::emit_token`], for use from inside
    /// a streaming node's synchronous handler. Must be called from a thread
    /// that isn't driving async tasks — the blocking pool a sync handler runs
    /// on, or a plain OS thread — never from a tokio runtime worker thread:
    /// `mpsc::Sender::blocking_send` panics if it detects one, to prevent
    /// thread-pool starvation. An async handler uses [`Self::emit_token`].
    pub fn emit_token_blocking(&self, token: &str) -> Result<(), StrataError> {
        self.emit_blocking(StreamChunkType::Token, token)
    }

    pub fn emit_diagnostic_blocking(
        &self,
        chunk_type: StreamChunkType,
        message: &str,
    ) -> Result<(), StrataError> {
        self.emit_blocking(chunk_type, message)
    }

    fn emit_blocking(&self, chunk_type: StreamChunkType, content: &str) -> Result<(), StrataError> {
        let chunk = StreamChunk {
            chunk_type,
            node_name: self.current_node.clone(),
            content: Arc::from(content),
        };
        let Some(sender) = self.sender() else {
            return Err(StrataError::ChannelClosed);
        };
        sender
            .blocking_send(chunk)
            .map_err(|_| StrataError::ChannelClosed)
    }

    /// The live sender, cloned out of the shared slot so no lock is held while
    /// sending. `None` once the channel has been closed.
    fn sender(&self) -> Option<mpsc::Sender<StreamChunk>> {
        self.sender
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Closes the channel for every context derived from it: later emits fail
    /// with [`StrataError::ChannelClosed`], and the receiver ends once the
    /// chunks already sent are read. The engine calls this when a streaming run
    /// ends, so the stream ends with the run even if a handler it abandoned
    /// (`design/17-Async-Execution.md`) is still running.
    pub(crate) fn close(&self) {
        self.sender
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};
    use tokio::time::sleep;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn default_channel_capacity_is_1024() {
        let (ctx, _rx) = StreamContext::channel("Node");
        assert_eq!(ctx.sender().unwrap().capacity(), DEFAULT_CHANNEL_CAPACITY);
    }

    /// Runs `total` `emit_token` calls against a channel bounded at
    /// `capacity`, only starting to drain after `drain_delay`. Returns the
    /// tokens received (in order) and how long the producer took overall.
    async fn run_backpressure_case(
        capacity: usize,
        total: usize,
        drain_delay: Duration,
    ) -> (Vec<String>, Duration) {
        let (ctx, mut rx) = StreamContext::channel_with_capacity("StreamLLMResponse", capacity);

        let start = Instant::now();
        let producer = tokio::spawn(async move {
            for i in 0..total {
                ctx.emit_token(&i.to_string()).await.unwrap();
            }
        });

        sleep(drain_delay).await;

        let mut received = Vec::with_capacity(total);
        while received.len() < total {
            match rx.recv().await {
                Some(chunk) => received.push(chunk.content.to_string()),
                None => break,
            }
        }

        producer.await.unwrap();
        (received, start.elapsed())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn backpressure_blocks_producer_without_dropping_chunks() {
        let total = 20;
        let delay = Duration::from_millis(50);
        let (received, elapsed) = run_backpressure_case(4, total, delay).await;

        let expected: Vec<String> = (0..total).map(|i| i.to_string()).collect();
        assert_eq!(
            received, expected,
            "backpressure must not drop or reorder chunks"
        );

        // Capacity 4 with 20 sends and a receiver that doesn't start
        // draining until `delay` has passed: the producer necessarily
        // blocks on the 5th send until the receiver starts reading, so
        // completion cannot happen before `delay` elapses.
        assert!(
            elapsed >= delay,
            "producer finished in {elapsed:?}, before the consumer even started draining ({delay:?}) \u{2014} backpressure did not pause it"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn default_capacity_handles_overflow_without_dropping_chunks() {
        // Exercises the real production bound (1024), not just a small
        // test capacity, proving the "1024 chunks" bound end-to-end.
        let total = DEFAULT_CHANNEL_CAPACITY + 50;
        let (received, _elapsed) =
            run_backpressure_case(DEFAULT_CHANNEL_CAPACITY, total, Duration::from_millis(20)).await;

        let expected: Vec<String> = (0..total).map(|i| i.to_string()).collect();
        assert_eq!(received, expected);
    }
}
