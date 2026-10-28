//! The chat-client trait and its error (`design/44-LLM-Integration.md`).

use std::fmt;
use std::pin::Pin;
use std::time::Duration;

use datalium_strata::BoxFuture;
use datalium_strata::graph::NodeError;
use futures_core::Stream;

use crate::types::{ChatRequest, ChatResponse, ChatUpdate};

/// A streamed answer: updates until the answer is whole, or an error.
pub type ChatStream = Pin<Box<dyn Stream<Item = Result<ChatUpdate, ChatError>> + Send>>;

/// A model provider (`design/44-LLM-Integration.md`): Strata's own trait, the
/// Rust counterpart of .NET's `IChatClient`. [`OpenAiChatClient`] and
/// [`AnthropicChatClient`] implement it, and so can anything else; the tests
/// use a scripted one. Object safe, so a graph holds an `Arc<dyn ChatClient>`.
///
/// A client does no retrying: a transient [`ChatError`] carries the
/// provider's retry hint, and the node's `RecoveryPolicy` retries.
///
/// [`OpenAiChatClient`]: crate::OpenAiChatClient
/// [`AnthropicChatClient`]: crate::AnthropicChatClient
pub trait ChatClient: Send + Sync {
    /// The provider's name (`openai`, `anthropic`, …): `gen_ai.provider.name`.
    fn provider(&self) -> &str;

    /// The model asked for when a request names none.
    fn default_model(&self) -> Option<&str> {
        None
    }

    /// One answer to `request`, whole.
    fn complete(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatResponse, ChatError>>;

    /// One answer to `request`, streamed. The future resolves when the
    /// provider has accepted the request (its status read), and the stream
    /// yields the answer as it comes.
    fn stream(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatStream, ChatError>>;

    /// The [`ModelRoute`](crate::route::ModelRoute) this client is, if it is
    /// one (`design/46-Model-Routes.md`): how [`complete_with`] finds a route
    /// behind a `&dyn ChatClient` and observes each of its attempts as a
    /// model call of its own. `None` for every other client.
    ///
    /// [`complete_with`]: crate::complete_with
    fn as_route(&self) -> Option<&crate::route::ModelRoute> {
        None
    }
}

/// Whether a model call's failure is worth retrying.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatErrorKind {
    /// A retry may succeed: 408, 409, 429, 5xx, a timeout, a lost connection,
    /// a broken stream.
    Transient,
    /// A retry would get the same answer: every other 4xx, an answer that
    /// can't be read.
    Fatal,
}

/// A model call's failure (`design/44-LLM-Integration.md`, "Faults").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatError {
    pub kind: ChatErrorKind,
    /// The HTTP status the provider answered, when it answered.
    pub status: Option<u16>,
    /// How long the provider asked to wait (`retry-after-ms`, `Retry-After`).
    pub retry_after: Option<Duration>,
    /// A low-cardinality class for `error.type`: the status (`429`),
    /// `timeout`, `connection`, `stream_broken`, `invalid_response`, ….
    pub error_type: String,
    pub message: String,
}

impl ChatError {
    /// A provider answered `status`: transient or fatal as
    /// [`is_transient_status`](crate::is_transient_status) says.
    pub fn status(status: u16, retry_after: Option<Duration>, message: impl Into<String>) -> Self {
        Self {
            kind: if crate::faults::is_transient_status(status) {
                ChatErrorKind::Transient
            } else {
                ChatErrorKind::Fatal
            },
            status: Some(status),
            retry_after,
            error_type: status.to_string(),
            message: message.into(),
        }
    }

    pub fn transient(error_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: ChatErrorKind::Transient,
            status: None,
            retry_after: None,
            error_type: error_type.into(),
            message: message.into(),
        }
    }

    pub fn fatal(error_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: ChatErrorKind::Fatal,
            status: None,
            retry_after: None,
            error_type: error_type.into(),
            message: message.into(),
        }
    }

    pub fn with_retry_after(mut self, retry_after: Duration) -> Self {
        self.retry_after = Some(retry_after);
        self
    }

    pub fn is_transient(&self) -> bool {
        self.kind == ChatErrorKind::Transient
    }
}

impl fmt::Display for ChatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "model call failed ({}): {}",
            self.error_type, self.message
        )
    }
}

impl std::error::Error for ChatError {}

impl From<ChatError> for NodeError {
    /// Transient with the hint (`NodeError::TransientAfter`), or fatal, as
    /// design/25's retries read it.
    fn from(error: ChatError) -> Self {
        let message = error.to_string();
        match (error.kind, error.retry_after) {
            (ChatErrorKind::Transient, Some(after)) => NodeError::transient_after(message, after),
            (ChatErrorKind::Transient, None) => NodeError::Transient(message),
            (ChatErrorKind::Fatal, _) => NodeError::Fatal(message),
        }
    }
}
