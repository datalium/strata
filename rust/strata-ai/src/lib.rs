//! Model calls for Datalium Strata (`design/44-LLM-Integration.md`, SG-23).
//!
//! - [`ChatClient`]: Strata's own chat-client trait, the Rust counterpart of
//!   .NET's `IChatClient`, with [`OpenAiChatClient`] (Chat Completions: OpenAI
//!   and every compatible server, Ollama and vLLM included) and
//!   [`AnthropicChatClient`] (Messages), both streaming. Neither retries: a
//!   transient [`ChatError`] carries the provider's retry hint, and the node's
//!   `RecoveryPolicy` retries.
//! - [`complete`] / [`complete_with`]: a model call from a node, streamed
//!   when the run streams, observed through the engine's `ModelCallScope`
//!   (`ModelCallStarted` / `ModelCallCompleted`, a `chat <model>` span, the
//!   GenAI metrics), and failed as a [`NodeError`](datalium_strata::graph::NodeError).
//! - [`StateGraphAiExt::add_model_node`]: one model call as a node.
//! - [`Agent`]: the model ↔ tool loop through a `ToolRegistry`, with
//!   provider-safe tool names ([`ToolDeclarations`]) and a typed answer
//!   (`#[derive(Deserialize, StrataSchema)]`) parsed, validated and repaired;
//!   [`StateGraphAiExt::add_agent_node`] runs one as a node.
//! - [`ModelRoute`] (`design/46-Model-Routes.md`): a chat client over ordered
//!   candidates that fails over and hedges, with health per provider
//!   ([`ModelHealth`]); each attempt is observed as a model call of its own.
//! - [`Embedder`] and [`embed`] / [`embed_with`] (`design/45-Long-Term-Memory.md`):
//!   an embedding call observed as a model call of operation `embeddings`,
//!   with [`OpenAiEmbedder`] (`/embeddings`: OpenAI, Azure, Ollama, vLLM).
//! - [`Memory`]: a memory store and an embedder together, so a node
//!   remembers ([`MemoryEntry`]) and recalls ([`RecallOptions`]) by text,
//!   labelled `<provider>/<model>` and in the run's tenant.

mod agent;
mod call;
mod client;
mod collect;
mod declarations;
mod embed;
mod faults;
mod http;
mod memory;
mod node;
mod types;

#[cfg(feature = "anthropic")]
mod anthropic;
#[cfg(feature = "openai")]
mod openai;
#[cfg(feature = "openai")]
mod openai_embed;
pub mod route;

pub use agent::{
    Agent, AgentOptions, AgentResult, AgentToolCall, AgentTurn, ConfigureTurn, ToolFailures,
    ToolFaultClassifier, Validate,
};
#[cfg(feature = "anthropic")]
pub use anthropic::{ANTHROPIC_VERSION, AnthropicChatClient, AnthropicOptions};
pub use call::{CallOptions, complete, complete_with};
pub use client::{ChatClient, ChatError, ChatErrorKind, ChatStream};
pub use collect::ChatUpdates;
pub use datalium_strata::observability::{ModelUsage, ToolCallResult};
pub use declarations::ToolDeclarations;
pub use embed::{Embedder, EmbeddingRequest, EmbeddingResponse, embed, embed_with};
pub use faults::{is_transient_status, parse_retry_after};
pub use memory::{Memory, MemoryEntry, MemoryError, RecallOptions};
pub use node::{ModelNodeOptions, StateGraphAiExt};
#[cfg(feature = "openai")]
pub use openai::{MaxTokensField, OpenAiChatClient, OpenAiOptions};
#[cfg(feature = "openai")]
pub use openai_embed::OpenAiEmbedder;
pub use route::{
    ModelCandidate, ModelHealth, ModelHealthEntry, ModelHealthOptions, ModelHealthState,
    ModelRoute, ModelRouteAttempt, ModelRouteOutcome, ModelRouteResult,
};
pub use types::{
    ChatRequest, ChatResponse, ChatUpdate, Content, Message, ResponseFormat, Role, ToolChoice,
    ToolDeclaration,
};
