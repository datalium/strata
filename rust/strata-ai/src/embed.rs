//! Embedding calls (`design/45-Long-Term-Memory.md`, "AI packages: embeddings
//! and the helper"): the [`Embedder`] trait, the counterpart of .NET's
//! `IEmbeddingGenerator<string, Embedding<float>>`, and [`embed`] /
//! [`embed_with`], one observed call from a node. Mirrors .NET's
//! `StrataEmbeddings.GenerateAsync`.

use datalium_strata::BoxFuture;
use datalium_strata::context::NodeContext;
use datalium_strata::graph::NodeError;
use datalium_strata::observability::{ModelCallInfo, ModelCallScope, ModelUsage};

use crate::call::CallOptions;
use crate::client::ChatError;

/// An embedding provider (`design/45-Long-Term-Memory.md`). Object safe, like
/// [`ChatClient`](crate::ChatClient), so a memory holds an
/// `Arc<dyn Embedder>`. [`OpenAiEmbedder`](crate::OpenAiEmbedder) implements
/// it, and so can anything else; the tests use a scripted one.
///
/// An embedder does no retrying: a transient [`ChatError`] carries the
/// provider's retry hint, and the node's `RecoveryPolicy` retries.
pub trait Embedder: Send + Sync {
    /// The provider's name (`openai`, `ollama`, …): `gen_ai.provider.name`,
    /// and the first half of the memory helper's label.
    fn provider(&self) -> &str;

    /// The model asked for when a request names none.
    fn default_model(&self) -> Option<&str> {
        None
    }

    /// One vector per input of `request`, in the inputs' order.
    fn embed(
        &self,
        request: EmbeddingRequest,
    ) -> BoxFuture<'_, Result<EmbeddingResponse, ChatError>>;
}

/// What to embed: the texts, in order, as one call.
#[derive(Debug, Clone, PartialEq, Default)]
#[non_exhaustive]
pub struct EmbeddingRequest {
    pub inputs: Vec<String>,
    /// The model; the embedder's default when `None`.
    pub model: Option<String>,
    /// The vectors' length, for a model that can shorten them (OpenAI's
    /// `text-embedding-3-*`); the model's own when `None`.
    pub dimensions: Option<u32>,
}

impl EmbeddingRequest {
    pub fn new(inputs: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            inputs: inputs.into_iter().map(Into::into).collect(),
            model: None,
            dimensions: None,
        }
    }

    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    pub fn with_dimensions(mut self, dimensions: u32) -> Self {
        self.dimensions = Some(dimensions);
        self
    }
}

/// An embedder's answer: one vector per input, in the inputs' order.
#[derive(Debug, Clone, PartialEq, Default)]
#[non_exhaustive]
pub struct EmbeddingResponse {
    pub vectors: Vec<Vec<f32>>,
    /// The model that answered, when the provider says.
    pub model: Option<String>,
    /// As the provider reported it; an embedding has input tokens only.
    pub usage: ModelUsage,
}

impl EmbeddingResponse {
    pub fn new(vectors: Vec<Vec<f32>>) -> Self {
        Self {
            vectors,
            model: None,
            usage: ModelUsage::default(),
        }
    }

    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    pub fn with_usage(mut self, usage: ModelUsage) -> Self {
        self.usage = usage;
        self
    }
}

/// Embeds `request` from inside a node: see [`embed_with`].
pub async fn embed(
    embedder: &dyn Embedder,
    request: EmbeddingRequest,
    ctx: &NodeContext,
) -> Result<EmbeddingResponse, NodeError> {
    embed_with(embedder, request, Some(ctx), &CallOptions::default()).await
}

/// Embeds `request` as one call (`design/45-Long-Term-Memory.md`). Mirrors
/// .NET's `StrataEmbeddings.GenerateAsync`.
///
/// - A request with no inputs returns an empty response at once: no call and
///   nothing recorded.
/// - It is recorded through a [`ModelCallScope`] with operation `embeddings`:
///   `ModelCallStarted` and `ModelCallCompleted` (operation `embeddings`, the
///   input tokens only), an `embeddings <model>` span and the GenAI metrics.
///   The model is the request's, else the embedder's default, else
///   `unknown` (a blank one counts as none). Without `ctx` it records no run event. It never streams.
/// - An answer with another number of vectors than inputs is a fatal
///   `invalid_response`.
/// - A fault returns a [`NodeError`]: transient with the provider's retry
///   hint, or fatal, as [`complete_with`](crate::complete_with)'s do. When the
///   node's token fires, the engine drops the call's future, which records it
///   cancelled.
pub async fn embed_with(
    embedder: &dyn Embedder,
    request: EmbeddingRequest,
    ctx: Option<&NodeContext>,
    options: &CallOptions,
) -> Result<EmbeddingResponse, NodeError> {
    if request.inputs.is_empty() {
        return Ok(EmbeddingResponse::default());
    }
    let model = [request.model.as_deref(), embedder.default_model()]
        .into_iter()
        .flatten()
        .find(|model| !model.trim().is_empty())
        .unwrap_or("unknown")
        .to_string();
    let mut info = ModelCallInfo::new(model)
        .with_provider(embedder.provider())
        .with_operation("embeddings");
    if let Some(agent) = &options.agent {
        info = info.with_agent(agent.clone());
    }
    let scope = ModelCallScope::start(ctx, info);
    let expected = request.inputs.len();
    // No race against the node's token here: when it fires the engine drops
    // this future, and dropping the scope records the call cancelled.
    let answer = embedder.embed(request).await.and_then(|response| {
        if response.vectors.len() == expected {
            Ok(response)
        } else {
            Err(ChatError::fatal(
                "invalid_response",
                format!(
                    "expected {expected} vectors, the embedder returned {}",
                    response.vectors.len()
                ),
            ))
        }
    });
    match answer {
        Ok(response) => {
            scope.succeeded(
                ModelUsage::new(response.usage.input_tokens, None),
                None,
                response.model.as_deref(),
                None,
            );
            Ok(response)
        }
        Err(error) => {
            scope.failed(&error.error_type, &error.message);
            Err(error.into())
        }
    }
}
