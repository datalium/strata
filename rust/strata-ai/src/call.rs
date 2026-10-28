//! A model call from a node (`design/44-LLM-Integration.md`): streamed when
//! the run streams, observed either way, failed the way a node fails.

use datalium_strata::context::NodeContext;
use datalium_strata::graph::NodeError;
use datalium_strata::observability::{ModelCallInfo, ModelCallScope};
use futures_util::StreamExt;

use crate::client::{ChatClient, ChatError};
use crate::collect::ChatUpdates;
use crate::types::{ChatRequest, ChatResponse, ChatUpdate};

/// What [`complete_with`] needs besides the request itself.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct CallOptions {
    /// The agent making the call, for the span's `strata.agent`.
    pub agent: Option<String>,
}

impl CallOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_agent(mut self, agent: impl Into<String>) -> Self {
        self.agent = Some(agent.into());
        self
    }
}

/// Asks `client` for an answer to `request` from inside a node: see
/// [`complete_with`].
pub async fn complete(
    client: &dyn ChatClient,
    request: ChatRequest,
    ctx: &NodeContext,
) -> Result<ChatResponse, NodeError> {
    complete_with(client, request, Some(ctx), &CallOptions::default()).await
}

/// Asks `client` for an answer to `request`. Mirrors .NET's
/// `StrataChat.GetResponseAsync`.
///
/// - When `ctx` has a stream (the run is streaming), the call streams and
///   each update's text goes to it as a `Token` chunk; otherwise it is one
///   request. Either way the whole answer comes back.
/// - It is recorded through a [`ModelCallScope`]: `ModelCallStarted` and
///   `ModelCallCompleted` with the provider's usage, a `chat <model>` span and
///   the GenAI metrics. The model is the request's, else the client's
///   default, else `unknown` (a blank one counts as none). Without `ctx` it
///   records no run event.
/// - A fault returns a [`NodeError`]: transient with the provider's retry
///   hint, or fatal. A stream that breaks after its first update is
///   transient. When the node's token fires, the engine drops the call's
///   future, which records it cancelled.
/// - A [`ModelRoute`](crate::route::ModelRoute) (`design/46-Model-Routes.md`)
///   is not one model call: each of its attempts is recorded as one, with the
///   route's name, under a `route <name>` span, and the call ends with one
///   `ModelRouted`. Only the winning attempt streams to the node's stream.
pub async fn complete_with(
    client: &dyn ChatClient,
    request: ChatRequest,
    ctx: Option<&NodeContext>,
    options: &CallOptions,
) -> Result<ChatResponse, NodeError> {
    if let Some(route) = client.as_route() {
        return route
            .call(request, ctx, options)
            .await
            .map_err(NodeError::from);
    }
    let model = [request.model.as_deref(), client.default_model()]
        .into_iter()
        .flatten()
        .find(|model| !model.trim().is_empty())
        .unwrap_or("unknown")
        .to_string();
    let mut info = ModelCallInfo::new(model).with_provider(client.provider());
    if let Some(max_tokens) = request.max_tokens {
        info = info.with_max_tokens(max_tokens);
    }
    if let Some(temperature) = request.temperature {
        info = info.with_temperature(temperature);
    }
    if let Some(agent) = &options.agent {
        info = info.with_agent(agent.clone());
    }
    let scope = ModelCallScope::start(ctx, info);
    // No race against the node's token here: when it fires the engine drops
    // this future, and dropping the scope records the call cancelled.
    match call(client, request, ctx, &scope).await {
        Ok(response) => {
            scope.succeeded(
                response.usage,
                response.finish_reason.as_deref(),
                response.model.as_deref(),
                response.id.as_deref(),
            );
            Ok(response)
        }
        Err(error) => {
            scope.failed(&error.error_type, &error.message);
            Err(error.into())
        }
    }
}

async fn call(
    client: &dyn ChatClient,
    request: ChatRequest,
    ctx: Option<&NodeContext>,
    scope: &ModelCallScope,
) -> Result<ChatResponse, ChatError> {
    let Some(stream_to) = ctx.and_then(NodeContext::stream) else {
        return client.complete(request).await;
    };
    let mut stream = client.stream(request).await?;
    let mut updates = ChatUpdates::new();
    let mut started = false;
    while let Some(update) = stream.next().await {
        let update = match update {
            Ok(update) => update,
            // Some of the answer has been streamed: whatever broke it, a new
            // attempt is what can finish it.
            Err(error) if started => {
                return Err(ChatError {
                    error_type: "stream_broken".to_string(),
                    ..ChatError::transient("stream_broken", error.message)
                }
                .with_hint(error.retry_after));
            }
            Err(error) => return Err(error),
        };
        if !started {
            started = true;
            scope.first_chunk();
        }
        if let ChatUpdate::Text(text) = &update
            && !text.is_empty()
            && let Err(error) = stream_to.emit_token(text).await
        {
            return Err(ChatError::transient("stream_closed", format!("{error:?}")));
        }
        updates.push(update);
    }
    updates.finish()
}

impl ChatError {
    fn with_hint(mut self, retry_after: Option<std::time::Duration>) -> Self {
        self.retry_after = retry_after;
        self
    }
}
