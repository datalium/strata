//! The model node (`design/44-LLM-Integration.md`): one model call as a node
//! of a graph.

use std::collections::BTreeMap;
use std::sync::Arc;

use datalium_strata::context::NodeContext;
use datalium_strata::graph::StateGraph;

use datalium_strata::tools::StrataSchema;
use serde::de::DeserializeOwned;

use crate::agent::{Agent, AgentResult};
use crate::call::{CallOptions, complete_with};
use crate::client::ChatClient;
use crate::types::{ChatRequest, ChatResponse, Message};

/// A model node's options ([`StateGraphAiExt::add_model_node`]).
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct ModelNodeOptions {
    /// The model; the client's default when `None`.
    pub model: Option<String>,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f64>,
    /// The node's description-only metadata (`design/22`).
    pub metadata: BTreeMap<String, String>,
}

impl ModelNodeOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    pub fn with_max_tokens(mut self, max_tokens: u32) -> Self {
        self.max_tokens = Some(max_tokens);
        self
    }

    pub fn with_temperature(mut self, temperature: f64) -> Self {
        self.temperature = Some(temperature);
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// Model and agent nodes on a [`StateGraph`]. Mirrors .NET's
/// `ModelNodes.AddModelNode` and `AgentNodes.AddAgentNode`.
pub trait StateGraphAiExt<S>: Sized {
    /// Adds a fallible async node that builds messages from the state
    /// (`prompt`), makes one call through [`complete_with`], and folds the
    /// answer into the state (`apply`). A transient fault fails the attempt
    /// transiently, so a `RecoveryPolicy` with a backoff retries it after the
    /// provider's hint (`design/25`).
    fn add_model_node<P, A>(
        self,
        name: impl Into<String>,
        client: Arc<dyn ChatClient>,
        prompt: P,
        apply: A,
        options: ModelNodeOptions,
    ) -> Self
    where
        P: Fn(&S, &NodeContext) -> Vec<Message> + Send + Sync + 'static,
        A: Fn(S, ChatResponse) -> S + Send + Sync + 'static;

    /// Adds a fallible async node that runs `agent` on the messages `prompt`
    /// builds from the state, and folds its result into the state (`apply`).
    /// Sugar over [`Agent::run`], which a node can call itself; the agent is
    /// not a node kind, and the node exports like any.
    fn add_agent_node<O, P, A>(
        self,
        name: impl Into<String>,
        agent: Arc<Agent<O>>,
        prompt: P,
        apply: A,
    ) -> Self
    where
        O: DeserializeOwned + StrataSchema + Send + 'static,
        P: Fn(&S, &NodeContext) -> Vec<Message> + Send + Sync + 'static,
        A: Fn(S, AgentResult<O>) -> S + Send + Sync + 'static;
}

impl<S> StateGraphAiExt<S> for StateGraph<S>
where
    S: Clone + Send + Sync + 'static,
{
    fn add_model_node<P, A>(
        self,
        name: impl Into<String>,
        client: Arc<dyn ChatClient>,
        prompt: P,
        apply: A,
        options: ModelNodeOptions,
    ) -> Self
    where
        P: Fn(&S, &NodeContext) -> Vec<Message> + Send + Sync + 'static,
        A: Fn(S, ChatResponse) -> S + Send + Sync + 'static,
    {
        let name = name.into();
        let metadata = options.metadata.clone();
        let prompt = Arc::new(prompt);
        let apply = Arc::new(apply);
        let options = Arc::new(options);
        let graph = self.add_async_fallible_node(name.clone(), move |state: S, ctx| {
            let client = client.clone();
            let prompt = prompt.clone();
            let apply = apply.clone();
            let options = options.clone();
            async move {
                let mut request = ChatRequest::new(prompt(&state, &ctx));
                request.model = options.model.clone();
                request.max_tokens = options.max_tokens;
                request.temperature = options.temperature;
                let response =
                    complete_with(&*client, request, Some(&ctx), &CallOptions::default()).await?;
                Ok(apply(state, response))
            }
        });
        if metadata.is_empty() {
            graph
        } else {
            graph.set_node_metadata(name, metadata)
        }
    }

    fn add_agent_node<O, P, A>(
        self,
        name: impl Into<String>,
        agent: Arc<Agent<O>>,
        prompt: P,
        apply: A,
    ) -> Self
    where
        O: DeserializeOwned + StrataSchema + Send + 'static,
        P: Fn(&S, &NodeContext) -> Vec<Message> + Send + Sync + 'static,
        A: Fn(S, AgentResult<O>) -> S + Send + Sync + 'static,
    {
        let prompt = Arc::new(prompt);
        let apply = Arc::new(apply);
        self.add_async_fallible_node(name, move |state: S, ctx| {
            let agent = agent.clone();
            let prompt = prompt.clone();
            let apply = apply.clone();
            async move {
                let result = agent.run(prompt(&state, &ctx), &ctx).await?;
                Ok(apply(state, result))
            }
        })
    }
}
