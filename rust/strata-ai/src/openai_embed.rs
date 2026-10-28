//! An OpenAI-compatible embeddings client (`design/45-Long-Term-Memory.md`):
//! `POST /embeddings` on OpenAI itself, Azure OpenAI's compatible endpoint,
//! Ollama, vLLM, and any other server that speaks it. Anthropic has no
//! embeddings endpoint, so there is no Anthropic embedder.

use datalium_strata::BoxFuture;
use datalium_strata::observability::ModelUsage;
use serde_json::{Map, Value, json};

use crate::client::ChatError;
use crate::embed::{Embedder, EmbeddingRequest, EmbeddingResponse};
use crate::http::{http_client, json, send};
use crate::openai::{OpenAiOptions, post};

/// An embeddings client over [`OpenAiOptions`]: the chat client's base URL,
/// key, headers, timeouts and provider name, and a model of its own (the
/// options' `model` is the chat model, so it isn't used). Asks for
/// `encoding_format` `float`, and sends `dimensions` only when a request
/// asks. Its `Debug` never prints the key.
#[derive(Debug, Clone)]
pub struct OpenAiEmbedder {
    options: OpenAiOptions,
    model: String,
    http: reqwest::Client,
}

impl OpenAiEmbedder {
    /// An embedder of `model` (`text-embedding-3-small`, `nomic-embed-text`, …)
    /// on the server `options` name.
    ///
    /// # Panics
    ///
    /// When the HTTP client can't be built (no TLS backend), which is a build
    /// problem, not a runtime one.
    pub fn new(options: OpenAiOptions, model: impl Into<String>) -> Self {
        let http = http_client(options.connect_timeout, options.read_timeout)
            .expect("the HTTP client builds");
        Self {
            options,
            model: model.into(),
            http,
        }
    }

    pub fn options(&self) -> &OpenAiOptions {
        &self.options
    }

    /// The model asked for when a request names none.
    pub fn model(&self) -> &str {
        &self.model
    }

    /// The request body this embedder sends for `request` (public for tests
    /// and for anyone checking what goes over the wire).
    pub fn body(&self, request: &EmbeddingRequest) -> Value {
        let mut body = Map::new();
        body.insert(
            "model".into(),
            Value::String(request.model.clone().unwrap_or_else(|| self.model.clone())),
        );
        body.insert("input".into(), json!(request.inputs));
        body.insert("encoding_format".into(), json!("float"));
        if let Some(dimensions) = request.dimensions {
            body.insert("dimensions".into(), json!(dimensions));
        }
        Value::Object(body)
    }
}

impl Embedder for OpenAiEmbedder {
    fn provider(&self) -> &str {
        &self.options.provider
    }

    fn default_model(&self) -> Option<&str> {
        Some(&self.model)
    }

    fn embed(
        &self,
        request: EmbeddingRequest,
    ) -> BoxFuture<'_, Result<EmbeddingResponse, ChatError>> {
        Box::pin(async move {
            let body = self.body(&request);
            let response = send(post(&self.http, &self.options, "embeddings", &body)).await?;
            parse_response(&json(response).await?)
        })
    }
}

fn invalid(message: impl Into<String>) -> ChatError {
    ChatError::fatal("invalid_response", message)
}

/// The answer's vectors in `index` order (a server may answer out of order),
/// its model and its input tokens.
fn parse_response(value: &Value) -> Result<EmbeddingResponse, ChatError> {
    let data = value
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("the answer has no data"))?;
    let mut vectors: Vec<Option<Vec<f32>>> = vec![None; data.len()];
    for item in data {
        let index = item
            .get("index")
            .and_then(Value::as_u64)
            .ok_or_else(|| invalid("an embedding in the answer has no index"))?;
        let slot = usize::try_from(index)
            .ok()
            .and_then(|index| vectors.get_mut(index))
            .ok_or_else(|| {
                invalid(format!(
                    "the answer has an embedding at index {index}, out of {}",
                    data.len()
                ))
            })?;
        if slot.is_some() {
            return Err(invalid(format!(
                "the answer has two embeddings at index {index}"
            )));
        }
        let vector = item
            .get("embedding")
            .and_then(Value::as_array)
            .and_then(|numbers| {
                numbers
                    .iter()
                    .map(|number| number.as_f64().map(|number| number as f32))
                    .collect::<Option<Vec<f32>>>()
            })
            .ok_or_else(|| {
                invalid(format!(
                    "the embedding at index {index} isn't an array of numbers"
                ))
            })?;
        *slot = Some(vector);
    }
    // Every slot is filled: as many items as slots, none twice, none out of
    // range.
    let vectors = vectors.into_iter().flatten().collect();
    let usage = ModelUsage::new(
        value
            .pointer("/usage/prompt_tokens")
            .and_then(Value::as_u64),
        None,
    );
    Ok(EmbeddingResponse {
        vectors,
        model: value
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_string),
        usage,
    })
}
