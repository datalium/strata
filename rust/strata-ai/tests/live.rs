//! Tests against live providers (`design/44-LLM-Integration.md`, "Tests"),
//! skipped unless their variables are set, and not part of CI:
//!
//! - OpenAI-compatible: `STRATA_TEST_OPENAI_MODEL`, and `STRATA_TEST_OPENAI_KEY`
//!   (OpenAI's API) or `STRATA_TEST_OPENAI_URL` (any compatible server up to
//!   `/v1`, Ollama's `http://localhost:11434/v1` say; the key is optional there).
//! - Anthropic: `STRATA_TEST_ANTHROPIC_KEY` and `STRATA_TEST_ANTHROPIC_MODEL`.
//! - OpenAI-compatible embeddings (`design/45-Long-Term-Memory.md`):
//!   `STRATA_TEST_OPENAI_EMBEDDING_MODEL`, with the same key or URL.
//!
//! Each provider answers one plain call, streams one through a graph run, and
//! runs one agent: a tool loop ending in a typed answer. The embedder embeds
//! two texts, and a `Memory` over an in-memory store remembers and recalls.
//! Mirrors the .NET `ModelLiveTests`.

use std::sync::Arc;

use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::context::RunConfig;
use datalium_strata::graph::StateGraph;
use datalium_strata::memory::InMemoryMemoryStore;
use datalium_strata::observability::{ModelCallResult, RunEventChannel, RunEventKind};
use datalium_strata::prelude::*;
use datalium_strata::streaming::StreamChunkType;
use datalium_strata_ai::{
    Agent, AgentOptions, AnthropicChatClient, AnthropicOptions, ChatClient, ChatRequest, Embedder,
    EmbeddingRequest, Memory, MemoryEntry, Message, ModelNodeOptions, OpenAiChatClient,
    OpenAiEmbedder, OpenAiOptions, RecallOptions, StateGraphAiExt, complete_with, embed_with,
};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::Deserialize;

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn openai() -> Option<Arc<dyn ChatClient>> {
    let model = var("STRATA_TEST_OPENAI_MODEL")?;
    Some(Arc::new(OpenAiChatClient::new(
        openai_options()?.with_model(model),
    )))
}

fn openai_embedder() -> Option<Arc<OpenAiEmbedder>> {
    let model = var("STRATA_TEST_OPENAI_EMBEDDING_MODEL")?;
    Some(Arc::new(OpenAiEmbedder::new(openai_options()?, model)))
}

/// The OpenAI-compatible server's options, without a model.
fn openai_options() -> Option<OpenAiOptions> {
    Some(
        match (var("STRATA_TEST_OPENAI_URL"), var("STRATA_TEST_OPENAI_KEY")) {
            (Some(url), key) => {
                let options = OpenAiOptions::compatible(url);
                match key {
                    Some(key) => options.with_api_key(key),
                    None => options,
                }
            }
            (None, Some(key)) => OpenAiOptions::new(key),
            (None, None) => return None,
        },
    )
}

fn anthropic() -> Option<Arc<dyn ChatClient>> {
    Some(Arc::new(AnthropicChatClient::new(
        AnthropicOptions::new(var("STRATA_TEST_ANTHROPIC_KEY")?)
            .with_model(var("STRATA_TEST_ANTHROPIC_MODEL")?),
    )))
}

async fn answers_a_plain_call(client: Arc<dyn ChatClient>) {
    let response = complete_with(
        &*client,
        ChatRequest::new(vec![
            Message::system("Answer with one word."),
            Message::user("What colour is a clear daytime sky?"),
        ])
        .with_max_tokens(50),
        None,
        &Default::default(),
    )
    .await
    .unwrap();

    assert!(!response.text().trim().is_empty());
    assert!(response.finish_reason.is_some());
    assert!(response.usage.input_tokens.unwrap_or_default() > 0);
}

async fn streams_through_a_graph_run(client: Arc<dyn ChatClient>) {
    let graph = Arc::new(
        StateGraph::<String>::new()
            .add_model_node(
                "Ask",
                client,
                |prompt: &String, _| vec![Message::user(prompt)],
                |_, response| response.text(),
                ModelNodeOptions::new().with_max_tokens(100),
            )
            .compile()
            .unwrap(),
    );
    let (observer, mut events) = RunEventChannel::new();

    let (handle, mut chunks) = graph.run_streaming_with(
        "live",
        "Count from one to five, in words.".to_string(),
        Arc::new(InMemoryCheckpointStore::new()),
        RunConfig::new().with_observer(observer),
    );
    let mut tokens = String::new();
    while let Some(chunk) = chunks.recv().await {
        if chunk.chunk_type == StreamChunkType::Token {
            tokens.push_str(&chunk.content);
        }
    }

    let GraphExecutionResult::Completed(answer) = handle.await.unwrap() else {
        panic!("expected completion")
    };
    assert_eq!(answer, tokens);
    let mut completed = None;
    while let Some(event) = events.recv().await {
        if let RunEventKind::ModelCallCompleted { result, usage, .. } = event.kind {
            completed = Some((result, usage));
        }
    }
    let (result, usage) = completed.unwrap();
    assert_eq!(result, ModelCallResult::Succeeded);
    assert!(
        usage.output_tokens.unwrap_or_default() > 0,
        "the streamed usage is reported"
    );
}

#[derive(Deserialize, StrataSchema)]
struct CityArgs {
    city: String,
}

#[strata_tool(
    name = "weather.now",
    description = "The weather in a city now, as a temperature in degrees Celsius."
)]
async fn weather_now(args: CityArgs) -> Result<String, StrataError> {
    Ok(format!(r#"{{"city":"{}","celsius":17}}"#, args.city))
}

#[derive(Debug, Deserialize, StrataSchema)]
struct Report {
    city: String,
    celsius: f64,
}

async fn runs_a_tool_loop_to_a_typed_answer(client: Arc<dyn ChatClient>) {
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(WeatherNowTool));
    let agent = Agent::<Report>::new(
        client,
        Arc::new(registry),
        AgentOptions::new("weather-reporter").with_max_tokens(400),
    );
    let ctx = datalium_strata::context::NodeContext::new("live-agent", "Report");

    let result = agent
        .run(
            vec![
                Message::system(
                    "Use the weather tool, then answer with one JSON object: the city and its temperature.",
                ),
                Message::user("How warm is it in Lisbon now?"),
            ],
            &ctx,
        )
        .await
        .unwrap();

    assert_eq!(result.tool_calls.len(), 1, "{:?}", result.messages);
    assert_eq!(result.tool_calls[0].name, "weather.now");
    assert!(result.output.city.contains("Lisbon"));
    assert_eq!(result.output.celsius, 17.0);
    assert!(result.turns >= 2);
}

#[tokio::test]
async fn openai_compatible_answers_and_streams() {
    let Some(client) = openai() else {
        eprintln!("skipped: STRATA_TEST_OPENAI_MODEL and STRATA_TEST_OPENAI_KEY or _URL not set");
        return;
    };
    answers_a_plain_call(client.clone()).await;
    streams_through_a_graph_run(client.clone()).await;
    runs_a_tool_loop_to_a_typed_answer(client).await;
}

#[tokio::test]
async fn anthropic_answers_and_streams() {
    let Some(client) = anthropic() else {
        eprintln!("skipped: STRATA_TEST_ANTHROPIC_KEY and STRATA_TEST_ANTHROPIC_MODEL not set");
        return;
    };
    answers_a_plain_call(client.clone()).await;
    streams_through_a_graph_run(client.clone()).await;
    runs_a_tool_loop_to_a_typed_answer(client).await;
}

#[tokio::test]
async fn openai_compatible_embeds_and_a_memory_remembers_and_recalls() {
    let Some(embedder) = openai_embedder() else {
        eprintln!(
            "skipped: STRATA_TEST_OPENAI_EMBEDDING_MODEL and STRATA_TEST_OPENAI_KEY or _URL not set"
        );
        return;
    };
    let texts = [
        "The disk on db-1 is full and writes are failing.",
        "The checkout pod is in a crash loop after the deploy.",
    ];

    let response = embed_with(
        &*embedder,
        EmbeddingRequest::new(texts),
        None,
        &Default::default(),
    )
    .await
    .unwrap();
    assert_eq!(response.vectors.len(), 2);
    let dimensions = response.vectors[0].len();
    assert!(dimensions > 0);
    assert_eq!(response.vectors[1].len(), dimensions);
    assert_ne!(response.vectors[0], response.vectors[1]);
    if var("STRATA_TEST_OPENAI_URL").is_none() {
        assert!(response.usage.input_tokens.unwrap() > 0);
    }
    assert_eq!(response.usage.output_tokens, None);

    let memory = Memory::new(
        Arc::new(InMemoryMemoryStore::new(dimensions)),
        embedder.clone(),
    );
    memory
        .remember_many(
            vec![
                MemoryEntry::new("incidents", "disk", texts[0]),
                MemoryEntry::new("incidents", "crash", texts[1]),
            ],
            None,
        )
        .await
        .unwrap();
    let matches = memory
        .recall(
            "incidents",
            "db-1 ran out of disk space",
            RecallOptions::new().limit(2),
            None,
        )
        .await
        .unwrap();
    assert_eq!(matches.len(), 2);
    assert_eq!(matches[0].record.id, "disk");
    assert_eq!(
        matches[0].record.embedding.model,
        format!(
            "{}/{}",
            embedder.provider(),
            embedder.default_model().unwrap()
        )
    );
}
