//! The embedding call and the memory helper (`design/45-Long-Term-Memory.md`,
//! "Tests"): `embed` / `embed_with` observed as a model call of operation
//! `embeddings` with input tokens only, failed as the fault table says;
//! `OpenAiEmbedder` against a raw local server (fixtures under
//! `tests/fixtures/openai/`); and what the shared cases don't pin about
//! `Memory` (the label refusal, `None` as the context, the node error). The
//! span and metrics are in `telemetry.rs`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{Answer, RawServer, ScriptedEmbedder};
use datalium_strata::StrataError;
use datalium_strata::checkpoint::{CheckpointStore, GraphExecutionResult};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{NodeError, StateGraph};
use datalium_strata::memory::{InMemoryMemoryStore, MemoryKey, MemoryStore, MemoryStoreError};
use datalium_strata::observability::{ModelCallResult, RunEvent, RunEventChannel, RunEventKind};
use datalium_strata_ai::{
    CallOptions, ChatError, ChatErrorKind, Embedder, EmbeddingRequest, EmbeddingResponse, Memory,
    MemoryEntry, MemoryError, ModelUsage, OpenAiEmbedder, OpenAiOptions, RecallOptions, embed,
    embed_with,
};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde_json::{Value, json};
use tokio::sync::mpsc;

const PATIENCE: Duration = Duration::from_secs(10);
const KEY: &str = "sk-test-0123456789";

fn store() -> Arc<dyn CheckpointStore> {
    Arc::new(InMemoryCheckpointStore::new())
}

fn scripted() -> ScriptedEmbedder {
    ScriptedEmbedder::new(
        "scripted",
        Some("embed-1"),
        &[
            ("disk full", &[1.0, 0.0, 0.0]),
            ("crash loop", &[0.0, 1.0, 0.0]),
        ],
    )
}

async fn drain(mut events: mpsc::UnboundedReceiver<RunEvent>) -> Vec<RunEvent> {
    let mut all = Vec::new();
    while let Some(event) = tokio::time::timeout(PATIENCE, events.recv()).await.unwrap() {
        all.push(event);
    }
    all
}

/// The model-call events of `events`, as `Started <operation> <model>
/// <provider>` and `Completed <operation> <result> <input>/<output>
/// <error>`.
fn model_calls(events: &[RunEvent]) -> Vec<String> {
    let tokens = |n: Option<u64>| n.map_or_else(|| "-".to_string(), |n| n.to_string());
    events
        .iter()
        .filter_map(|event| match &event.kind {
            RunEventKind::ModelCallStarted {
                operation,
                model,
                provider,
                ..
            } => Some(format!(
                "Started {operation} {model} {}",
                provider.as_deref().unwrap_or_default()
            )),
            RunEventKind::ModelCallCompleted {
                operation,
                result,
                usage,
                error,
                ..
            } => Some(format!(
                "Completed {operation} {result:?} {}/{} {}",
                tokens(usage.input_tokens),
                tokens(usage.output_tokens),
                error.as_deref().unwrap_or_default()
            )),
            _ => None,
        })
        .collect()
}

/// Runs one node that embeds `request` with `embedder` and returns what the
/// call returned and the run's model-call events.
async fn embed_in_a_run(
    embedder: Arc<ScriptedEmbedder>,
    request: EmbeddingRequest,
) -> (Result<EmbeddingResponse, NodeError>, Vec<String>) {
    let outcome = Arc::new(std::sync::Mutex::new(None));
    let node = {
        let outcome = outcome.clone();
        move |state: bool, ctx: NodeContext| {
            let (embedder, request, outcome) = (embedder.clone(), request.clone(), outcome.clone());
            async move {
                *outcome.lock().unwrap() = Some(embed(&*embedder, request, &ctx).await);
                state
            }
        }
    };
    let graph = StateGraph::<bool>::new()
        .add_async_node("Embed", node)
        .compile()
        .unwrap();
    let (observer, events) = RunEventChannel::new();
    graph
        .run_with(
            "embed",
            true,
            store(),
            RunConfig::new().with_observer(observer),
        )
        .await;
    let events = drain(events).await;
    let outcome = outcome.lock().unwrap().take().unwrap();
    (outcome, model_calls(&events))
}

// --- The call ----------------------------------------------------------------

#[tokio::test]
async fn an_embedding_call_is_a_model_call_of_operation_embeddings_with_input_tokens_only() {
    let mut embedder = scripted();
    embedder.output_tokens = Some(3);
    let embedder = Arc::new(embedder);

    let (outcome, events) = embed_in_a_run(
        embedder.clone(),
        EmbeddingRequest::new(["disk full", "crash loop"]),
    )
    .await;

    let response = outcome.unwrap();
    assert_eq!(
        response.vectors,
        vec![vec![1.0, 0.0, 0.0], vec![0.0, 1.0, 0.0]]
    );
    assert_eq!(response.model.as_deref(), Some("embed-1"));
    assert_eq!(
        events,
        [
            "Started embeddings embed-1 scripted",
            "Completed embeddings Succeeded 8/- ",
        ]
    );
    // The request names what the caller asked for, nothing more.
    let request = &embedder.requests.lock().unwrap()[0];
    assert_eq!(request.model, None);
    assert_eq!(request.dimensions, None);
}

#[tokio::test]
async fn the_model_is_the_request_s_else_the_embedder_s_else_unknown() {
    let (_, asked) = embed_in_a_run(
        Arc::new(scripted()),
        EmbeddingRequest::new(["disk full"]).with_model("embed-2"),
    )
    .await;
    let mut nameless = scripted();
    nameless.model = None;
    let (_, unknown) =
        embed_in_a_run(Arc::new(nameless), EmbeddingRequest::new(["disk full"])).await;

    assert_eq!(asked[0], "Started embeddings embed-2 scripted");
    assert_eq!(unknown[0], "Started embeddings unknown scripted");
}

#[tokio::test]
async fn a_blank_model_counts_as_none() {
    let (_, defaulted) = embed_in_a_run(
        Arc::new(scripted()),
        EmbeddingRequest::new(["disk full"]).with_model("  "),
    )
    .await;
    let mut blank = scripted();
    blank.model = Some(String::new());
    let (_, unknown) = embed_in_a_run(
        Arc::new(blank),
        EmbeddingRequest::new(["disk full"]).with_model(""),
    )
    .await;

    assert_eq!(defaulted[0], "Started embeddings embed-1 scripted");
    assert_eq!(unknown[0], "Started embeddings unknown scripted");
}

#[tokio::test]
async fn no_inputs_make_no_call_and_record_nothing() {
    let embedder = Arc::new(scripted());

    let (outcome, events) = embed_in_a_run(
        embedder.clone(),
        EmbeddingRequest::new(Vec::<String>::new()),
    )
    .await;

    assert_eq!(outcome.unwrap(), EmbeddingResponse::default());
    assert!(events.is_empty());
    assert!(embedder.calls().is_empty());
}

#[tokio::test]
async fn a_rate_limit_is_transient_with_the_providers_hint() {
    let embedder = Arc::new(scripted().failing(
        "disk full",
        ChatError::status(429, Some(Duration::from_secs(2)), "slow down"),
    ));

    let (outcome, events) = embed_in_a_run(embedder, EmbeddingRequest::new(["disk full"])).await;

    assert_eq!(
        outcome.unwrap_err(),
        NodeError::transient_after("model call failed (429): slow down", Duration::from_secs(2))
    );
    assert_eq!(events[1], "Completed embeddings Failed -/- slow down");
}

#[tokio::test]
async fn a_bad_request_is_fatal() {
    let embedder =
        Arc::new(scripted().failing("disk full", ChatError::status(400, None, "input too long")));

    let (outcome, _) = embed_in_a_run(embedder, EmbeddingRequest::new(["disk full"])).await;

    assert_eq!(
        outcome.unwrap_err(),
        NodeError::Fatal("model call failed (400): input too long".to_string())
    );
}

#[tokio::test]
async fn an_answer_short_of_vectors_is_a_fatal_invalid_response() {
    let mut embedder = scripted();
    embedder.unanswered.insert("crash loop".to_string());

    let (outcome, events) = embed_in_a_run(
        Arc::new(embedder),
        EmbeddingRequest::new(["disk full", "crash loop"]),
    )
    .await;

    assert_eq!(
        outcome.unwrap_err(),
        NodeError::Fatal(
            "model call failed (invalid_response): expected 2 vectors, the embedder returned 1"
                .to_string()
        )
    );
    assert_eq!(
        events[1],
        "Completed embeddings Failed -/- expected 2 vectors, the embedder returned 1"
    );
}

#[tokio::test]
async fn a_cancelled_run_records_the_call_cancelled() {
    let mut embedder = scripted();
    embedder.hang = true;
    let embedder = Arc::new(embedder);
    let graph = {
        let embedder = embedder.clone();
        StateGraph::<bool>::new()
            .add_async_node("Embed", move |state: bool, ctx: NodeContext| {
                let embedder = embedder.clone();
                async move {
                    let _ = embed(&*embedder, EmbeddingRequest::new(["disk full"]), &ctx).await;
                    state
                }
            })
            .compile()
            .unwrap()
    };
    let (observer, events) = RunEventChannel::new();
    let token = datalium_strata::CancellationToken::new();
    let config = RunConfig::new()
        .with_observer(observer)
        .with_cancellation(token.clone());

    let run = tokio::spawn(async move { graph.run_with("cancel", true, store(), config).await });
    while embedder.calls().is_empty() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    token.cancel();
    let result = tokio::time::timeout(PATIENCE, run).await.unwrap().unwrap();

    assert!(matches!(result, GraphExecutionResult::Cancelled { .. }));
    let done = drain(events)
        .await
        .into_iter()
        .find_map(|e| match e.kind {
            RunEventKind::ModelCallCompleted {
                result, operation, ..
            } => Some((result, operation)),
            _ => None,
        })
        .unwrap();
    assert_eq!(done, (ModelCallResult::Cancelled, "embeddings".to_string()));
}

#[tokio::test]
async fn a_call_outside_a_run_answers_and_names_its_agent() {
    let embedder = scripted();

    let response = embed_with(
        &embedder,
        EmbeddingRequest::new(["crash loop"]),
        None,
        &CallOptions::new().with_agent("librarian"),
    )
    .await
    .unwrap();

    assert_eq!(response.vectors, vec![vec![0.0, 1.0, 0.0]]);
    assert_eq!(response.usage, ModelUsage::new(Some(4), None));
}

// --- OpenAiEmbedder ----------------------------------------------------------

fn fixture(name: &str) -> Value {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/openai")
        .join(name);
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn openai(server: &RawServer) -> OpenAiEmbedder {
    OpenAiEmbedder::new(
        OpenAiOptions::new(KEY)
            .with_base_url(format!("{}/v1", server.url))
            .with_model("gpt-chat-not-used")
            .with_header("OpenAI-Organization", "org-7")
            .with_timeouts(Duration::from_secs(5), Duration::from_secs(5)),
        "text-embedding-3-small",
    )
}

#[tokio::test]
async fn openai_sends_an_embeddings_request_and_reads_the_vectors_in_index_order() {
    let server =
        RawServer::start(vec![Answer::json(200, fixture("embeddings-response.json"))]).await;
    let embedder = openai(&server);

    let response = embedder
        .embed(
            EmbeddingRequest::new(["disk full on db-1", "pod in a crash loop"]).with_dimensions(3),
        )
        .await
        .unwrap();

    assert_eq!(
        response.vectors,
        vec![vec![0.8, 0.1, 0.05], vec![0.0, 0.9, 0.1]]
    );
    assert_eq!(response.model.as_deref(), Some("text-embedding-3-small"));
    assert_eq!(response.usage, ModelUsage::new(Some(9), None));
    assert_eq!(embedder.provider(), "openai");
    assert_eq!(embedder.default_model(), Some("text-embedding-3-small"));
    let request = &server.received()[0];
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/v1/embeddings");
    assert_eq!(
        request.header("authorization"),
        Some("Bearer sk-test-0123456789")
    );
    assert_eq!(request.header("openai-organization"), Some("org-7"));
    assert_eq!(request.body, fixture("embeddings-request.json"));
}

#[tokio::test]
async fn openai_sends_no_dimensions_unless_asked_and_the_request_s_model_wins() {
    let server =
        RawServer::start(vec![Answer::json(200, fixture("embeddings-response.json"))]).await;

    openai(&server)
        .embed(
            EmbeddingRequest::new(["disk full on db-1", "pod in a crash loop"])
                .with_model("text-embedding-3-large"),
        )
        .await
        .unwrap();

    assert_eq!(
        server.received()[0].body,
        json!({
            "model": "text-embedding-3-large",
            "input": ["disk full on db-1", "pod in a crash loop"],
            "encoding_format": "float",
        })
    );
}

#[tokio::test]
async fn openai_a_rate_limit_is_transient_with_the_providers_hint() {
    let server = RawServer::start(vec![
        Answer::json(
            429,
            json!({ "error": { "message": "Rate limit reached", "type": "requests" } }),
        )
        .with_header("retry-after-ms", "1500"),
    ])
    .await;

    let error = openai(&server)
        .embed(EmbeddingRequest::new(["x"]))
        .await
        .unwrap_err();

    assert_eq!(error.kind, ChatErrorKind::Transient);
    assert_eq!(error.status, Some(429));
    assert_eq!(error.retry_after, Some(Duration::from_millis(1500)));
    assert_eq!(error.message, "429 Too Many Requests: Rate limit reached");
}

#[tokio::test]
async fn openai_a_bad_request_is_fatal() {
    let server = RawServer::start(vec![Answer::json(
        400,
        json!({ "error": { "message": "This model's maximum context length is 8192 tokens" } }),
    )])
    .await;

    let error = openai(&server)
        .embed(EmbeddingRequest::new(["x"]))
        .await
        .unwrap_err();

    assert_eq!(error.kind, ChatErrorKind::Fatal);
    assert_eq!(error.status, Some(400));
    assert!(!error.message.contains(KEY));
}

#[tokio::test]
async fn openai_an_answer_it_can_t_read_is_a_fatal_invalid_response() {
    let bodies = [
        json!({ "model": "m" }),
        json!({ "data": [{ "embedding": [0.1] }] }),
        json!({ "data": [{ "index": 0, "embedding": [0.1] }, { "index": 0, "embedding": [0.2] }] }),
        json!({ "data": [{ "index": 2, "embedding": [0.1] }] }),
        json!({ "data": [{ "index": 0, "embedding": "AAAA" }] }),
        json!({ "data": [{ "index": 0, "embedding": [0.1, "x"] }] }),
    ];
    let server = RawServer::start(
        bodies
            .iter()
            .map(|body| Answer::json(200, body.clone()))
            .collect(),
    )
    .await;
    let embedder = openai(&server);

    for body in &bodies {
        let error = embedder
            .embed(EmbeddingRequest::new(["x"]))
            .await
            .unwrap_err();
        assert_eq!(error.kind, ChatErrorKind::Fatal, "{body}");
        assert_eq!(error.error_type, "invalid_response", "{body}");
    }
}

#[test]
fn openai_embedder_s_debug_never_prints_the_key() {
    let embedder = OpenAiEmbedder::new(OpenAiOptions::new(KEY), "text-embedding-3-small");
    let printed = format!("{embedder:?}");
    assert!(!printed.contains(KEY));
    assert!(printed.contains("text-embedding-3-small"));
}

#[test]
fn ollama_s_embedder_posts_to_its_v1_path_as_provider_ollama() {
    let embedder = OpenAiEmbedder::new(
        OpenAiOptions::ollama("http://localhost:11434/"),
        "nomic-embed-text",
    );
    assert_eq!(embedder.provider(), "ollama");
    assert_eq!(embedder.options().base_url, "http://localhost:11434/v1");
}

// --- Memory --------------------------------------------------------------------

#[tokio::test]
async fn an_embedder_that_names_no_model_needs_a_label_and_is_refused_before_any_call() {
    let store = Arc::new(InMemoryMemoryStore::new(3));
    let mut nameless = scripted();
    nameless.model = None;
    let nameless = Arc::new(nameless);
    let memory = Memory::new(store.clone(), nameless.clone());

    let error = memory
        .remember(MemoryEntry::new("incidents", "inc-1", "disk full"), None)
        .await
        .unwrap_err();
    let recall = memory
        .recall("incidents", "disk full", RecallOptions::new(), None)
        .await
        .unwrap_err();

    assert!(matches!(
        &error,
        MemoryError::Embedding(NodeError::Fatal(message))
            if message == "the embedder names no model: label the memory's vectors with with_model"
    ));
    assert!(matches!(
        recall,
        MemoryError::Embedding(NodeError::Fatal(_))
    ));
    assert!(nameless.calls().is_empty());

    let labelled = memory.with_model("nomic-768");
    labelled
        .remember(MemoryEntry::new("incidents", "inc-1", "disk full"), None)
        .await
        .unwrap();
    let record = store
        .get(&MemoryKey::new("incidents", "", "inc-1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.embedding.model, "nomic-768");
}

#[tokio::test]
async fn a_blank_label_is_the_store_s_refusal_and_a_blank_default_model_names_none() {
    let store = Arc::new(InMemoryMemoryStore::new(3));
    let embedder = Arc::new(scripted());
    for label in ["", "   "] {
        let memory = Memory::new(store.clone(), embedder.clone()).with_model(label);
        let error = memory
            .remember(MemoryEntry::new("incidents", "inc-1", "disk full"), None)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, MemoryError::Store(MemoryStoreError(m)) if m == "the model is empty"),
            "{error:?}"
        );
        let error = memory
            .recall("incidents", "disk full", RecallOptions::new(), None)
            .await
            .unwrap_err();
        assert!(matches!(error, MemoryError::Store(_)), "{error:?}");
    }
    assert!(embedder.calls().is_empty());

    let mut blank = scripted();
    blank.model = Some(" ".to_string());
    let blank = Arc::new(blank);
    let error = Memory::new(store.clone(), blank.clone())
        .remember(MemoryEntry::new("incidents", "inc-1", "disk full"), None)
        .await
        .unwrap_err();
    assert!(
        matches!(&error, MemoryError::Embedding(NodeError::Fatal(m)) if m.starts_with("the embedder names no model")),
        "{error:?}"
    );
    assert!(blank.calls().is_empty());
}

#[tokio::test]
async fn an_empty_batch_makes_no_call_but_still_needs_a_label() {
    let store = Arc::new(InMemoryMemoryStore::new(3));
    let embedder = Arc::new(scripted());
    Memory::new(store.clone(), embedder.clone())
        .remember_many(Vec::new(), None)
        .await
        .unwrap();
    assert!(embedder.calls().is_empty());

    let mut nameless = scripted();
    nameless.model = None;
    let error = Memory::new(store, Arc::new(nameless))
        .remember_many(Vec::new(), None)
        .await
        .unwrap_err();
    assert!(
        matches!(error, MemoryError::Embedding(NodeError::Fatal(_))),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_nameless_provider_is_labelled_unknown() {
    let store = Arc::new(InMemoryMemoryStore::new(3));
    let mut embedder = scripted();
    embedder.provider = String::new();
    let memory = Memory::new(store.clone(), Arc::new(embedder));

    memory
        .remember(MemoryEntry::new("incidents", "inc-1", "disk full"), None)
        .await
        .unwrap();

    assert_eq!(memory.label().unwrap(), "unknown/embed-1");
    let found = store
        .get(&MemoryKey::new("incidents", "", "inc-1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.embedding.model, "unknown/embed-1");
}

#[tokio::test]
async fn a_vector_the_store_can_t_hold_is_the_store_s_refusal() {
    let store = Arc::new(InMemoryMemoryStore::new(2));
    let memory = Memory::new(store, Arc::new(scripted()));

    let error = memory
        .remember(MemoryEntry::new("incidents", "inc-1", "disk full"), None)
        .await
        .unwrap_err();

    assert!(matches!(
        &error,
        MemoryError::Store(MemoryStoreError(message))
            if message == "the vector has 3 dimensions, and this store has 2"
    ));
    assert_eq!(
        error.to_string(),
        "the vector has 3 dimensions, and this store has 2"
    );
    assert_eq!(
        NodeError::from(error),
        NodeError::Fatal("the vector has 3 dimensions, and this store has 2".to_string())
    );
}

#[tokio::test]
async fn an_embedding_failure_is_the_node_failure_through_question_mark() {
    let memory = Memory::new(
        Arc::new(InMemoryMemoryStore::new(3)),
        Arc::new(scripted().failing(
            "disk full",
            ChatError::status(503, Some(Duration::from_secs(1)), "overloaded"),
        )),
    );
    let graph = StateGraph::<bool>::new()
        .add_async_fallible_node("Remember", move |state: bool, ctx: NodeContext| {
            let memory = memory.clone();
            async move {
                memory
                    .remember(MemoryEntry::new("incidents", "inc-1", "disk full"), &ctx)
                    .await?;
                Ok(state)
            }
        })
        .compile()
        .unwrap();

    let result = graph.run("remember-fails", true, store()).await;

    let GraphExecutionResult::Failed(StrataError::NodeFailed { message, .. }) = result else {
        panic!("expected a node failure")
    };
    assert_eq!(message, "model call failed (503): overloaded");
}

#[test]
fn memory_s_debug_names_the_provider_and_the_label() {
    let memory = Memory::new(Arc::new(InMemoryMemoryStore::new(3)), Arc::new(scripted()));
    let printed = format!("{memory:?}");
    assert!(printed.contains("scripted"), "{printed}");
    assert!(printed.contains("scripted/embed-1"), "{printed}");
}
