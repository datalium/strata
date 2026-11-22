//! Samples for <https://stratamesh.dev/guides/ai/call-a-model/>. A stand-in model, `Answering`, answers every
//! call, so the tests need no provider. The streaming sample prints the answer, so the test that checks what
//! it prints runs it in a child process (`run_alone`).

use std::sync::Mutex;
use std::time::Duration;

use datalium_strata::BoxFuture;
use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::store::CheckpointStore;
use datalium_strata::streaming::StreamChunkType;
use datalium_strata_ai::{
    ChatError, ChatRequest, ChatResponse, ChatStream, ChatUpdate, ChatUpdates, complete,
};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};

// docs:begin guides/ai/call-a-model/model-node
use datalium_strata::graph::{CompiledGraph, Jitter, RecoveryPolicy, StateGraph};
use datalium_strata_ai::{ChatClient, Message, ModelNodeOptions, StateGraphAiExt};

fn build_graph(client: Arc<dyn ChatClient>) -> Arc<CompiledGraph<LlmStreamState>> {
    let graph = StateGraph::<LlmStreamState>::new()
        .add_model_node(
            "AnswerQuestion",
            client,
            |state: &LlmStreamState, _| {
                vec![
                    Message::system(
                        "You are a helpful assistant. Answer in two or three sentences.",
                    ),
                    Message::user(state.question.clone()),
                ]
            },
            |state: LlmStreamState, response| LlmStreamState {
                answer: response.text(),
                ..state
            },
            ModelNodeOptions::new().with_max_tokens(200),
        )
        .set_recovery_policy(
            "AnswerQuestion",
            RecoveryPolicy::retry(2).with_backoff(
                Duration::from_secs(1),
                Duration::from_secs(10),
                Jitter::Full,
            ),
        )
        .add_node("SummarizeAnswer", |state: LlmStreamState| LlmStreamState {
            answer_char_count: state.answer.chars().count(),
            ..state
        })
        .add_edge("AnswerQuestion", "SummarizeAnswer")
        .compile()
        .expect("graph is structurally valid");
    Arc::new(graph)
}
// docs:end guides/ai/call-a-model/model-node

use std::sync::Arc;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct LlmStreamState {
    question: String,
    answer: String,
    answer_char_count: usize,
}

const QUESTION: &str = "What is a borrow checker?";
const ANSWER: &str = "A borrow checker is the part of the Rust compiler that checks references. \
                      It makes sure no reference outlives its value, at compile time.";

/// A stand-in for a model: answers every request with `answer`, streamed a word at a time, and records each
/// request's `max_tokens` and whether it streamed.
struct Answering {
    answer: String,
    requests: Mutex<Vec<(Option<u32>, bool)>>,
}

impl Answering {
    fn new(answer: &str) -> Self {
        Self {
            answer: answer.to_string(),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn updates(&self, request: &ChatRequest, streamed: bool) -> Vec<ChatUpdate> {
        self.requests
            .lock()
            .unwrap()
            .push((request.max_tokens, streamed));
        let mut updates: Vec<ChatUpdate> = self
            .answer
            .split_inclusive(' ')
            .map(|word| ChatUpdate::Text(word.to_string()))
            .collect();
        updates.push(ChatUpdate::Finish("stop".to_string()));
        updates
    }

    fn requests(&self) -> Vec<(Option<u32>, bool)> {
        self.requests.lock().unwrap().clone()
    }
}

impl ChatClient for Answering {
    fn provider(&self) -> &str {
        "stand-in"
    }

    fn complete(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatResponse, ChatError>> {
        let updates = self.updates(&request, false);
        Box::pin(async move {
            let mut answer = ChatUpdates::new();
            for update in updates {
                answer.push(update);
            }
            answer.finish()
        })
    }

    fn stream(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatStream, ChatError>> {
        let updates = self.updates(&request, true);
        Box::pin(async move {
            let stream: ChatStream =
                Box::pin(futures_util::stream::iter(updates.into_iter().map(Ok)));
            Ok(stream)
        })
    }
}

fn question() -> LlmStreamState {
    LlmStreamState {
        question: QUESTION.to_string(),
        ..Default::default()
    }
}

/// Runs one test of this binary in a child process and returns what it printed.
fn run_alone(test: &str) -> String {
    let output =
        std::process::Command::new(std::env::current_exe().expect("the test binary's path"))
            .args(["--exact", test, "--nocapture", "--test-threads=1"])
            .output()
            .expect("the test binary runs");
    let stdout = String::from_utf8(output.stdout).expect("the output is UTF-8");
    assert!(
        output.status.success(),
        "{test} failed:\n{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains("1 passed"), "{test} didn't run:\n{stdout}");
    stdout
}

#[test]
fn create_client() {
    let api_key = "sk-not-a-real-key";
    let anthropic_key = "sk-ant-not-a-real-key";
    let anthropic_model = "claude-model";

    // docs:begin guides/ai/call-a-model/create-client
    use std::sync::Arc;

    use datalium_strata_ai::{
        AnthropicChatClient, AnthropicOptions, ChatClient, OpenAiChatClient, OpenAiOptions,
    };

    // OpenAI
    let openai: Arc<dyn ChatClient> = Arc::new(OpenAiChatClient::new(
        OpenAiOptions::new(api_key).with_model("gpt-4o-mini"),
    ));

    // Any OpenAI-compatible server: Ollama, vLLM, LM Studio, ...
    let local: Arc<dyn ChatClient> = Arc::new(OpenAiChatClient::new(
        OpenAiOptions::compatible("http://localhost:11434/v1").with_model("qwen2.5:3b"),
    ));

    // Anthropic
    let anthropic: Arc<dyn ChatClient> = Arc::new(AnthropicChatClient::new(
        AnthropicOptions::new(anthropic_key).with_model(anthropic_model),
    ));
    // docs:end guides/ai/call-a-model/create-client

    assert_eq!(openai.provider(), "openai");
    assert_eq!(openai.default_model(), Some("gpt-4o-mini"));
    assert_eq!(local.default_model(), Some("qwen2.5:3b"));
    assert_eq!(anthropic.provider(), "anthropic");
    assert_eq!(anthropic.default_model(), Some("claude-model"));

    // What the text under the sample says about the options.
    assert_eq!(
        OpenAiOptions::new(api_key).base_url,
        "https://api.openai.com/v1"
    );
    let compatible = OpenAiOptions::compatible("http://localhost:11434/v1");
    assert_eq!(compatible.api_key, None);
    assert_eq!(
        compatible.with_api_key("local-key").api_key.as_deref(),
        Some("local-key")
    );
    assert_eq!(
        OpenAiOptions::ollama("http://localhost:11434").base_url,
        "http://localhost:11434/v1"
    );
}

#[tokio::test]
async fn model_node() {
    let model = Arc::new(Answering::new(ANSWER));
    let graph = build_graph(model.clone());

    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    match graph
        .run("call-a-model-model-node", question(), store)
        .await
    {
        GraphExecutionResult::Completed(state) => {
            assert_eq!(state.answer, ANSWER);
            assert_eq!(state.answer_char_count, ANSWER.chars().count());
        }
        GraphExecutionResult::Failed(error) => panic!("the run failed: {error:?}"),
        _ => panic!("the run paused, but this graph has nothing that pauses"),
    }
    // One request, with the node's max tokens, that waited for the whole answer.
    assert_eq!(model.requests(), [(Some(200), false)]);
}

#[tokio::test]
async fn own_node() {
    let model = Arc::new(Answering::new(ANSWER));
    let client: Arc<dyn ChatClient> = model.clone();

    let graph = StateGraph::<LlmStreamState>::new()
        // docs:begin guides/ai/call-a-model/own-node
        .add_async_fallible_node("AnswerQuestion", move |state: LlmStreamState, ctx| {
            let client = client.clone();
            async move {
                let mut request = ChatRequest::new(vec![Message::user(state.question.clone())]);
                request.max_tokens = Some(200);
                let response = complete(&*client, request, &ctx).await?;
                Ok(LlmStreamState {
                    answer: response.text(),
                    ..state
                })
            }
        })
        // docs:end guides/ai/call-a-model/own-node
        .compile()
        .expect("graph is structurally valid");

    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    match graph.run("call-a-model-own-node", question(), store).await {
        GraphExecutionResult::Completed(state) => assert_eq!(state.answer, ANSWER),
        GraphExecutionResult::Failed(error) => panic!("the run failed: {error:?}"),
        _ => panic!("the run paused, but this graph has nothing that pauses"),
    }
    assert_eq!(model.requests(), [(Some(200), false)]);
}

#[tokio::test]
async fn stream() {
    let model = Arc::new(Answering::new(ANSWER));
    let graph = build_graph(model.clone());
    let initial = question();
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());

    // docs:begin guides/ai/call-a-model/stream
    let (handle, mut chunks) = graph.run_streaming("stream-thread", initial, store);

    while let Some(chunk) = chunks.recv().await {
        match chunk.chunk_type {
            StreamChunkType::Token => print!("{}", chunk.content),
            StreamChunkType::Retry => println!("\n[retry] {}", chunk.node_name),
            StreamChunkType::Error => println!("\n[error] {}: {}", chunk.node_name, chunk.content),
            _ => {}
        }
    }

    match handle.await.expect("the run's task doesn't panic") {
        GraphExecutionResult::Completed(state) => {
            println!("\n\n{} characters", state.answer_char_count)
        }
        // docs:hide ...
        GraphExecutionResult::Failed(error) => panic!("the run failed: {error:?}"),
        _ => panic!("the run paused, but this graph has nothing that pauses"),
        // docs:show
    }
    // docs:end guides/ai/call-a-model/stream

    // Because the run streamed, the call streamed too.
    assert_eq!(model.requests(), [(Some(200), true)]);
}

#[test]
fn stream_prints_the_answer() {
    let stdout = run_alone("guides::ai::call_a_model::stream");
    // The Token chunks, joined in order, are the whole answer.
    let expected = format!("{ANSWER}\n\n{} characters\n", ANSWER.chars().count());
    assert!(stdout.contains(&expected), "the run printed:\n{stdout}");
}
