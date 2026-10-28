//! The two HTTP clients (`design/44-LLM-Integration.md`, "Tests"): the
//! request each sends, the answers and server-sent events each reads (from
//! `tests/fixtures/`), and each failure classed as the fault table says,
//! against a raw local server.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{Answer, RawServer};
use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::graph::StateGraph;
use datalium_strata::streaming::StreamChunkType;
use datalium_strata_ai::{
    ANTHROPIC_VERSION, AnthropicChatClient, AnthropicOptions, ChatClient, ChatErrorKind,
    ChatRequest, ChatResponse, ChatUpdate, ChatUpdates, Content, MaxTokensField, Message,
    ModelNodeOptions, ModelUsage, OpenAiChatClient, OpenAiOptions, ResponseFormat, Role,
    StateGraphAiExt, ToolChoice, ToolDeclaration,
};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use futures_util::StreamExt;
use serde_json::{Value, json};

const KEY: &str = "sk-test-0123456789";

fn openai(server: &RawServer) -> OpenAiChatClient {
    OpenAiChatClient::new(
        OpenAiOptions::new(KEY)
            .with_base_url(format!("{}/v1", server.url))
            .with_model("gpt-test")
            .with_timeouts(Duration::from_secs(5), Duration::from_secs(5)),
    )
}

fn anthropic(server: &RawServer) -> AnthropicChatClient {
    AnthropicChatClient::new(
        AnthropicOptions::new(KEY)
            .with_base_url(&server.url)
            .with_model("claude-test")
            .with_timeouts(Duration::from_secs(5), Duration::from_secs(5)),
    )
}

/// Streams `request` and assembles the answer.
async fn streamed(client: &dyn ChatClient, request: ChatRequest) -> ChatResponse {
    let mut stream = client.stream(request).await.unwrap();
    let mut updates = ChatUpdates::new();
    while let Some(update) = stream.next().await {
        updates.push(update.unwrap());
    }
    updates.finish().unwrap()
}

fn call_of(response: &ChatResponse, index: usize) -> (String, String, Value) {
    let (id, name, arguments) = response.tool_calls().nth(index).unwrap();
    (id.to_string(), name.to_string(), arguments.clone())
}

// --- OpenAI-compatible -------------------------------------------------------

#[tokio::test]
async fn openai_sends_a_chat_completions_request_and_reads_the_answer() {
    let server = RawServer::start(vec![Answer::json(
        200,
        json!({
            "id": "chatcmpl-9",
            "model": "gpt-test-2026",
            "choices": [{ "index": 0, "message": { "role": "assistant", "content": "Roll back v42." }, "finish_reason": "stop" }],
            "usage": { "prompt_tokens": 17, "completion_tokens": 4, "total_tokens": 21 },
        }),
    )])
    .await;

    let response = openai(&server)
        .complete(
            ChatRequest::new(vec![Message::system("Be brief."), Message::user("Fix?")])
                .with_max_tokens(200)
                .with_temperature(0.2),
        )
        .await
        .unwrap();

    assert_eq!(response.text(), "Roll back v42.");
    assert_eq!(response.finish_reason.as_deref(), Some("stop"));
    assert_eq!(response.model.as_deref(), Some("gpt-test-2026"));
    assert_eq!(response.id.as_deref(), Some("chatcmpl-9"));
    assert_eq!(response.usage, ModelUsage::new(Some(17), Some(4)));
    let request = &server.received()[0];
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/v1/chat/completions");
    assert_eq!(
        request.header("authorization"),
        Some("Bearer sk-test-0123456789")
    );
    assert_eq!(
        request.body,
        json!({
            "model": "gpt-test",
            "messages": [
                { "role": "system", "content": "Be brief." },
                { "role": "user", "content": "Fix?" },
            ],
            "max_completion_tokens": 200,
            "temperature": 0.2,
        })
    );
}

#[tokio::test]
async fn openai_streams_text_with_its_usage() {
    let server = RawServer::start(vec![Answer::fixture("openai/stream-text.sse")]).await;

    let response = streamed(
        &openai(&server),
        ChatRequest::new(vec![Message::user("Fix?")]),
    )
    .await;

    assert_eq!(response.text(), "Roll back v42.");
    assert_eq!(response.finish_reason.as_deref(), Some("stop"));
    assert_eq!(response.usage, ModelUsage::new(Some(41), Some(4)));
    assert_eq!(response.model.as_deref(), Some("gpt-test-2026"));
    let body = &server.received()[0].body;
    assert_eq!(body["stream"], json!(true));
    assert_eq!(body["stream_options"], json!({ "include_usage": true }));
}

#[tokio::test]
async fn openai_streams_tool_calls_in_pieces() {
    let server = RawServer::start(vec![Answer::fixture("openai/stream-tool-calls.sse")]).await;

    let response = streamed(
        &openai(&server),
        ChatRequest::new(vec![Message::user("Look.")]),
    )
    .await;

    assert_eq!(response.finish_reason.as_deref(), Some("tool_calls"));
    assert_eq!(
        call_of(&response, 0),
        (
            "call_a".into(),
            "k8s_pods".into(),
            json!({ "namespace": "shop" })
        )
    );
    assert_eq!(
        call_of(&response, 1),
        ("call_b".into(), "prometheus_query".into(), json!({}))
    );
    assert_eq!(response.usage, ModelUsage::new(Some(90), Some(31)));
}

#[tokio::test]
async fn openai_streams_into_a_graph_run() {
    let server = RawServer::start(vec![Answer::fixture("openai/stream-text.sse")]).await;
    let client: Arc<dyn ChatClient> = Arc::new(openai(&server));
    let graph = Arc::new(
        StateGraph::<String>::new()
            .add_model_node(
                "Ask",
                client,
                |prompt: &String, _| vec![Message::user(prompt)],
                |_, response| response.text(),
                ModelNodeOptions::new(),
            )
            .compile()
            .unwrap(),
    );

    let (handle, mut chunks) = graph.run_streaming(
        "openai-stream",
        "Fix?".to_string(),
        Arc::new(InMemoryCheckpointStore::new()),
    );
    let mut tokens = Vec::new();
    while let Some(chunk) = chunks.recv().await {
        if chunk.chunk_type == StreamChunkType::Token {
            tokens.push(chunk.content.to_string());
        }
    }

    let GraphExecutionResult::Completed(answer) = handle.await.unwrap() else {
        panic!("expected completion")
    };
    assert_eq!(answer, "Roll back v42.");
    assert_eq!(tokens, vec!["Roll ", "back v42."]);
}

#[tokio::test]
async fn openai_a_rate_limit_is_transient_with_the_providers_hint() {
    let server = RawServer::start(vec![
        Answer::json(
            429,
            json!({ "error": { "message": "Rate limit reached", "type": "requests" } }),
        )
        .with_header("retry-after-ms", "1500")
        .with_header("retry-after", "2"),
    ])
    .await;

    let error = openai(&server)
        .complete(ChatRequest::new(vec![Message::user("x")]))
        .await
        .unwrap_err();

    assert_eq!(error.kind, ChatErrorKind::Transient);
    assert_eq!(error.status, Some(429));
    assert_eq!(error.retry_after, Some(Duration::from_millis(1500)));
    assert_eq!(error.error_type, "429");
    assert_eq!(error.message, "429 Too Many Requests: Rate limit reached");
}

#[tokio::test]
async fn openai_a_bad_key_is_fatal() {
    let server = RawServer::start(vec![Answer::json(
        401,
        json!({ "error": { "message": "Incorrect API key provided" } }),
    )])
    .await;

    let error = openai(&server)
        .stream(ChatRequest::new(vec![Message::user("x")]))
        .await
        .err()
        .unwrap();

    assert_eq!(error.kind, ChatErrorKind::Fatal);
    assert_eq!(error.status, Some(401));
    assert!(!error.message.contains(KEY));
}

#[tokio::test]
async fn openai_a_stream_cut_off_before_done_is_a_transient_failure() {
    let server = RawServer::start(vec![Answer::fixture("openai/stream-cut-off.sse")]).await;

    let mut stream = openai(&server)
        .stream(ChatRequest::new(vec![Message::user("x")]))
        .await
        .unwrap();
    let mut texts = Vec::new();
    let mut failure = None;
    while let Some(update) = stream.next().await {
        match update {
            Ok(datalium_strata_ai::ChatUpdate::Text(text)) => texts.push(text),
            Ok(_) => {}
            Err(error) => failure = Some(error),
        }
    }

    assert_eq!(texts, vec!["Half "]);
    let failure = failure.expect("the cut-off is reported");
    assert_eq!(failure.kind, ChatErrorKind::Transient);
    assert_eq!(failure.error_type, "connection");
}

#[test]
fn openai_maps_tool_turns_formats_and_choices_to_the_wire() {
    let client = OpenAiChatClient::new(OpenAiOptions::new(KEY).with_model("gpt-test"));
    let request = ChatRequest::new(vec![
        Message::user("Look."),
        Message::new(
            Role::Assistant,
            vec![
                Content::Reasoning {
                    text: "hidden".into(),
                    signature: Some("sig".into()),
                },
                Content::ToolCall {
                    id: "call_a".into(),
                    name: "k8s_pods".into(),
                    arguments: json!({ "namespace": "shop" }),
                },
            ],
        ),
        Message::tool_results([("call_a".to_string(), "[\"web-1\"]".to_string())]),
    ])
    .with_tools(vec![ToolDeclaration::new(
        "k8s_pods",
        "Lists pods.",
        json!({ "type": "object", "properties": { "namespace": { "type": "string" } } }),
    )])
    .with_tool_choice(ToolChoice::Tool("k8s_pods".into()))
    .with_response_format(ResponseFormat::JsonSchema {
        name: "diagnosis".into(),
        schema: json!({ "type": "object" }),
        strict: true,
    })
    .with_extra("seed", json!(7));

    let body = client.body(&request, false);

    assert_eq!(
        body["messages"],
        json!([
            { "role": "user", "content": "Look." },
            { "role": "assistant", "content": null, "tool_calls": [
                { "id": "call_a", "type": "function", "function": { "name": "k8s_pods", "arguments": "{\"namespace\":\"shop\"}" } },
            ] },
            { "role": "tool", "tool_call_id": "call_a", "content": "[\"web-1\"]" },
        ])
    );
    assert_eq!(
        body["tools"],
        json!([{ "type": "function", "function": {
            "name": "k8s_pods",
            "description": "Lists pods.",
            "parameters": { "type": "object", "properties": { "namespace": { "type": "string" } } },
        } }])
    );
    assert_eq!(
        body["tool_choice"],
        json!({ "type": "function", "function": { "name": "k8s_pods" } })
    );
    assert_eq!(
        body["response_format"],
        json!({ "type": "json_schema", "json_schema": { "name": "diagnosis", "schema": { "type": "object" }, "strict": true } })
    );
    assert_eq!(body["seed"], json!(7));
}

#[test]
fn openai_compatible_servers_get_max_tokens_and_ollama_its_v1_path() {
    let ollama = OpenAiOptions::ollama("http://localhost:11434/");
    assert_eq!(ollama.base_url, "http://localhost:11434/v1");
    assert_eq!(ollama.provider, "ollama");
    assert_eq!(ollama.api_key, None);
    assert_eq!(ollama.max_tokens_field, MaxTokensField::MaxTokens);
    let client = OpenAiChatClient::new(ollama.with_model("llama3.2"));
    let body = client.body(
        &ChatRequest::new(vec![Message::user("x")]).with_max_tokens(9),
        false,
    );
    assert_eq!(body["max_tokens"], json!(9));
    assert_eq!(body["model"], json!("llama3.2"));
    assert_eq!(client.provider(), "ollama");
    assert!(!format!("{:?}", OpenAiOptions::new(KEY)).contains(KEY));
}

// --- Anthropic ---------------------------------------------------------------

#[tokio::test]
async fn anthropic_sends_a_messages_request_and_reads_the_answer() {
    let server = RawServer::start(vec![Answer::json(
        200,
        json!({
            "id": "msg_9",
            "type": "message",
            "role": "assistant",
            "model": "claude-test-2026",
            "content": [{ "type": "text", "text": "Roll back v42." }],
            "stop_reason": "end_turn",
            "usage": { "input_tokens": 19, "output_tokens": 6 },
        }),
    )])
    .await;

    let response = anthropic(&server)
        .complete(ChatRequest::new(vec![
            Message::system("Be brief."),
            Message::system("Cite evidence."),
            Message::user("Fix?"),
        ]))
        .await
        .unwrap();

    assert_eq!(response.text(), "Roll back v42.");
    assert_eq!(response.finish_reason.as_deref(), Some("stop"));
    assert_eq!(response.usage, ModelUsage::new(Some(19), Some(6)));
    let request = &server.received()[0];
    assert_eq!(request.path, "/v1/messages");
    assert_eq!(request.header("x-api-key"), Some(KEY));
    assert_eq!(request.header("anthropic-version"), Some(ANTHROPIC_VERSION));
    assert_eq!(
        request.body,
        json!({
            "model": "claude-test",
            "max_tokens": 4096,
            "system": "Be brief.\n\nCite evidence.",
            "messages": [{ "role": "user", "content": [{ "type": "text", "text": "Fix?" }] }],
        })
    );
}

#[tokio::test]
async fn anthropic_streams_text_with_usage_from_both_ends() {
    let server = RawServer::start(vec![Answer::fixture("anthropic/stream-text.sse")]).await;

    let response = streamed(
        &anthropic(&server),
        ChatRequest::new(vec![Message::user("Fix?")]),
    )
    .await;

    assert_eq!(response.text(), "Roll back v42.");
    assert_eq!(response.finish_reason.as_deref(), Some("stop"));
    assert_eq!(response.usage, ModelUsage::new(Some(52), Some(6)));
    assert_eq!(response.id.as_deref(), Some("msg_1"));
    assert_eq!(server.received()[0].body["stream"], json!(true));
}

#[tokio::test]
async fn anthropic_streams_thinking_text_and_a_tool_use() {
    let server = RawServer::start(vec![Answer::fixture("anthropic/stream-tool-use.sse")]).await;

    let response = streamed(
        &anthropic(&server),
        ChatRequest::new(vec![Message::user("Look.")]),
    )
    .await;

    assert_eq!(response.finish_reason.as_deref(), Some("tool_calls"));
    assert_eq!(response.text(), "Looking.");
    assert_eq!(
        call_of(&response, 0),
        (
            "toolu_a".into(),
            "k8s_pods".into(),
            json!({ "namespace": "shop" })
        )
    );
    assert_eq!(
        response.message.content[0],
        Content::Reasoning {
            text: "Check the pods first.".into(),
            signature: Some("sig-abc".into())
        }
    );
    assert_eq!(response.usage, ModelUsage::new(Some(120), Some(44)));
}

#[tokio::test]
async fn anthropic_overloaded_is_transient_with_the_providers_hint() {
    let server = RawServer::start(vec![
        Answer::json(529, json!({ "type": "error", "error": { "type": "overloaded_error", "message": "Overloaded" } }))
            .with_header("retry-after", "3"),
    ])
    .await;

    let error = anthropic(&server)
        .complete(ChatRequest::new(vec![Message::user("x")]))
        .await
        .unwrap_err();

    assert_eq!(error.kind, ChatErrorKind::Transient);
    assert_eq!(error.status, Some(529));
    assert_eq!(error.retry_after, Some(Duration::from_secs(3)));
    assert!(error.message.ends_with("Overloaded"), "{}", error.message);
}

#[tokio::test]
async fn anthropic_an_error_event_mid_stream_is_classed_by_its_type() {
    let server = RawServer::start(vec![Answer::fixture("anthropic/stream-overloaded.sse")]).await;

    let mut stream = anthropic(&server)
        .stream(ChatRequest::new(vec![Message::user("x")]))
        .await
        .unwrap();
    let mut failure = None;
    while let Some(update) = stream.next().await {
        if let Err(error) = update {
            failure = Some(error);
        }
    }

    let failure = failure.unwrap();
    assert_eq!(failure.kind, ChatErrorKind::Transient);
    assert_eq!(failure.error_type, "529");
}

#[test]
fn anthropic_maps_tool_turns_formats_and_choices_to_the_wire() {
    let client = AnthropicChatClient::new(AnthropicOptions::new(KEY).with_model("claude-test"));
    let request = ChatRequest::new(vec![
        Message::system("Investigate."),
        Message::user("Look."),
        Message::new(
            Role::Assistant,
            vec![
                Content::Reasoning {
                    text: "Check the pods first.".into(),
                    signature: Some("sig-abc".into()),
                },
                Content::Reasoning {
                    text: "another provider's".into(),
                    signature: None,
                },
                Content::ToolCall {
                    id: "toolu_a".into(),
                    name: "k8s_pods".into(),
                    arguments: json!({ "namespace": "shop" }),
                },
            ],
        ),
        Message::tool_results([("toolu_a".to_string(), "[\"web-1\"]".to_string())]),
        Message::user("And the deploys?"),
    ])
    .with_tools(vec![ToolDeclaration::new(
        "k8s_pods",
        "Lists pods.",
        json!({ "type": "object" }),
    )])
    .with_tool_choice(ToolChoice::Required)
    .with_response_format(ResponseFormat::JsonSchema {
        name: "diagnosis".into(),
        schema: json!({ "type": "object" }),
        strict: true,
    })
    .with_max_tokens(512);

    let body = client.body(&request, false);

    assert_eq!(
        body["messages"],
        json!([
            { "role": "user", "content": [{ "type": "text", "text": "Look." }] },
            { "role": "assistant", "content": [
                { "type": "thinking", "thinking": "Check the pods first.", "signature": "sig-abc" },
                { "type": "tool_use", "id": "toolu_a", "name": "k8s_pods", "input": { "namespace": "shop" } },
            ] },
            { "role": "user", "content": [
                { "type": "tool_result", "tool_use_id": "toolu_a", "content": "[\"web-1\"]" },
                { "type": "text", "text": "And the deploys?" },
            ] },
        ])
    );
    assert_eq!(
        body["tools"],
        json!([{ "name": "k8s_pods", "description": "Lists pods.", "input_schema": { "type": "object" } }])
    );
    assert_eq!(body["tool_choice"], json!({ "type": "any" }));
    assert_eq!(body["max_tokens"], json!(512));
    let system = body["system"].as_str().unwrap();
    assert!(
        system.starts_with(
            "Investigate.\n\nAnswer with one JSON value that matches this JSON schema"
        )
    );
    assert!(system.ends_with("{\"type\":\"object\"}"));
    assert!(!format!("{:?}", AnthropicOptions::new(KEY)).contains(KEY));
}

#[test]
fn arguments_that_aren_t_an_object_are_kept_as_text_and_sent_back_safely() {
    let broken = "{\"namespace\": \"sh";
    let mut updates = ChatUpdates::new();
    updates.push(ChatUpdate::ToolCall {
        index: 0,
        id: Some("call_a".into()),
        name: Some("k8s_pods".into()),
        arguments: broken.into(),
    });
    let response = updates.finish().unwrap();
    assert_eq!(call_of(&response, 0).2, Value::String(broken.into()));

    let request = ChatRequest::new(vec![
        Message::user("Look."),
        response.message.clone(),
        Message::tool_results([("call_a".to_string(), "{\"error\":\"…\"}".to_string())]),
    ]);
    let openai =
        OpenAiChatClient::new(OpenAiOptions::new(KEY).with_model("gpt-test")).body(&request, false);
    assert_eq!(
        openai["messages"][1]["tool_calls"][0]["function"]["arguments"],
        json!(broken),
        "Chat Completions gets the model's own text back"
    );
    let anthropic = AnthropicChatClient::new(AnthropicOptions::new(KEY).with_model("claude-test"))
        .body(&request, false);
    assert_eq!(
        anthropic["messages"][1]["content"][0]["input"],
        json!({}),
        "Messages takes an object only"
    );
}
