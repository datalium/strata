//! A model call from a node (`design/44-LLM-Integration.md`, PR 1): streamed
//! under `run_streaming` and not otherwise, observed as `ModelCallStarted` /
//! `ModelCallCompleted` with the provider's usage, failed as the fault table
//! says, the hint reaching the retry. Mirrors the .NET suite's
//! `ModelCallTests.cs`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{MODEL, Scripted, ScriptedChatClient};
use datalium_strata::StrataError;
use datalium_strata::checkpoint::{CheckpointStore, GraphExecutionResult};
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{CompiledGraph, Jitter, NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::observability::{
    ModelCallInfo, ModelCallResult, ModelCallScope, RunEvent, RunEventChannel, RunEventKind,
};
use datalium_strata::streaming::StreamChunkType;
use datalium_strata_ai::{
    ChatClient, ChatError, ChatErrorKind, ChatRequest, Message, ModelNodeOptions, ModelUsage,
    StateGraphAiExt, complete, complete_with, is_transient_status, parse_retry_after,
};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

const PATIENCE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Job {
    prompt: String,
    answer: String,
}

fn job() -> Job {
    Job {
        prompt: "why?".to_string(),
        answer: String::new(),
    }
}

fn store() -> Arc<dyn CheckpointStore> {
    Arc::new(InMemoryCheckpointStore::new())
}

fn one_call(chat: Arc<dyn ChatClient>, policy: Option<RecoveryPolicy>) -> Arc<CompiledGraph<Job>> {
    let mut graph = StateGraph::<Job>::new().add_model_node(
        "Ask",
        chat,
        |job: &Job, _| vec![Message::user(&job.prompt)],
        |job: Job, response| Job {
            answer: response.text(),
            ..job
        },
        ModelNodeOptions::new().with_model("gpt-test"),
    );
    if let Some(policy) = policy {
        graph = graph.set_recovery_policy("Ask", policy);
    }
    Arc::new(graph.compile().unwrap())
}

fn observed() -> (RunConfig, mpsc::UnboundedReceiver<RunEvent>) {
    let (observer, events) = RunEventChannel::new();
    (RunConfig::new().with_observer(observer), events)
}

async fn drain(mut events: mpsc::UnboundedReceiver<RunEvent>) -> Vec<RunEvent> {
    let mut all = Vec::new();
    while let Some(event) = tokio::time::timeout(PATIENCE, events.recv()).await.unwrap() {
        all.push(event);
    }
    all
}

fn describe(event: &RunEvent) -> String {
    let path = event.node_path.as_deref().unwrap_or_default();
    let attempt = event.attempt.unwrap_or_default();
    match &event.kind {
        RunEventKind::ModelCallStarted {
            model, provider, ..
        } => format!(
            "ModelCallStarted {path}#{attempt} {model} {}",
            provider.as_deref().unwrap_or_default()
        ),
        RunEventKind::ModelCallCompleted {
            model,
            result,
            usage,
            finish_reason,
            ..
        } => match result {
            ModelCallResult::Succeeded => format!(
                "ModelCallCompleted {path}#{attempt} {model} {result:?} {}/{} {}",
                usage.input_tokens.unwrap_or_default(),
                usage.output_tokens.unwrap_or_default(),
                finish_reason.as_deref().unwrap_or_default()
            ),
            _ => format!("ModelCallCompleted {path}#{attempt} {model} {result:?}"),
        },
        RunEventKind::RunStarted { .. } => "RunStarted".to_string(),
        RunEventKind::NodeStarted => "NodeStarted".to_string(),
        RunEventKind::NodeCompleted { result, .. } => format!("NodeCompleted {path} {result:?}"),
        RunEventKind::RunCompleted { .. } => "RunCompleted".to_string(),
        other => format!("{other:?}")
            .split([' ', '{', '('])
            .next()
            .unwrap()
            .to_string(),
    }
}

#[tokio::test]
async fn a_call_outside_a_streaming_run_is_one_request_and_is_observed() {
    let chat = ScriptedChatClient::new(vec![Scripted::text(&["The cause ", "is a bad deploy."])]);
    let (config, events) = observed();

    let result = one_call(chat.clone(), None)
        .run_with("plain", job(), store(), config)
        .await;

    let GraphExecutionResult::Completed(state) = result else {
        panic!("expected completion")
    };
    assert_eq!(state.answer, "The cause is a bad deploy.");
    let requests = chat.requests();
    assert_eq!(requests.len(), 1);
    assert!(!requests[0].streamed);
    assert_eq!(requests[0].request.model.as_deref(), Some("gpt-test"));
    let events: Vec<String> = drain(events).await.iter().map(describe).collect();
    assert_eq!(
        events,
        vec![
            "RunStarted",
            "NodeStarted",
            "ModelCallStarted Ask#1 gpt-test scripted",
            "ModelCallCompleted Ask#1 gpt-test Succeeded 12/5 stop",
            "NodeCompleted Ask Committed",
            "RunCompleted",
        ]
    );
}

#[tokio::test]
async fn a_call_in_a_streaming_run_streams_its_text_and_reports_the_streamed_usage() {
    let chat = ScriptedChatClient::new(vec![
        Scripted::text(&["Roll ", "back ", "v42."]).with_usage(30, 3),
    ]);
    let (config, events) = observed();

    let (handle, mut chunks) =
        one_call(chat.clone(), None).run_streaming_with("streamed", job(), store(), config);
    let mut tokens = Vec::new();
    while let Some(chunk) = tokio::time::timeout(PATIENCE, chunks.recv()).await.unwrap() {
        if chunk.chunk_type == StreamChunkType::Token {
            assert_eq!(&*chunk.node_name, "Ask");
            tokens.push(chunk.content.to_string());
        }
    }

    let GraphExecutionResult::Completed(state) = handle.await.unwrap() else {
        panic!("expected completion")
    };
    assert_eq!(state.answer, "Roll back v42.");
    assert_eq!(tokens, vec!["Roll ", "back ", "v42."]);
    assert!(chat.requests()[0].streamed);
    let done = drain(events)
        .await
        .into_iter()
        .find_map(|e| match e.kind {
            RunEventKind::ModelCallCompleted {
                usage,
                finish_reason,
                operation,
                ..
            } => Some((usage, finish_reason, operation)),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        done,
        (
            ModelUsage::new(Some(30), Some(3)),
            Some("stop".to_string()),
            "chat".to_string()
        )
    );
}

#[tokio::test]
async fn a_rate_limit_is_transient_and_its_hint_is_the_retry_delay() {
    let chat = ScriptedChatClient::new(vec![
        Scripted::fails(ChatError::status(
            429,
            Some(Duration::from_millis(40)),
            "429 Too Many Requests",
        )),
        Scripted::text(&["ok"]),
    ]);
    let (config, events) = observed();
    let policy = RecoveryPolicy::retry(2).with_backoff(
        Duration::from_millis(1),
        Duration::from_secs(5),
        Jitter::Full,
    );

    let result = one_call(chat, Some(policy))
        .run_with("limited", job(), store(), config)
        .await;

    assert!(matches!(result, GraphExecutionResult::Completed(_)));
    let events = drain(events).await;
    let delay = events
        .iter()
        .find_map(|e| match &e.kind {
            RunEventKind::RetryScheduled { delay, .. } => Some(*delay),
            _ => None,
        })
        .unwrap();
    assert_eq!(delay, Duration::from_millis(40));
    let results: Vec<(ModelCallResult, Option<String>)> = events
        .iter()
        .filter_map(|e| match &e.kind {
            RunEventKind::ModelCallCompleted { result, error, .. } => {
                Some((*result, error.clone()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, ModelCallResult::Failed);
    assert!(results[0].1.as_deref().unwrap().contains("429"));
    assert_eq!(results[1].0, ModelCallResult::Succeeded);
}

#[tokio::test]
async fn a_bad_request_is_fatal_and_is_not_retried() {
    let chat = ScriptedChatClient::new(vec![
        Scripted::fails(ChatError::status(
            400,
            None,
            "400 Bad Request: no such model",
        )),
        Scripted::text(&["never"]),
    ]);

    let result = one_call(chat.clone(), Some(RecoveryPolicy::retry(3)))
        .run("bad", job(), store())
        .await;

    let GraphExecutionResult::Failed(StrataError::NodeFailed {
        attempts, message, ..
    }) = result
    else {
        panic!("expected a node failure")
    };
    assert_eq!(attempts, 1);
    assert!(message.contains("model call failed (400)"), "{message}");
    assert_eq!(chat.requests().len(), 1);
}

#[test]
fn each_status_is_classed_as_the_fault_table_says() {
    for status in [408, 409, 429, 500, 503, 529] {
        assert!(is_transient_status(status), "{status}");
        assert_eq!(
            ChatError::status(status, None, "x").kind,
            ChatErrorKind::Transient
        );
    }
    for status in [400, 401, 403, 404, 413, 422] {
        assert!(!is_transient_status(status), "{status}");
        assert_eq!(
            ChatError::status(status, None, "x").kind,
            ChatErrorKind::Fatal
        );
    }
}

#[test]
fn a_chat_error_becomes_the_node_error_design_25_reads() {
    let hinted: NodeError =
        ChatError::status(429, Some(Duration::from_secs(2)), "slow down").into();
    let plain: NodeError = ChatError::transient("timeout", "read timed out").into();
    let fatal: NodeError = ChatError::status(401, None, "bad key").into();

    assert_eq!(hinted.retry_after(), Some(Duration::from_secs(2)));
    assert!(hinted.is_transient());
    assert!(plain.is_transient());
    assert_eq!(plain.retry_after(), None);
    assert!(!fatal.is_transient());
    assert_eq!(fatal.message(), "model call failed (401): bad key");
}

#[test]
fn retry_after_reads_milliseconds_then_seconds_then_an_http_date() {
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-21T12:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let ms = |m| Some(Duration::from_millis(m));

    assert_eq!(parse_retry_after(Some("1500"), None, now), ms(1500));
    assert_eq!(parse_retry_after(None, Some("2"), now), ms(2000));
    assert_eq!(parse_retry_after(None, Some("0.5"), now), ms(500));
    assert_eq!(parse_retry_after(Some("250"), Some("9"), now), ms(250));
    assert_eq!(
        parse_retry_after(None, Some("Wed, 21 Oct 2026 12:00:30 GMT"), now),
        ms(30_000)
    );
    assert_eq!(
        parse_retry_after(None, Some("Wed, 21 Oct 2026 11:00:00 GMT"), now),
        Some(Duration::ZERO)
    );
    assert_eq!(parse_retry_after(None, Some("soon"), now), None);
    assert_eq!(parse_retry_after(None, None, now), None);
    assert_eq!(parse_retry_after(None, Some("-3"), now), None);
}

#[tokio::test]
async fn a_stream_that_breaks_after_it_started_is_retried_and_the_stream_says_so() {
    let chat = ScriptedChatClient::new(vec![
        Scripted::text(&["Half ", "never"])
            .failing_after(1, ChatError::fatal("invalid_response", "socket closed")),
        Scripted::text(&["Whole answer."]),
    ]);

    let (handle, mut chunks) =
        one_call(chat, Some(RecoveryPolicy::retry(1))).run_streaming("broken", job(), store());
    let mut seen = Vec::new();
    while let Some(chunk) = tokio::time::timeout(PATIENCE, chunks.recv()).await.unwrap() {
        match chunk.chunk_type {
            StreamChunkType::Token => seen.push(format!("Token:{}", chunk.content)),
            StreamChunkType::Retry => seen.push("Retry:".to_string()),
            _ => {}
        }
    }

    let GraphExecutionResult::Completed(state) = handle.await.unwrap() else {
        panic!("expected completion")
    };
    assert_eq!(state.answer, "Whole answer.");
    assert_eq!(seen, vec!["Token:Half ", "Retry:", "Token:Whole answer."]);
}

#[tokio::test]
async fn a_cancelled_run_records_the_call_cancelled() {
    let chat = ScriptedChatClient::new(vec![Scripted::hanging()]);
    let (observer, events) = RunEventChannel::new();
    let token = datalium_strata::CancellationToken::new();
    let config = RunConfig::new()
        .with_observer(observer)
        .with_cancellation(token.clone());

    let graph = one_call(chat.clone(), None);
    let run = tokio::spawn(async move { graph.run_with("cancel", job(), store(), config).await });
    while chat.requests().is_empty() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    token.cancel();
    let result = tokio::time::timeout(PATIENCE, run).await.unwrap().unwrap();

    assert!(matches!(result, GraphExecutionResult::Cancelled { .. }));
    let done = drain(events)
        .await
        .into_iter()
        .find_map(|e| match e.kind {
            RunEventKind::ModelCallCompleted { result, usage, .. } => Some((result, usage)),
            _ => None,
        })
        .unwrap();
    assert_eq!(done, (ModelCallResult::Cancelled, ModelUsage::default()));
}

#[tokio::test]
async fn a_call_outside_a_run_answers_without_streaming() {
    let chat = ScriptedChatClient::new(vec![Scripted::text(&["standalone"])]);

    let response = complete_with(
        &*chat,
        ChatRequest::new(vec![Message::user("hi")]).with_model("outside-model"),
        None,
        &Default::default(),
    )
    .await
    .unwrap();

    assert_eq!(response.text(), "standalone");
    assert_eq!(response.model.as_deref(), Some("outside-model"));
    assert!(!chat.requests()[0].streamed);
}

#[tokio::test]
async fn a_blank_request_model_falls_back_to_the_client_s() {
    let chat = ScriptedChatClient::new(vec![Scripted::text(&["ok"])]);
    let client: Arc<dyn ChatClient> = chat.clone();
    let graph = StateGraph::<Job>::new()
        .add_async_fallible_node("Ask", move |job: Job, ctx| {
            let client = client.clone();
            async move {
                complete(
                    &*client,
                    ChatRequest::new(vec![Message::user(&job.prompt)]).with_model("  "),
                    &ctx,
                )
                .await?;
                Ok(job)
            }
        })
        .compile()
        .unwrap();
    let (config, events) = observed();

    graph.run_with("blank-model", job(), store(), config).await;

    let started = drain(events)
        .await
        .into_iter()
        .find_map(|e| match e.kind {
            RunEventKind::ModelCallStarted { model, .. } => Some(model),
            _ => None,
        })
        .unwrap();
    assert_eq!(started, MODEL);
}

#[tokio::test]
async fn a_node_can_make_the_call_itself() {
    let chat = ScriptedChatClient::new(vec![Scripted::text(&["direct"])]);
    let client: Arc<dyn ChatClient> = chat.clone();
    let graph = Arc::new(
        StateGraph::<Job>::new()
            .add_async_fallible_node("Ask", move |job: Job, ctx| {
                let client = client.clone();
                async move {
                    let response = complete(
                        &*client,
                        ChatRequest::new(vec![Message::user(&job.prompt)]),
                        &ctx,
                    )
                    .await?;
                    Ok(Job {
                        answer: response.text(),
                        ..job
                    })
                }
            })
            .compile()
            .unwrap(),
    );

    let result = graph.run("direct", job(), store()).await;

    let GraphExecutionResult::Completed(state) = result else {
        panic!("expected completion")
    };
    assert_eq!(state.answer, "direct");
    assert_eq!(
        chat.requests()[0].request.model,
        None,
        "the client's default answers"
    );
}

#[test]
fn the_model_node_exports_as_a_node_with_its_metadata() {
    let graph = StateGraph::<Job>::new()
        .add_model_node(
            "Summarize",
            ScriptedChatClient::new(Vec::new()),
            |job: &Job, _| vec![Message::user(&job.prompt)],
            |job: Job, _| job,
            ModelNodeOptions::new().with_metadata("agent", "summarizer"),
        )
        .compile()
        .unwrap();

    let definition = graph.to_definition();

    assert_eq!(definition.nodes.len(), 1);
    assert_eq!(definition.nodes[0].name, "Summarize");
    assert_eq!(
        definition.nodes[0]
            .metadata
            .as_ref()
            .and_then(|m| m.get("agent"))
            .map(String::as_str),
        Some("summarizer")
    );
}

#[test]
fn a_scope_records_its_outcome_once_and_needs_a_model() {
    let scope = ModelCallScope::start(None, ModelCallInfo::new("m"));
    scope.succeeded(ModelUsage::new(Some(1), Some(2)), Some("stop"), None, None);
    scope.failed("500", "late");
    scope.cancelled();
    assert_eq!(scope.call_id().len(), 36);
    drop(scope);

    let blank = std::panic::catch_unwind(|| ModelCallScope::start(None, ModelCallInfo::new(" ")));
    assert!(blank.is_err());
}

#[test]
fn usage_totals_only_what_was_reported() {
    assert_eq!(ModelUsage::default().total_tokens(), None);
    assert_eq!(ModelUsage::new(Some(3), None).total_tokens(), Some(3));
    assert_eq!(ModelUsage::new(Some(3), Some(4)).total_tokens(), Some(7));
    assert_eq!(MODEL, "scripted-model");
}
