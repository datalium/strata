//! Model routes (`design/46-Model-Routes.md`) beyond the shared cases
//! (`route_cases.rs`): a route inside a run (a model call per attempt, one
//! `ModelRouted`), cancellation mid-route, an agent and a model node over a
//! route, a streaming run that streams only the winner's tokens, and a route
//! outside a node. Mirrors the .NET suite's `ModelRouteTests.cs`.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{Scripted, ScriptedChatClient};
use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::context::RunConfig;
use datalium_strata::graph::StateGraph;
use datalium_strata::observability::{
    ModelCallResult, RunEvent, RunEventChannel, RunEventKind, ToolCallResult,
};
use datalium_strata::prelude::*;
use datalium_strata::streaming::StreamChunkType;
use datalium_strata::{BoxFuture, CancellationToken};
use datalium_strata_ai::{
    Agent, AgentOptions, AgentResult, CallOptions, ChatClient, ChatError, ChatRequest,
    ChatResponse, ChatStream, ChatUpdate, Message, ModelCandidate, ModelHealthState,
    ModelNodeOptions, ModelRoute, ModelRouteOutcome, ModelRouteResult, ModelUsage, StateGraphAiExt,
    complete_with,
};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

const PATIENCE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Job {
    answer: String,
}

fn store() -> Arc<InMemoryCheckpointStore> {
    Arc::new(InMemoryCheckpointStore::new())
}

fn model_node(route: ModelRoute) -> Arc<datalium_strata::graph::CompiledGraph<Job>> {
    Arc::new(
        StateGraph::<Job>::new()
            .add_model_node(
                "Ask",
                Arc::new(route),
                |_: &Job, _| vec![Message::user("why?")],
                |_: Job, response| Job {
                    answer: response.text(),
                },
                ModelNodeOptions::new().with_model("requested"),
            )
            .compile()
            .unwrap(),
    )
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

/// One `ModelRouted`: who answered, and each candidate's outcome.
type Routed = (Option<String>, Vec<(String, ModelRouteOutcome)>);

fn routed(events: &[RunEvent]) -> Vec<Routed> {
    events
        .iter()
        .filter_map(|event| match &event.kind {
            RunEventKind::ModelRouted {
                answered, attempts, ..
            } => Some((
                answered.clone(),
                attempts
                    .iter()
                    .map(|a| (a.candidate.clone(), a.outcome))
                    .collect(),
            )),
            _ => None,
        })
        .collect()
}

/// A client that streams `chunks` after `delay` (the first update; the rest at
/// once), or answers them whole after it.
struct Slow {
    provider: &'static str,
    delay: Duration,
    chunks: Vec<&'static str>,
}

impl Slow {
    fn new(provider: &'static str, delay: Duration, chunks: &[&'static str]) -> Arc<Self> {
        Arc::new(Self {
            provider,
            delay,
            chunks: chunks.to_vec(),
        })
    }
}

impl ChatClient for Slow {
    fn provider(&self) -> &str {
        self.provider
    }

    fn complete(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatResponse, ChatError>> {
        Box::pin(async move {
            tokio::time::sleep(self.delay).await;
            let mut updates = datalium_strata_ai::ChatUpdates::new();
            updates.push(ChatUpdate::Text(self.chunks.concat()));
            updates.push(ChatUpdate::Meta {
                model: request.model,
                id: None,
            });
            updates.finish()
        })
    }

    fn stream(&self, _: ChatRequest) -> BoxFuture<'_, Result<ChatStream, ChatError>> {
        let delay = self.delay;
        let mut items: Vec<Result<ChatUpdate, ChatError>> = self
            .chunks
            .iter()
            .map(|c| Ok(ChatUpdate::Text(c.to_string())))
            .collect();
        items.push(Ok(ChatUpdate::Usage(ModelUsage::new(Some(1), Some(1)))));
        items.push(Ok(ChatUpdate::Finish("stop".to_string())));
        Box::pin(async move {
            let stream: ChatStream = Box::pin(futures_util::stream::unfold(
                (items.into_iter(), Some(delay)),
                |(mut items, delay)| async move {
                    if let Some(delay) = delay {
                        tokio::time::sleep(delay).await;
                    }
                    Some((items.next()?, (items, None)))
                },
            ));
            Ok(stream)
        })
    }
}

fn overloaded() -> ChatError {
    ChatError::status(503, None, "overloaded")
}

#[tokio::test]
async fn a_route_in_a_run_is_a_model_call_per_attempt_and_one_model_routed() {
    let a = ScriptedChatClient::new(vec![Scripted::fails(overloaded())]);
    let b = ScriptedChatClient::new(vec![Scripted::text(&["the answer"]).with_usage(7, 3)]);
    let route = ModelRoute::new("rca")
        .candidate(
            ModelCandidate::new("a", a)
                .with_model("m-a")
                .with_health_key("ka"),
        )
        .candidate(
            ModelCandidate::new("b", b)
                .with_model("m-b")
                .with_health_key("kb"),
        );
    let (config, events) = observed();

    let result = model_node(route)
        .run_with("routed", Job::default(), store(), config)
        .await;

    let GraphExecutionResult::Completed(job) = result else {
        panic!("expected completion")
    };
    assert_eq!(job.answer, "the answer");
    let events = drain(events).await;
    let model_events: Vec<String> = events
        .iter()
        .filter_map(|event| {
            let path = event.node_path.as_deref().unwrap_or_default();
            match &event.kind {
                RunEventKind::ModelCallStarted {
                    model,
                    provider,
                    route,
                    ..
                } => Some(format!(
                    "Started {path} {model} {} {}",
                    provider.as_deref().unwrap_or_default(),
                    route.as_deref().unwrap_or_default()
                )),
                RunEventKind::ModelCallCompleted {
                    model,
                    result,
                    usage,
                    route,
                    ..
                } => Some(format!(
                    "Completed {path} {model} {result:?} {} {}",
                    usage.input_tokens.unwrap_or_default(),
                    route.as_deref().unwrap_or_default()
                )),
                RunEventKind::ModelRouted {
                    route,
                    answered,
                    model,
                    hedges,
                    ..
                } => Some(format!(
                    "Routed {path} {route} {} {} {hedges}",
                    answered.as_deref().unwrap_or_default(),
                    model.as_deref().unwrap_or_default()
                )),
                _ => None,
            }
        })
        .collect();
    assert_eq!(
        model_events,
        [
            "Started Ask m-a scripted rca",
            "Completed Ask m-a Failed 0 rca",
            "Started Ask m-b scripted rca",
            "Completed Ask m-b Succeeded 7 rca",
            "Routed Ask rca b m-b 0",
        ]
    );
    // Each attempt names its model call.
    let started: Vec<String> = events
        .iter()
        .filter_map(|event| match &event.kind {
            RunEventKind::ModelCallStarted { call_id, .. } => Some(call_id.clone()),
            _ => None,
        })
        .collect();
    let RunEventKind::ModelRouted {
        attempts, call_id, ..
    } = &events
        .iter()
        .find(|e| matches!(e.kind, RunEventKind::ModelRouted { .. }))
        .unwrap()
        .kind
    else {
        unreachable!()
    };
    let ids: Vec<String> = attempts.iter().filter_map(|a| a.call_id.clone()).collect();
    assert_eq!(ids, started);
    assert_eq!(attempts[0].reason.as_deref(), Some("503"));
    assert!(!started.contains(call_id));
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_run_mid_route_cancels_the_attempts_and_leaves_health_alone() {
    let a = ScriptedChatClient::new(vec![Scripted::hanging()]);
    let b = ScriptedChatClient::new(vec![Scripted::hanging()]);
    let c = ScriptedChatClient::new(vec![]);
    let seen = Arc::new(Mutex::new(Vec::<ModelRouteResult>::new()));
    let route = ModelRoute::new("r")
        .candidate(ModelCandidate::new("a", a).with_health_key("ka"))
        .candidate(ModelCandidate::new("b", b).with_health_key("kb"))
        .candidate(ModelCandidate::new("c", c).with_health_key("kc"))
        .with_hedge_after(Duration::from_secs(1))
        .on_routed({
            let seen = seen.clone();
            move |result| seen.lock().unwrap().push(result.clone())
        });
    let health = route.health().clone();
    let token = CancellationToken::new();
    let (config, events) = observed();
    let graph = model_node(route);
    let run = tokio::spawn({
        let config = config.with_cancellation(token.clone());
        async move {
            graph
                .run_with("cancelled", Job::default(), store(), config)
                .await
        }
    });

    // a at 0, b hedged at 1 s, c due at 2 s.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    token.cancel();

    assert!(matches!(
        run.await.unwrap(),
        GraphExecutionResult::Cancelled { .. }
    ));
    let events = drain(events).await;
    assert_eq!(
        routed(&events),
        [(
            None,
            vec![
                ("a".to_string(), ModelRouteOutcome::Cancelled),
                ("b".to_string(), ModelRouteOutcome::Cancelled),
                ("c".to_string(), ModelRouteOutcome::NotTried),
            ]
        )]
    );
    let completed: Vec<ModelCallResult> = events
        .iter()
        .filter_map(|e| match &e.kind {
            RunEventKind::ModelCallCompleted { result, .. } => Some(*result),
            _ => None,
        })
        .collect();
    assert_eq!(
        completed,
        [ModelCallResult::Cancelled, ModelCallResult::Cancelled]
    );
    // ModelRouted comes after its attempts.
    let last_completed = events
        .iter()
        .rposition(|e| matches!(e.kind, RunEventKind::ModelCallCompleted { .. }))
        .unwrap();
    let routed_at = events
        .iter()
        .position(|e| matches!(e.kind, RunEventKind::ModelRouted { .. }))
        .unwrap();
    assert!(routed_at > last_completed);
    assert_eq!(seen.lock().unwrap()[0].hedges, 1);
    for entry in health.snapshot() {
        assert_eq!(entry.state, ModelHealthState::Available, "{}", entry.key);
        assert_eq!(entry.failures, 0, "{}", entry.key);
    }
}

#[derive(Deserialize, StrataSchema)]
struct PodsArgs {
    namespace: String,
}

#[strata_tool(name = "k8s.pods", description = "Lists a namespace's pods.")]
async fn k8s_pods(args: PodsArgs) -> Result<String, StrataError> {
    Ok(format!("3 pods in {}", args.namespace))
}

#[tokio::test]
async fn an_agent_over_a_route_fails_over_on_its_second_turn() {
    let a = ScriptedChatClient::new(vec![
        Scripted::tools(&[("1", "k8s_pods", r#"{"namespace":"prod"}"#)]),
        Scripted::fails(overloaded()),
    ]);
    let b = ScriptedChatClient::new(vec![Scripted::text(&["a bad deploy"])]);
    let route = ModelRoute::new("rca")
        .candidate(ModelCandidate::new("a", a).with_health_key("ka"))
        .candidate(ModelCandidate::new("b", b.clone()).with_health_key("kb"));
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(K8sPodsTool));
    let agent = Arc::new(Agent::<String>::new(
        Arc::new(route),
        Arc::new(registry),
        AgentOptions::new("analyst"),
    ));
    let graph = StateGraph::<Job>::new()
        .add_agent_node(
            "Investigate",
            agent,
            |_: &Job, _| vec![Message::user("Why?")],
            |_: Job, result: AgentResult<String>| Job {
                answer: result.output,
            },
        )
        .compile()
        .unwrap();
    let (config, events) = observed();

    let result = graph
        .run_with("agent", Job::default(), store(), config)
        .await;

    let GraphExecutionResult::Completed(job) = result else {
        panic!("expected completion")
    };
    assert_eq!(job.answer, "a bad deploy");
    // The second turn carries the tool's result to the candidate that answered.
    let second = &b.requests()[0].request;
    assert!(
        second
            .messages
            .iter()
            .any(|m| m.content.iter().any(|c| matches!(
                c,
                datalium_strata_ai::Content::ToolResult { content, .. } if content == "3 pods in prod"
            )))
    );
    let events = drain(events).await;
    assert_eq!(
        routed(&events),
        [
            (
                Some("a".to_string()),
                vec![
                    ("a".to_string(), ModelRouteOutcome::Answered),
                    ("b".to_string(), ModelRouteOutcome::NotTried),
                ]
            ),
            (
                Some("b".to_string()),
                vec![
                    ("a".to_string(), ModelRouteOutcome::Failed),
                    ("b".to_string(), ModelRouteOutcome::Answered),
                ]
            ),
        ]
    );
    let tools: Vec<ToolCallResult> = events
        .iter()
        .filter_map(|e| match &e.kind {
            RunEventKind::ToolCallCompleted { result, .. } => Some(*result),
            _ => None,
        })
        .collect();
    assert_eq!(tools, [ToolCallResult::Succeeded]);
}

#[tokio::test]
async fn a_model_node_over_a_route_fails_over_and_a_retry_skips_a_cooling_candidate() {
    use datalium_strata::graph::RecoveryPolicy;

    let a = ScriptedChatClient::new(vec![Scripted::fails(ChatError::status(
        429,
        Some(Duration::from_secs(60)),
        "slow down",
    ))]);
    let b = ScriptedChatClient::new(vec![
        Scripted::fails(overloaded()),
        Scripted::text(&["second time lucky"]),
    ]);
    let route = ModelRoute::new("r")
        .candidate(ModelCandidate::new("a", a.clone()).with_health_key("ka"))
        .candidate(ModelCandidate::new("b", b).with_health_key("kb"));
    let graph = StateGraph::<Job>::new()
        .add_model_node(
            "Ask",
            Arc::new(route),
            |_: &Job, _| vec![Message::user("why?")],
            |_: Job, response| Job {
                answer: response.text(),
            },
            ModelNodeOptions::new(),
        )
        .set_recovery_policy("Ask", RecoveryPolicy::retry(1))
        .compile()
        .unwrap();
    let (config, events) = observed();

    let result = graph
        .run_with("node", Job::default(), store(), config)
        .await;

    let GraphExecutionResult::Completed(job) = result else {
        panic!("expected completion")
    };
    assert_eq!(job.answer, "second time lucky");
    // The retry didn't ask the candidate still cooling down.
    assert_eq!(a.requests().len(), 1);
    let events = drain(events).await;
    assert_eq!(
        routed(&events),
        [
            (
                None,
                vec![
                    ("a".to_string(), ModelRouteOutcome::Failed),
                    ("b".to_string(), ModelRouteOutcome::Failed),
                ]
            ),
            (
                Some("b".to_string()),
                vec![
                    ("a".to_string(), ModelRouteOutcome::CoolingDown),
                    ("b".to_string(), ModelRouteOutcome::Answered),
                ]
            ),
        ]
    );
    let retry = events
        .iter()
        .find_map(|e| match &e.kind {
            RunEventKind::RetryScheduled { error, .. } => Some(error.clone()),
            _ => None,
        })
        .unwrap();
    assert!(
        retry.contains("no candidate of route 'r' answered: a failed (429), b failed (503)"),
        "{retry}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_streaming_run_through_a_route_streams_only_the_winner_s_tokens() {
    let route = ModelRoute::new("r")
        .candidate(ModelCandidate::new(
            "a",
            Slow::new("pa", Duration::from_secs(30), &["slow ", "loser"]),
        ))
        .candidate(ModelCandidate::new(
            "b",
            Slow::new("pb", Duration::from_secs(1), &["fast ", "winner"]),
        ))
        .with_hedge_after(Duration::from_secs(20));
    let (config, events) = observed();

    let (handle, mut chunks) =
        model_node(route).run_streaming_with("streamed", Job::default(), store(), config);
    let mut tokens = Vec::new();
    while let Some(chunk) = tokio::time::timeout(PATIENCE * 10, chunks.recv())
        .await
        .unwrap()
    {
        if chunk.chunk_type == StreamChunkType::Token {
            assert_eq!(&*chunk.node_name, "Ask");
            tokens.push(chunk.content.to_string());
        }
    }

    let GraphExecutionResult::Completed(job) = handle.await.unwrap() else {
        panic!("expected completion")
    };
    assert_eq!(job.answer, "fast winner");
    assert_eq!(tokens, ["fast ", "winner"]);
    let events = drain(events).await;
    assert_eq!(
        routed(&events),
        [(
            Some("b".to_string()),
            vec![
                ("a".to_string(), ModelRouteOutcome::LostRace),
                ("b".to_string(), ModelRouteOutcome::Answered),
            ]
        )]
    );
}

#[tokio::test]
async fn a_route_outside_a_node_still_routes_and_tells_on_routed() {
    let a = ScriptedChatClient::new(vec![Scripted::text(&["no"]).with_finish("content_filter")]);
    let b = ScriptedChatClient::new(vec![Scripted::text(&["yes"])]);
    let seen = Arc::new(Mutex::new(Vec::<ModelRouteResult>::new()));
    let route = ModelRoute::new("r")
        .candidate(ModelCandidate::new("a", a).with_health_key("ka"))
        .candidate(ModelCandidate::new("b", b).with_health_key("kb"))
        .on_routed({
            let seen = seen.clone();
            move |result| seen.lock().unwrap().push(result.clone())
        });

    let response = complete_with(
        &route,
        ChatRequest::new(vec![Message::user("?")]),
        None,
        &CallOptions::default(),
    )
    .await
    .unwrap();

    assert_eq!(response.text(), "yes");
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].answered.as_deref(), Some("b"));
    assert_eq!(seen[0].attempts[0].outcome, ModelRouteOutcome::Refused);
    assert_eq!(seen[0].model.as_deref(), Some(common::MODEL));
}

#[tokio::test]
async fn a_panicking_on_routed_is_ignored() {
    let a = ScriptedChatClient::new(vec![Scripted::text(&["fine"])]);
    let route = ModelRoute::new("r")
        .candidate(ModelCandidate::new("a", a))
        .on_routed(|_| panic!("a ledger that fails"));

    let response = route
        .complete(ChatRequest::new(vec![Message::user("?")]))
        .await
        .unwrap();

    assert_eq!(response.text(), "fine");
}

#[tokio::test(start_paused = true)]
async fn a_panic_judging_a_half_open_test_gives_the_test_back() {
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;

    let a = ScriptedChatClient::new(vec![
        Scripted::fails(ChatError::transient("connection", "connection refused")),
        Scripted::text(&["PANIC"]),
        Scripted::text(&["ok"]),
    ]);
    let health = datalium_strata_ai::ModelHealth::new(
        datalium_strata_ai::ModelHealthOptions::new().with_breaker(1, Duration::from_secs(10)),
    );
    let route = ModelRoute::new("r")
        .candidate(ModelCandidate::new("a", a).with_health_key("ka"))
        .with_health(health.clone())
        .with_accept(|response| {
            assert_ne!(response.text(), "PANIC", "an accept that fails");
            Ok(())
        });
    let ask = || route.complete(ChatRequest::new(vec![Message::user("?")]));

    assert!(ask().await.is_err());
    assert!(matches!(
        health.snapshot()[0].state,
        ModelHealthState::Open { .. }
    ));
    tokio::time::sleep(Duration::from_secs(11)).await;
    // The half-open test call's answer makes `accept` panic.
    assert!(AssertUnwindSafe(ask()).catch_unwind().await.is_err());

    // The test call was given back: the key is testable again.
    assert_eq!(health.snapshot()[0].state, ModelHealthState::HalfOpen);
    assert_eq!(ask().await.unwrap().text(), "ok");
    assert_eq!(health.snapshot()[0].state, ModelHealthState::Available);
}

#[tokio::test(start_paused = true)]
async fn a_loser_whose_answer_had_arrived_records_it() {
    // a answers at 25 s; b, hedged at 20 s, answers at 25 s too: whichever
    // the race picks, the other's answer had arrived.
    let route = ModelRoute::new("r")
        .candidate(ModelCandidate::new(
            "a",
            Slow::new("pa", Duration::from_secs(25), &["a"]),
        ))
        .candidate(ModelCandidate::new(
            "b",
            Slow::new("pb", Duration::from_secs(5), &["b"]),
        ))
        .with_hedge_after(Duration::from_secs(20));
    let (config, events) = observed();

    let result = model_node(route)
        .run_with("tie", Job::default(), store(), config)
        .await;

    assert!(matches!(result, GraphExecutionResult::Completed(_)));
    let events = drain(events).await;
    let mut outcomes: Vec<ModelRouteOutcome> =
        routed(&events)[0].1.iter().map(|(_, o)| *o).collect();
    outcomes.sort_by_key(|o| o.as_str());
    assert_eq!(
        outcomes,
        [ModelRouteOutcome::Answered, ModelRouteOutcome::LostRace]
    );
    let completed: Vec<ModelCallResult> = events
        .iter()
        .filter_map(|e| match &e.kind {
            RunEventKind::ModelCallCompleted { result, .. } => Some(*result),
            _ => None,
        })
        .collect();
    assert_eq!(
        completed,
        [ModelCallResult::Succeeded, ModelCallResult::Succeeded]
    );
}

#[tokio::test]
async fn a_stream_broken_after_its_first_update_is_recorded_stream_broken() {
    let a = ScriptedChatClient::new(vec![
        Scripted::text(&["Hel", "lo"]).failing_after(1, ChatError::status(500, None, "broken")),
    ]);
    let seen = Arc::new(Mutex::new(Vec::<ModelRouteResult>::new()));
    let route = ModelRoute::new("r")
        .candidate(ModelCandidate::new("a", a).with_health_key("ka"))
        .on_routed({
            let seen = seen.clone();
            move |result| seen.lock().unwrap().push(result.clone())
        });

    let mut stream = route
        .stream(ChatRequest::new(vec![Message::user("?")]))
        .await
        .unwrap();
    let mut error = None;
    while let Some(update) = futures_util::StreamExt::next(&mut stream).await {
        if let Err(e) = update {
            error = Some(e);
        }
    }

    // The caller gets the candidate's own error; the route records the break.
    assert_eq!(error.unwrap().error_type, "500");
    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].attempts[0].outcome, ModelRouteOutcome::Failed);
    assert_eq!(seen[0].attempts[0].reason.as_deref(), Some("stream_broken"));
    assert_eq!(route.health().snapshot()[0].failures, 1);
}

#[tokio::test(start_paused = true)]
async fn a_node_stream_that_refuses_a_token_fails_the_winning_attempt() {
    let route = ModelRoute::new("r").candidate(ModelCandidate::new(
        "a",
        Slow::new("pa", Duration::from_secs(1), &["never ", "delivered"]),
    ));
    let health = route.health().clone();
    let (config, events) = observed();

    let (handle, chunks) =
        model_node(route).run_streaming_with("closed", Job::default(), store(), config);
    // Nobody reads the run's stream any more.
    drop(chunks);

    assert!(matches!(
        handle.await.unwrap(),
        GraphExecutionResult::Failed(_)
    ));
    let events = drain(events).await;
    assert_eq!(
        routed(&events),
        [(None, vec![("a".to_string(), ModelRouteOutcome::Failed)])]
    );
    let completed: Vec<(ModelCallResult, Option<String>)> = events
        .iter()
        .filter_map(|e| match &e.kind {
            RunEventKind::ModelCallCompleted { result, error, .. } => {
                Some((*result, error.clone()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].0, ModelCallResult::Failed);
    // Not the provider's fault: its health is untouched.
    assert_eq!(health.snapshot()[0].failures, 0);
}

#[tokio::test(start_paused = true)]
async fn a_huge_retry_after_cools_a_key_down_for_a_day_at_most() {
    let a = ScriptedChatClient::new(vec![Scripted::fails(ChatError::status(
        429,
        Some(Duration::MAX),
        "come back never",
    ))]);
    let route = ModelRoute::new("r").candidate(ModelCandidate::new("a", a).with_health_key("ka"));
    let start = tokio::time::Instant::now();

    let error = route
        .complete(ChatRequest::new(vec![Message::user("?")]))
        .await
        .unwrap_err();

    assert_eq!(
        error.retry_after,
        Some(datalium_strata_ai::route::MAX_COOLDOWN)
    );
    let ModelHealthState::CoolingDown { until } = route.health().snapshot()[0].state else {
        panic!("expected a cooldown")
    };
    assert_eq!(until - start, Duration::from_secs(24 * 60 * 60));
}
