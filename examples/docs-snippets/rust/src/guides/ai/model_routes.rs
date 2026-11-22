//! Samples for <https://stratamesh.dev/guides/ai/model-routes/>.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{CompiledGraph, StateGraph};
use datalium_strata::observability::{RunEvent, RunEventChannel, RunEventKind};
use datalium_strata::prelude::*;
use datalium_strata::store::CheckpointStore;
use datalium_strata::{BoxFuture, StrataError};
use datalium_strata_ai::{
    Agent, AgentOptions, AgentResult, ChatClient, ChatError, ChatRequest, ChatResponse, ChatStream,
    Message, ModelHealth, ModelHealthOptions, ModelHealthState, ModelNodeOptions, ModelRouteResult,
    ModelUsage, StateGraphAiExt,
};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};

thread_local! {
    static PRINTED: RefCell<String> = const { RefCell::new(String::new()) };
}

// The samples print with `println!`. In this file it writes to `PRINTED`, so a
// test can check what a sample prints. A `#[tokio::test]` runs on one thread.
macro_rules! println {
    ($($arg:tt)*) => {
        PRINTED.with(|printed| {
            writeln!(printed.borrow_mut(), $($arg)*).expect("a String takes any text")
        })
    };
}

/// What the samples on this thread printed so far, and empties it.
fn printed() -> String {
    PRINTED.with(|printed| printed.take())
}

const ROLL_BACK: &str = "Roll back deploy 412, because the error rate rose with it.";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Ticket {
    question: String,
    answer: String,
    summary: String,
}

fn ticket(question: &str) -> Ticket {
    Ticket {
        question: question.to_string(),
        ..Ticket::default()
    }
}

#[derive(Debug, Deserialize, StrataSchema)]
struct RootCause {
    cause: String,
    action: String,
}

// docs:begin guides/ai/model-routes/build-route
use datalium_strata_ai::{ModelCandidate, ModelRoute};

// docs:hide
#[tokio::test]
async fn fail_over_and_see_which_candidate_answered() {
    // Scripted to fail with a 429, refuse, give an answer without a reason,
    // and answer well.
    let northwind = FakeModel::new(
        "northwind",
        "nw-large",
        vec![Turn::fail(429, Some(Duration::from_secs(30)), None)],
    );
    let contoso = FakeModel::new(
        "contoso",
        "ct-guarded",
        vec![Turn::refuse("I can't help with production systems.")],
    );
    let fabrikam = FakeModel::new("fabrikam", "fb-mini", vec![Turn::answer("Restart it.")]);
    let local = FakeModel::new("local", "llama-local", vec![Turn::answer(ROLL_BACK)]);
    let asked = [
        northwind.clone(),
        contoso.clone(),
        fabrikam.clone(),
        local.clone(),
    ];

    // docs:show
    let triage = ModelRoute::new("triage")
        .candidate(ModelCandidate::new("primary", northwind).with_model("nw-large"))
        .candidate(ModelCandidate::new("careful", contoso).with_model("ct-guarded"))
        .candidate(ModelCandidate::new("terse", fabrikam).with_model("fb-mini"))
        .candidate(ModelCandidate::new("local", local).with_model("llama-local"))
        // Your rule for a usable answer: here, it must give a reason.
        .with_accept(|response| {
            if response.text().to_lowercase().contains("because") {
                Ok(())
            } else {
                Err("it gives no reason".to_string())
            }
        });
    // docs:end guides/ai/model-routes/build-route

    // An agent's route and tools, for the agent in the next sample.
    let analyst = FakeModel::new(
        "local",
        "llama-local",
        vec![Turn::answer(
            r#"{"cause":"deploy 412","action":"roll it back"}"#,
        )],
    );
    let route = ModelRoute::new("rca").candidate(ModelCandidate::new("local", analyst));
    let registry = Arc::new(ToolRegistry::new());

    // docs:begin guides/ai/model-routes/use-route
    let graph = StateGraph::<Ticket>::new()
        .add_model_node(
            "Ask",
            Arc::new(triage),
            |t: &Ticket, _| vec![Message::user(t.question.clone())],
            |t: Ticket, r: ChatResponse| Ticket {
                answer: r.text(),
                ..t
            },
            ModelNodeOptions::new(),
        )
        .compile()
        .expect("the graph is valid");

    // An agent takes a route the same way.
    let agent = Arc::new(Agent::<RootCause>::new(
        Arc::new(route),
        registry,
        AgentOptions::new("rca-analyst"),
    ));
    // docs:end guides/ai/model-routes/use-route

    // The agent's turn goes through its route.
    let analysis = StateGraph::<Ticket>::new()
        .add_agent_node(
            "Analyse",
            agent,
            |t: &Ticket, _| vec![Message::user(t.question.clone())],
            |t: Ticket, r: AgentResult<RootCause>| Ticket {
                answer: format!("{}: {}", r.output.cause, r.output.action),
                ..t
            },
        )
        .compile()
        .expect("the graph is valid")
        .run(
            "model-routes-rca",
            ticket("Why is checkout failing?"),
            Arc::new(InMemoryCheckpointStore::new()),
        )
        .await;
    let GraphExecutionResult::Completed(analysed) = analysis else {
        panic!("expected the agent's run to complete");
    };
    assert_eq!(analysed.answer, "deploy 412: roll it back");

    let thread = "model-routes-triage";
    let ticket = ticket("Why is checkout failing?");
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());

    // docs:begin guides/ai/model-routes/see-routed
    let (observer, mut events) = RunEventChannel::new();
    let result = graph
        .run_with(
            thread,
            ticket,
            store,
            RunConfig::new().with_observer(observer),
        )
        .await;

    while let Some(event) = events.recv().await {
        if let RunEventKind::ModelRouted {
            route,
            answered,
            model,
            attempts,
            hedges,
            ..
        } = &event.kind
        {
            println!(
                "route '{route}': answered by {} ({}), hedges {hedges}",
                answered.as_deref().unwrap_or("nobody"),
                model.as_deref().unwrap_or_default()
            );
            for attempt in attempts {
                let line = format!(
                    "  {:<9} {:<12} {}",
                    attempt.candidate,
                    attempt.outcome.as_str(),
                    attempt.reason.as_deref().unwrap_or_default()
                );
                println!("{}", line.trim_end());
            }
        }
    }
    // docs:end guides/ai/model-routes/see-routed

    // What the page says the run prints, with the outcomes in snake case.
    assert_eq!(
        printed(),
        "route 'triage': answered by local (llama-local), hedges 0\n\
         \x20 primary   failed       429\n\
         \x20 careful   refused\n\
         \x20 terse     rejected     it gives no reason\n\
         \x20 local     answered\n"
    );
    let GraphExecutionResult::Completed(answered) = result else {
        panic!("expected the run to complete");
    };
    assert_eq!(answered.answer, ROLL_BACK);

    // Each candidate was asked once, for the model named on it.
    let models: Vec<Vec<String>> = asked.iter().map(|m| m.asked_for()).collect();
    assert_eq!(
        models,
        [["nw-large"], ["ct-guarded"], ["fb-mini"], ["llama-local"]]
    );
}

#[tokio::test(start_paused = true)]
async fn hedge_a_slow_candidate() {
    // primary hangs until it is dropped. The paused clock moves as soon as
    // every task waits on a timer, so the 2 seconds pass at once.
    let northwind = FakeModel::new("northwind", "nw-large", vec![Turn::hang()]);
    let local = FakeModel::new("local", "llama-local", vec![Turn::answer(ROLL_BACK)]);

    // docs:begin guides/ai/model-routes/hedge
    let hedged = ModelRoute::new("hedged")
        .candidate(ModelCandidate::new("primary", northwind).with_model("nw-large"))
        .candidate(ModelCandidate::new("local", local).with_model("llama-local"))
        .with_hedge_after(Duration::from_secs(2));
    // docs:end guides/ai/model-routes/hedge

    let (result, events) = run(
        &ask_graph(hedged),
        "model-routes-hedged",
        ticket("Why is checkout failing?"),
        &[],
    )
    .await;

    assert!(matches!(result, GraphExecutionResult::Completed(t) if t.answer == ROLL_BACK));
    assert_eq!(
        outcomes(&events),
        ["primary:lost_race, local:answered (local, hedges 1)"]
    );
}

#[tokio::test]
async fn choose_candidates_per_request() {
    let northwind = FakeModel::new("northwind", "nw-large", vec![Turn::answer(ROLL_BACK)]);
    let local = FakeModel::new("local", "llama-local", vec![Turn::answer(ROLL_BACK)]);
    let asked = [northwind.clone(), local.clone()];

    // docs:begin guides/ai/model-routes/eligible
    let plans = ModelRoute::new("plans")
        .candidate(
            ModelCandidate::new("frontier", northwind)
                .with_model("nw-large")
                // A tenant rule: the run's metadata says the customer's plan.
                .with_eligible(|_, ctx| {
                    let plan = ctx.and_then(|ctx| ctx.metadata().get("plan"));
                    if plan.map(String::as_str) == Some("free") {
                        Err("the free plan doesn't use frontier models".to_string())
                    } else {
                        Ok(())
                    }
                }),
        )
        .candidate(
            ModelCandidate::new("local", local)
                .with_model("llama-local")
                // A context-size rule: the local model takes short prompts only.
                .with_eligible(|request, _| {
                    let length: usize = request.messages.iter().map(|m| m.text().len()).sum();
                    if length > 4_000 {
                        Err("the prompt is too long for its context".to_string())
                    } else {
                        Ok(())
                    }
                }),
        );
    // docs:end guides/ai/model-routes/eligible

    let graph = ask_graph(plans);
    let question = ticket("Why is checkout failing?");

    // The three runs on the page, with plan set in the run's metadata.
    let (paid, events) = run(
        &graph,
        "model-routes-plan-paid",
        question.clone(),
        &[("plan", "paid")],
    )
    .await;
    assert!(matches!(paid, GraphExecutionResult::Completed(_)));
    assert_eq!(
        outcomes(&events),
        ["frontier:answered, local:not_tried (frontier, hedges 0)"]
    );

    let (free, events) = run(
        &graph,
        "model-routes-plan-free",
        question,
        &[("plan", "free")],
    )
    .await;
    assert!(matches!(free, GraphExecutionResult::Completed(_)));
    assert_eq!(
        outcomes(&events),
        [
            "frontier:ineligible:the free plan doesn't use frontier models, local:answered (local, hedges 0)"
        ]
    );

    let (long, events) = run(
        &graph,
        "model-routes-plan-free-long",
        ticket(&"?".repeat(4_001)),
        &[("plan", "free")],
    )
    .await;
    assert_eq!(
        outcomes(&events),
        [
            "frontier:ineligible:the free plan doesn't use frontier models, local:ineligible:the prompt is too long for its context (nobody, hedges 0)"
        ]
    );
    let GraphExecutionResult::Failed(StrataError::NodeFailed {
        node,
        attempts,
        message,
    }) = long
    else {
        panic!("expected the node to fail");
    };
    assert_eq!((node.as_str(), attempts), ("Ask", 1));
    assert_eq!(
        message,
        "model call failed (route_ineligible): no candidate of route 'plans' can take this request: \
         frontier is ineligible (the free plan doesn't use frontier models), \
         local is ineligible (the prompt is too long for its context)"
    );
    let calls: Vec<usize> = asked.iter().map(|m| m.calls()).collect();
    assert_eq!(calls, [1, 1]);
}

#[tokio::test(start_paused = true)]
async fn share_health_between_routes() {
    // Two models of one provider, northwind, and a steady local model.
    let large = FakeModel::new(
        "northwind",
        "nw-large",
        vec![Turn::fail(503, None, None), Turn::answer(ROLL_BACK)],
    );
    let small = FakeModel::new(
        "northwind",
        "nw-small",
        vec![
            Turn::fail(503, None, None),
            Turn::answer("Deploy 412 broke checkout."),
        ],
    );
    let steady = FakeModel::new(
        "local",
        "llama-local",
        vec![
            Turn::answer(ROLL_BACK),
            Turn::answer("Deploy 412 broke checkout."),
            Turn::answer(ROLL_BACK),
            Turn::answer("Deploy 412 broke checkout."),
        ],
    );

    // docs:begin guides/ai/model-routes/health
    let health =
        ModelHealth::new(ModelHealthOptions::new().with_breaker(2, Duration::from_secs(60)));

    let diagnose = ModelRoute::new("diagnose")
        .candidate(ModelCandidate::new("large", large))
        .candidate(ModelCandidate::new("steady", steady.clone()))
        .with_health(health.clone());
    let summarise = ModelRoute::new("summarise")
        .candidate(ModelCandidate::new("small", small))
        .candidate(ModelCandidate::new("steady", steady))
        .with_health(health.clone());
    // docs:end guides/ai/model-routes/health

    // A graph runs diagnose, then summarise.
    let graph = StateGraph::<Ticket>::new()
        .add_model_node(
            "Diagnose",
            Arc::new(diagnose),
            |t: &Ticket, _| vec![Message::user(t.question.clone())],
            |t: Ticket, r: ChatResponse| Ticket {
                answer: r.text(),
                ..t
            },
            ModelNodeOptions::new(),
        )
        .add_model_node(
            "Summarise",
            Arc::new(summarise),
            |t: &Ticket, _| vec![Message::user(t.answer.clone())],
            |t: Ticket, r: ChatResponse| Ticket {
                summary: r.text(),
                ..t
            },
            ModelNodeOptions::new(),
        )
        .add_edge("Diagnose", "Summarise")
        .compile()
        .expect("the graph is valid");
    let question = ticket("Why is checkout failing?");

    // Run 1: northwind fails once in each route, two failures in a row on one
    // key, so the breaker opens.
    let (_, events) = run(&graph, "model-routes-health-1", question.clone(), &[]).await;
    assert_eq!(
        outcomes(&events),
        [
            "large:failed:503, steady:answered (steady, hedges 0)",
            "small:failed:503, steady:answered (steady, hedges 0)"
        ]
    );
    assert_eq!(state_of(&health, "northwind"), (true, false, 2));

    // Run 2 skips northwind in both routes without calling it.
    let (result, events) = run(&graph, "model-routes-health-2", question.clone(), &[]).await;
    assert_eq!(
        outcomes(&events),
        [
            "large:breaker_open:60 s, steady:answered (steady, hedges 0)",
            "small:breaker_open:60 s, steady:answered (steady, hedges 0)"
        ]
    );
    let GraphExecutionResult::Completed(answered) = result else {
        panic!("expected run 2 to complete");
    };
    assert_eq!(
        (answered.answer.as_str(), answered.summary.as_str()),
        (ROLL_BACK, "Deploy 412 broke checkout.")
    );

    // After 60 seconds the key is half-open, and run 3's test call succeeds
    // and closes it.
    tokio::time::advance(Duration::from_secs(60)).await;
    assert_eq!(state_of(&health, "northwind"), (false, true, 2));
    let (_, events) = run(&graph, "model-routes-health-3", question, &[]).await;
    assert_eq!(
        outcomes(&events),
        [
            "large:answered, steady:not_tried (large, hedges 0)",
            "small:answered, steady:not_tried (small, hedges 0)"
        ]
    );
    let northwind = health
        .snapshot()
        .into_iter()
        .find(|e| e.key == "northwind")
        .expect("northwind has health");
    assert_eq!(
        (northwind.state, northwind.failures),
        (ModelHealthState::Available, 0)
    );
}

#[tokio::test]
async fn call_a_route_directly() {
    // docs:begin guides/ai/model-routes/test-route
    let primary = FakeModel::new("northwind", "nw-large", vec![Turn::fail(503, None, None)]);
    let backup = FakeModel::new("local", "llama-local", vec![Turn::answer(ROLL_BACK)]);
    let routed: Arc<Mutex<Option<ModelRouteResult>>> = Arc::default();
    let seen = routed.clone();
    let route = ModelRoute::new("triage")
        .candidate(ModelCandidate::new("primary", primary))
        .candidate(ModelCandidate::new("backup", backup))
        .on_routed(move |result| *seen.lock().unwrap() = Some(result.clone()));

    let response = route
        .complete(ChatRequest::new(vec![Message::user(
            "Why is checkout failing?",
        )]))
        .await
        .expect("backup answers");

    println!("{}", response.text());
    let routed = routed.lock().unwrap().take().expect("on_routed was called");
    let outcomes: Vec<String> = routed
        .attempts
        .iter()
        .map(|a| format!("{}={}", a.candidate, a.outcome.as_str()))
        .collect();
    println!("{}", outcomes.join(", "));
    // docs:end guides/ai/model-routes/test-route

    assert_eq!(
        printed(),
        format!("{ROLL_BACK}\nprimary=failed, backup=answered\n")
    );
}

fn ask_graph(route: ModelRoute) -> CompiledGraph<Ticket> {
    StateGraph::<Ticket>::new()
        .add_model_node(
            "Ask",
            Arc::new(route),
            |t: &Ticket, _| vec![Message::user(t.question.clone())],
            |t: Ticket, r: ChatResponse| Ticket {
                answer: r.text(),
                ..t
            },
            ModelNodeOptions::new(),
        )
        .compile()
        .expect("the ask graph is valid")
}

/// Runs `graph` on a fresh store, and collects the run's events.
async fn run(
    graph: &CompiledGraph<Ticket>,
    thread: &str,
    ticket: Ticket,
    metadata: &[(&str, &str)],
) -> (GraphExecutionResult<Ticket>, Vec<RunEvent>) {
    let (observer, mut receiver) = RunEventChannel::new();
    let mut config = RunConfig::new().with_observer(observer);
    for (key, value) in metadata {
        config = config.with_metadata(*key, *value);
    }
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    let result = graph.run_with(thread, ticket, store, config).await;
    let mut events = Vec::new();
    while let Ok(Some(event)) = tokio::time::timeout(Duration::from_secs(10), receiver.recv()).await
    {
        events.push(event);
    }
    (result, events)
}

/// Each `ModelRouted` event as `candidate:outcome[:reason], ... (answered,
/// hedges n)`, the candidates in the route's order.
fn outcomes(events: &[RunEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match &event.kind {
            RunEventKind::ModelRouted {
                answered,
                attempts,
                hedges,
                ..
            } => {
                let attempts: Vec<String> = attempts
                    .iter()
                    .map(|a| match &a.reason {
                        Some(reason) => format!("{}:{}:{reason}", a.candidate, a.outcome.as_str()),
                        None => format!("{}:{}", a.candidate, a.outcome.as_str()),
                    })
                    .collect();
                Some(format!(
                    "{} ({}, hedges {hedges})",
                    attempts.join(", "),
                    answered.as_deref().unwrap_or("nobody")
                ))
            }
            _ => None,
        })
        .collect()
}

/// A key's health: (breaker open, half-open, failures in a row).
fn state_of(health: &ModelHealth, key: &str) -> (bool, bool, u32) {
    let entry = health
        .snapshot()
        .into_iter()
        .find(|e| e.key == key)
        .expect("the key has health");
    (
        matches!(entry.state, ModelHealthState::Open { .. }),
        entry.state == ModelHealthState::HalfOpen,
        entry.failures,
    )
}

/// One scripted turn of a [`FakeModel`].
#[derive(Default)]
struct Turn {
    text: String,
    /// Answer with finish reason `content_filter`.
    refused: bool,
    failure: Option<ChatError>,
    /// Wait until the call is dropped (a hedge's loser).
    hang: bool,
}

impl Turn {
    fn answer(text: &str) -> Self {
        Self {
            text: text.to_string(),
            ..Self::default()
        }
    }

    fn refuse(text: &str) -> Self {
        Self {
            refused: true,
            ..Self::answer(text)
        }
    }

    fn hang() -> Self {
        Self {
            hang: true,
            ..Self::default()
        }
    }

    // docs:begin guides/ai/model-routes/fake-fault
    fn fail(status: u16, retry_after: Option<Duration>, message: Option<&str>) -> Self {
        let message = message.map_or_else(|| format!("{status} from the provider"), str::to_string);
        Self {
            failure: Some(ChatError::status(status, retry_after, message)),
            ..Self::default()
        }
    }

    // docs:hide In `impl ChatClient for FakeModel`: the provider is the default health key.
}

/// A scripted stand-in for a provider's chat client: it plays its turns in
/// order and names its provider and its default model.
struct FakeModel {
    provider: &'static str,
    model: &'static str,
    script: Mutex<VecDeque<Turn>>,
    calls: AtomicUsize,
    asked_for: Mutex<Vec<String>>,
}

impl FakeModel {
    fn new(provider: &'static str, model: &'static str, script: Vec<Turn>) -> Arc<Self> {
        Arc::new(Self {
            provider,
            model,
            script: Mutex::new(script.into()),
            calls: AtomicUsize::new(0),
            asked_for: Mutex::default(),
        })
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    /// The model each call asked for.
    fn asked_for(&self) -> Vec<String> {
        self.asked_for.lock().unwrap().clone()
    }
}

impl ChatClient for FakeModel {
    // docs:show
    fn provider(&self) -> &str {
        self.provider
    }
    // docs:end guides/ai/model-routes/fake-fault

    fn default_model(&self) -> Option<&str> {
        Some(self.model)
    }

    fn complete(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatResponse, ChatError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let turn = self.script.lock().unwrap().pop_front();
        let model = request.model.unwrap_or_else(|| self.model.to_string());
        self.asked_for.lock().unwrap().push(model.clone());
        Box::pin(async move {
            let Some(turn) = turn else {
                return Err(ChatError::fatal(
                    "script_exhausted",
                    format!("{model} has no more scripted turns"),
                ));
            };
            if turn.hang {
                std::future::pending::<()>().await;
            }
            tokio::task::yield_now().await;
            if let Some(failure) = turn.failure {
                return Err(failure);
            }
            let finish = if turn.refused {
                "content_filter"
            } else {
                "stop"
            };
            Ok(ChatResponse {
                message: Message::assistant(turn.text),
                finish_reason: Some(finish.to_string()),
                model: Some(model),
                id: None,
                usage: ModelUsage::new(Some(20), Some(8)),
            })
        })
    }

    // These runs don't stream, so the model node makes whole calls only.
    fn stream(&self, _: ChatRequest) -> BoxFuture<'_, Result<ChatStream, ChatError>> {
        Box::pin(async { Err(ChatError::fatal("not_supported", "these runs don't stream")) })
    }
}
