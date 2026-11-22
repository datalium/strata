//! Samples for <https://stratamesh.dev/guides/production/observability/>.

use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use datalium_strata::StrataError;
use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{CompiledGraph, NodeError, RecoveryPolicy, StateGraph};
use datalium_strata::observability::{RunEvent, RunEventChannel, RunEventKind, RunObserver};
use datalium_strata::streaming::StreamChunkType;
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};

use super::hosting::{TempDir, in_own_process, run_in_own_process};

// The samples print what they find. This `println!` keeps the lines instead, so the tests can check them.
// Each test runs on one thread (tokio's current-thread runtime), observers included, so the lines are kept
// per thread.
thread_local! {
    static PRINTED: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

macro_rules! println {
    ($($arg:tt)*) => {
        PRINTED.with(|printed| printed.borrow_mut().push(format!($($arg)*)))
    };
}

fn printed() -> Vec<String> {
    PRINTED.with(|printed| std::mem::take(&mut *printed.borrow_mut()))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Answer {
    text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    amount: i64,
    log: Vec<String>,
}

impl Order {
    fn new(amount: i64) -> Self {
        Self {
            amount,
            log: Vec::new(),
        }
    }

    fn then(mut self, step: &str) -> Self {
        self.log.push(step.to_string());
        self
    }
}

/// The review graph: Classify fails transiently once and is retried, then the run pauses at the
/// interrupt boundary on Approve.
fn review_graph() -> CompiledGraph<Order> {
    let calls = Arc::new(AtomicU32::new(0));
    StateGraph::<Order>::new()
        .add_async_fallible_node("Classify", move |o: Order, _: NodeContext| {
            let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
            async move {
                if first {
                    return Err(NodeError::Transient("gateway timeout".to_string()));
                }
                Ok(o.then("classify"))
            }
        })
        .add_node("Approve", |o: Order| o.then("approve"))
        .add_interrupt_boundary("Approve")
        .add_node("Execute", |o: Order| o.then("execute"))
        .add_edge("Classify", "Approve")
        .add_edge("Approve", "Execute")
        .set_recovery_policy("Classify", RecoveryPolicy::retry(1))
        .compile()
        .expect("the review graph is valid")
}

#[tokio::test]
async fn stream() {
    // docs:begin guides/production/observability/stream
    let graph = Arc::new(
        StateGraph::<Answer>::new()
            .add_async_streaming_node("Answer", |state: Answer, ctx: NodeContext| async move {
                let stream = ctx.stream().expect("a streaming run always has a stream");
                let mut text = state.text;
                for token in ["Checkpoints ", "make ", "runs ", "durable."] {
                    if stream.emit_token(token).await.is_err() {
                        break; // the consumer dropped the receiver
                    }
                    text.push_str(token);
                }
                Answer { text }
            })
            .add_node("Count", |state: Answer| Answer {
                text: format!("{} ({} chars)", state.text, state.text.len()),
            })
            .add_edge("Answer", "Count")
            .compile()
            .expect("the answer graph is valid"),
    );

    let (handle, mut chunks) = graph.clone().run_streaming(
        "answer-1",
        Answer {
            text: String::new(),
        },
        Arc::new(InMemoryCheckpointStore::new()),
    );

    while let Some(chunk) = chunks.recv().await {
        match chunk.chunk_type {
            StreamChunkType::Token => println!("  {}: {:?}", chunk.node_name, chunk.content),
            StreamChunkType::Retry | StreamChunkType::Error => {
                println!(
                    "{:?} {}: {}",
                    chunk.chunk_type, chunk.node_name, chunk.content
                )
            }
            _ => println!("{:?} {}", chunk.chunk_type, chunk.node_name),
        }
    }

    match handle.await.expect("the run's task doesn't panic") {
        GraphExecutionResult::Completed(state) => println!("Completed: {}", state.text),
        _ => println!("not completed"),
    }
    // docs:end guides/production/observability/stream

    assert_eq!(
        printed(),
        [
            "NodeStart Answer",
            "  Answer: \"Checkpoints \"",
            "  Answer: \"make \"",
            "  Answer: \"runs \"",
            "  Answer: \"durable.\"",
            "NodeEnd Answer",
            "NodeStart Count",
            "NodeEnd Count",
            "Completed: Checkpoints make runs durable. (30 chars)",
        ]
    );

    // docs:begin guides/production/observability/plain-run
    let plain = graph
        .run(
            "answer-2",
            Answer {
                text: String::new(),
            },
            Arc::new(InMemoryCheckpointStore::new()),
        )
        .await;
    if let GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) = plain {
        println!("{message}");
    }
    // docs:end guides/production/observability/plain-run

    assert_eq!(
        printed(),
        [
            "node 'Answer' is a streaming node but this run has no StreamContext — use run_streaming instead of run"
        ]
    );
}

#[tokio::test]
async fn events() {
    let graph = review_graph();
    let store = Arc::new(InMemoryCheckpointStore::new());

    // docs:begin guides/production/observability/events
    let (observer, mut events) = RunEventChannel::new();
    let result = graph
        .run_with(
            "order-2001",
            Order::new(40),
            store,
            RunConfig::new()
                .with_metadata("tenant", "acme")
                .with_observer(observer),
        )
        .await;

    while let Some(e) = events.recv().await {
        let attempt = e.attempt.map(|a| format!(" #{a}")).unwrap_or_default();
        let path = e.node_path.as_deref().unwrap_or_default();
        println!("#{} {} {path}{attempt}", e.sequence, kind_name(&e.kind));
    }

    fn kind_name(kind: &RunEventKind) -> &'static str {
        match kind {
            RunEventKind::RunStarted { .. } => "RunStarted",
            RunEventKind::NodeStarted => "NodeStarted",
            // docs:hide ...
            RunEventKind::RetryScheduled { .. } => "RetryScheduled",
            RunEventKind::NodeCompleted { .. } => "NodeCompleted",
            RunEventKind::CheckpointSaved { .. } => "CheckpointSaved",
            RunEventKind::Interrupted { .. } => "Interrupted",
            RunEventKind::RunCompleted { .. } => "RunCompleted",
            // docs:show
            _ => "other", // non-exhaustive: later versions add kinds
        }
    }
    // docs:end guides/production/observability/events

    assert!(matches!(result, GraphExecutionResult::Interrupted { .. }));
    assert_eq!(
        printed(),
        [
            "#1 RunStarted ",
            "#2 NodeStarted Classify #1",
            "#3 RetryScheduled Classify #1",
            "#4 NodeCompleted Classify #2",
            "#5 NodeStarted Approve #1",
            "#6 NodeCompleted Approve #1",
            "#7 CheckpointSaved ",
            "#8 Interrupted ",
            "#9 RunCompleted ",
        ]
    );
}

#[tokio::test]
async fn observer() {
    let graph = review_graph();
    let store = Arc::new(InMemoryCheckpointStore::new());

    // docs:begin guides/production/observability/observer
    struct ConsoleObserver;

    impl RunObserver for ConsoleObserver {
        fn on_event(&self, event: &RunEvent) {
            let path = event.node_path.as_deref().unwrap_or_default();
            let line = match &event.kind {
                RunEventKind::NodeCompleted { result, .. } => {
                    format!(
                        "{path} {result:?} in {} attempt(s)",
                        event.attempt.unwrap_or_default()
                    )
                }
                RunEventKind::RetryScheduled { error, .. } => {
                    format!(
                        "{path} attempt {} failed: {error}",
                        event.attempt.unwrap_or_default()
                    )
                }
                RunEventKind::ModelCallCompleted { model, usage, .. } => {
                    format!(
                        "{model}: {:?} in, {:?} out",
                        usage.input_tokens, usage.output_tokens
                    )
                }
                RunEventKind::RunCompleted { outcome, .. } => {
                    format!("run {} {outcome:?}", event.run_id)
                }
                _ => return, // later versions add event kinds
            };
            println!("[{}] {line}", event.thread_id);
        }
    }

    graph
        .run_with(
            "order-2002",
            Order::new(40),
            store,
            RunConfig::new().with_observer(Arc::new(ConsoleObserver)),
        )
        .await;
    // docs:end guides/production/observability/observer

    // The run can return before the observer has seen its last event.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut lines: Vec<String> = Vec::new();
    while !lines
        .last()
        .is_some_and(|l| l.starts_with("[order-2002] run "))
    {
        assert!(tokio::time::Instant::now() < deadline, "{lines:?}");
        tokio::time::sleep(Duration::from_millis(5)).await;
        lines.extend(printed());
    }
    let run = lines.pop().expect("the run's line");
    assert!(run.ends_with(" Interrupted"), "{run}");
    assert_eq!(
        lines,
        [
            "[order-2002] Classify attempt 1 failed: gateway timeout",
            "[order-2002] Classify Committed in 2 attempt(s)",
            "[order-2002] Approve Committed in 1 attempt(s)",
        ]
    );
}

#[test]
fn opentelemetry() {
    const TEST: &str = "guides::production::observability::opentelemetry";
    if !in_own_process(TEST) {
        // `init()` sets the process's one global subscriber, so the sample runs in a process of its own,
        // where the exporter prints the spans.
        let dir = TempDir::new("observability-opentelemetry");
        let printed = run_in_own_process(TEST, &dir.0);
        let attempt = printed
            .split("Name         : ")
            .find(|span| span.starts_with("Classify #1"))
            .unwrap_or_else(|| panic!("no span for Classify's first attempt in:\n{printed}"));
        for attribute in [
            "strata.thread_id: String(Owned(\"order-4001\"))",
            "strata.node.path: String(Owned(\"Classify\"))",
            "strata.attempt: String(Owned(\"1\"))",
            "otel.status_message: String(Owned(\"gateway timeout\"))",
            "error.type: String(Owned(\"transient\"))",
        ] {
            assert!(attempt.contains(attribute), "{attribute} in:\n{attempt}");
        }
        return;
    }

    let runtime = tokio::runtime::Runtime::new().expect("a tokio runtime");

    // docs:begin guides/production/observability/opentelemetry
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(opentelemetry_stdout::SpanExporter::default())
        .build();
    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("my-service")))
        .init();

    // docs:hide ... run graphs ...
    let result = runtime.block_on(review_graph().run(
        "order-4001",
        Order::new(40),
        Arc::new(InMemoryCheckpointStore::new()),
    ));
    assert!(matches!(result, GraphExecutionResult::Interrupted { .. }));
    // docs:show

    provider.shutdown().expect("spans flushed");
    // docs:end guides/production/observability/opentelemetry
}
