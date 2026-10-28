//! Telemetry (`design/18-Run-Observability.md`, SG-19): the span tree the
//! engine builds (run → node → attempt, branches under their group's attempt, a
//! subgraph's nodes under its attempt), a handler's own span parented under its
//! attempt or branch, and what each metric records. The subscriber and the
//! metrics recorder are process-wide, so this binary installs one of each, and
//! every test filters by its own thread id or by node names no other test uses.
//! The .NET twin is `dotnet/tests/Datalium.Strata.Tests/TelemetryTests.cs` —
//! keep the two in step.

mod lease_store;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use datalium_strata::BoxFuture;

use datalium_strata::CancellationToken;
use datalium_strata::StrataError;
use datalium_strata::checkpoint::{
    CancelReason, CheckpointStore, CheckpointStoreError, GraphExecutionResult, HumanSignal,
    LeaseOptions,
};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{
    CheckpointPolicy, CompileOptions, CompiledGraph, NodeError, RecoveryPolicy, StateGraph,
};
use datalium_strata::observability::{
    ModelCallInfo, ModelCallScope, ModelUsage, RunEvent, RunEventChannel, RunEventKind,
    RunObserver, metric_names,
};
use datalium_strata::parallel::ParallelBranches;
use datalium_strata::schema::Checkpoint;
use datalium_strata::tools::{ToolCallContext, ToolRegistry};
use lease_store::{LeaseStore, TTL};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tracing::Instrument;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

const PATIENCE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Job {
    n: i64,
}

#[derive(Default)]
struct TestStore {
    checkpoints: Mutex<HashMap<String, Checkpoint>>,
}

impl TestStore {
    fn save(&self, checkpoint: Checkpoint) -> Result<(), CheckpointStoreError> {
        self.checkpoints
            .lock()
            .unwrap()
            .insert(checkpoint.thread_id.clone(), checkpoint);
        Ok(())
    }

    fn load(&self, thread_id: &str) -> Result<Option<Checkpoint>, CheckpointStoreError> {
        Ok(self.checkpoints.lock().unwrap().get(thread_id).cloned())
    }
}

impl CheckpointStore for TestStore {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        Box::pin(std::future::ready(TestStore::save(self, checkpoint)))
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        Box::pin(std::future::ready(TestStore::load(self, thread_id)))
    }
}

fn store() -> Arc<TestStore> {
    Arc::new(TestStore::default())
}

// --- A capturing subscriber --------------------------------------------------

#[derive(Debug, Clone)]
struct SpanRecord {
    name: &'static str,
    fields: HashMap<String, String>,
    parent: Option<usize>,
}

impl SpanRecord {
    /// What a trace viewer shows: `otel.name`, or the span's own name.
    fn display(&self) -> &str {
        self.fields
            .get("otel.name")
            .map(String::as_str)
            .unwrap_or(self.name)
    }
}

#[derive(Debug, Clone)]
struct EventRecord {
    span: Option<usize>,
    message: String,
}

#[derive(Default)]
struct Captured {
    spans: Vec<SpanRecord>,
    events: Vec<EventRecord>,
    // Span ids are reused once a span closes, so they map to a record only while open.
    open: HashMap<u64, usize>,
}

static CAPTURED: LazyLock<Mutex<Captured>> = LazyLock::new(Mutex::default);

struct Fields<'a>(&'a mut HashMap<String, String>);

impl Visit for Fields<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}

struct Capture;

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut fields = HashMap::new();
        attrs.record(&mut Fields(&mut fields));
        let parent = ctx
            .span(id)
            .and_then(|span| span.parent())
            .map(|parent| parent.id().into_u64());
        let mut captured = CAPTURED.lock().unwrap();
        let parent = parent.and_then(|parent| captured.open.get(&parent).copied());
        let index = captured.spans.len();
        captured.spans.push(SpanRecord {
            name: attrs.metadata().name(),
            fields,
            parent,
        });
        captured.open.insert(id.into_u64(), index);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, _ctx: Context<'_, S>) {
        let mut captured = CAPTURED.lock().unwrap();
        if let Some(&index) = captured.open.get(&id.into_u64()) {
            values.record(&mut Fields(&mut captured.spans[index].fields));
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut fields = HashMap::new();
        event.record(&mut Fields(&mut fields));
        let span = ctx.event_span(event).map(|span| span.id().into_u64());
        let mut captured = CAPTURED.lock().unwrap();
        let span = span.and_then(|span| captured.open.get(&span).copied());
        captured.events.push(EventRecord {
            span,
            message: fields.remove("message").unwrap_or_default(),
        });
    }

    fn on_close(&self, id: Id, _ctx: Context<'_, S>) {
        CAPTURED.lock().unwrap().open.remove(&id.into_u64());
    }
}

static SNAPSHOTTER: LazyLock<Snapshotter> = LazyLock::new(|| {
    let subscriber = tracing_subscriber::registry().with(Capture);
    tracing::subscriber::set_global_default(subscriber).unwrap();
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().unwrap();
    snapshotter
});

/// Installs the subscriber and the recorder, once for the binary.
fn init() {
    LazyLock::force(&SNAPSHOTTER);
}

/// The spans of the run whose `strata.run` span has `thread_id`, and its events.
struct Trace {
    spans: Vec<(usize, SpanRecord)>,
    events: Vec<EventRecord>,
}

impl Trace {
    fn of_run(thread_id: &str) -> Self {
        let captured = CAPTURED.lock().unwrap();
        let root = captured
            .spans
            .iter()
            .position(|s| {
                s.name == "strata.run"
                    && s.fields.get("strata.thread_id").map(String::as_str) == Some(thread_id)
            })
            .expect("no run span for that thread");
        let under_root = |mut index: usize| loop {
            if index == root {
                return true;
            }
            match captured.spans[index].parent {
                Some(parent) => index = parent,
                None => return false,
            }
        };
        let spans: Vec<(usize, SpanRecord)> = captured
            .spans
            .iter()
            .enumerate()
            .filter(|(index, _)| under_root(*index))
            .map(|(index, span)| (index, span.clone()))
            .collect();
        let events = captured
            .events
            .iter()
            .filter(|e| e.span.is_some_and(under_root))
            .cloned()
            .collect();
        Self { spans, events }
    }

    fn named(&self, display: &str) -> (usize, &SpanRecord) {
        let found: Vec<_> = self
            .spans
            .iter()
            .filter(|(_, s)| s.display() == display)
            .collect();
        assert_eq!(found.len(), 1, "expected one span shown as '{display}'");
        (found[0].0, &found[0].1)
    }

    fn parent_of(&self, display: &str) -> usize {
        self.named(display).1.parent.expect("a parent")
    }

    fn id_of(&self, display: &str) -> usize {
        self.named(display).0
    }

    fn field(&self, display: &str, field: &str) -> Option<String> {
        self.named(display).1.fields.get(field).cloned()
    }
}

/// The span a handler was handed, and the one current while it ran.
type SpanIds = (Option<Id>, Option<Id>);

fn handler_span(name: &'static str) {
    let _span = match name {
        "classify-call" => tracing::info_span!("classify-call"),
        "draft-call" => tracing::info_span!("draft-call"),
        "acme-call" => tracing::info_span!("acme-call"),
        "globex-call" => tracing::info_span!("globex-call"),
        _ => tracing::info_span!("sync-call"),
    }
    .entered();
}

#[tokio::test]
async fn the_engine_builds_the_documented_span_tree() {
    init();
    let calls = Arc::new(AtomicU32::new(0));
    let child = StateGraph::<Job>::new()
        .add_async_node("Draft", |j: Job, _ctx: NodeContext| async move {
            handler_span("draft-call");
            j
        })
        .compile()
        .unwrap();
    let graph = StateGraph::<Job>::new()
        .add_async_fallible_node("Classify", move |j: Job, _ctx: NodeContext| {
            let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
            async move {
                if first {
                    return Err(NodeError::Transient("gateway timeout".to_string()));
                }
                handler_span("classify-call");
                Ok(j)
            }
        })
        .add_subgraph("Work", child, |j: &Job| j.clone(), |_: &Job, j: Job| j)
        .add_parallel_group(
            "Gather",
            ParallelBranches::new()
                .async_branch("Acme", |j: Job, _ctx: NodeContext| async move {
                    handler_span("acme-call");
                    Ok(j)
                })
                .branch("Globex", |j: &Job| {
                    handler_span("globex-call");
                    Ok(j.clone())
                }),
            |s: &Job, _| s.clone(),
        )
        .add_node("Sync", |j: Job| {
            handler_span("sync-call");
            j
        })
        .add_edge("Classify", "Work")
        .add_edge("Work", "Gather")
        .add_edge("Gather", "Sync")
        .set_recovery_policy("Classify", RecoveryPolicy::retry(1))
        .compile()
        .unwrap();

    graph
        .run_with(
            "span-tree",
            Job { n: 1 },
            store(),
            RunConfig::new()
                .with_metadata("tenant", "acme")
                .with_run_id("exec-1"),
        )
        .await;
    let trace = Trace::of_run("span-tree");

    let root = trace.id_of("strata.run");
    assert_eq!(
        trace.field("strata.run", "strata.metadata").as_deref(),
        Some(r#"{"tenant":"acme"}"#)
    );
    assert_eq!(
        trace.field("strata.run", "strata.run_id").as_deref(),
        Some("exec-1")
    );
    assert_eq!(
        trace.field("strata.run", "strata.outcome").as_deref(),
        Some("completed")
    );

    let classify = trace.id_of("Classify");
    assert_eq!(trace.parent_of("Classify"), root);
    assert_eq!(
        trace.field("Classify", "strata.retry_count").as_deref(),
        Some("1")
    );
    assert_eq!(trace.parent_of("Classify #1"), classify);
    assert_eq!(
        trace.field("Classify #1", "otel.status_code").as_deref(),
        Some("ERROR")
    );
    assert_eq!(
        trace.field("Classify #1", "error.type").as_deref(),
        Some("transient")
    );
    assert!(
        trace
            .events
            .iter()
            .any(|e| e.span == Some(classify) && e.message.starts_with("retry scheduled"))
    );
    assert_eq!(trace.parent_of("Classify #2"), classify);
    assert_eq!(trace.parent_of("classify-call"), trace.id_of("Classify #2"));

    // A subgraph's nodes nest under the subgraph node's attempt.
    assert_eq!(trace.parent_of("Work/Draft"), trace.id_of("Work #1"));
    assert_eq!(trace.parent_of("draft-call"), trace.id_of("Work/Draft #1"));

    // Branches nest under their group's attempt, and their handlers (async and sync) under them.
    let gather_attempt = trace.id_of("Gather #1");
    assert_eq!(trace.parent_of("Gather/Acme"), gather_attempt);
    assert_eq!(trace.parent_of("Gather/Globex"), gather_attempt);
    assert_eq!(trace.parent_of("acme-call"), trace.id_of("Gather/Acme"));
    assert_eq!(trace.parent_of("globex-call"), trace.id_of("Gather/Globex"));

    // A sync handler on the blocking pool still runs inside its attempt.
    assert_eq!(trace.parent_of("sync-call"), trace.id_of("Sync #1"));
}

#[tokio::test]
async fn an_inline_handler_runs_inside_its_attempt() {
    init();
    let graph = StateGraph::<Job>::new()
        .add_inline_node("Inline", |j: Job| {
            handler_span("sync-call");
            j
        })
        .compile()
        .unwrap();

    graph.run("inline-span", Job { n: 1 }, store()).await;

    let trace = Trace::of_run("inline-span");
    assert_eq!(trace.named("Inline #1").1.name, "strata.attempt");
    assert_eq!(trace.parent_of("sync-call"), trace.id_of("Inline #1"));
}

#[tokio::test]
async fn the_context_exposes_the_attempt_span() {
    init();
    let seen: Arc<Mutex<Option<SpanIds>>> = Arc::default();
    let graph = {
        let seen = seen.clone();
        StateGraph::<Job>::new()
            .add_async_node("Look", move |j: Job, ctx: NodeContext| {
                let seen = seen.clone();
                async move {
                    *seen.lock().unwrap() = Some((ctx.span().id(), tracing::Span::current().id()));
                    j
                }
            })
            .compile()
            .unwrap()
    };

    graph.run("context-span", Job { n: 1 }, store()).await;

    let (from_context, current) = seen.lock().unwrap().clone().unwrap();
    assert!(from_context.is_some());
    assert_eq!(from_context, current);
    let trace = Trace::of_run("context-span");
    assert_eq!(trace.named("Look #1").1.name, "strata.attempt");
}

#[tokio::test]
async fn a_failed_run_marks_its_spans_and_a_cancellation_does_not() {
    init();
    let failing = StateGraph::<Job>::new()
        .add_fallible_node("Charge", |_: &Job| {
            Err(NodeError::Fatal("card declined".to_string()))
        })
        .compile()
        .unwrap();
    failing.run("failed-spans", Job { n: 1 }, store()).await;

    let trace = Trace::of_run("failed-spans");
    assert_eq!(
        trace.field("strata.run", "otel.status_code").as_deref(),
        Some("ERROR")
    );
    assert_eq!(
        trace.field("strata.run", "strata.outcome").as_deref(),
        Some("failed")
    );
    assert_eq!(
        trace.field("Charge", "error.type").as_deref(),
        Some("fatal")
    );
    let root = trace.id_of("strata.run");
    assert!(
        trace
            .events
            .iter()
            .any(|e| e.span == Some(root) && e.message == "checkpoint saved")
    );

    let token = CancellationToken::new();
    token.cancel();
    failing
        .run_with(
            "cancelled-spans",
            Job { n: 1 },
            store(),
            RunConfig::new().with_cancellation(token),
        )
        .await;

    let trace = Trace::of_run("cancelled-spans");
    assert_eq!(trace.field("strata.run", "otel.status_code"), None);
    assert_eq!(
        trace.field("strata.run", "strata.cancel_reason").as_deref(),
        Some("requested")
    );
}

// --- Metrics -----------------------------------------------------------------

/// The recorder's snapshots drain it, so metric tests take turns.
static METRICS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// `design/24-Loop-Circuit-Breaker.md`: the twin of `HopLimitTests`'
/// `The_Run_Span_Says_Hop_Limit_And_The_Counter_Names_The_Node`.
#[tokio::test]
async fn a_hop_limit_marks_the_run_span_and_counts_the_refused_node() {
    init();
    let _turn = METRICS.lock().await;
    measurements();
    let graph = StateGraph::<Job>::new()
        .add_node("HopSpanA", |j: Job| j)
        .add_node("HopSpanB", |j: Job| j)
        .add_edge("HopSpanA", "HopSpanB")
        .compile_with(CompileOptions::new().with_max_node_hops(1))
        .unwrap();

    graph.run("hop-span", Job { n: 1 }, store()).await;

    let trace = Trace::of_run("hop-span");
    assert_eq!(
        trace.field("strata.run", "otel.status_code").as_deref(),
        Some("ERROR")
    );
    assert_eq!(
        trace.field("strata.run", "error.type").as_deref(),
        Some("hop_limit")
    );
    let root = trace.id_of("strata.run");
    assert!(
        trace
            .events
            .iter()
            .any(|e| e.span == Some(root) && e.message == "hop limit exceeded")
    );
    assert!(!trace.spans.iter().any(|(_, s)| s.display() == "HopSpanB"));

    let all = measurements();
    let counted = of(
        &all,
        metric_names::HOP_LIMIT_EXCEEDED,
        "strata.node.path",
        "HopSpanB",
    );
    assert_eq!(counted.len(), 1);
    assert_eq!(counted[0].value, DebugValue::Counter(1));
}

struct Measurement {
    name: String,
    labels: HashMap<String, String>,
    value: DebugValue,
}

fn measurements() -> Vec<Measurement> {
    SNAPSHOTTER
        .snapshot()
        .into_vec()
        .into_iter()
        .map(|(key, _, _, value)| Measurement {
            name: key.key().name().to_string(),
            labels: key
                .key()
                .labels()
                .map(|l| (l.key().to_string(), l.value().to_string()))
                .collect(),
            value,
        })
        .collect()
}

fn of<'a>(all: &'a [Measurement], name: &str, label: &str, value: &str) -> Vec<&'a Measurement> {
    all.iter()
        .filter(|m| m.name == name && m.labels.get(label).map(String::as_str) == Some(value))
        .collect()
}

/// `design/31-Graph-Identity.md`: the run span says which graph runs, and a
/// graph without an identity leaves both fields unset.
#[tokio::test]
async fn the_run_span_names_the_graph() {
    init();
    let graph = |options: CompileOptions| {
        StateGraph::<Job>::new()
            .add_node("Only", |j: Job| j)
            .compile_with(options)
            .unwrap()
    };
    graph(
        CompileOptions::new()
            .with_graph_id("incident")
            .with_graph_version("2"),
    )
    .run("named-run", Job { n: 1 }, store())
    .await;
    graph(CompileOptions::new())
        .run("unnamed-run", Job { n: 1 }, store())
        .await;

    let named = Trace::of_run("named-run");
    assert_eq!(
        named.field("strata.run", "strata.graph.id").as_deref(),
        Some("incident")
    );
    assert_eq!(
        named.field("strata.run", "strata.graph.version").as_deref(),
        Some("2")
    );
    let unnamed = Trace::of_run("unnamed-run");
    assert_eq!(unnamed.field("strata.run", "strata.graph.id"), None);
    assert_eq!(unnamed.field("strata.run", "strata.graph.version"), None);
}

#[tokio::test]
async fn each_instrument_records_what_the_design_says() {
    init();
    let _turn = METRICS.lock().await;
    measurements();
    let calls = Arc::new(AtomicU32::new(0));
    let graph = StateGraph::<Job>::new()
        .add_fallible_node("MetricsClassify", move |j: &Job| {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(NodeError::Transient("gateway timeout".to_string()))
            } else {
                Ok(j.clone())
            }
        })
        .add_node("MetricsApprove", |j: Job| j)
        .add_interrupt_boundary("MetricsApprove")
        .add_node("MetricsExecute", |j: Job| j)
        .add_edge("MetricsClassify", "MetricsApprove")
        .add_edge("MetricsApprove", "MetricsExecute")
        .set_recovery_policy("MetricsClassify", RecoveryPolicy::retry(1))
        .compile()
        .unwrap();
    let store = store();

    graph.run("metrics", Job { n: 1 }, store.clone()).await;
    graph
        .resume(
            "metrics",
            HumanSignal {
                is_approved: true,
                reviewer_token: "x".to_string(),
                field_overrides: HashMap::new(),
                ..Default::default()
            },
            store.clone(),
        )
        .await;
    let all = measurements();

    let transitions = of(
        &all,
        metric_names::GRAPH_TRANSITION,
        "strata.node.path",
        "MetricsClassify",
    );
    assert_eq!(transitions.len(), 1);
    assert_eq!(
        transitions[0].labels["strata.next_node.path"],
        "MetricsApprove"
    );
    assert_eq!(transitions[0].value, DebugValue::Counter(1));

    let retries = of(
        &all,
        metric_names::NODE_RETRIES,
        "strata.node.path",
        "MetricsClassify",
    );
    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].labels["error.type"], "transient");

    for node in ["MetricsClassify", "MetricsApprove", "MetricsExecute"] {
        let durations = of(&all, metric_names::NODE_DURATION, "strata.node.path", node);
        assert_eq!(durations.len(), 1);
        assert_eq!(durations[0].labels["strata.node.result"], "committed");
        assert!(matches!(&durations[0].value, DebugValue::Histogram(values) if values.len() == 1));
    }

    // Saves (the freeze and the resume's stamp) and the resume's load.
    let saves = of(
        &all,
        metric_names::CHECKPOINT_LATENCY,
        "strata.checkpoint.operation",
        "save",
    );
    assert!(
        saves
            .iter()
            .any(|m| m.labels["strata.checkpoint.status"] == "interrupted")
    );
    assert!(
        !of(
            &all,
            metric_names::CHECKPOINT_LATENCY,
            "strata.checkpoint.operation",
            "load"
        )
        .is_empty()
    );

    assert!(
        !of(
            &all,
            metric_names::GRAPHS_STARTED,
            "strata.run.mode",
            "resume"
        )
        .is_empty()
    );
    assert!(
        of(
            &all,
            metric_names::GRAPH_DURATION,
            "strata.outcome",
            "interrupted"
        )
        .iter()
        .any(|m| m.labels["strata.run.mode"] == "run")
    );
    assert!(all.iter().any(|m| m.name == metric_names::GRAPHS_ACTIVE));
}

#[derive(Default)]
struct AlwaysPanics {
    calls: AtomicU32,
    done: Notify,
}

impl RunObserver for AlwaysPanics {
    fn on_event(&self, event: &RunEvent) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if matches!(event.kind, RunEventKind::RunCompleted { .. }) {
            self.done.notify_one();
        }
        panic!("observer bug");
    }
}

#[tokio::test]
async fn observer_errors_are_counted() {
    init();
    let _turn = METRICS.lock().await;
    measurements();
    let observer = Arc::new(AlwaysPanics::default());
    let graph = StateGraph::<Job>::new()
        .add_node("ObserverErrorsOnly", |j: Job| j)
        .compile()
        .unwrap();

    let result = graph
        .run_with(
            "observer-errors",
            Job { n: 1 },
            store(),
            RunConfig::new().with_observer(observer.clone()),
        )
        .await;
    tokio::time::timeout(PATIENCE, observer.done.notified())
        .await
        .unwrap();

    // The last error is counted just after the observer panicked.
    let mut counted = 0;
    let deadline = std::time::Instant::now() + PATIENCE;
    while counted < 4 && std::time::Instant::now() < deadline {
        counted += measurements()
            .iter()
            .filter(|m| m.name == metric_names::OBSERVER_ERRORS)
            .map(|m| match m.value {
                DebugValue::Counter(n) => n,
                _ => 0,
            })
            .sum::<u64>();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(matches!(result, GraphExecutionResult::Completed(_)));
    assert_eq!(observer.calls.load(Ordering::SeqCst), 4);
    assert!(counted >= 4);
}

// --- Tool calls (design/20-Tool-Dispatch-And-Registry.md) ------------------

#[derive(serde::Deserialize, datalium_strata::prelude::StrataSchema)]
struct NoArgs {}

/// A tool that starts a span of its own, as an HTTP client would.
#[datalium_strata::prelude::strata_tool(name = "telemetry.lookup", description = "Looks.")]
async fn telemetry_lookup(_args: NoArgs, ctx: ToolCallContext) -> Result<String, StrataError> {
    let _client = tracing::info_span!("lookup-client-call").entered();
    Ok(format!("{:?}", ctx.span().id().map(|id| id.into_u64())))
}

#[datalium_strata::prelude::strata_tool(name = "telemetry.other", description = "Other.")]
async fn telemetry_other(_args: NoArgs) -> Result<String, StrataError> {
    Ok(String::new())
}

/// Tools only the metrics test calls, so other tests can't add to their series.
#[datalium_strata::prelude::strata_tool(name = "telemetry.metered", description = "Metered.")]
async fn telemetry_metered(_args: NoArgs) -> Result<String, StrataError> {
    Ok(String::new())
}

#[datalium_strata::prelude::strata_tool(name = "telemetry.metered_hidden", description = "Hidden.")]
async fn telemetry_metered_hidden(_args: NoArgs) -> Result<String, StrataError> {
    Ok(String::new())
}

fn telemetry_tools() -> Arc<ToolRegistry> {
    let mut tools = ToolRegistry::new();
    tools
        .register(Box::new(TelemetryLookupTool))
        .register(Box::new(TelemetryOtherTool));
    Arc::new(tools.subset(["telemetry.lookup"]).unwrap())
}

/// A node that calls `tool` through `tools`, as `caller`, with call id `call`.
fn calls_tool(
    tools: Arc<ToolRegistry>,
    tool: &'static str,
) -> impl Fn(Job, NodeContext) -> BoxFuture<'static, Job> + Send + Sync + 'static {
    move |j, ctx| {
        let tools = tools.clone();
        Box::pin(async move {
            let call = ToolCallContext::from(&ctx)
                .with_caller_id("investigator")
                .with_call_id(format!("call-{tool}"));
            let _ = tools.dispatch_with(tool, "{}", call).await;
            j
        })
    }
}

#[tokio::test]
async fn a_tool_call_is_a_span_under_the_calling_attempt_and_the_tool_runs_inside_it() {
    init();
    let graph = StateGraph::<Job>::new()
        .add_async_node(
            "ToolCaller",
            calls_tool(telemetry_tools(), "telemetry.lookup"),
        )
        .compile()
        .unwrap();

    graph.run("tool-spans", Job { n: 1 }, store()).await;

    let trace = Trace::of_run("tool-spans");
    let (tool_id, tool) = trace.named("execute_tool telemetry.lookup");
    assert_eq!(tool.name, "strata.tool");
    assert_eq!(tool.parent, Some(trace.id_of("ToolCaller #1")));
    for (field, value) in [
        ("gen_ai.operation.name", "execute_tool"),
        ("gen_ai.tool.name", "telemetry.lookup"),
        ("gen_ai.tool.call.id", "call-telemetry.lookup"),
        ("strata.thread_id", "tool-spans"),
        ("strata.node.path", "ToolCaller"),
        ("strata.attempt", "1"),
        ("strata.tool.caller", "investigator"),
        ("strata.tool.result", "succeeded"),
    ] {
        assert_eq!(
            tool.fields.get(field).map(String::as_str),
            Some(value),
            "{field}"
        );
    }
    assert!(!tool.fields.contains_key("otel.status_code"));
    assert_eq!(trace.parent_of("lookup-client-call"), tool_id);
}

#[tokio::test]
async fn a_span_the_handler_opens_around_a_tool_call_is_its_parent() {
    init();
    let tools = telemetry_tools();
    let graph = StateGraph::<Job>::new()
        .add_async_node("ToolIteration", move |j: Job, ctx: NodeContext| {
            let tools = tools.clone();
            async move {
                tools
                    .dispatch_with("telemetry.lookup", "{}", ToolCallContext::from(&ctx))
                    .instrument(tracing::info_span!("agent-iteration"))
                    .await
                    .unwrap();
                j
            }
        })
        .compile()
        .unwrap();

    graph.run("tool-iteration", Job { n: 1 }, store()).await;

    let trace = Trace::of_run("tool-iteration");
    assert_eq!(
        trace.parent_of("execute_tool telemetry.lookup"),
        trace.id_of("agent-iteration")
    );
    assert_eq!(
        trace.parent_of("agent-iteration"),
        trace.id_of("ToolIteration #1")
    );
}

#[tokio::test]
async fn a_tool_call_made_off_the_nodes_span_still_parents_to_its_attempt() {
    init();
    let tools = telemetry_tools();
    let graph = StateGraph::<Job>::new()
        .add_async_node("ToolSpawner", move |j: Job, ctx: NodeContext| {
            let tools = tools.clone();
            async move {
                // A spawned task doesn't carry the current span; the context does.
                let call = ToolCallContext::from(&ctx);
                tokio::spawn(
                    async move { tools.dispatch_with("telemetry.lookup", "{}", call).await },
                )
                .await
                .unwrap()
                .unwrap();
                j
            }
        })
        .compile()
        .unwrap();

    graph.run("tool-spawned", Job { n: 1 }, store()).await;

    let trace = Trace::of_run("tool-spawned");
    assert_eq!(
        trace.parent_of("execute_tool telemetry.lookup"),
        trace.id_of("ToolSpawner #1")
    );
}

#[tokio::test]
async fn a_refused_tool_call_is_an_error_span_with_its_reason() {
    init();
    let graph = StateGraph::<Job>::new()
        .add_async_node(
            "ToolRefused",
            calls_tool(telemetry_tools(), "telemetry.other"),
        )
        .add_async_node(
            "ToolUnknown",
            calls_tool(telemetry_tools(), "telemetry.nope"),
        )
        .add_edge("ToolRefused", "ToolUnknown")
        .compile()
        .unwrap();

    graph.run("tool-refused", Job { n: 1 }, store()).await;

    let trace = Trace::of_run("tool-refused");
    for (display, error_type) in [
        ("execute_tool telemetry.other", "not_permitted"),
        ("execute_tool telemetry.nope", "not_found"),
    ] {
        assert_eq!(
            trace.field(display, "strata.tool.result").as_deref(),
            Some("refused")
        );
        assert_eq!(
            trace.field(display, "error.type").as_deref(),
            Some(error_type)
        );
        assert_eq!(
            trace.field(display, "otel.status_code").as_deref(),
            Some("ERROR")
        );
    }
}

#[tokio::test]
async fn a_tool_call_outside_a_run_parents_to_the_current_span() {
    init();
    let tools = telemetry_tools();
    let outer = tracing::info_span!("outside-caller");

    tools
        .dispatch_with("telemetry.lookup", "{}", ToolCallContext::new())
        .instrument(outer)
        .await
        .unwrap();

    let captured = CAPTURED.lock().unwrap();
    let outer = captured
        .spans
        .iter()
        .rposition(|s| s.name == "outside-caller")
        .unwrap();
    let tool = captured
        .spans
        .iter()
        .rposition(|s| s.name == "strata.tool" && s.parent == Some(outer));
    assert!(
        tool.is_some(),
        "the tool span is a child of the current span"
    );
}

#[tokio::test]
async fn tool_metrics_carry_bounded_labels_only() {
    init();
    let _turn = METRICS.lock().await;
    measurements();
    let mut metered = ToolRegistry::new();
    metered
        .register(Box::new(TelemetryMeteredTool))
        .register(Box::new(TelemetryMeteredHiddenTool));
    let tools = metered.subset(["telemetry.metered"]).unwrap();

    tools.dispatch("telemetry.metered", "{}").await.unwrap();
    let _ = tools.dispatch("telemetry.metered_hidden", "{}").await;
    let _ = tools.dispatch("made-up-by-the-model", "{}").await;
    let all = measurements();

    let requests = |name: &str| -> Vec<&Measurement> {
        of(&all, metric_names::TOOL_REQUESTS, "gen_ai.tool.name", name)
    };
    let lookup = requests("telemetry.metered");
    assert_eq!(lookup.len(), 1);
    assert_eq!(lookup[0].labels["strata.tool.result"], "succeeded");
    let other = requests("telemetry.metered_hidden");
    assert_eq!(other.len(), 1);
    assert_eq!(other[0].labels["strata.tool.result"], "refused");
    assert!(requests("made-up-by-the-model").is_empty());
    assert!(all.iter().any(|m| m.name == metric_names::TOOL_REQUESTS
        && !m.labels.contains_key("gen_ai.tool.name")
        && m.labels["strata.tool.result"] == "refused"));
    let durations = of(
        &all,
        metric_names::TOOL_DURATION,
        "gen_ai.tool.name",
        "telemetry.metered",
    );
    assert!(matches!(&durations[0].value, DebugValue::Histogram(values) if values.len() == 1));
}

/// `design/32-Thread-Leases.md`: the twin of `ThreadLeaseTests`'
/// `The_Run_Span_Carries_The_Lease_And_The_Counters_Count_It`.
#[tokio::test]
async fn the_run_span_carries_the_lease_and_the_counters_count_it() {
    init();
    let _turn = METRICS.lock().await;
    measurements();
    let store = LeaseStore::new();
    let leased = || RunConfig::new().with_lease(LeaseOptions::new("worker-1", TTL));
    let graph = StateGraph::<Job>::new()
        .add_node("LeaseSpanA", |j: Job| j)
        .add_interrupt_boundary("LeaseSpanA")
        .compile()
        .unwrap();
    let fenced = StateGraph::<Job>::new()
        .add_async_node("LeaseSpanB", {
            let store = store.clone();
            move |j: Job, context: NodeContext| {
                store.take_over(context.thread_id());
                async move { j }
            }
        })
        .add_interrupt_boundary("LeaseSpanB")
        .compile()
        .unwrap();

    // One run keeps its lease, one is refused it, and one loses it.
    graph
        .run_with("lease-span-kept", Job { n: 1 }, store.clone(), leased())
        .await;
    store.acquire("lease-span-held", "worker-2").unwrap();
    assert!(matches!(
        graph
            .run_with("lease-span-held", Job { n: 1 }, store.clone(), leased())
            .await,
        GraphExecutionResult::Failed(_)
    ));
    assert!(matches!(
        fenced
            .run_with("lease-span-lost", Job { n: 1 }, store.clone(), leased())
            .await,
        GraphExecutionResult::Cancelled {
            reason: CancelReason::LeaseLost,
            ..
        }
    ));

    let kept = Trace::of_run("lease-span-kept");
    assert_eq!(
        kept.field("strata.run", "strata.lease.owner").as_deref(),
        Some("worker-1")
    );
    assert_eq!(
        kept.field("strata.run", "strata.lease.token").as_deref(),
        Some("1")
    );
    let held = Trace::of_run("lease-span-held");
    assert_eq!(held.field("strata.run", "strata.lease.owner"), None);
    let lost = Trace::of_run("lease-span-lost");
    assert_eq!(
        lost.field("strata.run", "strata.cancel_reason").as_deref(),
        Some("lease_lost")
    );
    assert_eq!(lost.field("strata.run", "otel.status_code"), None);
    let root = lost.id_of("strata.run");
    assert!(
        lost.events
            .iter()
            .any(|e| e.span == Some(root) && e.message == "lease lost")
    );

    let all = measurements();
    let acquisitions = |outcome: &str| {
        of(
            &all,
            metric_names::LEASE_ACQUISITIONS,
            "strata.lease.outcome",
            outcome,
        )
    };
    assert_eq!(acquisitions("acquired")[0].value, DebugValue::Counter(2));
    assert_eq!(acquisitions("held")[0].value, DebugValue::Counter(1));
    let losses: Vec<_> = all
        .iter()
        .filter(|m| m.name == metric_names::LEASE_LOSSES)
        .collect();
    assert_eq!(losses.len(), 1);
    assert_eq!(losses[0].value, DebugValue::Counter(1));
}

/// `design/33-Durable-Execution.md`: the twin of `DurableExecutionTests`'
/// `A_Recovery_Says_So_In_Its_Events_And_Telemetry`.
#[tokio::test]
async fn a_recovery_is_a_run_of_its_own_mode_and_the_counter_counts_it() {
    init();
    let _turn = METRICS.lock().await;
    measurements();
    let store = LeaseStore::new();
    let graph = StateGraph::<Job>::new()
        .add_node("RecoverSpanA", |j: Job| j)
        .add_node("RecoverSpanB", |j: Job| j)
        .add_edge("RecoverSpanA", "RecoverSpanB")
        .compile_with(CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode))
        .unwrap();

    // A thread whose process died after its first node, as that run's second
    // checkpoint left it.
    graph
        .run("recover-span-done", Job { n: 1 }, store.clone())
        .await;
    let mut dead = store.log("recover-span-done").remove(1);
    dead.thread_id = "recover-span".to_string();
    store.save(dead).await.unwrap();

    assert!(matches!(
        graph.recover("recover-span", store.clone()).await,
        GraphExecutionResult::Completed(_)
    ));

    let trace = Trace::of_run("recover-span");
    assert_eq!(
        trace.field("strata.recover", "strata.run.mode").as_deref(),
        Some("recover")
    );
    assert_eq!(
        trace
            .field("strata.recover", "strata.recoveries")
            .as_deref(),
        Some("1")
    );
    let recoveries: Vec<_> = measurements()
        .into_iter()
        .filter(|m| m.name == metric_names::THREAD_RECOVERIES)
        .collect();
    assert_eq!(recoveries.len(), 1);
    assert_eq!(recoveries[0].value, DebugValue::Counter(1));
}

/// `design/40-Run-Scheduler.md`: a run admitted by a scheduler carries its
/// priority and how long it waited on its span; a run it refuses has no span.
#[tokio::test]
async fn a_scheduled_run_records_its_wait_on_its_span() {
    use datalium_strata::scheduler::{AdmissionRequest, RunScheduler, SchedulerOptions};

    init();
    let scheduler = RunScheduler::new(
        SchedulerOptions::new(1)
            .with_max_queued(0)
            .with_name("telemetry-scheduler"),
    )
    .unwrap();
    let graph = StateGraph::<Job>::new()
        .add_node("SchedOnly", |j: Job| j)
        .compile()
        .unwrap();
    let config = || {
        RunConfig::new()
            .with_scheduler(scheduler.clone())
            .with_priority(2)
    };

    graph
        .run_with("scheduled-run", Job { n: 1 }, store(), config())
        .await;
    {
        let _busy = scheduler.admit(AdmissionRequest::new()).await.unwrap();
        let refused = graph
            .run_with("refused-run", Job { n: 1 }, store(), config())
            .await;
        assert!(matches!(
            refused,
            GraphExecutionResult::Failed(StrataError::NotAdmitted { .. })
        ));
    }

    let trace = Trace::of_run("scheduled-run");
    assert_eq!(
        trace.field("strata.run", "strata.run.priority").as_deref(),
        Some("2")
    );
    assert!(
        trace
            .field("strata.run", "strata.scheduler.wait_ms")
            .is_some()
    );
    assert!(
        !CAPTURED
            .lock()
            .unwrap()
            .spans
            .iter()
            .any(|s| s.name == "strata.run"
                && s.fields.get("strata.thread_id").map(String::as_str) == Some("refused-run"))
    );

    // The metrics are checked in `scheduler_engine.rs`: a snapshot here would
    // drain the histograms other tests in this binary are reading.
}

// --- Model calls (design/44-LLM-Integration.md, design/45-Long-Term-Memory.md)

/// A node that makes one model call through a scope with `operation`, which
/// succeeds with 4 input tokens and no output count.
fn calls_model(
    operation: &'static str,
) -> impl Fn(Job, NodeContext) -> BoxFuture<'static, Job> + Send + Sync + 'static {
    move |j, ctx| {
        Box::pin(async move {
            let scope = ModelCallScope::start(
                Some(&ctx),
                ModelCallInfo::new("embed-span")
                    .with_provider("scripted")
                    .with_operation(operation),
            );
            scope.succeeded(ModelUsage::new(Some(4), None), None, None, None);
            j
        })
    }
}

/// The model-call events' operations, in order, from a run of `graph`.
async fn model_call_operations(graph: &CompiledGraph<Job>, thread_id: &str) -> Vec<String> {
    let (observer, mut events) = RunEventChannel::new();
    graph
        .run_with(
            thread_id,
            Job { n: 1 },
            store(),
            RunConfig::new().with_observer(observer),
        )
        .await;
    let mut operations = Vec::new();
    while let Some(event) = tokio::time::timeout(PATIENCE, events.recv()).await.unwrap() {
        match event.kind {
            RunEventKind::ModelCallStarted { operation, .. } => {
                operations.push(format!("Started {operation}"))
            }
            RunEventKind::ModelCallCompleted {
                operation, usage, ..
            } => operations.push(format!(
                "Completed {operation} {:?}/{:?}",
                usage.input_tokens, usage.output_tokens
            )),
            _ => {}
        }
    }
    operations
}

#[tokio::test]
async fn an_embedding_call_s_scope_names_its_operation_on_the_span_and_the_events() {
    init();
    let embeddings = StateGraph::<Job>::new()
        .add_async_node("Embed", calls_model("embeddings"))
        .compile()
        .unwrap();
    let chat = StateGraph::<Job>::new()
        .add_async_node("Chat", calls_model("chat"))
        .compile()
        .unwrap();

    assert_eq!(
        model_call_operations(&embeddings, "embedding-scope").await,
        ["Started embeddings", "Completed embeddings Some(4)/None"]
    );
    assert_eq!(
        model_call_operations(&chat, "chat-scope").await,
        ["Started chat", "Completed chat Some(4)/None"]
    );

    let trace = Trace::of_run("embedding-scope");
    let (_, span) = trace.named("embeddings embed-span");
    assert_eq!(span.name, "strata.model");
    let field = |name: &str| span.fields.get(name).map(String::as_str);
    assert_eq!(field("gen_ai.operation.name"), Some("embeddings"));
    assert_eq!(field("gen_ai.usage.input_tokens"), Some("4"));
    assert_eq!(field("gen_ai.usage.output_tokens"), None);
    assert_eq!(
        Trace::of_run("chat-scope")
            .field("chat embed-span", "gen_ai.operation.name")
            .as_deref(),
        Some("chat")
    );
}
