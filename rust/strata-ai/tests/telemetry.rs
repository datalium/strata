//! A model call's span and metrics (`design/44-LLM-Integration.md`,
//! "Telemetry"): `chat <model>` under the node's attempt span with the GenAI
//! attributes, and `gen_ai.client.token.usage` by token type; an embedding
//! call's (`design/45-Long-Term-Memory.md`): `embeddings <model>`, input
//! tokens only. Its own binary,
//! since it installs a global subscriber and recorder. Mirrors .NET's
//! `The_Span_Carries_The_GenAI_Attributes_And_The_Metrics_Record_Usage`.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use common::{Scripted, ScriptedChatClient, ScriptedEmbedder};
use datalium_strata::graph::StateGraph;
use datalium_strata::observability::metric_names;
use datalium_strata_ai::{Message, ModelNodeOptions, StateGraphAiExt};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use tracing::Subscriber;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

#[derive(Debug, Clone)]
struct SpanRecord {
    name: &'static str,
    parent: Option<&'static str>,
    fields: HashMap<String, String>,
}

/// The spans seen, and the open ones by id (ids are reused once a span closes).
type Spans = (Vec<SpanRecord>, HashMap<u64, usize>);

static SPANS: LazyLock<Mutex<Spans>> = LazyLock::new(Mutex::default);

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
            .map(|parent| parent.name());
        let mut spans = SPANS.lock().unwrap();
        let index = spans.0.len();
        spans.0.push(SpanRecord {
            name: attrs.metadata().name(),
            parent,
            fields,
        });
        spans.1.insert(id.into_u64(), index);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, _ctx: Context<'_, S>) {
        let mut spans = SPANS.lock().unwrap();
        if let Some(&index) = spans.1.get(&id.into_u64()) {
            values.record(&mut Fields(&mut spans.0[index].fields));
        }
    }

    fn on_close(&self, id: Id, _ctx: Context<'_, S>) {
        SPANS.lock().unwrap().1.remove(&id.into_u64());
    }
}

/// Snapshots drain the recorder (counters to 0, histograms cleared), so the
/// tests that read metrics take turns.
static METRICS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

static SNAPSHOTTER: LazyLock<Snapshotter> = LazyLock::new(|| {
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Capture)).unwrap();
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().unwrap();
    snapshotter
});

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Job {
    answer: String,
}

#[tokio::test]
async fn the_span_carries_the_gen_ai_attributes_and_the_metrics_record_usage() {
    let _turn = METRICS.lock().await;
    LazyLock::force(&SNAPSHOTTER);
    let chat = ScriptedChatClient::new(vec![Scripted::text(&["ok"]).with_usage(101, 7)]);
    let graph = StateGraph::<Job>::new()
        .add_model_node(
            "Ask",
            chat,
            |_: &Job, _| vec![Message::user("?")],
            |_, response| Job {
                answer: response.text(),
            },
            ModelNodeOptions::new().with_model("gpt-span"),
        )
        .compile()
        .unwrap();

    graph
        .run(
            "span-thread",
            Job {
                answer: String::new(),
            },
            Arc::new(InMemoryCheckpointStore::new()),
        )
        .await;

    let spans = SPANS.lock().unwrap().0.clone();
    let span = spans
        .iter()
        .find(|s| {
            s.name == "strata.model"
                && s.fields.get("strata.thread_id").map(String::as_str) == Some("span-thread")
        })
        .expect("a strata.model span");
    let field = |name: &str| span.fields.get(name).map(String::as_str);
    assert_eq!(span.parent, Some("strata.attempt"));
    assert_eq!(field("otel.name"), Some("chat gpt-span"));
    assert_eq!(field("otel.kind"), Some("client"));
    assert_eq!(field("gen_ai.operation.name"), Some("chat"));
    assert_eq!(field("gen_ai.provider.name"), Some("scripted"));
    assert_eq!(field("gen_ai.request.model"), Some("gpt-span"));
    assert_eq!(field("gen_ai.response.model"), Some("gpt-span"));
    assert_eq!(field("gen_ai.response.finish_reasons"), Some("stop"));
    assert_eq!(field("gen_ai.usage.input_tokens"), Some("101"));
    assert_eq!(field("gen_ai.usage.output_tokens"), Some("7"));
    assert_eq!(field("strata.node.path"), Some("Ask"));
    assert_eq!(field("strata.model.call_id").map(str::len), Some(36));

    let usage: Vec<(String, DebugValue)> = SNAPSHOTTER
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, ..)| key.key().name() == metric_names::MODEL_TOKEN_USAGE)
        .filter(|(key, ..)| {
            key.key()
                .labels()
                .any(|l| l.key() == "gen_ai.request.model" && l.value() == "gpt-span")
        })
        .map(|(key, _, _, value)| {
            let kind = key
                .key()
                .labels()
                .find(|l| l.key() == "gen_ai.token.type")
                .map(|l| l.value().to_string())
                .unwrap_or_default();
            (kind, value)
        })
        .collect();
    let of = |kind: &str| {
        usage
            .iter()
            .find(|(k, _)| k == kind)
            .map(|(_, value)| match value {
                DebugValue::Histogram(values) => values.iter().map(|v| v.into_inner()).collect(),
                _ => Vec::new(),
            })
            .unwrap_or_default()
    };
    assert_eq!(of("input"), vec![101.0]);
    assert_eq!(of("output"), vec![7.0]);
}

#[tokio::test]
async fn an_agent_s_turns_carry_its_id_and_its_tool_calls_are_spans_of_the_attempt() {
    use datalium_strata::tools::ToolRegistry;
    use datalium_strata_ai::{Agent, AgentOptions};

    LazyLock::force(&SNAPSHOTTER);
    let chat = ScriptedChatClient::new(vec![
        Scripted::tools(&[("1", "nope", "{}")]),
        Scripted::text(&["ok"]),
    ]);
    let agent = Arc::new(Agent::<String>::new(
        chat,
        Arc::new(ToolRegistry::new()),
        AgentOptions::new("span-agent"),
    ));
    let graph = StateGraph::<Job>::new()
        .add_agent_node(
            "Investigate",
            agent,
            |_: &Job, _| vec![Message::user("?")],
            |_, result| Job {
                answer: result.output,
            },
        )
        .compile()
        .unwrap();

    graph
        .run(
            "agent-span-thread",
            Job {
                answer: String::new(),
            },
            Arc::new(InMemoryCheckpointStore::new()),
        )
        .await;

    let spans = SPANS.lock().unwrap().0.clone();
    let of_thread = |name: &str| -> Vec<SpanRecord> {
        spans
            .iter()
            .filter(|s| {
                s.name == name
                    && s.fields.get("strata.thread_id").map(String::as_str)
                        == Some("agent-span-thread")
            })
            .cloned()
            .collect()
    };
    let turns = of_thread("strata.model");
    assert_eq!(turns.len(), 2);
    for turn in &turns {
        assert_eq!(
            turn.fields.get("strata.agent").map(String::as_str),
            Some("span-agent")
        );
        assert_eq!(turn.parent, Some("strata.attempt"));
    }
    let tools = of_thread("strata.tool");
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].parent, Some("strata.attempt"));
}

#[tokio::test]
async fn a_route_s_span_holds_its_attempts_and_the_route_metrics_count_them() {
    use datalium_strata_ai::{ChatError, ModelCandidate, ModelRoute};

    let _turn = METRICS.lock().await;
    LazyLock::force(&SNAPSHOTTER);
    let a = ScriptedChatClient::new(vec![Scripted::fails(ChatError::status(
        503,
        None,
        "overloaded",
    ))]);
    let b = ScriptedChatClient::new(vec![Scripted::text(&["ok"])]);
    let route = ModelRoute::new("span-route")
        .candidate(
            ModelCandidate::new("a", a)
                .with_model("m-a")
                .with_health_key("span-a"),
        )
        .candidate(
            ModelCandidate::new("b", b)
                .with_model("m-b")
                .with_health_key("span-b"),
        );
    let graph = StateGraph::<Job>::new()
        .add_model_node(
            "Ask",
            Arc::new(route),
            |_: &Job, _| vec![Message::user("?")],
            |_, response| Job {
                answer: response.text(),
            },
            ModelNodeOptions::new(),
        )
        .compile()
        .unwrap();

    graph
        .run(
            "route-span-thread",
            Job {
                answer: String::new(),
            },
            Arc::new(InMemoryCheckpointStore::new()),
        )
        .await;

    let spans = SPANS.lock().unwrap().0.clone();
    let of_thread = |name: &str| -> Vec<SpanRecord> {
        spans
            .iter()
            .filter(|s| {
                s.name == name
                    && s.fields.get("strata.thread_id").map(String::as_str)
                        == Some("route-span-thread")
            })
            .cloned()
            .collect()
    };
    let routes = of_thread("strata.route");
    assert_eq!(routes.len(), 1);
    let route = &routes[0];
    let field = |name: &str| route.fields.get(name).map(String::as_str);
    assert_eq!(route.parent, Some("strata.attempt"));
    assert_eq!(field("otel.name"), Some("route span-route"));
    assert_eq!(field("strata.route"), Some("span-route"));
    assert_eq!(field("strata.route.answered"), Some("b"));
    assert_eq!(field("strata.route.attempts"), Some("2"));
    assert_eq!(field("error.type"), None);
    let attempts = of_thread("strata.model");
    // Parent, route, candidate, display name.
    type Described<'a> = (
        Option<&'a str>,
        Option<&'a str>,
        Option<&'a str>,
        Option<&'a str>,
    );
    let described: Vec<Described> = attempts
        .iter()
        .map(|s| {
            (
                s.parent,
                s.fields.get("strata.route").map(String::as_str),
                s.fields.get("strata.route.candidate").map(String::as_str),
                s.fields.get("otel.name").map(String::as_str),
            )
        })
        .collect();
    assert_eq!(
        described,
        [
            (
                Some("strata.route"),
                Some("span-route"),
                Some("a"),
                Some("chat m-a")
            ),
            (
                Some("strata.route"),
                Some("span-route"),
                Some("b"),
                Some("chat m-b")
            ),
        ]
    );

    let snapshot = SNAPSHOTTER.snapshot().into_vec();
    let counter = |name: &str, label: &str, value: &str| -> u64 {
        snapshot
            .iter()
            .filter(|(key, ..)| key.key().name() == name)
            .filter(|(key, ..)| {
                key.key()
                    .labels()
                    .any(|l| l.key() == "strata.route" && l.value() == "span-route")
                    && key
                        .key()
                        .labels()
                        .any(|l| l.key() == label && l.value() == value)
            })
            .map(|(.., value)| match value {
                DebugValue::Counter(count) => *count,
                _ => 0,
            })
            .sum()
    };
    assert_eq!(
        counter(metric_names::ROUTE_CALLS, "strata.outcome", "answered"),
        1
    );
    assert_eq!(
        counter(
            metric_names::ROUTE_FAILOVERS,
            "strata.route.reason",
            "failed"
        ),
        1
    );
}

#[tokio::test]
async fn an_embedding_call_s_span_and_metrics_carry_operation_embeddings_and_input_tokens_only() {
    use datalium_strata::context::NodeContext;
    use datalium_strata_ai::{EmbeddingRequest, embed};

    let _turn = METRICS.lock().await;
    LazyLock::force(&SNAPSHOTTER);
    let mut embedder = ScriptedEmbedder::new("scripted", Some("embed-span"), &[("x", &[1.0, 0.0])]);
    embedder.output_tokens = Some(5);
    let embedder = Arc::new(embedder);
    let graph = StateGraph::<Job>::new()
        .add_async_node("Embed", move |job: Job, ctx: NodeContext| {
            let embedder = embedder.clone();
            async move {
                embed(&*embedder, EmbeddingRequest::new(["x"]), &ctx)
                    .await
                    .unwrap();
                job
            }
        })
        .compile()
        .unwrap();

    graph
        .run(
            "embedding-span-thread",
            Job {
                answer: String::new(),
            },
            Arc::new(InMemoryCheckpointStore::new()),
        )
        .await;

    let spans = SPANS.lock().unwrap().0.clone();
    let span = spans
        .iter()
        .find(|s| {
            s.name == "strata.model"
                && s.fields.get("strata.thread_id").map(String::as_str)
                    == Some("embedding-span-thread")
        })
        .expect("a strata.model span");
    let field = |name: &str| span.fields.get(name).map(String::as_str);
    assert_eq!(span.parent, Some("strata.attempt"));
    assert_eq!(field("otel.name"), Some("embeddings embed-span"));
    assert_eq!(field("gen_ai.operation.name"), Some("embeddings"));
    assert_eq!(field("gen_ai.provider.name"), Some("scripted"));
    assert_eq!(field("gen_ai.request.model"), Some("embed-span"));
    assert_eq!(field("gen_ai.response.model"), Some("embed-span"));
    assert_eq!(field("gen_ai.usage.input_tokens"), Some("4"));
    assert_eq!(field("gen_ai.usage.output_tokens"), None);
    assert_eq!(field("gen_ai.request.max_tokens"), None);

    let measured: Vec<(String, Option<String>, DebugValue)> = SNAPSHOTTER
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, ..)| {
            key.key()
                .labels()
                .any(|l| l.key() == "gen_ai.request.model" && l.value() == "embed-span")
        })
        .map(|(key, _, _, value)| {
            let label = |name: &str| {
                key.key()
                    .labels()
                    .find(|l| l.key() == name)
                    .map(|l| l.value().to_string())
            };
            assert_eq!(
                label("gen_ai.operation.name").as_deref(),
                Some("embeddings")
            );
            (
                key.key().name().to_string(),
                label("gen_ai.token.type"),
                value,
            )
        })
        .collect();
    let values = |name: &str, kind: Option<&str>| -> Vec<f64> {
        measured
            .iter()
            .filter(|(n, k, _)| n == name && k.as_deref() == kind)
            .flat_map(|(_, _, value)| match value {
                DebugValue::Histogram(values) => values.iter().map(|v| v.into_inner()).collect(),
                _ => Vec::new(),
            })
            .collect()
    };
    assert_eq!(
        values(metric_names::MODEL_TOKEN_USAGE, Some("input")),
        vec![4.0]
    );
    assert!(values(metric_names::MODEL_TOKEN_USAGE, Some("output")).is_empty());
    assert_eq!(values(metric_names::MODEL_DURATION, None).len(), 1);
}
