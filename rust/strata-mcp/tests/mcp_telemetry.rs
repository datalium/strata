//! Each MCP request's client span and `mcp.client.operation.duration`
//! (`design/50-MCP-Client.md`, "Telemetry"): names, kind, parent and
//! attributes per outcome. Its own binary, since it installs a global
//! subscriber and recorder. Mirrors .NET's `McpTelemetryTests`.

mod support;

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use datalium_strata::tools::ToolRegistry;
use datalium_strata_mcp::{
    McpClient, McpRegistryExt, McpServer, McpToolOptions, OPERATION_DURATION,
};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde_json::json;
use support::server::{Era, Script, serve};
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

static SNAPSHOTTER: LazyLock<Snapshotter> = LazyLock::new(|| {
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Capture)).unwrap();
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().unwrap();
    snapshotter
});

fn mcp_spans(otel_name: &str) -> Vec<SpanRecord> {
    SPANS
        .lock()
        .unwrap()
        .0
        .iter()
        .filter(|s| {
            s.name == "mcp.request"
                && s.fields.get("otel.name").map(String::as_str) == Some(otel_name)
        })
        .cloned()
        .collect()
}

#[tokio::test]
async fn each_request_is_a_client_span_under_the_tool_span_and_is_measured() {
    LazyLock::force(&SNAPSHOTTER);
    let script = Script::new(Era::Modern);
    let (client_side, server_side) = tokio::io::duplex(1 << 20);
    let (server_read, server_write) = tokio::io::split(server_side);
    tokio::spawn(serve(script.clone(), server_read, server_write));
    let (read, write) = tokio::io::split(client_side);
    let client = McpClient::connect(McpServer::streams(read, write))
        .await
        .unwrap();
    let mut registry = ToolRegistry::new();
    registry
        .register_mcp(
            client
                .tool_set(McpToolOptions::new().prefix("fx."))
                .unwrap(),
        )
        .unwrap();

    registry.dispatch("fx.echo", "{}").await.unwrap();
    registry.dispatch("fx.fail", "{}").await.unwrap();
    let _ = client.call_tool("nope", json!({})).await;

    let discover = &mcp_spans("server/discover")[0];
    assert_eq!(discover.fields["mcp.method.name"], "server/discover");
    assert_eq!(discover.fields["mcp.protocol.version"], "2026-07-28");
    assert!(!mcp_spans("tools/list").is_empty());

    let echo = &mcp_spans("tools/call echo")[0];
    let field = |name: &str| echo.fields.get(name).map(String::as_str);
    assert_eq!(echo.parent, Some("strata.tool"));
    assert_eq!(field("otel.kind"), Some("client"));
    assert_eq!(field("mcp.method.name"), Some("tools/call"));
    assert_eq!(field("gen_ai.tool.name"), Some("echo"), "the server's name");
    assert_eq!(field("gen_ai.operation.name"), Some("execute_tool"));
    assert_eq!(field("network.transport"), Some("pipe"));
    assert!(field("jsonrpc.request.id").is_some_and(|id| id.parse::<u64>().is_ok()));
    assert_eq!(field("error.type"), None);

    let fail = &mcp_spans("tools/call fail")[0];
    assert_eq!(
        fail.fields.get("error.type").map(String::as_str),
        Some("tool_error")
    );
    assert_eq!(
        fail.fields.get("otel.status_code"),
        None,
        "a tool's own error isn't the span's"
    );

    let nope = &mcp_spans("tools/call nope")[0];
    assert_eq!(nope.parent, None);
    assert_eq!(nope.fields["error.type"], "-32602");
    assert_eq!(nope.fields["rpc.status_code"], "-32602");
    assert_eq!(nope.fields["otel.status_code"], "ERROR");
    assert_eq!(nope.fields["otel.status_message"], "Unknown tool: nope");

    let measured: Vec<(Vec<(String, String)>, usize)> = SNAPSHOTTER
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, ..)| key.key().name() == OPERATION_DURATION)
        .map(|(key, _, _, value)| {
            let labels = key
                .key()
                .labels()
                .map(|l| (l.key().to_string(), l.value().to_string()))
                .collect();
            let count = match value {
                DebugValue::Histogram(values) => values.len(),
                _ => 0,
            };
            (labels, count)
        })
        .collect();
    let with = |tool: &str, error: Option<&str>| {
        measured.iter().any(|(labels, count)| {
            *count > 0
                && labels.contains(&("gen_ai.tool.name".into(), tool.into()))
                && labels.contains(&("mcp.method.name".into(), "tools/call".into()))
                && labels.contains(&("network.transport".into(), "pipe".into()))
                && labels.contains(&("mcp.protocol.version".into(), "2026-07-28".into()))
                && error.is_none_or(|e| labels.contains(&("error.type".into(), e.into())))
        })
    };
    assert!(with("echo", None), "{measured:?}");
    assert!(with("fail", Some("tool_error")), "{measured:?}");
    assert!(with("nope", Some("-32602")), "{measured:?}");
}
