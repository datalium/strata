//! The client against the scripted server in memory, behind
//! `McpServer::streams` (`design/50-MCP-Client.md`, "Tests"): era
//! detection, the legacy handshake, `_meta`, listing, results, errors,
//! cancellation and timeouts, through the client and through a registry.
//! Mirrors .NET's `McpStreamsTests`.

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::BoxFuture;
use datalium_strata::CancellationToken;
use datalium_strata::StrataError;
use datalium_strata::tools::{
    Next, StrataTool, ToolCall, ToolCallContext, ToolMiddleware, ToolRegistry,
};
use datalium_strata_mcp::{
    McpClient, McpClientOptions, McpError, McpLogEntry, McpLogLevel, McpRegistryExt, McpServer,
    McpToolOptions, TraceContext, mcp_fault_classifier,
};
use serde_json::{Value, json};
use support::server::{Era, Script, serve};

fn server(script: &Script) -> McpServer {
    let (client_side, server_side) = tokio::io::duplex(1 << 22);
    let (server_read, server_write) = tokio::io::split(server_side);
    tokio::spawn(serve(script.clone(), server_read, server_write));
    let (read, write) = tokio::io::split(client_side);
    McpServer::streams(read, write).into()
}

async fn connect(script: &Script) -> McpClient {
    McpClient::connect(server(script)).await.unwrap()
}

async fn connect_with(script: &Script, options: McpClientOptions) -> Result<McpClient, McpError> {
    McpClient::connect_with(server(script), options).await
}

fn logged() -> (Arc<Mutex<Vec<McpLogEntry>>>, McpClientOptions) {
    let entries = Arc::new(Mutex::new(Vec::new()));
    let sink = entries.clone();
    let options =
        McpClientOptions::new().with_log(move |entry| sink.lock().unwrap().push(entry.clone()));
    (entries, options)
}

/// Waits until the server has received `method`, up to a second.
async fn received(script: &Script, method: &str) -> Vec<Value> {
    for _ in 0..100 {
        let found: Vec<Value> = script
            .received()
            .into_iter()
            .filter(|m| m["method"] == method)
            .collect();
        if !found.is_empty() {
            return found;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Vec::new()
}

// --- Eras ---------------------------------------------------------------

#[tokio::test]
async fn a_modern_server_is_probed_and_every_request_carries_meta() {
    let script = Script::new(Era::Modern);
    let client = connect(&script).await;

    assert_eq!(client.protocol_version(), "2026-07-28");
    assert_eq!(client.server_info().unwrap().name, "fixture");
    assert_eq!(client.server_info().unwrap().version, "1.2.3");
    assert_eq!(client.instructions(), Some("Use echo to echo."));
    assert_eq!(client.tools().len(), 12);
    assert_eq!(script.methods(), ["server/discover", "tools/list"]);

    let result = client
        .call_tool("echo", json!({"text": "hi"}))
        .await
        .unwrap();
    assert_eq!(result.text(), r#"{"text":"hi"}"#);
    for request in script.received() {
        let meta = &request["params"]["_meta"];
        assert_eq!(
            meta["io.modelcontextprotocol/protocolVersion"], "2026-07-28",
            "{request}"
        );
        assert_eq!(
            meta["io.modelcontextprotocol/clientCapabilities"],
            json!({})
        );
        assert_eq!(
            meta["io.modelcontextprotocol/clientInfo"]["name"],
            "datalium-strata"
        );
        assert_eq!(
            meta["io.modelcontextprotocol/clientInfo"]["version"],
            env!("CARGO_PKG_VERSION")
        );
    }
}

#[tokio::test]
async fn each_legacy_version_is_initialized() {
    for version in ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"] {
        let script = Script::new(Era::Legacy(version.to_string()));
        let client = connect(&script).await;

        assert_eq!(client.protocol_version(), version);
        assert_eq!(client.server_info().unwrap().name, "legacy-fixture");
        assert_eq!(client.instructions(), Some("Legacy instructions."));
        assert_eq!(
            script.methods(),
            [
                "server/discover",
                "initialize",
                "notifications/initialized",
                "tools/list"
            ],
            "{version}"
        );
        let initialize = &script.received()[1];
        assert_eq!(initialize["params"]["protocolVersion"], "2025-11-25");
        assert_eq!(initialize["params"]["capabilities"], json!({}));
        assert_eq!(
            initialize["params"]["clientInfo"]["name"],
            "datalium-strata"
        );
        // A legacy request carries no modern keys.
        let list = &script.received()[3];
        assert!(list["params"].get("_meta").is_none(), "{list}");
        assert_eq!(
            client.call_tool("echo", json!({})).await.unwrap().text(),
            "{}"
        );
    }
}

#[tokio::test]
async fn a_legacy_server_that_says_nothing_to_the_probe_is_initialized_after_the_probe_timeout() {
    let script = Script::new(Era::SilentLegacy("2025-06-18".into()));
    let client = connect_with(
        &script,
        McpClientOptions::new().with_probe_timeout(Duration::from_millis(200)),
    )
    .await
    .unwrap();

    assert_eq!(client.protocol_version(), "2025-06-18");
    // The probe isn't cancelled: a legacy server knows nothing of it.
    assert_eq!(
        script.methods(),
        [
            "server/discover",
            "initialize",
            "notifications/initialized",
            "tools/list"
        ]
    );
}

#[tokio::test]
async fn a_dual_era_server_stays_modern() {
    let script = Script::new(Era::Dual);
    let client = connect(&script).await;
    assert_eq!(client.protocol_version(), "2026-07-28");
    assert_eq!(script.methods(), ["server/discover", "tools/list"]);
}

#[tokio::test]
async fn an_unsupported_version_error_picks_a_legacy_version_from_its_list() {
    let script = Script::new(Era::Unsupported(vec![
        "2025-03-26".into(),
        "2025-06-18".into(),
    ]));
    let client = connect(&script).await;
    assert_eq!(client.protocol_version(), "2025-06-18");
    assert_eq!(
        script.received()[1]["params"]["protocolVersion"],
        "2025-06-18"
    );
}

#[tokio::test]
async fn a_server_with_no_version_in_common_fails_connecting_and_is_never_initialized() {
    let script = Script::new(Era::Unsupported(vec!["2099-01-01".into()]));
    let error = connect_with(&script, McpClientOptions::new())
        .await
        .err()
        .unwrap();
    let McpError::Connect(message) = &error else {
        panic!("{error:?}")
    };
    assert!(
        message.contains("2099-01-01") && message.contains("2026-07-28"),
        "{message}"
    );
    // A recognized modern error never falls back to initialize.
    assert_eq!(script.methods(), ["server/discover"]);
}

#[tokio::test]
async fn a_legacy_server_that_answers_with_an_unknown_version_fails_connecting() {
    let script = Script::new(Era::Legacy("2099-01-01".into()));
    let error = connect_with(&script, McpClientOptions::new())
        .await
        .err()
        .unwrap();
    assert!(
        error
            .to_string()
            .contains("'2099-01-01', which Strata doesn't speak"),
        "{error}"
    );
}

// --- Tools ---------------------------------------------------------------

#[tokio::test]
async fn tools_are_listed_across_pages_in_the_server_s_order() {
    let mut script = Script::new(Era::Modern);
    script.page_size = 3;
    let client = connect(&script).await;
    let names: Vec<&str> = client.tools().iter().map(|t| t.name.as_str()).collect();
    assert_eq!(names.len(), 12);
    assert_eq!(names[..3], ["echo", "fail", "slow"]);
    let cursors: Vec<Value> = script
        .received()
        .iter()
        .filter(|m| m["method"] == "tools/list")
        .map(|m| m["params"]["cursor"].clone())
        .collect();
    assert_eq!(cursors[0], Value::Null);
    assert_eq!(cursors[1], "page-1");
    assert_eq!(cursors.len(), 4); // 3, 3, 3, 3
}

#[tokio::test]
async fn a_repeated_cursor_fails_connecting() {
    let mut script = Script::new(Era::Modern);
    script.page_size = 3;
    script.repeat_cursor = true;
    let error = connect_with(&script, McpClientOptions::new())
        .await
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("cursor 'page-1' twice"),
        "{error}"
    );
}

#[tokio::test]
async fn a_repeated_name_keeps_the_first_with_a_warning() {
    let mut script = Script::new(Era::Modern);
    script.duplicate = true;
    let (log, options) = logged();
    let client = connect_with(&script, options).await.unwrap();
    let echoes: Vec<_> = client.tools().iter().filter(|t| t.name == "echo").collect();
    assert_eq!(echoes.len(), 1);
    assert_eq!(
        echoes[0].description.as_deref(),
        Some("Echoes its arguments")
    );
    assert!(
        log.lock()
            .unwrap()
            .iter()
            .any(|e| e.level == McpLogLevel::Warning
                && e.message.contains("listed the tool 'echo' twice"))
    );
}

#[tokio::test]
async fn a_tool_set_takes_a_prefix_and_an_include_and_refuses_an_unknown_one() {
    let script = Script::new(Era::Modern);
    let client = connect(&script).await;

    let set = client
        .tool_set(
            McpToolOptions::new()
                .prefix("fx.")
                .include(["fail", "echo"]),
        )
        .unwrap();
    // The server's order, not the include's.
    assert_eq!(set.names().collect::<Vec<_>>(), ["fx.echo", "fx.fail"]);
    let tools = set.into_tools();
    assert_eq!(tools[1].name(), "fx.fail");
    assert_eq!(
        tools[1].schema(),
        json!({"name": "fx.fail", "description": "Always fails", "parameters": {"type": "object"}})
    );

    let error = client
        .tool_set(McpToolOptions::new().include(["echo", "nope"]))
        .err()
        .unwrap();
    assert!(matches!(error, StrataError::ToolNotFound(name) if name == "nope"));
    assert_eq!(client.tool_set(McpToolOptions::new()).unwrap().len(), 12);
}

// --- Through a registry -----------------------------------------------------

struct Local;

impl StrataTool for Local {
    fn name(&self) -> &'static str {
        "local"
    }

    fn schema(&self) -> Value {
        json!({"name": "local"})
    }

    fn execute<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<String, StrataError>> {
        Box::pin(async { Ok("local".to_string()) })
    }
}

/// Denies `fail`, answers `image` itself, and records what it saw.
struct Policy(Arc<Mutex<Vec<String>>>);

impl ToolMiddleware for Policy {
    fn invoke<'a>(
        &'a self,
        call: ToolCall,
        next: Next<'a>,
    ) -> BoxFuture<'a, Result<String, StrataError>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(call.name().to_string());
            match call.name() {
                "fx.fail" => Err(StrataError::ToolCallDenied {
                    tool: call.name().to_string(),
                    reason: "not today".into(),
                }),
                "fx.image" => Ok("short-circuited".to_string()),
                _ => next.run(call).await,
            }
        })
    }
}

#[tokio::test]
async fn mcp_tools_sit_beside_strata_tools_with_subsets_and_middleware() {
    let script = Script::new(Era::Modern);
    let client = connect(&script).await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(Local));
    registry
        .register_mcp(
            client
                .tool_set(McpToolOptions::new().prefix("fx."))
                .unwrap(),
        )
        .unwrap()
        .use_middleware(Policy(seen.clone()));

    assert_eq!(registry.names().count(), 13);
    assert_eq!(registry.dispatch("local", "{}").await.unwrap(), "local");
    assert_eq!(
        registry
            .dispatch("fx.echo", r#"{"text":"a"}"#)
            .await
            .unwrap(),
        r#"{"text":"a"}"#
    );
    assert!(matches!(
        registry.dispatch("fx.fail", "{}").await,
        Err(StrataError::ToolCallDenied { .. })
    ));
    assert_eq!(
        registry.dispatch("fx.image", "{}").await.unwrap(),
        "short-circuited"
    );
    // Neither the denied nor the short-circuited call reached the server.
    assert!(
        !script
            .received()
            .iter()
            .any(|m| m["params"]["name"] == "fail" || m["params"]["name"] == "image")
    );
    assert_eq!(
        *seen.lock().unwrap(),
        ["local", "fx.echo", "fx.fail", "fx.image"]
    );

    let subset = registry.subset(["fx.echo"]).unwrap();
    assert!(matches!(
        subset.dispatch("fx.slow", "{}").await,
        Err(StrataError::ToolNotPermitted(_))
    ));
}

#[tokio::test]
async fn register_mcp_registers_all_or_nothing() {
    let script = Script::new(Era::Modern);
    let client = connect(&script).await;
    let mut registry = ToolRegistry::new();
    registry
        .register_mcp(
            client
                .tool_set(McpToolOptions::new().include(["slow"]))
                .unwrap(),
        )
        .unwrap();

    let error = registry
        .register_mcp(client.tool_set(McpToolOptions::new()).unwrap())
        .err()
        .unwrap();
    assert!(matches!(error, StrataError::DuplicateTool(name) if name == "slow"));
    assert_eq!(registry.names().collect::<Vec<_>>(), ["slow"]);
}

// --- Results and errors -----------------------------------------------------

fn registry(client: &McpClient) -> ToolRegistry {
    let mut registry = ToolRegistry::new();
    registry
        .register_mcp(client.tool_set(McpToolOptions::new()).unwrap())
        .unwrap();
    registry
}

#[tokio::test]
async fn results_become_text_and_a_tool_s_own_error_is_wrapped_for_the_model() {
    let script = Script::new(Era::Modern);
    let client = connect(&script).await;
    let tools = registry(&client);

    assert_eq!(
        tools.dispatch("fail", "{}").await.unwrap(),
        r#"{"error":"it failed"}"#
    );
    assert_eq!(
        tools.dispatch("structured", "").await.unwrap(),
        r#"{"temperature":22.5}"#
    );
    assert_eq!(
        tools.dispatch("image", "{}").await.unwrap(),
        "[image/png, 1 KB, not shown]"
    );
    let raw = client.call_tool("fail", json!({})).await.unwrap();
    assert!(raw.is_error);
    assert_eq!(raw.text(), "it failed");
    let image = client.call_tool("image", json!({})).await.unwrap();
    assert_eq!(image.content[0]["data"], "iVBORw0KGgo=");
}

#[tokio::test]
async fn a_json_rpc_error_keeps_its_code() {
    let script = Script::new(Era::Modern);
    let client = connect(&script).await;
    let error = client.call_tool("nope", json!({})).await.err().unwrap();
    assert_eq!(
        error,
        McpError::Protocol {
            code: -32602,
            message: "Unknown tool: nope".into()
        }
    );
}

#[tokio::test]
async fn input_required_fails_the_call() {
    let script = Script::new(Era::Modern);
    let client = connect(&script).await;
    let tools = registry(&client);
    let error = tools.dispatch("ask", "{}").await.err().unwrap();
    let StrataError::ToolCallFailed { tool, message } = &error else {
        panic!("{error:?}")
    };
    assert_eq!(tool, "ask");
    assert!(
        message.contains("input_required") && message.contains("elicitation/create"),
        "{message}"
    );
    assert!(mcp_fault_classifier(&error).is_none());
}

#[tokio::test]
async fn arguments_that_aren_t_an_object_are_refused_before_the_server() {
    let script = Script::new(Era::Modern);
    let client = connect(&script).await;
    let tools = registry(&client);
    assert!(matches!(
        tools.dispatch("echo", "[1]").await,
        Err(StrataError::ToolArgumentParseFailure(m)) if m == "the arguments aren't a JSON object"
    ));
}

#[tokio::test]
async fn a_line_that_isn_t_json_ends_the_connection() {
    let script = Script::new(Era::Modern);
    let client = connect(&script).await;
    let tools = registry(&client);
    let error = tools.dispatch("garbage", "{}").await.err().unwrap();
    assert!(error_text(&error).contains("isn't JSON"), "{error:?}");
    assert!(mcp_fault_classifier(&error).is_some());
    // A streams server isn't started again.
    let after = tools.dispatch("echo", "{}").await.err().unwrap();
    assert!(error_text(&after).contains("isn't JSON"), "{after:?}");
}

#[tokio::test]
async fn a_message_bigger_than_the_limit_fails_the_connection() {
    let script = Script::new(Era::Modern);
    let client = connect_with(
        &script,
        McpClientOptions::new().with_max_message_bytes(4096),
    )
    .await
    .unwrap();
    let error = client
        .call_tool("big", json!({"bytes": 10000}))
        .await
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("bigger than 4096 bytes"),
        "{error}"
    );
}

#[tokio::test]
async fn requests_from_a_server_are_answered_and_its_notifications_ignored() {
    let script = Script::new(Era::Legacy("2025-11-25".into()));
    let (log, options) = logged();
    let client = connect_with(&script, options).await.unwrap();
    let answers: Value =
        serde_json::from_str(&client.call_tool("ping_me", json!({})).await.unwrap().text())
            .unwrap();
    assert_eq!(answers[0]["result"], json!({}));
    assert_eq!(answers[1]["error"]["code"], -32601);
    for _ in 0..50 {
        if log
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.message.contains("notifications/message"))
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the notification wasn't logged");
}

#[tokio::test]
async fn calls_run_at_once_on_one_connection() {
    let script = Script::new(Era::Modern);
    let client = connect(&script).await;
    let started = std::time::Instant::now();
    let mut calls = tokio::task::JoinSet::new();
    for _ in 0..5 {
        let client = client.clone();
        calls.spawn(async move { client.call_tool("slow", json!({"ms": 300})).await });
    }
    while let Some(result) = calls.join_next().await {
        assert_eq!(result.unwrap().unwrap().text(), "slept 300");
    }
    assert!(
        started.elapsed() < Duration::from_millis(1200),
        "{:?}",
        started.elapsed()
    );
}

fn error_text(error: &StrataError) -> String {
    match error {
        StrataError::ToolCallFailed { message, .. } => message.clone(),
        other => format!("{other:?}"),
    }
}

// --- Cancellation and timeouts ----------------------------------------------

#[tokio::test]
async fn a_token_that_fires_cancels_the_request_on_the_server() {
    let script = Script::new(Era::Modern);
    let client = connect(&script).await;
    let tools = registry(&client);
    let token = CancellationToken::new();
    let firing = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        firing.cancel();
    });

    let error = tools
        .dispatch_with(
            "slow",
            r#"{"ms": 5000}"#,
            ToolCallContext::new().with_cancellation(token),
        )
        .await
        .err()
        .unwrap();

    assert!(matches!(error, StrataError::ToolCancelled(name) if name == "slow"));
    let call_id = script
        .received()
        .iter()
        .find(|m| m["params"]["name"] == "slow")
        .unwrap()["id"]
        .clone();
    let cancelled = received(&script, "notifications/cancelled").await;
    assert_eq!(cancelled.len(), 1);
    assert_eq!(cancelled[0]["params"]["requestId"], call_id);
    assert_eq!(cancelled[0]["params"]["reason"], "cancelled by the caller");
    assert_eq!(
        cancelled[0]["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"],
        "2026-07-28"
    );
    // The late answer is ignored, and the connection goes on.
    assert_eq!(
        client.call_tool("echo", json!({})).await.unwrap().text(),
        "{}"
    );
}

#[tokio::test]
async fn a_dropped_future_cancels_the_request() {
    let script = Script::new(Era::Legacy("2025-11-25".into()));
    let client = connect(&script).await;
    let abandoned = tokio::time::timeout(
        Duration::from_millis(100),
        client.call_tool("slow", json!({"ms": 5000})),
    )
    .await;
    assert!(abandoned.is_err());
    let cancelled = received(&script, "notifications/cancelled").await;
    assert_eq!(cancelled.len(), 1);
    // Legacy: no modern keys.
    assert!(cancelled[0]["params"].get("_meta").is_none());
}

#[tokio::test]
async fn a_call_timeout_is_a_failure_that_cancels_not_a_cancellation() {
    let script = Script::new(Era::Modern);
    let client = connect_with(
        &script,
        McpClientOptions::new().with_call_timeout(Duration::from_millis(100)),
    )
    .await
    .unwrap();
    let tools = registry(&client);

    let error = tools
        .dispatch("slow", r#"{"ms": 5000}"#)
        .await
        .err()
        .unwrap();
    let StrataError::ToolCallFailed { message, .. } = &error else {
        panic!("{error:?}")
    };
    assert_eq!(message, "mcp request tools/call timed out after 0.1s");
    assert!(matches!(mcp_fault_classifier(&error), Some(n) if n.is_transient()));
    assert_eq!(received(&script, "notifications/cancelled").await.len(), 1);
    assert!(matches!(
        client.call_tool("slow", json!({"ms": 5000})).await,
        Err(McpError::Timeout { .. })
    ));
}

// --- Lifetime and trace context ------------------------------------------------

#[tokio::test]
async fn calls_after_close_fail_as_closed() {
    let script = Script::new(Era::Modern);
    let client = connect(&script).await;
    let tools = registry(&client);
    client.close().await;
    client.close().await;
    assert_eq!(
        client.call_tool("echo", json!({})).await.err(),
        Some(McpError::Closed)
    );
    assert!(matches!(
        tools.dispatch("echo", "{}").await,
        Err(StrataError::ToolCallFailed { message, .. }) if message == "mcp connection closed"
    ));
}

#[tokio::test]
async fn the_trace_context_hook_fills_meta() {
    let script = Script::new(Era::Legacy("2025-11-25".into()));
    let client = connect_with(
        &script,
        McpClientOptions::new().with_trace_context(|| {
            Some(TraceContext {
                traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into(),
                tracestate: Some("vendor=1".into()),
            })
        }),
    )
    .await
    .unwrap();
    let meta: Value =
        serde_json::from_str(&client.call_tool("meta", json!({})).await.unwrap().text()).unwrap();
    assert_eq!(
        meta,
        json!({
            "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            "tracestate": "vendor=1",
        })
    );
}
