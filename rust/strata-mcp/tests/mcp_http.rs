//! The client over Streamable HTTP against a loopback server
//! (`design/50-MCP-Client.md`, "Streamable HTTP", "Authorization" and
//! "Tests"): both eras, JSON and SSE answers, the mirrored headers and
//! `x-mcp-header`, legacy sessions, the token hook, and cancellation.
//! Mirrors .NET's `McpHttpTests`.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::CancellationToken;
use datalium_strata::StrataError;
use datalium_strata::tools::{ToolCallContext, ToolRegistry};
use datalium_strata_mcp::{
    McpClient, McpClientOptions, McpError, McpLogLevel, McpRegistryExt, McpServer, McpToolOptions,
    mcp_fault_classifier,
};
use serde_json::{Value, json};
use support::http::{HttpFixture, Mode, State};

async fn connect(fixture: &HttpFixture) -> McpClient {
    McpClient::connect(McpServer::http(&fixture.url).unwrap())
        .await
        .unwrap()
}

fn registry(client: &McpClient) -> ToolRegistry {
    let mut registry = ToolRegistry::new();
    registry
        .register_mcp(client.tool_set(McpToolOptions::new()).unwrap())
        .unwrap();
    registry
}

async fn eventually(check: impl Fn() -> bool) -> bool {
    for _ in 0..200 {
        if check() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

#[tokio::test]
async fn a_modern_server_gets_the_mirrored_headers_on_every_request() {
    let fixture = HttpFixture::start(Mode::Modern).await;
    let warnings = Arc::new(Mutex::new(Vec::new()));
    let sink = warnings.clone();
    let client = McpClient::connect_with(
        McpServer::http(&fixture.url).unwrap(),
        McpClientOptions::new().with_log(move |entry| {
            if entry.level == McpLogLevel::Warning {
                sink.lock().unwrap().push(entry.message.clone());
            }
        }),
    )
    .await
    .unwrap();

    assert_eq!(client.protocol_version(), "2026-07-28");
    assert_eq!(client.server_info().unwrap().name, "http-fixture");
    // The tool whose annotation breaks the rules is left out, with a warning.
    assert!(!client.tools().iter().any(|t| t.name == "bad_header"));
    assert_eq!(client.tools().len(), 6);
    assert_eq!(
        *warnings.lock().unwrap(),
        [
            "left out the tool 'bad_header': x-mcp-header on 'price' is on a \"number\" property: only string, integer and boolean can be headers"
        ]
    );

    let ran = client
        .call_tool(
            "execute_sql",
            json!({"region": "us-west1", "query": "SELECT 1"}),
        )
        .await
        .unwrap();
    assert_eq!(ran.text(), "ran in \"us-west1\"");
    for seen in fixture.seen() {
        assert_eq!(seen.header("mcp-protocol-version"), Some("2026-07-28"));
        assert_eq!(seen.header("mcp-method"), Some(seen.method()));
        assert_eq!(
            seen.header("accept"),
            Some("application/json, text/event-stream")
        );
        assert_eq!(seen.header("content-type"), Some("application/json"));
        assert!(seen.header("mcp-session-id").is_none());
    }
    let call = &fixture.posts("tools/call")[0];
    assert_eq!(call.header("mcp-name"), Some("execute_sql"));
    assert_eq!(call.header("mcp-param-region"), Some("us-west1"));

    // A name that isn't plain ASCII is sent Base64-encoded; the server
    // decodes it, finds no such tool, and says so.
    let unknown = client.call_tool("naïve", json!({})).await.err().unwrap();
    assert_eq!(
        unknown,
        McpError::Protocol {
            code: -32602,
            message: "Unknown tool: naïve".into()
        }
    );
    assert_eq!(
        fixture.posts("tools/call")[1].header("mcp-name"),
        Some("=?base64?bmHDr3Zl?=")
    );
}

#[tokio::test]
async fn answers_may_come_as_server_sent_events() {
    for mode in [Mode::Modern, Mode::Legacy("2025-11-25".into())] {
        let fixture = HttpFixture::start_with(
            mode,
            State {
                sse: true,
                ..State::default()
            },
        )
        .await;
        let client = connect(&fixture).await;
        assert_eq!(
            client.tools().len(),
            if fixture.mode == Mode::Modern { 6 } else { 7 }
        );
        let echoed = client.call_tool("echo", json!({"a": 1})).await.unwrap();
        assert_eq!(echoed.text(), r#"{"a":1}"#);
        client.close().await;
    }
}

#[tokio::test]
async fn a_header_mismatch_lists_the_tools_again_and_retries_once() {
    let fixture = HttpFixture::start_with(
        Mode::Modern,
        State {
            annotate_later: true,
            ..State::default()
        },
    )
    .await;
    let client = connect(&fixture).await;
    let ran = client
        .call_tool("execute_sql", json!({"region": "eu", "query": "x"}))
        .await
        .unwrap();
    assert_eq!(ran.text(), "ran in \"eu\"");
    let calls = fixture.posts("tools/call");
    assert_eq!(calls.len(), 2);
    assert!(calls[0].header("mcp-param-region").is_none());
    assert_eq!(calls[1].header("mcp-param-region"), Some("eu"));
    assert_eq!(fixture.posts("tools/list").len(), 2);
}

#[tokio::test]
async fn a_legacy_server_gets_a_session_which_is_sent_and_deleted() {
    let fixture = HttpFixture::start(Mode::Legacy("2025-06-18".into())).await;
    let client = connect(&fixture).await;
    assert_eq!(client.protocol_version(), "2025-06-18");
    assert_eq!(
        client.tools().len(),
        7,
        "legacy HTTP doesn't read x-mcp-header"
    );
    client.call_tool("echo", json!({})).await.unwrap();
    client.close().await;

    let seen = fixture.seen();
    let methods: Vec<String> = seen
        .iter()
        .map(|s| format!("{} {}", s.verb, s.method()))
        .collect();
    assert_eq!(
        methods,
        [
            "POST server/discover",
            "POST initialize",
            "POST notifications/initialized",
            "POST tools/list",
            "POST tools/call",
            "DELETE ",
        ]
    );
    assert!(seen[1].header("mcp-session-id").is_none());
    assert!(
        seen[1].header("mcp-protocol-version").is_none(),
        "not agreed yet"
    );
    for later in &seen[2..] {
        assert_eq!(
            later.header("mcp-session-id"),
            Some("session-1"),
            "{}",
            later.method()
        );
        assert_eq!(later.header("mcp-protocol-version"), Some("2025-06-18"));
        assert!(later.header("mcp-method").is_none());
    }
}

#[tokio::test]
async fn a_session_that_ended_is_initialized_again_and_the_request_retried() {
    let fixture = HttpFixture::start_with(
        Mode::Legacy("2025-11-25".into()),
        State {
            expire_after: Some(1),
            ..State::default()
        },
    )
    .await;
    let client = connect(&fixture).await;
    let echoed = client.call_tool("echo", json!({"n": 1})).await.unwrap();
    assert_eq!(echoed.text(), r#"{"n":1}"#);
    assert_eq!(fixture.state.lock().unwrap().sessions, 2);
    let calls = fixture.posts("tools/call");
    assert_eq!(calls[0].header("mcp-session-id"), Some("session-1"));
    assert_eq!(calls[1].header("mcp-session-id"), Some("session-2"));
}

#[tokio::test]
async fn a_legacy_server_s_request_on_the_stream_is_answered_with_a_post() {
    let fixture = HttpFixture::start(Mode::Legacy("2025-11-25".into())).await;
    let client = connect(&fixture).await;
    let answered: Value =
        serde_json::from_str(&client.call_tool("ping_me", json!({})).await.unwrap().text())
            .unwrap();
    assert_eq!(
        answered,
        json!({"jsonrpc": "2.0", "id": "srv-1", "result": {}})
    );
}

#[tokio::test]
async fn the_token_hook_runs_before_every_request() {
    let fixture = HttpFixture::start(Mode::Modern).await;
    let count = Arc::new(AtomicUsize::new(0));
    let counter = count.clone();
    let client = McpClient::connect(
        McpServer::http(&fixture.url)
            .unwrap()
            .header("X-Tenant", "acme")
            .access_token(move || {
                let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
                async move { Ok::<_, String>(Some(format!("t{n}"))) }
            }),
    )
    .await
    .unwrap();
    client.call_tool("echo", json!({})).await.unwrap();
    let tokens: Vec<String> = fixture
        .seen()
        .iter()
        .map(|s| s.header("authorization").unwrap().to_string())
        .collect();
    assert_eq!(tokens, ["Bearer t1", "Bearer t2", "Bearer t3"]);
    assert!(
        fixture
            .seen()
            .iter()
            .all(|s| s.header("x-tenant") == Some("acme"))
    );
}

#[tokio::test]
async fn a_failing_hook_fails_the_request_and_no_token_appears_in_errors() {
    let fixture = HttpFixture::start(Mode::Modern).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let client = McpClient::connect(McpServer::http(&fixture.url).unwrap().access_token(
        move || {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    Ok(Some("s3cret-token".to_string()))
                } else {
                    Err("the vault is sealed")
                }
            }
        },
    ))
    .await
    .unwrap();
    let tools = registry(&client);
    let error = tools.dispatch("echo", "{}").await.err().unwrap();
    let StrataError::ToolCallFailed { message, .. } = &error else {
        panic!("{error:?}")
    };
    assert!(
        message.contains("the access token hook failed: the vault is sealed"),
        "{message}"
    );
    assert!(!message.contains("s3cret"));
}

#[tokio::test]
async fn unauthorized_fails_connecting_with_the_status() {
    let fixture = HttpFixture::start_with(
        Mode::Modern,
        State {
            require_token: Some("right".into()),
            ..State::default()
        },
    )
    .await;
    let error = McpClient::connect(
        McpServer::http(&fixture.url)
            .unwrap()
            .access_token(|| async { Ok::<_, String>(Some("wrong".into())) }),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error, McpError::Connect("mcp unauthorized (401)".into()));
    assert!(!error.to_string().contains("wrong"));
}

#[tokio::test]
async fn a_5xx_is_a_transient_failure() {
    let fixture = HttpFixture::start(Mode::Modern).await;
    let client = connect(&fixture).await;
    let tools = registry(&client);
    let error = tools.dispatch("boom", "{}").await.err().unwrap();
    let StrataError::ToolCallFailed { message, .. } = &error else {
        panic!("{error:?}")
    };
    assert_eq!(message, "mcp transport error: HTTP 503: busy");
    assert!(mcp_fault_classifier(&error).is_some_and(|n| n.is_transient()));
    assert_eq!(
        tools.dispatch("fail", "{}").await.unwrap(),
        r#"{"error":"it failed"}"#
    );
}

#[tokio::test]
async fn cancelling_aborts_the_request_and_legacy_also_posts_cancelled() {
    for mode in [Mode::Modern, Mode::Legacy("2025-11-25".into())] {
        let fixture = HttpFixture::start(mode.clone()).await;
        let client = connect(&fixture).await;
        let tools = registry(&client);
        let token = CancellationToken::new();
        let firing = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
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
        assert!(matches!(error, StrataError::ToolCancelled(_)), "{error:?}");
        let state = fixture.state.clone();
        assert!(
            eventually(|| !state.lock().unwrap().aborted.is_empty()).await,
            "{mode:?}"
        );
        let cancelled = fixture.posts("notifications/cancelled");
        match mode {
            Mode::Modern => assert!(cancelled.is_empty()),
            Mode::Legacy(_) => {
                let fixture = &fixture;
                assert!(eventually(|| !fixture.posts("notifications/cancelled").is_empty()).await);
                let cancelled = &fixture.posts("notifications/cancelled")[0];
                assert_eq!(cancelled.header("mcp-session-id"), Some("session-1"));
                assert_eq!(
                    cancelled.body["params"]["requestId"],
                    fixture.posts("tools/call")[0].body["id"]
                );
            }
        }
    }
}

#[tokio::test]
async fn a_server_nobody_answers_at_fails_connecting() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    drop(listener);
    let error = McpClient::connect(McpServer::http(&url).unwrap())
        .await
        .err()
        .unwrap();
    assert!(
        matches!(&error, McpError::Connect(m) if m.starts_with("mcp transport error")),
        "{error:?}"
    );
    assert!(McpServer::http("not a url").is_err());
    assert!(McpServer::http("ftp://example.com/mcp").is_err());
}
