//! Samples for <https://stratamesh.dev/guides/tools/mcp-servers/>.
//!
//! The samples connect to `inventory-mcp`, this crate's example
//! `examples/inventory-mcp`: as a stdio child process found on the `PATH` by
//! its name, as a reader's own server would be, and on loopback for HTTP. Its
//! C# twin is `examples/docs-snippets/inventory-mcp`.

#[path = "../../../examples/inventory-mcp/server.rs"]
mod server;

// docs:begin guides/tools/mcp-servers/connect-stdio
use datalium_strata_mcp::{McpClient, McpServer};

// docs:hide
#[tokio::test]
async fn connect_over_stdio() -> Result<(), Box<dyn std::error::Error>> {
    // docs:show
    let inventory = McpClient::connect(
        McpServer::stdio("inventory-mcp")
            // docs:hide
            .env("PATH", inventory_mcp_path())
            // docs:show
            .args(["--warehouse", "east"]),
    )
    .await?;

    let name = inventory
        .server_info()
        .map_or("?", |info| info.name.as_str());
    println!("{name} speaks MCP {}", inventory.protocol_version());
    for tool in inventory.tools() {
        println!(
            "{}: {}",
            tool.name,
            tool.description.as_deref().unwrap_or("")
        );
    }
    // docs:end guides/tools/mcp-servers/connect-stdio

    assert_eq!(name, "inventory-mcp");
    assert_eq!(inventory.protocol_version(), "2026-07-28");
    let listed: Vec<_> = inventory.tools().iter().map(|t| t.name.as_str()).collect();
    assert_eq!(listed, ["lookup_stock", "list_warehouses", "reserve_stock"]);
    inventory.close().await;
    Ok(())
}

#[tokio::test]
async fn give_the_server_its_environment() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = "a-key-from-your-secret-store";
    // docs:begin guides/tools/mcp-servers/environment
    let server = McpServer::stdio("inventory-mcp")
        // docs:hide
        .env("PATH", inventory_mcp_path())
        // docs:show
        .args(["--warehouse", "west"])
        .env("INVENTORY_API_KEY", api_key);
    // docs:end guides/tools/mcp-servers/environment

    let log = Arc::new(Mutex::new(Vec::new()));
    let logged = log.clone();
    let options = McpClientOptions::new()
        .with_log(move |entry| logged.lock().unwrap().push(entry.message.clone()));
    let inventory = McpClient::connect_with(server, options).await?;
    let result = inventory
        .call_tool("lookup_stock", json!({"sku": "AB-12"}))
        .await?;

    assert_eq!(result.text(), "AB-12: 3 units in west");
    assert_eq!(
        first(&log, "stderr: ").await.as_deref(),
        Some("stderr: inventory-mcp: serving west, with an API key")
    );
    inventory.close().await;
    Ok(())
}

#[tokio::test]
async fn set_the_client_s_options() -> Result<(), Box<dyn std::error::Error>> {
    // docs:begin guides/tools/mcp-servers/options
    let options = McpClientOptions::new()
        .with_call_timeout(Duration::from_secs(30)) // tools/call has no time limit of its own
        .with_log(|entry| println!("[mcp] {:?}: {}", entry.level, entry.message));

    let inventory = McpClient::connect_with(
        McpServer::stdio("inventory-mcp")
            // docs:hide
            .env("PATH", inventory_mcp_path())
            // docs:show
            .args(["--warehouse", "east"]),
        options,
    )
    .await?;
    // docs:end guides/tools/mcp-servers/options

    assert_eq!(inventory.tools().len(), 3);
    inventory.close().await;
    Ok(())
}

#[tokio::test]
async fn connect_over_http() -> Result<(), Box<dyn std::error::Error>> {
    let secrets = SecretStore::new([("inventory-mcp-token", "token-1")]);
    let http = InventoryOverHttp::start("token-1").await;
    let endpoint = http.endpoint.clone();
    // docs:begin guides/tools/mcp-servers/connect-http
    let inventory = McpClient::connect(McpServer::http(&endpoint)?.access_token(move || {
        // Called before each request, so the code that owns the token can refresh it.
        let secrets = secrets.clone();
        async move { secrets.get("inventory-mcp-token").await }
    }))
    .await?;
    // docs:end guides/tools/mcp-servers/connect-http

    assert_eq!(inventory.protocol_version(), "2026-07-28");
    assert_eq!(inventory.tools().len(), 3);
    let tokens = http.tokens.lock().unwrap().clone();
    assert!(!tokens.is_empty());
    assert!(tokens.iter().all(|t| t == "Bearer token-1"), "{tokens:?}");
    inventory.close().await;
    Ok(())
}

#[tokio::test]
async fn a_wrong_token_fails_connecting() {
    let http = InventoryOverHttp::start("token-1").await;

    let refused = McpClient::connect(
        McpServer::http(&http.endpoint)
            .unwrap()
            .access_token(|| async { Ok::<_, String>(Some("token-2".to_string())) }),
    )
    .await
    .err()
    .expect("a wrong token is refused");

    assert!(refused.to_string().contains("401"), "{refused}");
}

// docs:begin guides/tools/mcp-servers/register
use datalium_strata::tools::ToolRegistry;
use datalium_strata_mcp::{McpRegistryExt, McpToolOptions};

// docs:hide
#[tokio::test]
async fn register_call_and_close() -> Result<(), Box<dyn std::error::Error>> {
    let inventory = McpClient::connect(
        McpServer::stdio("inventory-mcp")
            .env("PATH", inventory_mcp_path())
            .args(["--warehouse", "east"]),
    )
    .await?;
    // docs:show
    let mut tools = ToolRegistry::new();
    tools
        .register_mcp(
            inventory
                .tool_set(
                    McpToolOptions::new()
                        .prefix("inventory.")
                        .include(["lookup_stock", "list_warehouses"]), // the server's names, before the prefix
                )
                .expect("the server lists both"),
        )
        .expect("the names are free")
        .use_middleware(Audit); // runs for MCP tools as for any other

    for name in tools.names() {
        println!("{name}");
    }
    // docs:end guides/tools/mcp-servers/register

    let names: Vec<_> = tools.names().collect();
    assert_eq!(names.len(), 2);
    assert!(
        names.contains(&"inventory.lookup_stock") && names.contains(&"inventory.list_warehouses"),
        "{names:?}"
    );

    // docs:begin guides/tools/mcp-servers/call
    let support = ToolCallContext::new().with_caller_id("support-agent");

    for (tool, arguments) in [
        ("inventory.lookup_stock", r#"{"sku":"AB-12"}"#),
        ("inventory.lookup_stock", r#"{"sku":"ZZ-99"}"#),
        ("inventory.list_warehouses", "{}"),
    ] {
        let result = tools.dispatch_with(tool, arguments, support.clone()).await;
        println!("{}", result.expect("the server answered"));
    }
    // docs:end guides/tools/mcp-servers/call

    let answered = tools
        .dispatch_with(
            "inventory.lookup_stock",
            r#"{"sku":"ZZ-99"}"#,
            support.clone(),
        )
        .await;
    assert_eq!(answered.unwrap(), r#"{"error":"no SKU 'ZZ-99' in east"}"#);
    let warehouses = tools
        .dispatch_with("inventory.list_warehouses", "{}", support.clone())
        .await;
    assert_eq!(warehouses.unwrap(), r#"{"warehouses":["east","west"]}"#);

    // docs:begin guides/tools/mcp-servers/protocol-error
    match tools
        .dispatch_with("inventory.lookup_stock", "{}", support.clone())
        .await
    {
        Err(StrataError::ToolCallFailed { message, .. }) => println!("failed: {message}"),
        other => println!("{other:?}"),
    }
    // docs:end guides/tools/mcp-servers/protocol-error

    match tools
        .dispatch_with("inventory.lookup_stock", "{}", support.clone())
        .await
    {
        Err(StrataError::ToolCallFailed { message, .. }) => {
            assert_eq!(message, "mcp error -32602: Invalid params: sku is required")
        }
        other => panic!("{other:?}"),
    }

    // docs:begin guides/tools/mcp-servers/close
    // At shutdown, when nothing calls the tools any more.
    inventory.close().await;
    // docs:end guides/tools/mcp-servers/close

    match tools
        .dispatch_with("inventory.lookup_stock", r#"{"sku":"AB-12"}"#, support)
        .await
    {
        Err(StrataError::ToolCallFailed { message, .. }) => {
            assert_eq!(message, "mcp connection closed")
        }
        other => panic!("{other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn an_included_name_the_server_did_not_list_is_refused()
-> Result<(), Box<dyn std::error::Error>> {
    let inventory =
        McpClient::connect(McpServer::stdio("inventory-mcp").env("PATH", inventory_mcp_path()))
            .await?;

    let refused = inventory.tool_set(McpToolOptions::new().include(["restock"]));

    assert!(matches!(refused, Err(StrataError::ToolNotFound(name)) if name == "restock"));
    inventory.close().await;
    Ok(())
}

// docs:begin guides/tools/mcp-servers/call-directly
use serde_json::json;

// docs:hide
#[tokio::test]
async fn call_a_tool() -> Result<(), Box<dyn std::error::Error>> {
    let inventory =
        McpClient::connect(McpServer::stdio("inventory-mcp").env("PATH", inventory_mcp_path()))
            .await?;
    // docs:show
    let result = inventory.call_tool("list_warehouses", json!({})).await?;

    println!(
        "error: {}, blocks: {}",
        result.is_error,
        result.content.len()
    );
    println!("{}", result.structured_content.clone().unwrap_or_default());
    // docs:end guides/tools/mcp-servers/call-directly

    assert!(!result.is_error);
    assert_eq!(result.content.len(), 1);
    assert_eq!(
        result.structured_content,
        Some(json!({"warehouses": ["east", "west"]}))
    );
    inventory.close().await;
    Ok(())
}

// docs:begin guides/tools/mcp-servers/fault-classifier
use datalium_strata_ai::AgentOptions;
use datalium_strata_mcp::mcp_fault_classifier;

// docs:hide
#[test]
fn the_fault_classifier_makes_a_link_failure_transient() {
    // docs:show
    // A link failure, a 5xx or 429, or a timeout becomes transient, so the node's
    // recovery policy can retry it. Anything else stays fatal.
    let options =
        AgentOptions::<String>::new("support-agent").with_fault_classifier(mcp_fault_classifier);
    // docs:end guides/tools/mcp-servers/fault-classifier

    drop(options);
    let exited = StrataError::ToolCallFailed {
        tool: "inventory.lookup_stock".into(),
        message: McpError::Transport("the server exited".into()).to_string(),
    };
    assert!(matches!(
        mcp_fault_classifier(&exited),
        Some(NodeError::Transient(_))
    ));
    let refused = StrataError::ToolCallFailed {
        tool: "inventory.lookup_stock".into(),
        message: McpError::Protocol {
            code: -32602,
            message: "Invalid params: sku is required".into(),
        }
        .to_string(),
    };
    assert!(mcp_fault_classifier(&refused).is_none());
}

use std::collections::HashMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use datalium_strata::BoxFuture;
use datalium_strata::StrataError;
use datalium_strata::graph::NodeError;
use datalium_strata::tools::{Next, ToolCall, ToolCallContext, ToolMiddleware};
use datalium_strata_mcp::{McpClientOptions, McpError};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// The `PATH` with `inventory-mcp`'s directory first, so
/// `McpServer::stdio("inventory-mcp")` finds it by name, as it would a server
/// you installed. Cargo builds the example only when a command builds
/// examples, so ask it (`CARGO`, set for tests) to build it, a no-op when it is
/// up to date, and take the directory it reports.
fn inventory_mcp_path() -> OsString {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    let dir = DIR.get_or_init(|| {
        let output = std::process::Command::new(env!("CARGO"))
            .args(["build", "--quiet", "--message-format=json", "--example"])
            .arg("inventory-mcp")
            .arg("--manifest-path")
            .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
            .output()
            .expect("cargo runs");
        assert!(
            output.status.success(),
            "building inventory-mcp failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|message| message["target"]["name"] == "inventory-mcp")
            .find_map(|message| message["executable"].as_str().map(PathBuf::from))
            .and_then(|executable| executable.parent().map(PathBuf::from))
            .expect("cargo reports the example's executable")
    });
    let path = std::env::var_os("PATH").unwrap_or_default();
    std::env::join_paths(std::iter::once(dir.clone()).chain(std::env::split_paths(&path)))
        .expect("the PATH can be joined")
}

/// The first log message that starts with `prefix`, waiting for it: a
/// server's stderr arrives on a reader of its own.
async fn first(log: &Mutex<Vec<String>>, prefix: &str) -> Option<String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Some(found) = log
            .lock()
            .unwrap()
            .iter()
            .find(|message| message.starts_with(prefix))
        {
            return Some(found.clone());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    None
}

/// The audit log of /guides/tools/tool-middleware/, which keeps its own
/// private.
struct Audit;

impl ToolMiddleware for Audit {
    fn invoke<'a>(
        &'a self,
        call: ToolCall,
        next: Next<'a>,
    ) -> BoxFuture<'a, Result<String, StrataError>> {
        Box::pin(async move {
            let caller = call.context().caller_id().unwrap_or("-").to_string();
            let tool = call.name().to_string();
            let clock = Instant::now();
            let result = next.run(call).await;
            let outcome = match &result {
                Ok(_) => "succeeded".to_string(),
                Err(StrataError::ToolCallFailed { message, .. }) => format!("failed ({message})"),
                Err(other) => format!("failed ({other:?})"),
            };
            println!(
                "[audit] {caller} called {tool}: {outcome} in {} ms",
                clock.elapsed().as_millis()
            );
            result
        })
    }
}

/// Stands in for your secret store.
#[derive(Clone)]
struct SecretStore(Arc<HashMap<String, String>>);

impl SecretStore {
    fn new<const N: usize>(secrets: [(&str, &str); N]) -> Self {
        Self(Arc::new(
            secrets
                .into_iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
        ))
    }

    async fn get(&self, name: &str) -> Result<Option<String>, String> {
        Ok(self.0.get(name).cloned())
    }
}

/// The inventory server on `127.0.0.1`, one request per connection, answering
/// `401` to a request without the token.
struct InventoryOverHttp {
    endpoint: String,
    /// The `Authorization` header of each request.
    tokens: Arc<Mutex<Vec<String>>>,
}

impl InventoryOverHttp {
    async fn start(expected_token: &'static str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
        let tokens = Arc::new(Mutex::new(Vec::new()));
        let seen = tokens.clone();
        tokio::spawn(async move {
            let server = Arc::new(server::InventoryServer::new("east"));
            while let Ok((socket, _)) = listener.accept().await {
                tokio::spawn(answer(socket, server.clone(), expected_token, seen.clone()));
            }
        });
        Self { endpoint, tokens }
    }
}

async fn answer(
    mut socket: TcpStream,
    server: Arc<server::InventoryServer>,
    expected_token: &str,
    tokens: Arc<Mutex<Vec<String>>>,
) {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(read) => buffer.extend_from_slice(&chunk[..read]),
        }
        if let Some(end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            break end;
        }
    };
    let head = String::from_utf8_lossy(&buffer[..head_end]).to_string();
    let header = |name: &str| {
        head.lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(have, _)| have.trim().eq_ignore_ascii_case(name))
            .map(|(_, value)| value.trim().to_string())
    };
    let length: usize = header("content-length")
        .and_then(|l| l.parse().ok())
        .unwrap_or(0);
    let authorization = header("authorization").unwrap_or_default();
    let mut body = buffer[head_end + 4..].to_vec();
    while body.len() < length {
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(read) => body.extend_from_slice(&chunk[..read]),
        }
    }
    tokens.lock().unwrap().push(authorization.clone());

    let (status, json) = if authorization != format!("Bearer {expected_token}") {
        ("401 Unauthorized", None)
    } else {
        match serde_json::from_slice::<Value>(&body)
            .ok()
            .and_then(|message| server.answer(&message))
        {
            Some(answer) => ("200 OK", Some(answer.to_string())),
            None => ("202 Accepted", None),
        }
    };
    let body = json.unwrap_or_default();
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = socket.write_all(response.as_bytes()).await;
    let _ = socket.shutdown().await;
}
