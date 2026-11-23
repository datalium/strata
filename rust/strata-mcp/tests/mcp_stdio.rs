//! The client against the `mcp_fixture_server` example as a child process
//! (`design/50-MCP-Client.md`, "stdio" and "Tests"): starting, stderr,
//! restart after an exit, and shutdown that stops the whole process tree,
//! from `close` and from `Drop`. Mirrors .NET's `McpStdioTests`.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use datalium_strata_mcp::{
    McpClient, McpClientOptions, McpError, McpLogLevel, McpServer, StdioServer,
};
use serde_json::json;

/// The example's binary: `cargo test` builds examples beside the tests.
fn fixture() -> PathBuf {
    let deps = std::env::current_exe().unwrap();
    let debug = deps.parent().unwrap().parent().unwrap();
    debug.join("examples").join(format!(
        "mcp_fixture_server{}",
        std::env::consts::EXE_SUFFIX
    ))
}

fn stdio(args: &[&str]) -> StdioServer {
    McpServer::stdio(fixture()).args(args.iter().copied())
}

fn temp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("strata-mcp-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn alive(pid: u32) -> bool {
    #[cfg(windows)]
    {
        let out = std::process::Command::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/NH"])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).contains(&pid.to_string())
    }
    #[cfg(not(windows))]
    {
        std::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            .status()
            .unwrap()
            .success()
    }
}

async fn gone(pids: &[u32]) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if pids.iter().all(|pid| !alive(*pid)) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// The server's pid and its child's, once it wrote them.
async fn pids(file: &PathBuf) -> Vec<u32> {
    for _ in 0..100 {
        if let Ok(text) = std::fs::read_to_string(file) {
            let pids: Vec<u32> = text
                .split_whitespace()
                .filter_map(|p| p.parse().ok())
                .collect();
            if pids.len() == 2 {
                return pids;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no pid file");
}

#[tokio::test]
async fn a_stdio_server_is_started_probed_called_and_closed() {
    for era in ["modern", "legacy:2025-06-18", "legacy:2024-11-05"] {
        let client = McpClient::connect(stdio(&[era])).await.unwrap();
        assert_eq!(client.tools().len(), 12, "{era}");
        let result = client
            .call_tool("echo", json!({"text": "über"}))
            .await
            .unwrap();
        assert_eq!(result.text(), r#"{"text":"über"}"#);
        let started = Instant::now();
        client.close().await;
        // The server exits when its stdin closes: no waiting for the timeout.
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "{era}: {:?}",
            started.elapsed()
        );
        assert_eq!(
            client.call_tool("echo", json!({})).await.err(),
            Some(McpError::Closed)
        );
    }
}

#[tokio::test]
async fn stderr_goes_to_the_log() {
    let lines = Arc::new(Mutex::new(Vec::new()));
    let sink = lines.clone();
    let client = McpClient::connect_with(
        stdio(&["modern", "--stderr", "hello from stderr"]),
        McpClientOptions::new()
            .with_log(move |entry| sink.lock().unwrap().push(entry.message.clone())),
    )
    .await
    .unwrap();
    for _ in 0..100 {
        if lines
            .lock()
            .unwrap()
            .iter()
            .any(|l| l == "stderr: hello from stderr")
        {
            client.close().await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("stderr wasn't logged: {:?}", lines.lock().unwrap());
}

#[tokio::test]
async fn a_server_that_exits_fails_the_call_in_flight_and_is_started_again_for_the_next() {
    let entries = Arc::new(Mutex::new(Vec::new()));
    let sink = entries.clone();
    let client = McpClient::connect_with(
        stdio(&["modern"]),
        McpClientOptions::new().with_log(move |entry| sink.lock().unwrap().push(entry.clone())),
    )
    .await
    .unwrap();
    let error = client.call_tool("crash", json!({})).await.err().unwrap();
    assert_eq!(error, McpError::Transport("the server exited".into()));
    assert!(error.is_transient());

    let result = client
        .call_tool("echo", json!({"again": true}))
        .await
        .unwrap();
    assert_eq!(result.text(), r#"{"again":true}"#);
    assert!(
        entries
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.level == McpLogLevel::Information
                && e.message
                    == "starting the MCP server again: mcp transport error: the server exited")
    );
    client.close().await;
}

#[tokio::test]
async fn without_restart_a_server_that_exited_stays_down() {
    let client = McpClient::connect_with(
        stdio(&["modern"]),
        McpClientOptions::new().with_restart_on_exit(false),
    )
    .await
    .unwrap();
    let _ = client.call_tool("crash", json!({})).await;
    assert_eq!(
        client.call_tool("echo", json!({})).await.err(),
        Some(McpError::Transport("the server exited".into()))
    );
    client.close().await;
}

#[tokio::test]
async fn close_stops_a_server_that_won_t_exit_and_the_processes_it_started() {
    let dir = temp("tree-close");
    let pid_file = dir.join("pids");
    let client = McpClient::connect(
        stdio(&[
            "modern",
            "--stubborn",
            "--child",
            pid_file.to_str().unwrap(),
        ])
        .shutdown_timeout(Duration::from_millis(300)),
    )
    .await
    .unwrap();
    let pids = pids(&pid_file).await;
    assert!(pids.iter().all(|pid| alive(*pid)));

    client.close().await;

    assert!(gone(&pids).await, "still running: {pids:?}");
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn dropping_the_last_handle_stops_the_process_tree() {
    let dir = temp("tree-drop");
    let pid_file = dir.join("pids");
    let client = McpClient::connect(stdio(&["modern", "--child", pid_file.to_str().unwrap()]))
        .await
        .unwrap();
    let pids = pids(&pid_file).await;
    let copy = client.clone();
    drop(client);
    assert!(pids.iter().all(|pid| alive(*pid)), "a clone keeps it");
    drop(copy);

    assert!(gone(&pids).await, "still running: {pids:?}");
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn a_command_that_won_t_start_fails_connecting() {
    let error = McpClient::connect(McpServer::stdio("strata-no-such-command-anywhere"))
        .await
        .err()
        .unwrap();
    assert!(
        matches!(&error, McpError::Connect(m) if m.starts_with("couldn't start strata-no-such-command-anywhere")),
        "{error:?}"
    );
}

#[tokio::test]
async fn the_environment_is_inherited_added_to_and_withheld() {
    // SAFETY: set before any server starts, read only by the children.
    unsafe { std::env::set_var("STRATA_MCP_INHERITED", "from the parent") };
    let env = |client: McpClient, name: &'static str| async move {
        client
            .call_tool("env", json!({"name": name}))
            .await
            .unwrap()
            .text()
    };

    let inheriting = McpClient::connect(stdio(&["modern"]).env("STRATA_MCP_GIVEN", "given"))
        .await
        .unwrap();
    assert_eq!(
        env(inheriting.clone(), "STRATA_MCP_INHERITED").await,
        "from the parent"
    );
    assert_eq!(env(inheriting.clone(), "STRATA_MCP_GIVEN").await, "given");
    inheriting.close().await;

    let removing = McpClient::connect(stdio(&["modern"]).env_remove("STRATA_MCP_INHERITED"))
        .await
        .unwrap();
    assert_eq!(
        env(removing.clone(), "STRATA_MCP_INHERITED").await,
        "<unset>"
    );
    removing.close().await;

    let withheld = McpClient::connect(
        stdio(&["modern"])
            .inherit_env(false)
            .env("STRATA_MCP_GIVEN", "given"),
    )
    .await
    .unwrap();
    assert_eq!(
        env(withheld.clone(), "STRATA_MCP_INHERITED").await,
        "<unset>"
    );
    assert_eq!(env(withheld.clone(), "STRATA_MCP_GIVEN").await, "given");
    assert_ne!(env(withheld.clone(), "PATH").await, "<unset>");
    withheld.close().await;
}

/// A `.cmd` found through `PATHEXT` runs through `cmd.exe`, with its
/// arguments quoted for it: spaces and `&` reach the server intact.
#[cfg(windows)]
#[tokio::test]
async fn a_cmd_found_through_pathext_runs_with_its_arguments_intact() {
    let dir = temp("pathext");
    std::fs::write(
        dir.join("strata-mcp-fixture.cmd"),
        format!("@echo off\r\n\"{}\" %*\r\n", fixture().display()),
    )
    .unwrap();
    let path = format!("{};{}", dir.display(), std::env::var("PATH").unwrap());
    let lines = Arc::new(Mutex::new(Vec::new()));
    let sink = lines.clone();
    let client = McpClient::connect_with(
        McpServer::stdio("strata-mcp-fixture")
            .env("PATH", path)
            .args(["modern", "--stderr", "a b & c"]),
        McpClientOptions::new()
            .with_log(move |entry| sink.lock().unwrap().push(entry.message.clone())),
    )
    .await
    .unwrap();
    assert_eq!(client.tools().len(), 12);
    for _ in 0..100 {
        if lines.lock().unwrap().iter().any(|l| l == "stderr: a b & c") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        lines.lock().unwrap().iter().any(|l| l == "stderr: a b & c"),
        "{:?}",
        lines.lock().unwrap()
    );
    client.close().await;
    let _ = std::fs::remove_dir_all(dir);
}
