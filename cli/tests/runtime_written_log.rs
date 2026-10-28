//! Until the runtimes' SQLite stores adopted the shared checkpoint-log
//! layout, the CLI could only read a database it had seeded itself
//! (`tests/checkpoint_replay_interop.rs` inserts the shared fixture through
//! `cli::store`). This test closes that gap for the Rust runtime: a real
//! graph is run and resumed through `SqliteCheckpointStore`, and the `strata`
//! binary then reads the file that store wrote -- no seeding through
//! `cli::store` at all.
//!
//! The .NET runtime's half of the same claim can't be tested from here (it
//! needs a live .NET process); `strata-demos-*`'s `09-cross-language-interop`
//! proves it end to end, and `EfCoreCheckpointStoreTests` pins its raw row
//! layout against the same fixtures this crate's `checkpoint_log_layout.rs`
//! uses.

use std::collections::HashMap;
use std::process::Command;
use std::sync::Arc;

use datalium_strata::checkpoint::{GraphExecutionResult, HumanSignal};
use datalium_strata::graph::StateGraph;
use datalium_strata_memory::checkpoint::SqliteCheckpointStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireTransferState {
    invoice_generated: bool,
    amount: f64,
    approved_by: Option<String>,
    is_completed: bool,
}

fn strata_json(args: &[&str], storage: &std::path::Path) -> serde_json::Value {
    let output = Command::new(env!("CARGO_BIN_EXE_strata"))
        .args(args)
        .args(["--json", "--storage"])
        .arg(storage)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "strata {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[tokio::test]
async fn cli_reads_the_log_a_rust_runtime_store_wrote_across_freeze_and_resume() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.sqlite");

    let graph = StateGraph::<WireTransferState>::new()
        .add_node("PrepareWireTransfer", |s: WireTransferState| {
            WireTransferState {
                invoice_generated: true,
                ..s
            }
        })
        .add_interrupt_boundary("PrepareWireTransfer")
        .add_node("ExecuteWireTransfer", |s: WireTransferState| {
            WireTransferState {
                is_completed: true,
                ..s
            }
        })
        .add_edge("PrepareWireTransfer", "ExecuteWireTransfer")
        .compile()
        .unwrap();

    {
        let store = Arc::new(SqliteCheckpointStore::open(&path).unwrap());
        let initial = WireTransferState {
            invoice_generated: false,
            amount: 4200.5,
            approved_by: None,
            is_completed: false,
        };
        assert!(matches!(
            graph.run("wire-1", initial, store.clone()).await,
            GraphExecutionResult::Interrupted { .. }
        ));
        let signal = HumanSignal {
            is_approved: true,
            reviewer_token: "t".to_string(),
            field_overrides: HashMap::from([("approved_by".to_string(), "admin".to_string())]),
            ..Default::default()
        };
        assert!(matches!(
            graph.resume("wire-1", signal, store.clone()).await,
            GraphExecutionResult::Completed(_)
        ));
        // `store` is dropped here (the engine keeps no clone once a run returns),
        // so the CLI below reads only what is on disk.
    }

    // `thread list`: one thread, latest checkpoint is the resumed one.
    let threads = strata_json(&["thread", "list"], &path);
    let threads = threads.as_array().unwrap();
    assert_eq!(threads.len(), 1);
    assert_eq!(threads[0]["thread_id"], "wire-1");
    assert_eq!(threads[0]["sequence"], 1);
    // The resume stamp is the latest row: the thread is no longer waiting.
    assert_eq!(threads[0]["state"], "resumed");
    assert_eq!(threads[0]["status"], "interrupted");

    // `checkpoint log`: the freeze (#0) then the resume marker (#1) -- history
    // the old one-row-per-thread store would have overwritten.
    let log = strata_json(&["checkpoint", "log", "wire-1"], &path);
    let log = log.as_array().unwrap();
    assert_eq!(log.len(), 2);
    assert_eq!(log[0]["suspended_node"], "PrepareWireTransfer");
    assert_eq!(log[0]["next_node"], "ExecuteWireTransfer");
    assert!(log[0]["resumed_at"].is_null());
    assert!(log[1]["resumed_at"].is_string());

    // `checkpoint replay`: the frozen state the runtime serialized.
    let replay = strata_json(
        &["checkpoint", "replay", "wire-1", "--sequence", "0"],
        &path,
    );
    assert_eq!(replay["state"]["invoice_generated"], true);
    assert_eq!(replay["state"]["amount"], 4200.5);
    assert_eq!(replay["state"]["is_completed"], false);
}

#[tokio::test]
async fn cli_reads_a_thread_the_rust_runtime_ended() {
    // `design/28-Ending-A-Thread.md`: freeze, then end, through the runtime's
    // own SQLite store; the CLI lists the thread as ended.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.sqlite");
    let graph = StateGraph::<WireTransferState>::new()
        .add_node("PrepareWireTransfer", |s: WireTransferState| s)
        .add_interrupt_boundary("PrepareWireTransfer")
        .add_node("ExecuteWireTransfer", |s: WireTransferState| s)
        .add_edge("PrepareWireTransfer", "ExecuteWireTransfer")
        .compile()
        .unwrap();
    {
        let store = Arc::new(SqliteCheckpointStore::open(&path).unwrap());
        let initial = WireTransferState {
            invoice_generated: false,
            amount: 1.0,
            approved_by: None,
            is_completed: false,
        };
        graph.run("wire-2", initial, store.clone()).await;
        assert!(matches!(
            graph
                .end_with(
                    "wire-2",
                    datalium_strata::checkpoint::ThreadEnd::by("oncall"),
                    store.clone(),
                    Default::default(),
                )
                .await,
            GraphExecutionResult::Cancelled { .. }
        ));
    }

    let threads = strata_json(&["thread", "list", "--status", "ended"], &path);
    assert_eq!(threads[0]["thread_id"], "wire-2");
    assert_eq!(threads[0]["sequence"], 1);
    let inspect = strata_json(&["thread", "inspect", "wire-2"], &path);
    assert_eq!(inspect["status"], "ended");
    assert_eq!(inspect["metadata"]["strata.ended_by"], "oncall");
    assert!(inspect["next_node"].is_null());
}
