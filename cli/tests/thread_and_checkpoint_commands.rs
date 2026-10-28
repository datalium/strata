//! `strata thread list/inspect` and `strata checkpoint log/diff/replay`
//! end-to-end: seed a temp SQLite checkpoint log directly via `cli::store`
//! (the same `datalium-strata-memory` store the runtime writes with)
//! (there's no `strata checkpoint import` command — seeding is test setup,
//! not a CLI feature), then drive the actual compiled binary as a
//! subprocess against it.

use std::process::Command;

use datalium_strata::schema::{Checkpoint, CheckpointStatus};

fn strata() -> Command {
    Command::new(env!("CARGO_BIN_EXE_strata"))
}

fn temp_db_path() -> std::path::PathBuf {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.sqlite");
    // Leak the tempdir so it outlives the test instead of being deleted the
    // moment this function returns — fine for short-lived test processes.
    std::mem::forget(dir);
    path
}

fn checkpoint(
    thread_id: &str,
    suspended_node: &str,
    next_node: &str,
    state: serde_json::Value,
) -> Checkpoint {
    Checkpoint {
        schema_version: datalium_strata::schema::SCHEMA_VERSION.to_string(),
        thread_id: thread_id.to_string(),
        status: CheckpointStatus::Interrupted,
        suspended_node: Some(suspended_node.to_string()),
        next_node: Some(next_node.to_string()),
        frozen_state: state,
        metadata: None,
        created_at: "2026-09-16T00:00:00Z".to_string(),
        resumed_at: None,
        nested: None,
        decision: None,
        wait: None,
        graph: None,
    }
}

#[tokio::test]
async fn a_thread_frozen_inside_a_subgraph_shows_where_it_is_waiting() {
    let db_path = temp_db_path();
    let pool = cli::store::open(db_path.to_str().unwrap()).await.unwrap();
    // The shared fixture: suspended at the subgraph node `Work`, whose child is
    // waiting at `Approve`.
    let nested: Checkpoint = serde_json::from_str(
        &std::fs::read_to_string(format!(
            "{}/../schema/fixtures/checkpoint-lifecycle/nested.json",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap(),
    )
    .unwrap();
    pool.insert(&nested).await.unwrap();
    drop(pool);
    let run = |args: &[&str]| {
        let output = strata().args(args).arg("--storage").arg(&db_path).output();
        let output = output.unwrap();
        assert!(output.status.success(), "{args:?}");
        String::from_utf8_lossy(&output.stdout).into_owned()
    };

    // The wire field is untouched; the path is a rendering of it.
    let list: serde_json::Value =
        serde_json::from_str(&run(&["thread", "list", "--json"])).unwrap();
    assert_eq!(list[0]["suspended_node"], "Work");
    assert!(run(&["thread", "list"]).contains("Work/Approve"));

    let inspect: serde_json::Value =
        serde_json::from_str(&run(&["thread", "inspect", "thread_nested_demo", "--json"])).unwrap();
    assert_eq!(inspect["nested"]["suspended_node"], "Approve");
    assert_eq!(inspect["nested"]["frozen_state"]["n"], 10);
    assert!(
        run(&["thread", "inspect", "thread_nested_demo"]).contains("Waiting at:      Work/Approve")
    );

    assert!(run(&["checkpoint", "log", "thread_nested_demo"]).contains("node=Work/Approve"));

    let replay: serde_json::Value = serde_json::from_str(&run(&[
        "checkpoint",
        "replay",
        "thread_nested_demo",
        "--sequence",
        "0",
        "--json",
    ]))
    .unwrap();
    assert_eq!(replay["nested"]["next_node"], "Send");
    assert!(
        run(&[
            "checkpoint",
            "replay",
            "thread_nested_demo",
            "--sequence",
            "0"
        ])
        .contains("Waiting inside: Work/Approve")
    );
}

#[tokio::test]
async fn a_thread_with_no_nested_child_is_shown_exactly_as_before() {
    let db_path = temp_db_path();
    let pool = cli::store::open(db_path.to_str().unwrap()).await.unwrap();
    pool.insert(&checkpoint(
        "plain",
        "CallLLM",
        "Verify",
        serde_json::json!({"n": 1}),
    ))
    .await
    .unwrap();
    drop(pool);

    let inspect = strata()
        .args(["thread", "inspect", "plain", "--storage"])
        .arg(&db_path)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&inspect.stdout);

    assert!(inspect.status.success());
    assert!(!stdout.contains("Waiting at"));
    let json = strata()
        .args(["thread", "inspect", "plain", "--json", "--storage"])
        .arg(&db_path)
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&json.stdout).unwrap();
    assert!(json["nested"].is_null());
}

#[tokio::test]
async fn thread_list_shows_the_latest_checkpoint_per_thread() {
    let db_path = temp_db_path();
    let pool = cli::store::open(db_path.to_str().unwrap()).await.unwrap();

    pool.insert(&checkpoint(
        "thread_a",
        "CallLLM",
        "VerifyResult",
        serde_json::json!({"n": 1}),
    ))
    .await
    .unwrap();
    pool.insert(&checkpoint(
        "thread_a",
        "VerifyResult",
        "End",
        serde_json::json!({"n": 2}),
    ))
    .await
    .unwrap();
    pool.insert(&checkpoint(
        "thread_b",
        "Start",
        "CallLLM",
        serde_json::json!({"n": 1}),
    ))
    .await
    .unwrap();
    drop(pool);

    let output = strata()
        .args(["thread", "list", "--json", "--storage"])
        .arg(&db_path)
        .output()
        .unwrap();

    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let rows = json.as_array().unwrap();
    assert_eq!(rows.len(), 2);

    let thread_a = rows.iter().find(|r| r["thread_id"] == "thread_a").unwrap();
    // thread_a's LATEST checkpoint is sequence 1 (VerifyResult), not sequence 0.
    assert_eq!(thread_a["sequence"], 1);
    assert_eq!(thread_a["suspended_node"], "VerifyResult");

    // The human-readable (`tabled`) path renders without crashing and
    // contains the same data, not just the `--json` path.
    let human_output = strata()
        .args(["thread", "list", "--storage"])
        .arg(&db_path)
        .output()
        .unwrap();
    assert!(human_output.status.success());
    let human_stdout = String::from_utf8_lossy(&human_output.stdout);
    assert!(human_stdout.contains("Thread ID"));
    assert!(human_stdout.contains("thread_a"));
    assert!(human_stdout.contains("thread_b"));
    assert!(human_stdout.contains("VerifyResult"));
}

#[tokio::test]
async fn thread_list_filters_by_status() {
    let db_path = temp_db_path();
    let pool = cli::store::open(db_path.to_str().unwrap()).await.unwrap();

    let mut completed = checkpoint("thread_done", "End", "End", serde_json::json!({}));
    completed.status = CheckpointStatus::Completed;
    pool.insert(&completed).await.unwrap();
    pool.insert(&checkpoint(
        "thread_stuck",
        "CallLLM",
        "VerifyResult",
        serde_json::json!({}),
    ))
    .await
    .unwrap();
    drop(pool);

    let output = strata()
        .args([
            "thread",
            "list",
            "--json",
            "--status",
            "interrupted",
            "--storage",
        ])
        .arg(&db_path)
        .output()
        .unwrap();

    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let rows = json.as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["thread_id"], "thread_stuck");
}

#[tokio::test]
async fn thread_inspect_reports_the_checkpoint_count_and_latest_state() {
    let db_path = temp_db_path();
    let pool = cli::store::open(db_path.to_str().unwrap()).await.unwrap();
    pool.insert(&checkpoint(
        "thread_a",
        "CallLLM",
        "VerifyResult",
        serde_json::json!({"n": 1}),
    ))
    .await
    .unwrap();
    pool.insert(&checkpoint(
        "thread_a",
        "VerifyResult",
        "End",
        serde_json::json!({"n": 2}),
    ))
    .await
    .unwrap();
    drop(pool);

    let output = strata()
        .args(["thread", "inspect", "thread_a", "--json", "--storage"])
        .arg(&db_path)
        .output()
        .unwrap();

    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["checkpoint_count"], 2);
    assert_eq!(json["frozen_state"]["n"], 2);

    let human_output = strata()
        .args(["thread", "inspect", "thread_a", "--storage"])
        .arg(&db_path)
        .output()
        .unwrap();
    assert!(human_output.status.success());
    let human_stdout = String::from_utf8_lossy(&human_output.stdout);
    assert!(human_stdout.contains("Checkpoint count: 2"));
    assert!(human_stdout.contains("\"n\": 2"));
}

#[tokio::test]
async fn checkpoint_log_lists_every_checkpoint_in_order() {
    let db_path = temp_db_path();
    let pool = cli::store::open(db_path.to_str().unwrap()).await.unwrap();
    pool.insert(&checkpoint(
        "thread_a",
        "EntryPoint",
        "CallLLM",
        serde_json::json!({}),
    ))
    .await
    .unwrap();
    pool.insert(&checkpoint(
        "thread_a",
        "CallLLM",
        "VerifyResult",
        serde_json::json!({}),
    ))
    .await
    .unwrap();
    drop(pool);

    let output = strata()
        .args(["checkpoint", "log", "thread_a", "--json", "--storage"])
        .arg(&db_path)
        .output()
        .unwrap();

    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let rows = json.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["sequence"], 0);
    assert_eq!(rows[0]["suspended_node"], "EntryPoint");
    assert_eq!(rows[1]["sequence"], 1);
    assert_eq!(rows[1]["suspended_node"], "CallLLM");

    let human_output = strata()
        .args(["checkpoint", "log", "thread_a", "--storage"])
        .arg(&db_path)
        .output()
        .unwrap();
    assert!(human_output.status.success());
    let human_stdout = String::from_utf8_lossy(&human_output.stdout);
    assert!(human_stdout.contains("#0"));
    assert!(human_stdout.contains("#1"));
    assert!(human_stdout.contains("EntryPoint"));
    assert!(human_stdout.contains("CallLLM"));
}

#[tokio::test]
async fn checkpoint_diff_reports_only_changed_top_level_fields() {
    let db_path = temp_db_path();
    let pool = cli::store::open(db_path.to_str().unwrap()).await.unwrap();
    pool.insert(&checkpoint(
        "thread_a",
        "EntryPoint",
        "CallLLM",
        serde_json::json!({"query": "Optimize indexes...", "response": "", "iteration_count": 0}),
    ))
    .await
    .unwrap();
    pool.insert(&checkpoint(
            "thread_a",
            "CallLLM",
            "VerifyResult",
            serde_json::json!({"query": "Optimize indexes...", "response": "done", "iteration_count": 1}),
        ),
    )
    .await
    .unwrap();
    drop(pool);

    let output = strata()
        .args([
            "checkpoint",
            "diff",
            "thread_a",
            "--seq-a",
            "0",
            "--seq-b",
            "1",
            "--json",
            "--storage",
        ])
        .arg(&db_path)
        .output()
        .unwrap();

    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let changes = json["changes"].as_array().unwrap();
    let changed_keys: Vec<&str> = changes.iter().map(|c| c["key"].as_str().unwrap()).collect();
    assert!(changed_keys.contains(&"response"));
    assert!(changed_keys.contains(&"iteration_count"));
    assert!(!changed_keys.contains(&"query")); // unchanged, must not appear

    let human_output = strata()
        .args([
            "checkpoint",
            "diff",
            "thread_a",
            "--seq-a",
            "0",
            "--seq-b",
            "1",
            "--storage",
        ])
        .arg(&db_path)
        .output()
        .unwrap();
    assert!(human_output.status.success());
    let human_stdout = String::from_utf8_lossy(&human_output.stdout);
    assert!(human_stdout.contains("--- Checkpoint #0"));
    assert!(human_stdout.contains("+++ Checkpoint #1"));
    assert!(human_stdout.contains("\"response\""));
}

#[tokio::test]
async fn checkpoint_replay_loads_the_requested_historical_state() {
    let db_path = temp_db_path();
    let pool = cli::store::open(db_path.to_str().unwrap()).await.unwrap();
    pool.insert(&checkpoint(
        "thread_a",
        "CallLLM",
        "VerifyResult",
        serde_json::json!({"iteration_count": 1}),
    ))
    .await
    .unwrap();
    drop(pool);

    let output = strata()
        .args([
            "checkpoint",
            "replay",
            "thread_a",
            "--sequence",
            "0",
            "--json",
            "--storage",
        ])
        .arg(&db_path)
        .output()
        .unwrap();

    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["next_node"], "VerifyResult");
    assert_eq!(json["state"]["iteration_count"], 1);

    let human_output = strata()
        .args([
            "checkpoint",
            "replay",
            "thread_a",
            "--sequence",
            "0",
            "--storage",
        ])
        .arg(&db_path)
        .output()
        .unwrap();
    assert!(human_output.status.success());
    let human_stdout = String::from_utf8_lossy(&human_output.stdout);
    assert!(human_stdout.contains("Loading historical state from Checkpoint #0"));
    assert!(human_stdout.contains("VerifyResult"));
}

#[tokio::test]
async fn checkpoint_replay_of_an_unknown_sequence_fails_cleanly() {
    let db_path = temp_db_path();
    cli::store::open(db_path.to_str().unwrap()).await.unwrap();

    let output = strata()
        .args([
            "checkpoint",
            "replay",
            "does-not-exist",
            "--sequence",
            "0",
            "--storage",
        ])
        .arg(&db_path)
        .output()
        .unwrap();

    assert!(!output.status.success());
}

#[tokio::test]
async fn a_thread_that_failed_inside_a_subgraph_says_where_it_failed_not_where_it_waits() {
    let db_path = temp_db_path();
    let pool = cli::store::open(db_path.to_str().unwrap()).await.unwrap();
    // The shared fixture: failed at the subgraph node `Work`, whose child
    // failed at `Send`.
    let failed: Checkpoint = serde_json::from_str(
        &std::fs::read_to_string(format!(
            "{}/../schema/fixtures/checkpoint-lifecycle/nested-failed.json",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap(),
    )
    .unwrap();
    pool.insert(&failed).await.unwrap();
    drop(pool);
    let run = |args: &[&str]| {
        let output = strata()
            .args(args)
            .arg("--storage")
            .arg(&db_path)
            .output()
            .unwrap();
        assert!(output.status.success(), "{args:?}");
        String::from_utf8_lossy(&output.stdout).into_owned()
    };

    assert!(run(&["thread", "list"]).contains("Work/Send"));
    let inspect = run(&["thread", "inspect", "thread_nested_failed_demo"]);
    assert!(inspect.contains("Failed at:"), "{inspect}");
    assert!(inspect.contains("Work/Send"), "{inspect}");
    assert!(!inspect.contains("Waiting at"), "{inspect}");
    assert!(run(&["checkpoint", "log", "thread_nested_failed_demo"]).contains("node=Work/Send"));
    let replay = run(&[
        "checkpoint",
        "replay",
        "thread_nested_failed_demo",
        "--sequence",
        "0",
    ]);
    assert!(replay.contains("Failed inside: Work/Send"), "{replay}");
}

#[tokio::test]
async fn a_thread_with_decisions_shows_who_decided_at_which_level() {
    let db_path = temp_db_path();
    let pool = cli::store::open(db_path.to_str().unwrap()).await.unwrap();
    // The shared fixtures (design/21-Typed-Human-Signals.md): a flat failure
    // after Alice approved, and a failure inside `Work` after Bob rejected.
    for name in ["decision-failed.json", "nested-decision.json"] {
        let checkpoint: Checkpoint = serde_json::from_str(
            &std::fs::read_to_string(format!(
                "{}/../schema/fixtures/checkpoint-lifecycle/{name}",
                env!("CARGO_MANIFEST_DIR")
            ))
            .unwrap(),
        )
        .unwrap();
        pool.insert(&checkpoint).await.unwrap();
    }
    drop(pool);
    let run = |args: &[&str]| {
        let output = strata()
            .args(args)
            .arg("--storage")
            .arg(&db_path)
            .output()
            .unwrap();
        assert!(output.status.success(), "{args:?}");
        String::from_utf8_lossy(&output.stdout).into_owned()
    };

    let flat = run(&["thread", "inspect", "thread_decision_demo"]);
    assert!(
        flat.contains(
            "Decision:        approved by alice@acme.io at Approve, 2026-10-04T00:00:00Z"
        ),
        "{flat}"
    );
    let nested = run(&["thread", "inspect", "thread_nested_decision_demo"]);
    assert!(
        nested.contains(
            "Decision:        rejected by bob@acme.io at Work/Approve, 2026-10-04T00:00:00Z"
        ),
        "{nested}"
    );
    let replay = run(&[
        "checkpoint",
        "replay",
        "thread_nested_decision_demo",
        "--sequence",
        "0",
    ]);
    assert!(
        replay.contains("Decision in effect: rejected by bob@acme.io at Work/Approve"),
        "{replay}"
    );

    let json: serde_json::Value = serde_json::from_str(&run(&[
        "thread",
        "inspect",
        "thread_decision_demo",
        "--json",
    ]))
    .unwrap();
    assert_eq!(json["decision"]["reviewer"], "alice@acme.io");
    assert_eq!(json["decision"]["overrides"]["replicas"], 3);
    let json: serde_json::Value = serde_json::from_str(&run(&[
        "checkpoint",
        "replay",
        "thread_nested_decision_demo",
        "--sequence",
        "0",
        "--json",
    ]))
    .unwrap();
    assert!(json["decision"].is_null());
    assert_eq!(json["nested"]["decision"]["is_approved"], false);
}

#[tokio::test]
async fn a_cancelled_thread_says_where_and_why_it_was_cancelled() {
    let db_path = temp_db_path();
    let pool = cli::store::open(db_path.to_str().unwrap()).await.unwrap();
    // The shared fixtures (`design/17-Async-Execution.md`): one cancelled at
    // `Charge`, one at the subgraph node `Work` whose child was at `Send`.
    for name in ["cancelled.json", "nested-cancelled.json"] {
        let checkpoint: Checkpoint = serde_json::from_str(
            &std::fs::read_to_string(format!(
                "{}/../schema/fixtures/checkpoint-lifecycle/{name}",
                env!("CARGO_MANIFEST_DIR")
            ))
            .unwrap(),
        )
        .unwrap();
        pool.insert(&checkpoint).await.unwrap();
    }
    drop(pool);
    let run = |args: &[&str]| {
        let output = strata()
            .args(args)
            .arg("--storage")
            .arg(&db_path)
            .output()
            .unwrap();
        assert!(output.status.success(), "{args:?}");
        String::from_utf8_lossy(&output.stdout).into_owned()
    };

    let listed: serde_json::Value =
        serde_json::from_str(&run(&["thread", "list", "--status", "cancelled", "--json"])).unwrap();
    let mut ids: Vec<&str> = listed
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["thread_id"].as_str().unwrap())
        .collect();
    ids.sort();
    assert_eq!(
        ids,
        ["thread_cancelled_demo", "thread_nested_cancelled_demo"]
    );
    assert!(run(&["thread", "list"]).contains("Work/Send"));

    let inspect = run(&["thread", "inspect", "thread_nested_cancelled_demo"]);
    assert!(inspect.contains("Status:          cancelled"), "{inspect}");
    assert!(inspect.contains("Cancelled at:"), "{inspect}");
    assert!(inspect.contains("Work/Send"), "{inspect}");
    assert!(inspect.contains("Reason:          requested"), "{inspect}");
    assert!(!inspect.contains("Waiting at"), "{inspect}");

    let flat = run(&["thread", "inspect", "thread_cancelled_demo"]);
    assert!(flat.contains("Current node:    Charge"), "{flat}");
    assert!(flat.contains("Reason:          requested"), "{flat}");
    let flat_json: serde_json::Value = serde_json::from_str(&run(&[
        "thread",
        "inspect",
        "thread_cancelled_demo",
        "--json",
    ]))
    .unwrap();
    assert_eq!(flat_json["status"], "cancelled");
    assert_eq!(flat_json["metadata"]["cancel_reason"], "requested");

    assert!(
        run(&["checkpoint", "log", "thread_nested_cancelled_demo"])
            .contains("[cancelled] node=Work/Send")
    );
    let replay = run(&[
        "checkpoint",
        "replay",
        "thread_nested_cancelled_demo",
        "--sequence",
        "0",
    ]);
    assert!(replay.contains("Cancelled inside: Work/Send"), "{replay}");
}

#[tokio::test]
async fn an_ended_thread_says_where_it_stood_why_and_by_whom() {
    let db_path = temp_db_path();
    let pool = cli::store::open(db_path.to_str().unwrap()).await.unwrap();
    // The shared fixtures (`design/28-Ending-A-Thread.md`): one ended at
    // `PrepareWireTransfer` by `oncall`, one ended while its child waited at
    // `Approve` inside the subgraph node `Work`.
    for name in ["ended.json", "nested-ended.json"] {
        let checkpoint: Checkpoint = serde_json::from_str(
            &std::fs::read_to_string(format!(
                "{}/../schema/fixtures/checkpoint-lifecycle/{name}",
                env!("CARGO_MANIFEST_DIR")
            ))
            .unwrap(),
        )
        .unwrap();
        pool.insert(&checkpoint).await.unwrap();
    }
    drop(pool);
    let run = |args: &[&str]| {
        let output = strata()
            .args(args)
            .arg("--storage")
            .arg(&db_path)
            .output()
            .unwrap();
        assert!(output.status.success(), "{args:?}");
        String::from_utf8_lossy(&output.stdout).into_owned()
    };

    let listed: serde_json::Value =
        serde_json::from_str(&run(&["thread", "list", "--status", "ended", "--json"])).unwrap();
    let mut ids: Vec<&str> = listed
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["thread_id"].as_str().unwrap())
        .collect();
    ids.sort();
    assert_eq!(ids, ["thread_ended_demo", "thread_nested_ended_demo"]);
    assert_eq!(listed[0]["state"], "ended");
    let waiting = run(&["thread", "list", "--status", "interrupted"]);
    assert!(waiting.contains("No threads found."), "{waiting}");

    let flat = run(&["thread", "inspect", "thread_ended_demo"]);
    assert!(flat.contains("Status:          ended"), "{flat}");
    assert!(flat.contains("Thread state:    ended"), "{flat}");
    assert!(
        flat.contains("Current node:    PrepareWireTransfer"),
        "{flat}"
    );
    assert!(flat.contains("Reason:          requested"), "{flat}");
    assert!(flat.contains("Ended by:        oncall"), "{flat}");
    assert!(flat.contains("Next node:       -"), "{flat}");
    assert!(!flat.contains("Metadata:"), "{flat}");

    let nested = run(&["thread", "inspect", "thread_nested_ended_demo"]);
    assert!(nested.contains("Ended at:"), "{nested}");
    assert!(nested.contains("Work/Approve"), "{nested}");
    assert!(nested.contains("Metadata:        tenant=acme"), "{nested}");
    assert!(!nested.contains("Ended by:"), "{nested}");

    let replay = run(&[
        "checkpoint",
        "replay",
        "thread_nested_ended_demo",
        "--sequence",
        "0",
    ]);
    assert!(replay.contains("Ended inside: Work/Approve"), "{replay}");
    assert!(!replay.contains("Would resume at"), "{replay}");
}

#[tokio::test]
async fn a_running_thread_says_where_its_run_is() {
    let db_path = temp_db_path();
    let pool = cli::store::open(db_path.to_str().unwrap()).await.unwrap();
    // The shared fixtures (`design/33-Durable-Execution.md`): a run past `A`,
    // one inside the subgraph node `Work`, a resume's claim with its
    // decision, and a run that finished.
    for name in [
        "running.json",
        "nested-running.json",
        "claimed.json",
        "completed.json",
    ] {
        let checkpoint: Checkpoint = serde_json::from_str(
            &std::fs::read_to_string(format!(
                "{}/../schema/fixtures/checkpoint-lifecycle/{name}",
                env!("CARGO_MANIFEST_DIR")
            ))
            .unwrap(),
        )
        .unwrap();
        pool.insert(&checkpoint).await.unwrap();
    }
    drop(pool);
    let run = |args: &[&str]| {
        let output = strata()
            .args(args)
            .arg("--storage")
            .arg(&db_path)
            .output()
            .unwrap();
        assert!(output.status.success(), "{args:?}");
        String::from_utf8_lossy(&output.stdout).into_owned()
    };
    let listed = |status: &str| -> Vec<String> {
        let listed: serde_json::Value =
            serde_json::from_str(&run(&["thread", "list", "--status", status, "--json"])).unwrap();
        let mut ids: Vec<String> = listed
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["thread_id"].as_str().unwrap().to_string())
            .collect();
        ids.sort();
        ids
    };

    assert_eq!(
        listed("running"),
        [
            "thread_claimed_demo",
            "thread_nested_running_demo",
            "thread_running_demo"
        ]
    );
    assert_eq!(listed("completed"), ["thread_completed_demo"]);

    let flat = run(&["thread", "inspect", "thread_running_demo"]);
    assert!(flat.contains("Status:          running"), "{flat}");
    assert!(flat.contains("Thread state:    running"), "{flat}");
    assert!(
        flat.contains("Running:         after 'A', next 'B'"),
        "{flat}"
    );
    assert!(flat.contains("Hops:            1"), "{flat}");
    assert!(!flat.contains("Recoveries:"), "{flat}");

    let nested = run(&["thread", "inspect", "thread_nested_running_demo"]);
    assert!(
        nested.contains("Running:         in 'Work', after 'X', next 'Y'"),
        "{nested}"
    );
    assert!(!nested.contains("Waiting at:"), "{nested}");

    let claimed = run(&["thread", "inspect", "thread_claimed_demo"]);
    assert!(claimed.contains("Decision:"), "{claimed}");
    assert!(claimed.contains("alice"), "{claimed}");

    let done = run(&["thread", "inspect", "thread_completed_demo"]);
    assert!(done.contains("Thread state:    completed"), "{done}");
    assert!(done.contains("Next node:       -"), "{done}");

    let json: serde_json::Value = serde_json::from_str(&run(&[
        "thread",
        "inspect",
        "thread_nested_running_demo",
        "--json",
    ]))
    .unwrap();
    assert_eq!(json["state"], "running");
    assert_eq!(json["nested"]["next_node"], "Y");
}

#[tokio::test]
async fn a_thread_shows_its_caller_metadata_and_can_be_listed_by_it() {
    let db_path = temp_db_path();
    let pool = cli::store::open(db_path.to_str().unwrap()).await.unwrap();
    // The shared fixtures (`design/18-Run-Observability.md`): tenant `acme` on
    // both, incident INC-284891 on one and INC-284892 on the other, the
    // second beside the engine's own `error` keys.
    for name in [
        "metadata-nested.json",
        "metadata-failed.json",
        "nested.json",
    ] {
        let checkpoint: Checkpoint = serde_json::from_str(
            &std::fs::read_to_string(format!(
                "{}/../schema/fixtures/checkpoint-lifecycle/{name}",
                env!("CARGO_MANIFEST_DIR")
            ))
            .unwrap(),
        )
        .unwrap();
        pool.insert(&checkpoint).await.unwrap();
    }
    drop(pool);
    let run = |args: &[&str]| {
        let output = strata()
            .args(args)
            .arg("--storage")
            .arg(&db_path)
            .output()
            .unwrap();
        assert!(output.status.success(), "{args:?}");
        String::from_utf8_lossy(&output.stdout).into_owned()
    };
    let listed = |args: &[&str]| {
        let mut all = vec!["thread", "list", "--json"];
        all.extend_from_slice(args);
        let listed: serde_json::Value = serde_json::from_str(&run(&all)).unwrap();
        let mut ids: Vec<String> = listed
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["thread_id"].as_str().unwrap().to_string())
            .collect();
        ids.sort();
        ids
    };

    // Caller keys only, sorted; the engine's keys keep their own lines.
    let failed = run(&["thread", "inspect", "thread_metadata_failed_demo"]);
    assert!(
        failed.contains("Metadata:        incident=INC-284892, tenant=acme"),
        "{failed}"
    );
    assert!(!failed.contains("error_kind="), "{failed}");
    assert!(!run(&["thread", "inspect", "thread_nested_demo"]).contains("Metadata:"));

    assert_eq!(
        listed(&["--meta", "tenant=acme"]),
        ["thread_metadata_demo", "thread_metadata_failed_demo"]
    );
    assert_eq!(
        listed(&["--meta", "tenant=acme", "--meta", "incident=INC-284891"]),
        ["thread_metadata_demo"]
    );
    // The query port's filter: the engine's keys match too
    // (`design/19-Async-Queryable-Store.md`).
    assert_eq!(
        listed(&["--meta", "error_kind=fatal"]),
        ["thread_metadata_failed_demo"]
    );

    let bad = strata()
        .args(["thread", "list", "--meta", "no-equals-sign", "--storage"])
        .arg(&db_path)
        .output()
        .unwrap();
    assert!(!bad.status.success());
}

#[tokio::test]
async fn a_log_from_before_nested_checkpoints_is_upgraded_when_opened() {
    let db_path = temp_db_path();
    // The `checkpoints` table as it was before nested checkpoints: no
    // `nested` column. Seeded directly, as a file an older runtime left behind.
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE checkpoints (
                thread_id TEXT NOT NULL, sequence INTEGER NOT NULL,
                schema_version TEXT NOT NULL, status TEXT NOT NULL,
                suspended_node TEXT, next_node TEXT, frozen_state TEXT NOT NULL,
                metadata TEXT, created_at TEXT NOT NULL, resumed_at TEXT,
                PRIMARY KEY (thread_id, sequence)
            );
            INSERT INTO checkpoints VALUES
                ('old_thread', 0, '1.0.0', 'interrupted', 'CallLLM', 'Verify', '{\"n\":1}', NULL,
                 '2026-09-16T00:00:00Z', NULL);",
        )
        .unwrap();
    }

    // Every command reads `nested`, so this only works if opening upgraded the file.
    let list = strata()
        .args(["thread", "list", "--json", "--storage"])
        .arg(&db_path)
        .output()
        .unwrap();
    assert!(
        list.status.success(),
        "{}",
        String::from_utf8_lossy(&list.stderr)
    );
    let list: serde_json::Value = serde_json::from_slice(&list.stdout).unwrap();
    assert_eq!(list[0]["thread_id"], "old_thread");
    assert_eq!(list[0]["suspended_node"], "CallLLM");

    // The upgraded file takes a checkpoint with a child, and opening it again is a no-op.
    let pool = cli::store::open(db_path.to_str().unwrap()).await.unwrap();
    let nested: Checkpoint = serde_json::from_str(
        &std::fs::read_to_string(format!(
            "{}/../schema/fixtures/checkpoint-lifecycle/nested.json",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap(),
    )
    .unwrap();
    pool.insert(&nested).await.unwrap();
    let history = pool.log("thread_nested_demo").await.unwrap();
    assert_eq!(
        cli::store::waiting_path(&history[0].checkpoint).as_deref(),
        Some("Work/Approve")
    );
    let old = pool.log("old_thread").await.unwrap();
    assert_eq!(old[0].checkpoint.nested, None);
}

#[tokio::test]
async fn inspect_shows_the_hop_count_of_a_graph_with_a_hop_limit() {
    let db_path = temp_db_path();
    let pool = cli::store::open(db_path.to_str().unwrap()).await.unwrap();
    // `design/24-Loop-Circuit-Breaker.md`: a thread the breaker stopped before
    // `B`, and one from a graph without a limit.
    let mut tripped = checkpoint("thread_hop_limit", "B", "B", serde_json::json!({ "n": 3 }));
    tripped.status = CheckpointStatus::Failed;
    tripped.metadata = Some(std::collections::HashMap::from([
        (
            "error".to_string(),
            "hop limit of 5 reached: node 'B' would be hop 6".to_string(),
        ),
        ("error_kind".to_string(), "hop_limit".to_string()),
        ("strata.hops".to_string(), "5".to_string()),
    ]));
    pool.insert(&tripped).await.unwrap();
    pool.insert(&checkpoint(
        "thread_unlimited",
        "B",
        "A",
        serde_json::json!({ "n": 1 }),
    ))
    .await
    .unwrap();
    drop(pool);
    let run = |args: &[&str]| {
        let output = strata()
            .args(args)
            .arg("--storage")
            .arg(&db_path)
            .output()
            .unwrap();
        assert!(output.status.success(), "{args:?}");
        String::from_utf8_lossy(&output.stdout).into_owned()
    };

    let inspect = run(&["thread", "inspect", "thread_hop_limit"]);
    assert!(inspect.contains("Status:          failed"), "{inspect}");
    assert!(inspect.contains("Hops:            5"), "{inspect}");
    // An engine key, not the caller's metadata.
    assert!(!inspect.contains("Metadata:"), "{inspect}");

    let unlimited = run(&["thread", "inspect", "thread_unlimited"]);
    assert!(!unlimited.contains("Hops:"), "{unlimited}");
}
