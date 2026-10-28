//! Phase 7 exit criterion: "diff/replay a checkpoint produced by either
//! language's runtime." `schema/fixtures/checkpoint-lifecycle/frozen.json`
//! is the Phase 4 interop artifact — `strata-memory`'s and
//! `Datalium.Strata.Tests`' own test suites already independently prove
//! that freezing the STRATA-SPEC-002 wire-transfer graph in *either*
//! runtime produces a checkpoint matching this exact file. This test seeds
//! the CLI's checkpoint log from that same file and proves the CLI can read
//! it back correctly via `checkpoint replay` — the same "shared fixture,
//! not a live cross-process pipe" pattern every interop proof since Phase 1
//! has used, applied here to the CLI specifically.

use std::process::Command;

use datalium_strata::schema::Checkpoint;

fn strata() -> Command {
    Command::new(env!("CARGO_BIN_EXE_strata"))
}

fn shared_fixture() -> Checkpoint {
    let path = format!(
        "{}/../schema/fixtures/checkpoint-lifecycle/frozen.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {path}: {e}"));
    serde_json::from_str(&raw).unwrap()
}

#[tokio::test]
async fn cli_replays_the_checkpoint_shape_both_runtimes_independently_produce() {
    let fixture = shared_fixture();

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("checkpoints.sqlite");
    let pool = cli::store::open(db_path.to_str().unwrap()).await.unwrap();
    let sequence = pool.insert(&fixture).await.unwrap();
    drop(pool);

    let output = strata()
        .args([
            "checkpoint",
            "replay",
            &fixture.thread_id,
            "--sequence",
            &sequence.to_string(),
            "--json",
            "--storage",
        ])
        .arg(&db_path)
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(
        json["suspended_node"].as_str(),
        fixture.suspended_node.as_deref()
    );
    assert_eq!(json["next_node"].as_str(), fixture.next_node.as_deref());
    // The replayed state must be exactly the frozen_state the fixture carries
    // — the same shape both the Rust `checkpoint::tests::freezing_the_wire_transfer_graph_...`
    // and the C# `CheckpointFixtureParityTests.Freezing_...` tests assert their
    // own runtime produces.
    assert_eq!(json["state"], fixture.frozen_state);
}

#[tokio::test]
async fn cli_diffs_the_shared_fixture_against_a_second_checkpoint() {
    let fixture = shared_fixture();

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("checkpoints.sqlite");
    let pool = cli::store::open(db_path.to_str().unwrap()).await.unwrap();
    let seq_a = pool.insert(&fixture).await.unwrap();

    let mut resumed = fixture.clone();
    resumed.frozen_state["is_completed"] = serde_json::json!(true);
    resumed.frozen_state["approved_by"] = serde_json::json!("admin_01");
    let seq_b = pool.insert(&resumed).await.unwrap();
    drop(pool);

    let output = strata()
        .args([
            "checkpoint",
            "diff",
            &fixture.thread_id,
            "--seq-a",
            &seq_a.to_string(),
            "--seq-b",
            &seq_b.to_string(),
            "--json",
            "--storage",
        ])
        .arg(&db_path)
        .output()
        .unwrap();

    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let changed_keys: Vec<&str> = json["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["key"].as_str().unwrap())
        .collect();
    assert!(changed_keys.contains(&"is_completed"));
    assert!(changed_keys.contains(&"approved_by"));
    assert!(!changed_keys.contains(&"currency")); // unchanged between the two
}
