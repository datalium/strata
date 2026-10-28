//! `strata checkpoint log` / `diff` / `replay` (design/05.Strata-CLI.md),
//! read through the query port (`design/19-Async-Queryable-Store.md`).

use std::process::ExitCode;

use crate::store::{self, Storage};

async fn open(storage: &str, blobs: Option<&str>) -> Result<Storage, ExitCode> {
    store::open_with(storage, blobs).await.map_err(|err| {
        eprintln!("strata: failed to open storage '{storage}': {err}");
        ExitCode::from(2)
    })
}

pub async fn log(thread_id: &str, storage: &str, blobs: Option<&str>, json: bool) -> ExitCode {
    let storage = match open(storage, blobs).await {
        Ok(storage) => storage,
        Err(code) => return code,
    };

    let rows = match storage.log(thread_id).await {
        Ok(rows) => rows,
        Err(err) => {
            eprintln!("strata: query failed: {err}");
            return ExitCode::from(2);
        }
    };

    if rows.is_empty() {
        eprintln!("strata: no checkpoints found for thread '{thread_id}'");
        return ExitCode::from(1);
    }

    if json {
        let out: Vec<_> = rows
            .iter()
            .map(|row| {
                let checkpoint = &row.checkpoint;
                serde_json::json!({
                    "sequence": row.sequence,
                    "status": store::status_name(checkpoint.status),
                    "suspended_node": checkpoint.suspended_node,
                    "next_node": checkpoint.next_node,
                    "created_at": checkpoint.created_at,
                    "resumed_at": checkpoint.resumed_at,
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&out).unwrap());
    } else {
        for row in &rows {
            let checkpoint = &row.checkpoint;
            println!(
                "#{:<4} [{}] node={} -> {} at={}",
                row.sequence,
                store::status_name(checkpoint.status),
                store::waiting_path(checkpoint)
                    .as_deref()
                    .or(checkpoint.suspended_node.as_deref())
                    .unwrap_or("-"),
                checkpoint.next_node.as_deref().unwrap_or("-"),
                checkpoint.created_at,
            );
        }
    }

    ExitCode::SUCCESS
}

struct FieldDiff {
    key: String,
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
}

/// A shallow (top-level keys only) diff between two JSON objects — matches
/// the granularity of `design/05.Strata-CLI.md`'s own `checkpoint diff`
/// example, which only ever shows top-level state-field changes.
fn diff_json_objects(a: &serde_json::Value, b: &serde_json::Value) -> Vec<FieldDiff> {
    let empty = serde_json::Map::new();
    let map_a = a.as_object().unwrap_or(&empty);
    let map_b = b.as_object().unwrap_or(&empty);

    let mut keys: Vec<&String> = map_a.keys().chain(map_b.keys()).collect();
    keys.sort();
    keys.dedup();

    keys.into_iter()
        .filter_map(|key| {
            let before = map_a.get(key);
            let after = map_b.get(key);
            if before == after {
                return None;
            }
            Some(FieldDiff {
                key: key.clone(),
                before: before.cloned(),
                after: after.cloned(),
            })
        })
        .collect()
}

pub async fn diff(
    thread_id: &str,
    seq_a: i64,
    seq_b: i64,
    storage: &str,
    blobs: Option<&str>,
    json: bool,
) -> ExitCode {
    let storage = match open(storage, blobs).await {
        Ok(storage) => storage,
        Err(code) => return code,
    };

    let result_a = storage.checkpoint(thread_id, seq_a).await;
    let result_b = storage.checkpoint(thread_id, seq_b).await;

    let (a, b) = match (result_a, result_b) {
        (Err(err), _) | (_, Err(err)) => {
            eprintln!("strata: query failed: {err}");
            return ExitCode::from(2);
        }
        (Ok(None), _) => {
            eprintln!("strata: no checkpoint #{seq_a} found for thread '{thread_id}'");
            return ExitCode::from(1);
        }
        (_, Ok(None)) => {
            eprintln!("strata: no checkpoint #{seq_b} found for thread '{thread_id}'");
            return ExitCode::from(1);
        }
        (Ok(Some(a)), Ok(Some(b))) => (a, b),
    };

    let changes = diff_json_objects(&a.frozen_state, &b.frozen_state);

    if json {
        let out = serde_json::json!({
            "thread_id": thread_id,
            "seq_a": seq_a,
            "seq_b": seq_b,
            "node_a": a.suspended_node,
            "node_b": b.suspended_node,
            "changes": changes.iter().map(|c| serde_json::json!({
                "key": c.key,
                "before": c.before,
                "after": c.after,
            })).collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string_pretty(&out).unwrap());
    } else {
        println!(
            "--- Checkpoint #{seq_a} (Node: {})",
            a.suspended_node.as_deref().unwrap_or("-")
        );
        println!(
            "+++ Checkpoint #{seq_b} (Node: {})",
            b.suspended_node.as_deref().unwrap_or("-")
        );
        println!("{{");
        if changes.is_empty() {
            println!("  (no state field changes)");
        }
        for change in &changes {
            if let Some(before) = &change.before {
                println!("-  \"{}\": {},", change.key, before);
            }
            if let Some(after) = &change.after {
                println!("+  \"{}\": {},", change.key, after);
            }
        }
        println!("}}");
    }

    ExitCode::SUCCESS
}

/// Loads and displays the historical state a node would see if replayed
/// from this checkpoint. This deliberately does *not* re-invoke arbitrary
/// node business logic (the `--local-override-model` re-execution
/// `design/05.Strata-CLI.md`'s example shows): a generic `strata` binary
/// has no way to dynamically load a specific Rust program's `#[strata_tool]`/
/// node closures — those are compiled directly into whatever binary defined
/// the graph, not into this CLI. What's shown here — the exact frozen state
/// and next node a real replay would start from — is the genuinely
/// CLI-scoped half of that feature; wiring it to an actual re-execution
/// engine would require a plugin/scripting story that's out of scope for
/// this phase's checklist.
pub async fn replay(
    thread_id: &str,
    sequence: i64,
    storage: &str,
    blobs: Option<&str>,
    json: bool,
) -> ExitCode {
    let storage = match open(storage, blobs).await {
        Ok(storage) => storage,
        Err(code) => return code,
    };

    let row = match storage.checkpoint(thread_id, sequence).await {
        Ok(Some(row)) => row,
        Ok(None) => {
            eprintln!("strata: no checkpoint #{sequence} found for thread '{thread_id}'");
            return ExitCode::from(1);
        }
        Err(err) => {
            eprintln!("strata: query failed: {err}");
            return ExitCode::from(2);
        }
    };

    let state = &row.frozen_state;

    if json {
        let out = serde_json::json!({
            "thread_id": thread_id,
            "sequence": sequence,
            "status": store::status_name(row.status),
            "suspended_node": row.suspended_node,
            "next_node": row.next_node,
            "state": state,
            "nested": row.nested,
            "decision": row.decision,
        });
        println!("{}", serde_json::to_string_pretty(&out).unwrap());
    } else {
        println!("Loading historical state from Checkpoint #{sequence}... Done.");
        if let Some(node) = &row.suspended_node {
            println!("Suspended at node: {node}");
        }
        if let Some(path) = store::waiting_path(&row) {
            println!("{} inside: {path}", store::stopped_verb(row.status));
        }
        for decision in store::decisions(&row) {
            println!("Decision in effect: {decision}");
        }
        if let Some(next) = &row.next_node {
            println!("Would resume at node: {next}");
        }
        println!("State at this checkpoint:");
        println!("{}", serde_json::to_string_pretty(state).unwrap());
    }

    ExitCode::SUCCESS
}
