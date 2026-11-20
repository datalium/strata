//! The CLI's step of the cross-engine migration test
//! (`design/Arch-Specs/STRATA-SPEC-005.md`, "Conformance";
//! `schema/fixtures/cross-engine/scenario.json`): after the Rust engine has
//! read, migrated and resumed the .NET engine's databases, the CLI reads them
//! too: every thread listed, each lifecycle fixture replayed as it was
//! written, the resumed threads completed, and the current database's rows,
//! deltas and offloaded values the .NET store wrote, decoded.
//!
//! It runs only as a step of `.github/workflows/cross-engine-ci.yml`, with
//! `STRATA_INTEROP_DIR` and `STRATA_INTEROP_STEP=read` set (and
//! `STRATA_TEST_POSTGRES_URL`); otherwise it says `SKIPPED` and passes: the CLI
//! writes no databases of its own to read back.

use std::path::PathBuf;
use std::process::Command;

use serde_json::Value;

const OTHER: &str = "dotnet";

fn fixture(path: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../schema/fixtures")
        .join(path);
    serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap()
}

fn read(storage: &str, args: &[&str]) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_strata"))
        .args(args)
        .arg("--json")
        .arg("--storage")
        .arg(storage)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{args:?} on {storage}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn log(storage: &str, thread_id: &str) -> Vec<Value> {
    read(storage, &["checkpoint", "log", thread_id])
        .as_array()
        .unwrap()
        .clone()
}

fn replay(storage: &str, thread_id: &str, sequence: &Value) -> Value {
    read(
        storage,
        &[
            "checkpoint",
            "replay",
            thread_id,
            "--sequence",
            &sequence.to_string(),
        ],
    )
}

fn read_older(storage: &str) {
    let scenario = fixture("cross-engine/scenario.json");
    let older = &scenario["older"];
    let fixtures = older["fixtures"].as_str().unwrap();
    let prefix = older["thread_prefix"].as_str().unwrap();
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../schema/fixtures")
        .join(fixtures);
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.ends_with(".json"))
        .collect();
    names.sort();

    let listed: Vec<String> = read(storage, &["thread", "list"])
        .as_array()
        .unwrap()
        .iter()
        .map(|thread| thread["thread_id"].as_str().unwrap().to_string())
        .collect();
    let resumed: Vec<&str> = older["resumes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|resume| resume["thread_id"].as_str().unwrap())
        .collect();
    for name in &names {
        let thread_id = format!("{prefix}{}", name.trim_end_matches(".json"));
        let expected = fixture(&format!("{fixtures}/{name}"));
        assert!(listed.contains(&thread_id), "{storage}: {thread_id} listed");
        let entries = log(storage, &thread_id);
        let first = &entries[0];
        assert_eq!(
            first["status"], expected["status"],
            "{storage}: {thread_id}"
        );
        assert_eq!(
            replay(storage, &thread_id, &first["sequence"])["state"],
            expected["frozen_state"],
            "{storage}: {thread_id}"
        );
        let last = entries.last().unwrap();
        if resumed.contains(&thread_id.as_str()) {
            assert_eq!(last["status"], "completed", "{storage}: {thread_id}");
        } else {
            assert_eq!(entries.len(), 1, "{storage}: {thread_id}");
        }
    }
}

fn read_current(storage: &str) {
    let scenario = fixture("cross-engine/scenario.json");
    let current = &scenario["current"];
    let thread_id = current["thread_id"].as_str().unwrap();
    let evidence_bytes = current["evidence_bytes"].as_u64().unwrap() as usize;
    let entries = log(storage, thread_id);
    let written = current["written"].as_array().unwrap();
    assert!(
        entries.len() > written.len(),
        "{storage}: the reader's rows follow"
    );
    for (entry, row) in entries.iter().zip(written) {
        assert_eq!(
            (&entry["status"], &entry["next_node"]),
            (&row["status"], &row["next_node"]),
            "{storage}"
        );
        let state = &replay(storage, thread_id, &entry["sequence"])["state"];
        assert_eq!(state["trail"], row["trail"], "{storage}");
        assert_eq!(
            state["evidence"].as_str().map(str::len),
            Some(evidence_bytes),
            "{storage}"
        );
    }
    assert_eq!(entries.last().unwrap()["status"], "completed", "{storage}");
    let inspected = read(storage, &["thread", "inspect", thread_id]);
    assert_eq!(inspected["status"], "completed", "{storage}");
}

#[test]
fn the_cli_reads_what_the_other_engine_wrote() {
    let dir = match std::env::var("STRATA_INTEROP_DIR") {
        Ok(dir)
            if !dir.is_empty() && std::env::var("STRATA_INTEROP_STEP").as_deref() == Ok("read") =>
        {
            PathBuf::from(dir)
        }
        _ => {
            eprintln!(
                "SKIPPED: a step of the cross-engine job (STRATA_INTEROP_DIR, STRATA_INTEROP_STEP=read)"
            );
            return;
        }
    };
    let url = std::env::var("STRATA_TEST_POSTGRES_URL")
        .expect("a cross-engine step needs STRATA_TEST_POSTGRES_URL");
    let separator = if url.contains('?') { '&' } else { '?' };
    let schema = |database: &str| {
        format!("{url}{separator}options=-c%20search_path%3Dstrata_interop_{OTHER}_{database}")
    };
    let sqlite = |database: &str| {
        dir.join(format!("{OTHER}-{database}.sqlite"))
            .to_str()
            .unwrap()
            .to_string()
    };

    read_older(&sqlite("older"));
    read_older(&schema("older"));
    read_current(&sqlite("current"));
    read_current(&schema("current"));
}
