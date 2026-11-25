//! The CLI's step of the cross-engine migration test
//! (`design/Arch-Specs/STRATA-SPEC-005.md`, "Conformance";
//! `schema/fixtures/cross-engine/scenario.json`): after the Rust engine has
//! read, migrated and resumed the .NET engine's databases, the CLI reads them
//! too: every thread listed, each lifecycle fixture replayed as it was
//! written, the resumed threads completed, and the current database's rows,
//! deltas and offloaded values the .NET store wrote, decoded.
//!
//! Before that, in a step of its own (`STRATA_INTEROP_STEP=read-older`), the
//! CLI reads both engines' older databases, at the checkpoints component's
//! version 1, as they were written: read-only
//! (`design/51-Read-Only-Store-Open.md`), so before any engine has migrated
//! them, and leaving them as they were for the engines' steps after it.
//!
//! Each runs only as a step of `.github/workflows/cross-engine-ci.yml`, with
//! `STRATA_INTEROP_DIR` and `STRATA_INTEROP_STEP` set (and
//! `STRATA_TEST_POSTGRES_URL`); otherwise it says `SKIPPED` and passes: the CLI
//! writes no databases of its own to read back.

use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

const OTHER: &str = "dotnet";

/// The engines whose databases the `read-older` step reads: both, unless
/// `STRATA_INTEROP_ENGINES` names fewer (to run one engine's write step and
/// this one locally).
fn engines() -> Vec<String> {
    std::env::var("STRATA_INTEROP_ENGINES")
        .ok()
        .filter(|engines| !engines.is_empty())
        .unwrap_or_else(|| "rust,dotnet".to_string())
        .split(',')
        .map(|engine| engine.trim().to_string())
        .collect()
}

/// The interop directory, if this run is the cross-engine step `step`.
fn step_dir(step: &str) -> Option<PathBuf> {
    match std::env::var("STRATA_INTEROP_DIR") {
        Ok(dir)
            if !dir.is_empty() && std::env::var("STRATA_INTEROP_STEP").as_deref() == Ok(step) =>
        {
            Some(PathBuf::from(dir))
        }
        _ => {
            eprintln!(
                "SKIPPED: a step of the cross-engine job (STRATA_INTEROP_DIR, STRATA_INTEROP_STEP={step})"
            );
            None
        }
    }
}

/// `url` with its connections' `search_path` set to `schema`.
fn in_schema(url: &str, schema: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}options=-c%20search_path%3D{schema}")
}

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

/// Each lifecycle fixture as the older database holds it once the reader
/// has migrated and resumed it: listed, its first entry its fixture, the
/// resumed threads completed and every other log one entry long.
fn read_older(storage: &str) {
    read_older_with(storage, |thread_id, entries, resumed| {
        if resumed {
            let last = entries.last().unwrap();
            assert_eq!(last["status"], "completed", "{storage}: {thread_id}");
        } else {
            assert_eq!(entries.len(), 1, "{storage}: {thread_id}");
        }
    });
}

/// [`read_older`] as the writer left it, before any engine has resumed a
/// thread: every log is its one fixture.
fn read_older_as_written(storage: &str) {
    read_older_with(storage, |thread_id, entries, _| {
        assert_eq!(entries.len(), 1, "{storage}: {thread_id}");
    });
}

/// Every lifecycle fixture listed and its first entry as written, then
/// `logs` given each thread's whole log and whether the scenario resumes it.
fn read_older_with(storage: &str, logs: impl Fn(&str, &[Value], bool)) {
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
        logs(&thread_id, &entries, resumed.contains(&thread_id.as_str()));
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
    let Some(dir) = step_dir("read") else {
        return;
    };
    let url = std::env::var("STRATA_TEST_POSTGRES_URL")
        .expect("a cross-engine step needs STRATA_TEST_POSTGRES_URL");
    let schema = |database: &str| in_schema(&url, &format!("strata_interop_{OTHER}_{database}"));
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

/// The SHA-256 of the file at `path`, as hex.
fn sha256(path: &Path) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(std::fs::read(path).unwrap())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A Postgres schema's checkpoints component version, and whether it has a
/// blob table.
async fn postgres_layout(url: &str, schema: &str) -> (i64, bool) {
    let client = datalium_strata_memory::postgres_store::connect(&in_schema(url, schema))
        .await
        .unwrap();
    let version = client
        .query_one(
            "SELECT version FROM strata_schema_version WHERE component = 'checkpoints'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let blobs = client
        .query_one("SELECT to_regclass('checkpoint_blobs') IS NOT NULL", &[])
        .await
        .unwrap()
        .get(0);
    (version, blobs)
}

/// The `read-older` step, after both engines wrote and before either reads:
/// the CLI reads each engine's older database at version 1, read-only, and
/// leaves it as it was, the SQLite file byte for byte and the Postgres schema
/// at version 1 with no blob table, for the engines to migrate next.
#[tokio::test(flavor = "multi_thread")]
async fn the_cli_reads_the_older_databases_before_any_engine_migrates_them() {
    let Some(dir) = step_dir("read-older") else {
        return;
    };
    let url = std::env::var("STRATA_TEST_POSTGRES_URL")
        .expect("a cross-engine step needs STRATA_TEST_POSTGRES_URL");
    let written = fixture("cross-engine/scenario.json")["older"]["written_checkpoints_version"]
        .as_i64()
        .unwrap();
    for engine in engines() {
        let path = dir.join(format!("{engine}-older.sqlite"));
        let before = sha256(&path);
        read_older_as_written(path.to_str().unwrap());
        assert_eq!(sha256(&path), before, "{}", path.display());
        for suffix in ["-journal", "-wal", "-shm"] {
            let beside = PathBuf::from(format!("{}{suffix}", path.display()));
            assert!(!beside.exists(), "{}", beside.display());
        }

        let schema = format!("strata_interop_{engine}_older");
        assert_eq!(
            postgres_layout(&url, &schema).await,
            (written, false),
            "{schema} as written"
        );
        read_older_as_written(&in_schema(&url, &schema));
        assert_eq!(
            postgres_layout(&url, &schema).await,
            (written, false),
            "{schema} after the CLI read it"
        );
    }
}
