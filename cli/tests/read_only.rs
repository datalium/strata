//! The CLI is read-only (`design/51-Read-Only-Store-Open.md`, "The CLI"):
//! every command opens its store with the read-only factory, so it creates
//! nothing, migrates nothing and writes nothing. A missing SQLite file is an
//! error instead of an empty new database, and a Redis prefix no current
//! runtime has indexed is refused instead of indexed. The messages are the
//! shared ones of `schema/fixtures/checkpoint-log/read-only-errors.json`.
//!
//! The Redis and Postgres tests need `STRATA_TEST_REDIS_URL` /
//! `STRATA_TEST_POSTGRES_URL`; unset, they say `SKIPPED` and pass, unless
//! `STRATA_REQUIRE_BACKENDS=1`.

mod common;

use std::collections::BTreeMap;
use std::path::Path;
use std::process::{Command, Output};

use datalium_strata::schema::Checkpoint;

fn strata(args: &[&str], storage: &str, blobs: Option<&Path>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_strata"));
    command.args(args).arg("--storage").arg(storage);
    if let Some(blobs) = blobs {
        command.arg("--blobs").arg(blobs);
    }
    command.output().unwrap()
}

fn backend_url(var: &str) -> Option<String> {
    match std::env::var(var) {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            assert!(
                std::env::var("STRATA_REQUIRE_BACKENDS").as_deref() != Ok("1"),
                "{var} is not set, but STRATA_REQUIRE_BACKENDS=1 says these tests must run"
            );
            eprintln!("SKIPPED: {var} is not set (start `docker compose up -d --wait`)");
            None
        }
    }
}

fn unique(label: &str) -> String {
    format!(
        "strata_cli_test_{}_{}_{label}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

/// Every file in `dir`, with its SHA-256.
fn files(dir: &Path) -> BTreeMap<String, String> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            (
                path.file_name().unwrap().to_string_lossy().into_owned(),
                common::sha256(&path),
            )
        })
        .collect()
}

/// The shared query seed (`checkpoint-query/cases.json`).
fn seed() -> Vec<Checkpoint> {
    let raw = std::fs::read_to_string(format!(
        "{}/../schema/fixtures/checkpoint-query/cases.json",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap();
    let cases: serde_json::Value = serde_json::from_str(&raw).unwrap();
    serde_json::from_value(cases["seed"].clone()).unwrap()
}

/// Makes `dir` and every file in it read-only, and back.
#[cfg(unix)]
fn set_read_only(dir: &Path, read_only: bool) {
    use std::os::unix::fs::PermissionsExt;
    let (file, directory) = if read_only {
        (0o444, 0o555)
    } else {
        (0o644, 0o755)
    };
    if !read_only {
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(directory)).unwrap();
    }
    for entry in std::fs::read_dir(dir).unwrap() {
        std::fs::set_permissions(entry.unwrap().path(), std::fs::Permissions::from_mode(file))
            .unwrap();
    }
    if read_only {
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(directory)).unwrap();
    }
}

/// Every command, against a SQLite file the CLI may not change (on Unix, the
/// file and its directory are made read-only first; elsewhere only the
/// file's SHA-256 says so). Each succeeds, and the file is as it was, with no
/// `-journal`, `-wal` or `-shm` beside it.
#[tokio::test]
async fn every_command_reads_a_sqlite_file_without_changing_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.sqlite");
    let storage = path.to_str().unwrap().to_string();
    let seeded = common::seed(&storage).await;
    for checkpoint in seed() {
        seeded.insert(&checkpoint).await.unwrap();
    }
    seeded
        .hold(
            "approve-acme-1",
            "worker-2",
            std::time::Duration::from_secs(3600),
        )
        .await
        .unwrap();
    drop(seeded);
    #[cfg(unix)]
    set_read_only(dir.path(), true);
    let before = files(dir.path());
    let sha = common::sha256(&path);

    let commands: [&[&str]; 9] = [
        &["thread", "list"],
        &["thread", "list", "--json", "--status", "failed"],
        &["thread", "inspect", "failed-acme"],
        &["thread", "inspect", "approve-acme-1", "--json"],
        &["checkpoint", "log", "failed-acme"],
        &["checkpoint", "log", "resumed-acme", "--json"],
        &[
            "checkpoint",
            "diff",
            "failed-acme",
            "--seq-a",
            "0",
            "--seq-b",
            "2",
        ],
        &["checkpoint", "replay", "failed-acme", "--sequence", "1"],
        &[
            "checkpoint",
            "replay",
            "cancelled-nested",
            "--sequence",
            "0",
            "--json",
        ],
    ];
    let mut failed = Vec::new();
    for args in commands {
        let output = strata(args, &storage, None);
        if !output.status.success() {
            failed.push(format!(
                "{args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    }
    let after = files(dir.path());
    #[cfg(unix)]
    set_read_only(dir.path(), false);

    assert!(failed.is_empty(), "{failed:#?}");
    assert_eq!(common::sha256(&path), sha, "the file's SHA-256");
    assert_eq!(after, before, "nothing appeared beside it");
}

#[test]
fn thread_list_of_a_missing_sqlite_file_fails_and_creates_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("missing.db");
    let storage = path.to_str().unwrap();

    let output = strata(&["thread", "list"], storage, None);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(&common::open_error(
            "missing_sqlite_file",
            &[("path", storage)]
        )),
        "{stderr}"
    );
    assert!(!path.exists(), "no file was created");
    assert!(files(dir.path()).is_empty());
}

#[tokio::test]
async fn a_missing_blobs_directory_is_refused_and_not_created() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.sqlite");
    let storage = path.to_str().unwrap();
    common::create_sqlite(&path);
    let blobs = dir.path().join("blobs");
    let before = files(dir.path());

    let output = strata(&["checkpoint", "log", "any"], storage, Some(&blobs));

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(&common::open_error(
            "missing_blob_directory",
            &[("dir", blobs.to_str().unwrap())]
        )),
        "{stderr}"
    );
    assert!(!blobs.exists());
    assert_eq!(files(dir.path()), before);
}

#[tokio::test]
async fn a_redis_prefix_no_runtime_has_indexed_is_refused_not_indexed() {
    let Some(url) = backend_url("STRATA_TEST_REDIS_URL") else {
        return;
    };
    let prefix = unique("unindexed");
    let mut conn = redis::Client::open(url.as_str())
        .unwrap()
        .get_connection()
        .unwrap();
    // What a runtime from before the index wrote: a list, and no marker.
    let list = format!("{prefix}:checkpoints:old-thread");
    let _: i64 = redis::cmd("RPUSH")
        .arg(&list)
        .arg(serde_json::to_string(&seed()[0]).unwrap())
        .query(&mut conn)
        .unwrap();

    let output = strata(&["thread", "list"], &format!("{url}?prefix={prefix}"), None);

    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{prefix}:*"))
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("DEL").arg(&keys).query(&mut conn).unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(&common::open_error(
            "redis_prefix_not_indexed",
            &[("prefix", &prefix)]
        )),
        "{stderr}"
    );
    assert_eq!(keys, [list], "nothing was indexed");
}

#[tokio::test]
async fn a_postgres_schema_with_no_strata_log_is_refused_and_left_empty() {
    let Some(url) = backend_url("STRATA_TEST_POSTGRES_URL") else {
        return;
    };
    let schema = unique("empty");
    let admin = datalium_strata_memory::postgres_store::connect(&url)
        .await
        .unwrap();
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let database: String = admin
        .query_one("SELECT current_database()::text", &[])
        .await
        .unwrap()
        .get(0);
    let separator = if url.contains('?') { '&' } else { '?' };
    let storage = format!("{url}{separator}options=-c%20search_path%3D{schema}");

    let output = strata(&["thread", "list"], &storage, None);

    let tables: i64 = admin
        .query_one(
            "SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1",
            &[&schema],
        )
        .await
        .unwrap()
        .get(0);
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(&common::open_error(
            "no_strata_log",
            &[("store", &database)]
        )),
        "{stderr}"
    );
    assert_eq!(tables, 0, "nothing was created");
}
