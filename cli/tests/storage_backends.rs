//! `--storage` takes a SQLite path, a `postgres://` URL or a `redis://` URL
//! (`design/19-Async-Queryable-Store.md`, "The CLI"): the same commands read
//! the same answers from each, through `strata-memory`'s query port. The seed
//! is the shared query fixture's (`schema/fixtures/checkpoint-query/cases.json`).
//!
//! The Postgres and Redis tests need `STRATA_TEST_POSTGRES_URL` /
//! `STRATA_TEST_REDIS_URL` (the repo's `docker-compose.yml`); unset, they say
//! `SKIPPED` and pass, unless `STRATA_REQUIRE_BACKENDS=1` (set in CI).

use std::process::Command;

use datalium_strata::schema::Checkpoint;

fn strata() -> Command {
    Command::new(env!("CARGO_BIN_EXE_strata"))
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

fn seed() -> Vec<Checkpoint> {
    let raw = std::fs::read_to_string(format!(
        "{}/../schema/fixtures/checkpoint-query/cases.json",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap();
    let cases: serde_json::Value = serde_json::from_str(&raw).unwrap();
    serde_json::from_value(cases["seed"].clone()).unwrap()
}

/// Seeds `storage` with the shared query seed, through the CLI's own store,
/// and has `worker-2` hold one of its threads (`design/32-Thread-Leases.md`).
async fn seed_storage(storage: &str) {
    let opened = cli::store::open(storage).await.unwrap();
    for checkpoint in seed() {
        opened.insert(&checkpoint).await.unwrap();
    }
    let held = opened
        .hold(
            "approve-acme-1",
            "worker-2",
            std::time::Duration::from_secs(3600),
        )
        .await
        .unwrap();
    assert_eq!(held.map(|lease| lease.token), Some(1));
}

fn run(storage: &str, args: &[&str]) -> String {
    let output = strata()
        .args(args)
        .arg("--storage")
        .arg(storage)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn listed(storage: &str, args: &[&str]) -> Vec<String> {
    let mut all = vec!["thread", "list", "--json"];
    all.extend_from_slice(args);
    let listed: serde_json::Value = serde_json::from_str(&run(storage, &all)).unwrap();
    listed
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["thread_id"].as_str().unwrap().to_string())
        .collect()
}

/// The same questions, answered the same way by every backend.
fn assert_answers(storage: &str) {
    // The waiting page: for a human, an event or a timer, newest first.
    assert_eq!(
        listed(storage, &["--status", "interrupted"]),
        [
            "approve-globex",
            "approve-acme-2",
            "approve-acme-1",
            "nested-waiting",
            "timer-initech",
            "event-globex",
            "event-forever",
            "nested-timer"
        ]
    );
    // A resumed thread is no longer listed as interrupted.
    assert_eq!(
        listed(storage, &["--status", "resumed"]),
        ["resumed-acme", "timer-resumed"]
    );
    // Due threads (design/29-Event-Waits-And-Timers.md): the innermost wake
    // time decides, a resumed timer isn't due, and a wait with no timeout never is.
    assert_eq!(
        listed(storage, &["--due-by", "2026-10-01T13:00:00Z"]),
        ["timer-initech", "nested-timer"]
    );
    assert_eq!(
        listed(
            storage,
            &[
                "--due-by",
                "2026-10-01T14:00:00Z",
                "--meta",
                "tenant=globex"
            ]
        ),
        ["event-globex"]
    );
    // Long past every wake time.
    assert_eq!(
        listed(storage, &["--due"]),
        ["timer-initech", "event-globex", "nested-timer"]
    );
    let waiting = run(storage, &["thread", "inspect", "event-globex"]);
    assert!(
        waiting.contains("Waiting for:     event 'deployment.finished'"),
        "{waiting}"
    );
    assert!(
        waiting.contains("Times out at:    2026-10-01T14:00:00Z"),
        "{waiting}"
    );
    let timer = run(storage, &["thread", "inspect", "nested-timer"]);
    assert!(timer.contains("Waiting for:     a timer"), "{timer}");
    assert!(
        timer.contains("Wakes at:        2026-10-01T12:30:00Z"),
        "{timer}"
    );
    assert!(
        run(storage, &["thread", "list"]).contains("timer until 2026-10-01T13:00:00Z"),
        "the table says what each thread waits for"
    );
    assert_eq!(
        listed(storage, &["--meta", "tenant=acme", "--status", "failed"]),
        ["failed-acme"]
    );
    assert_eq!(listed(storage, &[]).len(), 15);
    // One graph's threads (design/31-Graph-Identity.md): the outermost
    // level's identity decides, and a version narrows it.
    assert_eq!(
        listed(storage, &["--graph", "billing"]),
        ["approve-acme-2", "approve-acme-1", "failed-acme"]
    );
    assert_eq!(
        listed(storage, &["--graph", "billing", "--graph-version", "2"]),
        ["approve-acme-2", "failed-acme"]
    );
    assert!(listed(storage, &["--graph", "approval"]).is_empty());
    let nested = run(storage, &["thread", "inspect", "nested-waiting"]);
    assert!(
        nested.contains("Graph:           incident version 1"),
        "{nested}"
    );
    assert!(
        nested.contains("Graph at Work:   approval version 3"),
        "{nested}"
    );
    assert_eq!(listed(storage, &["--status", "ended"]), ["ended-acme"]);
    assert!(run(storage, &["thread", "list"]).contains("Work/Approve"));

    let inspect = run(storage, &["thread", "inspect", "failed-acme"]);
    assert!(inspect.contains("Checkpoint count: 3"), "{inspect}");
    assert!(inspect.contains("Thread state:    failed"), "{inspect}");

    // Who holds a thread, when someone does (design/32-Thread-Leases.md).
    assert!(!inspect.contains("Lease:"), "{inspect}");
    let held = run(storage, &["thread", "inspect", "approve-acme-1"]);
    let lease = held
        .lines()
        .find(|line| line.trim_start().starts_with("Lease:"))
        .unwrap_or_else(|| panic!("no lease line in: {held}"));
    assert!(
        lease.starts_with("  Lease:           held by 'worker-2' until 20")
            && lease.ends_with("Z (token 1)"),
        "{lease}"
    );
    let inspected = |thread: &str| -> serde_json::Value {
        serde_json::from_str(&run(storage, &["thread", "inspect", thread, "--json"])).unwrap()
    };
    let held = inspected("approve-acme-1");
    assert_eq!(held["lease"]["owner"], "worker-2");
    assert_eq!(held["lease"]["token"], 1);
    assert!(held["lease"]["expires_at"].is_string());
    assert!(inspected("failed-acme")["lease"].is_null());
    assert!(
        run(storage, &["checkpoint", "log", "resumed-acme"]).contains("#1"),
        "the resume stamp is the second entry"
    );
    let replay: serde_json::Value = serde_json::from_str(&run(
        storage,
        &[
            "checkpoint",
            "replay",
            "cancelled-nested",
            "--sequence",
            "0",
            "--json",
        ],
    ))
    .unwrap();
    assert_eq!(replay["nested"]["suspended_node"], "Send");

    // A running thread (design/33-Durable-Execution.md), recovered once.
    assert_eq!(
        listed(storage, &["--status", "running"]),
        ["running-initech"]
    );
    let running = run(storage, &["thread", "inspect", "running-initech"]);
    assert!(running.contains("Thread state:    running"), "{running}");
    assert!(
        running.contains("Running:         after 'Plan', next 'Execute'"),
        "{running}"
    );
    assert!(running.contains("Recoveries:      1"), "{running}");

    let unknown = strata()
        .args([
            "thread",
            "list",
            "--status",
            "sleeping",
            "--storage",
            storage,
        ])
        .output()
        .unwrap();
    assert!(!unknown.status.success());
}

#[tokio::test]
async fn a_sqlite_file_answers_through_the_query_port() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.sqlite");
    let storage = path.to_str().unwrap();
    seed_storage(storage).await;
    assert_answers(storage);
}

#[tokio::test]
async fn a_postgres_url_answers_the_same() {
    let Some(url) = backend_url("STRATA_TEST_POSTGRES_URL") else {
        return;
    };
    // A schema of its own, named in the URL's `options` so the CLI's own
    // connection uses it too.
    let schema = unique("pg");
    let admin = datalium_strata_memory::postgres_store::connect(&url)
        .await
        .unwrap();
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let separator = if url.contains('?') { '&' } else { '?' };
    let storage = format!("{url}{separator}options=-c%20search_path%3D{schema}");

    seed_storage(&storage).await;
    let checked = std::panic::catch_unwind(|| assert_answers(&storage));
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    checked.unwrap();
}

#[tokio::test]
async fn a_redis_url_answers_the_same() {
    let Some(url) = backend_url("STRATA_TEST_REDIS_URL") else {
        return;
    };
    let prefix = unique("redis");
    let storage = format!("{url}?prefix={prefix}");

    seed_storage(&storage).await;
    let checked = std::panic::catch_unwind(|| assert_answers(&storage));
    let mut conn = redis::Client::open(url.as_str())
        .unwrap()
        .get_connection()
        .unwrap();
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{prefix}:*"))
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("DEL").arg(keys).query(&mut conn).unwrap();
    checked.unwrap();
}
