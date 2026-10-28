//! Leases and the query port on the Cloud store
//! (`design/43-Cloud-Leases-And-Listing.md`, SG-36), beyond the shared
//! fixtures: the engine's leased runs, its wake and recovery drivers and a
//! wake through a resume, against the emulated Cloud; and what the store
//! refuses before it sends anything. Mirrors the .NET
//! `CloudLeaseAndListingTests`. Keep the two in step.

mod common;

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use common::{
    EmulatedCloud, RecordingSleep, Reply, Server, envelope, fixtures_dir, read_json, summary_json,
};
use datalium_strata::checkpoint::{
    CancelReason, CheckpointQuery, CheckpointStore, GraphExecutionResult, LeaseAttempt,
    LeaseOptions, LevelSignals, ThreadQuery, ThreadState, ThreadSummary,
};
use datalium_strata::context::RunConfig;
use datalium_strata::graph::{CheckpointPolicy, CompileOptions, CompiledGraph, StateGraph};
use datalium_strata::recovery::RecoverOptions;
use datalium_strata::schema::{Checkpoint, CheckpointStatus};
use datalium_strata::store::{LeasedSave, ThreadLease};
use datalium_strata_cloud::{StrataCloudCheckpointStore, equivalent};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Transfer {
    drafted: bool,
    sent: bool,
}

fn transfer() -> Transfer {
    Transfer {
        drafted: false,
        sent: false,
    }
}

/// `Draft` -> `Send`, with a checkpoint per node; `on_draft` runs inside
/// `Draft`.
fn durable_graph(on_draft: impl Fn() + Send + Sync + 'static) -> Arc<CompiledGraph<Transfer>> {
    Arc::new(
        StateGraph::new()
            .add_node("Draft", move |mut s: Transfer| {
                on_draft();
                s.drafted = true;
                s
            })
            .add_node("Send", |mut s: Transfer| {
                s.sent = true;
                s
            })
            .add_edge("Draft", "Send")
            .compile_with(CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode))
            .unwrap(),
    )
}

/// `Draft` -> a one-hour timer `Window` -> `Send`.
fn timer_graph() -> Arc<CompiledGraph<Transfer>> {
    Arc::new(
        StateGraph::new()
            .add_node("Draft", |mut s: Transfer| {
                s.drafted = true;
                s
            })
            .add_timer("Window", Duration::from_secs(3600))
            .add_node("Send", |mut s: Transfer| {
                s.sent = true;
                s
            })
            .add_edge("Draft", "Window")
            .add_edge("Window", "Send")
            .compile()
            .unwrap(),
    )
}

fn leased(owner: &str) -> RunConfig {
    RunConfig::new().with_lease(LeaseOptions::new(owner, Duration::from_secs(60)))
}

fn store_of(cloud: &EmulatedCloud) -> Arc<StrataCloudCheckpointStore> {
    Arc::new(
        StrataCloudCheckpointStore::new(cloud.server.options(&RecordingSleep::default())).unwrap(),
    )
}

async fn statuses(store: &StrataCloudCheckpointStore, thread_id: &str) -> Vec<CheckpointStatus> {
    store
        .get_log(thread_id, None, 100)
        .await
        .unwrap()
        .entries
        .into_iter()
        .map(|entry| entry.checkpoint.status)
        .collect()
}

#[tokio::test]
async fn a_leased_run_saves_every_checkpoint_under_its_lease_and_releases_it() {
    use CheckpointStatus::{Completed, Running};
    let cloud = EmulatedCloud::start().await;
    let store = store_of(&cloud);
    let graph = durable_graph(|| {});

    let result = graph
        .run_with("wire-1", transfer(), store.clone(), leased("worker-1"))
        .await;

    assert!(matches!(result, GraphExecutionResult::Completed(_)));
    assert_eq!(
        statuses(&store, "wire-1").await,
        [Running, Running, Completed]
    );
    let requests = cloud.server.requests();
    let saves: Vec<_> = requests
        .iter()
        .filter(|r| r.method == "POST" && r.path == "/v1/sdk/checkpoints")
        .collect();
    assert_eq!(3, saves.len());
    for save in saves {
        assert_eq!(Some("1"), save.header("strata-lease-token"));
    }
    let acquisition = requests
        .iter()
        .find(|r| r.method == "POST" && r.path == "/v1/sdk/leases")
        .unwrap();
    assert!(acquisition.header("idempotency-key").is_some());
    assert_eq!(
        Some((1, "worker-1".to_string(), true)),
        cloud.lease("wire-1")
    );
    assert_eq!(None, store.get_lease("wire-1").await.unwrap());
}

#[tokio::test]
async fn a_thread_another_owner_holds_is_refused_with_nothing_written() {
    let cloud = EmulatedCloud::start().await;
    let store = store_of(&cloud);
    cloud.take_over("wire-2", "worker-2");

    let result = durable_graph(|| {})
        .run_with("wire-2", transfer(), store.clone(), leased("worker-1"))
        .await;

    match result {
        GraphExecutionResult::Failed(error) => {
            let message = format!("{error:?}");
            assert!(
                message.contains("is held by 'worker-2' until "),
                "{message}"
            );
        }
        _ => panic!("expected the held thread to be refused"),
    }
    assert_eq!(0, cloud.saves("wire-2"));
}

#[tokio::test]
async fn a_save_after_a_takeover_is_cancelled_as_lease_lost_and_writes_nothing() {
    let cloud = Arc::new(EmulatedCloud::start().await);
    let store = store_of(&cloud);
    let thief = cloud.clone();
    // While `Draft` runs, the thread's lease runs out and another owner takes
    // it: the run's next save is fenced off.
    let graph = durable_graph(move || {
        thief.take_over("wire-3", "worker-2");
    });

    let result = graph
        .run_with("wire-3", transfer(), store.clone(), leased("worker-1"))
        .await;

    assert!(
        matches!(
            result,
            GraphExecutionResult::Cancelled {
                reason: CancelReason::LeaseLost,
                ..
            }
        ),
        "expected the run to lose its lease"
    );
    // Only the claim before `Draft`: nothing after the takeover.
    assert_eq!(1, cloud.saves("wire-3"));
    assert_eq!(
        Some((2, "worker-2".to_string(), false)),
        cloud.lease("wire-3")
    );
}

#[tokio::test]
async fn wake_due_follows_short_pages_and_wakes_every_due_timer() {
    let cloud = EmulatedCloud::start().await;
    let store = store_of(&cloud);
    let graph = timer_graph();
    for thread_id in ["timer-1", "timer-2", "timer-3"] {
        let result = graph.run(thread_id, transfer(), store.clone()).await;
        assert!(matches!(result, GraphExecutionResult::Interrupted { .. }));
    }
    // One thread a page, the first page empty: only a null cursor ends it.
    cloud.set_list_cap(Some(1));

    let report = graph
        .wake_due(store.clone(), Utc::now() + chrono::Duration::hours(2))
        .await
        .unwrap();

    assert_eq!(3, report.woken.len(), "skipped: {:?}", report.skipped);
    for woken in &report.woken {
        assert!(matches!(woken.result, GraphExecutionResult::Completed(_)));
    }
    let listings: Vec<_> = cloud
        .server
        .requests()
        .into_iter()
        .filter(|r| r.path == "/v1/sdk/threads")
        .collect();
    assert_eq!(4, listings.len());
    assert!(!listings[0].query.contains_key("cursor"));
    for listing in &listings[1..] {
        assert!(listing.query["cursor"].starts_with("emulated:"));
        assert!(listing.query.contains_key("due_by"));
    }
}

#[tokio::test]
async fn a_due_timer_wakes_through_a_resume_on_cloud() {
    let cloud = EmulatedCloud::start().await;
    let store = store_of(&cloud);
    let graph = timer_graph();
    graph.run("timer-4", transfer(), store.clone()).await;

    let early = graph
        .resume_with(
            "timer-4",
            LevelSignals::due(),
            store.clone(),
            RunConfig::new(),
        )
        .await;
    assert!(matches!(early, GraphExecutionResult::Failed(_)));

    let woken = graph
        .resume_with(
            "timer-4",
            LevelSignals::due_at(Utc::now() + chrono::Duration::hours(2)),
            store.clone(),
            leased("worker-1"),
        )
        .await;

    match woken {
        GraphExecutionResult::Completed(state) => assert!(state.sent),
        _ => panic!("expected the due timer to wake"),
    }
}

#[tokio::test]
async fn recover_stale_leaves_a_held_running_thread_and_recovers_it_once_let_go() {
    use CheckpointStatus::{Completed, Running};
    let cloud = EmulatedCloud::start().await;
    let store = store_of(&cloud);
    let graph = durable_graph(|| {});
    graph
        .run_with("wire-5", transfer(), store.clone(), leased("worker-1"))
        .await;

    // A thread whose process died after `Draft`, still held by its lease.
    let mut dead = store
        .get_log("wire-5", None, 100)
        .await
        .unwrap()
        .entries
        .remove(1)
        .checkpoint;
    dead.thread_id = "wire-dead".to_string();
    let lease = store
        .try_acquire_lease("wire-dead", "worker-9", Duration::from_secs(60))
        .await
        .unwrap()
        .acquired()
        .unwrap();
    assert_eq!(
        LeasedSave::Saved,
        store.save_under_lease(dead, &lease, None).await.unwrap()
    );

    let options = || RecoverOptions::new().with_config(leased("worker-1"));
    let held = graph
        .recover_stale(store.clone(), Utc::now(), options())
        .await
        .unwrap();
    assert!(held.recovered.is_empty());
    assert!(
        held.skipped[0]
            .reason
            .starts_with("it is held by 'worker-9' until "),
        "{}",
        held.skipped[0].reason
    );

    assert!(store.release_lease(&lease).await.unwrap());
    let report = graph
        .recover_stale(store.clone(), Utc::now(), options())
        .await
        .unwrap();

    assert_eq!(1, report.recovered.len());
    assert_eq!("wire-dead", report.recovered[0].thread_id);
    assert!(matches!(
        report.recovered[0].result,
        GraphExecutionResult::Completed(_)
    ));
    assert_eq!(
        statuses(&store, "wire-dead").await,
        [Running, Running, Completed]
    );
}

#[tokio::test]
async fn the_listing_maps_each_row_to_the_summary_the_engine_makes() {
    let cloud = EmulatedCloud::start().await;
    let store = store_of(&cloud);
    let mut seeded = Vec::new();
    for name in [
        "nested-timer",
        "nested-identified",
        "running",
        "ended",
        "waiting-event",
    ] {
        let checkpoint: Checkpoint = serde_json::from_value(read_json(
            &fixtures_dir().join(format!("../checkpoint-lifecycle/{name}.json")),
        ))
        .unwrap();
        cloud.seed(&checkpoint);
        seeded.push(ThreadSummary::of(0, &checkpoint).unwrap());
    }

    let page = store.list_threads(&ThreadQuery::default()).await.unwrap();

    assert_eq!(None, page.next_cursor);
    assert_eq!(seeded.len(), page.threads.len());
    for summary in &seeded {
        let listed = page
            .threads
            .iter()
            .find(|t| t.thread_id == summary.thread_id)
            .unwrap();
        assert_eq!(summary, listed);
    }

    let running = store
        .list_threads(&ThreadQuery::default().with_states([ThreadState::Running]))
        .await
        .unwrap();
    assert_eq!(1, running.threads.len());
    assert_eq!(ThreadState::Running, running.threads[0].state);
}

#[tokio::test]
async fn the_log_follows_early_stops_until_it_has_the_limit() {
    let cloud = EmulatedCloud::start().await;
    let store = store_of(&cloud);
    durable_graph(|| {})
        .run("wire-6", transfer(), store.clone())
        .await;
    assert_eq!(3, cloud.saves("wire-6"));
    cloud.set_log_cap(Some(1));

    let full = store.get_log("wire-6", None, 3).await.unwrap();
    let all = store.get_log("wire-6", None, 10).await.unwrap();
    let rest = store.get_log("wire-6", Some(0), 1).await.unwrap();
    let unknown = store.get_log("nobody", None, 10).await.unwrap();

    let sequences = |page: &datalium_strata::store::CheckpointLogPage| {
        page.entries.iter().map(|e| e.sequence).collect::<Vec<_>>()
    };
    assert_eq!(vec![0, 1, 2], sequences(&full));
    assert_eq!(Some(2), full.next_after_sequence);
    assert_eq!(vec![0, 1, 2], sequences(&all));
    assert_eq!(None, all.next_after_sequence);
    assert_eq!(vec![1], sequences(&rest));
    assert_eq!(Some(1), rest.next_after_sequence);
    assert!(unknown.entries.is_empty());
    assert_eq!(None, unknown.next_after_sequence);
}

#[tokio::test]
async fn each_acquisition_sends_an_idempotency_key_of_its_own() {
    let cloud = EmulatedCloud::start().await;
    let store = store_of(&cloud);

    let first = store
        .try_acquire_lease("t", "worker-1", Duration::from_secs(30))
        .await
        .unwrap();
    let second = store
        .try_acquire_lease("t", "worker-1", Duration::from_secs(30))
        .await
        .unwrap();

    assert!(matches!(first, LeaseAttempt::Acquired(_)));
    // The same owner name is held off too (contract §11).
    assert_eq!(
        first.acquired().map(|l| l.token),
        second.held_by().map(|l| l.token)
    );
    let keys: Vec<String> = cloud
        .server
        .requests()
        .iter()
        .map(|r| r.header("idempotency-key").unwrap().to_string())
        .collect();
    assert_eq!(2, keys.len());
    assert_ne!(keys[0], keys[1]);
}

#[tokio::test]
async fn a_423_on_a_save_without_a_lease_is_an_error_not_a_lost_lease() {
    let server = Server::scripted(vec![Reply::json(
        423,
        envelope(423, "STRATA_CLOUD_API_LEASE_LOST"),
    )])
    .await;
    let store =
        StrataCloudCheckpointStore::new(server.options(&RecordingSleep::default())).unwrap();
    let checkpoint: Checkpoint = serde_json::from_value(read_json(
        &fixtures_dir().join("../checkpoint-lifecycle/running.json"),
    ))
    .unwrap();

    let error = store.save(checkpoint).await.unwrap_err();

    assert!(
        error.0.contains("(HTTP 423 STRATA_CLOUD_API_LEASE_LOST)"),
        "{}",
        error.0
    );
    assert_eq!(1, server.requests().len());
}

#[tokio::test]
async fn what_a_store_would_refuse_is_refused_before_anything_is_sent() {
    let server = Server::scripted(Vec::new()).await;
    let store =
        StrataCloudCheckpointStore::new(server.options(&RecordingSleep::default())).unwrap();
    let checkpoint: Checkpoint = serde_json::from_value(read_json(
        &fixtures_dir().join("../checkpoint-lifecycle/running.json"),
    ))
    .unwrap();
    let other = ThreadLease {
        thread_id: "another-thread".to_string(),
        owner: "worker-1".to_string(),
        token: 1,
        expires_at: Utc::now(),
    };

    let mut version_without_id = ThreadQuery::default();
    version_without_id.graph_version = Some("2".to_string());
    assert!(store.list_threads(&version_without_id).await.is_err());
    assert!(
        store
            .list_threads(&ThreadQuery::default().with_limit(0))
            .await
            .is_err()
    );
    assert!(
        store
            .list_threads(&ThreadQuery::default().with_limit(1001))
            .await
            .is_err()
    );
    assert!(store.get_log("t", None, 0).await.is_err());
    assert!(store.get_log("t", None, 1001).await.is_err());
    assert!(
        store
            .try_acquire_lease("t", " ", Duration::from_secs(30))
            .await
            .is_err()
    );
    assert!(
        store
            .try_acquire_lease("t", "worker-1", Duration::ZERO)
            .await
            .is_err()
    );
    assert!(
        store
            .save_under_lease(checkpoint, &other, None)
            .await
            .is_err()
    );
    assert!(server.requests().is_empty());
}

fn case_query(json: &Value) -> ThreadQuery {
    let time = |value: &Value| {
        chrono::DateTime::parse_from_rfc3339(value.as_str().unwrap())
            .unwrap()
            .with_timezone(&Utc)
    };
    let mut query = ThreadQuery::default();
    for state in json["states"].as_array().into_iter().flatten() {
        query = query.with_states([ThreadState::parse(state.as_str().unwrap()).unwrap()]);
    }
    for (key, value) in json["metadata"].as_object().into_iter().flatten() {
        query = query.with_metadata(key, value.as_str().unwrap());
    }
    query.updated_after = json.get("updated_after").map(time);
    query.updated_before = json.get("updated_before").map(time);
    query.due_by = json.get("due_by").map(time);
    query.graph_id = json["graph_id"].as_str().map(str::to_string);
    query.graph_version = json["graph_version"].as_str().map(str::to_string);
    if let Some(limit) = json["limit"].as_u64() {
        query = query.with_limit(limit as usize);
    }
    query
}

/// `checkpoint-query/cases.json` through the Cloud store: every filter goes
/// over the wire and back, and the emulator answers by the engine's own
/// rules, with a cursor of its own. Mirrors the .NET store's run of
/// `CheckpointQueryContractTests.RunCasesAsync`.
#[tokio::test]
async fn the_shared_query_cases_are_answered_through_the_cloud_store() {
    let cloud = EmulatedCloud::start().await;
    let store = store_of(&cloud);
    let cases = read_json(&fixtures_dir().join("../checkpoint-query/cases.json"));
    let seed: Vec<Checkpoint> = serde_json::from_value(cases["seed"].clone()).unwrap();
    for checkpoint in &seed {
        store.save(checkpoint.clone()).await.unwrap();
    }

    for case in cases["list"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let mut query = case_query(&case["query"]);
        let mut pages: Vec<Vec<String>> = Vec::new();
        loop {
            let page = store.list_threads(&query).await.unwrap();
            assert!(page.threads.len() <= query.limit, "{name}: page too long");
            for summary in &page.threads {
                let mut row = summary_json(summary);
                row.as_object_mut().unwrap().remove("thread_id");
                assert_eq!(
                    cases["summaries"][&summary.thread_id], row,
                    "{name}: {}",
                    summary.thread_id
                );
            }
            pages.push(page.threads.iter().map(|t| t.thread_id.clone()).collect());
            match page.next_cursor {
                Some(cursor) => {
                    assert!(cursor.starts_with("emulated:"), "{name}: {cursor}");
                    query = query.with_cursor(cursor);
                }
                None => break,
            }
            assert!(pages.len() < 50, "{name}: the cursor never ended");
        }
        let expected: Vec<Vec<String>> = serde_json::from_value(case["pages"].clone()).unwrap();
        assert_eq!(expected, pages, "{name}");
    }

    for case in cases["log"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let thread_id = case["thread_id"].as_str().unwrap();
        let page = store
            .get_log(
                thread_id,
                case["after_sequence"].as_i64(),
                case["limit"].as_u64().unwrap() as usize,
            )
            .await
            .unwrap();
        let sequences: Vec<i64> = page.entries.iter().map(|e| e.sequence).collect();
        assert_eq!(serde_json::json!(sequences), case["sequences"], "{name}");
        assert_eq!(
            case["next_after_sequence"].as_i64(),
            page.next_after_sequence,
            "{name}"
        );
        let log: Vec<&Checkpoint> = seed.iter().filter(|c| c.thread_id == thread_id).collect();
        for entry in &page.entries {
            assert!(
                equivalent(log[entry.sequence as usize], &entry.checkpoint),
                "{name}: entry {}",
                entry.sequence
            );
        }
    }

    // A cursor Cloud didn't issue is Cloud's to refuse.
    assert!(
        store
            .list_threads(&ThreadQuery::default().with_cursor("not a cursor"))
            .await
            .is_err()
    );
}
