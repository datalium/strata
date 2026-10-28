//! Mirrors the .NET `CloudStoreTests` (`design/30-Strata-Cloud-Store.md`,
//! SG-18): the Cloud store's behaviour beyond the shared fixtures. Keep the
//! two in step. Telemetry is in `telemetry.rs`, a binary of its own, since it
//! installs global recorders.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{
    EmulatedCloud, RecordingSleep, Reply, Rollout, Server, envelope, initial, rollout_graph,
};
use datalium_strata::checkpoint::{GraphExecutionResult, HumanSignal};
use datalium_strata::store::CheckpointStore;
use datalium_strata_cloud::{
    CloudError, SaveOutcome, StrataCloudCheckpointStore, StrataCloudOptions,
    is_entitlement_exceeded, wire,
};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde_json::json;

fn completed(result: GraphExecutionResult<Rollout>) -> Rollout {
    match result {
        GraphExecutionResult::Completed(state) => state,
        _ => panic!("expected Completed"),
    }
}

fn frozen() -> datalium_strata::schema::Checkpoint {
    serde_json::from_value(common::read_json(
        &common::fixtures_dir().join("../checkpoint-lifecycle/frozen.json"),
    ))
    .unwrap()
}

#[tokio::test]
async fn a_run_saved_to_cloud_and_resumed_from_it_ends_as_on_the_in_memory_store() {
    let (graph, executed) = rollout_graph();
    let cloud = EmulatedCloud::start().await;
    let store: Arc<dyn CheckpointStore> = Arc::new(
        StrataCloudCheckpointStore::new(cloud.server.options(&RecordingSleep::default())).unwrap(),
    );

    let first = graph.run("roll-1", initial(), store.clone()).await;
    assert!(matches!(first, GraphExecutionResult::Interrupted { .. }));

    // Cloud's copy really is normalised: 1.0 came back as 1.
    let stored = cloud.latest("roll-1");
    assert_eq!("1", stored["frozen_state"]["amount"].to_string());

    let signal = HumanSignal::approve("alice").with_override("replicas", 3);
    let resumed = completed(graph.resume("roll-1", signal.clone(), store).await);

    let local: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    graph.run("roll-1", initial(), local.clone()).await;
    let expected = completed(graph.resume("roll-1", signal, local).await);

    assert_eq!(expected, resumed);
    assert_eq!(9_007_199_254_740_993, resumed.big);
    assert_eq!(vec!["prepared", "scaled to 3"], resumed.log);
    assert_eq!(2, executed.load(Ordering::SeqCst));

    // The resume's stamp is the thread's latest on Cloud: the decision is
    // acknowledged.
    assert!(!cloud.latest("roll-1")["resumed_at"].is_null());
}

#[tokio::test]
async fn the_conditional_save_refuses_a_stale_sequence_and_falls_back_without_the_headers() {
    let cloud = EmulatedCloud::start().await;
    let store = Arc::new(
        StrataCloudCheckpointStore::new(cloud.server.options(&RecordingSleep::default())).unwrap(),
    );
    let (graph, _) = rollout_graph();
    graph.run("roll-2", initial(), store.clone()).await;

    let latest = store.load_checkpoint("roll-2").await.unwrap().unwrap();
    assert_eq!(Some(cloud.saves("roll-2") as i64 - 1), latest.sequence);
    let sequence = latest.sequence.unwrap();
    let mut stamp = latest.checkpoint.clone();
    stamp.resumed_at = Some("2026-10-09T12:00:00Z".to_string());

    assert_eq!(
        SaveOutcome::NotLatest,
        store
            .save_checkpoint(&stamp, Some(sequence - 1))
            .await
            .unwrap()
    );
    assert_eq!(sequence as usize + 1, cloud.saves("roll-2"));
    assert!(
        store
            .save_if_latest(stamp.clone(), Some(sequence))
            .await
            .unwrap()
    );

    // A Cloud without the headers: no sequence on the load, and the expected
    // sequence is ignored.
    cloud.set_conditional(false);
    let unsequenced = store.load_checkpoint("roll-2").await.unwrap().unwrap();
    assert_eq!(None, unsequenced.sequence);
    stamp.resumed_at = Some("2026-10-09T12:00:01Z".to_string());
    assert!(store.save_if_latest(stamp, Some(0)).await.unwrap());
}

#[tokio::test]
async fn a_new_execution_past_the_plan_limit_reaches_the_host_as_entitlement_exceeded() {
    let server = Server::scripted(vec![Reply::json(
        403,
        envelope(403, wire::ENTITLEMENT_EXCEEDED_CODE),
    )])
    .await;
    let store = Arc::new(
        StrataCloudCheckpointStore::new(server.options(&RecordingSleep::default())).unwrap(),
    );
    let (graph, _) = rollout_graph();

    match graph.run("roll-3", initial(), store.clone()).await {
        GraphExecutionResult::Failed(error) => {
            assert!(is_entitlement_exceeded(&error), "{error:?}")
        }
        _ => panic!("expected Failed"),
    }
    assert_eq!(1, server.requests().len());

    // The typed error, for a caller of the store's own methods.
    let server = Server::scripted(vec![Reply::json(
        403,
        envelope(403, wire::ENTITLEMENT_EXCEEDED_CODE),
    )])
    .await;
    let store =
        StrataCloudCheckpointStore::new(server.options(&RecordingSleep::default())).unwrap();
    assert!(matches!(
        store.save_checkpoint(&frozen(), None).await,
        Err(CloudError::EntitlementExceeded { .. })
    ));
}

#[tokio::test]
async fn an_attempt_that_hangs_times_out_and_is_retried() {
    let server = Server::scripted(vec![
        Reply::Hang,
        Reply::json(
            201,
            json!({ "thread_id": "t", "sequence": 0, "received_at": "2026-10-09T12:00:00Z" }),
        ),
    ])
    .await;
    let sleep = RecordingSleep::default();
    let store = StrataCloudCheckpointStore::new(
        server
            .options(&sleep)
            .with_request_timeout(Duration::from_millis(300)),
    )
    .unwrap();

    assert_eq!(
        SaveOutcome::Saved,
        store.save_checkpoint(&frozen(), None).await.unwrap()
    );
    assert_eq!(2, server.requests().len());
    assert_eq!(1, sleep.waits().len());
}

#[tokio::test]
async fn computed_waits_are_jittered_under_the_doubling_cap() {
    let conflict = || Reply::json(409, envelope(409, "STRATA_CLOUD_API_SEQUENCE_CONFLICT"));
    let server = Server::scripted(vec![conflict(), conflict(), conflict(), conflict()]).await;
    let sleep = RecordingSleep::default();
    let store = StrataCloudCheckpointStore::new(server.options(&sleep)).unwrap();

    let failure = store.save_checkpoint(&frozen(), None).await.unwrap_err();

    assert!(failure.is_transient());
    assert_eq!(4, failure.attempts());
    let waits = sleep.waits();
    assert_eq!(3, waits.len());
    for (wait, cap) in waits.iter().zip([200, 400, 800]) {
        assert!(*wait <= Duration::from_millis(cap), "{wait:?}");
    }
}

#[tokio::test]
async fn a_retry_after_beyond_the_longest_wait_fails_at_once() {
    let server = Server::scripted(vec![
        Reply::json(429, envelope(429, "STRATA_CLOUD_API_RATE_LIMITED"))
            .with_header("Retry-After", "120"),
    ])
    .await;
    let sleep = RecordingSleep::default();
    let store = StrataCloudCheckpointStore::new(server.options(&sleep)).unwrap();

    let failure = store.load_checkpoint("t").await.unwrap_err();

    assert!(failure.is_transient());
    assert_eq!(Some(Duration::from_secs(120)), failure.retry_after());
    assert!(sleep.waits().is_empty());
    assert_eq!(1, server.requests().len());
}

#[tokio::test]
async fn a_404_that_is_not_thread_not_found_is_an_error_not_an_empty_thread() {
    let server = Server::scripted(vec![Reply::json(
        404,
        envelope(404, "STRATA_CLOUD_API_NOT_FOUND_ERROR"),
    )])
    .await;
    let store =
        StrataCloudCheckpointStore::new(server.options(&RecordingSleep::default())).unwrap();

    let failure = store.load_checkpoint("t").await.unwrap_err();

    assert!(matches!(failure, CloudError::Rejected { status: 404, .. }));
}

#[tokio::test]
async fn the_key_is_on_every_request_and_in_no_message_or_debug() {
    let cloud = EmulatedCloud::start().await;
    let options = cloud.server.options(&RecordingSleep::default());
    let store: Arc<dyn CheckpointStore> =
        Arc::new(StrataCloudCheckpointStore::new(options.clone()).unwrap());
    let (graph, _) = rollout_graph();

    graph.run("roll-4", initial(), store.clone()).await;
    graph
        .resume("roll-4", HumanSignal::approve("alice"), store)
        .await;
    let wrong = StrataCloudCheckpointStore::new(StrataCloudOptions::new(
        &cloud.server.url,
        "strata_live_WRONG",
    ))
    .unwrap();
    let refused = wrong.load_checkpoint("roll-4").await.unwrap_err();

    let requests = cloud.server.requests();
    for request in &requests[..requests.len() - 1] {
        assert_eq!(
            Some(format!("Bearer {}", common::API_KEY).as_str()),
            request.header("authorization")
        );
    }
    assert!(matches!(refused, CloudError::Unauthorized { .. }));
    assert!(!refused.to_string().contains("strata_live_WRONG"));
    assert!(!format!("{refused:?}").contains("strata_live_WRONG"));
    assert!(!format!("{options:?}").contains(common::API_KEY));
}
