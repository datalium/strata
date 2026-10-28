//! The run scheduler on its own (`design/40-Run-Scheduler.md`): the shared
//! cases in `schema/fixtures/scheduler/cases.json`, which
//! `dotnet/tests/Datalium.Strata.Tests/Scheduling/RunSchedulerTests.cs` replays
//! too, and the waits that need a clock (a paused tokio runtime). Keep the two
//! in step.

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use datalium_strata::CancellationToken;
use datalium_strata::scheduler::{
    AdmissionRequest, NotAdmitted, NotAdmittedReason, RunScheduler, RunSlot, SchedulerOptions,
    TenantUsage,
};
use serde_json::Value;
use tokio::task::JoinHandle;

fn cases() -> Vec<Value> {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/fixtures/scheduler/cases.json");
    let json: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    json["cases"].as_array().unwrap().clone()
}

fn options_from(json: &Value) -> SchedulerOptions {
    let mut options = SchedulerOptions::new(
        json.get("max_concurrent")
            .and_then(Value::as_u64)
            .unwrap_or(16) as u32,
    );
    if let Some(max) = json.get("max_queued").and_then(Value::as_u64) {
        options = options.with_max_queued(max as u32);
    }
    for reservation in json
        .get("reservations")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        options = options.with_reservation(
            reservation["min_priority"].as_i64().unwrap() as i32,
            reservation["slots"].as_u64().unwrap() as u32,
        );
    }
    if let Some(limit) = json.get("tenant_limit").and_then(Value::as_u64) {
        options = options.with_tenant_limit(limit as u32);
    }
    for (tenant, limit) in json
        .get("tenant_limits")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
    {
        options = options.with_tenant_override(tenant.clone(), limit.as_u64().unwrap() as u32);
    }
    if let Some(max) = json.get("tenant_max_queued").and_then(Value::as_u64) {
        options = options.with_tenant_max_queued(max as u32);
    }
    options
}

type Admission = JoinHandle<Result<RunSlot, NotAdmitted>>;

fn names(json: &Value) -> BTreeSet<String> {
    json.as_array()
        .unwrap()
        .iter()
        .map(|id| id.as_str().unwrap().to_string())
        .collect()
}

/// A run's decision, a while at most.
async fn decided(at: &str, id: &str, admission: Admission) -> Result<RunSlot, NotAdmitted> {
    tokio::time::timeout(Duration::from_secs(10), admission)
        .await
        .unwrap_or_else(|_| panic!("{at}: '{id}' was never decided"))
        .unwrap()
}

#[tokio::test]
async fn every_shared_case() {
    let cases = cases();
    assert!(!cases.is_empty());
    for case in &cases {
        play(case).await;
    }
}

async fn play(case: &Value) {
    let name = case["name"].as_str().unwrap();
    let scheduler = RunScheduler::new(options_from(&case["options"])).unwrap();
    let mut pending: HashMap<String, Admission> = HashMap::new();
    let mut tokens: HashMap<String, CancellationToken> = HashMap::new();
    let mut slots: HashMap<String, RunSlot> = HashMap::new();
    let mut queued: BTreeSet<String> = BTreeSet::new();
    for (number, step) in case["steps"].as_array().unwrap().iter().enumerate() {
        let at = format!("{name}, step {}", number + 1);
        if let Some(id) = step.get("arrive").and_then(Value::as_str) {
            let token = CancellationToken::new();
            let mut request = AdmissionRequest::new()
                .with_priority(step.get("priority").and_then(Value::as_i64).unwrap_or(0) as i32)
                .with_cancellation(token.clone());
            if let Some(tenant) = step.get("tenant").and_then(Value::as_str) {
                request = request.with_tenant(tenant);
            }
            // The place in the queue is taken here, before the task first runs.
            pending.insert(id.to_string(), tokio::spawn(scheduler.admit(request)));
            tokens.insert(id.to_string(), token);
            queued.insert(id.to_string());
        } else if let Some(id) = step.get("release").and_then(Value::as_str) {
            drop(slots.remove(id).expect("a run holding a slot"));
        } else if let Some(id) = step.get("cancel").and_then(Value::as_str) {
            tokens[id].cancel();
        } else if let Some(tenant) = step.get("set_tenant_limit").and_then(Value::as_str) {
            scheduler.set_tenant_limit(tenant, step["limit"].as_u64().map(|l| l as u32));
        } else if step.get("close").is_some() {
            scheduler.close();
        }

        let active = names(&step["active"]);
        let expected_queued = names(&step["queued"]);
        let refused: HashMap<String, String> = step["refused"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(id, reason)| (id.clone(), reason.as_str().unwrap().to_string()))
            .collect();

        // Every run the step decided has its decision.
        for id in queued.clone() {
            if active.contains(&id) {
                let decided = decided(&at, &id, pending.remove(&id).unwrap()).await;
                let slot = decided
                    .unwrap_or_else(|r| panic!("{at}: '{id}' should be admitted, was {r:?}"));
                slots.insert(id.clone(), slot);
                queued.remove(&id);
            } else if let Some(reason) = refused.get(&id) {
                let decided = decided(&at, &id, pending.remove(&id).unwrap()).await;
                let refusal = decided
                    .err()
                    .unwrap_or_else(|| panic!("{at}: '{id}' should be refused {reason}"));
                assert_eq!(refusal.reason.as_str(), reason, "{at}: '{id}'");
                queued.remove(&id);
            }
        }

        let holding: BTreeSet<String> = slots.keys().cloned().collect();
        assert_eq!(holding, active, "{at}: active");
        assert_eq!(queued, expected_queued, "{at}: queued");
        for id in &queued {
            assert!(
                !pending[id].is_finished(),
                "{at}: '{id}' should still be waiting"
            );
        }
        let snapshot = scheduler.snapshot();
        assert_eq!(
            (snapshot.active as usize, snapshot.queued as usize),
            (active.len(), queued.len()),
            "{at}: snapshot"
        );
    }
}

fn one_slot(max_queue_time: Option<Duration>) -> RunScheduler {
    let mut options = SchedulerOptions::new(1);
    if let Some(time) = max_queue_time {
        options = options.with_max_queue_time(time);
    }
    RunScheduler::new(options).unwrap()
}

/// Lets the spawned tasks run until they wait again.
async fn settle() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_run_waits_at_most_max_queue_time() {
    let scheduler = one_slot(Some(Duration::from_secs(30)));
    let _holder = scheduler.admit(AdmissionRequest::new()).await.unwrap();

    let waiting = tokio::spawn(scheduler.admit(AdmissionRequest::new()));
    tokio::time::advance(Duration::from_secs(29)).await;
    settle().await;
    assert!(!waiting.is_finished());

    tokio::time::advance(Duration::from_secs(1)).await;
    let refused = waiting.await.unwrap().unwrap_err();
    assert_eq!(refused.reason, NotAdmittedReason::QueueTimeout);
    assert_eq!(refused.waited, Duration::from_secs(30));
    assert_eq!(scheduler.snapshot().queued, 0);
}

#[tokio::test(start_paused = true)]
async fn a_deadline_ends_a_wait_and_the_earlier_of_the_two_says_why() {
    let scheduler = one_slot(Some(Duration::from_secs(300)));
    let _holder = scheduler.admit(AdmissionRequest::new()).await.unwrap();

    let deadline = (tokio::time::Instant::now() + Duration::from_secs(10)).into_std();
    let waiting = tokio::spawn(scheduler.admit(AdmissionRequest::new().with_deadline(deadline)));
    tokio::time::advance(Duration::from_secs(10)).await;

    let refused = waiting.await.unwrap().unwrap_err();
    assert_eq!(refused.reason, NotAdmittedReason::Deadline);
    assert_eq!(refused.waited, Duration::from_secs(10));
}

#[tokio::test(start_paused = true)]
async fn a_deadline_already_passed_refuses_a_wait_at_once_but_not_a_free_slot() {
    let scheduler = one_slot(None);
    let late = || {
        AdmissionRequest::new()
            .with_deadline((tokio::time::Instant::now() - Duration::from_secs(1)).into_std())
    };

    // A free slot is taken: the run's own deadline check stops it, as for a run with no scheduler.
    let _admitted = scheduler.admit(late()).await.unwrap();

    let refused = scheduler.admit(late()).await.unwrap_err();
    assert_eq!(refused.reason, NotAdmittedReason::Deadline);
}

#[tokio::test(start_paused = true)]
async fn an_admission_that_races_its_timeout_keeps_its_slot() {
    let scheduler = one_slot(Some(Duration::from_secs(10)));
    let holder = scheduler.admit(AdmissionRequest::new()).await.unwrap();
    let waiting = tokio::spawn(scheduler.admit(AdmissionRequest::new()));

    // Admitted under the lock first; the timer that fires next finds it decided.
    drop(holder);
    tokio::time::advance(Duration::from_secs(10)).await;

    let _admitted = waiting.await.unwrap().unwrap();
    assert_eq!(scheduler.snapshot().active, 1);
}

#[tokio::test]
async fn dropping_a_waiting_admission_withdraws_it_and_dropping_a_decided_one_gives_its_slot_back()
{
    let scheduler = one_slot(None);
    let holder = scheduler.admit(AdmissionRequest::new()).await.unwrap();

    let withdrawn = scheduler.admit(AdmissionRequest::new());
    assert_eq!(scheduler.snapshot().queued, 1);
    drop(withdrawn);
    assert_eq!(scheduler.snapshot().queued, 0);

    // Admitted while nobody was polling it: dropping the future gives the slot back.
    let unpolled = scheduler.admit(AdmissionRequest::new());
    drop(holder);
    assert_eq!(scheduler.snapshot().active, 1);
    drop(unpolled);
    assert_eq!(scheduler.snapshot().active, 0);

    let _next = scheduler.admit(AdmissionRequest::new()).await.unwrap();
    assert_eq!(scheduler.snapshot().active, 1);
}

#[tokio::test]
async fn the_snapshot_shows_each_tenant_and_its_limit() {
    let scheduler = RunScheduler::new(
        SchedulerOptions::new(4)
            .with_name("investigations")
            .with_tenant_limit(2)
            .with_tenant_override("paused", 0),
    )
    .unwrap();
    let _a = scheduler
        .admit(AdmissionRequest::new().with_tenant("acme"))
        .await
        .unwrap();
    let waiting = tokio::spawn(scheduler.admit(AdmissionRequest::new().with_tenant("paused")));

    let snapshot = scheduler.snapshot();
    assert_eq!(snapshot.name, "investigations");
    assert_eq!(
        (
            snapshot.active,
            snapshot.queued,
            snapshot.max_concurrent,
            snapshot.max_queued,
            snapshot.closed
        ),
        (1, 1, 4, 1024, false)
    );
    let usage = |active, queued, limit| TenantUsage {
        active,
        queued,
        limit,
    };
    assert_eq!(snapshot.tenants["acme"], usage(1, 0, Some(2)));
    assert_eq!(snapshot.tenants["paused"], usage(0, 1, Some(0)));

    scheduler.close();
    assert_eq!(
        waiting.await.unwrap().unwrap_err().reason,
        NotAdmittedReason::Closed
    );
    assert!(scheduler.is_closed());
    assert_eq!(scheduler.snapshot().tenants["paused"], usage(0, 0, Some(0)));
}

#[test]
fn invalid_options_are_refused() {
    let refused = |options: SchedulerOptions| match RunScheduler::new(options) {
        Err(datalium_strata::StrataError::ExecutionAborted(message)) => message,
        other => panic!("expected a refusal, got {other:?}"),
    };
    assert!(refused(SchedulerOptions::new(0)).contains("max_concurrent must be at least 1"));
    assert!(
        refused(SchedulerOptions::new(4).with_max_queue_time(Duration::ZERO))
            .contains("max_queue_time")
    );
    assert!(refused(SchedulerOptions::new(4).with_tenant_key(" ")).contains("tenant_key"));
    assert!(refused(SchedulerOptions::new(4).with_name("")).contains("name"));
    assert!(refused(SchedulerOptions::new(4).with_reservation(3, 0)).contains("at least one slot"));
    assert!(
        refused(
            SchedulerOptions::new(4)
                .with_reservation(3, 1)
                .with_reservation(3, 1)
        )
        .contains("two reservations are for priority 3")
    );
    assert!(
        refused(
            SchedulerOptions::new(4)
                .with_reservation(3, 2)
                .with_reservation(2, 2)
        )
        .contains("the reservations keep 4 of 4 slots")
    );
}

#[test]
fn a_refusal_reads_like_the_dotnet_engines() {
    let refused = |reason| {
        NotAdmitted {
            reason,
            waited: Duration::ZERO,
        }
        .to_string()
    };
    assert_eq!(
        refused(NotAdmittedReason::QueueFull),
        "run not admitted: the scheduler's queue is full"
    );
    assert_eq!(
        refused(NotAdmittedReason::Shed),
        "run not admitted: shed from the scheduler's queue for a higher-priority run"
    );
    assert_eq!(
        refused(NotAdmittedReason::QueueTimeout),
        "run not admitted: no slot within the scheduler's longest wait"
    );
    assert_eq!(
        refused(NotAdmittedReason::Cancelled),
        "run not admitted: cancelled while waiting for a slot"
    );
    assert_eq!(
        refused(NotAdmittedReason::Deadline),
        "run not admitted: its deadline passed while it waited for a slot"
    );
    assert_eq!(
        refused(NotAdmittedReason::Closed),
        "run not admitted: the scheduler is closed"
    );
}
