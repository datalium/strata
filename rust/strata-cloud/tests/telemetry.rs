//! The Cloud adapter's spans and metrics (`design/30-Strata-Cloud-Store.md`,
//! "Telemetry"). Mirrors the telemetry tests in the .NET `CloudStoreTests`. A
//! binary of its own: it installs a global subscriber and metrics recorder.

mod common;

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use common::{API_KEY, RecordingSleep, Reply, Server, envelope};
use datalium_strata::store::{CheckpointQuery, CheckpointStore, ThreadQuery};
use datalium_strata_cloud::{DecisionQuery, Decisions, StrataCloudCheckpointStore};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde_json::json;
use tracing::Instrument;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Subscriber, info_span};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

#[derive(Debug, Clone)]
struct SpanRecord {
    name: &'static str,
    fields: HashMap<String, String>,
    parent: Option<&'static str>,
}

#[derive(Default)]
struct Captured {
    spans: Vec<SpanRecord>,
    open: HashMap<u64, usize>,
}

static CAPTURED: LazyLock<Mutex<Captured>> = LazyLock::new(Mutex::default);

struct Fields<'a>(&'a mut HashMap<String, String>);

impl Visit for Fields<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}

struct Capture;

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut fields = HashMap::new();
        attrs.record(&mut Fields(&mut fields));
        let parent = ctx
            .span(id)
            .and_then(|span| span.parent())
            .map(|parent| parent.name());
        let mut captured = CAPTURED.lock().unwrap();
        let index = captured.spans.len();
        captured.spans.push(SpanRecord {
            name: attrs.metadata().name(),
            fields,
            parent,
        });
        captured.open.insert(id.into_u64(), index);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, _ctx: Context<'_, S>) {
        let mut captured = CAPTURED.lock().unwrap();
        if let Some(&index) = captured.open.get(&id.into_u64()) {
            values.record(&mut Fields(&mut captured.spans[index].fields));
        }
    }

    fn on_close(&self, id: Id, _ctx: Context<'_, S>) {
        CAPTURED.lock().unwrap().open.remove(&id.into_u64());
    }
}

static SNAPSHOTTER: LazyLock<Snapshotter> = LazyLock::new(|| {
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Capture)).unwrap();
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().unwrap();
    snapshotter
});

fn requests(route: &str) -> Vec<SpanRecord> {
    CAPTURED
        .lock()
        .unwrap()
        .spans
        .iter()
        .filter(|s| s.name == "strata.cloud.request")
        .filter(|s| s.fields.get("strata.cloud.route").map(String::as_str) == Some(route))
        .cloned()
        .collect()
}

#[tokio::test]
async fn each_attempt_is_a_span_each_retry_is_counted_and_idle_polls_leave_no_trace() {
    LazyLock::force(&SNAPSHOTTER);

    // A save: one span per attempt, children of the caller's span.
    let server = Server::scripted(vec![
        Reply::json(503, envelope(503, "STRATA_CLOUD_API_DATABASE_ERROR")).with_header("Retry-After", "2"),
        Reply::json(201, json!({ "thread_id": "thread_hitl_demo", "sequence": 0, "received_at": "2026-10-09T12:00:00Z" })),
    ])
    .await;
    let store =
        StrataCloudCheckpointStore::new(server.options(&RecordingSleep::default())).unwrap();
    let checkpoint = serde_json::from_value(common::read_json(
        &common::fixtures_dir().join("../checkpoint-lifecycle/frozen.json"),
    ))
    .unwrap();
    store
        .save_checkpoint(&checkpoint, None)
        .instrument(info_span!("strata.node"))
        .await
        .unwrap();

    let saves = requests("checkpoints.save");
    assert_eq!(2, saves.len());
    assert_eq!(Some("strata.node"), saves[0].parent);
    assert_eq!("POST", saves[0].fields["http.request.method"]);
    assert_eq!("503", saves[0].fields["http.response.status_code"]);
    assert_eq!("503", saves[0].fields["error.type"]);
    assert_eq!("thread_hitl_demo", saves[0].fields["strata.thread.id"]);
    assert_eq!("1", saves[0].fields["strata.cloud.attempt"]);
    assert_eq!("201", saves[1].fields["http.response.status_code"]);
    assert_eq!("2", saves[1].fields["strata.cloud.attempt"]);
    assert!(!saves[1].fields.contains_key("error.type"));

    let retries: u64 = SNAPSHOTTER
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, ..)| key.key().name() == "strata.cloud.retries")
        .filter(|(key, ..)| {
            let labels: HashMap<_, _> = key
                .key()
                .labels()
                .map(|l| (l.key().to_string(), l.value().to_string()))
                .collect();
            labels.get("route").map(String::as_str) == Some("checkpoints.save")
                && labels.get("reason").map(String::as_str) == Some("status_503")
        })
        .map(|(.., value)| match value {
            DebugValue::Counter(n) => n,
            _ => 0,
        })
        .sum();
    assert_eq!(1, retries);

    // Decisions: an empty long-poll leaves no span; one that brings work does.
    let item = json!({
        "approval_id": "A1", "thread_id": "t", "sequence": 0, "decided_at": "2026-10-09T12:00:00Z",
        "signal": { "is_approved": true, "reviewer_token": "alice", "field_overrides": {} }
    });
    let server = Server::scripted(vec![
        Reply::json(200, json!({ "items": [], "next_cursor": null })),
        Reply::json(200, json!({ "items": [item], "next_cursor": null })),
    ])
    .await;
    let decisions = Decisions::new(server.options(&RecordingSleep::default())).unwrap();
    let poll = DecisionQuery::new().with_wait(Duration::from_secs(25));
    decisions.get(&poll).await.unwrap();
    assert!(requests("decisions").is_empty());
    decisions.get(&poll).await.unwrap();
    let polls = requests("decisions");
    assert_eq!(1, polls.len());
    assert_eq!("1", polls[0].fields["strata.cloud.items"]);
    assert!(polls[0].fields.contains_key("strata.cloud.duration_ms"));

    // Leases and the listing (design/43): a span per attempt under a route of
    // their own, with the thread when there is one.
    let server = Server::scripted(vec![
        Reply::json(503, envelope(503, "STRATA_CLOUD_API_DATABASE_ERROR")),
        Reply::json(200, json!({ "lease": null })),
        Reply::json(200, json!({ "threads": [], "next_cursor": null })),
    ])
    .await;
    let store =
        StrataCloudCheckpointStore::new(server.options(&RecordingSleep::default())).unwrap();
    assert_eq!(None, store.get_lease("leased-thread").await.unwrap());
    store.list_threads(&ThreadQuery::default()).await.unwrap();
    let gets = requests("leases.get");
    assert_eq!(2, gets.len());
    assert_eq!("GET", gets[1].fields["http.request.method"]);
    assert_eq!("leased-thread", gets[1].fields["strata.thread.id"]);
    assert_eq!("2", gets[1].fields["strata.cloud.attempt"]);
    let listings = requests("threads.list");
    assert_eq!(1, listings.len());
    assert!(!listings[0].fields.contains_key("strata.thread.id"));

    // Nothing recorded carries the key.
    for span in &CAPTURED.lock().unwrap().spans {
        for value in span.fields.values() {
            assert!(!value.contains(API_KEY), "{span:?}");
        }
    }
}
