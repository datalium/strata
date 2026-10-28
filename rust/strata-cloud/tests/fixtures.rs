//! The shared wire fixtures (`schema/fixtures/cloud/`,
//! `design/30-Strata-Cloud-Store.md`, "Tests"): each is a recorded exchange
//! the adapter must send and the answer it must make of it. The .NET
//! `CloudFixtureTests` reads the same files: that's the parity check.

mod common;

use std::path::PathBuf;
use std::time::Duration;

use chrono::{DateTime, Utc};
use common::{
    API_KEY, Recorded, RecordingSleep, Reply, Server, fixtures_dir, read_json, summary_json,
};
use datalium_strata::schema::Checkpoint;
use datalium_strata::store::{
    CheckpointQuery, CheckpointStore, LeaseAttempt, LeasedSave, ThreadLease, ThreadQuery,
    ThreadState,
};
use datalium_strata_cloud::{
    CloudError, DecisionPage, DecisionQuery, Decisions, SaveOutcome, StrataCloudCheckpointStore,
    equivalent,
};
use serde_json::Value;

fn fixture_names() -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(fixtures_dir())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .map(|path| path.file_stem().unwrap().to_string_lossy().to_string())
        .collect();
    names.sort();
    names
}

#[test]
fn every_fixture_is_read() {
    assert_eq!(fixture_names().len(), 41);
}

#[tokio::test]
async fn every_fixture() {
    for name in fixture_names() {
        run_fixture(&name).await;
    }
}

fn relative(path: &Value) -> PathBuf {
    fixtures_dir().join(path.as_str().unwrap())
}

fn checkpoint_from(value: &Value) -> Checkpoint {
    let json = if value.is_string() {
        read_json(&relative(value))
    } else {
        value.clone()
    };
    serde_json::from_value(json).unwrap()
}

fn kind(error: &CloudError) -> &'static str {
    match error {
        CloudError::Unauthorized { .. } => "unauthorized",
        CloudError::EntitlementExceeded { .. } => "entitlement_exceeded",
        CloudError::Rejected { .. } => "rejected",
        CloudError::Unavailable { .. } => "unavailable",
        CloudError::InvalidResponse { .. } => "invalid_response",
        _ => "other",
    }
}

async fn run_fixture(name: &str) {
    let fixture = read_json(&fixtures_dir().join(format!("{name}.json")));
    let mut script = Vec::new();
    for exchange in fixture["exchanges"].as_array().unwrap() {
        let response = &exchange["response"];
        if response["lost"] == Value::Bool(true) {
            script.push(Reply::Lose);
            continue;
        }
        let body = match response.get("body_file") {
            Some(file) => Some(read_json(&relative(file))),
            None => Some(response["body"].clone()).filter(|b| !b.is_null()),
        };
        let headers = response["headers"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string()))
            .collect();
        script.push(Reply::Respond {
            status: response["status"].as_u64().unwrap() as u16,
            headers,
            body,
        });
    }
    let server = Server::scripted(script).await;
    let sleep = RecordingSleep::default();
    let mut options = server.options(&sleep).with_retry(common::unjittered());
    if let Some(threshold) = fixture["options"]["gzip_threshold_bytes"].as_u64() {
        options = options.with_gzip_threshold(threshold as usize);
    }
    let expect = &fixture["expect"];
    let mut sent: Option<Checkpoint> = None;

    let outcome: Result<(), CloudError> = match fixture["operation"].as_str().unwrap() {
        "save" => {
            let checkpoint = checkpoint_from(&fixture["checkpoint"]);
            sent = Some(checkpoint.clone());
            let store = StrataCloudCheckpointStore::new(options).unwrap();
            let expected = fixture["expected_sequence"].as_i64();
            store
                .save_checkpoint(&checkpoint, expected)
                .await
                .map(|saved| {
                    assert!(expect["error"].is_null(), "{name}: expected an error");
                    assert_eq!(
                        expect["saved"].as_bool().unwrap(),
                        saved == SaveOutcome::Saved,
                        "{name}"
                    );
                })
        }
        "load" => {
            let store = StrataCloudCheckpointStore::new(options).unwrap();
            store
                .load_checkpoint(fixture["thread_id"].as_str().unwrap())
                .await
                .map(|latest| {
                    assert!(expect["error"].is_null(), "{name}: expected an error");
                    if expect["checkpoint"].is_null() {
                        assert!(latest.is_none(), "{name}");
                    } else {
                        let latest = latest.unwrap_or_else(|| panic!("{name}: no checkpoint"));
                        let expected = checkpoint_from(&expect["checkpoint"]);
                        assert!(
                            equivalent(&expected, &latest.checkpoint),
                            "{name}: loaded {:?}",
                            latest.checkpoint
                        );
                        assert_eq!(expect["sequence"].as_i64(), latest.sequence, "{name}");
                    }
                })
        }
        "decisions" => {
            let decisions = Decisions::new(options).unwrap();
            let query = &fixture["query"];
            let mut ask = DecisionQuery::new()
                .with_wait(Duration::from_secs(query["wait_seconds"].as_u64().unwrap()));
            if let Some(thread_id) = query["thread_id"].as_str() {
                ask = ask.with_thread_id(thread_id);
            }
            if let Some(limit) = query["limit"].as_u64() {
                ask = ask.with_limit(limit as u32);
            }
            if let Some(cursor) = query["cursor"].as_str() {
                ask = ask.with_cursor(cursor);
            }
            decisions.get(&ask).await.map(|page| {
                assert!(expect["error"].is_null(), "{name}: expected an error");
                assert_page(name, &expect["page"], &page);
            })
        }
        operation @ ("acquire" | "renew" | "release" | "get_lease" | "save_under_lease"
        | "list" | "log") => {
            let store = StrataCloudCheckpointStore::new(options).unwrap();
            // Through the ports, whose error is a string: a refusal is
            // checked by the status and code its message carries.
            match run_port(name, operation, &fixture, &store, &mut sent).await {
                Ok(()) => assert!(expect["error"].is_null(), "{name}: expected an error"),
                Err(message) => {
                    let expected = &expect["error"];
                    assert!(!expected.is_null(), "{name}: unexpected {message}");
                    assert_eq!("rejected", expected["kind"].as_str().unwrap(), "{name}");
                    let detail = format!(
                        "(HTTP {} {})",
                        expected["status"],
                        expected["code"].as_str().unwrap()
                    );
                    assert!(message.contains(&detail), "{name}: {message}");
                    assert!(!message.contains(API_KEY), "{name}");
                }
            }
            Ok(())
        }
        other => panic!("{name}: unknown operation {other}"),
    };

    if let Err(error) = outcome {
        let expected = &expect["error"];
        assert!(!expected.is_null(), "{name}: unexpected {error}");
        assert_eq!(expected["kind"].as_str().unwrap(), kind(&error), "{name}");
        assert_eq!(
            expected["status"].as_u64().map(|s| s as u16),
            error.status(),
            "{name}"
        );
        assert_eq!(expected["code"].as_str(), error.code(), "{name}");
        assert_eq!(
            expected["attempts"].as_u64().unwrap() as u32,
            error.attempts(),
            "{name}"
        );
        assert_eq!(
            expected["retry_after_ms"].as_u64(),
            error.retry_after().map(|d| d.as_millis() as u64),
            "{name}"
        );
        assert!(!error.to_string().contains(API_KEY), "{name}");
    }

    assert_requests(
        name,
        fixture["exchanges"].as_array().unwrap(),
        &server.requests(),
        sent.as_ref(),
    );

    if let Some(header) = expect["same_header"].as_str() {
        let requests = server.requests();
        let first = requests[0].header(header);
        assert!(first.is_some(), "{name}: {header}");
        for request in &requests {
            assert_eq!(first, request.header(header), "{name}: {header}");
        }
    }

    let min_waits: Vec<u64> = expect["min_waits_ms"]
        .as_array()
        .map(|waits| waits.iter().map(|w| w.as_u64().unwrap()).collect())
        .unwrap_or_default();
    let waits = sleep.waits();
    assert_eq!(min_waits.len(), waits.len(), "{name}: waits {waits:?}");
    for (wait, min) in waits.iter().zip(&min_waits) {
        assert!(*wait >= Duration::from_millis(*min), "{name}: {wait:?}");
        assert!(
            *wait <= Duration::from_millis((*min).max(5000)),
            "{name}: {wait:?}"
        );
    }
}

fn assert_requests(
    name: &str,
    exchanges: &[Value],
    requests: &[Recorded],
    sent: Option<&Checkpoint>,
) {
    assert_eq!(exchanges.len(), requests.len(), "{name}: requests");
    for (exchange, actual) in exchanges.iter().zip(requests) {
        let expected = &exchange["request"];
        assert_eq!(
            expected["method"].as_str().unwrap(),
            actual.method,
            "{name}"
        );
        assert_eq!(expected["path"].as_str().unwrap(), actual.path, "{name}");
        // A key whose value is a list is repeated, once per value.
        let mut query: Vec<(String, String)> = Vec::new();
        for (key, value) in expected["query"].as_object().unwrap() {
            match value {
                Value::Array(values) => query.extend(
                    values
                        .iter()
                        .map(|v| (key.clone(), v.as_str().unwrap().to_string())),
                ),
                value => query.push((key.clone(), value.as_str().unwrap().to_string())),
            }
        }
        query.sort();
        let mut pairs = actual.query_pairs.clone();
        pairs.sort();
        assert_eq!(query, pairs, "{name}: query");
        for (header, value) in expected["headers"].as_object().unwrap() {
            let value = value.as_str().unwrap().replace("{api_key}", API_KEY);
            assert_eq!(
                Some(value.as_str()),
                actual.header(header),
                "{name}: {header}"
            );
        }
        for absent in expected["absent_headers"].as_array().unwrap() {
            let absent = absent.as_str().unwrap();
            assert_eq!(None, actual.header(absent), "{name}: {absent}");
        }
        for present in expected["present_headers"].as_array().into_iter().flatten() {
            let present = present.as_str().unwrap();
            assert!(actual.header(present).is_some(), "{name}: {present}");
        }
        if expected["body"].is_null() {
            assert!(actual.body.is_empty(), "{name}: body");
        } else if expected["body"].is_object() {
            assert_eq!(expected["body"], actual.json(), "{name}: body");
        } else {
            assert_eq!("checkpoint", expected["body"].as_str().unwrap());
            let received: Checkpoint = serde_json::from_slice(&actual.body).unwrap();
            assert!(equivalent(sent.unwrap(), &received), "{name}: body");
        }
    }
}

fn assert_page(name: &str, expected: &Value, page: &DecisionPage) {
    assert_eq!(
        expected["next_cursor"].as_str(),
        page.next_cursor.as_deref(),
        "{name}"
    );
    let items = expected["items"].as_array().unwrap();
    assert_eq!(items.len(), page.items.len(), "{name}");
    for (item, decision) in items.iter().zip(&page.items) {
        assert_eq!(item["approval_id"], decision.approval_id.as_str());
        assert_eq!(item["thread_id"], decision.thread_id.as_str());
        assert_eq!(item["sequence"].as_i64().unwrap(), decision.sequence);
        assert_eq!(
            DateTime::parse_from_rfc3339(item["decided_at"].as_str().unwrap()).unwrap(),
            decision.decided_at
        );
        assert_eq!(
            item["decided_by"].as_str(),
            decision.decided_by.as_deref(),
            "{name}"
        );
        assert_eq!(
            item["decided_by_email"].as_str(),
            decision.decided_by_email.as_deref(),
            "{name}"
        );
        assert_eq!(item["notes"].as_str(), decision.notes.as_deref(), "{name}");
        let signal = &item["signal"];
        assert_eq!(signal["is_approved"], decision.signal.is_approved);
        assert_eq!(
            signal["reviewer_token"],
            decision.signal.reviewer_token.as_str()
        );
        let field_overrides: std::collections::HashMap<String, String> =
            serde_json::from_value(signal["field_overrides"].clone()).unwrap();
        assert_eq!(field_overrides, decision.signal.field_overrides, "{name}");
        let overrides: std::collections::HashMap<String, Value> =
            serde_json::from_value(signal["overrides"].clone()).unwrap();
        assert_eq!(overrides, decision.signal.overrides, "{name}");
    }
}

/// Runs a lease or query port operation, checking a success against
/// `expect`; a failure is the store error's message.
async fn run_port(
    name: &str,
    operation: &str,
    fixture: &Value,
    store: &StrataCloudCheckpointStore,
    sent: &mut Option<Checkpoint>,
) -> Result<(), String> {
    let expect = &fixture["expect"];
    let ttl = || Duration::from_millis(fixture["ttl_ms"].as_u64().unwrap());
    match operation {
        "acquire" => {
            let attempt = store
                .try_acquire_lease(
                    fixture["thread_id"].as_str().unwrap(),
                    fixture["owner"].as_str().unwrap(),
                    ttl(),
                )
                .await
                .map_err(|err| err.0)?;
            let expected = lease(&expect["lease"]);
            if expect["acquired"].as_bool().unwrap() {
                assert_eq!(LeaseAttempt::Acquired(expected), attempt, "{name}");
            } else {
                assert_eq!(LeaseAttempt::Held(expected), attempt, "{name}");
            }
        }
        "renew" => {
            let renewed = store
                .renew_lease(&lease(&fixture["lease"]), ttl())
                .await
                .map_err(|err| err.0)?;
            assert_eq!(optional_lease(&expect["lease"]), renewed, "{name}");
        }
        "release" => {
            let released = store
                .release_lease(&lease(&fixture["lease"]))
                .await
                .map_err(|err| err.0)?;
            assert_eq!(expect["released"].as_bool().unwrap(), released, "{name}");
        }
        "get_lease" => {
            let held = store
                .get_lease(fixture["thread_id"].as_str().unwrap())
                .await
                .map_err(|err| err.0)?;
            assert_eq!(optional_lease(&expect["lease"]), held, "{name}");
        }
        "save_under_lease" => {
            let checkpoint = checkpoint_from(&fixture["checkpoint"]);
            *sent = Some(checkpoint.clone());
            let saved = store
                .save_under_lease(
                    checkpoint,
                    &lease(&fixture["lease"]),
                    fixture["expected_sequence"].as_i64(),
                )
                .await
                .map_err(|err| err.0)?;
            let expected = match expect["leased_save"].as_str().unwrap() {
                "saved" => LeasedSave::Saved,
                "not_latest" => LeasedSave::NotLatest,
                "lease_lost" => LeasedSave::LeaseLost,
                other => panic!("{name}: {other}"),
            };
            assert_eq!(expected, saved, "{name}");
        }
        "list" => {
            let page = store
                .list_threads(&thread_query(&fixture["query"]))
                .await
                .map_err(|err| err.0)?;
            let expected = &expect["page"];
            assert_eq!(
                expected["next_cursor"].as_str(),
                page.next_cursor.as_deref(),
                "{name}"
            );
            let rows: Vec<Value> = page.threads.iter().map(summary_json).collect();
            assert_eq!(expected["threads"].as_array().unwrap(), &rows, "{name}");
        }
        "log" => {
            let page = store
                .get_log(
                    fixture["thread_id"].as_str().unwrap(),
                    fixture["after_sequence"].as_i64(),
                    fixture["limit"].as_u64().unwrap() as usize,
                )
                .await
                .map_err(|err| err.0)?;
            let sequences: Vec<i64> = page.entries.iter().map(|entry| entry.sequence).collect();
            let expected: Vec<i64> = expect["sequences"]
                .as_array()
                .unwrap()
                .iter()
                .map(|s| s.as_i64().unwrap())
                .collect();
            assert_eq!(expected, sequences, "{name}");
            let checkpoints = expect["checkpoints"].as_array().unwrap();
            for (entry, checkpoint) in page.entries.iter().zip(checkpoints) {
                assert!(
                    equivalent(&checkpoint_from(checkpoint), &entry.checkpoint),
                    "{name}: {}",
                    entry.sequence
                );
            }
            assert_eq!(
                expect["next_after_sequence"].as_i64(),
                page.next_after_sequence,
                "{name}"
            );
        }
        other => panic!("{name}: unknown operation {other}"),
    }
    Ok(())
}

fn lease(value: &Value) -> ThreadLease {
    ThreadLease {
        thread_id: value["thread_id"].as_str().unwrap().to_string(),
        owner: value["owner"].as_str().unwrap().to_string(),
        token: value["token"].as_i64().unwrap(),
        expires_at: time(&value["expires_at"]),
    }
}

fn optional_lease(value: &Value) -> Option<ThreadLease> {
    (!value.is_null()).then(|| lease(value))
}

fn time(value: &Value) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value.as_str().unwrap())
        .unwrap()
        .with_timezone(&Utc)
}

fn thread_query(value: &Value) -> ThreadQuery {
    let mut query = ThreadQuery::default();
    for state in value["states"].as_array().into_iter().flatten() {
        query
            .states
            .push(ThreadState::parse(state.as_str().unwrap()).unwrap());
    }
    for (key, value) in value["metadata"].as_object().into_iter().flatten() {
        query
            .metadata
            .insert(key.clone(), value.as_str().unwrap().to_string());
    }
    query.updated_after = value.get("updated_after").map(time);
    query.updated_before = value.get("updated_before").map(time);
    query.due_by = value.get("due_by").map(time);
    query.graph_id = value["graph_id"].as_str().map(str::to_string);
    query.graph_version = value["graph_version"].as_str().map(str::to_string);
    if let Some(limit) = value["limit"].as_u64() {
        query.limit = limit as usize;
    }
    query.cursor = value["cursor"].as_str().map(str::to_string);
    query
}
