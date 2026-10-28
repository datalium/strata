//! The memory helper's shared cases (`design/45-Long-Term-Memory.md`, "The
//! helper"): `schema/fixtures/ai/memory-cases.json`, which
//! `dotnet/tests/Datalium.Strata.Tests/AI/MemoryCaseTests.cs` replays too.
//! Keep the two in step.

mod common;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::ScriptedEmbedder;
use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::StateGraph;
use datalium_strata::memory::{InMemoryMemoryStore, MemoryKey, MemoryRecord, MemoryStore};
use datalium_strata::observability::{RunEvent, RunEventChannel, RunEventKind};
use datalium_strata_ai::{ChatError, Memory, MemoryEntry, MemoryError, RecallOptions};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde_json::{Map, Value, json};

const PATIENCE: Duration = Duration::from_secs(10);

fn fixture() -> Value {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/fixtures/ai/memory-cases.json");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn text(value: &Value) -> String {
    value.as_str().unwrap().to_string()
}

fn pairs(value: &Value) -> Vec<(String, String)> {
    value
        .as_object()
        .into_iter()
        .flatten()
        .map(|(key, value)| (key.clone(), text(value)))
        .collect()
}

/// The fixture's embedder.
fn embedder(spec: &Value) -> ScriptedEmbedder {
    let mut embedder = ScriptedEmbedder::new(
        spec["provider"].as_str().unwrap(),
        spec["model"].as_str(),
        &[],
    );
    for (input, vector) in spec["vectors"].as_object().unwrap() {
        embedder.vectors.insert(
            input.clone(),
            vector
                .as_array()
                .unwrap()
                .iter()
                .map(|x| x.as_f64().unwrap() as f32)
                .collect(),
        );
    }
    for (input, fails) in spec["fails"].as_object().unwrap() {
        embedder.fails.insert(
            input.clone(),
            ChatError::status(
                fails["status"].as_u64().unwrap() as u16,
                fails["retry_after_ms"].as_u64().map(Duration::from_millis),
                text(&fails["message"]),
            ),
        );
    }
    embedder.unanswered = spec["unanswered"]
        .as_array()
        .unwrap()
        .iter()
        .map(text)
        .collect::<HashSet<_>>();
    embedder
}

fn entry(value: &Value) -> MemoryEntry {
    let mut entry = MemoryEntry::new(
        text(&value["namespace"]),
        text(&value["id"]),
        text(&value["text"]),
    );
    if let Some(tenant) = value.get("tenant") {
        entry = entry.with_tenant(text(tenant));
    }
    if let Some(Value::Object(content)) = value.get("content") {
        entry = entry.with_content(content.clone());
    }
    for (key, value) in pairs(&value["metadata"]) {
        entry = entry.with_metadata(key, value);
    }
    entry
}

fn recall_options(value: &Value) -> RecallOptions {
    let mut options = RecallOptions::new();
    if let Some(tenant) = value.get("tenant") {
        options = options.tenant(text(tenant));
    }
    if let Some(limit) = value.get("limit") {
        options = options.limit(limit.as_u64().unwrap() as usize);
    }
    if let Some(min) = value.get("min_similarity") {
        options = options.min_similarity(min.as_f64().unwrap());
    }
    for id in value["exclude"].as_array().into_iter().flatten() {
        options = options.exclude(text(id));
    }
    for (key, value) in pairs(&value["metadata"]) {
        options = options.with_metadata(key, value);
    }
    options
}

/// A record as the fixture writes it: its JSON form without schema_version and
/// the timestamps.
fn record_json(record: &MemoryRecord) -> Value {
    json!({
        "namespace": record.namespace,
        "tenant": record.tenant,
        "id": record.id,
        "embedding": { "model": record.embedding.model, "vector": record.embedding.vector },
        "content": record.content.clone().map(Value::Object),
        "text": record.text,
        "metadata": record
            .metadata
            .iter()
            .map(|(k, v)| (k.clone(), Value::from(v.clone())))
            .collect::<Map<_, _>>(),
    })
}

/// The expected record with its vector as f32s, the precision a store keeps
/// (JSON `0.9` is the f32 nearest).
fn as_stored(expected: &Value) -> Value {
    let mut expected = expected.clone();
    if let Some(vector) = expected.pointer_mut("/embedding/vector") {
        *vector = Value::Array(
            vector
                .as_array()
                .unwrap()
                .iter()
                .map(|x| json!(x.as_f64().unwrap() as f32))
                .collect(),
        );
    }
    expected
}

/// Runs a case's steps against `memory` and `store`, returning the first
/// mismatch.
async fn run_steps(
    memory: &Memory,
    store: &dyn MemoryStore,
    steps: &[Value],
    ctx: Option<&NodeContext>,
) -> Result<(), String> {
    for (index, step) in steps.iter().enumerate() {
        let at = format!("step {index}");
        let outcome: Result<Option<Value>, MemoryError> = if let Some(value) = step.get("remember")
        {
            memory.remember(entry(value), ctx).await.map(|()| None)
        } else if let Some(value) = step.get("remember_many") {
            let entries = value.as_array().unwrap().iter().map(entry).collect();
            memory.remember_many(entries, ctx).await.map(|()| None)
        } else if let Some(value) = step.get("recall") {
            memory
                .recall(
                    value["namespace"].as_str().unwrap(),
                    value["text"].as_str().unwrap(),
                    recall_options(&value["options"]),
                    ctx,
                )
                .await
                .map(|matches| {
                    Some(Value::Array(
                        matches
                            .iter()
                            .map(|m| json!({ "id": m.record.id, "similarity": m.similarity }))
                            .collect(),
                    ))
                })
        } else if let Some(value) = step.get("get") {
            let key = MemoryKey::new(
                text(&value["namespace"]),
                text(&value["tenant"]),
                text(&value["id"]),
            );
            let found = store.get(&key).await.map_err(|e| format!("{at}: {e}"))?;
            Ok(Some(found.as_ref().map_or(Value::Null, record_json)))
        } else {
            return Err(format!("{at}: an unknown step {step}"));
        };

        match (outcome, step.get("failure"), step.get("error")) {
            (Err(MemoryError::Embedding(error)), Some(failure), None) => {
                let expected = (
                    failure["transient"].as_bool().unwrap(),
                    failure["retry_after_ms"].as_u64(),
                    text(&failure["message"]),
                );
                let actual = (
                    error.is_transient(),
                    error.retry_after().map(|d| d.as_millis() as u64),
                    error.message().to_string(),
                );
                if actual != expected {
                    return Err(format!("{at}: failed with {actual:?}, not {expected:?}"));
                }
            }
            (Err(MemoryError::Store(error)), None, Some(message)) => {
                if error.0 != text(message) {
                    return Err(format!("{at}: refused with '{error}', not {message}"));
                }
            }
            (Ok(found), None, None) => {
                let Some(expected) = step.get("expect") else {
                    continue;
                };
                let found = found.unwrap_or(Value::Null);
                if step.get("get").is_some() {
                    if found != as_stored(expected) {
                        return Err(format!("{at}: got {found}, not {expected}"));
                    }
                } else if !same_matches(&found, expected) {
                    return Err(format!("{at}: recalled {found}, not {expected}"));
                }
            }
            (outcome, failure, error) => {
                return Err(format!(
                    "{at}: {outcome:?}, expected failure {failure:?} / error {error:?}"
                ));
            }
        }
    }
    Ok(())
}

fn same_matches(actual: &Value, expected: &Value) -> bool {
    let (actual, expected) = (actual.as_array().unwrap(), expected.as_array().unwrap());
    actual.len() == expected.len()
        && actual.iter().zip(expected).all(|(a, e)| {
            a["id"] == e["id"]
                && (a["similarity"].as_f64().unwrap() - e["similarity"].as_f64().unwrap()).abs()
                    < 1e-5
        })
}

fn describe(event: &RunEvent) -> Option<String> {
    Some(match &event.kind {
        RunEventKind::ModelCallStarted {
            operation,
            model,
            provider,
            ..
        } => format!(
            "ModelCallStarted {operation} {model} {}",
            provider.as_deref().unwrap_or_default()
        ),
        RunEventKind::ModelCallCompleted {
            operation,
            result,
            usage,
            ..
        } => format!(
            "ModelCallCompleted {operation} {result:?} {}",
            usage
                .input_tokens
                .map_or_else(|| "-".to_string(), |n| n.to_string())
        ),
        _ => return None,
    })
}

/// Runs one case; the events are `None` for a case without a context.
async fn run_case(
    spec: &Value,
    case: &Value,
) -> (Result<(), String>, Vec<Vec<String>>, Option<Vec<String>>) {
    let store = Arc::new(InMemoryMemoryStore::new(
        spec["dimensions"].as_u64().unwrap() as usize,
    ));
    let embedder = Arc::new(embedder(&spec["embedder"]));
    let mut memory = Memory::new(store.clone(), embedder.clone());
    if let Some(model) = case.pointer("/options/model") {
        memory = memory.with_model(text(model));
    }
    if let Some(key) = case.pointer("/options/tenant_key") {
        memory = memory.with_tenant_key(text(key));
    }
    let steps = case["steps"].as_array().unwrap().clone();

    if case["context"].is_null() {
        let outcome = run_steps(&memory, &*store, &steps, None).await;
        return (outcome, embedder.calls(), None);
    }

    let outcome = Arc::new(Mutex::new(None));
    let node = {
        let (memory, store, outcome) = (memory.clone(), store.clone(), outcome.clone());
        move |state: bool, ctx: NodeContext| {
            let (memory, store, outcome, steps) = (
                memory.clone(),
                store.clone(),
                outcome.clone(),
                steps.clone(),
            );
            async move {
                let result = run_steps(&memory, &*store, &steps, Some(&ctx)).await;
                *outcome.lock().unwrap() = Some(result);
                state
            }
        }
    };
    let graph = StateGraph::<bool>::new()
        .add_async_node("Memory", node)
        .compile()
        .unwrap();
    let (observer, mut events) = RunEventChannel::new();
    let mut config = RunConfig::new().with_observer(observer);
    for (key, value) in pairs(&case["context"]["metadata"]) {
        config = config.with_metadata(key, value);
    }
    let result = graph
        .run_with(
            "memory-case",
            true,
            Arc::new(InMemoryCheckpointStore::new()),
            config,
        )
        .await;
    assert!(
        matches!(result, GraphExecutionResult::Completed(_)),
        "{}: the run didn't complete",
        case["name"]
    );
    let mut described = Vec::new();
    while let Some(event) = tokio::time::timeout(PATIENCE, events.recv()).await.unwrap() {
        described.extend(describe(&event));
    }
    let outcome = outcome.lock().unwrap().take().expect("the node ran");
    (outcome, embedder.calls(), Some(described))
}

#[tokio::test]
async fn every_shared_memory_case_passes() {
    let spec = fixture();
    let cases = spec["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 11);
    let mut names = HashMap::new();
    for case in cases {
        let name = case["name"].as_str().unwrap();
        assert!(names.insert(name, ()).is_none(), "two cases named '{name}'");
        let (outcome, calls, events) = run_case(&spec, case).await;
        if let Err(mismatch) = outcome {
            panic!("{name}: {mismatch}");
        }
        if let Some(expected) = case.get("calls") {
            let expected: Vec<Vec<String>> = serde_json::from_value(expected.clone()).unwrap();
            assert_eq!(calls, expected, "{name}: the embedding calls");
        }
        if let Some(expected) = case.get("events") {
            let expected: Vec<String> = serde_json::from_value(expected.clone()).unwrap();
            assert_eq!(
                events.as_ref(),
                Some(&expected),
                "{name}: the model-call events"
            );
        }
    }
}
