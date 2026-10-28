//! The model route's shared cases (`design/46-Model-Routes.md`, "The exact
//! rules"): `schema/fixtures/ai/route-cases.json`, which
//! `dotnet/tests/Datalium.Strata.Tests/AI/ModelRouteCaseTests.cs` replays too.
//! Keep the two in step. The clock is tokio's, paused: each call starts when
//! the clock reaches its `at_ms` from the case's start.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::BoxFuture;
use datalium_strata_ai::{
    ChatClient, ChatError, ChatRequest, ChatResponse, ChatStream, ChatUpdate, ChatUpdates, Message,
    ModelCandidate, ModelHealth, ModelHealthOptions, ModelRoute, ModelRouteResult, ModelUsage,
};
use futures_util::StreamExt;
use serde_json::Value;
use tokio::time::Instant;

fn cases() -> Vec<Value> {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/fixtures/ai/route-cases.json");
    let json: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    json["cases"].as_array().unwrap().clone()
}

fn ms(value: &Value) -> Duration {
    Duration::from_millis(value.as_u64().unwrap_or(0))
}

/// A step's failure: a status (transient or fatal by design/44's rule) or a
/// lost connection.
fn failure(spec: &Value) -> ChatError {
    let message = spec["message"].as_str().unwrap_or_default();
    let retry_after = spec["retry_after_ms"].as_u64().map(Duration::from_millis);
    match spec["status"].as_u64() {
        Some(status) => ChatError::status(status as u16, retry_after, message),
        None => {
            let error = ChatError::transient(spec["error"].as_str().unwrap(), message);
            match retry_after {
                Some(after) => error.with_retry_after(after),
                None => error,
            }
        }
    }
}

/// A candidate's client: one script step per time it is asked, answering with
/// the model it was asked for, usage 10 in and 5 out.
struct CaseClient {
    provider: String,
    script: Mutex<VecDeque<Value>>,
}

impl CaseClient {
    fn next(&self) -> Value {
        self.script
            .lock()
            .unwrap()
            .pop_front()
            .expect("the candidate's script has no more steps")
    }
}

fn finish(updates: &mut ChatUpdates, step: &Value, model: Option<String>) {
    updates.push(ChatUpdate::Meta {
        model,
        id: Some("resp".to_string()),
    });
    updates.push(ChatUpdate::Usage(ModelUsage::new(Some(10), Some(5))));
    updates.push(ChatUpdate::Finish(
        step["finish_reason"].as_str().unwrap_or("stop").to_string(),
    ));
}

impl ChatClient for CaseClient {
    fn provider(&self) -> &str {
        &self.provider
    }

    fn complete(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatResponse, ChatError>> {
        let step = self.next();
        Box::pin(async move {
            tokio::time::sleep(ms(&step["delay_ms"])).await;
            if !step["fail"].is_null() {
                return Err(failure(&step["fail"]));
            }
            let mut updates = ChatUpdates::new();
            updates.push(ChatUpdate::Text(
                step["answer"].as_str().expect("an answer step").to_string(),
            ));
            finish(&mut updates, &step, request.model);
            updates.finish()
        })
    }

    fn stream(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatStream, ChatError>> {
        let step = self.next();
        Box::pin(async move {
            let delay = ms(&step["delay_ms"]);
            if !step["fail"].is_null() {
                tokio::time::sleep(delay).await;
                return Err(failure(&step["fail"]));
            }
            let mut items: Vec<Result<ChatUpdate, ChatError>> = step["stream"]
                .as_array()
                .expect("a stream step")
                .iter()
                .map(|text| Ok(ChatUpdate::Text(text.as_str().unwrap().to_string())))
                .collect();
            if step["break"].is_null() {
                items.push(Ok(ChatUpdate::Meta {
                    model: request.model,
                    id: Some("resp".to_string()),
                }));
                items.push(Ok(ChatUpdate::Usage(ModelUsage::new(Some(10), Some(5)))));
                items.push(Ok(ChatUpdate::Finish(
                    step["finish_reason"].as_str().unwrap_or("stop").to_string(),
                )));
            } else {
                items.push(Err(failure(&step["break"])));
            }
            // The first item comes after the delay, the rest at once.
            let stream: ChatStream = Box::pin(futures_util::stream::unfold(
                (VecDeque::from(items), Some(delay)),
                |(mut items, delay)| async move {
                    if let Some(delay) = delay {
                        tokio::time::sleep(delay).await;
                    }
                    let item = items.pop_front()?;
                    Some((item, (items, None)))
                },
            ));
            Ok(stream)
        })
    }
}

fn route(case: &Value, routed: Arc<Mutex<Option<ModelRouteResult>>>) -> (ModelRoute, ModelHealth) {
    let mut health_options = ModelHealthOptions::new();
    if let Some(breaker) = case.get("breaker") {
        health_options = health_options.with_breaker(
            breaker["failures"].as_u64().unwrap() as u32,
            ms(&breaker["open_for_ms"]),
        );
    }
    let health = ModelHealth::new(health_options);
    let mut route = ModelRoute::new("r")
        .with_health(health.clone())
        .on_routed(move |result| *routed.lock().unwrap() = Some(result.clone()));
    if let Some(after) = case.get("hedge_after_ms") {
        route = route.with_hedge_after(ms(after));
    }
    if let Some(accept) = case.get("accept") {
        let containing = accept["reject_containing"].as_str().unwrap().to_string();
        let reason = accept["reason"].as_str().unwrap().to_string();
        route = route.with_accept(move |response| {
            if response.text().contains(&containing) {
                Err(reason.clone())
            } else {
                Ok(())
            }
        });
    }
    for spec in case["candidates"].as_array().unwrap() {
        let name = spec["name"].as_str().unwrap();
        let client = Arc::new(CaseClient {
            provider: spec["provider"]
                .as_str()
                .map_or_else(|| format!("p-{name}"), str::to_string),
            script: Mutex::new(spec["script"].as_array().unwrap().clone().into()),
        });
        let mut candidate = ModelCandidate::new(name, client);
        if let Some(model) = spec["model"].as_str() {
            candidate = candidate.with_model(model);
        }
        if let Some(reason) = spec["ineligible"].as_str() {
            let reason = reason.to_string();
            candidate = candidate.with_eligible(move |_, _| Err(reason.clone()));
        }
        route = route.candidate(candidate);
    }
    (route, health)
}

/// Runs one case's calls; the differences from what it expects, if any.
async fn replay(case: &Value) -> Vec<String> {
    let routed = Arc::new(Mutex::new(None));
    let (route, health) = route(case, routed.clone());
    let start = Instant::now();
    let mut problems = Vec::new();
    for (n, call) in case["calls"].as_array().unwrap().iter().enumerate() {
        let mut problem = |what: String| problems.push(format!("call {n}: {what}"));
        tokio::time::sleep_until(start + ms(&call["at_ms"])).await;
        *routed.lock().unwrap() = None;
        let request = ChatRequest::new(vec![Message::user("?")]).with_model("requested");
        let result = if call["stream"].as_bool().unwrap_or(false) {
            match route.stream(request).await {
                Err(error) => Err(error),
                Ok(mut stream) => {
                    let mut updates = ChatUpdates::new();
                    let mut failed = None;
                    while let Some(update) = stream.next().await {
                        match update {
                            Ok(update) => updates.push(update),
                            Err(error) => {
                                failed = Some(error);
                                break;
                            }
                        }
                    }
                    match failed {
                        Some(error) => Err(error),
                        None => updates.finish(),
                    }
                }
            }
        } else {
            route.complete(request).await
        };
        let ended = start.elapsed();
        let routed = routed
            .lock()
            .unwrap()
            .clone()
            .expect("on_routed was called before the call returned");
        let expect = &call["expect"];

        match (&result, &expect["answer"], &expect["error"]) {
            (Ok(response), answer, _) if !answer.is_null() => {
                let candidate = routed.answered.as_deref().unwrap_or_default();
                if candidate != answer["candidate"] {
                    problem(format!(
                        "answered by {candidate}, not {}",
                        answer["candidate"]
                    ));
                }
                if response.text() != answer["text"] {
                    problem(format!(
                        "answer {:?}, not {}",
                        response.text(),
                        answer["text"]
                    ));
                }
                let model = response.model.as_deref().unwrap_or_default();
                if model != answer["model"] {
                    problem(format!("model {model}, not {}", answer["model"]));
                }
                if routed.model.as_deref() != answer["model"].as_str() {
                    problem(format!("routed model {:?}", routed.model));
                }
            }
            (Err(error), _, expected) if !expected.is_null() => {
                let kind = if error.is_transient() {
                    "transient"
                } else {
                    "fatal"
                };
                if kind != expected["kind"] {
                    problem(format!("{kind}, not {}", expected["kind"]));
                }
                if error.error_type != expected["error_type"] {
                    problem(format!(
                        "error type {}, not {}",
                        error.error_type, expected["error_type"]
                    ));
                }
                let hint = error.retry_after.map(|after| after.as_millis() as u64);
                if hint != expected["retry_after_ms"].as_u64() {
                    problem(format!("hint {hint:?}, not {}", expected["retry_after_ms"]));
                }
                if error.message != expected["message"] {
                    problem(format!(
                        "message {:?}, not {}",
                        error.message, expected["message"]
                    ));
                }
                if routed.answered.is_some() {
                    problem(format!("routed answered {:?}", routed.answered));
                }
            }
            (result, ..) => problem(format!(
                "unexpected {:?}",
                result.as_ref().map(|r| r.text())
            )),
        }

        if let Some(at) = expect["ends_at_ms"].as_u64()
            && ended != Duration::from_millis(at)
        {
            problem(format!("ended at {} ms, not {at}", ended.as_millis()));
        }
        let attempts: Vec<(String, String)> = routed
            .attempts
            .iter()
            .map(|a| (a.candidate.clone(), a.outcome.as_str().to_string()))
            .collect();
        let expected: Vec<(String, String)> = expect["attempts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| {
                (
                    a["candidate"].as_str().unwrap().to_string(),
                    a["outcome"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        if attempts != expected {
            problem(format!("attempts {attempts:?}, not {expected:?}"));
        }
        for attempt in &routed.attempts {
            let asked = !matches!(
                attempt.outcome.as_str(),
                "ineligible" | "cooling_down" | "breaker_open" | "not_tried"
            );
            if attempt.call_id.is_some() != asked {
                problem(format!(
                    "{}'s call id {:?}",
                    attempt.candidate, attempt.call_id
                ));
            }
        }
        if let Some(hedges) = expect["hedges"].as_u64()
            && u64::from(routed.hedges) != hedges
        {
            problem(format!("{} hedges, not {hedges}", routed.hedges));
        }
        if let Some(keys) = expect["health"].as_object() {
            let snapshot = health.snapshot();
            for (key, expected) in keys {
                let Some(entry) = snapshot.iter().find(|e| &e.key == key) else {
                    problem(format!("no health for {key}"));
                    continue;
                };
                if entry.state.as_str() != expected["state"] {
                    problem(format!(
                        "{key} is {}, not {}",
                        entry.state.as_str(),
                        expected["state"]
                    ));
                }
                if u64::from(entry.failures) != expected["failures"].as_u64().unwrap() {
                    problem(format!(
                        "{key} has {} failures, not {}",
                        entry.failures, expected["failures"]
                    ));
                }
                let until = entry
                    .state
                    .until()
                    .map(|until| (until - start).as_millis() as u64);
                if until != expected["until_ms"].as_u64() {
                    problem(format!(
                        "{key} until {until:?}, not {}",
                        expected["until_ms"]
                    ));
                }
            }
        }
    }
    problems
}

#[tokio::test(start_paused = true)]
async fn every_shared_route_case_passes() {
    let cases = cases();
    assert!(!cases.is_empty());
    let mut failures = Vec::new();
    for case in &cases {
        let name = case["name"].as_str().unwrap();
        for problem in replay(case).await {
            failures.push(format!("{name}: {problem}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
