//! Test support for the Cloud adapter (`design/30-Strata-Cloud-Store.md`,
//! "Tests"): a real local HTTP server that answers from a script or as a small
//! Strata Cloud, a sleep that records, and the graph the tests run. Mirrors
//! the .NET tests' `CloudFakes.cs`.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::io::Read;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::graph::{CompileOptions, CompiledGraph, StateGraph};
use datalium_strata_cloud::{RetryPolicy, Sleep, StrataCloudOptions};
use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::net::TcpListener;

pub const API_KEY: &str = "strata_live_TESTKEY0123456789abcdefghijklmnopqrstuvwxyzABCDEF";

pub fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../schema/fixtures/cloud")
}

pub fn read_json(path: &PathBuf) -> Value {
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// A request as the fake saw it: headers lower-cased, the body decompressed.
#[derive(Debug, Clone)]
pub struct Recorded {
    pub method: String,
    pub path: String,
    pub query: BTreeMap<String, String>,
    /// Every `key=value` of the query string, in order, repeats included.
    pub query_pairs: Vec<(String, String)>,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl Recorded {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(&name.to_lowercase()).map(String::as_str)
    }

    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap()
    }
}

/// What the fake does with a request.
pub enum Reply {
    Respond {
        status: u16,
        headers: Vec<(String, String)>,
        body: Option<Value>,
    },
    /// The request reaches the fake, and its response is lost (the
    /// connection closes).
    Lose,
    /// The request never gets an answer: only its timeout ends it.
    Hang,
}

impl Reply {
    pub fn json(status: u16, body: Value) -> Self {
        Reply::Respond {
            status,
            headers: Vec::new(),
            body: Some(body),
        }
    }

    pub fn with_header(self, name: &str, value: &str) -> Self {
        match self {
            Reply::Respond {
                status,
                mut headers,
                body,
            } => {
                headers.push((name.to_string(), value.to_string()));
                Reply::Respond {
                    status,
                    headers,
                    body,
                }
            }
            other => other,
        }
    }
}

pub fn envelope(status: u16, code: &str) -> Value {
    json!({
        "status": "FAILED",
        "code": code,
        "error": "test",
        "timestamp": "2026-10-09T12:00:00.000Z",
        "statusCode": status,
    })
}

type Handler = Arc<dyn Fn(Recorded) -> Pin<Box<dyn Future<Output = Reply> + Send>> + Send + Sync>;

/// A local HTTP server: every request is recorded, then `handler` answers it.
pub struct Server {
    pub url: String,
    pub requests: Arc<Mutex<Vec<Recorded>>>,
}

impl Server {
    pub async fn start(handler: Handler) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let (handler, recorded) = (handler.clone(), recorded.clone());
                tokio::spawn(async move {
                    let service = service_fn(move |request: Request<Incoming>| {
                        let (handler, recorded) = (handler.clone(), recorded.clone());
                        async move {
                            let request = record(request).await;
                            recorded.lock().unwrap().push(request.clone());
                            match handler(request).await {
                                Reply::Respond {
                                    status,
                                    headers,
                                    body,
                                } => {
                                    let mut response = Response::builder()
                                        .status(StatusCode::from_u16(status).unwrap());
                                    for (name, value) in headers {
                                        response = response.header(name, value);
                                    }
                                    let body = body.map(|b| b.to_string()).unwrap_or_default();
                                    if !body.is_empty() {
                                        response =
                                            response.header("content-type", "application/json");
                                    }
                                    Ok(response.body(Full::new(Bytes::from(body))).unwrap())
                                }
                                Reply::Lose => {
                                    Err(std::io::Error::other("the connection was reset"))
                                }
                                Reply::Hang => std::future::pending().await,
                            }
                        }
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        Self { url, requests }
    }

    pub fn requests(&self) -> Vec<Recorded> {
        self.requests.lock().unwrap().clone()
    }

    /// A server answering from `script`, one reply per request.
    pub async fn scripted(script: Vec<Reply>) -> Self {
        let script = Arc::new(Mutex::new(std::collections::VecDeque::from(script)));
        Self::start(Arc::new(move |request: Recorded| {
            let next = script.lock().unwrap().pop_front();
            Box::pin(async move {
                next.unwrap_or_else(|| {
                    panic!("unexpected request {} {}", request.method, request.path)
                })
            })
        }))
        .await
    }

    pub fn options(&self, sleep: &RecordingSleep) -> StrataCloudOptions {
        StrataCloudOptions::new(&self.url, API_KEY).with_sleep(sleep.sleep())
    }
}

async fn record(request: Request<Incoming>) -> Recorded {
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let query_pairs: Vec<(String, String)> = request
        .uri()
        .query()
        .unwrap_or_default()
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (key, value) = part.split_once('=').unwrap_or((part, ""));
            (decode(key), decode(value))
        })
        .collect();
    let query = query_pairs.iter().cloned().collect();
    let headers: HashMap<String, String> = request
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_lowercase(),
                value.to_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    let mut body = request
        .into_body()
        .collect()
        .await
        .map(|collected| collected.to_bytes().to_vec())
        .unwrap_or_default();
    if headers.get("content-encoding").map(String::as_str) == Some("gzip") {
        let mut plain = Vec::new();
        flate2::read::GzDecoder::new(body.as_slice())
            .read_to_end(&mut plain)
            .unwrap();
        body = plain;
    }
    Recorded {
        method,
        path,
        query,
        query_pairs,
        headers,
        body,
    }
}

fn decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap();
            out.push(u8::from_str_radix(hex, 16).unwrap());
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap()
}

/// A [`Sleep`] that records every wait and returns at once.
#[derive(Clone, Default)]
pub struct RecordingSleep {
    waits: Arc<Mutex<Vec<Duration>>>,
}

impl RecordingSleep {
    pub fn sleep(&self) -> Sleep {
        let waits = self.waits.clone();
        Arc::new(move |duration| {
            waits.lock().unwrap().push(duration);
            Box::pin(tokio::task::yield_now())
        })
    }

    pub fn waits(&self) -> Vec<Duration> {
        self.waits.lock().unwrap().clone()
    }
}

/// The default retry policy without jitter, so every wait is exactly the
/// larger of the backoff and `Retry-After`.
pub fn unjittered() -> RetryPolicy {
    let mut policy = RetryPolicy::default();
    policy.backoff.jitter = datalium_strata::graph::Jitter::None;
    policy
}

/// A state Cloud normalises: `amount` is written `1.0` and comes back `1`,
/// `big` is above 2^53, `labels` has keys Cloud reorders.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Rollout {
    pub service: String,
    pub amount: f64,
    pub big: u64,
    pub replicas: i64,
    pub labels: BTreeMap<String, String>,
    pub log: Vec<String>,
}

pub fn initial() -> Rollout {
    Rollout {
        service: "api".to_string(),
        amount: 1.0,
        big: 9_007_199_254_740_993,
        replicas: 1,
        labels: BTreeMap::from([
            ("zone".to_string(), "eu".to_string()),
            ("app".to_string(), "api".to_string()),
        ]),
        log: Vec::new(),
    }
}

/// `Prepare` -> boundary -> `Execute`, counting executions.
pub fn rollout_graph() -> (Arc<CompiledGraph<Rollout>>, Arc<AtomicU32>) {
    rollout_graph_with(CompileOptions::new())
}

/// [`rollout_graph`] compiled with `options` (an identity, say).
pub fn rollout_graph_with(
    options: CompileOptions,
) -> (Arc<CompiledGraph<Rollout>>, Arc<AtomicU32>) {
    let executed = Arc::new(AtomicU32::new(0));
    let counter = executed.clone();
    let graph = StateGraph::<Rollout>::new()
        .add_node("Prepare", |mut s: Rollout| {
            s.log.push("prepared".to_string());
            s
        })
        .add_interrupt_boundary("Prepare")
        .add_node("Execute", move |mut s: Rollout| {
            counter.fetch_add(1, Ordering::SeqCst);
            s.log.push(format!("scaled to {}", s.replicas));
            s
        })
        .add_edge("Prepare", "Execute")
        .compile_with(options)
        .unwrap();
    (Arc::new(graph), executed)
}

struct Decision {
    approval_id: String,
    thread_id: String,
    sequence: usize,
    signal: Value,
}

/// A thread's lease on the emulated Cloud (contract §11).
struct LeaseRow {
    token: i64,
    owner: String,
    expires_at: chrono::DateTime<chrono::Utc>,
    released: bool,
    /// The `Idempotency-Key` that acquired the latest token.
    key: Option<String>,
}

impl LeaseRow {
    fn json(&self, thread_id: &str) -> Value {
        json!({
            "thread_id": thread_id,
            "owner": self.owner,
            "token": self.token,
            "expires_at": self.expires_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        })
    }

    fn held(&self) -> bool {
        !self.released && self.expires_at > chrono::Utc::now()
    }
}

#[derive(Default)]
struct CloudState {
    threads: HashMap<String, Vec<Value>>,
    /// The lease token each checkpoint was saved under, beside `threads`.
    tokens: HashMap<String, Vec<Option<i64>>>,
    leases: HashMap<String, LeaseRow>,
    decisions: Vec<Decision>,
    /// At most this many threads a listing page, the first page of a listing
    /// empty: Cloud's short pages (contract §12).
    list_cap: Option<usize>,
    /// At most this many entries a log page: Cloud's early stop.
    log_cap: Option<usize>,
}

/// A small Strata Cloud on a local server: threads of checkpoints stored the
/// way Cloud stores them (numbers printed as JavaScript prints them,
/// timestamps at whole seconds; key order is a `serde_json` map's), the
/// idempotent `200`, approvals decided by the test, the decision feed, the
/// conditional save while `conditional` is on, leases and the fenced save
/// (contract §11), and the listing and the log (§12), whose cursor is its own.
pub struct EmulatedCloud {
    pub server: Server,
    state: Arc<Mutex<CloudState>>,
    conditional: Arc<std::sync::atomic::AtomicBool>,
    before_load: Arc<Mutex<Option<BeforeLoad>>>,
}

pub type BeforeLoad = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

impl EmulatedCloud {
    pub async fn start() -> Self {
        let state = Arc::new(Mutex::new(CloudState::default()));
        let conditional = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let before_load: Arc<Mutex<Option<BeforeLoad>>> = Arc::new(Mutex::new(None));
        let (s, c, b) = (state.clone(), conditional.clone(), before_load.clone());
        let server = Server::start(Arc::new(move |request: Recorded| {
            let (state, conditional, before_load) = (s.clone(), c.clone(), b.clone());
            Box::pin(async move {
                if request.header("authorization") != Some(&format!("Bearer {API_KEY}")) {
                    return Reply::json(401, envelope(401, "STRATA_CLOUD_API_UNAUTHORIZED_ERROR"));
                }
                let conditional = conditional.load(Ordering::SeqCst);
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/v1/sdk/checkpoints") => save(&state, conditional, &request),
                    (_, "/v1/sdk/checkpoints/latest") => {
                        // The head is read first and the answer held: two
                        // loads released together saw the same head.
                        let loaded = load(&state, conditional, &request.query["thread_id"]);
                        let hook = before_load.lock().unwrap().clone();
                        if let Some(hook) = hook {
                            hook().await;
                        }
                        loaded
                    }
                    (_, "/v1/sdk/decisions") => feed(&state, &request.query),
                    (_, "/v1/sdk/leases") => leases(&state, &request),
                    ("GET", "/v1/sdk/threads") => list(&state, &request),
                    ("GET", "/v1/sdk/checkpoints") => log(&state, &request.query),
                    _ => Reply::json(404, envelope(404, "STRATA_CLOUD_API_NOT_FOUND_ERROR")),
                }
            })
        }))
        .await;
        Self {
            server,
            state,
            conditional,
            before_load,
        }
    }

    pub fn set_conditional(&self, on: bool) {
        self.conditional.store(on, Ordering::SeqCst);
    }

    pub fn set_before_load(&self, hook: Option<BeforeLoad>) {
        *self.before_load.lock().unwrap() = hook;
    }

    /// Listing pages hold at most `cap` threads, and a listing's first page
    /// none, each with a cursor while more follow.
    pub fn set_list_cap(&self, cap: Option<usize>) {
        self.state.lock().unwrap().list_cap = cap;
    }

    /// Log pages stop after `cap` entries, with more to come.
    pub fn set_log_cap(&self, cap: Option<usize>) {
        self.state.lock().unwrap().log_cap = cap;
    }

    /// Another owner takes the thread over, as if its lease had run out and
    /// been acquired: the next token, held for a minute.
    pub fn take_over(&self, thread_id: &str, owner: &str) -> i64 {
        let mut state = self.state.lock().unwrap();
        let token = state.leases.get(thread_id).map_or(0, |row| row.token) + 1;
        state.leases.insert(
            thread_id.to_string(),
            LeaseRow {
                token,
                owner: owner.to_string(),
                expires_at: chrono::Utc::now() + chrono::Duration::minutes(1),
                released: false,
                key: None,
            },
        );
        token
    }

    /// The thread's lease as Cloud keeps it (released or not), or `None`.
    pub fn lease(&self, thread_id: &str) -> Option<(i64, String, bool)> {
        self.state
            .lock()
            .unwrap()
            .leases
            .get(thread_id)
            .map(|row| (row.token, row.owner.clone(), row.released))
    }

    /// Stores `checkpoint` as the thread's latest, as a save would.
    pub fn seed(&self, checkpoint: &datalium_strata::schema::Checkpoint) {
        let stored = normalize(&serde_json::to_value(checkpoint).unwrap(), None);
        let mut state = self.state.lock().unwrap();
        let thread_id = checkpoint.thread_id.clone();
        state
            .threads
            .entry(thread_id.clone())
            .or_default()
            .push(stored);
        state.tokens.entry(thread_id).or_default().push(None);
    }

    pub fn saves(&self, thread_id: &str) -> usize {
        self.state
            .lock()
            .unwrap()
            .threads
            .get(thread_id)
            .map_or(0, Vec::len)
    }

    pub fn latest(&self, thread_id: &str) -> Value {
        self.state.lock().unwrap().threads[thread_id]
            .last()
            .unwrap()
            .clone()
    }

    /// A reviewer decides the thread's current approval (its latest,
    /// interrupted checkpoint).
    pub fn decide(
        &self,
        thread_id: &str,
        approved: bool,
        reviewer: &str,
        overrides: Option<Value>,
    ) -> String {
        let mut state = self.state.lock().unwrap();
        let sequence = state.threads[thread_id].len() - 1;
        let approval_id = format!("A{:04}", state.decisions.len() + 1);
        let mut signal = json!({
            "is_approved": approved,
            "reviewer_token": reviewer,
            "field_overrides": {},
        });
        if let Some(overrides) = overrides {
            signal["overrides"] = overrides;
        }
        state.decisions.push(Decision {
            approval_id: approval_id.clone(),
            thread_id: thread_id.to_string(),
            sequence,
            signal,
        });
        approval_id
    }
}

fn receipt(thread_id: &str, sequence: usize) -> Value {
    json!({ "thread_id": thread_id, "sequence": sequence, "received_at": "2026-10-09T12:00:00Z" })
}

fn save(state: &Mutex<CloudState>, conditional: bool, request: &Recorded) -> Reply {
    let stored = normalize(&request.json(), None);
    let thread_id = stored["thread_id"].as_str().unwrap().to_string();
    let mut state = state.lock().unwrap();
    let token = request
        .header("strata-lease-token")
        .map(|token| token.parse::<i64>().unwrap());
    // The lease first: a token that isn't the thread's latest stores nothing.
    if let Some(token) = token
        && state.leases.get(&thread_id).map(|row| row.token) != Some(token)
    {
        return Reply::json(423, envelope(423, "STRATA_CLOUD_API_LEASE_LOST"));
    }
    let state = &mut *state;
    let log = state.threads.entry(thread_id.clone()).or_default();
    let tokens = state.tokens.entry(thread_id.clone()).or_default();
    // The precondition next: two resume stamps made in the same second are
    // identical, and the second must be refused, not taken for the first
    // one's retry (design/30, "The conditional save"). Under a lease, our own
    // checkpoint at the head, saved under our token, is the retry's 200.
    if conditional
        && let Some(expected) = request.header("strata-if-sequence")
        && expected.parse::<i64>().unwrap() != log.len() as i64 - 1
    {
        if token.is_some() && log.last() == Some(&stored) && tokens.last() == Some(&token) {
            return Reply::json(200, receipt(&thread_id, log.len() - 1));
        }
        return Reply::json(412, envelope(412, "STRATA_CLOUD_API_PRECONDITION_FAILED"));
    }
    if log.last() == Some(&stored) {
        return Reply::json(200, receipt(&thread_id, log.len() - 1));
    }
    log.push(stored);
    tokens.push(token);
    Reply::json(201, receipt(&thread_id, log.len() - 1))
}

fn leases(state: &Mutex<CloudState>, request: &Recorded) -> Reply {
    let mut state = state.lock().unwrap();
    let now = chrono::Utc::now();
    match request.method.as_str() {
        "POST" => {
            let body = request.json();
            let thread_id = body["thread_id"].as_str().unwrap();
            let key = request.header("idempotency-key").map(str::to_string);
            if let Some(row) = state.leases.get(thread_id) {
                let mine = key.is_some() && row.key == key && !row.released;
                if mine || row.held() {
                    return Reply::json(
                        200,
                        json!({ "acquired": mine, "lease": row.json(thread_id) }),
                    );
                }
            }
            let token = state.leases.get(thread_id).map_or(0, |row| row.token) + 1;
            let row = LeaseRow {
                token,
                owner: body["owner"].as_str().unwrap().to_string(),
                expires_at: now + chrono::Duration::milliseconds(body["ttl_ms"].as_i64().unwrap()),
                released: false,
                key,
            };
            let lease = row.json(thread_id);
            state.leases.insert(thread_id.to_string(), row);
            Reply::json(200, json!({ "acquired": true, "lease": lease }))
        }
        "PUT" => {
            let body = request.json();
            let thread_id = body["thread_id"].as_str().unwrap();
            match state.leases.get_mut(thread_id) {
                Some(row) if row.token == body["token"].as_i64().unwrap() => {
                    row.owner = body["owner"].as_str().unwrap().to_string();
                    row.expires_at =
                        now + chrono::Duration::milliseconds(body["ttl_ms"].as_i64().unwrap());
                    row.released = false;
                    Reply::json(200, json!({ "lease": row.json(thread_id) }))
                }
                _ => Reply::json(200, json!({ "lease": null })),
            }
        }
        "DELETE" => {
            let thread_id = &request.query["thread_id"];
            let token: i64 = request.query["token"].parse().unwrap();
            match state.leases.get_mut(thread_id) {
                Some(row) if row.token == token => {
                    row.released = true;
                    Reply::json(200, json!({ "released": true }))
                }
                _ => Reply::json(200, json!({ "released": false })),
            }
        }
        _ => {
            let thread_id = &request.query["thread_id"];
            let lease = state
                .leases
                .get(thread_id)
                .filter(|row| row.held())
                .map(|row| row.json(thread_id));
            Reply::json(200, json!({ "lease": lease }))
        }
    }
}

/// `GET /v1/sdk/threads`, filtered and ordered by the engine's own rules,
/// with a cursor of the emulator's own, which the adapter must pass back as
/// it came.
fn list(state: &Mutex<CloudState>, request: &Recorded) -> Reply {
    use datalium_strata::store::{ThreadCursor, ThreadQuery, ThreadState, ThreadSummary};
    const PREFIX: &str = "emulated:";
    let time = |text: &str| {
        chrono::DateTime::parse_from_rfc3339(text)
            .unwrap()
            .with_timezone(&chrono::Utc)
    };
    let mut query = ThreadQuery::default();
    let mut cursor = None;
    for (key, value) in &request.query_pairs {
        match key.as_str() {
            "state" => query.states.push(ThreadState::parse(value).unwrap()),
            "due_by" => query.due_by = Some(time(value)),
            "updated_after" => query.updated_after = Some(time(value)),
            "updated_before" => query.updated_before = Some(time(value)),
            "graph_id" => query.graph_id = Some(value.clone()),
            "graph_version" => query.graph_version = Some(value.clone()),
            "limit" => query.limit = value.parse().unwrap(),
            "cursor" => match value.strip_prefix(PREFIX) {
                Some(position) => cursor = Some(ThreadCursor::decode(position).unwrap()),
                None => {
                    return Reply::json(400, envelope(400, "STRATA_CLOUD_API_VALIDATION_ERROR"));
                }
            },
            key => match key.strip_prefix("meta.") {
                Some(meta) => {
                    query.metadata.insert(meta.to_string(), value.clone());
                }
                None => {
                    return Reply::json(422, envelope(422, "STRATA_CLOUD_API_VALIDATION_ERROR"));
                }
            },
        }
    }
    let state = state.lock().unwrap();
    let mut heads: Vec<(i64, datalium_strata::schema::Checkpoint)> = state
        .threads
        .values()
        .filter(|log| !log.is_empty())
        .map(|log| {
            (
                log.len() as i64 - 1,
                serde_json::from_value(log.last().unwrap().clone()).unwrap(),
            )
        })
        .filter(|(_, checkpoint)| query.matches(checkpoint))
        .collect();
    let key = |checkpoint: &datalium_strata::schema::Checkpoint| {
        (
            datalium_strata::store::updated_at_text(checkpoint).to_string(),
            checkpoint.thread_id.clone(),
        )
    };
    heads.sort_by_key(|(_, checkpoint)| std::cmp::Reverse(key(checkpoint)));
    let after: Vec<_> = heads
        .into_iter()
        .filter(|(_, checkpoint)| {
            let (updated_at, thread_id) = key(checkpoint);
            cursor
                .as_ref()
                .is_none_or(|cursor| cursor.admits(&updated_at, &thread_id))
        })
        .collect();
    let take = match state.list_cap {
        // A listing's first page is short and empty.
        Some(_) if cursor.is_none() => 0,
        Some(cap) => cap.min(query.limit),
        None => query.limit,
    };
    let page: Vec<_> = after.iter().take(take).collect();
    let next_cursor = if page.len() < after.len() {
        let last = match page.last() {
            Some((_, checkpoint)) => {
                let (updated_at, thread_id) = key(checkpoint);
                ThreadCursor::new(updated_at, thread_id)
            }
            // An empty page continues from where it started: before the
            // first thread.
            None => ThreadCursor::new("9999", ""),
        };
        Value::String(format!("{PREFIX}{}", last.encode()))
    } else {
        Value::Null
    };
    let threads: Vec<Value> = page
        .iter()
        .map(|(sequence, checkpoint)| {
            summary_json(&ThreadSummary::of(*sequence, checkpoint).unwrap())
        })
        .collect();
    Reply::json(
        200,
        json!({ "threads": threads, "next_cursor": next_cursor }),
    )
}

fn log(state: &Mutex<CloudState>, query: &BTreeMap<String, String>) -> Reply {
    let state = state.lock().unwrap();
    let after = query
        .get("after_sequence")
        .map_or(-1, |after| after.parse::<i64>().unwrap());
    let limit: usize = query
        .get("limit")
        .map_or(100, |limit| limit.parse().unwrap());
    let take = state.log_cap.map_or(limit, |cap| cap.min(limit));
    let log = state
        .threads
        .get(&query["thread_id"])
        .cloned()
        .unwrap_or_default();
    let rest: Vec<(usize, Value)> = log
        .into_iter()
        .enumerate()
        .filter(|(sequence, _)| *sequence as i64 > after)
        .collect();
    let entries: Vec<Value> = rest
        .iter()
        .take(take)
        .map(|(sequence, checkpoint)| json!({ "sequence": sequence, "checkpoint": checkpoint }))
        .collect();
    let next = if rest.len() > entries.len() {
        json!(rest[entries.len() - 1].0)
    } else {
        Value::Null
    };
    Reply::json(
        200,
        json!({ "entries": entries, "next_after_sequence": next }),
    )
}

/// A summary in the wire's form (contract §12, as `strata thread list --json`
/// prints it).
pub fn summary_json(summary: &datalium_strata::store::ThreadSummary) -> Value {
    json!({
        "thread_id": summary.thread_id,
        "sequence": summary.sequence,
        "state": summary.state.as_str(),
        "status": summary.status,
        "suspended_node": summary.suspended_node,
        "next_node": summary.next_node,
        "waiting_at": summary.waiting_at,
        "metadata": summary.metadata,
        "updated_at": datalium_strata::store::timestamp_text(summary.updated_at),
        "schema_version": summary.schema_version,
        "wait": summary.wait,
        "graph": summary.graph,
    })
}

fn load(state: &Mutex<CloudState>, conditional: bool, thread_id: &str) -> Reply {
    let state = state.lock().unwrap();
    match state.threads.get(thread_id).filter(|log| !log.is_empty()) {
        None => Reply::json(404, envelope(404, "STRATA_CLOUD_API_THREAD_NOT_FOUND")),
        Some(log) => {
            let reply = Reply::json(200, log.last().unwrap().clone());
            if conditional {
                reply.with_header("Strata-Sequence", &(log.len() - 1).to_string())
            } else {
                reply
            }
        }
    }
}

fn feed(state: &Mutex<CloudState>, query: &BTreeMap<String, String>) -> Reply {
    let state = state.lock().unwrap();
    let waiting: Vec<&Decision> = state
        .decisions
        .iter()
        .filter(|d| {
            query
                .get("thread_id")
                .is_none_or(|only| *only == d.thread_id)
        })
        .filter(|d| {
            let log = &state.threads[&d.thread_id];
            let latest = log.last().unwrap();
            log.len() - 1 == d.sequence
                && latest["status"] == "interrupted"
                && latest["resumed_at"].is_null()
        })
        .collect();
    let start = query
        .get("cursor")
        .and_then(|cursor| waiting.iter().position(|d| d.approval_id == *cursor))
        .map_or(0, |i| i + 1);
    let limit = query.get("limit").map_or(25, |l| l.parse().unwrap());
    let page: Vec<&&Decision> = waiting.iter().skip(start).take(limit).collect();
    let more = start + page.len() < waiting.len();
    let items: Vec<Value> = page
        .iter()
        .map(|d| {
            json!({
                "approval_id": d.approval_id,
                "thread_id": d.thread_id,
                "sequence": d.sequence,
                "decided_at": "2026-10-09T12:00:00Z",
                "decided_by": format!("user-{}", d.signal["reviewer_token"].as_str().unwrap()),
                "decided_by_email": format!("{}@example.com", d.signal["reviewer_token"].as_str().unwrap()),
                "notes": null,
                "signal": d.signal,
            })
        })
        .collect();
    let next_cursor = if more {
        Value::String(page.last().unwrap().approval_id.clone())
    } else {
        Value::Null
    };
    Reply::json(200, json!({ "items": items, "next_cursor": next_cursor }))
}

/// What Cloud's JSONB and timestamp normalisation make of a checkpoint
/// (contract §3).
pub fn normalize(value: &Value, key: Option<&str>) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(name, value)| (name.clone(), normalize(value, Some(name))))
                .collect(),
        ),
        Value::Array(items) => {
            Value::Array(items.iter().map(|item| normalize(item, None)).collect())
        }
        Value::Number(number) => match number.as_f64() {
            // A double that holds an integer exactly is printed as one.
            Some(f) if number.is_f64() && f.fract() == 0.0 && f.abs() < 9_007_199_254_740_992.0 => {
                json!(f as i64)
            }
            _ => value.clone(),
        },
        Value::String(text) if matches!(key, Some("created_at" | "resumed_at")) => {
            match chrono::DateTime::parse_from_rfc3339(text) {
                Ok(time) => Value::String(
                    time.with_timezone(&chrono::Utc)
                        .format("%Y-%m-%dT%H:%M:%SZ")
                        .to_string(),
                ),
                Err(_) => value.clone(),
            }
        }
        _ => value.clone(),
    }
}
