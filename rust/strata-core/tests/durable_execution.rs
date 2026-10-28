//! A checkpoint per node, and recovering a thread whose process died
//! (`design/33-Durable-Execution.md`, SG-10), through the public API only.
//! The .NET twin is
//! `dotnet/tests/Datalium.Strata.Tests/DurableExecutionTests.cs` — keep the
//! two in step.
//!
//! Every test runs on a paused tokio clock, which the store's leases follow,
//! so a ttl passes without waiting.

mod lease_store;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, SecondsFormat, Timelike, Utc};
use datalium_strata::checkpoint::{
    CancelReason, CheckpointLogPage, CheckpointQuery, CheckpointStore, CheckpointStoreError,
    EventSignal, GraphExecutionResult, HumanSignal, LatestCheckpoint, LeaseAttempt, LeaseOptions,
    ThreadLease, ThreadPage, ThreadQuery, ThreadState,
};
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{
    CheckpointPolicy, CompileOptions, CompiledGraph, NodeError, StateGraph,
};
use datalium_strata::observability::{RunEvent, RunEventChannel, RunEventKind, RunMode};
use datalium_strata::parallel::ParallelBranches;
use datalium_strata::recovery::{RECOVERIES_KEY, RecoverOptions, SkippedThread, recoveries_of};
use datalium_strata::schema::{Checkpoint, CheckpointStatus};
use datalium_strata::store::LeasedSave;
use datalium_strata::streaming::StreamChunkType;
use datalium_strata::{BoxFuture, CancellationToken, StrataError};
use lease_store::{LeaseStore, TTL};
use serde::{Deserialize, Serialize};

const PATIENCE: Duration = Duration::from_secs(600);

/// What a store call returns in a process that has died.
const DIED: &str = "the process died";

/// A state whose `trail` every node adds its name to.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Job {
    trail: String,
    #[serde(default)]
    approved: bool,
    #[serde(default)]
    limit: i64,
}

fn job() -> Job {
    Job {
        trail: String::new(),
        approved: false,
        limit: 0,
    }
}

/// A store as one process sees it. Once the process has died nothing it does
/// reaches the store: not the checkpoint after the node in flight, not a
/// `failed` one, not the release of its lease. With `rewound`, a process that
/// loaded that checkpoint before somebody else moved the thread on.
struct Process {
    inner: Arc<LeaseStore>,
    dead: AtomicBool,
    rewound: Option<LatestCheckpoint>,
}

impl Process {
    fn new(inner: &Arc<LeaseStore>) -> Arc<Self> {
        Arc::new(Self {
            inner: inner.clone(),
            dead: AtomicBool::new(false),
            rewound: None,
        })
    }

    fn rewound(inner: &Arc<LeaseStore>, stale: LatestCheckpoint) -> Arc<Self> {
        Arc::new(Self {
            inner: inner.clone(),
            dead: AtomicBool::new(false),
            rewound: Some(stale),
        })
    }

    fn die(&self) {
        self.dead.store(true, Ordering::SeqCst);
    }

    fn alive<'a, T: Send + 'a>(
        &self,
        call: impl FnOnce() -> BoxFuture<'a, Result<T, CheckpointStoreError>>,
    ) -> BoxFuture<'a, Result<T, CheckpointStoreError>> {
        if self.dead.load(Ordering::SeqCst) {
            Box::pin(std::future::ready(Err(CheckpointStoreError(
                DIED.to_string(),
            ))))
        } else {
            call()
        }
    }
}

impl CheckpointStore for Process {
    fn save(&self, checkpoint: Checkpoint) -> BoxFuture<'_, Result<(), CheckpointStoreError>> {
        self.alive(|| self.inner.save(checkpoint))
    }

    fn load<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<Checkpoint>, CheckpointStoreError>> {
        self.alive(|| self.inner.load(thread_id))
    }

    fn load_latest<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<LatestCheckpoint>, CheckpointStoreError>> {
        match &self.rewound {
            Some(stale) => Box::pin(std::future::ready(Ok(Some(stale.clone())))),
            None => self.alive(|| self.inner.load_latest(thread_id)),
        }
    }

    fn save_if_latest(
        &self,
        checkpoint: Checkpoint,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'_, Result<bool, CheckpointStoreError>> {
        self.alive(|| self.inner.save_if_latest(checkpoint, expected_sequence))
    }

    fn try_acquire_lease<'a>(
        &'a self,
        thread_id: &'a str,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<LeaseAttempt, CheckpointStoreError>> {
        self.alive(|| self.inner.try_acquire_lease(thread_id, owner, ttl))
    }

    fn renew_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        self.alive(|| self.inner.renew_lease(lease, ttl))
    }

    fn release_lease<'a>(
        &'a self,
        lease: &'a ThreadLease,
    ) -> BoxFuture<'a, Result<bool, CheckpointStoreError>> {
        self.alive(|| self.inner.release_lease(lease))
    }

    fn get_lease<'a>(
        &'a self,
        thread_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<ThreadLease>, CheckpointStoreError>> {
        self.alive(|| self.inner.get_lease(thread_id))
    }

    fn save_under_lease<'a>(
        &'a self,
        checkpoint: Checkpoint,
        lease: &'a ThreadLease,
        expected_sequence: Option<i64>,
    ) -> BoxFuture<'a, Result<LeasedSave, CheckpointStoreError>> {
        self.alive(|| {
            self.inner
                .save_under_lease(checkpoint, lease, expected_sequence)
        })
    }
}

impl CheckpointQuery for Process {
    fn list_threads<'a>(
        &'a self,
        query: &'a ThreadQuery,
    ) -> BoxFuture<'a, Result<ThreadPage, CheckpointStoreError>> {
        self.alive(|| self.inner.list_threads(query))
    }

    fn get_log<'a>(
        &'a self,
        thread_id: &'a str,
        after_sequence: Option<i64>,
        limit: usize,
    ) -> BoxFuture<'a, Result<CheckpointLogPage, CheckpointStoreError>> {
        self.alive(|| self.inner.get_log(thread_id, after_sequence, limit))
    }
}

/// One run of a handler: where, under which key, and whether it may have run
/// before.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Visit {
    path: String,
    key: String,
    recovery: bool,
    reviewer: Option<String>,
}

fn visit(path: &str, key: &str, recovery: bool) -> Visit {
    Visit {
        path: path.to_string(),
        key: key.to_string(),
        recovery,
        reviewer: None,
    }
}

type Hook = Box<dyn Fn(&NodeContext) + Send + Sync>;

/// What every handler of a test graph ran as, in order, and what to do when
/// one is reached.
#[derive(Default)]
struct Journal {
    visits: Mutex<Vec<Visit>>,
    hooks: Mutex<HashMap<String, Hook>>,
}

impl Journal {
    fn new() -> Arc<Self> {
        Arc::default()
    }

    fn visits(&self) -> Vec<Visit> {
        self.visits.lock().unwrap().clone()
    }

    fn keys(&self) -> Vec<String> {
        self.visits().into_iter().map(|v| v.key).collect()
    }

    fn count(&self, path: &str) -> usize {
        self.visits().iter().filter(|v| v.path == path).count()
    }

    /// Runs `hook` the next time thread `thread_id` reaches `path`.
    fn on(&self, thread_id: &str, path: &str, hook: impl Fn(&NodeContext) + Send + Sync + 'static) {
        self.hooks
            .lock()
            .unwrap()
            .insert(format!("{thread_id}:{path}"), Box::new(hook));
    }

    /// The process dies the next time thread `thread_id` reaches `path`.
    fn die_at(&self, thread_id: &str, path: &str, process: &Arc<Process>) {
        let process = process.clone();
        self.on(thread_id, path, move |_| process.die());
    }

    fn visit(&self, mut job: Job, context: &NodeContext) -> Job {
        self.visits.lock().unwrap().push(Visit {
            path: context.node_path().to_string(),
            key: context.idempotency_key().to_string(),
            recovery: context.is_recovery(),
            reviewer: context.decision().map(|d| d.reviewer.clone()),
        });
        let hook = self.hooks.lock().unwrap().remove(&format!(
            "{}:{}",
            context.thread_id(),
            context.node_path()
        ));
        if let Some(hook) = hook {
            hook(context);
        }
        job.trail.push_str(context.node());
        job
    }
}

// --- Graphs ------------------------------------------------------------------

fn durable() -> CompileOptions {
    CompileOptions::new().with_checkpointing(CheckpointPolicy::EveryNode)
}

fn node(graph: StateGraph<Job>, journal: &Arc<Journal>, name: &str) -> StateGraph<Job> {
    let journal = journal.clone();
    graph.add_async_node(name, move |job: Job, context: NodeContext| {
        let journal = journal.clone();
        async move { journal.visit(job, &context) }
    })
}

/// The nodes in a line, each adding its name to the trail.
fn chain(journal: &Arc<Journal>, nodes: &[&str]) -> StateGraph<Job> {
    let mut graph = StateGraph::<Job>::new();
    for (index, name) in nodes.iter().enumerate() {
        graph = node(graph, journal, name);
        if index > 0 {
            graph = graph.add_edge(nodes[index - 1], *name);
        }
    }
    graph
}

fn line(journal: &Arc<Journal>, nodes: &[&str]) -> Arc<CompiledGraph<Job>> {
    Arc::new(chain(journal, nodes).compile_with(durable()).unwrap())
}

/// `A` (a boundary, rejected to `Rejected`) -> `B` -> `C`.
fn approval(journal: &Arc<Journal>) -> Arc<CompiledGraph<Job>> {
    let graph =
        chain(journal, &["A", "B", "C"]).add_interrupt_boundary_with_reject("A", "Rejected");
    Arc::new(
        node(graph, journal, "Rejected")
            .compile_with(durable())
            .unwrap(),
    )
}

/// `Before` -> `Work` (a subgraph: `X` -> `Y` -> `Z`) -> `After`.
fn with_subgraph(journal: &Arc<Journal>) -> Arc<CompiledGraph<Job>> {
    let child = chain(journal, &["X", "Y", "Z"]).compile().unwrap();
    let graph = node(StateGraph::<Job>::new(), journal, "Before").add_subgraph(
        "Work",
        child,
        |_: &Job| job(),
        |parent: &Job, done: Job| Job {
            trail: format!("{}[{}]", parent.trail, done.trail),
            ..parent.clone()
        },
    );
    Arc::new(
        node(graph, journal, "After")
            .add_edge("Before", "Work")
            .add_edge("Work", "After")
            .compile_with(durable())
            .unwrap(),
    )
}

fn branch(
    journal: Arc<Journal>,
) -> impl Fn(Job, NodeContext) -> BoxFuture<'static, Result<Job, NodeError>> {
    move |job, context| {
        let journal = journal.clone();
        Box::pin(async move { Ok(journal.visit(job, &context)) })
    }
}

/// `Group` (branches `b1`, `b2`) -> `Fan` (a map over `k1`, `k2`: `Step` ->
/// `Then`) -> `Done`.
fn with_fan_outs(journal: &Arc<Journal>) -> Arc<CompiledGraph<Job>> {
    let item = chain(journal, &["Step", "Then"]).compile().unwrap();
    let graph = StateGraph::<Job>::new()
        .add_parallel_group(
            "Group",
            ParallelBranches::new()
                .async_branch("b1", branch(journal.clone()))
                .async_branch("b2", branch(journal.clone())),
            |entry: &Job, _| Job {
                trail: format!("{}G", entry.trail),
                ..entry.clone()
            },
        )
        .add_map(
            "Fan",
            |_: &Job| vec!["k1".to_string(), "k2".to_string()],
            |key: &String| key.clone(),
            item,
            |_: &Job, key: String| Job {
                trail: key,
                ..job()
            },
            |entry: &Job, _: Vec<Job>| Job {
                trail: format!("{}M", entry.trail),
                ..entry.clone()
            },
        );
    Arc::new(
        node(graph, journal, "Done")
            .add_edge("Group", "Fan")
            .add_edge("Fan", "Done")
            .compile_with(durable())
            .unwrap(),
    )
}

// --- Helpers -----------------------------------------------------------------

fn leased(owner: &str) -> RunConfig {
    RunConfig::new().with_lease(LeaseOptions::new(owner, TTL))
}

fn completed(result: GraphExecutionResult<Job>) -> Job {
    match result {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(error) => panic!("expected a completed run, got: {error:?}"),
        GraphExecutionResult::Interrupted { .. } => {
            panic!("expected a completed run, got a freeze")
        }
        GraphExecutionResult::Cancelled { reason, .. } => {
            panic!("expected a completed run, got one cancelled as {reason:?}")
        }
    }
}

fn refused<S>(result: GraphExecutionResult<S>) -> String {
    match result {
        GraphExecutionResult::Failed(StrataError::ExecutionAborted(message)) => message,
        GraphExecutionResult::Failed(StrataError::NodeNotFound(message)) => message,
        GraphExecutionResult::Failed(error) => format!("{error:?}"),
        _ => panic!("expected a refusal"),
    }
}

fn status(status: CheckpointStatus) -> &'static str {
    match status {
        CheckpointStatus::Interrupted => "interrupted",
        CheckpointStatus::Completed => "completed",
        CheckpointStatus::Failed => "failed",
        CheckpointStatus::Cancelled => "cancelled",
        CheckpointStatus::Ended => "ended",
        CheckpointStatus::Running => "running",
    }
}

/// `running:A>B` for each level, outermost first.
fn shape(checkpoint: &Checkpoint) -> String {
    let mut levels = Vec::new();
    let mut level = Some(checkpoint);
    while let Some(current) = level {
        levels.push(format!(
            "{}:{}>{}",
            status(current.status),
            current.suspended_node.as_deref().unwrap_or("-"),
            current.next_node.as_deref().unwrap_or("-")
        ));
        level = current.nested.as_deref();
    }
    levels.join(" / ")
}

fn shapes(store: &LeaseStore, thread_id: &str) -> Vec<String> {
    store.log(thread_id).iter().map(shape).collect()
}

fn trail(checkpoint: &Checkpoint) -> String {
    checkpoint.frozen_state["trail"]
        .as_str()
        .unwrap()
        .to_string()
}

fn meta<'a>(checkpoint: &'a Checkpoint, key: &str) -> Option<&'a str> {
    checkpoint
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get(key))
        .map(String::as_str)
}

/// `time` rounded up to the second, as a refusal writes it.
fn text(time: DateTime<Utc>) -> String {
    let floored = time.with_nanosecond(0).unwrap();
    let rounded = if floored == time {
        floored
    } else {
        floored + chrono::Duration::seconds(1)
    };
    rounded.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn sorted(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values
}

/// Runs thread `thread_id` in a process that dies when it reaches `path`.
async fn crash(
    graph: &Arc<CompiledGraph<Job>>,
    journal: &Arc<Journal>,
    store: &Arc<LeaseStore>,
    thread_id: &str,
    path: &str,
    config: RunConfig,
) {
    let process = Process::new(store);
    journal.die_at(thread_id, path, &process);
    let result = graph.run_with(thread_id, job(), process, config).await;
    assert_eq!(refused(result), DIED);
}

async fn events_of(mut events: tokio::sync::mpsc::UnboundedReceiver<RunEvent>) -> Vec<RunEvent> {
    let mut all = Vec::new();
    while let Some(event) = tokio::time::timeout(PATIENCE, events.recv()).await.unwrap() {
        all.push(event);
    }
    all
}

fn name(event: &RunEvent) -> &'static str {
    match event.kind {
        RunEventKind::RunStarted { .. } => "RunStarted",
        RunEventKind::Recovered { .. } => "Recovered",
        RunEventKind::Resumed { .. } => "Resumed",
        RunEventKind::NodeStarted => "NodeStarted",
        RunEventKind::NodeCompleted { .. } => "NodeCompleted",
        RunEventKind::CheckpointSaved { .. } => "CheckpointSaved",
        RunEventKind::RunCompleted { .. } => "RunCompleted",
        _ => "other",
    }
}

// --- What is written ---------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_durable_run_writes_a_checkpoint_before_each_node_and_completes() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = line(&journal, &["A", "B", "C"]);

    let result = graph.run("t", job(), store.clone()).await;

    assert_eq!(completed(result).trail, "ABC");
    let log = store.log("t");
    assert_eq!(
        shapes(&store, "t"),
        ["running:->A", "running:A>B", "running:B>C", "completed:C>-"]
    );
    assert_eq!(
        log.iter().map(trail).collect::<Vec<_>>(),
        ["", "A", "AB", "ABC"]
    );
    assert_eq!(
        log.iter()
            .map(|c| meta(c, "strata.hops").unwrap())
            .collect::<Vec<_>>(),
        ["0", "1", "2", "3"]
    );
    assert_eq!(
        log.iter()
            .map(|c| c.schema_version.as_str())
            .collect::<Vec<_>>(),
        ["1.7.0", "1.7.0", "1.7.0", "1.0.0"]
    );
    assert!(log.iter().all(|c| c.resumed_at.is_none()));

    // A finished thread says so.
    let done = store
        .list_threads(&ThreadQuery::default().with_states([ThreadState::Completed]))
        .await
        .unwrap();
    assert_eq!(
        done.threads
            .iter()
            .map(|t| t.thread_id.as_str())
            .collect::<Vec<_>>(),
        ["t"]
    );
    assert_eq!(journal.keys(), ["t/A#1", "t/B#2", "t/C#3"]);
    assert!(journal.visits().iter().all(|v| !v.recovery));
}

#[tokio::test(start_paused = true)]
async fn a_run_without_the_policy_writes_nothing_between_nodes() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = chain(&journal, &["A", "B", "C"]).compile().unwrap();

    let result = graph
        .run_with(
            "t",
            job(),
            store.clone(),
            RunConfig::new().with_run_id("run-1"),
        )
        .await;

    assert_eq!(completed(result).trail, "ABC");
    assert!(store.log("t").is_empty());
    assert_eq!(graph.checkpointing(), CheckpointPolicy::Boundaries);

    // Its hop counts aren't stored, so its keys end with the run id: a later
    // resume can't repeat one.
    assert_eq!(
        journal.keys(),
        ["t/A#1@run-1", "t/B#2@run-1", "t/C#3@run-1"]
    );
}

#[tokio::test(start_paused = true)]
async fn a_parallel_group_and_a_map_are_one_checkpoint_each() {
    let store = LeaseStore::new();
    let journal = Journal::new();

    let result = with_fan_outs(&journal).run("t", job(), store.clone()).await;

    assert_eq!(completed(result).trail, "GMDone");
    assert_eq!(
        shapes(&store, "t"),
        [
            "running:->Group",
            "running:Group>Fan",
            "running:Fan>Done",
            "completed:Done>-"
        ]
    );

    // A branch's key is its group's hop; an item's nodes count their own hops
    // under the map's.
    assert_eq!(
        sorted(journal.keys()),
        [
            "t/Done#3",
            "t/Fan/k1/Step#2.1",
            "t/Fan/k1/Then#2.2",
            "t/Fan/k2/Step#2.1",
            "t/Fan/k2/Then#2.2",
            "t/Group/b1#1",
            "t/Group/b2#1",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_subgraph_nests_its_childs_progress_in_the_parents_checkpoint() {
    let store = LeaseStore::new();
    let journal = Journal::new();

    let result = with_subgraph(&journal).run("t", job(), store.clone()).await;

    assert_eq!(completed(result).trail, "Before[XYZ]After");
    let log = store.log("t");
    assert_eq!(
        shapes(&store, "t"),
        [
            "running:->Before",
            "running:Before>Work",
            "running:Work>Work / running:X>Y",
            "running:Work>Work / running:Y>Z",
            "running:Work>After",
            "completed:After>-",
        ]
    );

    // The parent holds the state its subgraph node started with; the child
    // holds its own.
    let inside = &log[3];
    assert_eq!(
        (trail(inside).as_str(), meta(inside, "strata.hops")),
        ("Before", Some("1"))
    );
    let child = inside.nested.as_deref().unwrap();
    assert_eq!(
        (
            trail(child).as_str(),
            meta(child, "strata.hops"),
            child.thread_id.as_str()
        ),
        ("XY", Some("2"), "Work")
    );
    assert_eq!(inside.schema_version, "1.7.0");
    assert_eq!(
        journal.keys(),
        [
            "t/Before#1",
            "t/Work/X#2.1",
            "t/Work/Y#2.2",
            "t/Work/Z#2.3",
            "t/After#3"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn progress_two_levels_down_is_nested_at_every_level() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let inner = chain(&journal, &["P", "Q"]).compile().unwrap();
    let mid = node(
        StateGraph::<Job>::new()
            .add_subgraph("Stage", inner, Job::clone, |_: &Job, done: Job| done),
        &journal,
        "Wrap",
    )
    .add_edge("Stage", "Wrap")
    .compile()
    .unwrap();
    let graph = StateGraph::<Job>::new()
        .add_subgraph("Work", mid, Job::clone, |_: &Job, done: Job| done)
        .compile_with(durable())
        .unwrap();

    let result = graph.run("t", job(), store.clone()).await;

    assert_eq!(completed(result).trail, "PQWrap");
    assert_eq!(
        shapes(&store, "t"),
        [
            "running:->Work",
            "running:Work>Work / running:Stage>Stage / running:P>Q",
            "running:Work>Work / running:Stage>Wrap",
            "completed:Work>-",
        ]
    );
    assert_eq!(
        journal.keys(),
        [
            "t/Work/Stage/P#1.1.1",
            "t/Work/Stage/Q#1.1.2",
            "t/Work/Wrap#1.2"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_boundary_writes_an_interrupted_checkpoint_and_no_running_one_after_it() {
    let store = LeaseStore::new();
    let graph = approval(&Journal::new());

    let result = graph.run("t", job(), store.clone()).await;

    assert!(matches!(result, GraphExecutionResult::Interrupted { .. }));
    assert_eq!(shapes(&store, "t"), ["running:->A", "interrupted:A>B"]);
    assert_eq!(meta(&store.log("t")[1], "strata.hops"), Some("1"));
}

// --- Never left running ------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_failed_node_a_cancellation_and_a_hop_limit_leave_their_own_checkpoint() {
    let store = LeaseStore::new();
    let journal = Journal::new();

    let failing = node(StateGraph::<Job>::new(), &journal, "A")
        .add_fallible_node("B", |_: &Job| Err(NodeError::Fatal("boom".to_string())))
        .add_edge("A", "B")
        .compile_with(durable())
        .unwrap();
    assert!(matches!(
        failing.run("failed", job(), store.clone()).await,
        GraphExecutionResult::Failed(StrataError::NodeFailed { .. })
    ));
    assert_eq!(
        shapes(&store, "failed"),
        ["running:->A", "running:A>B", "failed:B>B"]
    );

    let token = CancellationToken::new();
    let stopping = node(StateGraph::<Job>::new(), &journal, "A")
        .add_async_node("B", {
            let token = token.clone();
            move |_: Job, _: NodeContext| {
                token.cancel();
                // Never finishes: the engine stops waiting for it.
                std::future::pending::<Job>()
            }
        })
        .add_edge("A", "B")
        .compile_with(durable())
        .unwrap();
    assert!(matches!(
        stopping
            .run_with(
                "cancelled",
                job(),
                store.clone(),
                RunConfig::new().with_cancellation(token)
            )
            .await,
        GraphExecutionResult::Cancelled { .. }
    ));
    assert_eq!(
        shapes(&store, "cancelled"),
        ["running:->A", "running:A>B", "cancelled:B>B"]
    );

    let limited = chain(&journal, &["A", "B"])
        .compile_with(durable().with_max_node_hops(1))
        .unwrap();
    assert!(matches!(
        limited.run("limited", job(), store.clone()).await,
        GraphExecutionResult::Failed(StrataError::HopLimitExceeded { .. })
    ));
    assert_eq!(
        shapes(&store, "limited"),
        ["running:->A", "running:A>B", "failed:B>B"]
    );
}

#[tokio::test(start_paused = true)]
async fn a_routing_gap_leaves_a_failed_checkpoint_that_can_be_resumed() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = node(node(StateGraph::<Job>::new(), &journal, "A"), &journal, "B")
        .add_conditional_edge("A", "B", |job: &Job| job.approved)
        .compile_with(durable())
        .unwrap();

    let result = graph.run("t", job(), store.clone()).await;

    // The run gave up with nothing to say where; the thread says it failed at
    // the node it was running.
    assert!(matches!(
        result,
        GraphExecutionResult::Failed(StrataError::NoMatchingEdge(node)) if node == "A"
    ));
    let log = store.log("t");
    assert_eq!(shapes(&store, "t"), ["running:->A", "failed:A>A"]);
    assert_eq!(
        (
            meta(&log[1], "error"),
            meta(&log[1], "error_kind"),
            trail(&log[1]).as_str()
        ),
        (
            Some("no outgoing edge condition matched for node 'A'"),
            Some("fatal"),
            ""
        )
    );

    let resumed = graph
        .resume(
            "t",
            HumanSignal::approve("ops").with_override("approved", true),
            store.clone(),
        )
        .await;

    assert_eq!(completed(resumed).trail, "AB");
    assert_eq!(
        shapes(&store, "t"),
        [
            "running:->A",
            "failed:A>A",
            "running:A>A",
            "running:A>B",
            "completed:B>-"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_panic_out_of_a_node_leaves_a_failed_checkpoint_and_still_propagates() {
    let store = LeaseStore::new();
    let graph = Arc::new(
        node(StateGraph::<Job>::new(), &Journal::new(), "A")
            .add_async_node("B", |_: Job, _: NodeContext| async move { panic!("a bug") })
            .add_edge("A", "B")
            .compile_with(durable())
            .unwrap(),
    );

    let run = tokio::spawn({
        let (graph, store) = (graph.clone(), store.clone());
        async move { graph.run("t", job(), store).await }
    });

    assert!(run.await.is_err_and(|joined| joined.is_panic()));
    let log = store.log("t");
    assert_eq!(
        shapes(&store, "t"),
        ["running:->A", "running:A>B", "failed:B>B"]
    );
    assert_eq!(
        (
            meta(&log[2], "error"),
            meta(&log[2], "error_kind"),
            trail(&log[2]).as_str()
        ),
        (Some("a bug"), Some("fatal"), "A")
    );
}

// --- A resume's claim --------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_resume_claims_the_thread_with_a_running_checkpoint_holding_its_decision() {
    let store = LeaseStore::new();
    let graph = approval(&Journal::new());
    graph.run("t", job(), store.clone()).await;

    let result = graph
        .resume(
            "t",
            HumanSignal::approve("alice").with_override("limit", 5),
            store.clone(),
        )
        .await;

    assert_eq!(
        completed(result),
        Job {
            trail: "ABC".to_string(),
            approved: false,
            limit: 5
        }
    );
    let log = store.log("t");
    assert_eq!(
        shapes(&store, "t"),
        [
            "running:->A",
            "interrupted:A>B",
            "running:A>B",
            "running:B>C",
            "completed:C>-"
        ]
    );

    // The claim is the resume: the decision, the state with the override, and
    // no stamp anywhere.
    let claim = &log[2];
    let decision = claim.decision.as_ref().unwrap();
    assert_eq!(
        (
            decision.reviewer.as_str(),
            decision.is_approved,
            decision.boundary.as_str()
        ),
        ("alice", true, "A")
    );
    assert_eq!(claim.frozen_state["limit"], 5);
    assert_eq!(meta(claim, "strata.hops"), Some("1"));
    assert!(log.iter().all(|c| c.resumed_at.is_none()));

    // A rejection's claim holds the route it took.
    graph.run("no", job(), store.clone()).await;
    let rejected = graph
        .resume("no", HumanSignal::reject("bob"), store.clone())
        .await;
    assert_eq!(completed(rejected).trail, "ARejected");
    let rejection = &store.log("no")[2];
    assert_eq!(
        (
            shape(rejection).as_str(),
            rejection.decision.as_ref().unwrap().is_approved
        ),
        ("running:A>Rejected", false)
    );
}

#[tokio::test(start_paused = true)]
async fn a_running_thread_refuses_a_resume_and_a_signal() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = line(&journal, &["A", "B", "C"]);
    crash(&graph, &journal, &store, "t", "B", RunConfig::new()).await;
    let before = shapes(&store, "t");

    let refusal =
        "thread 't' is running: only a waiting, failed or cancelled thread can be resumed";
    assert_eq!(
        refused(
            graph
                .resume("t", HumanSignal::approve("alice"), store.clone())
                .await
        ),
        refusal
    );
    assert_eq!(
        refused(
            graph
                .signal("t", EventSignal::new("paid"), store.clone())
                .await
        ),
        refusal
    );

    assert_eq!(shapes(&store, "t"), before);
    assert_eq!(journal.count("B"), 1);
}

#[tokio::test(start_paused = true)]
async fn of_two_resumes_that_loaded_one_checkpoint_one_runs() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = approval(&journal);
    graph.run("t", job(), store.clone()).await;
    let loaded = store.load_latest("t").await.unwrap().unwrap();

    let first = graph
        .resume("t", HumanSignal::approve("alice"), store.clone())
        .await;
    assert_eq!(completed(first).trail, "ABC");
    let second = graph
        .resume(
            "t",
            HumanSignal::approve("bob"),
            Process::rewound(&store, loaded),
        )
        .await;

    assert_eq!(
        refused(second),
        "checkpoint for thread 't' changed before it could be resumed: another resume or end got there first"
    );
    assert_eq!(journal.count("B"), 1);
}

// --- A crash recovered -------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_crash_mid_run_is_recovered_and_the_node_in_flight_runs_again_with_the_same_key() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = line(&journal, &["A", "B", "C"]);

    crash(&graph, &journal, &store, "t", "B", RunConfig::new()).await;

    // Nothing after the node in flight reached the store.
    assert_eq!(shapes(&store, "t"), ["running:->A", "running:A>B"]);
    let running = store
        .list_threads(&ThreadQuery::default().with_states([ThreadState::Running]))
        .await
        .unwrap();
    assert_eq!(
        running
            .threads
            .iter()
            .map(|t| t.thread_id.as_str())
            .collect::<Vec<_>>(),
        ["t"]
    );

    // A new process continues the thread where its last checkpoint says.
    let result = graph.recover("t", store.clone()).await;

    assert_eq!(completed(result).trail, "ABC");
    assert_eq!(
        journal.visits(),
        [
            visit("A", "t/A#1", false),
            visit("B", "t/B#2", false),
            visit("B", "t/B#2", true),
            visit("C", "t/C#3", false),
        ]
    );
    let log = store.log("t");
    assert_eq!(
        shapes(&store, "t"),
        [
            "running:->A",
            "running:A>B",
            "running:A>B",
            "running:B>C",
            "completed:C>-"
        ]
    );

    // Only the claim counts the recovery: a node that commits starts the
    // count again.
    assert_eq!(meta(&log[2], "strata.recoveries"), Some("1"));
    assert_eq!(meta(&log[3], "strata.recoveries"), None);
}

#[tokio::test(start_paused = true)]
async fn a_crash_after_an_approval_is_recovered_with_the_decision() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = approval(&journal);
    graph.run("t", job(), store.clone()).await;
    let process = Process::new(&store);
    journal.die_at("t", "B", &process);

    let died = graph
        .resume(
            "t",
            HumanSignal::approve("alice").with_override("limit", 5),
            process,
        )
        .await;
    assert_eq!(refused(died), DIED);

    // The claim is all that was written, and it is enough.
    assert_eq!(
        shapes(&store, "t"),
        ["running:->A", "interrupted:A>B", "running:A>B"]
    );

    let result = graph.recover("t", store.clone()).await;

    assert_eq!(
        completed(result),
        Job {
            trail: "ABC".to_string(),
            approved: false,
            limit: 5
        }
    );
    let after: Vec<Visit> = journal
        .visits()
        .into_iter()
        .filter(|v| v.path != "A")
        .collect();
    assert!(after.iter().all(|v| v.reviewer.as_deref() == Some("alice")));
    assert_eq!(
        after.iter().map(|v| v.recovery).collect::<Vec<_>>(),
        [false, true, false]
    );
}

#[tokio::test(start_paused = true)]
async fn a_crash_inside_a_subgraph_re_enters_the_child_at_its_node_in_flight() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = with_subgraph(&journal);

    crash(&graph, &journal, &store, "t", "Work/Y", RunConfig::new()).await;
    assert_eq!(
        shapes(&store, "t").last().unwrap(),
        "running:Work>Work / running:X>Y"
    );

    let result = graph.recover("t", store.clone()).await;

    // The child's first node doesn't run again; the node in flight does, with
    // the key it had.
    assert_eq!(completed(result).trail, "Before[XYZ]After");
    assert_eq!(
        journal.visits(),
        [
            visit("Before", "t/Before#1", false),
            visit("Work/X", "t/Work/X#2.1", false),
            visit("Work/Y", "t/Work/Y#2.2", false),
            visit("Work/Y", "t/Work/Y#2.2", true),
            visit("Work/Z", "t/Work/Z#2.3", false),
            visit("After", "t/After#3", false),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_crash_in_a_childs_first_node_re_enters_the_child_from_its_input() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = with_subgraph(&journal);

    crash(&graph, &journal, &store, "t", "Work/X", RunConfig::new()).await;
    assert_eq!(shapes(&store, "t").last().unwrap(), "running:Before>Work");

    let result = graph.recover("t", store.clone()).await;

    assert_eq!(completed(result).trail, "Before[XYZ]After");
    assert_eq!(
        journal.visits(),
        [
            visit("Before", "t/Before#1", false),
            visit("Work/X", "t/Work/X#2.1", false),
            visit("Work/X", "t/Work/X#2.1", true),
            visit("Work/Y", "t/Work/Y#2.2", false),
            visit("Work/Z", "t/Work/Z#2.3", false),
            visit("After", "t/After#3", false),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_crash_inside_a_group_or_a_map_runs_every_branch_and_item_again() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = with_fan_outs(&journal);
    let recovered = |journal: &Journal, thread: &str| {
        sorted(
            journal
                .visits()
                .into_iter()
                .filter(|v| v.recovery && v.key.starts_with(thread))
                .map(|v| v.key)
                .collect(),
        )
    };

    crash(
        &graph,
        &journal,
        &store,
        "group",
        "Group/b2",
        RunConfig::new(),
    )
    .await;
    assert_eq!(
        completed(graph.recover("group", store.clone()).await).trail,
        "GMDone"
    );
    assert_eq!(
        recovered(&journal, "group/"),
        ["group/Group/b1#1", "group/Group/b2#1"]
    );

    // An item isn't checkpointed, so every node of every item may have run
    // before.
    crash(
        &graph,
        &journal,
        &store,
        "map",
        "Fan/k1/Then",
        RunConfig::new(),
    )
    .await;
    assert_eq!(
        completed(graph.recover("map", store.clone()).await).trail,
        "GMDone"
    );
    assert_eq!(
        recovered(&journal, "map/"),
        [
            "map/Fan/k1/Step#2.1",
            "map/Fan/k1/Then#2.2",
            "map/Fan/k2/Step#2.1",
            "map/Fan/k2/Then#2.2"
        ]
    );
    assert_eq!(journal.count("Done"), 2);
}

#[tokio::test(start_paused = true)]
async fn a_crash_while_waiting_at_a_boundary_leaves_nothing_to_recover() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = approval(&journal);
    graph.run("t", job(), Process::new(&store)).await;

    // The process is gone, and the thread holds nothing: it is waiting, not
    // running.
    let recovery = graph.recover("t", store.clone()).await;

    assert_eq!(
        refused(recovery),
        "thread 't' is not running: its latest checkpoint is interrupted"
    );
    assert_eq!(shapes(&store, "t"), ["running:->A", "interrupted:A>B"]);
    let resumed = graph
        .resume("t", HumanSignal::approve("alice"), store.clone())
        .await;
    assert_eq!(completed(resumed).trail, "ABC");
}

#[tokio::test(start_paused = true)]
async fn a_graph_with_a_streaming_node_is_recovered_by_recover_streaming() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = Arc::new(
        node(StateGraph::<Job>::new(), &journal, "A")
            .add_async_streaming_node("S", {
                let journal = journal.clone();
                move |job: Job, context: NodeContext| {
                    let journal = journal.clone();
                    async move {
                        context.stream().unwrap().emit_token("tok").await.unwrap();
                        journal.visit(job, &context)
                    }
                }
            })
            .add_edge("A", "S")
            .compile_with(durable())
            .unwrap(),
    );
    let process = Process::new(&store);
    journal.die_at("t", "S", &process);
    let (dying, mut chunks) = graph.clone().run_streaming("t", job(), process);
    while chunks.recv().await.is_some() {}
    assert_eq!(refused(dying.await.unwrap()), DIED);

    let (handle, mut chunks) = graph.clone().recover_streaming("t", store.clone());
    let mut seen = Vec::new();
    while let Some(chunk) = chunks.recv().await {
        let kind = match chunk.chunk_type {
            StreamChunkType::NodeStart => "NodeStart",
            StreamChunkType::Token => "Token",
            StreamChunkType::NodeEnd => "NodeEnd",
            _ => "other",
        };
        seen.push(format!("{kind}:{}", chunk.node_name));
    }

    assert_eq!(completed(handle.await.unwrap()).trail, "AS");
    assert_eq!(seen, ["NodeStart:S", "Token:S", "NodeEnd:S"]);
}

// --- Refusals ----------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_recovery_is_refused_without_the_policy_for_another_graphs_thread_and_for_no_thread() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = Arc::new(
        chain(&journal, &["A", "B", "C"])
            .compile_with(durable().with_graph_id("billing"))
            .unwrap(),
    );
    crash(&graph, &journal, &store, "t", "B", RunConfig::new()).await;
    let before = shapes(&store, "t");

    let plain = chain(&journal, &["A", "B", "C"])
        .compile_with(CompileOptions::new().with_graph_id("billing"))
        .unwrap();
    assert_eq!(
        refused(plain.recover("t", store.clone()).await),
        "this graph doesn't checkpoint every node: only a graph compiled with CheckpointPolicy::EveryNode can recover a thread"
    );

    let other = chain(&journal, &["A", "B", "C"])
        .compile_with(durable().with_graph_id("shipping"))
        .unwrap();
    assert!(
        refused(other.recover("t", store.clone()).await)
            .starts_with("thread 't' was written by graph 'billing'")
    );

    assert_eq!(
        refused(graph.recover("nobody", store.clone()).await),
        "no checkpoint found for thread 'nobody'"
    );

    assert_eq!(shapes(&store, "t"), before);
    assert_eq!(journal.count("B"), 1);
}

#[tokio::test(start_paused = true)]
async fn a_live_lease_refuses_a_recovery_with_or_without_a_lease_of_its_own() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = line(&journal, &["A", "B", "C"]);
    let until = store.now() + chrono::Duration::from_std(TTL).unwrap();

    // The process died holding its lease: nothing released it.
    crash(&graph, &journal, &store, "t", "B", leased("worker-1")).await;
    let before = shapes(&store, "t");
    let held = format!("thread 't' is held by 'worker-1' until {}", text(until));

    assert_eq!(
        refused(
            graph
                .recover_with("t", store.clone(), leased("worker-2"))
                .await
        ),
        held
    );
    assert_eq!(refused(graph.recover("t", store.clone()).await), held);
    assert_eq!(shapes(&store, "t"), before);
    assert_eq!(journal.count("B"), 1);

    // The lease runs out, and the thread is nobody's.
    store.advance(TTL + Duration::from_millis(1));
    let result = graph
        .recover_with("t", store.clone(), leased("worker-2"))
        .await;

    assert_eq!(completed(result).trail, "ABC");
    assert!(store.held("t").is_none());
}

#[tokio::test(start_paused = true)]
async fn of_two_recoveries_one_runs() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = line(&journal, &["A", "B", "C"]);
    crash(&graph, &journal, &store, "t", "B", RunConfig::new()).await;
    let loaded = store.load_latest("t").await.unwrap().unwrap();

    assert_eq!(
        completed(graph.recover("t", store.clone()).await).trail,
        "ABC"
    );
    let second = graph.recover("t", Process::rewound(&store, loaded)).await;

    assert_eq!(
        refused(second),
        "thread 't' changed before it could be recovered: its run went on, or another recovery got there first"
    );
    assert_eq!(journal.count("B"), 2);
    assert_eq!(journal.count("C"), 1);
}

// --- The driver --------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn the_driver_skips_a_live_lease_and_recovers_a_thread_whose_lease_expired() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = line(&journal, &["A", "B", "C"]);
    crash(&graph, &journal, &store, "dead", "B", leased("worker-1")).await;
    crash(
        &graph,
        &journal,
        &store,
        "live",
        "B",
        RunConfig::new().with_lease(LeaseOptions::new("worker-9", Duration::from_secs(3600))),
    )
    .await;
    let live_until = store.held("live").unwrap().expires_at;
    let options = || RecoverOptions::new().with_config(leased("worker-2"));

    // Both are held: nothing is recovered, and nothing is written.
    let early = graph
        .recover_stale(store.clone(), store.now(), options())
        .await
        .unwrap();
    assert!(early.recovered.is_empty());
    assert_eq!(
        sorted(early.skipped.into_iter().map(|s| s.thread_id).collect()),
        ["dead", "live"]
    );

    store.advance(TTL + Duration::from_millis(1));
    let mut report = graph
        .recover_stale(store.clone(), store.now(), options())
        .await
        .unwrap();

    assert_eq!(report.recovered.len(), 1);
    let recovered = report.recovered.remove(0);
    assert_eq!(
        (recovered.thread_id.as_str(), recovered.recoveries),
        ("dead", 1)
    );
    assert_eq!(recovered.at, ["B"]);
    assert_eq!(completed(recovered.result).trail, "ABC");
    assert_eq!(
        report.skipped,
        [SkippedThread {
            thread_id: "live".to_string(),
            reason: format!("it is held by 'worker-9' until {}", text(live_until)),
        }]
    );
    assert_eq!(shapes(&store, "live").last().unwrap(), "running:A>B");
}

#[tokio::test(start_paused = true)]
async fn without_leases_the_driver_needs_an_age_and_recovers_only_threads_older_than_it() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = line(&journal, &["A", "B", "C"]);
    crash(&graph, &journal, &store, "t", "B", RunConfig::new()).await;
    let five_minutes = || RecoverOptions::new().with_stale_after(Duration::from_secs(300));

    // Only the caller knows how long its nodes take.
    let Err(StrataError::ExecutionAborted(refusal)) = graph
        .recover_stale(store.clone(), Utc::now(), RecoverOptions::new())
        .await
    else {
        panic!("expected a refusal");
    };
    assert!(refusal.starts_with("recover_stale needs RecoverOptions::with_stale_after"));

    let fresh = graph
        .recover_stale(store.clone(), Utc::now(), five_minutes())
        .await
        .unwrap();
    assert!(fresh.recovered.is_empty());
    assert_eq!(journal.count("B"), 1);

    let later = Utc::now() + chrono::Duration::minutes(10);
    let mut report = graph
        .recover_stale(store.clone(), later, five_minutes())
        .await
        .unwrap();

    assert_eq!(report.recovered.len(), 1);
    assert_eq!(completed(report.recovered.remove(0).result).trail, "ABC");
}

#[tokio::test]
async fn a_stop_token_recovers_no_more_threads_and_lets_the_one_in_flight_finish() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = line(&journal, &["A", "B", "C"]);
    crash(&graph, &journal, &store, "a", "B", RunConfig::new()).await;
    crash(&graph, &journal, &store, "b", "B", RunConfig::new()).await;
    let later = Utc::now() + chrono::Duration::minutes(10);
    let stop = CancellationToken::new();
    let five_minutes = || RecoverOptions::new().with_stale_after(Duration::from_secs(300));
    let last_shape = |thread_id: &str| shapes(&store, thread_id).pop().unwrap();

    // The host is asked to stop while a recovery is in flight
    // (design/35-Hosting-Integration.md).
    for thread_id in ["a", "b"] {
        let stop = stop.clone();
        journal.on(thread_id, "B", move |_| stop.cancel());
    }
    let mut report = graph
        .recover_stale(store.clone(), later, five_minutes().with_stop(stop.clone()))
        .await
        .unwrap();

    // The recovery in flight finished, not cancelled, and the other thread
    // was never started.
    assert_eq!(report.recovered.len(), 1);
    let recovered = report.recovered.remove(0);
    assert_eq!(completed(recovered.result).trail, "ABC");
    let other = if recovered.thread_id == "a" { "b" } else { "a" };
    assert_eq!(last_shape(other), "running:A>B");

    // A token that has fired starts nothing.
    let none = graph
        .recover_stale(store.clone(), later, five_minutes().with_stop(stop.clone()))
        .await
        .unwrap();
    assert!(none.recovered.is_empty());
    assert_eq!(last_shape(other), "running:A>B");

    // The thread is still dead for the next driver.
    let next = graph
        .recover_stale(store.clone(), later, five_minutes())
        .await
        .unwrap();
    assert_eq!(next.recovered.len(), 1);
    assert_eq!(next.recovered[0].thread_id, other);
}

#[tokio::test(start_paused = true)]
async fn the_driver_lists_its_own_graphs_threads_by_metadata_and_counts_recoveries_in_its_limit() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let named = |id: &str| {
        Arc::new(
            chain(&journal, &["A", "B", "C"])
                .compile_with(durable().with_graph_id(id))
                .unwrap(),
        )
    };
    let billing = named("billing");
    let tenant = |tenant: &str| RunConfig::new().with_metadata("tenant", tenant);
    crash(&billing, &journal, &store, "acme-1", "B", tenant("acme")).await;
    crash(&billing, &journal, &store, "acme-2", "B", tenant("acme")).await;
    crash(&billing, &journal, &store, "other", "B", tenant("other")).await;
    crash(
        &named("shipping"),
        &journal,
        &store,
        "shipment",
        "B",
        tenant("acme"),
    )
    .await;
    let options = || {
        RecoverOptions::new()
            .with_stale_after(Duration::ZERO)
            .with_metadata("tenant", "acme")
            .with_limit(1)
    };

    let mut recovered = Vec::new();
    for _ in 0..3 {
        let report = billing
            .recover_stale(store.clone(), Utc::now(), options())
            .await
            .unwrap();
        assert!(report.skipped.is_empty());
        recovered.extend(report.recovered.into_iter().map(|r| r.thread_id));
    }

    assert_eq!(sorted(recovered), ["acme-1", "acme-2"]);
    assert_eq!(shapes(&store, "other").last().unwrap(), "running:A>B");
    assert_eq!(shapes(&store, "shipment").last().unwrap(), "running:A>B");
}

#[tokio::test(start_paused = true)]
async fn the_driver_stops_recovering_a_thread_that_keeps_dying() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = line(&journal, &["A", "B", "C"]);
    crash(&graph, &journal, &store, "t", "B", RunConfig::new()).await;
    let options = || {
        RecoverOptions::new()
            .with_stale_after(Duration::ZERO)
            .with_max_recoveries(2)
    };

    // Each recovery dies in the same node.
    for recovery in 1..=2 {
        let process = Process::new(&store);
        journal.die_at("t", "B", &process);
        let mut report = graph
            .recover_stale(process, Utc::now(), options())
            .await
            .unwrap();
        assert_eq!(refused(report.recovered.remove(0).result), DIED);
        assert_eq!(
            meta(&store.latest("t"), "strata.recoveries"),
            Some(recovery.to_string().as_str())
        );
    }

    let report = graph
        .recover_stale(store.clone(), Utc::now(), options())
        .await
        .unwrap();

    assert!(report.recovered.is_empty());
    assert_eq!(
        report.skipped,
        [SkippedThread {
            thread_id: "t".to_string(),
            reason: "it was recovered 2 time(s) without a node completing".to_string(),
        }]
    );
    assert_eq!(journal.count("B"), 3);

    // An operator can still recover it by hand, or end it.
    assert_eq!(
        completed(graph.recover("t", store.clone()).await).trail,
        "ABC"
    );
}

#[tokio::test(start_paused = true)]
async fn an_application_limits_its_own_recoveries_by_the_public_count() {
    // `recover` always recovers: an application that recovers each thread
    // itself (not through the driver) reads the count a claim records and
    // stops at its own limit.
    const LIMIT: u32 = 2;
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = line(&journal, &["A", "B", "C"]);
    crash(&graph, &journal, &store, "t", "B", RunConfig::new()).await;
    assert_eq!(recoveries_of(&store.latest("t")), 0);

    let mut recoveries = 0;
    while recoveries_of(&store.latest("t")) < LIMIT {
        let process = Process::new(&store);
        journal.die_at("t", "B", &process);
        assert_eq!(refused(graph.recover("t", process).await), DIED);
        recoveries += 1;
    }

    let latest = store.latest("t");
    assert_eq!(recoveries, LIMIT);
    assert_eq!(recoveries_of(&latest), LIMIT);
    assert_eq!(meta(&latest, RECOVERIES_KEY), Some("2"));
    assert_eq!(RECOVERIES_KEY, "strata.recoveries");

    // Past its limit it recovers by hand: the claim counts once more, and the
    // node that commits clears it.
    assert_eq!(
        completed(graph.recover("t", store.clone()).await).trail,
        "ABC"
    );
    assert_eq!(
        store.log("t").iter().map(recoveries_of).collect::<Vec<_>>(),
        [0, 0, 1, 2, 3, 0, 0]
    );
}

// --- Ending, and leases ------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_running_thread_is_ended_unless_a_live_lease_holds_it() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = line(&journal, &["A", "B", "C"]);
    let until = store.now() + chrono::Duration::from_std(TTL).unwrap();
    crash(&graph, &journal, &store, "t", "B", leased("worker-1")).await;

    assert_eq!(
        refused(graph.end("t", store.clone()).await),
        format!("thread 't' is held by 'worker-1' until {}", text(until))
    );
    assert_eq!(shapes(&store, "t").last().unwrap(), "running:A>B");

    store.advance(TTL + Duration::from_millis(1));
    let GraphExecutionResult::Cancelled {
        reason,
        node,
        state,
        stopped_at,
        ..
    } = graph.end("t", store.clone()).await
    else {
        panic!("expected an ended thread");
    };

    // It stood at the node that was to run.
    assert_eq!(
        (reason, node.as_str(), state.trail.as_str()),
        (CancelReason::Ended, "B", "A")
    );
    assert_eq!(stopped_at, ["B"]);
    assert_eq!(shapes(&store, "t").last().unwrap(), "ended:B>-");
    assert_eq!(
        refused(graph.recover("t", store.clone()).await),
        "thread 't' is not running: its latest checkpoint is ended"
    );
}

#[tokio::test(start_paused = true)]
async fn a_stale_owners_completion_is_fenced() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = line(&journal, &["A", "B"]);

    // While the last node runs, the lease runs out on the store's clock and
    // another worker takes the thread.
    journal.on("t", "B", {
        let store = store.clone();
        move |_| {
            store.take_over("t");
        }
    });

    let result = graph
        .run_with("t", job(), store.clone(), leased("worker-1"))
        .await;

    assert!(matches!(
        result,
        GraphExecutionResult::Cancelled {
            reason: CancelReason::LeaseLost,
            ..
        }
    ));
    assert_eq!(shapes(&store, "t"), ["running:->A", "running:A>B"]);
}

// --- Events ------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_recovery_says_so_in_its_events() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let graph = line(&journal, &["A", "B", "C"]);
    crash(&graph, &journal, &store, "t", "B", RunConfig::new()).await;
    let (observer, events) = RunEventChannel::new();

    graph
        .recover_with("t", store.clone(), RunConfig::new().with_observer(observer))
        .await;

    let events = events_of(events).await;
    assert_eq!(
        events.iter().map(name).collect::<Vec<_>>(),
        [
            "RunStarted",
            "CheckpointSaved",
            "Recovered",
            "NodeStarted",
            "NodeCompleted",
            "CheckpointSaved",
            "NodeStarted",
            "NodeCompleted",
            "CheckpointSaved",
            "RunCompleted",
        ]
    );
    assert!(events.iter().any(|e| matches!(
        &e.kind,
        RunEventKind::RunStarted { mode: RunMode::Recover, node, .. } if node == "B"
    )));
    assert!(events.iter().any(|e| matches!(
        &e.kind,
        RunEventKind::Recovered { at, recoveries: 1 } if at == &["B".to_string()]
    )));
    let saved: Vec<CheckpointStatus> = events
        .iter()
        .filter_map(|e| match &e.kind {
            RunEventKind::CheckpointSaved { status, .. } => Some(*status),
            _ => None,
        })
        .collect();
    assert_eq!(
        saved,
        [
            CheckpointStatus::Running,
            CheckpointStatus::Running,
            CheckpointStatus::Completed
        ]
    );
}

// --- Shared fixtures ---------------------------------------------------------

fn lifecycle_fixture(name: &str) -> Checkpoint {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schema/fixtures/checkpoint-lifecycle")
        .join(name);
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// Blanks every level's `created_at` and `decided_at`, which a live run sets
/// to now.
fn without_timestamps(mut checkpoint: Checkpoint) -> Checkpoint {
    checkpoint.created_at = String::new();
    if let Some(decision) = &mut checkpoint.decision {
        decision.decided_at = String::new();
    }
    checkpoint.nested = checkpoint
        .nested
        .map(|child| Box::new(without_timestamps(*child)));
    checkpoint
}

fn assert_matches_fixture(name: &str, actual: Checkpoint) {
    assert_eq!(
        without_timestamps(actual),
        without_timestamps(lifecycle_fixture(name)),
        "{name}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_durable_run_writes_what_the_shared_fixtures_hold() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    let line = line(&journal, &["A", "B", "C"]);

    crash(
        &line,
        &journal,
        &store,
        "thread_running_demo",
        "B",
        RunConfig::new(),
    )
    .await;
    assert_matches_fixture("running.json", store.latest("thread_running_demo"));

    line.run("thread_completed_demo", job(), store.clone())
        .await;
    assert_matches_fixture("completed.json", store.latest("thread_completed_demo"));

    crash(
        &with_subgraph(&journal),
        &journal,
        &store,
        "thread_nested_running_demo",
        "Work/Y",
        RunConfig::new(),
    )
    .await;
    assert_matches_fixture(
        "nested-running.json",
        store.latest("thread_nested_running_demo"),
    );

    let approval = approval(&journal);
    approval
        .run("thread_claimed_demo", job(), store.clone())
        .await;
    let process = Process::new(&store);
    journal.die_at("thread_claimed_demo", "B", &process);
    let died = approval
        .resume(
            "thread_claimed_demo",
            HumanSignal::approve("alice").with_override("limit", 5),
            process,
        )
        .await;
    assert_eq!(refused(died), DIED);
    assert_matches_fixture("claimed.json", store.latest("thread_claimed_demo"));
}

#[tokio::test(start_paused = true)]
async fn a_thread_the_shared_fixtures_hold_is_recovered() {
    let store = LeaseStore::new();
    let journal = Journal::new();
    for name in ["running.json", "nested-running.json", "claimed.json"] {
        store.save(lifecycle_fixture(name)).await.unwrap();
    }

    let line = line(&journal, &["A", "B", "C"]);
    assert_eq!(
        completed(line.recover("thread_running_demo", store.clone()).await).trail,
        "ABC"
    );
    let nested = with_subgraph(&journal);
    assert_eq!(
        completed(
            nested
                .recover("thread_nested_running_demo", store.clone())
                .await
        )
        .trail,
        "Before[XYZ]After"
    );
    let approval = approval(&journal);
    assert_eq!(
        completed(approval.recover("thread_claimed_demo", store.clone()).await),
        Job {
            trail: "ABC".to_string(),
            approved: false,
            limit: 5
        }
    );

    // The node in flight ran again with the key the fixture's hop count gives
    // it, and the claim's decision.
    let visits = journal.visits();
    assert!(visits.contains(&visit("B", "thread_running_demo/B#2", true)));
    assert!(visits.contains(&visit(
        "Work/Y",
        "thread_nested_running_demo/Work/Y#2.2",
        true
    )));
    assert!(visits.contains(&Visit {
        reviewer: Some("alice".to_string()),
        ..visit("B", "thread_claimed_demo/B#2", true)
    }));
    assert_eq!(journal.count("Work/X"), 0);
}

// --- Cost --------------------------------------------------------------------

#[tokio::test]
async fn fifteen_nodes_write_sixteen_rows_and_none_without_the_policy() {
    let names: Vec<String> = (1..=15).map(|i| format!("N{i}")).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let durable = chain(&Journal::new(), &names)
        .compile_with(durable())
        .unwrap();
    let plain = chain(&Journal::new(), &names).compile().unwrap();
    let store = LeaseStore::new();

    durable.run("durable", job(), store.clone()).await;
    plain.run("plain", job(), store.clone()).await;

    assert_eq!(store.log("durable").len(), 16);
    assert!(store.log("plain").is_empty());

    // Not asserted: a figure for the design doc's "Cost".
    const RUNS: usize = 200;
    async fn microseconds_per_node(
        graph: &CompiledGraph<Job>,
        store: &Arc<LeaseStore>,
        prefix: &str,
    ) -> f64 {
        let timer = Instant::now();
        for run in 0..RUNS {
            graph
                .run(format!("{prefix}-{run}"), job(), store.clone())
                .await;
        }
        timer.elapsed().as_secs_f64() * 1e6 / (RUNS * 15) as f64
    }
    microseconds_per_node(&durable, &store, "warm-durable").await;
    microseconds_per_node(&plain, &store, "warm-plain").await;
    println!(
        "in-memory store, 15 nodes: {:.1} us/node with Boundaries, {:.1} us/node with EveryNode",
        microseconds_per_node(&plain, &store, "p").await,
        microseconds_per_node(&durable, &store, "d").await
    );
}
