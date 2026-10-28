//! What an async node handler is told about its run ([`NodeContext`]), and
//! the run-level inputs that feed it ([`RunConfig`]) — `design/17-Async-Execution.md`
//! (SG-02, SG-03) and `design/18-Run-Observability.md` (SG-14, SG-19, SG-20).

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, LazyLock, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tracing::Span;

use crate::ExecutionContext;
use crate::checkpoint::{CancelReason, CheckpointStoreError, HOPS_KEY};
use crate::graph::NodeFailure;
use crate::history::HistoryStore;
use crate::lease::{LeaseOptions, RunLease};
use crate::observability::{RunMode, RunObserver, RunTelemetry, run_metadata};
use crate::scheduler::{AdmissionRequest, NotAdmitted, NotAdmittedReason, RunScheduler, RunSlot};
use crate::schema::{Checkpoint, HumanDecision};
use crate::store::BoxFuture;
use crate::streaming::StreamContext;

pub(crate) type Services = HashMap<TypeId, Arc<dyn Any + Send + Sync>>;

/// The service of type `T` in `services`, if any. Shared with
/// `ToolCallContext`, so the tool module stays free of `std::any`.
pub(crate) fn service_of<T: Send + Sync + 'static>(services: &Services) -> Option<Arc<T>> {
    services
        .get(&TypeId::of::<T>())
        .cloned()
        .and_then(|service| service.downcast::<T>().ok())
}

/// Adds `service` to `services`, replacing any of the same type.
pub(crate) fn insert_service<T: Send + Sync + 'static>(services: &mut Services, service: Arc<T>) {
    services.insert(TypeId::of::<T>(), service);
}

pub(crate) static NO_METADATA: LazyLock<Arc<HashMap<String, String>>> =
    LazyLock::new(|| Arc::new(HashMap::new()));

/// What a run with no services shares with its nodes.
static NO_SERVICES: LazyLock<Arc<Services>> = LazyLock::new(|| Arc::new(HashMap::new()));

/// The hop chain of the outermost level: nothing.
static NO_HOPS: LazyLock<Arc<str>> = LazyLock::new(|| Arc::from(""));

/// Run-level inputs to `run_with` / `resume_with` / `run_streaming_with` /
/// `resume_streaming_with`, so new ones don't keep lengthening those methods'
/// parameter lists. Mirrors the .NET engine's `RunOptions` (which keeps the
/// cancellation token as a separate parameter, as is usual there).
#[derive(Clone, Default)]
pub struct RunConfig {
    cancellation: Option<CancellationToken>,
    deadline: Option<Instant>,
    history: Option<Arc<dyn HistoryStore>>,
    services: Services,
    metadata: HashMap<String, String>,
    run_id: Option<String>,
    observer: Option<Arc<dyn RunObserver>>,
    max_node_hops: Option<u32>,
    lease: Option<LeaseOptions>,
    scheduler: Option<RunScheduler>,
    priority: i32,
    /// How long the run waited for its scheduler's slot, once it has one.
    pub(crate) queued_for: Option<Duration>,
}

impl RunConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Cancelling `token` reaches every node, branch and subgraph at any depth
    /// and ends the run `Cancelled` with a `cancelled` checkpoint at the node
    /// that was running, which a resume re-enters.
    pub fn with_cancellation(mut self, token: CancellationToken) -> Self {
        self.cancellation = Some(token);
        self
    }

    /// When the run must stop. Once it passes, the run is cancelled exactly as
    /// if its token had been, with [`CancelReason::Deadline`]. Absolute, so one
    /// deadline can cover a run, its freeze and its resume.
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Handed to every async handler as [`NodeContext::history`], at every depth.
    pub fn with_history(mut self, history: Arc<dyn HistoryStore>) -> Self {
        self.history = Some(history);
        self
    }

    /// Makes `service` available to every async handler as
    /// [`NodeContext::service`]`::<T>()`, at every depth. One service per type;
    /// adding another of the same type replaces it.
    pub fn with_service<T: Send + Sync + 'static>(mut self, service: Arc<T>) -> Self {
        insert_service(&mut self.services, service);
        self
    }

    /// Caller metadata (`tenant`, `incident`, …), stamped on every checkpoint
    /// the run writes, at every nesting level, and handed to every async
    /// handler as [`NodeContext::metadata`]. Setting a key again replaces its
    /// value. A resume carries forward the metadata of the checkpoint it
    /// resumes, with this laid over it key by key.
    ///
    /// The checkpoint's `metadata` map also holds the engine's keys, so
    /// `error`, `error_kind`, `attempts`, `cancel_reason`, every key starting
    /// `strata.`, and an empty key are refused: the run returns
    /// `GraphExecutionResult::Failed` ("run metadata key '<key>' is reserved")
    /// before it starts.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// The id the run's events, spans and [`NodeContext::run_id`] carry; a
    /// fresh UUID v7 per call when not set. The engine doesn't check that it
    /// is unique. An empty id is refused like a reserved metadata key.
    pub fn with_run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    /// Receives the run's events ([`crate::observability::RunEvent`]), in every
    /// run mode. It can't stall or fail the run: see [`RunObserver`].
    pub fn with_observer(mut self, observer: Arc<dyn RunObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Replaces the graph's compiled
    /// [`CompileOptions::with_max_node_hops`](crate::graph::CompileOptions::with_max_node_hops)
    /// for this run or resume, higher or lower, or sets one on a graph compiled
    /// without (`design/24-Loop-Circuit-Breaker.md`). It applies to the
    /// outermost graph only: a subgraph's child keeps its own. It isn't
    /// stored, so the next resume uses the compiled limit unless it passes
    /// this again. 0 is refused like a reserved metadata key.
    pub fn with_max_node_hops(mut self, limit: u32) -> Self {
        self.max_node_hops = Some(limit);
        self
    }

    /// Runs the thread under a lease (`design/32-Thread-Leases.md`): the run,
    /// resume, signal, wake or end acquires the thread's lease from the store
    /// before its first save, renews it on tokio's clock, saves under it, and
    /// releases it when it returns. A thread another owner holds is refused
    /// with `Failed`; losing the lease ends the run `Cancelled` with
    /// [`CancelReason::LeaseLost`], with nothing more written. A blank owner
    /// or a ttl below one millisecond is refused like a reserved metadata
    /// key, and a store without leases refuses the run when it acquires.
    pub fn with_lease(mut self, lease: LeaseOptions) -> Self {
        self.lease = Some(lease);
        self
    }

    /// Waits for a slot of this scheduler before the run loads or writes
    /// anything, and holds it until the run returns
    /// (`design/40-Run-Scheduler.md`). A run it doesn't admit returns
    /// `Failed(StrataError::NotAdmitted { .. })` with nothing loaded or
    /// written. The run's tenant is its metadata's
    /// [`SchedulerOptions::with_tenant_key`](crate::scheduler::SchedulerOptions::with_tenant_key)
    /// value. Ending a thread takes no slot. Without one, the run starts at
    /// once, as before.
    pub fn with_scheduler(mut self, scheduler: RunScheduler) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    /// The run's priority for its scheduler: higher first. 0 by default; any
    /// value, negative too.
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// The run's scheduler, if it has one.
    pub fn scheduler(&self) -> Option<&RunScheduler> {
        self.scheduler.as_ref()
    }

    /// The run's priority for its scheduler.
    pub fn priority(&self) -> i32 {
        self.priority
    }

    /// Waits for a slot of the run's scheduler, before the run loads or writes
    /// anything: `None` when it has none. The tenant is the run's metadata's,
    /// laid over `listed` (the metadata a driver listed the thread with).
    /// `stop`, a driver's, ends the wait like the caller's token. Records how
    /// long the run waited, for its telemetry.
    pub(crate) async fn admit(
        &mut self,
        listed: Option<&HashMap<String, String>>,
        stop: Option<&CancellationToken>,
    ) -> Result<Option<RunSlot>, NotAdmitted> {
        let Some(scheduler) = &self.scheduler else {
            return Ok(None);
        };
        let key = scheduler.options().tenant_key();
        let mut request = AdmissionRequest::new().with_priority(self.priority);
        if let Some(tenant) = self
            .metadata
            .get(key)
            .or_else(|| listed.and_then(|listed| listed.get(key)))
        {
            request = request.with_tenant(tenant.clone());
        }
        if let Some(deadline) = self.deadline {
            request = request.with_deadline(deadline);
        }
        if let Some(token) = &self.cancellation {
            request = request.with_cancellation(token.clone());
        }
        let started = tokio::time::Instant::now();
        let admission = scheduler.admit(request);
        let slot = match stop {
            None => admission.await?,
            Some(stop) => tokio::select! {
                biased;
                decided = admission => decided?,
                // Dropping the wait withdraws the run.
                () = stop.cancelled() => {
                    return Err(NotAdmitted {
                        reason: NotAdmittedReason::Cancelled,
                        waited: started.elapsed(),
                    });
                }
            },
        };
        self.queued_for = Some(slot.waited());
        Ok(Some(slot))
    }

    /// The caller's cancellation token, if any.
    pub(crate) fn cancellation(&self) -> Option<&CancellationToken> {
        self.cancellation.as_ref()
    }

    /// Whether the run takes a lease on its thread ([`Self::with_lease`]).
    pub fn has_lease(&self) -> bool {
        self.lease.is_some()
    }

    /// What refuses the run before it starts, if anything.
    pub(crate) fn validate(&self) -> Result<(), String> {
        run_metadata::validate(&self.metadata, self.run_id.as_deref())?;
        if self.max_node_hops == Some(0) {
            return Err("a run's max_node_hops must be at least 1".to_string());
        }
        if let Some(lease) = &self.lease {
            lease.validate()?;
        }
        Ok(())
    }
}

/// What every node of a run shares: the caller's thread id and run-level inputs.
#[derive(Clone)]
struct Shared {
    thread_id: Arc<str>,
    history: Option<Arc<dyn HistoryStore>>,
    services: Arc<Services>,
}

/// What an async node handler is told about the run it is part of
/// (`design/17-Async-Execution.md`). Mirrors the .NET engine's `NodeContext`.
///
/// Fields are private behind accessors, so new ones can be added without
/// breaking anyone. A handler can be unit-tested with a context built by hand:
/// [`Self::new`] and the `with_*` methods.
#[derive(Clone)]
pub struct NodeContext {
    shared: Arc<Shared>,
    node: Arc<str>,
    path: Arc<str>,
    attempt: u32,
    stream: Option<StreamContext>,
    /// The node's token, made the first time it is asked for when nothing
    /// but the level's token can cancel it: a child of `level_token`. A copy
    /// of the context made before then makes a token of its own.
    cancellation: OnceLock<CancellationToken>,
    /// The token of the level the node runs at. `None` in a hand-built
    /// context and in one whose own token was made up front.
    level_token: Option<Arc<CancellationToken>>,
    run_id: Arc<str>,
    metadata: Arc<HashMap<String, String>>,
    span: Span,
    /// The answer to the most recent interrupt boundary of this node's level
    /// (`design/21-Typed-Human-Signals.md`), if any.
    decision: Option<Arc<HumanDecision>>,
    /// Set only for a fallback node: why the node it falls back from failed
    /// (`design/25-Retry-Backoff.md`).
    last_failure: Option<Arc<NodeFailure>>,
    /// The run's hook layer, for the tool calls a handler makes
    /// (`ToolCallContext::from`). Weak, so a context kept past its run doesn't
    /// keep the run's telemetry alive. `None` in a hand-built context.
    telemetry: Option<Weak<RunTelemetry>>,
    /// Names this execution of this node (`design/33-Durable-Execution.md`).
    /// Made the first time it is asked for, from `hops`: most handlers never
    /// ask (`design/38-Rust-Executor-Overhead.md`).
    idempotency_key: OnceLock<Arc<str>>,
    /// The hop numbers the key ends with. `None` in a hand-built context,
    /// whose key is what [`Self::with_idempotency_key`] set, or empty.
    hops: Option<KeyHops>,
    /// Whether this node may have run before, in a process that died.
    is_recovery: bool,
}

/// What follows a node's path in its idempotency key: the hop numbers from
/// the outermost level in (`chain` is the enclosing levels', each followed by
/// a dot), and whether the run id follows them, as it does in every run that
/// isn't durable.
#[derive(Clone)]
struct KeyHops {
    chain: Arc<str>,
    hop: u32,
    with_run_id: bool,
}

impl NodeContext {
    /// A context for unit-testing a handler: `node` at the top level of thread
    /// `thread_id`, attempt 1, no stream, history, services, metadata, run id
    /// or span, and a token nobody cancels.
    pub fn new(thread_id: impl Into<Arc<str>>, node: impl Into<Arc<str>>) -> Self {
        let node = node.into();
        Self {
            shared: Arc::new(Shared {
                thread_id: thread_id.into(),
                history: None,
                services: Arc::new(HashMap::new()),
            }),
            path: node.clone(),
            node,
            attempt: 1,
            stream: None,
            cancellation: OnceLock::new(),
            level_token: None,
            run_id: Arc::from(""),
            metadata: NO_METADATA.clone(),
            span: Span::none(),
            decision: None,
            last_failure: None,
            telemetry: None,
            idempotency_key: OnceLock::new(),
            hops: None,
            is_recovery: false,
        }
    }

    /// The thread id the caller passed to the outermost `run` / `resume`, at
    /// every depth. Inside a subgraph it is not the child's own thread id (the
    /// subgraph node's name, which only labels nested checkpoints).
    pub fn thread_id(&self) -> &str {
        &self.shared.thread_id
    }

    /// The node's own name: `"Draft"`, or a branch's name inside a parallel group.
    pub fn node(&self) -> &str {
        &self.node
    }

    /// The node's qualified name, the same one streaming chunks carry:
    /// `"<subgraph>/<node>"` to any depth, `"<group>/<branch>"` for a branch.
    pub fn node_path(&self) -> &str {
        &self.path
    }

    /// 1-based, counting this node's retries under its recovery policy. A
    /// branch reports its group's attempt. A node inside a subgraph counts its
    /// own retries only.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// The node's stream context whenever the run is streaming, `None`
    /// otherwise. Always `Some` in a streaming node or a streaming branch,
    /// since a plain run refuses those before running them. Use the async
    /// emits: the `_blocking` ones panic on a runtime worker thread.
    pub fn stream(&self) -> Option<&StreamContext> {
        self.stream.as_ref()
    }

    /// What [`RunConfig::with_history`] supplied, if anything. The store is
    /// synchronous; call it through `tokio::task::spawn_blocking` from an async
    /// handler (the Postgres adapter panics if called on a runtime thread).
    pub fn history(&self) -> Option<Arc<dyn HistoryStore>> {
        self.shared.history.clone()
    }

    /// The service of type `T` that [`RunConfig::with_service`] supplied, if any.
    pub fn service<T: Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        service_of(&self.shared.services)
    }

    /// The node's token: cancelled when the run is cancelled or its deadline
    /// passes, when the node times out, or (for a branch) when a sibling fails.
    /// The engine stops waiting for the handler at that moment and drops its
    /// future; pass the token on to work the handler spawned itself.
    pub fn cancellation(&self) -> &CancellationToken {
        self.cancellation.get_or_init(|| match &self.level_token {
            Some(level) => level.child_token(),
            None => CancellationToken::new(),
        })
    }

    /// The id of the run this node is part of, at every depth: what
    /// [`RunConfig::with_run_id`] supplied, or the one the engine generated. A
    /// resume is a new run with a new id.
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// The run's metadata ([`RunConfig::with_metadata`], merged with a resumed
    /// checkpoint's), at every depth. Empty when there is none.
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    /// The span this handler runs inside: its attempt's, or for a branch the
    /// branch's. It is also the current span while the handler runs.
    /// [`Span::none`] in a hand-built context.
    pub fn span(&self) -> &Span {
        &self.span
    }

    /// The answer to the most recent interrupt boundary of this node's graph
    /// level (`design/21-Typed-Human-Signals.md`): set from the resume that
    /// answered it on, for every node and branch of the level, until another
    /// boundary of the level is answered. Kept across a failed or cancelled
    /// thread's resume, which doesn't replace it. `None` when no boundary of
    /// the level has been answered; a subgraph's child doesn't inherit its
    /// parent's.
    pub fn decision(&self) -> Option<&HumanDecision> {
        self.decision.as_deref()
    }

    /// Set only in a fallback node's context, on every attempt of it: why the
    /// node whose recovery policy routed here failed
    /// (`design/25-Retry-Backoff.md`). A parallel group used as a fallback
    /// hands it to its branches. `None` everywhere else, including the node
    /// after the fallback, and in a fallback re-entered by a resume.
    pub fn last_failure(&self) -> Option<&NodeFailure> {
        self.last_failure.as_deref()
    }

    /// Names this execution of this node in this thread
    /// (`design/33-Durable-Execution.md`, "The crash contract"): the thread
    /// id, the node's path, and its hop number at every level, as
    /// `inc-42/Remediate/Execute#3.2`. The same on every retry of the node,
    /// and another when the thread reaches the node again. Pass it to whatever
    /// the node changes outside its state, so doing it twice does it once.
    ///
    /// In a graph compiled with `CheckpointPolicy::EveryNode` it is also the
    /// same when the node runs again in a recovery ([`Self::is_recovery`]). In
    /// any other run it ends with `@` and the run id, so a later resume never
    /// repeats one. A branch of a parallel group has its own, under its
    /// group's hop. Empty in a hand-built context, unless set.
    pub fn idempotency_key(&self) -> &str {
        self.idempotency_key.get_or_init(|| match &self.hops {
            Some(hops) => {
                let (thread_id, path) = (&self.shared.thread_id, &self.path);
                let (chain, hop) = (&hops.chain, hops.hop);
                Arc::from(if hops.with_run_id {
                    format!("{thread_id}/{path}#{chain}{hop}@{}", self.run_id)
                } else {
                    format!("{thread_id}/{path}#{chain}{hop}")
                })
            }
            None => Arc::from(""),
        })
    }

    /// Whether this node may have run before, in a process that died
    /// (`design/33-Durable-Execution.md`): it is the node a recovery
    /// (`CompiledGraph::recover`) starts with at its level, or its fallback,
    /// or inside a parallel group or a map that a recovery runs again. On
    /// every attempt. It doesn't say the node ran, only that it may have: what
    /// it did then may already be done. `false` once a node of the level has
    /// committed, and in every run that isn't a recovery.
    pub fn is_recovery(&self) -> bool {
        self.is_recovery
    }

    /// An [`ExecutionContext`] (thread id, history and the run's metadata) for
    /// code that takes one; `None` if the run has no history store.
    pub fn execution_context(&self) -> Option<ExecutionContext> {
        self.history().map(|history| {
            let mut context = ExecutionContext::new(self.thread_id(), history);
            context.metadata = (*self.metadata).clone();
            context
        })
    }

    pub(crate) fn metadata_arc(&self) -> Arc<HashMap<String, String>> {
        self.metadata.clone()
    }

    pub(crate) fn services_arc(&self) -> Arc<Services> {
        self.shared.services.clone()
    }

    pub(crate) fn telemetry(&self) -> Option<Weak<RunTelemetry>> {
        self.telemetry.clone()
    }

    pub fn with_node_path(mut self, path: impl Into<Arc<str>>) -> Self {
        // The key names the node the context was made for.
        self.idempotency_key();
        self.path = path.into();
        self
    }

    pub fn with_attempt(mut self, attempt: u32) -> Self {
        self.attempt = attempt;
        self
    }

    pub fn with_stream(mut self, stream: StreamContext) -> Self {
        self.stream = Some(stream);
        self
    }

    pub fn with_history(mut self, history: Arc<dyn HistoryStore>) -> Self {
        Arc::make_mut(&mut self.shared).history = Some(history);
        self
    }

    pub fn with_service<T: Send + Sync + 'static>(mut self, service: Arc<T>) -> Self {
        let shared = Arc::make_mut(&mut self.shared);
        let mut services = (*shared.services).clone();
        insert_service(&mut services, service);
        shared.services = Arc::new(services);
        self
    }

    pub fn with_cancellation(mut self, token: CancellationToken) -> Self {
        self.cancellation = OnceLock::from(token);
        self
    }

    pub fn with_run_id(mut self, run_id: impl Into<Arc<str>>) -> Self {
        // The key names the run the context was made in.
        self.idempotency_key();
        self.run_id = run_id.into();
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        Arc::make_mut(&mut self.metadata).insert(key.into(), value.into());
        self
    }

    pub fn with_span(mut self, span: Span) -> Self {
        self.span = span;
        self
    }

    pub fn with_decision(mut self, decision: HumanDecision) -> Self {
        self.decision = Some(Arc::new(decision));
        self
    }

    pub fn with_last_failure(mut self, failure: NodeFailure) -> Self {
        self.last_failure = Some(Arc::new(failure));
        self
    }

    pub fn with_idempotency_key(mut self, key: impl Into<Arc<str>>) -> Self {
        self.idempotency_key = OnceLock::from(key.into());
        self
    }

    pub fn with_recovery(mut self, is_recovery: bool) -> Self {
        self.is_recovery = is_recovery;
        self
    }

    /// The context of a branch named `branch` of the group this context is
    /// for, at `path` (`"<this context's path>/<branch>"`), running inside
    /// `span`.
    pub(crate) fn for_branch(
        &self,
        branch: &str,
        path: Arc<str>,
        stream: Option<StreamContext>,
        cancellation: CancellationToken,
        span: Span,
    ) -> Self {
        Self {
            shared: self.shared.clone(),
            node: Arc::from(branch),
            path,
            attempt: self.attempt,
            stream,
            cancellation: OnceLock::from(cancellation),
            level_token: None,
            run_id: self.run_id.clone(),
            metadata: self.metadata.clone(),
            span,
            decision: self.decision.clone(),
            last_failure: self.last_failure.clone(),
            telemetry: self.telemetry.clone(),
            // The branch's own key, at its group's hop: made of its own path.
            idempotency_key: OnceLock::new(),
            hops: self.hops.clone(),
            is_recovery: self.is_recovery,
        }
    }
}

/// Takes the `running` checkpoint of a subgraph's child, each time a node of
/// the child commits, to nest it in the parent's and save that
/// (`design/33-Durable-Execution.md`, "Subgraphs, groups and maps"). `Err` is
/// the store's, which ends the child's run as it would end the parent's.
pub(crate) type Progress =
    Arc<dyn Fn(Checkpoint) -> BoxFuture<'static, Result<(), CheckpointStoreError>> + Send + Sync>;

/// One run's root: the caller's thread id and inputs, the token that the
/// caller's token, the deadline or the loss of the run's lease cancels, and
/// the run's telemetry
/// (`design/18-Run-Observability.md`), which also holds its id and metadata.
pub(crate) struct RunScope {
    shared: Arc<Shared>,
    pub(crate) token: CancellationToken,
    caller: Option<CancellationToken>,
    pub(crate) telemetry: Arc<RunTelemetry>,
    /// The run's replacement for the outermost graph's compiled hop limit, if any.
    pub(crate) max_node_hops: Option<u32>,
    /// Set when the run wrote nothing because its thread changed after it was
    /// loaded: a resume whose conditional stamp lost, or a timeout's end that
    /// did (`design/29-Event-Waits-And-Timers.md`, "Waking"). The wake driver
    /// then skips the thread instead of reporting it woken.
    pub(crate) superseded: AtomicBool,
    /// The run's lease on its thread, when it runs under one
    /// (`design/32-Thread-Leases.md`).
    pub(crate) lease: Option<RunLease>,
    /// Set when the run was refused its thread's lease, to who holds it:
    /// `held by 'worker-2' until …`. The wake driver then skips the thread
    /// instead of reporting it woken.
    held: Mutex<Option<String>>,
    /// Whether the run checkpoints every node
    /// (`design/33-Durable-Execution.md`): its outermost graph was compiled
    /// with `CheckpointPolicy::EveryNode`.
    pub(crate) durable: bool,
    /// The `running` checkpoint this run last saved, while it is still the
    /// last thing it saved: what the thread's log ends with unless someone
    /// else wrote. A run that ends with one set has left its thread `running`,
    /// and marks it failed first.
    last_running: Mutex<Option<Checkpoint>>,
}

impl RunScope {
    /// Who holds the thread this run was refused, if it was.
    pub(crate) fn held(&self) -> Option<String> {
        self.held
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    pub(crate) fn set_held(&self, held: String) {
        *self
            .held
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(held);
    }

    /// Notes what the run just saved at the outermost level: a `running`
    /// checkpoint is what its thread's log now ends with, and anything else
    /// means the thread is no longer running.
    pub(crate) fn saved(&self, running: Option<Checkpoint>) {
        *self
            .last_running
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = running;
    }

    /// The `running` checkpoint the thread's log still ends with, if the run
    /// left one.
    pub(crate) fn last_running(&self) -> Option<Checkpoint> {
        self.last_running
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    pub(crate) fn reason(&self) -> CancelReason {
        if self.lease.as_ref().is_some_and(RunLease::is_lost) {
            CancelReason::LeaseLost
        } else if self
            .caller
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            CancelReason::Requested
        } else {
            CancelReason::Deadline
        }
    }
}

/// One graph level of a run: the outermost graph, or a subgraph's child.
/// `token` is the level's root (the run's, or the subgraph node's attempt
/// token), and `prefix` qualifies its node names (`None` at the top, `"Work"`
/// inside the subgraph node `Work`). `decision` is the answer to the level's
/// most recent interrupt boundary, if any (`design/21-Typed-Human-Signals.md`):
/// every node of the level sees it, and every checkpoint the level writes
/// carries it. `hops` is set while the level has a hop limit in effect
/// (`design/24-Loop-Circuit-Breaker.md`): the hops it made before the node a
/// checkpoint written now would resume at, which every such checkpoint records
/// as `strata.hops`. `last_failure` is set only for a fallback node, and says
/// why the node it falls back from failed (`design/25-Retry-Backoff.md`). A
/// run that checkpoints every node (`design/33-Durable-Execution.md`) counts
/// hops at every level, limit or no limit.
#[derive(Clone)]
pub(crate) struct LevelScope {
    /// What every node of the level shares. Behind one `Arc`, so the copy the
    /// executor makes for each node is a reference count and a few integers,
    /// not a token and a handful of `Arc`s
    /// (`design/38-Rust-Executor-Overhead.md`).
    core: Arc<LevelCore>,
    pub(crate) hops: Option<u32>,
    last_failure: Option<Arc<NodeFailure>>,
    /// The hop number of the node this level is running, from 1: set per node
    /// by the executor.
    hop: u32,
    /// Whether the node this level is running may have run before its process
    /// died ([`NodeContext::is_recovery`]): set by a recovery for the first
    /// node of each level it re-enters, and kept until a node of the level
    /// commits.
    pub(crate) recovering: bool,
}

/// The part of a [`LevelScope`] that is the same for every node of the level.
#[derive(Clone)]
pub(crate) struct LevelCore {
    pub(crate) run: Arc<RunScope>,
    /// Shared, so a node's context can hold on to it without the lock a
    /// token takes to be cloned.
    pub(crate) token: Arc<CancellationToken>,
    prefix: Option<Arc<str>>,
    decision: Option<Arc<HumanDecision>>,
    /// The hop numbers of the nodes this level is inside, each followed by a
    /// dot: empty at the top, `"3."` inside the outermost graph's third node.
    hop_chain: Arc<str>,
    /// Where a subgraph's child hands each `running` checkpoint of its own,
    /// for its parent to nest and save. `None` at the outermost level, which
    /// saves to the store, and for a map's item, whose progress isn't kept.
    pub(crate) progress: Option<Progress>,
    /// What [`LevelScope::child`] gives the child as `progress`: set while a
    /// subgraph node runs.
    child_progress: Option<Progress>,
}

impl std::ops::Deref for LevelScope {
    type Target = LevelCore;

    fn deref(&self) -> &LevelCore {
        &self.core
    }
}

impl LevelScope {
    /// [`Self::path_of`] for a name already shared: at the outermost level a
    /// node's path is its name, and this copies nothing.
    pub(crate) fn path_arc(&self, node: &Arc<str>) -> Arc<str> {
        match &self.prefix {
            Some(prefix) => Arc::from(format!("{prefix}/{node}")),
            None => node.clone(),
        }
    }

    pub(crate) fn path_of(&self, node: &str) -> String {
        match &self.prefix {
            Some(prefix) => format!("{prefix}/{node}"),
            None => node.to_string(),
        }
    }

    /// Whether this is the outermost graph of the run, not a subgraph's child:
    /// the only level that writes to the caller's store.
    pub(crate) fn is_root(&self) -> bool {
        self.prefix.is_none()
    }

    /// The `cancel_reason` a checkpoint at this level records: the run's
    /// reason, or `"timeout"` when the run is still going and it was the
    /// enclosing subgraph node's timeout that stopped this level.
    pub(crate) fn wire_reason(&self) -> &'static str {
        if !self.run.token.is_cancelled() {
            "timeout"
        } else {
            match self.run.reason() {
                CancelReason::Requested => "requested",
                CancelReason::Deadline => "deadline",
                CancelReason::Ended => "ended",
                // Only ever on a child's level, nested in a checkpoint that
                // is never saved.
                CancelReason::LeaseLost => "lease_lost",
            }
        }
    }

    /// The level a subgraph node's child (or a map's item) runs at: rooted at
    /// the node's attempt token, named under its path, with no decision until
    /// the child answers a boundary of its own, counting its own hops under
    /// this node's, and with no failure to report. It hands its progress to
    /// what [`Self::with_child_progress`] set, and its first node may have run
    /// before if this node may have.
    pub(crate) fn child(&self, token: CancellationToken, node_path: &str) -> Self {
        Self {
            core: Arc::new(LevelCore {
                run: self.run.clone(),
                token: Arc::new(token),
                prefix: Some(Arc::from(node_path)),
                decision: None,
                hop_chain: Arc::from(format!("{}{}.", self.hop_chain, self.hop)),
                progress: self.child_progress.clone(),
                child_progress: None,
            }),
            hops: None,
            last_failure: None,
            hop: 0,
            recovering: self.recovering,
        }
    }

    /// This level with its shared part changed by `change`.
    fn with_core(&self, change: impl FnOnce(&mut LevelCore)) -> Self {
        let mut core = (*self.core).clone();
        change(&mut core);
        Self {
            core: Arc::new(core),
            ..self.clone()
        }
    }

    /// Whether this level writes a checkpoint as each node commits: the run is
    /// durable, and the level is the outermost or a subgraph's child whose
    /// parent does.
    pub(crate) fn checkpoints(&self) -> bool {
        self.run.durable && (self.is_root() || self.progress.is_some())
    }

    /// This level running its node number `hop`, which may have run before if
    /// `recovering`, and writing `hops` as `strata.hops` (`None`: writing
    /// none).
    pub(crate) fn at_hop(&self, hops: Option<u32>, hop: u32, recovering: bool) -> Self {
        Self {
            hops,
            hop,
            recovering,
            ..self.clone()
        }
    }

    /// This level running a subgraph node whose child hands its progress to
    /// `sink`.
    pub(crate) fn with_child_progress(&self, sink: Progress) -> Self {
        self.with_core(|core| core.child_progress = Some(sink))
    }

    /// This level continued by a recovery: its first node may have run before.
    pub(crate) fn recovering(&self) -> Self {
        Self {
            recovering: true,
            ..self.clone()
        }
    }

    /// What follows a path in the idempotency key of a handler running at this
    /// level's current hop ([`NodeContext::idempotency_key`]): the hop numbers
    /// from the outermost level in. A durable run stores its hop counts, so a
    /// recovery repeats the key; any other run adds its run id, so a later
    /// resume, which counts from wherever its checkpoint says, can't repeat
    /// one.
    fn key_hops(&self) -> KeyHops {
        KeyHops {
            chain: self.hop_chain.clone(),
            hop: self.hop,
            with_run_id: !self.run.durable,
        }
    }

    /// This level running a fallback node, which `failure` explains.
    pub(crate) fn with_last_failure(&self, failure: NodeFailure) -> Self {
        Self {
            last_failure: Some(Arc::new(failure)),
            ..self.clone()
        }
    }

    /// This level writing `hops` as `strata.hops` on its checkpoints (`None`:
    /// writing none).
    pub(crate) fn with_hops(&self, hops: Option<u32>) -> Self {
        Self {
            hops,
            ..self.clone()
        }
    }

    /// This level running under `decision`.
    pub(crate) fn with_decision(&self, decision: Option<HumanDecision>) -> Self {
        self.with_core(|core| core.decision = decision.map(Arc::new))
    }

    /// The decision every checkpoint this level writes carries.
    pub(crate) fn decision(&self) -> Option<HumanDecision> {
        self.decision.as_deref().cloned()
    }

    /// What a checkpoint at this level holds in `metadata`: the run's keys,
    /// `engine`'s, and the hop count when the level has a limit.
    pub(crate) fn checkpoint_metadata(
        &self,
        engine: Option<HashMap<String, String>>,
    ) -> Option<HashMap<String, String>> {
        let engine = match self.hops {
            Some(hops) => {
                let mut engine = engine.unwrap_or_default();
                engine.insert(HOPS_KEY.to_string(), hops.to_string());
                Some(engine)
            }
            None => engine,
        };
        self.run.telemetry.checkpoint_metadata(engine)
    }

    /// The context of the node `node`, at `path` in the run
    /// ([`Self::path_arc`]), for its attempt number `attempt`. `cancellation`
    /// is the attempt's own token when it has one (a node with a timeout, or
    /// one that hands its token on to branches or a child); without one the
    /// context makes a child of the level's token if its handler asks
    /// (`design/38-Rust-Executor-Overhead.md`).
    pub(crate) fn context_for(
        &self,
        node: &Arc<str>,
        path: &Arc<str>,
        attempt: u32,
        stream: Option<StreamContext>,
        cancellation: Option<CancellationToken>,
        span: Span,
    ) -> NodeContext {
        NodeContext {
            shared: self.run.shared.clone(),
            node: node.clone(),
            idempotency_key: OnceLock::new(),
            hops: Some(self.key_hops()),
            is_recovery: self.recovering,
            path: path.clone(),
            attempt,
            stream,
            level_token: cancellation.is_none().then(|| self.token.clone()),
            cancellation: cancellation.map(OnceLock::from).unwrap_or_default(),
            run_id: self.run.telemetry.run_id.clone(),
            metadata: self.run.telemetry.metadata(),
            span,
            decision: self.decision.clone(),
            last_failure: self.last_failure.clone(),
            telemetry: Some(Arc::downgrade(&self.run.telemetry)),
        }
    }
}

/// The root of one run's token tree: the caller's token, plus the deadline if
/// there is one. Dropping it stops the deadline's timer.
pub(crate) struct RootScope {
    pub(crate) level: LevelScope,
    timer: Option<tokio::task::JoinHandle<()>>,
}

impl RootScope {
    /// Starts a run: its token tree, and its telemetry, whose span is a child
    /// of whatever is current. The caller has validated `config`
    /// ([`RunConfig::validate`]). `durable` is whether the outermost graph
    /// checkpoints every node (`design/33-Durable-Execution.md`).
    pub(crate) fn start(thread_id: &str, config: RunConfig, mode: RunMode, durable: bool) -> Self {
        let token = match &config.cancellation {
            Some(caller) => caller.child_token(),
            None => CancellationToken::new(),
        };
        let timer = match config.deadline {
            Some(deadline) if deadline <= Instant::now() => {
                token.cancel();
                None
            }
            Some(deadline) => {
                let token = token.clone();
                Some(tokio::spawn(async move {
                    tokio::time::sleep_until(deadline.into()).await;
                    token.cancel();
                }))
            }
            None => None,
        };
        let max_node_hops = config.max_node_hops;
        let thread_id: Arc<str> = Arc::from(thread_id);
        let telemetry = Arc::new(RunTelemetry::start(
            thread_id.clone(),
            config.run_id,
            mode,
            config.metadata,
            config.observer,
            config.queued_for.map(|waited| (waited, config.priority)),
        ));
        let lease = config
            .lease
            .map(|lease| RunLease::new(lease, token.clone(), telemetry.clone()));
        let run = Arc::new(RunScope {
            shared: Arc::new(Shared {
                thread_id,
                history: config.history,
                services: if config.services.is_empty() {
                    NO_SERVICES.clone()
                } else {
                    Arc::new(config.services)
                },
            }),
            token: token.clone(),
            caller: config.cancellation,
            telemetry,
            max_node_hops,
            superseded: AtomicBool::new(false),
            lease,
            held: Mutex::new(None),
            durable,
            last_running: Mutex::new(None),
        });
        Self {
            level: LevelScope {
                core: Arc::new(LevelCore {
                    run,
                    token: Arc::new(token),
                    prefix: None,
                    decision: None,
                    hop_chain: NO_HOPS.clone(),
                    progress: None,
                    child_progress: None,
                }),
                hops: None,
                last_failure: None,
                hop: 0,
                recovering: false,
            },
            timer,
        }
    }

    pub(crate) fn telemetry(&self) -> &Arc<RunTelemetry> {
        &self.level.run.telemetry
    }
}

impl Drop for RootScope {
    fn drop(&mut self) {
        if let Some(timer) = self.timer.take() {
            timer.abort();
        }
        // A run dropped before it returned can't release its lease: it stops
        // renewing it, and the lease expires by itself.
        if let Some(lease) = &self.level.run.lease {
            lease.stop();
        }
    }
}
