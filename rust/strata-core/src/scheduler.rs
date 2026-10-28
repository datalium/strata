//! The run scheduler (`design/40-Run-Scheduler.md`): admits runs to a fixed
//! number of slots, higher priorities first and in order of arrival within one,
//! with slots kept for higher priorities, at most so many runs of one tenant at
//! once, and a bounded queue that sheds its lowest-ranked run for a newcomer
//! that outranks it.
//!
//! A run gets one through [`crate::context::RunConfig::with_scheduler`];
//! [`RunScheduler::admit`] is public so an application can schedule work of its
//! own beside its runs. Mirrors the .NET engine's `Datalium.Strata.Scheduling`.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::StrataError;
use crate::observability::metric_names;

/// Why a run got no slot (`design/40-Run-Scheduler.md`, "Not admitted").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NotAdmittedReason {
    /// The queue (or the tenant's share of it) was full, and the run outranked
    /// nobody waiting.
    QueueFull,
    /// The run was waiting when a run that outranks it arrived at a full queue,
    /// and left to make room.
    Shed,
    /// The run waited the scheduler's longest queue time.
    QueueTimeout,
    /// The run's token was cancelled while it waited (or a driver's stop token
    /// fired, or the waiting future was dropped).
    Cancelled,
    /// The run's deadline passed while it waited.
    Deadline,
    /// The scheduler was closed ([`RunScheduler::close`]).
    Closed,
}

impl NotAdmittedReason {
    /// The reason's `strata.scheduler.outcome`: `queue_full`, `shed`,
    /// `queue_timeout`, `cancelled`, `deadline` or `closed`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::QueueFull => "queue_full",
            Self::Shed => "shed",
            Self::QueueTimeout => "queue_timeout",
            Self::Cancelled => "cancelled",
            Self::Deadline => "deadline",
            Self::Closed => "closed",
        }
    }

    fn explanation(self) -> &'static str {
        match self {
            Self::QueueFull => "the scheduler's queue is full",
            Self::Shed => "shed from the scheduler's queue for a higher-priority run",
            Self::QueueTimeout => "no slot within the scheduler's longest wait",
            Self::Cancelled => "cancelled while waiting for a slot",
            Self::Deadline => "its deadline passed while it waited for a slot",
            Self::Closed => "the scheduler is closed",
        }
    }
}

/// A run the scheduler gave no slot: why, after how long.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotAdmitted {
    pub reason: NotAdmittedReason,
    pub waited: Duration,
}

impl fmt::Display for NotAdmitted {
    /// `run not admitted: <why>`, the same words as the .NET engine's.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "run not admitted: {}", self.reason.explanation())
    }
}

impl From<NotAdmitted> for StrataError {
    fn from(refused: NotAdmitted) -> Self {
        StrataError::NotAdmitted {
            reason: refused.reason,
            waited: refused.waited,
        }
    }
}

/// How a [`RunScheduler`] admits runs: how many at once, how many may wait, for
/// how long, the slots kept for higher priorities, and each tenant's share.
#[derive(Debug, Clone)]
pub struct SchedulerOptions {
    name: String,
    max_concurrent: u32,
    max_queued: u32,
    max_queue_time: Option<Duration>,
    reservations: Vec<(i32, u32)>,
    tenant_key: String,
    tenant_limit: Option<u32>,
    tenant_limits: HashMap<String, u32>,
    tenant_max_queued: Option<u32>,
}

impl Default for SchedulerOptions {
    /// 16 slots, a queue of 1,024, no queue time, no reservations, no tenant
    /// limits, tenants by `tenant`.
    fn default() -> Self {
        Self::new(16)
    }
}

impl SchedulerOptions {
    /// `max_concurrent` slots (at least 1), and every other option at its default.
    pub fn new(max_concurrent: u32) -> Self {
        Self {
            name: "default".to_string(),
            max_concurrent,
            max_queued: 1024,
            max_queue_time: None,
            reservations: Vec::new(),
            tenant_key: "tenant".to_string(),
            tenant_limit: None,
            tenant_limits: HashMap::new(),
            tenant_max_queued: None,
        }
    }

    /// Names the scheduler on its metrics (`strata.scheduler.name`), for a
    /// process with several. `default` by default.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// How many runs may wait for a slot (1,024 by default). When a newcomer
    /// would make it more, the lowest-ranked waiting run leaves the queue: shed,
    /// if the newcomer outranks it, or else the newcomer, refused. 0: a run
    /// starts at once or not at all.
    pub fn with_max_queued(mut self, max_queued: u32) -> Self {
        self.max_queued = max_queued;
        self
    }

    /// How long a run may wait for a slot before it is refused. None by default:
    /// as long as it takes.
    pub fn with_max_queue_time(mut self, max_queue_time: Duration) -> Self {
        self.max_queue_time = Some(max_queue_time);
        self
    }

    /// Keeps `slots` slots that only runs at `min_priority` or more may take: a
    /// run at priority `p` can't take a slot that would leave fewer free than
    /// every reservation above `p` keeps. Together they must leave at least one
    /// slot for everyone.
    pub fn with_reservation(mut self, min_priority: i32, slots: u32) -> Self {
        self.reservations.push((min_priority, slots));
        self
    }

    /// The metadata key that names a run's tenant (`tenant` by default).
    pub fn with_tenant_key(mut self, key: impl Into<String>) -> Self {
        self.tenant_key = key.into();
        self
    }

    /// How many runs of one tenant may hold slots at once. None by default.
    pub fn with_tenant_limit(mut self, limit: u32) -> Self {
        self.tenant_limit = Some(limit);
        self
    }

    /// One tenant's limit, in place of [`Self::with_tenant_limit`]. 0 pauses it.
    pub fn with_tenant_override(mut self, tenant: impl Into<String>, limit: u32) -> Self {
        self.tenant_limits.insert(tenant.into(), limit);
        self
    }

    /// How many runs of one tenant may wait for a slot. None by default.
    pub fn with_tenant_max_queued(mut self, max_queued: u32) -> Self {
        self.tenant_max_queued = Some(max_queued);
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn max_concurrent(&self) -> u32 {
        self.max_concurrent
    }

    pub fn max_queued(&self) -> u32 {
        self.max_queued
    }

    pub fn max_queue_time(&self) -> Option<Duration> {
        self.max_queue_time
    }

    pub fn tenant_key(&self) -> &str {
        &self.tenant_key
    }

    fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("the scheduler's name must not be empty".to_string());
        }
        if self.max_concurrent < 1 {
            return Err("the scheduler's max_concurrent must be at least 1".to_string());
        }
        if self.max_queue_time == Some(Duration::ZERO) {
            return Err("the scheduler's max_queue_time must be more than zero".to_string());
        }
        let mut priorities = HashSet::new();
        let mut reserved: u64 = 0;
        for &(min_priority, slots) in &self.reservations {
            if slots < 1 {
                return Err(format!(
                    "the reservation for priority {min_priority} must keep at least one slot"
                ));
            }
            if !priorities.insert(min_priority) {
                return Err(format!(
                    "two reservations are for priority {min_priority}: give each priority one"
                ));
            }
            reserved += u64::from(slots);
        }
        if reserved >= u64::from(self.max_concurrent) {
            return Err(format!(
                "the reservations keep {reserved} of {} slots: they must leave at least one for every priority",
                self.max_concurrent
            ));
        }
        if self.tenant_key.trim().is_empty() {
            return Err("the scheduler's tenant_key must not be empty".to_string());
        }
        if self
            .tenant_limits
            .keys()
            .any(|tenant| tenant.trim().is_empty())
        {
            return Err("a tenant limit names no tenant".to_string());
        }
        Ok(())
    }
}

/// A request for a slot ([`RunScheduler::admit`]).
#[derive(Debug, Clone, Default)]
pub struct AdmissionRequest {
    priority: i32,
    tenant: Option<String>,
    deadline: Option<std::time::Instant>,
    cancellation: Option<CancellationToken>,
}

impl AdmissionRequest {
    pub fn new() -> Self {
        Self::default()
    }

    /// Higher runs first; runs of one priority run in the order they arrived.
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Whose run it is, for the tenant limits. None: nobody's, and no tenant
    /// limit applies.
    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    /// When the run must stop: a wait still going then ends
    /// [`NotAdmittedReason::Deadline`].
    pub fn with_deadline(mut self, deadline: std::time::Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Cancelling `token` ends a wait with [`NotAdmittedReason::Cancelled`].
    pub fn with_cancellation(mut self, token: CancellationToken) -> Self {
        self.cancellation = Some(token);
        self
    }
}

/// A scheduler at one moment ([`RunScheduler::snapshot`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerSnapshot {
    pub name: String,
    /// Runs holding a slot.
    pub active: u32,
    /// Runs waiting for one.
    pub queued: u32,
    pub max_concurrent: u32,
    pub max_queued: u32,
    pub closed: bool,
    /// Every tenant with a run active or waiting, or a limit of its own.
    pub tenants: HashMap<String, TenantUsage>,
}

/// One tenant's runs. `limit` None: no limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantUsage {
    pub active: u32,
    pub queued: u32,
    pub limit: Option<u32>,
}

/// A slot. Dropping it gives the slot back.
pub struct RunSlot {
    scheduler: Option<Arc<Inner>>,
    tenant: Option<String>,
    waited: Duration,
}

impl RunSlot {
    /// How long the run waited for it: next to nothing for a run admitted on arrival.
    pub fn waited(&self) -> Duration {
        self.waited
    }
}

impl fmt::Debug for RunSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunSlot")
            .field("tenant", &self.tenant)
            .field("waited", &self.waited)
            .finish()
    }
}

impl Drop for RunSlot {
    fn drop(&mut self) {
        if let Some(scheduler) = self.scheduler.take() {
            scheduler.release(self.tenant.as_deref());
        }
    }
}

/// Admits runs to a fixed number of slots (`design/40-Run-Scheduler.md`).
/// Cheap to clone: every clone is the same scheduler.
#[derive(Clone)]
pub struct RunScheduler {
    inner: Arc<Inner>,
}

impl fmt::Debug for RunScheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunScheduler")
            .field("name", &self.inner.options.name)
            .finish()
    }
}

type Decision = Result<RunSlot, NotAdmitted>;

struct Waiter {
    id: u64,
    priority: i32,
    tenant: Option<String>,
    arrived: Instant,
    sender: oneshot::Sender<Decision>,
}

#[derive(Default)]
struct TenantCount {
    active: u32,
    queued: u32,
}

struct State {
    // Waiting runs in rank order: higher priority first, then earlier arrival.
    queue: Vec<Waiter>,
    // Each tenant's limit of its own, set by the options or set_tenant_limit.
    limits: HashMap<String, u32>,
    // Tenants with a run active or waiting.
    tenants: HashMap<String, TenantCount>,
    active: u32,
    arrivals: u64,
    closed: bool,
}

struct Inner {
    options: SchedulerOptions,
    state: Mutex<State>,
}

/// Slots decided for runs that stopped waiting before they could take them:
/// given back once the scheduler's lock is released.
type Undelivered = Vec<RunSlot>;

impl RunScheduler {
    /// A scheduler with `options`; invalid options are
    /// [`StrataError::ExecutionAborted`].
    pub fn new(options: SchedulerOptions) -> Result<Self, StrataError> {
        options.validate().map_err(StrataError::ExecutionAborted)?;
        crate::observability::describe_metrics();
        let limits = options.tenant_limits.clone();
        Ok(Self {
            inner: Arc::new(Inner {
                options,
                state: Mutex::new(State {
                    queue: Vec::new(),
                    limits,
                    tenants: HashMap::new(),
                    active: 0,
                    arrivals: 0,
                    closed: false,
                }),
            }),
        })
    }

    /// The options it was made with. [`Self::set_tenant_limit`] changes don't show here.
    pub fn options(&self) -> &SchedulerOptions {
        &self.inner.options
    }

    /// Asks for a slot. The run takes its place in the queue when this is
    /// called, not when the future is first polled; the future resolves to the
    /// slot, or to why there is none: the request's token was cancelled, its
    /// deadline or the longest queue time passed, a run that outranks it shed
    /// it, or the scheduler was closed. Dropping the future while it waits
    /// withdraws the run ([`NotAdmittedReason::Cancelled`]); dropping it after it
    /// was admitted gives the slot back.
    pub fn admit(
        &self,
        request: AdmissionRequest,
    ) -> impl Future<Output = Result<RunSlot, NotAdmitted>> + Send + 'static {
        let arrived = Instant::now();
        let (sender, mut receiver) = oneshot::channel();
        let mut undelivered = Undelivered::new();
        let id = {
            let mut state = self.inner.lock();
            let id = state.arrivals;
            state.arrivals += 1;
            let waiter = Waiter {
                id,
                priority: request.priority,
                tenant: request.tenant.clone(),
                arrived,
                sender,
            };
            if state.closed {
                self.inner.refuse(waiter, NotAdmittedReason::Closed);
            } else {
                self.inner.enqueue(&mut state, waiter);
                self.inner.dispatch(&mut state, &mut undelivered);
                self.inner.overflow(&mut state, id);
            }
            id
        };
        drop(undelivered);

        // The wait's own end: the deadline, or the longest a run may wait, whichever comes first.
        let mut limit = self
            .inner
            .options
            .max_queue_time
            .map(|time| (arrived + time, NotAdmittedReason::QueueTimeout));
        if let Some(deadline) = request.deadline {
            let deadline = Instant::from_std(deadline);
            if limit.is_none_or(|(at, _)| deadline <= at) {
                limit = Some((deadline, NotAdmittedReason::Deadline));
            }
        }
        // Made now, not at the first poll: a future dropped unpolled withdraws its run too.
        let mut pending = Pending {
            inner: Some(self.inner.clone()),
            id,
        };
        let inner = self.inner.clone();
        let token = request.cancellation;
        async move {
            let ended = tokio::select! {
                biased;
                decided = &mut receiver => Some(decided),
                () = cancelled(token.as_ref()) => None,
                () = until(limit.map(|(at, _)| at)) => None,
            };
            let decided = match ended {
                Some(decided) => decided,
                None => {
                    let reason = if token.as_ref().is_some_and(|t| t.is_cancelled()) {
                        NotAdmittedReason::Cancelled
                    } else {
                        limit.map_or(NotAdmittedReason::Cancelled, |(_, reason)| reason)
                    };
                    // Admitted or refused at the same moment: that stands.
                    inner.withdraw(id, reason);
                    receiver.await
                }
            };
            pending.disarm();
            decided.unwrap_or(Err(NotAdmitted {
                reason: NotAdmittedReason::Closed,
                waited: arrived.elapsed(),
            }))
        }
    }

    /// Gives `tenant` a limit of its own (0 pauses it), or with None returns it
    /// to the options' tenant limit. Takes effect at once: waiting runs a higher
    /// limit allows start, and no running run is stopped by a lower one.
    pub fn set_tenant_limit(&self, tenant: impl Into<String>, limit: Option<u32>) {
        let tenant = tenant.into();
        let mut undelivered = Undelivered::new();
        {
            let mut state = self.inner.lock();
            match limit {
                Some(limit) => {
                    state.limits.insert(tenant, limit);
                }
                None => {
                    state.limits.remove(&tenant);
                }
            }
            self.inner.dispatch(&mut state, &mut undelivered);
        }
        drop(undelivered);
    }

    /// The scheduler's runs, and each tenant's, now.
    pub fn snapshot(&self) -> SchedulerSnapshot {
        let state = self.inner.lock();
        let mut tenants: HashMap<String, TenantUsage> = state
            .tenants
            .iter()
            .map(|(tenant, count)| {
                (
                    tenant.clone(),
                    TenantUsage {
                        active: count.active,
                        queued: count.queued,
                        limit: self.inner.limit_of(&state, tenant),
                    },
                )
            })
            .collect();
        for tenant in state.limits.keys() {
            tenants
                .entry(tenant.clone())
                .or_insert_with(|| TenantUsage {
                    active: 0,
                    queued: 0,
                    limit: self.inner.limit_of(&state, tenant),
                });
        }
        SchedulerSnapshot {
            name: self.inner.options.name.clone(),
            active: state.active,
            queued: state.queue.len() as u32,
            max_concurrent: self.inner.options.max_concurrent,
            max_queued: self.inner.options.max_queued,
            closed: state.closed,
            tenants,
        }
    }

    /// Admits nothing more: every waiting run, and every run that asks from now
    /// on, is refused with [`NotAdmittedReason::Closed`]. Runs holding slots are
    /// not touched.
    pub fn close(&self) {
        let mut state = self.inner.lock();
        state.closed = true;
        while let Some(waiter) = state.queue.pop() {
            self.inner.dequeued(&mut state, &waiter);
            self.inner.refuse(waiter, NotAdmittedReason::Closed);
        }
    }

    /// Whether [`Self::close`] was called.
    pub fn is_closed(&self) -> bool {
        self.inner.lock().closed
    }
}

/// Withdraws a run whose waiting future is dropped before it is decided.
struct Pending {
    // `None` once the run is decided: nothing to withdraw.
    inner: Option<Arc<Inner>>,
    id: u64,
}

impl Pending {
    fn disarm(&mut self) {
        self.inner.take();
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.withdraw(self.id, NotAdmittedReason::Cancelled);
        }
    }
}

async fn cancelled(token: Option<&CancellationToken>) {
    match token {
        Some(token) => token.cancelled().await,
        None => std::future::pending().await,
    }
}

async fn until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Gives a slot back, and admits whoever it lets start.
    fn release(self: &Arc<Self>, tenant: Option<&str>) {
        let mut undelivered = Undelivered::new();
        {
            let mut state = self.lock();
            state.active -= 1;
            metrics::gauge!(metric_names::SCHEDULER_ACTIVE, "strata.scheduler.name" => self.options.name.clone())
                .decrement(1.0);
            if let Some(tenant) = tenant {
                if let Some(count) = state.tenants.get_mut(tenant) {
                    count.active -= 1;
                }
                forget(&mut state, tenant);
            }
            self.dispatch(&mut state, &mut undelivered);
        }
        drop(undelivered);
    }

    /// Takes a waiting run out of the queue and refuses it, if it is still there.
    fn withdraw(&self, id: u64, reason: NotAdmittedReason) {
        let mut state = self.lock();
        if let Some(index) = state.queue.iter().position(|w| w.id == id) {
            let waiter = state.queue.remove(index);
            self.dequeued(&mut state, &waiter);
            self.refuse(waiter, reason);
        }
    }

    /// Admits every waiting run that may start now, in rank order.
    fn dispatch(self: &Arc<Self>, state: &mut State, undelivered: &mut Undelivered) {
        let mut index = 0;
        while index < state.queue.len() {
            let waiter = &state.queue[index];

            // No slot for this priority is none for any after it: they have the same headroom or less.
            if !self.slot_free(state, waiter.priority) {
                return;
            }

            // A tenant at its limit holds back only its own runs.
            if !self.tenant_free(state, waiter.tenant.as_deref()) {
                index += 1;
                continue;
            }

            let waiter = state.queue.remove(index);
            self.dequeued(state, &waiter);
            state.active += 1;
            metrics::gauge!(metric_names::SCHEDULER_ACTIVE, "strata.scheduler.name" => self.options.name.clone())
                .increment(1.0);
            if let Some(tenant) = &waiter.tenant {
                state.tenants.entry(tenant.clone()).or_default().active += 1;
            }
            let waited = waiter.arrived.elapsed();
            self.record(None, waiter.priority, waited);
            let slot = RunSlot {
                scheduler: Some(self.clone()),
                tenant: waiter.tenant,
                waited,
            };
            if let Err(Ok(slot)) = waiter.sender.send(Ok(slot)) {
                // Nobody is waiting for it any more: given back once the lock is released.
                undelivered.push(slot);
            }
        }
    }

    /// Makes room in a queue the newcomer `id` made too long.
    fn overflow(&self, state: &mut State, id: u64) {
        let Some(index) = state.queue.iter().position(|w| w.id == id) else {
            return;
        };
        if let (Some(max), Some(tenant)) = (
            self.options.tenant_max_queued,
            state.queue[index].tenant.as_deref(),
        ) && state
            .tenants
            .get(tenant)
            .is_some_and(|count| count.queued > max)
        {
            let newcomer = state.queue.remove(index);
            self.dequeued(state, &newcomer);
            self.refuse(newcomer, NotAdmittedReason::QueueFull);
            return;
        }
        if state.queue.len() > self.options.max_queued as usize {
            // The lowest-ranked run: shed for a newcomer that outranks it, or the newcomer itself.
            let lowest = state.queue.pop().expect("a queue longer than its limit");
            self.dequeued(state, &lowest);
            let reason = if lowest.id == id {
                NotAdmittedReason::QueueFull
            } else {
                NotAdmittedReason::Shed
            };
            self.refuse(lowest, reason);
        }
    }

    fn slot_free(&self, state: &State, priority: i32) -> bool {
        let reserved: u32 = self
            .options
            .reservations
            .iter()
            .filter(|(min_priority, _)| *min_priority > priority)
            .map(|(_, slots)| slots)
            .sum();
        state.active < self.options.max_concurrent - reserved
    }

    fn tenant_free(&self, state: &State, tenant: Option<&str>) -> bool {
        let Some(tenant) = tenant else {
            return true;
        };
        match self.limit_of(state, tenant) {
            None => true,
            Some(limit) => state.tenants.get(tenant).map_or(0, |c| c.active) < limit,
        }
    }

    fn limit_of(&self, state: &State, tenant: &str) -> Option<u32> {
        state
            .limits
            .get(tenant)
            .copied()
            .or(self.options.tenant_limit)
    }

    fn enqueue(&self, state: &mut State, waiter: Waiter) {
        // After every run of its priority or higher: it arrived last.
        let mut index = state.queue.len();
        while index > 0 && state.queue[index - 1].priority < waiter.priority {
            index -= 1;
        }
        metrics::gauge!(metric_names::SCHEDULER_QUEUED, "strata.scheduler.name" => self.options.name.clone())
            .increment(1.0);
        if let Some(tenant) = &waiter.tenant {
            state.tenants.entry(tenant.clone()).or_default().queued += 1;
        }
        state.queue.insert(index, waiter);
    }

    /// Accounts for a waiter just taken out of the queue.
    fn dequeued(&self, state: &mut State, waiter: &Waiter) {
        metrics::gauge!(metric_names::SCHEDULER_QUEUED, "strata.scheduler.name" => self.options.name.clone())
            .decrement(1.0);
        if let Some(tenant) = &waiter.tenant {
            if let Some(count) = state.tenants.get_mut(tenant) {
                count.queued -= 1;
            }
            forget(state, tenant);
        }
    }

    fn refuse(&self, waiter: Waiter, reason: NotAdmittedReason) {
        let waited = waiter.arrived.elapsed();
        self.record(Some(reason), waiter.priority, waited);
        let _ = waiter.sender.send(Err(NotAdmitted { reason, waited }));
    }

    fn record(&self, refusal: Option<NotAdmittedReason>, priority: i32, waited: Duration) {
        let outcome = refusal.map_or("admitted", NotAdmittedReason::as_str);
        metrics::counter!(
            metric_names::SCHEDULER_ADMISSIONS,
            "strata.scheduler.name" => self.options.name.clone(),
            "strata.scheduler.outcome" => outcome,
            "strata.run.priority" => priority.to_string(),
        )
        .increment(1);
        metrics::histogram!(
            metric_names::SCHEDULER_WAIT,
            "strata.scheduler.name" => self.options.name.clone(),
            "strata.scheduler.outcome" => outcome,
        )
        .record(waited.as_secs_f64());
    }
}

fn forget(state: &mut State, tenant: &str) {
    if state
        .tenants
        .get(tenant)
        .is_some_and(|count| count.active == 0 && count.queued == 0)
    {
        state.tenants.remove(tenant);
    }
}
