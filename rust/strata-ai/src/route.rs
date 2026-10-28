//! Model routes (`design/46-Model-Routes.md`, SG-24): a chat client over
//! ordered candidates that fails over and, optionally, hedges.
//!
//! - [`ModelRoute`]: the route. It is a [`ChatClient`], so anything that takes
//!   a client takes it: [`complete`](crate::complete) /
//!   [`complete_with`](crate::complete_with), the model node, an
//!   [`Agent`](crate::Agent). Called from a node, each attempt is a model call
//!   of its own (`ModelCallStarted` / `ModelCallCompleted` with `route`) under
//!   a `route <name>` span, and the call ends with one `ModelRouted`.
//! - [`ModelCandidate`]: one candidate: a client, the model to ask it for, its
//!   health key, and an optional eligibility check.
//! - [`ModelHealth`]: cooldowns (`Retry-After`) and a simple breaker per health
//!   key, shared by every route given the same one.
//!
//! The route moves on after a transient fault, a refusal (finish reason
//! `content_filter`) or an answer its `accept` rejects; a fatal fault stops it.
//! It doesn't retry a candidate: the node's `RecoveryPolicy` retries the whole
//! route. Mirrors .NET's `ModelRoute`.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalium_strata::BoxFuture;
use datalium_strata::context::NodeContext;
use datalium_strata::observability::{
    ModelCallInfo, ModelCallScope, ModelRouteScope, model_breaker_transition,
};
pub use datalium_strata::observability::{ModelRouteAttempt, ModelRouteOutcome};
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use tokio::time::Instant;

use crate::call::CallOptions;
use crate::client::{ChatClient, ChatError, ChatErrorKind, ChatStream};
use crate::collect::ChatUpdates;
use crate::types::{ChatRequest, ChatResponse, ChatUpdate};

/// The finish reason of a refusal, in OpenAI's words (which the Anthropic
/// client maps to).
const REFUSAL: &str = "content_filter";

/// Judges an answer: `Ok` accepts it, `Err` gives the reason it is rejected.
pub type Accept = Arc<dyn Fn(&ChatResponse) -> Result<(), String> + Send + Sync>;

/// Whether a candidate may take a request: `Ok`, or why not. It gets the
/// request as the route received it and the calling node's context (`None`
/// outside a node): where an application's tenant, budget, tool or context
/// rules go.
pub type Eligible =
    Arc<dyn Fn(&ChatRequest, Option<&NodeContext>) -> Result<(), String> + Send + Sync>;

/// Reads a candidate's fault: the kind the route should treat it as, or
/// `None` to keep the fault's own. Making a provider's 4xx transient makes the
/// route fail over on it.
pub type RouteFaultClassifier =
    Arc<dyn Fn(&ModelCandidate, &ChatError) -> Option<ChatErrorKind> + Send + Sync>;

/// Told what became of each call through the route ([`ModelRoute::on_routed`]).
pub type OnRouted = Arc<dyn Fn(&ModelRouteResult) + Send + Sync>;

// --- Health ------------------------------------------------------------------

/// A breaker: after `failures` transient failures in a row, a key is skipped
/// for `open_for`, after which one call is let through to test it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BreakerOptions {
    pub failures: u32,
    pub open_for: Duration,
}

/// What a [`ModelHealth`] keeps besides cooldowns. Mirrors .NET's
/// `ModelHealthOptions`.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ModelHealthOptions {
    /// The breaker, `None` (the default) for none: only `Retry-After`
    /// cooldowns then skip a key.
    pub breaker: Option<BreakerOptions>,
}

impl ModelHealthOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Opens a key's breaker for `open_for` after `failures` transient
    /// failures in a row. Panics when either is zero.
    pub fn with_breaker(mut self, failures: u32, open_for: Duration) -> Self {
        assert!(failures > 0, "a breaker needs at least one failure to open");
        assert!(
            !open_for.is_zero(),
            "a breaker must stay open for some time"
        );
        self.breaker = Some(BreakerOptions { failures, open_for });
        self
    }
}

/// A health key's state, as [`ModelHealth::snapshot`] reports it. The
/// instants are tokio's ([`tokio::time::Instant`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelHealthState {
    /// Its candidates are asked.
    Available,
    /// A `Retry-After` is in force until `until`.
    CoolingDown { until: Instant },
    /// Its breaker is open until `until`.
    Open { until: Instant },
    /// Its breaker's `open_for` has passed: one call at a time is let through
    /// to test it.
    HalfOpen,
}

impl ModelHealthState {
    /// `available`, `cooling_down`, `open` or `half_open`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::CoolingDown { .. } => "cooling_down",
            Self::Open { .. } => "open",
            Self::HalfOpen => "half_open",
        }
    }

    /// When a cooldown or an open breaker ends.
    pub fn until(&self) -> Option<Instant> {
        match self {
            Self::CoolingDown { until } | Self::Open { until } => Some(*until),
            _ => None,
        }
    }
}

/// One health key in a [`ModelHealth::snapshot`]: its state and its
/// transient failures in a row.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ModelHealthEntry {
    pub key: String,
    pub state: ModelHealthState,
    pub failures: u32,
}

/// Health per key (`design/46-Model-Routes.md`, "Health"): a cooldown while a
/// fault's `Retry-After` is in force, and, with a breaker, a key skipped after
/// failures in a row. Kept in memory, per process; cheap to clone, and every
/// clone, and every route given one, shares the same health. Mirrors .NET's
/// `ModelHealth`.
///
/// A cooldown is capped at [`MAX_COOLDOWN`] (24 hours), whatever the
/// provider's `Retry-After` asked for.
///
/// The clock is tokio's, so tests run it paused (`start_paused`).
#[derive(Debug, Clone, Default)]
pub struct ModelHealth {
    inner: Arc<HealthInner>,
}

#[derive(Debug, Default)]
struct HealthInner {
    breaker: Option<BreakerOptions>,
    keys: Mutex<HashMap<String, KeyHealth>>,
}

#[derive(Debug, Default)]
struct KeyHealth {
    failures: u32,
    cooldown_until: Option<Instant>,
    open_until: Option<Instant>,
    /// A half-open key's one test call is out.
    probing: bool,
}

/// The longest a [`ModelHealth`] cools a key down for, whatever the
/// provider's `Retry-After` says: 24 hours.
pub const MAX_COOLDOWN: Duration = Duration::from_secs(24 * 60 * 60);

/// A candidate's turn, as its key's health decides it.
enum Turn {
    /// It may be asked; `probe` when it is a half-open key's test call.
    Available {
        probe: bool,
    },
    CoolingDown(Instant),
    /// Open until then, or (`None`) half-open with its test call already out.
    BreakerOpen(Option<Instant>),
}

impl ModelHealth {
    pub fn new(options: ModelHealthOptions) -> Self {
        Self {
            inner: Arc::new(HealthInner {
                breaker: options.breaker,
                keys: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Every key a route has asked about, by key, with its state now.
    pub fn snapshot(&self) -> Vec<ModelHealthEntry> {
        let now = Instant::now();
        let keys = self.keys();
        let mut entries: Vec<ModelHealthEntry> = keys
            .iter()
            .map(|(key, health)| ModelHealthEntry {
                key: key.clone(),
                state: health.state(now),
                failures: health.failures,
            })
            .collect();
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        entries
    }

    fn keys(&self) -> std::sync::MutexGuard<'_, HashMap<String, KeyHealth>> {
        self.inner
            .keys
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// A candidate's turn has come: whether its key lets it be asked. A
    /// half-open key gives out its one test call here.
    fn turn(&self, key: &str, now: Instant) -> Turn {
        let mut keys = self.keys();
        let health = keys.entry(key.to_string()).or_default();
        if let Some(until) = health.open_until {
            if now < until {
                return Turn::BreakerOpen(Some(until));
            }
            if health.probing {
                return Turn::BreakerOpen(None);
            }
            health.probing = true;
            model_breaker_transition(key, "half_open");
            return Turn::Available { probe: true };
        }
        match health.cooldown_until {
            Some(until) if now < until => Turn::CoolingDown(until),
            _ => Turn::Available { probe: false },
        }
    }

    /// How long until a candidate under `key` could be asked: zero when it
    /// could be at once.
    fn wait(&self, key: &str, now: Instant) -> Duration {
        let keys = self.keys();
        let Some(health) = keys.get(key) else {
            return Duration::ZERO;
        };
        health
            .state(now)
            .until()
            .map_or(Duration::ZERO, |until| until - now)
    }

    fn succeeded(&self, key: &str, probe: bool) {
        let mut keys = self.keys();
        let health = keys.entry(key.to_string()).or_default();
        health.failures = 0;
        if probe {
            health.probing = false;
        }
        if health.open_until.take().is_some() {
            health.probing = false;
            model_breaker_transition(key, "closed");
        }
    }

    /// A transient failure. Concurrent attempts (hedging, or routes sharing
    /// the health) can fail on one key in any order, so a shorter
    /// `Retry-After` never cuts a longer cooldown short, and a breaker already
    /// open is neither opened again nor pushed back: only its test call's
    /// failure reopens it.
    fn failed(&self, key: &str, probe: bool, retry_after: Option<Duration>, now: Instant) {
        let mut keys = self.keys();
        let health = keys.entry(key.to_string()).or_default();
        health.failures = health.failures.saturating_add(1);
        if let Some(after) = retry_after.filter(|after| !after.is_zero()) {
            let until = now + after.min(MAX_COOLDOWN);
            if health.cooldown_until.is_none_or(|current| until > current) {
                health.cooldown_until = Some(until);
            }
        }
        if probe {
            health.probing = false;
        }
        if let Some(breaker) = self.inner.breaker
            && (probe || (health.open_until.is_none() && health.failures >= breaker.failures))
        {
            health.open_until = Some(now + breaker.open_for);
            model_breaker_transition(key, "open");
        }
    }

    /// An attempt ended in a way that says nothing of the key's health
    /// (refused, rejected, a fatal fault, a lost race, cancelled): only its
    /// test call, if it was one, is given back.
    fn released(&self, key: &str, probe: bool) {
        if probe && let Some(health) = self.keys().get_mut(key) {
            health.probing = false;
        }
    }
}

impl KeyHealth {
    fn state(&self, now: Instant) -> ModelHealthState {
        if let Some(until) = self.open_until {
            return if now < until {
                ModelHealthState::Open { until }
            } else {
                ModelHealthState::HalfOpen
            };
        }
        match self.cooldown_until {
            Some(until) if now < until => ModelHealthState::CoolingDown { until },
            _ => ModelHealthState::Available,
        }
    }
}

// --- Candidates and the route -------------------------------------------------

/// One candidate of a [`ModelRoute`]. Mirrors .NET's `ModelCandidate`.
#[derive(Clone)]
pub struct ModelCandidate {
    name: String,
    client: Arc<dyn ChatClient>,
    model: Option<String>,
    health_key: Option<String>,
    eligible: Option<Eligible>,
}

impl ModelCandidate {
    /// A candidate named `name` (unique within its route; it appears in
    /// spans, events and messages) asking `client`.
    pub fn new(name: impl Into<String>, client: Arc<dyn ChatClient>) -> Self {
        let name = name.into();
        assert!(!name.trim().is_empty(), "a model candidate needs a name");
        Self {
            name,
            client,
            model: None,
            health_key: None,
            eligible: None,
        }
    }

    /// The model this candidate is asked for, in place of the request's.
    /// Panics when it is blank.
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        let model = model.into();
        assert!(
            !model.trim().is_empty(),
            "candidate '{}' needs a model name, or none to ask for the request's",
            self.name
        );
        self.model = Some(model);
        self
    }

    /// The key its health is kept under: the client's provider by default
    /// (an outage or rate limit takes every model of a provider), so name one
    /// per model or region to keep them apart.
    pub fn with_health_key(mut self, key: impl Into<String>) -> Self {
        self.health_key = Some(key.into());
        self
    }

    /// Checks each request this candidate would take: `Err(reason)` skips it
    /// as ineligible.
    pub fn with_eligible(
        mut self,
        eligible: impl Fn(&ChatRequest, Option<&NodeContext>) -> Result<(), String>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.eligible = Some(Arc::new(eligible));
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn client(&self) -> &Arc<dyn ChatClient> {
        &self.client
    }

    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    /// Its health key: the one given, else the client's provider, else its
    /// name.
    pub fn health_key(&self) -> &str {
        match &self.health_key {
            Some(key) => key,
            None if !self.client.provider().is_empty() => self.client.provider(),
            None => &self.name,
        }
    }
}

impl fmt::Debug for ModelCandidate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelCandidate")
            .field("name", &self.name)
            .field("provider", &self.client.provider())
            .field("model", &self.model)
            .field("health_key", &self.health_key())
            .finish_non_exhaustive()
    }
}

/// What became of one call through a route, as [`ModelRoute::on_routed`] is
/// told it: what `ModelRouted` carries. Mirrors .NET's `ModelRouteResult`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ModelRouteResult {
    pub route: String,
    /// The route call's id; each attempt's model call has its own.
    pub call_id: String,
    /// The candidate whose answer the route returned, `None` when none did.
    pub answered: Option<String>,
    /// The model that gave it.
    pub model: Option<String>,
    /// Every candidate, once, in declaration order.
    pub attempts: Vec<ModelRouteAttempt>,
    /// Attempts hedging started beside one still running.
    pub hedges: u32,
    pub duration: Duration,
}

/// A chat client over ordered candidates (`design/46-Model-Routes.md`): it
/// asks the first available one, moves on after a transient fault, a refusal
/// or a rejected answer, and, with [`Self::with_hedge_after`], starts the next
/// candidate beside a slow one, the first accepted answer winning. Mirrors
/// .NET's `ModelRoute`.
///
/// - **Choosing.** Each candidate's turn is decided when it comes: ineligible
///   (its check said why not), cooling down (its key has a `Retry-After` in
///   force), breaker open, or available.
/// - **Failing.** A fatal fault stops the route with the candidate's own
///   error. Out of candidates, it fails transiently (`route_exhausted`, the
///   hint the shortest wait until some candidate is free again, none when one
///   is free at once), or fatally (`route_ineligible`) when every candidate
///   was ineligible. [`ChatError`] has no room for the attempts:
///   [`Self::on_routed`] and `ModelRouted` carry them.
/// - **Streams** fail over only before their first update. After it, a break
///   fails the call, and an answer refused or rejected at its end fails it
///   transiently (`refused` / `rejected`): its tokens have gone out.
/// - **Health** ([`ModelHealth`]) persists across calls, so the node's retry
///   skips a candidate still cooling down.
///
/// Cheap to clone; clones share everything, health included.
#[derive(Clone)]
pub struct ModelRoute {
    inner: Arc<RouteInner>,
}

#[derive(Clone)]
struct RouteInner {
    name: String,
    candidates: Vec<ModelCandidate>,
    accept: Option<Accept>,
    hedge_after: Option<Duration>,
    health: ModelHealth,
    fault_classifier: Option<RouteFaultClassifier>,
    on_routed: Option<OnRouted>,
}

impl ModelRoute {
    /// A route named `name` (in spans, events and messages), with no
    /// candidates yet, no hedging, and health of its own with no breaker.
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        assert!(!name.trim().is_empty(), "a model route needs a name");
        Self {
            inner: Arc::new(RouteInner {
                name,
                candidates: Vec::new(),
                accept: None,
                hedge_after: None,
                health: ModelHealth::default(),
                fault_classifier: None,
                on_routed: None,
            }),
        }
    }

    fn edit(&mut self) -> &mut RouteInner {
        Arc::make_mut(&mut self.inner)
    }

    /// Adds the next candidate. Panics when the route already has one of
    /// that name.
    pub fn candidate(mut self, candidate: ModelCandidate) -> Self {
        assert!(
            self.inner
                .candidates
                .iter()
                .all(|c| c.name != candidate.name),
            "route '{}' already has a candidate named '{}'",
            self.inner.name,
            candidate.name
        );
        self.edit().candidates.push(candidate);
        self
    }

    /// Judges each answer: `Err(reason)` rejects it, and the route moves on
    /// (a whole answer) or fails the call (a streamed one).
    pub fn with_accept(
        mut self,
        accept: impl Fn(&ChatResponse) -> Result<(), String> + Send + Sync + 'static,
    ) -> Self {
        self.edit().accept = Some(Arc::new(accept));
        self
    }

    /// Hedges: when the latest attempt hasn't answered (or a stream hasn't
    /// given its first update) within `after`, the next available candidate
    /// starts beside it. Off by default: a hedge costs the tokens of every
    /// attempt it starts. Panics when `after` is zero.
    pub fn with_hedge_after(mut self, after: Duration) -> Self {
        assert!(!after.is_zero(), "a hedge delay must be above zero");
        self.edit().hedge_after = Some(after);
        self
    }

    /// The health this route keeps and reads, shared with every route given
    /// the same one.
    pub fn with_health(mut self, health: ModelHealth) -> Self {
        self.edit().health = health;
        self
    }

    /// Reclassifies candidates' faults before the route acts on them.
    pub fn with_fault_classifier(
        mut self,
        classifier: impl Fn(&ModelCandidate, &ChatError) -> Option<ChatErrorKind>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.edit().fault_classifier = Some(Arc::new(classifier));
        self
    }

    /// Called once per call through the route, inside a run or not, with what
    /// `ModelRouted` carries, before the call returns (for a stream, before it
    /// ends). For a caller outside a run: a ledger, a test. It must not block;
    /// a panic in it is caught and ignored.
    pub fn on_routed(
        mut self,
        on_routed: impl Fn(&ModelRouteResult) + Send + Sync + 'static,
    ) -> Self {
        self.edit().on_routed = Some(Arc::new(on_routed));
        self
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn candidates(&self) -> &[ModelCandidate] {
        &self.inner.candidates
    }

    pub fn health(&self) -> &ModelHealth {
        &self.inner.health
    }

    /// One answer, as [`complete_with`](crate::complete_with) asks for it:
    /// streamed to the node's stream when it has one (only the winning
    /// attempt's tokens), whole otherwise.
    pub(crate) async fn call(
        &self,
        request: ChatRequest,
        ctx: Option<&NodeContext>,
        options: &CallOptions,
    ) -> Result<ChatResponse, ChatError> {
        let Some(stream_to) = ctx.and_then(NodeContext::stream) else {
            return self.whole(request, ctx, options).await;
        };
        let mut tail = match self.streamed(request, ctx, options, true).await? {
            Streamed::Whole(response) => return Ok(response),
            Streamed::Tail(tail) => tail,
        };
        let mut updates = ChatUpdates::new();
        while let Some(update) = tail.pull().await {
            let update = update?;
            if let ChatUpdate::Text(text) = &update
                && !text.is_empty()
                && let Err(error) = stream_to.emit_token(text).await
            {
                // The node's stream is gone: the winning attempt can't
                // deliver its answer, which is no fault of its provider.
                let error = ChatError::transient("stream_closed", format!("{error:?}"));
                return Err(tail.abandon(error));
            }
            updates.push(update);
        }
        updates.finish()
    }

    async fn whole(
        &self,
        request: ChatRequest,
        ctx: Option<&NodeContext>,
        options: &CallOptions,
    ) -> Result<ChatResponse, ChatError> {
        let mut run = Run::start(self.inner.clone(), ctx, options);
        match run.settle(&request, ctx, false).await {
            Settled::Answer(index, call, response) => {
                run.answered(index, call, &response);
                Ok(response)
            }
            Settled::Failed(outcome, error) => {
                run.finish(outcome, None, Some(&error));
                Err(error)
            }
            Settled::Started(..) => unreachable!("a whole call commits to no stream"),
        }
    }

    /// One streamed answer: the attempts race to their first update, and the
    /// stream is the winner's. `node` turns a break after the first update
    /// into `stream_broken`, as a node's call reports it.
    async fn streamed(
        &self,
        request: ChatRequest,
        ctx: Option<&NodeContext>,
        options: &CallOptions,
        node: bool,
    ) -> Result<Streamed, ChatError> {
        let mut run = Run::start(self.inner.clone(), ctx, options);
        match run.settle(&request, ctx, true).await {
            Settled::Answer(index, call, response) => {
                // A stream that ended without an update: a whole answer, and
                // nothing has gone out.
                run.answered(index, call, &response);
                Ok(Streamed::Whole(response))
            }
            Settled::Failed(outcome, error) => {
                run.finish(outcome, None, Some(&error));
                Err(error)
            }
            Settled::Started(index, call, first, rest) => {
                run.committed = Some((index, call));
                Ok(Streamed::Tail(Box::new(Tail {
                    run,
                    first: Some(first),
                    rest,
                    updates: ChatUpdates::new(),
                    node,
                    done: false,
                })))
            }
        }
    }
}

impl fmt::Debug for ModelRoute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelRoute")
            .field("name", &self.inner.name)
            .field("candidates", &self.inner.candidates)
            .field("hedge_after", &self.inner.hedge_after)
            .field("health", &self.inner.health)
            .finish_non_exhaustive()
    }
}

impl ChatClient for ModelRoute {
    /// `route`: a route is no provider of its own; each attempt reports its
    /// candidate's.
    fn provider(&self) -> &str {
        "route"
    }

    fn complete(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatResponse, ChatError>> {
        Box::pin(async move { self.whole(request, None, &CallOptions::default()).await })
    }

    /// The candidates' streams race to their first update; the winner's is
    /// returned, its errors as the candidate gave them.
    fn stream(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatStream, ChatError>> {
        Box::pin(async move {
            let stream: ChatStream = match self
                .streamed(request, None, &CallOptions::default(), false)
                .await?
            {
                // It ended before its first update, with nothing in it.
                Streamed::Whole(_) => Box::pin(futures_util::stream::empty()),
                Streamed::Tail(tail) => {
                    Box::pin(futures_util::stream::unfold(tail, |mut tail| async move {
                        let item = tail.pull().await?;
                        Some((item, tail))
                    }))
                }
            };
            Ok(stream)
        })
    }

    fn as_route(&self) -> Option<&ModelRoute> {
        Some(self)
    }
}

// --- One call through the route ----------------------------------------------

/// What an attempt got before the route could judge it.
enum Answer {
    Whole(ChatResponse),
    /// A stream's first update, and the rest of it.
    Started(ChatUpdate, ChatStream),
}

type AttemptOutput = (usize, ModelCallScope, Result<Answer, ChatError>);
type AttemptFuture = Pin<Box<dyn Future<Output = AttemptOutput> + Send>>;

/// A streamed call once the race is over.
enum Streamed {
    /// The winner's stream ended before its first update.
    Whole(ChatResponse),
    /// The winner's stream, from its first update.
    Tail(Box<Tail>),
}

/// How the race over the candidates ended.
enum Settled {
    /// An accepted whole answer.
    Answer(usize, ModelCallScope, ChatResponse),
    /// A stream gave its first update: the route has committed to it.
    Started(usize, ModelCallScope, ChatUpdate, ChatStream),
    /// The route failed: `strata.route.calls`' outcome, and the error.
    Failed(&'static str, ChatError),
}

/// A candidate's part in one call.
#[derive(Default)]
struct Slot {
    outcome: Option<ModelRouteOutcome>,
    reason: Option<String>,
    call_id: Option<String>,
    /// The model it was asked for.
    model: Option<String>,
    /// Its attempt holds its key's half-open test call, not yet given back
    /// (by a success, a failure or a release: `Run::take_probe`). Whatever
    /// ends the call, a panic included, `Run::drop` gives back every one
    /// still held, so a key can't stay half-open with nobody testing it.
    probe: bool,
    /// Its attempt is in flight (or, committed, streaming).
    running: bool,
}

/// One call through a route, from its first candidate to `ModelRouted`.
/// Dropped before it finished (the node's token fired and the engine dropped
/// the call, or a stream's consumer let it go), it records the attempts in
/// flight `Cancelled` and the rest `NotTried`, health untouched.
struct Run {
    route: Arc<RouteInner>,
    scope: ModelRouteScope,
    started: Instant,
    agent: Option<String>,
    slots: Vec<Slot>,
    running: FuturesUnordered<AttemptFuture>,
    /// The streaming attempt the route committed to.
    committed: Option<(usize, ModelCallScope)>,
    hedges: u32,
    finished: bool,
}

impl Run {
    fn start(route: Arc<RouteInner>, ctx: Option<&NodeContext>, options: &CallOptions) -> Self {
        let scope = ModelRouteScope::start(ctx, &route.name);
        let slots = route.candidates.iter().map(|_| Slot::default()).collect();
        Self {
            route,
            scope,
            started: Instant::now(),
            agent: options.agent.clone(),
            slots,
            running: FuturesUnordered::new(),
            committed: None,
            hedges: 0,
            finished: false,
        }
    }

    /// Asks candidates, failing over and hedging, until one answers (or a
    /// stream starts) or the route fails.
    async fn settle(
        &mut self,
        request: &ChatRequest,
        ctx: Option<&NodeContext>,
        stream: bool,
    ) -> Settled {
        if self.route.candidates.is_empty() {
            return Settled::Failed(
                "ineligible",
                ChatError::fatal(
                    "route_ineligible",
                    format!("route '{}' has no candidates", self.route.name),
                ),
            );
        }
        let mut next = 0;
        let mut last_start = Instant::now();
        loop {
            if self.running.is_empty() {
                if !self.start_next(&mut next, request, ctx, stream) {
                    return self.out_of_candidates();
                }
                last_start = Instant::now();
            }
            let hedge_at = match self.route.hedge_after {
                Some(after) if next < self.slots.len() => Some(last_start + after),
                _ => None,
            };
            let ended = match hedge_at {
                Some(at) => tokio::select! {
                    biased;
                    ended = self.running.next() => ended,
                    _ = tokio::time::sleep_until(at) => None,
                },
                None => self.running.next().await,
            };
            let Some((index, call, result)) = ended else {
                // The hedge timer: the next available candidate starts beside
                // the ones running.
                if self.start_next(&mut next, request, ctx, stream) {
                    self.hedges += 1;
                    self.scope.hedged();
                    last_start = Instant::now();
                }
                continue;
            };
            self.slots[index].running = false;
            match result {
                Err(error) => {
                    let error = self.classified(index, error);
                    call.failed(&error.error_type, &error.message);
                    self.set(index, ModelRouteOutcome::Failed, Some(&error.error_type));
                    if !error.is_transient() {
                        self.released(index);
                        self.cancel_running(ModelRouteOutcome::Cancelled);
                        return Settled::Failed("fatal", error);
                    }
                    self.failed(index, error.retry_after);
                    self.scope.failover("failed");
                }
                Ok(Answer::Whole(response)) => {
                    call.succeeded(
                        response.usage,
                        response.finish_reason.as_deref(),
                        response.model.as_deref(),
                        response.id.as_deref(),
                    );
                    if response.finish_reason.as_deref() == Some(REFUSAL) {
                        self.released(index);
                        self.set(index, ModelRouteOutcome::Refused, None);
                        self.scope.failover("refused");
                    } else if let Err(reason) = self.judge(&response) {
                        self.released(index);
                        self.set(index, ModelRouteOutcome::Rejected, Some(&reason));
                        self.scope.failover("rejected");
                    } else {
                        self.cancel_running(ModelRouteOutcome::LostRace);
                        return Settled::Answer(index, call, response);
                    }
                }
                Ok(Answer::Started(first, rest)) => {
                    self.cancel_running(ModelRouteOutcome::LostRace);
                    self.slots[index].running = true;
                    return Settled::Started(index, call, first, rest);
                }
            }
        }
    }

    /// Decides candidates from `next` on until one is available, and starts
    /// it. False when none was.
    fn start_next(
        &mut self,
        next: &mut usize,
        request: &ChatRequest,
        ctx: Option<&NodeContext>,
        stream: bool,
    ) -> bool {
        while *next < self.slots.len() {
            let index = *next;
            *next += 1;
            let candidate = self.route.candidates[index].clone();
            if let Some(eligible) = &candidate.eligible
                && let Err(reason) = eligible(request, ctx)
            {
                self.set(index, ModelRouteOutcome::Ineligible, Some(&reason));
                self.scope.failover("ineligible");
                continue;
            }
            let now = Instant::now();
            match self.route.health.turn(candidate.health_key(), now) {
                Turn::CoolingDown(until) => {
                    let left = seconds(until - now);
                    self.set(index, ModelRouteOutcome::CoolingDown, Some(&left));
                    self.scope.failover("cooling_down");
                }
                Turn::BreakerOpen(until) => {
                    let left = seconds(until.map_or(Duration::ZERO, |until| until - now));
                    self.set(index, ModelRouteOutcome::BreakerOpen, Some(&left));
                    self.scope.failover("breaker_open");
                }
                Turn::Available { probe } => {
                    // Held by the slot at once, so nothing after this can
                    // leak it.
                    self.slots[index].probe = probe;
                    self.ask(index, candidate, request, ctx, stream);
                    return true;
                }
            }
        }
        false
    }

    fn ask(
        &mut self,
        index: usize,
        candidate: ModelCandidate,
        request: &ChatRequest,
        ctx: Option<&NodeContext>,
        stream: bool,
    ) {
        let mut request = request.clone();
        if let Some(model) = &candidate.model {
            request.model = Some(model.clone());
        }
        let model = request
            .model
            .clone()
            .or_else(|| candidate.client.default_model().map(str::to_string))
            .filter(|model| !model.trim().is_empty())
            .unwrap_or_else(|| "unknown".to_string());
        self.slots[index].model = Some(model.clone());
        let mut info = ModelCallInfo::new(model)
            .with_provider(candidate.client.provider())
            .with_route(self.route.name.clone())
            .with_route_candidate(candidate.name.clone());
        if let Some(max_tokens) = request.max_tokens {
            info = info.with_max_tokens(max_tokens);
        }
        if let Some(temperature) = request.temperature {
            info = info.with_temperature(temperature);
        }
        if let Some(agent) = &self.agent {
            info = info.with_agent(agent.clone());
        }
        let call = self.scope.attempt(ctx, info);
        let slot = &mut self.slots[index];
        slot.call_id = Some(call.call_id().to_string());
        slot.running = true;
        let client = candidate.client;
        self.running.push(Box::pin(async move {
            let result = if stream {
                match client.stream(request).await {
                    Err(error) => Err(error),
                    Ok(mut updates) => match updates.next().await {
                        // Ended before its first update: a whole (empty) answer.
                        None => ChatUpdates::new().finish().map(Answer::Whole),
                        Some(Err(error)) => Err(error),
                        Some(Ok(first)) => {
                            call.first_chunk();
                            Ok(Answer::Started(first, updates))
                        }
                    },
                }
            } else {
                client.complete(request).await.map(Answer::Whole)
            };
            (index, call, result)
        }));
    }

    fn classified(&self, index: usize, mut error: ChatError) -> ChatError {
        if let Some(classifier) = &self.route.fault_classifier
            && let Some(kind) = classifier(&self.route.candidates[index], &error)
        {
            error.kind = kind;
        }
        error
    }

    fn judge(&self, response: &ChatResponse) -> Result<(), String> {
        match &self.route.accept {
            Some(accept) => accept(response),
            None => Ok(()),
        }
    }

    fn set(&mut self, index: usize, outcome: ModelRouteOutcome, reason: Option<&str>) {
        let slot = &mut self.slots[index];
        slot.outcome = Some(outcome);
        slot.reason = reason.map(str::to_string);
    }

    /// Stops every attempt in flight: dropping one records its model call
    /// cancelled. Each is recorded `outcome`, and its test call given back.
    ///
    /// A race's losers (`LostRace`) are polled once first: one whose answer
    /// had already arrived when the race was decided records it (its tokens
    /// were billed), and only one still pending is cancelled.
    fn cancel_running(&mut self, outcome: ModelRouteOutcome) {
        for attempt in std::mem::take(&mut self.running) {
            if outcome != ModelRouteOutcome::LostRace {
                continue;
            }
            match attempt.now_or_never() {
                Some((_, call, Ok(Answer::Whole(response)))) => call.succeeded(
                    response.usage,
                    response.finish_reason.as_deref(),
                    response.model.as_deref(),
                    response.id.as_deref(),
                ),
                Some((_, call, Err(error))) => call.failed(&error.error_type, &error.message),
                // A stream that has started, or a call still pending: cancelled.
                _ => {}
            }
        }
        for index in 0..self.slots.len() {
            if self.slots[index].running {
                self.slots[index].running = false;
                self.released(index);
                self.set(index, outcome, None);
            }
        }
    }

    /// The slot's test call, if it holds one, handed over to be given back.
    fn take_probe(&mut self, index: usize) -> bool {
        std::mem::take(&mut self.slots[index].probe)
    }

    fn released(&mut self, index: usize) {
        let probe = self.take_probe(index);
        self.route
            .health
            .released(self.route.candidates[index].health_key(), probe);
    }

    fn failed(&mut self, index: usize, retry_after: Option<Duration>) {
        let probe = self.take_probe(index);
        self.route.health.failed(
            self.route.candidates[index].health_key(),
            probe,
            retry_after,
            Instant::now(),
        );
    }

    /// No candidate is left to ask, and none is running.
    fn out_of_candidates(&self) -> Settled {
        let name = &self.route.name;
        let all_ineligible = self
            .slots
            .iter()
            .all(|slot| slot.outcome == Some(ModelRouteOutcome::Ineligible));
        if all_ineligible {
            return Settled::Failed(
                "ineligible",
                ChatError::fatal(
                    "route_ineligible",
                    format!(
                        "no candidate of route '{name}' can take this request: {}",
                        self.describe()
                    ),
                ),
            );
        }
        let error = ChatError::transient(
            "route_exhausted",
            format!(
                "no candidate of route '{name}' answered: {}",
                self.describe()
            ),
        );
        Settled::Failed(
            "exhausted",
            match self.hint() {
                Some(hint) => error.with_retry_after(hint),
                None => error,
            },
        )
    }

    /// Each candidate and what became of it, for a route's own message.
    fn describe(&self) -> String {
        self.route
            .candidates
            .iter()
            .zip(&self.slots)
            .filter_map(|(candidate, slot)| {
                let c = &candidate.name;
                let reason = slot.reason.as_deref().unwrap_or_default();
                Some(match slot.outcome? {
                    ModelRouteOutcome::Failed => format!("{c} failed ({reason})"),
                    ModelRouteOutcome::Refused => format!("{c} refused"),
                    ModelRouteOutcome::Rejected => {
                        format!("{c}'s answer was rejected ({reason})")
                    }
                    ModelRouteOutcome::Ineligible => format!("{c} is ineligible ({reason})"),
                    ModelRouteOutcome::CoolingDown => format!("{c} is cooling down for {reason}"),
                    ModelRouteOutcome::BreakerOpen => format!("{c}'s breaker is open for {reason}"),
                    _ => return None,
                })
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The shortest wait until some eligible candidate could be asked again;
    /// `None` when one could be at once.
    fn hint(&self) -> Option<Duration> {
        let now = Instant::now();
        let mut shortest: Option<Duration> = None;
        for (candidate, slot) in self.route.candidates.iter().zip(&self.slots) {
            if slot.outcome == Some(ModelRouteOutcome::Ineligible) {
                continue;
            }
            let wait = self.route.health.wait(candidate.health_key(), now);
            if wait.is_zero() {
                return None;
            }
            shortest = Some(shortest.map_or(wait, |s| s.min(wait)));
        }
        shortest
    }

    /// The route's answer: its key is healthy, and the call is over.
    fn answered(&mut self, index: usize, call: ModelCallScope, response: &ChatResponse) {
        call.succeeded(
            response.usage,
            response.finish_reason.as_deref(),
            response.model.as_deref(),
            response.id.as_deref(),
        );
        self.won(index, response);
    }

    fn won(&mut self, index: usize, response: &ChatResponse) {
        let probe = self.take_probe(index);
        self.slots[index].running = false;
        self.route
            .health
            .succeeded(self.route.candidates[index].health_key(), probe);
        self.set(index, ModelRouteOutcome::Answered, None);
        let model = response
            .model
            .clone()
            .or_else(|| self.slots[index].model.clone());
        self.finish("answered", model.as_deref(), None);
    }

    /// Records the call once: `ModelRouted`, the span, the metric, and
    /// `on_routed`. Candidates never reached are `NotTried`.
    fn finish(&mut self, outcome: &str, model: Option<&str>, error: Option<&ChatError>) {
        if self.finished {
            return;
        }
        self.finished = true;
        let attempts: Vec<ModelRouteAttempt> = self
            .route
            .candidates
            .iter()
            .zip(&self.slots)
            .map(|(candidate, slot)| {
                let mut attempt = ModelRouteAttempt::new(
                    candidate.name.clone(),
                    slot.outcome.unwrap_or(ModelRouteOutcome::NotTried),
                );
                attempt.reason = slot.reason.clone();
                attempt.call_id = slot.call_id.clone();
                attempt
            })
            .collect();
        let duration = self.started.elapsed();
        self.scope.finish(
            outcome,
            model,
            attempts.clone(),
            self.hedges,
            duration,
            error.map(|e| (e.error_type.as_str(), e.message.as_str())),
        );
        if let Some(on_routed) = &self.route.on_routed {
            let result = ModelRouteResult {
                route: self.route.name.clone(),
                call_id: self.scope.call_id().to_string(),
                answered: attempts
                    .iter()
                    .find(|a| a.outcome == ModelRouteOutcome::Answered)
                    .map(|a| a.candidate.clone()),
                model: model.map(str::to_string),
                attempts,
                hedges: self.hedges,
                duration,
            };
            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| on_routed(&result)));
        }
    }
}

impl Drop for Run {
    fn drop(&mut self) {
        if !self.finished {
            // The attempts' model calls end (cancelled) before the route's
            // ModelRouted, which comes after its last attempt.
            if let Some((index, call)) = self.committed.take() {
                drop(call);
                self.slots[index].running = false;
                self.set(index, ModelRouteOutcome::Cancelled, None);
            }
            self.cancel_running(ModelRouteOutcome::Cancelled);
            let cancelled = ChatError::transient("cancelled", "cancelled");
            self.finish("cancelled", None, Some(&cancelled));
        }
        // Whatever ended the call (a panic in `accept` or the fault classifier
        // included), no test call outlives it.
        for index in 0..self.slots.len() {
            self.released(index);
        }
    }
}

/// The rest of a stream the route committed to: the winner's updates, then
/// its judgement at the end.
struct Tail {
    run: Run,
    first: Option<ChatUpdate>,
    rest: ChatStream,
    updates: ChatUpdates,
    node: bool,
    done: bool,
}

impl Tail {
    /// The next update, an error that ends the call, or `None` when the
    /// answer is whole and accepted.
    async fn pull(&mut self) -> Option<Result<ChatUpdate, ChatError>> {
        if self.done {
            return None;
        }
        if let Some(first) = self.first.take() {
            self.updates.push(first.clone());
            return Some(Ok(first));
        }
        match self.rest.next().await {
            Some(Ok(update)) => {
                self.updates.push(update.clone());
                Some(Ok(update))
            }
            Some(Err(error)) => {
                self.done = true;
                Some(Err(self.broken(error)))
            }
            None => {
                self.done = true;
                let updates = std::mem::take(&mut self.updates);
                let response = match updates.finish() {
                    Ok(response) => response,
                    Err(error) => return Some(Err(self.broken(error))),
                };
                self.end(response).err().map(Err)
            }
        }
    }

    /// The committed stream broke after its first update: the attempt and
    /// the route record `stream_broken`, and the failure counts against the
    /// key's health. A node's call fails with `stream_broken`, as design/44's
    /// does; a caller of the route's own stream gets the candidate's error.
    fn broken(&mut self, error: ChatError) -> ChatError {
        let (index, call) = self.run.committed.take().expect("a committed attempt");
        let broken = ChatError {
            retry_after: error.retry_after,
            ..ChatError::transient("stream_broken", error.message.clone())
        };
        call.failed(&broken.error_type, &broken.message);
        let run = &mut self.run;
        run.slots[index].running = false;
        run.failed(index, broken.retry_after);
        run.set(index, ModelRouteOutcome::Failed, Some(&broken.error_type));
        run.finish("failed", None, Some(&broken));
        if self.node { broken } else { error }
    }

    /// The node's stream refused a token: the winning attempt can't deliver
    /// its answer. It is recorded `Failed` with `error`'s type and the call
    /// `failed`; the key's health is untouched, since the provider did
    /// nothing wrong.
    fn abandon(&mut self, error: ChatError) -> ChatError {
        self.done = true;
        let (index, call) = self.run.committed.take().expect("a committed attempt");
        call.failed(&error.error_type, &error.message);
        let run = &mut self.run;
        run.slots[index].running = false;
        run.released(index);
        run.set(index, ModelRouteOutcome::Failed, Some(&error.error_type));
        run.finish("failed", None, Some(&error));
        error
    }

    /// The committed stream ended: refused or rejected, it fails the call
    /// (its tokens have gone out); otherwise it is the route's answer.
    fn end(&mut self, response: ChatResponse) -> Result<(), ChatError> {
        let (index, call) = self.run.committed.take().expect("a committed attempt");
        call.succeeded(
            response.usage,
            response.finish_reason.as_deref(),
            response.model.as_deref(),
            response.id.as_deref(),
        );
        let run = &mut self.run;
        let name = run.route.name.clone();
        let candidate = run.route.candidates[index].name.clone();
        let failure = if response.finish_reason.as_deref() == Some(REFUSAL) {
            Some((
                ModelRouteOutcome::Refused,
                None,
                ChatError::transient(
                    "refused",
                    format!("route '{name}': {candidate}'s streamed answer was refused"),
                ),
            ))
        } else if let Err(reason) = run.judge(&response) {
            let message =
                format!("route '{name}': {candidate}'s streamed answer was rejected ({reason})");
            Some((
                ModelRouteOutcome::Rejected,
                Some(reason),
                ChatError::transient("rejected", message),
            ))
        } else {
            None
        };
        let Some((outcome, reason, error)) = failure else {
            run.won(index, &response);
            return Ok(());
        };
        run.slots[index].running = false;
        run.released(index);
        run.set(index, outcome, reason.as_deref());
        run.finish("failed", None, Some(&error));
        Err(error)
    }
}

/// A wait as the messages give it: whole seconds, rounded up.
fn seconds(wait: Duration) -> String {
    let nanos = wait.as_nanos();
    format!("{} s", nanos.div_ceil(1_000_000_000))
}
