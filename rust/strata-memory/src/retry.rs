//! How the Postgres and Redis stores repeat a call that failed for a transient
//! reason (`design/34-Storage-Hardening.md`, "Retries"): a dropped connection,
//! a server shutting down or starting, a deadlock or a serialization failure,
//! a timeout. Each store decides what is transient and what repeating a call
//! means; a save that may have happened is looked up before it is made again.
//! Mirrors the .NET `StoreRetryPolicy` and `StoreRetrier`.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use datalium_strata::checkpoint::CheckpointStoreError;
use datalium_strata::history::HistoryStoreError;
use datalium_strata::memory::MemoryStoreError;
use datalium_strata::schema::Checkpoint;

/// The counter of store calls repeated after a transient failure, by
/// [`STORE_LABEL`] (`postgres`, `redis`) and [`OPERATION_LABEL`] (`save`,
/// `load`, `list`, `lease`, `migrate`, `purge`, `append`, and `memory` for
/// every call of a memory store).
pub const RETRIES_METRIC: &str = "strata.store.retries";

/// The label naming the store a call was repeated in.
pub const STORE_LABEL: &str = "strata.store";

/// The label naming the call that was repeated.
pub const OPERATION_LABEL: &str = "strata.store.operation";

/// How a Postgres or Redis store repeats a call that failed for a transient
/// reason. The default makes a call at most 4 times, waiting 50 ms before the
/// first repeat and doubling up to 2 s, each wait jittered over the whole of
/// it. Mirrors the .NET `StoreRetryPolicy`.
#[derive(Debug, Clone, PartialEq)]
pub struct StoreRetry {
    max_attempts: u32,
    initial_delay: Duration,
    max_delay: Duration,
}

impl Default for StoreRetry {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(2),
        }
    }
}

impl StoreRetry {
    pub fn new() -> Self {
        Self::default()
    }

    /// No retries: every failure is the call's, as before this policy existed.
    pub fn none() -> Self {
        Self::new().with_max_attempts(1)
    }

    /// How many times a call is made at most, the first included. 1 never
    /// repeats; 0 is taken as 1.
    pub fn with_max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts.max(1);
        self
    }

    /// The wait before the first repeat, doubled before each later one.
    pub fn with_initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self
    }

    /// The longest wait between two attempts.
    pub fn with_max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    pub fn initial_delay(&self) -> Duration {
        self.initial_delay
    }

    pub fn max_delay(&self) -> Duration {
        self.max_delay
    }

    /// The wait before repeat number `repeat` (from 1): the initial delay
    /// doubled `repeat` − 1 times, capped at the longest, then jittered over
    /// the whole of it by `sample` (0 to 1), so stores that failed together
    /// don't come back together.
    pub fn delay_before(&self, repeat: u32, sample: f64) -> Duration {
        let doubled = self.initial_delay.as_secs_f64() * 2f64.powi(repeat.saturating_sub(1) as i32);
        let capped = doubled.min(self.max_delay.as_secs_f64());
        Duration::from_secs_f64(capped * sample.clamp(0.0, 1.0))
    }
}

/// A store call's failure, with whether the store repeats a call for it.
#[derive(Debug, Clone)]
pub(crate) struct Failure {
    pub message: String,
    pub transient: bool,
    /// Whether the command can't have reached the server.
    pub never_sent: bool,
    /// The failure's class, for the retry's warning: a SQLSTATE, an error
    /// kind. Never the message, which can hold a connection string.
    pub class: String,
}

impl Failure {
    /// A failure no retry helps.
    pub fn fatal(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            transient: false,
            never_sent: false,
            class: "fatal".to_string(),
        }
    }

    /// What a test's fault hook raises at `point`: what a lost connection
    /// would.
    pub fn injected(point: &str) -> Self {
        Self {
            message: format!("injected fault at {point}"),
            transient: true,
            never_sent: false,
            class: "injected".to_string(),
        }
    }
}

impl From<CheckpointStoreError> for Failure {
    fn from(err: CheckpointStoreError) -> Self {
        Self::fatal(err.0)
    }
}

impl From<HistoryStoreError> for Failure {
    fn from(err: HistoryStoreError) -> Self {
        Self::fatal(err.0)
    }
}

impl From<Failure> for CheckpointStoreError {
    fn from(failure: Failure) -> Self {
        CheckpointStoreError(failure.message)
    }
}

impl From<Failure> for HistoryStoreError {
    fn from(failure: Failure) -> Self {
        HistoryStoreError(failure.message)
    }
}

impl From<MemoryStoreError> for Failure {
    fn from(err: MemoryStoreError) -> Self {
        Self::fatal(err.0)
    }
}

impl From<Failure> for MemoryStoreError {
    fn from(failure: Failure) -> Self {
        MemoryStoreError(failure.message)
    }
}

/// For tests: called at named points of a store call, where `true` makes the
/// call fail there as a lost connection would.
#[doc(hidden)]
pub type FaultHook = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// The failure `fault` raises at `point`, if it does.
pub(crate) fn fault_at(fault: &Option<FaultHook>, point: &str) -> Result<(), Failure> {
    match fault {
        Some(fault) if fault(point) => Err(Failure::injected(point)),
        _ => Ok(()),
    }
}

/// Runs a store's calls under its [`StoreRetry`]: an attempt that fails for a
/// transient reason is made again after the policy's wait, on tokio's clock,
/// until the attempts run out.
#[derive(Debug, Clone)]
pub(crate) struct Retrier {
    policy: StoreRetry,
    store: &'static str,
}

impl Retrier {
    pub fn new(policy: StoreRetry, store: &'static str) -> Self {
        Self { policy, store }
    }

    /// Whether the call that just failed with `failure` on attempt `number`
    /// (from 1) is made again. If it is, this has counted the repeat and
    /// waited before saying so.
    pub async fn again(&self, operation: &'static str, number: u32, failure: &Failure) -> bool {
        if !failure.transient || number >= self.policy.max_attempts {
            return false;
        }
        metrics::counter!(RETRIES_METRIC, STORE_LABEL => self.store, OPERATION_LABEL => operation)
            .increment(1);
        tracing::warn!(
            strata.store = self.store,
            strata.store.operation = operation,
            strata.store.attempt = number,
            error.type = failure.class.as_str(),
            "a store call failed for a transient reason and is made again"
        );
        let delay = self.policy.delay_before(number, jitter_sample());
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        true
    }

    /// Makes `attempt` until it returns, fails for good, or the attempts run
    /// out. For calls that are the same done once or twice.
    pub async fn run<T, F, Fut>(
        &self,
        operation: &'static str,
        mut attempt: F,
    ) -> Result<T, Failure>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, Failure>>,
    {
        let mut number = 1;
        loop {
            match attempt().await {
                Ok(value) => return Ok(value),
                Err(failure) => {
                    if !self.again(operation, number, &failure).await {
                        return Err(failure);
                    }
                }
            }
            number += 1;
        }
    }
}

/// A sample in `[0, 1)` for jitter, from the standard library's randomly
/// seeded hasher: enough to spread waits, and no dependency.
fn jitter_sample() -> f64 {
    use std::hash::{BuildHasher, Hasher, RandomState};
    let bits = RandomState::new().build_hasher().finish();
    (bits >> 11) as f64 / (1u64 << 53) as f64
}

/// What became of a save whose attempt may or may not have been written
/// (`design/34-Storage-Hardening.md`, "A save that may have happened").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SaveFate {
    /// The checkpoint is in the log where the save would have put it.
    Saved,
    /// A conditional save's place was taken by another checkpoint: it lost
    /// the race.
    Lost,
    /// Nothing was written: the save is made again.
    NotWritten,
}

/// Whether `stored` is `saved` as a store gives it back: the same wire JSON,
/// keys in any order (a `JSONB` column doesn't keep them).
pub(crate) fn same(saved: &Checkpoint, stored: Option<&Checkpoint>) -> bool {
    stored.is_some_and(
        |stored| match (serde_json::to_value(saved), serde_json::to_value(stored)) {
            (Ok(saved), Ok(stored)) => saved == stored,
            _ => false,
        },
    )
}

/// What a look at the log says about a save of `saved` that may have
/// happened: for a conditional one, `next` is the entry right after its
/// expected sequence; for a plain one, the thread's latest.
pub(crate) fn fate_of(
    saved: &Checkpoint,
    next: Option<&Checkpoint>,
    conditional: bool,
) -> SaveFate {
    if same(saved, next) {
        SaveFate::Saved
    } else if conditional && next.is_some() {
        SaveFate::Lost
    } else {
        SaveFate::NotWritten
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datalium_strata::schema::CheckpointStatus;
    use serde_json::json;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn transient() -> Failure {
        Failure::injected("test")
    }

    fn fast(attempts: u32) -> Retrier {
        Retrier::new(
            StoreRetry::new()
                .with_max_attempts(attempts)
                .with_initial_delay(Duration::ZERO),
            "test",
        )
    }

    fn checkpoint(thread_id: &str, state: serde_json::Value) -> Checkpoint {
        Checkpoint {
            schema_version: "1.0.0".to_string(),
            thread_id: thread_id.to_string(),
            status: CheckpointStatus::Interrupted,
            suspended_node: Some("Review".to_string()),
            next_node: Some("Send".to_string()),
            frozen_state: state,
            metadata: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            resumed_at: None,
            nested: None,
            decision: None,
            wait: None,
            graph: None,
        }
    }

    #[test]
    fn the_default_policy_makes_four_attempts_from_fifty_milliseconds_to_two_seconds() {
        let policy = StoreRetry::new();
        assert_eq!(policy.max_attempts(), 4);
        assert_eq!(policy.initial_delay(), Duration::from_millis(50));
        assert_eq!(policy.max_delay(), Duration::from_secs(2));
        assert_eq!(StoreRetry::none().max_attempts(), 1);
        assert_eq!(StoreRetry::new().with_max_attempts(0).max_attempts(), 1);
    }

    #[test]
    fn a_delay_doubles_from_the_initial_one_and_stops_at_the_cap() {
        let policy = StoreRetry::new();
        let millis = |repeat| policy.delay_before(repeat, 1.0).as_millis();
        assert_eq!(millis(1), 50);
        assert_eq!(millis(2), 100);
        assert_eq!(millis(3), 200);
        assert_eq!(millis(6), 1600);
        assert_eq!(millis(7), 2000);
        assert_eq!(millis(40), 2000);
    }

    #[test]
    fn a_delay_is_jittered_over_the_whole_of_it() {
        let policy = StoreRetry::new();
        assert_eq!(policy.delay_before(3, 0.0), Duration::ZERO);
        assert_eq!(policy.delay_before(3, 0.5), Duration::from_millis(100));
        assert_eq!(policy.delay_before(3, 1.0), Duration::from_millis(200));
        // A sample outside 0 to 1 is brought back into it.
        assert_eq!(policy.delay_before(3, 7.0), Duration::from_millis(200));
        assert_eq!(policy.delay_before(3, -1.0), Duration::ZERO);
        for _ in 0..100 {
            assert!((0.0..1.0).contains(&jitter_sample()));
        }
    }

    #[tokio::test]
    async fn a_transient_failure_is_repeated_until_the_call_succeeds() {
        let calls = AtomicU32::new(0);
        let result = fast(4)
            .run("load", || async {
                if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                    Err(transient())
                } else {
                    Ok("done")
                }
            })
            .await;
        assert_eq!(result.unwrap(), "done");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn the_attempts_run_out_and_the_last_failure_is_the_calls() {
        let calls = AtomicU32::new(0);
        let result: Result<(), Failure> = fast(4)
            .run("load", || async {
                let number = calls.fetch_add(1, Ordering::SeqCst) + 1;
                Err(Failure {
                    message: format!("failure {number}"),
                    ..transient()
                })
            })
            .await;
        assert_eq!(result.unwrap_err().message, "failure 4");
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn a_failure_that_is_not_transient_is_not_repeated() {
        let calls = AtomicU32::new(0);
        let result: Result<(), Failure> = fast(4)
            .run("load", || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(Failure::fatal("relation does not exist"))
            })
            .await;
        assert_eq!(result.unwrap_err().message, "relation does not exist");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn no_retries_means_one_attempt() {
        let calls = AtomicU32::new(0);
        let retry = Retrier::new(StoreRetry::none(), "test");
        let result: Result<(), Failure> = retry
            .run("load", || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(transient())
            })
            .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn the_waits_between_attempts_are_the_policys_on_tokios_clock() {
        let retry = Retrier::new(
            StoreRetry::new()
                .with_initial_delay(Duration::from_secs(10))
                .with_max_delay(Duration::from_secs(15)),
            "test",
        );
        let started = tokio::time::Instant::now();
        let result: Result<(), Failure> = retry.run("load", || async { Err(transient()) }).await;
        assert!(result.is_err());
        // Three waits, jittered below 10 s, 15 s and 15 s: time passed, and no
        // more than the policy allows.
        assert!(started.elapsed() <= Duration::from_secs(40));
    }

    #[test]
    fn a_fault_hook_fails_only_the_points_it_names() {
        let hook: Option<FaultHook> = Some(Arc::new(|point| point == "save:after-commit"));
        assert!(fault_at(&hook, "save:before-commit").is_ok());
        let failure = fault_at(&hook, "save:after-commit").unwrap_err();
        assert!(failure.transient);
        assert!(fault_at(&None, "save:after-commit").is_ok());
    }

    #[test]
    fn a_stored_checkpoint_is_the_saved_one_whatever_order_its_keys_came_back_in() {
        let saved = checkpoint("t", json!({"amount": 12, "payee": "Initech"}));
        let stored = checkpoint("t", json!({"payee": "Initech", "amount": 12}));
        assert!(same(&saved, Some(&stored)));
        assert!(!same(
            &saved,
            Some(&checkpoint("t", json!({"amount": 13, "payee": "Initech"})))
        ));
        assert!(!same(&saved, None));
    }

    #[test]
    fn a_look_at_the_log_says_what_became_of_a_save() {
        let saved = checkpoint("t", json!({"amount": 12}));
        let other = checkpoint("t", json!({"amount": 99}));
        // Its own checkpoint is there: saved, conditional or not.
        assert_eq!(fate_of(&saved, Some(&saved), true), SaveFate::Saved);
        assert_eq!(fate_of(&saved, Some(&saved), false), SaveFate::Saved);
        // Another took a conditional save's place: it lost. A plain save
        // whose latest is another's is made again.
        assert_eq!(fate_of(&saved, Some(&other), true), SaveFate::Lost);
        assert_eq!(fate_of(&saved, Some(&other), false), SaveFate::NotWritten);
        // Nothing there: made again.
        assert_eq!(fate_of(&saved, None, true), SaveFate::NotWritten);
        assert_eq!(fate_of(&saved, None, false), SaveFate::NotWritten);
    }
}
